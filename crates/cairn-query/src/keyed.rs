// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Tables by key**: tables where every query must name exactly one entity —
//! one invocation (`id = …`) or one object (`service = … AND key = …`). No key
//! ⇒ the query is refused at planning time (the asker's error), never a scan
//! of a whole range.
//!
//! Each table is a [`Spec`]: columns, the accepted key sets, and a function
//! reading one entity from the owner range's snapshot. An `=` condition on a
//! key column is `Exact` (rows read out carry exactly that value); other
//! conditions are left for DataFusion to filter. An owner range on another
//! node ⇒ the same read function runs there ([`crate::remote`], `Scan::Keyed`).
//!
//! Tenant: tables by `id` read metadata to learn the invocation's tenant (one
//! point read) and return nothing when the asker cannot see that tenant; tables
//! by object take the tenant from the key (`tenant = …`, or the asker's tenant).

use {
    crate::{
        ctx::{QueryCtx, TenantScope},
        QueryError,
    },
    cairn_store::{
        id_from_hex, id_hex,
        keys::{self, ArtifactRow, ChunkBody, StreamChunk},
        partition_key_of,
        query_index::{self as qi, InvocationMeta},
        range_read::RangeSnapshot,
        InvocationId, PartitionKey, Scope, TenantId,
    },
    datafusion::{
        arrow::{
            array::{ArrayRef, BooleanBuilder, Int64Builder, RecordBatch, StringBuilder},
            datatypes::{DataType, Field, Schema, SchemaRef},
        },
        catalog::{Session, TableProvider},
        common::{DataFusionError, Result, ScalarValue},
        datasource::{memory::MemorySourceConfig, TableType},
        logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown},
        physical_plan::ExecutionPlan,
    },
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, fmt, sync::Arc},
};

/// Byte cap of one value (payload, state, log body) returned in a cell.
pub const VALUE_MAX: usize = 64 << 10;

/// One cell — can travel over the wire (remote scan).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cell {
    Null,
    Str(String),
    Int(i64),
    Bool(bool),
}

#[derive(Clone, Copy, Debug)]
pub enum Ty {
    Str,
    Int,
    Bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Column {
    pub name: &'static str,
    pub ty: Ty,
    pub nullable: bool,
    pub doc: &'static str,
}

const fn c(name: &'static str, ty: Ty, nullable: bool, doc: &'static str) -> Column {
    Column {
        name,
        ty,
        nullable,
        doc,
    }
}

/// Key values filtered by `=`: `column → value`.
pub type Args = BTreeMap<String, String>;

type Rows = Vec<Vec<Cell>>;

/// One table by key.
pub struct Spec {
    pub name: &'static str,
    pub doc: &'static str,
    pub columns: &'static [Column],
    /// The accepted key sets; the query must have `=` on every column of at
    /// least one set.
    pub keys: &'static [&'static [&'static str]],
    /// Extra columns that may be filtered by `=` (optional) — still `Exact`.
    pub optional: &'static [&'static str],
}

/// Every table by key.
pub const TABLES: &[&Spec] = &[
    &JOURNAL,
    &STATE,
    &VALUE,
    &PROMISE,
    &ARTIFACT,
    &LOG,
    &VQUEUE_ENTRY,
    &DELAYED,
    &TIMER,
    &STREAM,
    &STREAM_RECORD,
    &PROMISE_WAITER,
    &OUTBOX,
    &INBOUND,
    &ATTACHMENT,
    &SPAN,
    &OBJECT,
];

pub fn spec(name: &str) -> Option<&'static Spec> {
    TABLES.iter().copied().find(|s| s.name == name)
}

impl Spec {
    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| {
                    let ty = match c.ty {
                        Ty::Str => DataType::Utf8,
                        Ty::Int => DataType::Int64,
                        Ty::Bool => DataType::Boolean,
                    };
                    Field::new(c.name, ty, c.nullable)
                })
                .collect::<Vec<_>>(),
        ))
    }

    fn key_column(&self, name: &str) -> bool {
        self.keys.iter().any(|k| k.contains(&name)) || self.optional.contains(&name)
    }

    /// The first complete key set that `args` satisfies; `None` ⇒ the query is refused.
    fn satisfied(&self, args: &Args) -> bool {
        self.keys
            .iter()
            .any(|k| k.iter().all(|c| args.contains_key(*c)))
    }

    fn refusal(&self) -> DataFusionError {
        let want = self
            .keys
            .iter()
            .map(|k| {
                k.iter()
                    .map(|c| format!("{c} = …"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            })
            .collect::<Vec<_>>()
            .join(" or ");
        DataFusionError::Plan(format!(
            "table {} requires a filter on its key: {want}",
            self.name
        ))
    }
}

/// `column = 'string'` (either way round) ⇒ `(column, string)`.
fn equality(e: &Expr) -> Option<(String, String)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = e
    else {
        return None;
    };
    let lit = |e: &Expr| match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
        _ => None,
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), v) | (v, Expr::Column(c)) => Some((c.name.clone(), lit(v)?)),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────── TableProvider

#[derive(Clone)]
pub struct KeyedTable {
    spec: &'static Spec,
    ctx: QueryCtx,
}

impl KeyedTable {
    pub fn new(spec: &'static Spec, ctx: QueryCtx) -> Self {
        Self { spec, ctx }
    }
}

impl fmt::Debug for KeyedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.spec.name)
    }
}

#[async_trait::async_trait]
impl TableProvider for KeyedTable {
    fn schema(&self) -> SchemaRef {
        self.spec.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match equality(f) {
                Some((col, _)) if self.spec.key_column(&col) => TableProviderFilterPushDown::Exact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut args = Args::new();
        for f in filters {
            if let Some((col, v)) = equality(f) {
                if !self.spec.key_column(&col) {
                    continue;
                }
                // Two different `=` on the same column: no rows.
                if args.get(&col).is_some_and(|old| *old != v) {
                    return empty(self.spec, projection);
                }
                args.insert(col, v);
            }
        }
        if !self.spec.satisfied(&args) {
            return Err(self.spec.refusal());
        }
        // A table (or a keyless path) only for cluster-admin: refuse before asking
        // any range — a node holding no range would turn the remote error into just `partial`.
        let bulk = self.spec.name == "sys_inbound"
            || (self.spec.name == "sys_outbox" && !args.contains_key("id"));
        if bulk {
            cluster_admin_only(&self.ctx, self.spec.name).map_err(df)?;
        }
        // One entity: read right at planning time (a few rows, read-capped),
        // return an in-memory source.
        let mut rows = local(&self.ctx, self.spec, &args).await.map_err(df)?;
        let far = crate::remote::fan_out(&self.ctx, || crate::remote::Scan::Keyed {
            name: self.spec.name.to_string(),
            args: args.clone(),
        })
        .await
        .map_err(df)?;
        rows.extend(far.into_iter().flat_map(|r| r.cells));
        let batch = to_batch(self.spec, &rows)?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.spec.schema(),
            projection.cloned(),
        )?)
    }
}

fn empty(spec: &Spec, projection: Option<&Vec<usize>>) -> Result<Arc<dyn ExecutionPlan>> {
    let batch = to_batch(spec, &[])?;
    Ok(MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        spec.schema(),
        projection.cloned(),
    )?)
}

fn to_batch(spec: &Spec, rows: &[Vec<Cell>]) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(spec.columns.len());
    for (i, col) in spec.columns.iter().enumerate() {
        let cell = |r: &Vec<Cell>| r.get(i).cloned().unwrap_or(Cell::Null);
        cols.push(match col.ty {
            Ty::Str => {
                let mut b = StringBuilder::new();
                for r in rows {
                    match cell(r) {
                        Cell::Str(s) => b.append_value(s),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Ty::Int => {
                let mut b = Int64Builder::new();
                for r in rows {
                    match cell(r) {
                        Cell::Int(n) => b.append_value(n),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Ty::Bool => {
                let mut b = BooleanBuilder::new();
                for r in rows {
                    match cell(r) {
                        Cell::Bool(v) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        });
    }
    Ok(RecordBatch::try_new(spec.schema(), cols)?)
}

// ─────────────────────────────────────────────────────── reads

/// Read on the local ranges of `ctx` (also the serving side of a remote scan).
pub(crate) async fn local(ctx: &QueryCtx, spec: &Spec, args: &Args) -> Result<Rows, QueryError> {
    let mut out = Vec::new();
    for h in &ctx.ranges {
        let range = h.range().id.0;
        let snap = match h.snapshot() {
            Ok(s) => s,
            Err(e) => {
                ctx.warn_partial(Some(range), &e);
                continue;
            }
        };
        let got = match spec.name {
            "sys_journal" => journal(ctx, &snap, args).await,
            "sys_state" => state(ctx, &snap, args).await,
            "sys_value" => value_refs(ctx, &snap, args).await,
            "sys_promise" => promise(ctx, &snap, args).await,
            "sys_artifact" => artifact(ctx, &snap, args).await,
            "sys_log" => log(ctx, &snap, args).await,
            "sys_vqueue_entry" => vqueue_entry(ctx, &snap, args).await,
            "sys_delayed" => delayed(ctx, &snap).await,
            "sys_timer" => timer(ctx, &snap, args).await,
            "sys_stream" => stream(ctx, &snap, args).await,
            "sys_stream_record" => stream_record(ctx, &snap, args).await,
            "sys_promise_waiter" => promise_waiter(ctx, &snap, args).await,
            "sys_outbox" => outbox(ctx, &snap, args).await,
            "sys_inbound" => inbound(ctx, &snap).await,
            "sys_attachment" => attachment(ctx, &snap, args).await,
            "sys_span" => span(ctx, &snap, args).await,
            "sys_object" => object(ctx, &snap, args).await,
            other => Err(QueryError::Internal(format!("no table {other}"))),
        };
        match got {
            Ok(rows) => out.extend(rows),
            // An error of the query (cap, role) belongs to the whole query, not to one range.
            Err(e @ (QueryError::Budget(_) | QueryError::Sql(_))) => return Err(e),
            Err(e) => ctx.warn_partial(Some(range), &e),
        }
    }
    Ok(out)
}

/// An error of the query (role, parameters) stays the asker's error after DataFusion.
fn df(e: QueryError) -> DataFusionError {
    match e {
        QueryError::Sql(m) => DataFusionError::Plan(m),
        e => DataFusionError::Execution(e.to_string()),
    }
}

fn s(v: impl Into<String>) -> Cell {
    Cell::Str(v.into())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// User bytes: verbatim if UTF-8, hex otherwise; cut at [`VALUE_MAX`].
fn text(b: &[u8]) -> Cell {
    let b = &b[..b.len().min(VALUE_MAX)];
    match std::str::from_utf8(b) {
        Ok(t) => s(t),
        Err(_) => s(hex(b)),
    }
}

fn owns(snap: &RangeSnapshot, pk: PartitionKey) -> bool {
    snap.handle().range().contains_key(pk)
}

/// The invocation of `args["id"]` if this range holds it **and** the asker can
/// see its tenant (one metadata point read).
async fn visible_id(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    args: &Args,
) -> Result<Option<InvocationId>, QueryError> {
    let Some(id) = args.get("id").and_then(|h| id_from_hex(h)) else {
        return Ok(None);
    };
    if !owns(snap, partition_key_of(&id)) {
        return Ok(None);
    }
    ctx.charge(0, 1)?;
    let meta = snap
        .get(&qi::meta_key(&id))
        .await?
        .and_then(|raw| InvocationMeta::decode(&raw));
    Ok(meta.filter(|m| ctx.sees(&m.tenant)).map(|_| id))
}

/// The object of `args` (`tenant`, `scope`, `service`, `key`) if this range owns
/// it and the asker can see the tenant.
fn visible_object(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    args: &Args,
) -> Option<(PartitionKey, TenantId, Option<Scope>, String, String)> {
    let tenant = match (args.get("tenant"), &ctx.tenant) {
        (Some(t), _) => t.clone(),
        (None, TenantScope::One(t)) => t.clone(),
        (None, TenantScope::All) => TenantId::default().as_str().to_string(),
    };
    if !ctx.sees(&tenant) {
        return None;
    }
    let tenant = TenantId::new(&tenant).ok()?;
    let scope = match args.get("scope") {
        Some(sc) => Some(Scope::new(sc).ok()?),
        None => None,
    };
    let service = args.get("service")?.clone();
    let key = args.get("key")?.clone();
    let pk = keys::object_owner(&tenant, scope.as_ref(), &service, &key)?;
    owns(snap, pk).then_some((pk, tenant, scope, service, key))
}

/// Every row under `prefix`, read-capped.
async fn under(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    prefix: Vec<u8>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, QueryError> {
    ctx.stats
        .seeks
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let end = qi::prefix_end(&prefix);
    let budget = ctx.read_budget.saturating_sub(ctx.stats.units()) as usize + 1;
    let rows = snap.scan(prefix, end, budget, false, true).await?;
    ctx.charge(rows.len() as u64, 0)?;
    Ok(rows
        .into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect())
}

const JOURNAL: Spec = Spec {
    name: "sys_journal",
    doc: "Journal of one invocation, in write order.",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c("index", Ty::Int, false, "entry index"),
        c("ty", Ty::Str, false, "message type code, as `0x0402`"),
        c(
            "name",
            Ty::Str,
            true,
            "message type name (`Sleep`, `Call`, …)",
        ),
        c("payload", Ty::Str, false, "payload as hex, cut at 64 KiB"),
        c("payload_len", Ty::Int, false, "payload size, bytes"),
        c(
            "entry_lite",
            Ty::Str,
            false,
            "decoded head, JSON: `kind`, `name`, target (`service`/`handler`/`key`), `invocationId`, `wakeUpAtMs`, `promise`, `stateKey`, `signal`, `failure`, `awaits`/`completes`, `completed` (commands awaiting a result: `false` = still waiting), `hasBody`",
        ),
        c(
            "body",
            Ty::Str,
            true,
            "decoded user body, JSON `{encoding: json|utf8|base64, size, truncated, text|base64}`, cut at 64 KiB; NULL when the entry has no body",
        ),
    ],
    keys: &[&["id"]],
    optional: &[],
};

async fn journal(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    for (k, v) in under(ctx, snap, keys::journal_prefix(&id)).await? {
        let Some(index) = k
            .len()
            .checked_sub(4)
            .and_then(|at| k.get(at..))
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_be_bytes)
        else {
            continue;
        };
        let Ok(entry) = cairn_runtime::journal::JournalEntry::decode(bytes::Bytes::from(v)) else {
            continue;
        };
        entries.push((index, entry.ty.raw(), entry.payload));
    }
    // Decoding needs the whole journal: "waiting" = a command with no notification yet.
    let decoded = crate::journal_json::decode_journal(
        &entries
            .iter()
            .map(|(_, ty, p)| (*ty, p.clone()))
            .collect::<Vec<_>>(),
        VALUE_MAX,
    );
    Ok(entries
        .into_iter()
        .zip(decoded)
        .map(|((index, ty, payload), d)| {
            vec![
                s(id_hex(&id)),
                Cell::Int(i64::from(index)),
                s(format!("0x{ty:04X}")),
                cairn_protocol::MessageType(ty).name().map_or(Cell::Null, s),
                s(hex(&payload[..payload.len().min(VALUE_MAX)])),
                Cell::Int(payload.len() as i64),
                s(d.lite.to_string()),
                d.body.map_or(Cell::Null, |b| s(b.to_string())),
            ]
        })
        .collect())
}

const STATE: Spec = Spec {
    name: "sys_state",
    doc: "State of one virtual object.",
    columns: &[
        c("tenant", Ty::Str, false, "tenant"),
        c("scope", Ty::Str, true, "scope (absent = no scope)"),
        c("service", Ty::Str, false, "service"),
        c("key", Ty::Str, false, "object key"),
        c(
            "state_key",
            Ty::Str,
            false,
            "state key (UTF-8, hex otherwise)",
        ),
        c(
            "value",
            Ty::Str,
            true,
            "value (UTF-8, hex otherwise), cut at 64 KiB; `NULL` for a segmented value (the bytes live in segments)",
        ),
        c("value_len", Ty::Int, false, "value size (logical), bytes"),
    ],
    keys: &[&["service", "key"]],
    optional: &["tenant", "scope"],
};

fn object_cells(tenant: &TenantId, scope: &Option<Scope>, service: &str, key: &str) -> Vec<Cell> {
    vec![
        s(tenant.as_str()),
        scope.as_ref().map_or(Cell::Null, |sc| s(sc.as_str())),
        s(service),
        s(key),
    ]
}

async fn state(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some((pk, tenant, scope, service, key)) = visible_object(ctx, snap, args) else {
        return Ok(Vec::new());
    };
    let prefix = keys::state_prefix(pk, &tenant, scope.as_ref(), &service, &key);
    let mut out: Vec<(Vec<u8>, Vec<Cell>)> = Vec::new();
    for (k, v) in under(ctx, snap, prefix).await? {
        let Some(user) = keys::parse_state_user_key(&k) else {
            continue;
        };
        let mut row = object_cells(&tenant, &scope, &service, &key);
        row.extend([text(user), text(&v), Cell::Int(v.len() as i64)]);
        out.push((user.to_vec(), row));
    }
    // Segmented value: the logical length, no bytes. A corrupt row is skipped —
    // `verify` check (n) reports it.
    let seg = keys::segmented_state_prefix(pk, &tenant, scope.as_ref(), &service, &key);
    for (k, v) in under(ctx, snap, seg).await? {
        let (Some(user), Some(r)) = (
            keys::parse_segmented_state_user_key(&k),
            cairn_store::value::SegmentedRow::decode(&bytes::Bytes::copy_from_slice(&v)),
        ) else {
            continue;
        };
        let mut row = object_cells(&tenant, &scope, &service, &key);
        row.extend([text(user), Cell::Null, Cell::Int(r.len as i64)]);
        out.push((user.to_vec(), row));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out.into_iter().map(|(_, r)| r).collect())
}

const VALUE: Spec = Spec {
    name: "sys_value",
    doc: "Segments of the segmented values of one object: one row per \
          (holder, segment) of the owner object — a segment with several holders has several rows. \
          Bytes/segments of the owner: `count(DISTINCT sha || gen)`, `sum(len)` over the rows \
          `holder_kind = 'state'`.",
    columns: &[
        c("tenant", Ty::Str, false, "tenant"),
        c("scope", Ty::Str, true, "scope (absent = no scope)"),
        c("service", Ty::Str, false, "service"),
        c("key", Ty::Str, false, "object key"),
        c(
            "holder_kind",
            Ty::Str,
            false,
            "`state` (the current value of a key), `invocation` (journal), `promise`",
        ),
        c(
            "holder",
            Ty::Str,
            false,
            "state key (UTF-8, hex otherwise), invocation id (48 hex) or promise name",
        ),
        c("sha", Ty::Str, false, "sha256 of the segment (64 hex)"),
        c("gen", Ty::Str, false, "sealing generation (16 hex)"),
        c("len", Ty::Int, false, "segment size, bytes"),
    ],
    keys: &[&["service", "key"]],
    optional: &["tenant", "scope"],
};

async fn value_refs(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    use cairn_store::value::{decode_ref_row, object_owner, Holder};
    let Some((_, tenant, scope, service, key)) = visible_object(ctx, snap, args) else {
        return Ok(Vec::new());
    };
    let Some((owner_pk, owner)) = object_owner(&tenant, scope.as_ref(), &service, &key) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (k, v) in under(
        ctx,
        snap,
        keys::value_refs_prefix(owner_pk, &tenant, &owner),
    )
    .await?
    {
        // A corrupt row is skipped — `verify` check (n) reports it.
        let (Some(r), Some(len)) = (keys::parse_value_ref(&k), decode_ref_row(&v)) else {
            continue;
        };
        let (kind, holder) = match &r.holder {
            Holder::State(k) => ("state", text(k)),
            Holder::Invocation(id) => ("invocation", s(id_hex(id))),
            Holder::Promise(n) => ("promise", s(n.clone())),
        };
        let mut row = object_cells(&tenant, &scope, &service, &key);
        row.extend([
            s(kind),
            holder,
            s(hex(&r.sha)),
            s(format!("{:016x}", r.gen)),
            Cell::Int(len as i64),
        ]);
        out.push(row);
    }
    Ok(out)
}

const PROMISE: Spec = Spec {
    name: "sys_promise",
    doc: "**Completed** durable promises of one object (workflow). Pending promises: the waiter's `sys_journal`.",
    columns: &[
        c("tenant", Ty::Str, false, "tenant"),
        c("scope", Ty::Str, true, "scope"),
        c("service", Ty::Str, false, "service"),
        c("key", Ty::Str, false, "object key"),
        c("name", Ty::Str, false, "promise name"),
        c("ok", Ty::Bool, false, "completed with a value (not a failure)"),
        c("value", Ty::Str, true, "value (UTF-8, hex otherwise), cut at 64 KiB"),
        c("failure_code", Ty::Int, true, "error code"),
        c("failure_message", Ty::Str, true, "error message"),
        c("completed_by", Ty::Str, false, "id of the invocation that completed it"),
    ],
    keys: &[&["service", "key"]],
    optional: &["tenant", "scope"],
};

async fn promise(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    use cairn_engine::{PromiseRecord, ReplyResult};
    let Some((pk, tenant, scope, service, key)) = visible_object(ctx, snap, args) else {
        return Ok(Vec::new());
    };
    let prefix = keys::promises_prefix(pk, &tenant, scope.as_ref(), &service, &key);
    let mut out = Vec::new();
    for (k, v) in under(ctx, snap, prefix).await? {
        let Some(name) = keys::parse_promise_name(&k) else {
            continue;
        };
        let Ok(rec) = PromiseRecord::decode(bytes::Bytes::from(v)) else {
            continue;
        };
        let mut row = object_cells(&tenant, &scope, &service, &key);
        row.push(text(name));
        match &rec.result {
            ReplyResult::Value(v) => {
                row.extend([Cell::Bool(true), text(v), Cell::Null, Cell::Null])
            }
            ReplyResult::Failure { code, message } => row.extend([
                Cell::Bool(false),
                Cell::Null,
                Cell::Int(i64::from(*code)),
                s(message.clone()),
            ]),
            // A promise value is always inline; a segmented record has no build
            // path — never read the tail as the whole value.
            ReplyResult::Segmented(_) => {
                row.extend([Cell::Bool(true), Cell::Null, Cell::Null, Cell::Null])
            }
        }
        row.push(s(id_hex(&rec.completer.0)));
        out.push(row);
    }
    Ok(out)
}

const ARTIFACT: Spec = Spec {
    name: "sys_artifact",
    doc: "Artifacts of one invocation (content: REST `/invocations/{id}/artifacts/{sha}`).",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c("sha256", Ty::Str, false, "content hash, hex"),
        c(
            "size",
            Ty::Int,
            true,
            "size, bytes (absent when the row cannot be read)",
        ),
        c("content_type", Ty::Str, true, "content type"),
        c("epoch", Ty::Int, true, "writer epoch"),
    ],
    keys: &[&["id"]],
    optional: &[],
};

async fn artifact(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (k, v) in under(ctx, snap, keys::artifacts_prefix(&id)).await? {
        let Some((_, sha)) = keys::parse_artifact(&k) else {
            continue;
        };
        let row = ArtifactRow::decode(&v);
        out.push(vec![
            s(id_hex(&id)),
            s(hex(&sha)),
            row.as_ref()
                .map_or(Cell::Null, |r| Cell::Int(r.size as i64)),
            row.as_ref()
                .map_or(Cell::Null, |r| s(r.content_type.clone())),
            row.as_ref()
                .map_or(Cell::Null, |r| Cell::Int(r.epoch as i64)),
        ]);
    }
    Ok(out)
}

/// Name of the handler log stream (`cairn-server::streams::LOG`).
const LOG_STREAM: &str = "$log";

const LOG: Spec = Spec {
    name: "sys_log",
    doc: "Handler log (stream `$log`) of one invocation, by offset.",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c("offset", Ty::Int, false, "record offset"),
        c("step", Ty::Int, false, "step"),
        c("attempt", Ty::Int, false, "attempt"),
        c(
            "kind",
            Ty::Str,
            false,
            "`data`, `gap` (dropped by the cap), `superseded`",
        ),
        c("body", Ty::Str, true, "body (UTF-8, hex otherwise)"),
    ],
    keys: &[&["id"]],
    optional: &[],
};

async fn log(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (k, v) in under(ctx, snap, keys::stream_prefix(&id, LOG_STREAM)).await? {
        let Some((_, _, first)) = keys::parse_stream_chunk(&k) else {
            continue;
        };
        let Some(chunk) = StreamChunk::decode(first, &v) else {
            continue;
        };
        for r in chunk.records {
            let (kind, body) = match &r.body {
                ChunkBody::Data(b) => ("data", text(b)),
                ChunkBody::Gap { dropped } => ("gap", s(format!("{dropped} records dropped"))),
                ChunkBody::Superseded { step, attempt } => (
                    "superseded",
                    s(format!("superseded by step {step} attempt {attempt}")),
                ),
            };
            out.push(vec![
                s(id_hex(&id)),
                Cell::Int(r.offset as i64),
                Cell::Int(i64::from(r.step)),
                Cell::Int(i64::from(r.attempt)),
                s(kind),
                body,
            ]);
        }
    }
    Ok(out)
}

const VQUEUE_ENTRY: Spec = Spec {
    name: "sys_vqueue_entry",
    doc: "Entries of one virtual queue in order (`Index::VQueue`): who holds the slot/lock (`running`), who waits (`inbox`, by `seq`). The queue of an object: `service` + `key`; of an unkeyed service: `service`.",
    columns: &[
        c("tenant", Ty::Str, false, "tenant"),
        c("scope", Ty::Str, true, "scope"),
        c("limit_key", Ty::Str, true, "limit key"),
        c("service", Ty::Str, false, "service"),
        c("key", Ty::Str, true, "object key (the object's exclusive queue)"),
        c("stage", Ty::Str, false, "`inbox`, `running`, `suspended`, `paused`"),
        c("seq", Ty::Int, false, "order in the queue (of the range holding the entry)"),
        c("id", Ty::Str, false, "invocation id"),
        c("range", Ty::Int, false, "range holding the entry"),
    ],
    keys: &[&["service"]],
    optional: &["tenant", "scope", "limit_key", "key"],
};

async fn vqueue_entry(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    args: &Args,
) -> Result<Rows, QueryError> {
    let tenant = match (args.get("tenant"), &ctx.tenant) {
        (Some(t), _) => t.clone(),
        (None, TenantScope::One(t)) => t.clone(),
        (None, TenantScope::All) => TenantId::default().as_str().to_string(),
    };
    if !ctx.sees(&tenant) {
        return Ok(Vec::new());
    }
    let bad = |e: String| QueryError::Sql(e);
    let queue = cairn_store::QueueName {
        tenant: TenantId::new(&tenant).map_err(|e| bad(e.to_string()))?,
        scope: args
            .get("scope")
            .map(|s| Scope::new(s))
            .transpose()
            .map_err(|e| bad(e.to_string()))?,
        limit: args
            .get("limit_key")
            .map(|l| cairn_store::LimitKey::parse(l))
            .transpose()
            .map_err(|e| bad(e.to_string()))?,
        exclusive: args.contains_key("key"),
        service: args.get("service").cloned().unwrap_or_default(),
        object_key: args.get("key").cloned().unwrap_or_default(),
    };
    let encoded = queue.encode();
    let range = i64::from(snap.handle().range().id.0);
    let mut out = Vec::new();
    for (k, _) in under(ctx, snap, keys::ix_vqueue_prefix(&encoded)).await? {
        let Some((q, stage, seq, id)) = keys::parse_ix_vqueue(&k) else {
            continue;
        };
        if q != encoded {
            continue;
        }
        out.push(vec![
            s(queue.tenant.as_str()),
            queue.scope.as_ref().map_or(Cell::Null, |sc| s(sc.as_str())),
            queue
                .limit
                .as_ref()
                .map_or(Cell::Null, |l| s(l.to_string())),
            s(queue.service.clone()),
            if queue.exclusive {
                s(queue.object_key.clone())
            } else {
                Cell::Null
            },
            s(format!("{stage:?}").to_lowercase()),
            Cell::Int(seq as i64),
            s(id_hex(&id)),
            Cell::Int(range),
        ]);
    }
    Ok(out)
}

const DELAYED: Spec = Spec {
    name: "sys_delayed",
    doc: "Delayed one-way calls not yet due (`Index::Delayed`), by due time. No key needed: the index holds only pending calls (the query's read cap bounds the size).",
    columns: &[
        c("caller_id", Ty::Str, false, "id of the sending invocation"),
        c("completion_id", Ty::Int, false, "completion of the send command"),
        c("due_at_ms", Ty::Int, false, "time it will be sent, ms"),
        c("tenant", Ty::Str, false, "tenant of the sender"),
        c("service", Ty::Str, true, "target service"),
        c("handler", Ty::Str, true, "target handler"),
        c("key", Ty::Str, true, "target object key"),
        c("range", Ty::Int, false, "range holding the call"),
    ],
    keys: &[&[]],
    optional: &[],
};

async fn delayed(ctx: &QueryCtx, snap: &RangeSnapshot) -> Result<Rows, QueryError> {
    let range = i64::from(snap.handle().range().id.0);
    let mut tenants: BTreeMap<InvocationId, Option<String>> = BTreeMap::new();
    let mut out = Vec::new();
    for (k, _) in under(ctx, snap, keys::index_prefix(keys::Index::Delayed)).await? {
        let Some((due, caller, cid)) = keys::parse_ix_delayed(&k) else {
            continue;
        };
        let tenant = match tenants.get(&caller) {
            Some(t) => t.clone(),
            None => {
                ctx.charge(0, 1)?;
                let t = snap
                    .get(&qi::meta_key(&caller))
                    .await?
                    .and_then(|raw| InvocationMeta::decode(&raw))
                    .map(|m| m.tenant);
                tenants.insert(caller, t.clone());
                t
            }
        };
        let Some(tenant) = tenant.filter(|t| ctx.sees(t)) else {
            continue;
        };
        ctx.charge(0, 1)?;
        let target = snap
            .get(&keys::delayed(&caller, cid))
            .await?
            .and_then(|raw| {
                let (_, row) = keys::decode_due(&raw)?;
                match cairn_engine::OutboxEntry::decode(bytes::Bytes::copy_from_slice(row)).ok()? {
                    cairn_engine::OutboxEntry::Call(c) => {
                        Some((c.service, c.handler, c.object_key))
                    }
                    _ => None,
                }
            });
        let (svc, h, key) = match target {
            Some((svc, h, key)) => (
                s(svc),
                s(h),
                if key.is_empty() { Cell::Null } else { s(key) },
            ),
            None => (Cell::Null, Cell::Null, Cell::Null),
        };
        out.push(vec![
            s(id_hex(&caller)),
            Cell::Int(i64::from(cid)),
            Cell::Int(due as i64),
            s(tenant),
            svc,
            h,
            key,
            Cell::Int(range),
        ]);
    }
    Ok(out)
}

const TIMER: Spec = Spec {
    name: "sys_timer",
    doc: "Pending timers (`ctx.sleep`) of one invocation — \"what is it waiting for\" in time.",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c(
            "completion_id",
            Ty::Int,
            false,
            "completion the timer completes",
        ),
        c("due_at_ms", Ty::Int, false, "due time, ms"),
    ],
    keys: &[&["id"]],
    optional: &[],
};

async fn timer(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let prefix = keys::entity(partition_key_of(&id), keys::Table::Timers, &id);
    let mut out = Vec::new();
    for (k, v) in under(ctx, snap, prefix).await? {
        let (Some((_, cid)), Some((due, _))) = (keys::parse_timer(&k), keys::decode_due(&v)) else {
            continue;
        };
        out.push(vec![
            s(id_hex(&id)),
            Cell::Int(i64::from(cid)),
            Cell::Int(due as i64),
        ]);
    }
    Ok(out)
}

/// Every chunk of every stream of `id`: `(name, first offset, chunk)`.
async fn chunks(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    id: &InvocationId,
    name: Option<&str>,
) -> Result<Vec<(String, StreamChunk)>, QueryError> {
    let prefix = match name {
        Some(n) => keys::stream_prefix(id, n),
        None => keys::streams_prefix(id),
    };
    let mut out = Vec::new();
    for (k, v) in under(ctx, snap, prefix).await? {
        let Some((_, n, first)) = keys::parse_stream_chunk(&k) else {
            continue;
        };
        if name.is_some_and(|want| want != n) {
            continue;
        }
        if let Some(c) = StreamChunk::decode(first, &v) {
            out.push((n, c));
        }
    }
    Ok(out)
}

const STREAM: Spec = Spec {
    name: "sys_stream",
    doc: "Durable streams of one invocation (`ctx.stream`, `$log`, `$trace`), one row per stream.",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c("name", Ty::Str, false, "stream name"),
        c("chunks", Ty::Int, false, "chunk count"),
        c("records", Ty::Int, false, "record count"),
        c(
            "total_bytes",
            Ty::Int,
            false,
            "payload bytes received (cumulative)",
        ),
        c("dropped", Ty::Int, false, "records dropped by the cap"),
        c("last_offset", Ty::Int, true, "offset of the last record"),
    ],
    keys: &[&["id"]],
    optional: &[],
};

async fn stream(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let mut by: BTreeMap<String, (i64, i64, i64, i64, Option<i64>)> = BTreeMap::new();
    for (n, c) in chunks(ctx, snap, &id, None).await? {
        let e = by.entry(n).or_default();
        e.0 += 1;
        e.1 += c.records.len() as i64;
        // A chunk's two counters are cumulative up to the end of the chunk: the last chunk carries the total.
        e.2 = e.2.max(c.total_bytes as i64);
        e.3 = e.3.max(c.dropped as i64);
        if let Some(r) = c.records.last() {
            e.4 = Some(e.4.unwrap_or(0).max(r.offset as i64));
        }
    }
    Ok(by
        .into_iter()
        .map(|(n, (chunks, records, bytes, dropped, last))| {
            vec![
                s(id_hex(&id)),
                s(n),
                Cell::Int(chunks),
                Cell::Int(records),
                Cell::Int(bytes),
                Cell::Int(dropped),
                last.map_or(Cell::Null, Cell::Int),
            ]
        })
        .collect())
}

const STREAM_RECORD: Spec = Spec {
    name: "sys_stream_record",
    doc: "Records of one stream of one invocation, by offset.",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c("name", Ty::Str, false, "stream name"),
        c("offset", Ty::Int, false, "offset"),
        c("step", Ty::Int, false, "step"),
        c("attempt", Ty::Int, false, "attempt"),
        c("kind", Ty::Str, false, "`data`, `gap`, `superseded`"),
        c(
            "body",
            Ty::Str,
            true,
            "body (UTF-8, hex otherwise), cut at 64 KiB",
        ),
    ],
    keys: &[&["id", "name"]],
    optional: &[],
};

async fn stream_record(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    args: &Args,
) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let name = args.get("name").cloned().unwrap_or_default();
    let mut out = Vec::new();
    for (n, chunk) in chunks(ctx, snap, &id, Some(&name)).await? {
        for r in chunk.records {
            let (kind, body) = match &r.body {
                ChunkBody::Data(b) => ("data", text(b)),
                ChunkBody::Gap { dropped } => ("gap", s(format!("{dropped} records dropped"))),
                ChunkBody::Superseded { step, attempt } => (
                    "superseded",
                    s(format!("superseded by step {step} attempt {attempt}")),
                ),
            };
            out.push(vec![
                s(id_hex(&id)),
                s(n.clone()),
                Cell::Int(r.offset as i64),
                Cell::Int(i64::from(r.step)),
                Cell::Int(i64::from(r.attempt)),
                s(kind),
                body,
            ]);
        }
    }
    Ok(out)
}

const PROMISE_WAITER: Spec = Spec {
    name: "sys_promise_waiter",
    doc: "Who is waiting for the **not yet** completed promises of one object (workflow).",
    columns: &[
        c("tenant", Ty::Str, false, "tenant"),
        c("scope", Ty::Str, true, "scope"),
        c("service", Ty::Str, false, "service"),
        c("key", Ty::Str, false, "object key"),
        c("name", Ty::Str, false, "promise name"),
        c("waiter_id", Ty::Str, false, "id of the waiting invocation"),
        c("completion_id", Ty::Int, false, "completion of the waiter"),
    ],
    keys: &[&["service", "key"]],
    optional: &["tenant", "scope"],
};

async fn promise_waiter(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    args: &Args,
) -> Result<Rows, QueryError> {
    let Some((pk, tenant, scope, service, key)) = visible_object(ctx, snap, args) else {
        return Ok(Vec::new());
    };
    let prefix = keys::promise_waiters_object_prefix(pk, &tenant, scope.as_ref(), &service, &key);
    let mut out = Vec::new();
    for (k, _) in under(ctx, snap, prefix).await? {
        let Some((name, waiter, cid)) = keys::parse_promise_waiter(&k) else {
            continue;
        };
        let mut row = object_cells(&tenant, &scope, &service, &key);
        row.extend([text(name), s(id_hex(&waiter)), Cell::Int(i64::from(cid))]);
        out.push(row);
    }
    Ok(out)
}

// ───────────────────────────────────── calls between invocations

/// A table only cluster-admin reads without a key.
fn cluster_admin_only(ctx: &QueryCtx, table: &str) -> Result<(), QueryError> {
    match ctx.tenant {
        TenantScope::All => Ok(()),
        TenantScope::One(_) => Err(QueryError::Sql(format!(
            "{table} without an `id` filter is reserved for cluster-admin"
        ))),
    }
}

const OUTBOX: Spec = Spec {
    name: "sys_outbox",
    doc: "Undelivered calls / replies / watches / cancels (`Outbox`). Filtered by `id` (sender): rows that invocation produced; unfiltered: the whole range in production order, cluster-admin only. Replies of a finished invocation are keyed by the receiver — visible only unfiltered.",
    columns: &[
        c("range", Ty::Int, false, "range holding the row"),
        c("seq", Ty::Int, false, "production order"),
        c("id", Ty::Str, false, "invocation in the key (the caller; the receiver for `reply`)"),
        c("completion_id", Ty::Int, false, "completion in the key"),
        c("kind", Ty::Str, false, "`call`, `reply`, `attach`, `peek_output`, `cancel`, `fire`, `signal` (`completion_id` is the send entry), `unknown`"),
        c("target", Ty::Str, true, "target invocation (`attach`, `peek_output`, `cancel`)"),
        c("service", Ty::Str, true, "target service (`call`, `fire`)"),
        c("handler", Ty::Str, true, "target handler (`call`, `fire`)"),
        c("key", Ty::Str, true, "target object key (`call`, `fire`)"),
    ],
    keys: &[&["id"], &[]],
    optional: &[],
};

fn outbox_row(
    range: i64,
    seq: u64,
    id: &InvocationId,
    cid: u32,
    payload: &[u8],
    signal: bool,
) -> Vec<Cell> {
    use cairn_engine::OutboxEntry as E;
    let none = || (Cell::Null, Cell::Null, Cell::Null, Cell::Null);
    let opt = |k: &str| if k.is_empty() { Cell::Null } else { s(k) };
    let (kind, (target, svc, h, key)) = match E::decode(bytes::Bytes::copy_from_slice(payload)) {
        _ if signal => ("signal", none()),
        Ok(E::Call(c)) => (
            "call",
            (Cell::Null, s(c.service), s(c.handler), opt(&c.object_key)),
        ),
        Ok(E::Reply(_)) => ("reply", none()),
        Ok(E::Attach(w)) => (
            "attach",
            (s(id_hex(&w.target)), Cell::Null, Cell::Null, Cell::Null),
        ),
        Ok(E::PeekOutput(w)) => (
            "peek_output",
            (s(id_hex(&w.target)), Cell::Null, Cell::Null, Cell::Null),
        ),
        Ok(E::Cancel(c)) => (
            "cancel",
            (s(id_hex(&c.target)), Cell::Null, Cell::Null, Cell::Null),
        ),
        Ok(E::Fire(f)) => (
            "fire",
            (Cell::Null, s(f.service), s(f.handler), opt(&f.object_key)),
        ),
        _ => ("unknown", none()),
    };
    vec![
        Cell::Int(range),
        Cell::Int(seq as i64),
        s(id_hex(id)),
        Cell::Int(i64::from(cid)),
        s(kind),
        target,
        svc,
        h,
        key,
    ]
}

async fn outbox(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let range = i64::from(snap.handle().range().id.0);
    let mut out = Vec::new();
    if args.contains_key("id") {
        let Some(id) = visible_id(ctx, snap, args).await? else {
            return Ok(Vec::new());
        };
        for (table, signal) in [
            (keys::Table::Outbox, false),
            (keys::Table::SignalOutbox, true),
        ] {
            let prefix = keys::entity(partition_key_of(&id), table, &id);
            for (k, v) in under(ctx, snap, prefix).await? {
                let parsed = if signal {
                    keys::parse_signal_outbox(&k)
                } else {
                    keys::parse_outbox(&k)
                };
                let (Some((rid, cid)), Some((seq, payload))) = (parsed, keys::decode_seq(&v))
                else {
                    continue;
                };
                if rid == id {
                    out.push(outbox_row(range, seq, &rid, cid, payload, signal));
                }
            }
        }
        return Ok(out);
    }
    cluster_admin_only(ctx, "sys_outbox")?;
    for (ix, signal) in [(keys::Index::Outbox, false), (keys::Index::Signal, true)] {
        for (k, _) in under(ctx, snap, keys::index_prefix(ix)).await? {
            let Some((_, ekey)) = keys::parse_ix_seq(&k, ix) else {
                continue;
            };
            let parsed = if signal {
                keys::parse_signal_outbox(ekey)
            } else {
                keys::parse_outbox(ekey)
            };
            let Some((id, cid)) = parsed else {
                continue;
            };
            ctx.charge(0, 1)?;
            if let Some((seq, payload)) =
                snap.get(ekey).await?.as_deref().and_then(keys::decode_seq)
            {
                out.push(outbox_row(range, seq, &id, cid, payload, signal));
            }
        }
    }
    Ok(out)
}

const INBOUND: Spec = Spec {
    name: "sys_inbound",
    doc: "Cross-range messages received, not yet applied (`Inbound`), in receive order. cluster-admin only.",
    columns: &[
        c("range", Ty::Int, false, "receiving range"),
        c("seq", Ty::Int, false, "receive order"),
        c(
            "kind",
            Ty::Str,
            false,
            "`start_call`, `completion`, `attach`, `peek_output`, `signal`, `cancel`",
        ),
        c(
            "id",
            Ty::Str,
            false,
            "invocation in the key (the callee for `start_call`, otherwise the sender / waiter)",
        ),
        c("completion_id", Ty::Int, false, "completion in the key"),
        c("bytes", Ty::Int, false, "message size"),
    ],
    keys: &[&[]],
    optional: &[],
};

async fn inbound(ctx: &QueryCtx, snap: &RangeSnapshot) -> Result<Rows, QueryError> {
    cluster_admin_only(ctx, "sys_inbound")?;
    use keys::InboundKind as K;
    let range = i64::from(snap.handle().range().id.0);
    let mut out = Vec::new();
    for (k, _) in under(ctx, snap, keys::index_prefix(keys::Index::Inbound)).await? {
        let Some((seq, ekey)) = keys::parse_ix_seq(&k, keys::Index::Inbound) else {
            continue;
        };
        let Some((kind, id, cid)) = keys::parse_inbound(ekey) else {
            continue;
        };
        ctx.charge(0, 1)?;
        let Some(raw) = snap.get(ekey).await? else {
            continue;
        };
        let kind = match kind {
            K::StartCall => "start_call",
            K::Completion => "completion",
            K::Attach => "attach",
            K::PeekOutput => "peek_output",
            K::Signal => "signal",
            K::Cancel => "cancel",
        };
        out.push(vec![
            Cell::Int(range),
            Cell::Int(seq as i64),
            s(kind),
            s(id_hex(&id)),
            Cell::Int(i64::from(cid)),
            Cell::Int(keys::decode_seq(&raw).map_or(0, |(_, p)| p.len()) as i64),
        ]);
    }
    Ok(out)
}

const ATTACHMENT: Spec = Spec {
    name: "sys_attachment",
    doc: "Extra waiters of one invocation (attach by id / workflow, duplicate calls): who is waiting for the result.",
    columns: &[
        c("id", Ty::Str, false, "the awaited invocation"),
        c("caller_id", Ty::Str, false, "the waiting invocation"),
        c("completion_id", Ty::Int, false, "completion of the waiter that receives the result"),
    ],
    keys: &[&["id"]],
    optional: &[],
};

async fn attachment(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (k, _) in under(ctx, snap, keys::attachments_prefix(&id)).await? {
        if let Some((caller, cid)) = keys::parse_attachment(&k) {
            out.push(vec![
                s(id_hex(&id)),
                s(id_hex(&caller)),
                Cell::Int(i64::from(cid)),
            ]);
        }
    }
    Ok(out)
}

// ─────────────────────────────────────────────────── spans

const SPAN: Spec = Spec {
    name: "sys_span",
    doc: "Spans of the `$trace` of one invocation, one row per span (`start` ∪ `end`, like `GET /invocations/{id}/spans` but flat — the tree is built with `parent_id`). Durable chunks only: records still in the leader's RAM are not seen.",
    columns: &[
        c("id", Ty::Str, false, "invocation id"),
        c("span_id", Ty::Str, false, "span id"),
        c("parent_id", Ty::Str, true, "parent span"),
        c("name", Ty::Str, false, "name"),
        c("start_ts", Ty::Str, true, "`ts` of `start` (JSON)"),
        c("end_ts", Ty::Str, true, "`ts` of `end` (JSON)"),
        c("status", Ty::Str, true, "status at `end`"),
        c("unfinished", Ty::Bool, false, "has `start`, no `end`"),
        c("start_lost", Ty::Bool, false, "has `end`, no `start`"),
        c("attrs", Ty::Str, false, "merged attributes (JSON)"),
        c("input_tokens", Ty::Int, true, "`attrs.input_tokens` at `end`"),
        c("output_tokens", Ty::Int, true, "`attrs.output_tokens` at `end`"),
        c("step", Ty::Int, false, "step"),
        c("attempt", Ty::Int, false, "attempt"),
    ],
    keys: &[&["id"]],
    optional: &[],
};

#[derive(Default)]
struct SpanRow {
    parent: Option<String>,
    name: String,
    start_ts: Option<String>,
    end_ts: Option<String>,
    status: Option<String>,
    attrs: serde_json::Map<String, serde_json::Value>,
    tokens: (Option<i64>, Option<i64>),
    step: u32,
    attempt: u32,
    started: bool,
    ended: bool,
}

async fn span(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let Some(id) = visible_id(ctx, snap, args).await? else {
        return Ok(Vec::new());
    };
    let json_text = |v: &serde_json::Value| (!v.is_null()).then(|| v.to_string());
    let mut order: Vec<String> = Vec::new();
    let mut spans: BTreeMap<String, SpanRow> = BTreeMap::new();
    for (_, chunk) in chunks(ctx, snap, &id, Some("$trace")).await? {
        for r in chunk.records {
            let ChunkBody::Data(b) = &r.body else {
                continue;
            };
            let Ok(d) = serde_json::from_slice::<serde_json::Value>(b) else {
                continue;
            };
            let Some(sid) = d["span_id"].as_str().map(str::to_string) else {
                continue;
            };
            let kind = d["kind"].as_str();
            if !matches!(kind, Some("start" | "end")) {
                continue;
            }
            if !spans.contains_key(&sid) {
                order.push(sid.clone());
            }
            let n = spans.entry(sid).or_default();
            let start = kind == Some("start");
            if start || !n.started {
                n.name = d["name"].as_str().unwrap_or_default().to_string();
                n.parent = d["parent_id"].as_str().map(str::to_string);
                n.step = r.step;
                n.attempt = r.attempt;
            }
            if start {
                n.start_ts = json_text(&d["ts"]);
                n.started = true;
            } else {
                n.end_ts = json_text(&d["ts"]);
                n.status = d["status"].as_str().map(str::to_string);
                n.tokens = (
                    d["attrs"]["input_tokens"].as_i64(),
                    d["attrs"]["output_tokens"].as_i64(),
                );
                n.ended = true;
            }
            if let Some(m) = d["attrs"].as_object() {
                for (k, v) in m {
                    n.attrs.insert(k.clone(), v.clone());
                }
            }
        }
    }
    let opt = |v: Option<String>| v.map_or(Cell::Null, Cell::Str);
    Ok(order
        .into_iter()
        .filter_map(|sid| spans.remove(&sid).map(|n| (sid, n)))
        .map(|(sid, n)| {
            vec![
                s(id_hex(&id)),
                s(sid),
                opt(n.parent),
                s(n.name),
                opt(n.start_ts),
                opt(n.end_ts),
                opt(n.status),
                Cell::Bool(n.started && !n.ended),
                Cell::Bool(n.ended && !n.started),
                s(serde_json::Value::Object(n.attrs).to_string()),
                n.tokens.0.map_or(Cell::Null, Cell::Int),
                n.tokens.1.map_or(Cell::Null, Cell::Int),
                Cell::Int(i64::from(n.step)),
                Cell::Int(i64::from(n.attempt)),
            ]
        })
        .collect())
}

// ─────────────────────────────────────────── objects of a service

const OBJECT: Spec = Spec {
    name: "sys_object",
    doc: "Objects (virtual objects, workflows) of one service — the `ObjectKeys` hint index: one row per object that ever had an invocation, **checked** on read (the object still has state or an invocation; hidden otherwise). Order by backlog: `JOIN sys_vqueue`.",
    columns: &[
        c("tenant", Ty::Str, false, "tenant"),
        c("scope", Ty::Str, true, "scope"),
        c("service", Ty::Str, false, "service"),
        c("key", Ty::Str, false, "object key"),
        c("has_state", Ty::Bool, false, "still has state"),
        c("invocations", Ty::Int, false, "invocations still held (counted up to 16; 16 = at least 16)"),
        c("range", Ty::Int, false, "owner range"),
    ],
    keys: &[&["service"]],
    optional: &["tenant", "scope"],
};

/// Cap on counting an object's invocations during the check.
const OBJECT_INVOCATIONS_MAX: usize = 16;

async fn object(ctx: &QueryCtx, snap: &RangeSnapshot, args: &Args) -> Result<Rows, QueryError> {
    let tenant = match (args.get("tenant"), &ctx.tenant) {
        (Some(t), _) => t.clone(),
        (None, TenantScope::One(t)) => t.clone(),
        (None, TenantScope::All) => TenantId::default().as_str().to_string(),
    };
    if !ctx.sees(&tenant) {
        return Ok(Vec::new());
    }
    let Some(service) = args.get("service") else {
        return Ok(Vec::new());
    };
    let ix = &qi::OBJECT_KEYS;
    // A range that has not finished building the descriptor: return nothing from it (the asker sees `indexing`).
    let readable = snap.readable_indexes().await?;
    if !readable
        .iter()
        .any(|r| r.name == ix.name && r.version == ix.version)
    {
        ctx.warn(Some(snap.handle().range().id.0), "indexing");
        return Ok(Vec::new());
    }
    let Some(prefix) = qi::scan_prefix(
        ix,
        &[qi::Fixed::Tenant(&tenant), qi::Fixed::Service(service)],
    ) else {
        return Ok(Vec::new());
    };
    let tid = TenantId::new(&tenant).map_err(|e| QueryError::Sql(e.to_string()))?;
    let range = i64::from(snap.handle().range().id.0);
    let mut out = Vec::new();
    for (k, v) in under(ctx, snap, prefix).await? {
        let Some(row) = qi::decode_row(ix, &k, &v) else {
            continue;
        };
        let (Some(key), scope) = (row.key, row.scope.filter(|s| !s.is_empty())) else {
            continue;
        };
        if args
            .get("scope")
            .is_some_and(|want| Some(want) != scope.as_ref())
        {
            continue;
        }
        let sc = match &scope {
            Some(s) => match Scope::new(s) {
                Ok(s) => Some(s),
                Err(_) => continue,
            },
            None => None,
        };
        let Some(pk) = keys::object_owner(&tid, sc.as_ref(), service, &key) else {
            continue;
        };
        if !owns(snap, pk) {
            continue;
        }
        // Check the hint: does it still have state, does any invocation of this
        // very object remain (pk may collide — compare metadata).
        let state = keys::state_prefix(pk, &tid, sc.as_ref(), service, &key);
        let seg = keys::segmented_state_prefix(pk, &tid, sc.as_ref(), service, &key);
        ctx.charge(0, 2)?;
        let has_state = !snap
            .scan(state.clone(), qi::prefix_end(&state), 1, false, true)
            .await?
            .is_empty()
            || !snap
                .scan(seg.clone(), qi::prefix_end(&seg), 1, false, true)
                .await?
                .is_empty();
        let metas = keys::entity(pk, keys::Table::InvocationMeta, &[]);
        let found = snap
            .scan(
                metas.clone(),
                qi::prefix_end(&metas),
                OBJECT_INVOCATIONS_MAX * 4,
                false,
                true,
            )
            .await?;
        ctx.charge(found.len() as u64, 0)?;
        let invocations = found
            .iter()
            .filter_map(|(_, raw)| InvocationMeta::decode(raw))
            .filter(|m| {
                m.tenant == tenant
                    && m.service == *service
                    && m.key == key
                    && m.scope == scope.clone().unwrap_or_default()
            })
            .take(OBJECT_INVOCATIONS_MAX)
            .count();
        if !has_state && invocations == 0 {
            continue;
        }
        out.push(vec![
            s(tenant.clone()),
            scope.map_or(Cell::Null, Cell::Str),
            s(service.clone()),
            s(key),
            Cell::Bool(has_state),
            Cell::Int(invocations as i64),
            Cell::Int(range),
        ]);
    }
    Ok(out)
}
