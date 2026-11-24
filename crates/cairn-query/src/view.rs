// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **View tables**: RAM/bucket tables mirroring an existing REST read endpoint
//! — deployments, services, handlers, queues, scheduler, limits, ranges, keys,
//! metering.
//!
//! A table is a [`ViewSpec`]: the REST path, where the row array sits in the
//! returned JSON, columns taken from JSON keys. The server runs exactly that
//! `GET` **in process** (through its own router, carrying the asker's key —
//! [`ViewSource`]), so roles, tenant filtering and the data shape are REST's;
//! there is no second copy to drift. Columns are declared statically
//! (DataFusion needs the schema at planning time); a server test compares the
//! declared columns with the real JSON (`views_match_rest`).
//!
//! View tables read no range's store (cost no read units) and push no filter
//! down: DataFusion filters the rows it already has.

use {
    crate::{keyed::Cell, QueryError},
    datafusion::{
        arrow::{
            array::{
                ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
            },
            datatypes::{DataType, Field, Schema, SchemaRef},
        },
        catalog::{Session, TableProvider},
        common::ScalarValue,
        common::{DataFusionError, Result},
        datasource::{memory::MemorySourceConfig, TableType},
        logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown},
        physical_plan::ExecutionPlan,
    },
    serde_json::Value,
    std::{fmt, sync::Arc},
};

#[derive(Clone, Copy, Debug)]
pub enum VTy {
    Str,
    Int,
    Bool,
    Float,
    /// Nested array/object: a JSON string.
    Json,
}

#[derive(Clone, Copy, Debug)]
pub struct VCol {
    /// SQL column name.
    pub name: &'static str,
    /// JSON key of the element; `^key` is a key of the **parent** element
    /// (tables that flatten [`ViewSpec::each`]); `/a/b` is a JSON pointer in the element.
    pub key: &'static str,
    pub ty: VTy,
    pub doc: &'static str,
}

const fn v(name: &'static str, key: &'static str, ty: VTy, doc: &'static str) -> VCol {
    VCol { name, key, ty, doc }
}

pub struct ViewSpec {
    pub name: &'static str,
    pub doc: &'static str,
    /// `GET` path (with query) on the node's ingress.
    pub path: &'static str,
    /// JSON pointer to the row array (`""` = the root). An object instead of an
    /// array ⇒ one row.
    pub pointer: &'static str,
    /// Flatten a nested array: each child element is one row.
    pub each: Option<&'static str>,
    /// Columns that must be filtered by `=`: the value replaces `{column}` in
    /// [`Self::path`] (`/archive?date={date}`). Missing ⇒ the query is refused, as for tables by key.
    pub params: &'static [&'static str],
    pub columns: &'static [VCol],
}

/// Run `GET path` with the asker's identity.
#[async_trait::async_trait]
pub trait ViewSource: Send + Sync {
    async fn get(&self, path: &str) -> Result<Value, QueryError>;
}

impl ViewSpec {
    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| {
                    let ty = match c.ty {
                        VTy::Str | VTy::Json => DataType::Utf8,
                        VTy::Int => DataType::Int64,
                        VTy::Bool => DataType::Boolean,
                        VTy::Float => DataType::Float64,
                    };
                    Field::new(c.name, ty, true)
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// Rows (one cell per column each) from the endpoint's JSON.
    pub fn rows(&self, doc: &Value) -> Vec<Vec<Cell>> {
        let items: Vec<&Value> = match doc.pointer(self.pointer) {
            Some(Value::Array(a)) => a.iter().collect(),
            Some(o @ Value::Object(_)) => vec![o],
            _ => Vec::new(),
        };
        let mut out = Vec::new();
        for item in items {
            match self.each {
                None => out.push(self.row(None, item)),
                Some(field) => {
                    for child in item
                        .get(field)
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        out.push(self.row(Some(item), child));
                    }
                }
            }
        }
        out
    }

    fn row(&self, parent: Option<&Value>, item: &Value) -> Vec<Cell> {
        self.columns
            .iter()
            .map(|c| {
                let v = match c.key.strip_prefix('^') {
                    Some(k) => parent.and_then(|p| p.get(k)),
                    None if c.key.starts_with('/') => item.pointer(c.key),
                    None => item.get(c.key),
                };
                cell(c.ty, v)
            })
            .collect()
    }
}

fn cell(ty: VTy, v: Option<&Value>) -> Cell {
    match (ty, v) {
        (_, None | Some(Value::Null)) => Cell::Null,
        (VTy::Float, Some(v)) => v.as_f64().map_or(Cell::Null, |f| Cell::Str(f.to_string())),
        (VTy::Int, Some(v)) => v
            .as_i64()
            .or_else(|| v.as_u64().map(|u| u.min(i64::MAX as u64) as i64))
            .map_or(Cell::Null, Cell::Int),
        (VTy::Bool, Some(v)) => v.as_bool().map_or(Cell::Null, Cell::Bool),
        (VTy::Str, Some(Value::String(s))) => Cell::Str(s.clone()),
        (VTy::Str | VTy::Json, Some(v)) => Cell::Str(v.to_string()),
    }
}

fn to_batch(spec: &ViewSpec, rows: &[Vec<Cell>]) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(spec.columns.len());
    for (i, c) in spec.columns.iter().enumerate() {
        let at = |r: &Vec<Cell>| r.get(i).cloned().unwrap_or(Cell::Null);
        cols.push(match c.ty {
            VTy::Str | VTy::Json => {
                let mut b = StringBuilder::new();
                for r in rows {
                    match at(r) {
                        Cell::Str(s) => b.append_value(s),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            VTy::Int => {
                let mut b = Int64Builder::new();
                for r in rows {
                    match at(r) {
                        Cell::Int(n) => b.append_value(n),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            VTy::Bool => {
                let mut b = BooleanBuilder::new();
                for r in rows {
                    match at(r) {
                        Cell::Bool(x) => b.append_value(x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            VTy::Float => {
                // The cell carries the float as a string (`Cell` has no float
                // variant — it travels over the remote-scan wire and must be `Eq`).
                let mut b = Float64Builder::new();
                for r in rows {
                    match at(r) {
                        Cell::Str(s) => b.append_option(s.parse::<f64>().ok()),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        });
    }
    Ok(RecordBatch::try_new(spec.schema(), cols)?)
}

#[derive(Clone)]
pub struct ViewTable {
    spec: &'static ViewSpec,
    source: Arc<dyn ViewSource>,
}

impl ViewTable {
    pub fn new(spec: &'static ViewSpec, source: Arc<dyn ViewSource>) -> Self {
        Self { spec, source }
    }
}

impl fmt::Debug for ViewTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.spec.name)
    }
}

#[async_trait::async_trait]
impl TableProvider for ViewTable {
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
        // Path parameters: the value is known at planning time, DataFusion still
        // re-filters the rows (`Inexact`).
        Ok(filters
            .iter()
            .map(|f| match param_eq(f) {
                Some((c, _)) if self.spec.params.contains(&c.as_str()) => {
                    TableProviderFilterPushDown::Inexact
                }
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
        let mut path = self.spec.path.to_string();
        for p in self.spec.params {
            let v = filters
                .iter()
                .filter_map(param_eq)
                .find(|(c, _)| c == p)
                .map(|(_, v)| v)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "table {} requires a filter `{p} = '…'`",
                        self.spec.name
                    ))
                })?;
            if !param_value_ok(&v) {
                return Err(DataFusionError::Plan(format!(
                    "{}: value {v:?} of {p} is not valid",
                    self.spec.name
                )));
            }
            path = path.replace(&format!("{{{p}}}"), &v);
        }
        let doc = self.source.get(&path).await.map_err(|e| {
            let msg = format!("{}: {e}", self.spec.name);
            // The asker's error at REST (role, parameters) stays the asker's
            // error at `/query`.
            if e.is_client() {
                DataFusionError::Plan(msg)
            } else {
                DataFusionError::Execution(msg)
            }
        })?;
        let batch = to_batch(self.spec, &self.spec.rows(&doc))?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.spec.schema(),
            projection.cloned(),
        )?)
    }
}

/// A valid path parameter value: not empty, only digits, ASCII letters and `-_.:`
/// (no `/`, `?`, `&`, `%` — cannot escape the declared path).
fn param_value_ok(v: &str) -> bool {
    !v.is_empty()
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}

/// `path` is exactly the path some view table of [`VIEWS`] calls (parameters
/// replaced by valid values) — the parent node's check of a view-table request
/// from the child process of `/query`: the child cannot call any other `GET`
/// on the router, even though it carries the asker's key.
pub fn is_view_path(path: &str) -> bool {
    VIEWS.iter().any(|spec| {
        // Template `a{p}b{q}c`: fixed text segments interleaved with parameters.
        let mut rest = path;
        let mut tpl = spec.path;
        loop {
            match tpl.find('{') {
                None => return rest == tpl,
                Some(open) => {
                    let Some(close) = tpl[open..].find('}').map(|c| open + c) else {
                        return false;
                    };
                    let Some(after) = rest.strip_prefix(&tpl[..open]) else {
                        return false;
                    };
                    tpl = &tpl[close + 1..];
                    // The value runs up to the next fixed segment (or the end of the path).
                    let next = tpl.find('{').map_or(tpl, |n| &tpl[..n]);
                    let end = if next.is_empty() {
                        after.len()
                    } else {
                        match after.find(next) {
                            Some(e) => e,
                            None => return false,
                        }
                    };
                    if !param_value_ok(&after[..end]) {
                        return false;
                    }
                    rest = &after[end..];
                }
            }
        }
    })
}

/// `column = 'string'` (either side).
fn param_eq(e: &Expr) -> Option<(String, String)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = e
    else {
        return None;
    };
    let pick = |a: &Expr, b: &Expr| match (a, b) {
        (
            Expr::Column(c),
            Expr::Literal(
                ScalarValue::Utf8(Some(v))
                | ScalarValue::Utf8View(Some(v))
                | ScalarValue::LargeUtf8(Some(v)),
                _,
            ),
        ) => Some((c.name.clone(), v.clone())),
        _ => None,
    };
    pick(left, right).or_else(|| pick(right, left))
}

use VTy::{Bool, Float, Int, Json, Str};

/// Every view table.
pub const VIEWS: &[&ViewSpec] = &[
    &DEPLOYMENT,
    &SERVICE,
    &HANDLER,
    &VQUEUE,
    &SCHEDULER,
    &LIMIT,
    &RANGE,
    &KEY,
    &HANDLER_STATS,
    &METERING,
    &RULE,
    &SCHEDULE,
    &RANGE_OP,
    &RANGE_SUGGESTION,
    &QUARANTINE,
    &CAPTURE,
    &CLUSTER_VERSION,
    &SLOW_RUN,
    &NODE,
    &NODEWAL,
    &SCRUB,
    &TRASH,
    &PRICING,
    &TENANT,
    &ARCHIVE,
    &RANGE_LOAD,
    &RANGE_WINDOW,
    &HANDLER_HISTORY,
    &AUDIT,
];

const HANDLER_HISTORY: ViewSpec = ViewSpec {
    name: "sys_handler_history",
    doc: "Per-handler summary **by hour**, last 30 days (`GET /stats/history?days=30`; one `stats/` object per day per node) — charts from 24 h to 30 days.",
    path: "/stats/history?days=30",
    pointer: "/rows",
    each: None,
    params: &[],
    columns: &[
        v("hour_ms", "hourMs", Int, "start of the hour, ms"),
        v("tenant", "tenant", Str, "tenant"),
        v("service", "service", Str, "service"),
        v("handler", "handler", Str, "handler"),
        v("deployment", "deployment", Str, "deployment"),
        v("turns", "turns", Int, "SDK turns"),
        v("completed", "completed", Int, "completed invocations"),
        v("failed", "failed", Int, "failed invocations"),
        v("paused", "paused", Int, "suspended invocations"),
        v("failed_attempts", "failedAttempts", Int, "failed attempts"),
        v("suspensions", "suspensions", Int, "suspensions"),
        v("steps", "steps", Int, "durable steps"),
        v("run_ms", "runMs", Int, "run time, ms"),
        v("cu", "cu", Int, "CU"),
        v("p50_ms", "p50Ms", Float, "p50 turn duration, ms (bucket upper bound)"),
        v("p99_ms", "p99Ms", Float, "p99 turn duration, ms (bucket upper bound)"),
        v("max_ms", "maxMs", Float, "longest turn, ms"),
    ],
};

const RANGE_LOAD: ViewSpec = ViewSpec {
    name: "sys_range_load",
    doc: "Load per range, cluster-wide (`GET /ranges/stats?scope=cluster`) — the input of auto split.",
    path: "/ranges/stats?scope=cluster",
    pointer: "/ranges",
    each: None,
    params: &[],
    columns: &[
        v("range", "range", Int, "range"),
        v("node", "node", Str, "leader node"),
        v("windows", "windows", Int, "number of measurement windows"),
        v("ops_per_sec", "opsPerSec", Float, "commands per second"),
        v("turn_ms_per_sec", "turnMsPerSec", Float, "turn ms per second"),
        v("bytes_per_sec", "bytesPerSec", Float, "bytes written per second"),
        v(
            "turn_share",
            "turnShare",
            Float,
            "share of the node's turn time",
        ),
        v("hot_streak", "hotStreak", Int, "consecutive hot windows"),
        v("size_bytes", "sizeBytes", Int, "Db size, latest measurement"),
    ],
};

const RANGE_WINDOW: ViewSpec = ViewSpec {
    name: "sys_range_window",
    doc: "Per-range flush windows (`GET /ranges/windows?format=rows`, object `range-windows`).",
    path: "/ranges/windows?format=rows",
    pointer: "/rows",
    each: None,
    params: &[],
    columns: &[
        v("range", "range", Int, "range"),
        v("window_ms", "windowMs", Int, "flush window, ms"),
    ],
};

const NODE: ViewSpec = ViewSpec {
    name: "sys_node",
    doc: "Nodes leading at least one range, as each sees itself (`GET /cluster/nodes?scope=cluster`). Each node's `knows`: `sys_cluster_version`.",
    path: "/cluster/nodes?scope=cluster",
    pointer: "/rows",
    each: None,
    params: &[],
    columns: &[
        v("node", "node", Str, "node"),
        v("leading", "leading", Json, "ranges it leads (JSON)"),
        v("slow", "slow", Int, "published slowness score; NULL = not slow"),
        v("p99_put_ms", "p99PutMs", Int, "p99 PUT of the latest round, ms"),
        v("draining", "draining", Bool, "draining ranges away"),
        v(
            "disk_cache_bytes",
            "diskCacheBytes",
            Int,
            "bytes in the SST disk cache; NULL = off",
        ),
        v(
            "disk_cache_hit_ratio",
            "diskCacheHitRatio",
            Float,
            "disk cache partial hit ratio since start; NULL = off or nothing read yet",
        ),
    ],
};

const NODEWAL: ViewSpec = ViewSpec {
    name: "sys_nodewal",
    doc: "Live shared WAL logs: floor and the pinning ranges (`GET /admin/nodewal?scope=cluster`).",
    path: "/admin/nodewal?scope=cluster",
    pointer: "/rows",
    each: None,
    params: &[],
    columns: &[
        v("node", "node", Str, "node"),
        v("log", "log", Str, "log"),
        v(
            "floor",
            "floor",
            Int,
            "lowest data object still on the bucket",
        ),
        v("pinned_by", "pinnedByRange", Int, "first pinning range"),
        v(
            "pinned_wal_id",
            "pinnedWalId",
            Int,
            "wal id of the held frame",
        ),
        v("pinned_for_ms", "pinnedForMs", Int, "floor stuck for, ms"),
    ],
};

const SCRUB: ViewSpec = ViewSpec {
    name: "sys_scrub",
    doc: "Open findings of the latest round of the background scrubber (`GET /admin/scrub?scope=cluster`; only the runner node has rounds). Clean round ⇒ no rows.",
    path: "/admin/scrub?scope=cluster",
    pointer: "/rows",
    each: Some("findings"),
    params: &[],
    columns: &[
        v("node", "^node", Str, "node that ran the round"),
        v("at_ms", "^atMs", Int, "time the round finished, ms"),
        v("complete", "^complete", Bool, "complete round (skipped no more than 2 %)"),
        v("check", "check", Str, "check (`verify`)"),
        v("range", "range", Int, "range"),
        v("detail", "detail", Str, "detail"),
        v("heals", "heals", Str, "background loop that heals it; NULL = a real leak or corruption"),
    ],
};

const TRASH: ViewSpec = ViewSpec {
    name: "sys_trash",
    doc: "Trash entries (`GET /admin/trash`, like `cairn-ctl trash list`).",
    path: "/admin/trash",
    pointer: "/entries",
    each: None,
    params: &[],
    columns: &[
        v("entry", "entry", Str, "entry id"),
        v("created_ms", "createdMs", Int, "time it was trashed, ms"),
        v(
            "subject",
            "subject",
            Str,
            "what was trashed (`range-db 3`, `wal-log …`)",
        ),
        v("in_place", "inPlace", Bool, "held in place (artifact)"),
        v("reason", "reason", Str, "reason"),
        v("objects", "objects", Int, "number of objects under `obj/`"),
        v("bytes", "bytes", Int, "total bytes"),
        v("unreadable", "unreadable", Str, "error reading the entry"),
    ],
};

const PRICING: ViewSpec = ViewSpec {
    name: "sys_pricing",
    doc: "Token price list by model (`GET /metering/pricing`), micro-USD per 1 000 000 tokens.",
    path: "/metering/pricing",
    pointer: "/models",
    each: None,
    params: &[],
    columns: &[
        v("model", "model", Str, "model"),
        v("input", "input", Int, "input token price"),
        v("output", "output", Int, "output token price"),
    ],
};

const TENANT: ViewSpec = ViewSpec {
    name: "sys_tenant",
    doc: "Per-tenant settings (`GET /admin/tenants`): hash chain, slow log threshold, quarantine.",
    path: "/admin/tenants",
    pointer: "/tenants",
    each: None,
    params: &[],
    columns: &[
        v("tenant", "tenant", Str, "tenant"),
        v("hash_chain", "hashChain", Bool, "hash chain enabled"),
        v("slow_ms", "slowMs", Int, "own slow log threshold, ms"),
        v(
            "quarantine_mode",
            "quarantineMode",
            Str,
            "own quarantine mode",
        ),
        v(
            "quarantine_rule",
            "quarantineRule",
            Str,
            "own quarantine rule",
        ),
    ],
};

const ARCHIVE: ViewSpec = ViewSpec {
    name: "sys_archive",
    doc: "Purged invocations of the tenant in one UTC day (`GET /archive?date=…`; filtering by `date = 'yyyy-mm-dd'` is required). Journal, status, hash chain: `GET /archive/{id}`.",
    path: "/archive?date={date}",
    pointer: "",
    each: Some("entries"),
    params: &["date"],
    columns: &[
        v("date", "^date", Str, "UTC day"),
        v("id", "id", Str, "invocation id"),
        v("run", "run", Int, "run within the day (0 = first)"),
        v("bytes", "bytes", Int, "object size"),
        v("written_at_ms", "writtenAtMs", Int, "time the archive was written, ms"),
    ],
};

const SLOW_RUN: ViewSpec = ViewSpec {
    name: "sys_slow_run",
    doc: "Latest slow runs, cluster-wide (`GET /stats/slow-runs?scope=cluster`; a ring of 1 000 rows per node, capped at 10 rows/s).",
    path: "/stats/slow-runs?scope=cluster",
    pointer: "/runs",
    each: None,
    params: &[],
    columns: &[
        v("at_ms", "atMs", Int, "time recorded, ms"),
        v("node", "node", Str, "node"),
        v("tenant", "tenant", Str, "tenant"),
        v("service", "service", Str, "service"),
        v("handler", "handler", Str, "handler"),
        v("deployment", "deployment", Str, "deployment"),
        v("invocation", "invocation", Str, "invocation id"),
        v("outcome", "outcome", Str, "`completed`, `failed`, `paused`, `retry`, `suspended`, `error`"),
        v("total_ms", "totalMs", Int, "total time of the run, ms"),
        v("threshold_ms", "thresholdMs", Int, "the tenant's threshold, ms"),
        v("turns", "turns", Int, "SDK turns"),
        v("steps", "steps", Int, "durable steps"),
        v("run_ms", "runMs", Int, "run time, ms"),
        v("sdk_ms", "sdkMs", Int, "waiting for the SDK, ms"),
        v("durable_waits", "durableWaits", Int, "number of durability waits"),
        v("durable_wait_ms", "durableWaitMs", Int, "waiting for durability, ms"),
        v("engine_wait_ms", "engineWaitMs", Int, "waiting for the engine, ms"),
        v("permit_wait_ms", "permitWaitMs", Int, "waiting for a flow-control permit, ms"),
    ],
};

const RANGE_OP: ViewSpec = ViewSpec {
    name: "sys_range_op",
    doc: "Split/merge operations this node coordinates (`GET /ranges/ops`, cluster-admin).",
    path: "/ranges/ops",
    pointer: "/ops",
    each: None,
    params: &[],
    columns: &[
        v("id", "id", Str, "operation id"),
        v("kind", "kind", Str, "`split` / `merge`"),
        v("ranges", "ranges", Json, "source ranges (JSON)"),
        v("at", "at", Str, "split point (hex)"),
        v("state", "state", Json, "state (JSON)"),
        v("elapsed_ms", "elapsedMs", Float, "running for, ms"),
        v("error", "error", Json, "error (JSON)"),
    ],
};

const RANGE_SUGGESTION: ViewSpec = ViewSpec {
    name: "sys_range_suggestion",
    doc: "Suggestions to split hot/large ranges and merge cold adjacent ranges, cluster-wide (`GET /ranges/suggestions`, cluster-admin).",
    path: "/ranges/suggestions",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("range", "range", Int, "range"),
        v("reason", "reason", Json, "reason (JSON)"),
        v("at", "at", Str, "suggested split point (hex)"),
        v("balance", "balance", Float, "balance of the two halves"),
        v("blocked_by", "blockedBy", Str, "why it has not split by itself"),
        v("owner", "owner", Str, "node holding the range"),
        v(
            "turn_share",
            "turnShare",
            Float,
            "share of the node's turn time",
        ),
        v("hot_secs", "hotSecs", Int, "continuously hot, seconds"),
        v("size_bytes", "sizeBytes", Int, "size, bytes"),
        v("since_ms", "sinceMs", Int, "suggested since, ms"),
        v("node", "node", Str, "node making the suggestion"),
        v("merge_with", "mergeWith", Int, "merge suggestion: right range"),
        v("cold_secs", "coldSecs", Int, "merge suggestion: continuously cold, seconds"),
    ],
};

const QUARANTINE: ViewSpec = ViewSpec {
    name: "sys_quarantine",
    doc: "Quarantined handlers of the tenant (`GET /admin/quarantine`).",
    path: "/admin/quarantine",
    pointer: "/entries",
    each: None,
    params: &[],
    columns: &[
        v("service", "service", Str, "service"),
        v("handler", "handler", Str, "handler"),
        v("deployment", "deployment", Str, "deployment"),
        v("action", "action", Json, "action (JSON)"),
        v("since_ms", "sinceMs", Int, "since, ms"),
        v("expires_ms", "expiresMs", Int, "expires, ms"),
        v("reason", "reason", Str, "reason"),
    ],
};

const CAPTURE: ViewSpec = ViewSpec {
    name: "sys_capture",
    doc:
        "Live requests to capture the tenant's next run (`GET /admin/capture`); captured bundles: REST.",
    path: "/admin/capture",
    pointer: "/captures",
    each: None,
    params: &[],
    columns: &[
        v("id", "id", Str, "request id"),
        v("tenant", "tenant", Str, "tenant"),
        v("service", "service", Str, "service"),
        v("handler", "handler", Str, "handler"),
        v("deployment", "deployment", Str, "deployment"),
        v("remaining", "remaining", Int, "captures remaining"),
        v("expires_ms", "expiresMs", Int, "expires, ms"),
    ],
};

const CLUSTER_VERSION: ViewSpec = ViewSpec {
    name: "sys_cluster_version",
    doc: "Cluster-tier migration ledger (`GET /admin/cluster-version`, cluster-admin), one row per step.",
    path: "/admin/cluster-version",
    pointer: "/binary/ledger",
    each: None,
    params: &[],
    columns: &[
        v("to", "to", Int, "version after the step"),
        v("name", "name", Str, "step name"),
        v("tier", "tier", Str, "tier"),
    ],
};

const HANDLER_STATS: ViewSpec = ViewSpec {
    name: "sys_handler_stats",
    doc: "Per-handler summary, cluster-wide, default window (`GET /stats/handlers?scope=cluster`).",
    path: "/stats/handlers?scope=cluster",
    pointer: "/handlers",
    each: None,
    params: &[],
    columns: &[
        v("node", "node", Str, "node (absent in the merged row)"),
        v("tenant", "tenant", Str, "tenant"),
        v("service", "service", Str, "service"),
        v("handler", "handler", Str, "handler"),
        v("deployment", "deployment", Str, "deployment"),
        v("turns", "turns", Int, "turns"),
        v("completed", "completed", Int, "completed"),
        v("failed", "failed", Int, "failures"),
        v("paused", "paused", Int, "paused awaiting a human"),
        v("failed_attempts", "failedAttempts", Int, "failed attempts"),
        v("suspensions", "suspensions", Int, "suspensions"),
        v("steps", "steps", Int, "steps"),
        v("run_ms", "runMs", Int, "run time, ms"),
        v("cu", "cu", Int, "compute units"),
        v("p50_ms", "p50Ms", Float, "p50 turn, ms"),
        v("p99_ms", "p99Ms", Float, "p99 turn, ms"),
        v("max_ms", "maxMs", Float, "longest turn, ms"),
        v("quarantine", "quarantine", Str, "quarantined because of"),
    ],
};

const AUDIT: ViewSpec = ViewSpec {
    name: "sys_audit",
    doc: "Data-plane audit log of the tenant (`GET /audit`, last 24 hours): \
          every action by a person — rows in a range, intents/outcomes on objects, merged denials and \
          no-op outcomes.",
    path: "/audit",
    pointer: "/records",
    each: None,
    params: &[],
    columns: &[
        v("ts", "ts", Int, "time of the action, ms"),
        v("tenant", "tenant", Str, "tenant"),
        v(
            "kind",
            "kind",
            Str,
            "`row` (in a range), `intent`/`done` (on objects), `denied`/`noop` (merged)",
        ),
        v(
            "action",
            "action",
            Str,
            "action (`invocation.cancel`, `deployment.register`, …)",
        ),
        v(
            "outcome",
            "outcome",
            Str,
            "`ok`, `noop`, `not_found`, `pending`, `error`, `denied`, `unknown` (reconciled: the outcome never came)",
        ),
        v("actor_type", "/actor/type", Str, "actor type"),
        v("actor_id", "/actor/id", Str, "actor id (key id)"),
        v("actor_role", "/actor/role", Str, "role of the key"),
        v("target", "target", Json, "target (JSON)"),
        v("status", "status", Int, "HTTP code (outcome on objects)"),
        v("count", "count", Int, "number of occurrences (merged `denied`/`noop` records)"),
        v("ip", "ip", Str, "client IP"),
        v("user_agent", "userAgent", Str, "user agent"),
        v("request_id", "requestId", Str, "request id"),
        v("name", "name", Str, "object name"),
    ],
};

const METERING: ViewSpec = ViewSpec {
    name: "sys_metering",
    doc: "The tenant's metering for the current hour (`GET /metering`).",
    path: "/metering",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("tenant", "tenant", Str, "tenant"),
        v("hour", "hour", Str, "hour, `yyyy-mm-ddThh` UTC"),
        v("invocations", "invocations", Int, "invocation"),
        v("steps", "steps", Int, "steps"),
        v("run_ms", "runMs", Int, "run time, ms"),
        v("input_tokens", "inputTokens", Int, "input tokens"),
        v("output_tokens", "outputTokens", Int, "output tokens"),
        v("cost_micros", "costMicros", Int, "cost, millionths"),
        v(
            "sdk_bytes_out",
            "sdkBytesOut",
            Int,
            "wire bytes sent to the SDK endpoint (after compression)",
        ),
        v(
            "sdk_bytes_in",
            "sdkBytesIn",
            Int,
            "wire bytes received from the SDK endpoint",
        ),
        v(
            "units",
            "units",
            Int,
            "billing units: each durable command ⌈payload / 64 KiB⌉ (at least 1), +2 per invocation",
        ),
    ],
};

const RULE: ViewSpec = ViewSpec {
    name: "sys_rule",
    doc: "The cluster's rule book (`GET /rules`, cluster-admin).",
    path: "/rules",
    pointer: "/rules",
    each: None,
    params: &[],
    columns: &[
        v("pattern", "pattern", Str, "pattern"),
        v("concurrency", "concurrency", Str, "concurrency cap"),
        v("rate", "rate", Str, "rate cap"),
        v("burst", "burst", Int, "burst"),
        v("description", "description", Str, "description"),
        v("disabled", "disabled", Bool, "disabled"),
        v("modified_ms", "modifiedMs", Int, "time modified, ms"),
        v("level", "level", Int, "level"),
    ],
};

const SCHEDULE: ViewSpec = ViewSpec {
    name: "sys_schedule",
    doc: "Server-side schedules of the tenant (`GET /schedules`, ranges this node leads).",
    path: "/schedules",
    pointer: "/schedules",
    each: None,
    params: &[],
    columns: &[
        v("name", "name", Str, "schedule name"),
        v("id", "id", Str, "schedule id"),
        v("service", "/target/service", Str, "target service"),
        v("handler", "/target/handler", Str, "target handler"),
        v("overlap", "overlap", Str, "`skip` / `allow`"),
        v("paused", "paused", Bool, "paused"),
        v("next_at", "nextAt", Int, "next fire, ms"),
        v("fired", "fired", Int, "fired"),
        v("missed", "missed", Int, "missed"),
        v("skipped", "skipped", Int, "skipped (overlap)"),
        v("created_at", "createdAt", Int, "time created, ms"),
        v("updated_at", "updatedAt", Int, "time modified, ms"),
    ],
};

const DEPLOYMENT: ViewSpec = ViewSpec {
    name: "sys_deployment",
    doc: "Registered deployments (`GET /deployments?pinned=true`).",
    path: "/deployments?pinned=true",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("id", "id", Str, "deployment id"),
        v("tenant", "tenant", Str, "tenant"),
        v("created_at_ms", "createdAtMs", Int, "time registered, ms"),
        v("endpoint", "endpoint", Str, "SDK URI"),
        v("wire_version", "wireVersion", Int, "wire version"),
        v(
            "protocol_mode",
            "protocolMode",
            Str,
            "`BIDI_STREAM` / `REQUEST_RESPONSE`",
        ),
        v("services", "services", Json, "its services (JSON)"),
        v(
            "current_for",
            "currentFor",
            Json,
            "services for which it is the current revision (JSON)",
        ),
        v(
            "pinned",
            "pinnedInvocations",
            Int,
            "invocations still pinned (counted cluster-wide; NULL when a range has not answered) — 0 ⇒ removable",
        ),
    ],
};

const SERVICE: ViewSpec = ViewSpec {
    name: "sys_service",
    doc: "Current services (`GET /services`).",
    path: "/services",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("name", "name", Str, "service name"),
        v(
            "ty",
            "ty",
            Str,
            "kind (`service`, `virtual_object`, `workflow`)",
        ),
        v("deployment", "deployment", Str, "current deployment URI"),
        v(
            "deployment_id",
            "deploymentId",
            Str,
            "id of the current deployment",
        ),
    ],
};

const HANDLER: ViewSpec = ViewSpec {
    name: "sys_handler",
    doc: "Handlers of every current service (`GET /services`, `handlers` flattened).",
    path: "/services",
    pointer: "",
    each: Some("handlers"),
    params: &[],
    columns: &[
        v("service", "^name", Str, "service"),
        v("name", "name", Str, "handler name"),
        v(
            "ty",
            "ty",
            Str,
            "handler kind (`exclusive`, `shared`, `workflow`)",
        ),
        v(
            "deployment_id",
            "^deploymentId",
            Str,
            "id of the current deployment",
        ),
    ],
};

const VQUEUE: ViewSpec = ViewSpec {
    name: "sys_vqueue",
    doc: "Virtual queues of the ranges this node leads (`GET /vqueues`).",
    path: "/vqueues",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("range", "partition", Int, "range"),
        v("queue", "queue", Str, "queue name"),
        v("scope", "scope", Str, "scope"),
        v("limit_key", "limitKey", Str, "limit key"),
        v("service", "service", Str, "service"),
        v("object_key", "objectKey", Str, "object key"),
        v("exclusive", "exclusive", Bool, "exclusive (virtual object)"),
        v("waiting", "waiting", Int, "waiting"),
        v("granted", "granted", Int, "slots granted (cumulative)"),
        v("blocked_times", "blockedTimes", Int, "times blocked"),
        v("blocked_on", "blockedOn", Str, "currently blocked by"),
        v("lock_holder", "lockHolder", Str, "id holding the lock"),
    ],
};

const SCHEDULER: ViewSpec = ViewSpec {
    name: "sys_scheduler",
    doc: "Turn scheduler of each range this node leads (`GET /scheduler`).",
    path: "/scheduler",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("range", "partition", Int, "range"),
        v("eligible", "eligible", Int, "queues eligible to run"),
        v("queues", "queues", Int, "queues"),
        v("waiting", "waiting", Int, "waiting invocations"),
        v("turns_in_use", "turnsInUse", Int, "turn slots in use"),
        v("max_turns", "maxTurns", Int, "turn slot cap"),
        v("locks_held", "locksHeld", Int, "locks held"),
        v("rules_version", "rulesVersion", Int, "rule book version"),
        v("granted", "granted", Int, "granted (cumulative)"),
        v("blocked", "blocked", Json, "blocks by reason (JSON)"),
        v("tenants", "tenants", Json, "rate per tenant (JSON)"),
    ],
};

const LIMIT: ViewSpec = ViewSpec {
    name: "sys_limit",
    doc: "Concurrency-cap counters of the ranges this node leads (`GET /limits`).",
    path: "/limits",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("range", "partition", Int, "range"),
        v("counter", "counter", Str, "counter"),
        v("level", "level", Int, "level"),
        v("used", "used", Int, "in use"),
        v("limit", "limit", Int, "cap"),
        v("available", "available", Int, "available"),
        v("rule", "rule", Str, "rule setting the cap"),
    ],
};

const RANGE: ViewSpec = ViewSpec {
    name: "sys_range",
    doc: "The cluster's range table and the leader of each range (`GET /cluster`).",
    path: "/cluster",
    pointer: "/ranges",
    each: None,
    params: &[],
    columns: &[
        v("range", "range", Int, "range id"),
        v("start", "start", Str, "start key (hex)"),
        v("end", "end", Str, "end key (hex)"),
        v("gen", "gen", Int, "generation"),
        v("intent", "intent", Json, "unfinished split/merge (JSON)"),
        v("leader", "leader", Str, "leader node"),
        v("address", "address", Str, "internal port of the leader"),
        v("term", "term", Int, "term"),
    ],
};

const KEY: ViewSpec = ViewSpec {
    name: "sys_key",
    doc: "API keys (without secrets) (`GET /admin/keys`, cluster-admin).",
    path: "/admin/keys",
    pointer: "",
    each: None,
    params: &[],
    columns: &[
        v("id", "id", Str, "key id"),
        v("tenant", "tenant", Str, "tenant"),
        v("role", "role", Str, "role"),
        v("created_at_ms", "createdAtMs", Int, "time created, ms"),
    ],
};

pub fn view(name: &str) -> Option<&'static ViewSpec> {
    VIEWS.iter().copied().find(|s| s.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_rows_take_parent_columns() {
        let doc = serde_json::json!([
            {"name": "Cart", "deploymentId": "d1", "handlers": [{"name": "add", "ty": "exclusive"}, {"name": "get"}]},
            {"name": "Greeter", "deploymentId": "d2", "handlers": []}
        ]);
        let rows = HANDLER.rows(&doc);
        assert_eq!(
            rows,
            vec![
                vec![
                    Cell::Str("Cart".into()),
                    Cell::Str("add".into()),
                    Cell::Str("exclusive".into()),
                    Cell::Str("d1".into())
                ],
                vec![
                    Cell::Str("Cart".into()),
                    Cell::Str("get".into()),
                    Cell::Null,
                    Cell::Str("d1".into())
                ],
            ]
        );
        let one = RANGE
            .rows(&serde_json::json!({"ranges": [{"range": 3, "gen": 1, "intent": {"split": 1}}]}));
        assert_eq!(one[0][0], Cell::Int(3));
        assert_eq!(one[0][4], Cell::Str("{\"split\":1}".into()));
    }

    /// The child of `/query` can only call the path of a view table.
    #[test]
    fn only_view_paths_are_accepted() {
        for spec in VIEWS {
            if spec.params.is_empty() {
                assert!(is_view_path(spec.path), "{}", spec.path);
            }
        }
        assert!(is_view_path("/archive?date=2026-11-12"));
        for bad in [
            "/archive?date=",
            "/archive?date=2026/../../admin/keys",
            "/archive?date=x&scope=cluster",
            "/archive?date=a%2F",
            "/admin/keys?x=1",
            "/admin/failpoints",
            "/internal/query/scan",
            "/services/../admin/keys",
            "/stats/history?days=3000",
            "",
        ] {
            assert!(!is_view_path(bad), "{bad}");
        }
    }
}
