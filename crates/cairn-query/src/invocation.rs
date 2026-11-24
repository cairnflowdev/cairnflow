// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`sys_invocation`**: the console's main table, over the declared index
//! framework.
//!
//! Path chooser (at planning time, from the pushed-down filters and the RAM counters):
//!
//! | Filter / order | Path |
//! |---|---|
//! | `id =`/`IN` | point read of metadata + status, owner range |
//! | attribute `=` (`parent_id`, `schedule`, `deployment`, `restarted_from`, `root_id`) | `ByAttr` |
//! | `ORDER BY transitioned_at`, a `transitioned_at` range, or a rare `class` (≤ 5 % by the counters) | `ByTransition[class…]` |
//! | `service =`/`IN` | `ByService` |
//! | the rest | `ByCreated` (`root = …` ⇒ only that stream) |
//!
//! Each range merges its own streams; all ranges are merged in the declared
//! order (`[created_at DESC, id ASC]` or `[transitioned_at DESC, id ASC]`;
//! reversed for `ASC`). A path on the same axis as the order stops at `fetch`
//! rows per stream. A range where the needed descriptor is not `readable` yet
//! ⇒ scan the metadata table (capped) + warning `indexing`. A range that cannot
//! be read ⇒ warning `partial`.

use {
    crate::{
        ctx::{QueryCtx, TenantScope},
        filter::{self, Pred},
        remote::{Scan, WirePath},
    },
    cairn_engine::InvocationStatus,
    cairn_store::{
        id_from_hex, id_hex,
        query_index::{
            self as qi, attr, Class, Fixed, IndexRow, InvocationMeta, Placement, QueryIndex,
        },
        range_read::{RangeReadHandle, RangeSnapshot},
        InvocationId,
    },
    datafusion::{
        arrow::compute::SortOptions,
        arrow::{
            array::{
                ArrayRef, BooleanBuilder, Int64Builder, RecordBatch, RecordBatchOptions,
                StringBuilder, TimestampMillisecondBuilder, UInt32Builder,
            },
            datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        },
        catalog::{Session, TableProvider},
        common::{tree_node::TreeNodeRecursion, DataFusionError, Result},
        datasource::TableType,
        execution::{SendableRecordBatchStream, TaskContext},
        logical_expr::{Expr, TableProviderFilterPushDown},
        physical_expr::{
            expressions::Column, EquivalenceProperties, PhysicalExpr, PhysicalSortExpr,
        },
        physical_plan::{
            execution_plan::{Boundedness, EmissionType},
            sort_pushdown::SortOrderPushdownResult,
            stream::RecordBatchStreamAdapter,
            DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        },
    },
    std::{fmt, sync::Arc},
};

/// "Rare class" threshold: the filtered class makes up ≤ 5 % of the rows in the
/// tenant (/service) span by the counters ⇒ take the `ByTransition[class]` stream.
pub const RARE_RATIO: f64 = 0.05;

/// Read chunk per scan of one stream.
const BATCH: usize = 256;

/// Cap on the metadata rows the fallback path returns per range.
const FALLBACK_MAX: usize = 20_000;

/// Cap on the hot-region keys (every table) the fallback path reads per range.
const FALLBACK_SCAN_MAX: usize = FALLBACK_MAX * 32;

// ─────────────────────────────────────────────────────── schema

pub mod col {
    pub const ID: usize = 0;
    pub const TENANT: usize = 1;
    pub const SERVICE: usize = 2;
    pub const HANDLER: usize = 3;
    pub const KEY: usize = 4;
    pub const KIND: usize = 5;
    pub const CLASS: usize = 6;
    pub const STATUS: usize = 7;
    pub const ROOT: usize = 8;
    pub const CREATED_AT: usize = 9;
    pub const TRANSITIONED_AT: usize = 10;
    pub const ENDED_AT: usize = 11;
    pub const DURATION_MS: usize = 12;
    pub const SCOPE: usize = 13;
    pub const PARENT_ID: usize = 14;
    pub const SCHEDULE: usize = 15;
    pub const IDEMPOTENCY_KEY: usize = 16;
    pub const DEPLOYMENT: usize = 17;
    pub const RESTARTED_FROM: usize = 18;
    pub const TAGS: usize = 19;
    pub const RANGE: usize = 20;
    // Detail columns (features F4, F5, F8, F10, F12): each group costs one point
    // read per **selected** row, only when projected.
    pub const FAILURE_CODE: usize = 21;
    pub const FAILURE_MESSAGE: usize = 22;
    pub const ATTEMPT: usize = 23;
    pub const RETRY_AT: usize = 24;
    pub const LAST_FAILURE_CODE: usize = 25;
    pub const LAST_FAILURE_MESSAGE: usize = 26;
    pub const LAST_FAILURE_AT: usize = 27;
    pub const PURGE_AT: usize = 28;
    pub const HASH_CHAIN_LEN: usize = 29;
    pub const PINNED_DEPLOYMENT: usize = 30;
    /// Root of the call tree — metadata.
    pub const ROOT_ID: usize = 31;
    /// Number of journal entries (console question 32) — one reverse scan for the last entry.
    pub const JOURNAL_LEN: usize = 32;
    /// Limit key at creation — metadata.
    pub const LIMIT_KEY: usize = 33;
    /// The full status row.
    pub const STATUS_DETAIL: [usize; 2] = [FAILURE_CODE, FAILURE_MESSAGE];
    /// The `Retries` row.
    pub const RETRY: [usize; 2] = [ATTEMPT, RETRY_AT];
    /// The `LastFailure` row.
    pub const LAST_FAILURE: [usize; 3] = [LAST_FAILURE_CODE, LAST_FAILURE_MESSAGE, LAST_FAILURE_AT];
    /// Columns that need the metadata row (one hot point read per row).
    pub const META: [usize; 9] = [
        SCOPE,
        PARENT_ID,
        SCHEDULE,
        IDEMPOTENCY_KEY,
        DEPLOYMENT,
        RESTARTED_FROM,
        TAGS,
        ROOT_ID,
        LIMIT_KEY,
    ];
}

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

/// Full schema of `sys_invocation`. **It is API**: adding columns is fine,
/// changing/removing one breaks it.
pub fn schema() -> SchemaRef {
    let s = |n: &str| Field::new(n, DataType::Utf8, false);
    let o = |n: &str| Field::new(n, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        s("id"),
        s("tenant"),
        s("service"),
        s("handler"),
        s("key"),
        s("kind"),
        s("class"),
        s("status"),
        Field::new("root", DataType::Boolean, false),
        Field::new("created_at", ts(), false),
        Field::new("transitioned_at", ts(), false),
        Field::new("ended_at", ts(), true),
        Field::new("duration_ms", DataType::Int64, true),
        o("scope"),
        o("parent_id"),
        o("schedule"),
        o("idempotency_key"),
        o("deployment"),
        o("restarted_from"),
        o("tags"),
        Field::new("range", DataType::UInt32, false),
        Field::new("failure_code", DataType::Int64, true),
        o("failure_message"),
        Field::new("attempt", DataType::Int64, true),
        Field::new("retry_at", ts(), true),
        Field::new("last_failure_code", DataType::Int64, true),
        o("last_failure_message"),
        Field::new("last_failure_at", ts(), true),
        Field::new("purge_at", ts(), true),
        Field::new("hash_chain_len", DataType::Int64, true),
        o("pinned_deployment"),
        o("root_id"),
        Field::new("journal_len", DataType::Int64, true),
        o("limit_key"),
    ]))
}

// ─────────────────────────────────────────────────────── plan

/// Axis and direction of the output order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Order {
    /// `[created_at DESC, id ASC]` — the default.
    CreatedDesc,
    /// `[created_at ASC, id DESC]` — reverse scan.
    CreatedAsc,
    TransitionedDesc,
    TransitionedAsc,
}

impl Order {
    fn transition(self) -> bool {
        matches!(self, Self::TransitionedDesc | Self::TransitionedAsc)
    }
    fn descending(self) -> bool {
        matches!(self, Self::CreatedDesc | Self::TransitionedDesc)
    }
    fn name(self) -> &'static str {
        match self {
            Self::CreatedDesc => "created_at DESC, id ASC",
            Self::CreatedAsc => "created_at ASC, id DESC",
            Self::TransitionedDesc => "transitioned_at DESC, id ASC",
            Self::TransitionedAsc => "transitioned_at ASC, id DESC",
        }
    }
}

/// The chosen scan path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Path {
    Point,
    /// Scan the metadata table — the remote node lacks the right descriptor version.
    Meta,
    Index(&'static QueryIndex),
}

impl Path {
    fn name(&self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Meta => "meta",
            Self::Index(ix) => ix.name,
        }
    }
}

#[derive(Clone, Debug)]
struct Plan {
    pred: Pred,
    order: Order,
    path: Path,
    fetch: Option<usize>,
    /// Indexes of the columns (of the full schema) to output.
    projection: Vec<usize>,
}

/// One group of the local-range counters the path chooser uses
/// ([`class_share`]), sent to the child process of `/query`: the child has no
/// ranges, so without these numbers it would choose a different path than the
/// coordinating node.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClassCount {
    pub tenant: String,
    pub service: String,
    pub class: String,
    pub n: u64,
}

/// Counters of the local ranges, grouped by `(tenant, service, class)`, only
/// tenants the asker can see. `None` when some range has not built its counters yet.
pub fn class_counts(ctx: &QueryCtx) -> Option<Vec<ClassCount>> {
    let mut sum: std::collections::BTreeMap<(String, String, &'static str), u64> =
        std::collections::BTreeMap::new();
    for h in &ctx.ranges {
        for (k, n) in h.counts().snapshot()? {
            if ctx.sees(&k.tenant) {
                *sum.entry((k.tenant, k.service, k.class.name()))
                    .or_default() += n;
            }
        }
    }
    Some(
        sum.into_iter()
            .map(|((tenant, service, class), n)| ClassCount {
                tenant,
                service,
                class: class.to_string(),
                n,
            })
            .collect(),
    )
}

/// Counters summed over every readable range: `(rows of the filtered class,
/// total rows)` in the tenant (/service) span. `None` when some range has not
/// built its counters yet. In the child process: the numbers the coordinating
/// node sent ([`QueryCtx::class_counts`]).
fn class_share(ctx: &QueryCtx, pred: &Pred) -> Option<(u64, u64)> {
    let classes = pred.classes.as_ref()?;
    let (mut hit, mut all) = (0u64, 0u64);
    if let Some(hint) = &ctx.class_counts {
        for c in hint.as_ref()? {
            let Some(class) = Class::from_name(&c.class) else {
                continue;
            };
            let tenant_ok =
                ctx.sees(&c.tenant) && pred.tenants.as_ref().is_none_or(|t| t.contains(&c.tenant));
            let service_ok = pred
                .services
                .as_ref()
                .is_none_or(|s| s.contains(&c.service));
            if tenant_ok && service_ok {
                all += c.n;
                if classes.contains(&class) {
                    hit += c.n;
                }
            }
        }
        return Some((hit, all));
    }
    for h in &ctx.ranges {
        for (k, n) in h.counts().snapshot()? {
            let tenant_ok =
                ctx.sees(&k.tenant) && pred.tenants.as_ref().is_none_or(|t| t.contains(&k.tenant));
            let service_ok = pred
                .services
                .as_ref()
                .is_none_or(|s| s.contains(&k.service));
            if tenant_ok && service_ok {
                all += n;
                if classes.contains(&k.class) {
                    hit += n;
                }
            }
        }
    }
    Some((hit, all))
}

/// The path chooser (table at the top of the module).
fn choose(ctx: &QueryCtx, pred: &Pred, order: Order) -> Path {
    if pred.ids.is_some() {
        return Path::Point;
    }
    if pred.parent_ids.is_some()
        || pred.schedules.is_some()
        || pred.deployments.is_some()
        || pred.restarted_from.is_some()
        || pred.root_ids.is_some()
    {
        return Path::Index(&qi::BY_ATTR);
    }
    let rare = class_share(ctx, pred)
        .is_some_and(|(hit, all)| all == 0 || (hit as f64) <= RARE_RATIO * all as f64);
    if order.transition() || !pred.transitioned.is_open() || rare {
        return Path::Index(&qi::BY_TRANSITION);
    }
    if pred.services.is_some() {
        return Path::Index(&qi::BY_SERVICE);
    }
    Path::Index(&qi::BY_CREATED)
}

// ─────────────────────────────────────────────────────── TableProvider

/// `sys_invocation`.
#[derive(Clone)]
pub struct InvocationTable {
    ctx: QueryCtx,
}

impl fmt::Debug for InvocationTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sys_invocation")
    }
}

impl InvocationTable {
    pub fn new(ctx: QueryCtx) -> Self {
        Self { ctx }
    }
}

#[async_trait::async_trait]
impl TableProvider for InvocationTable {
    fn schema(&self) -> SchemaRef {
        schema()
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
            .map(|f| {
                if filter::supported(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pred = Pred::from_filters(filters);
        let order = Order::CreatedDesc;
        let path = choose(&self.ctx, &pred, order);
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..schema().fields().len()).collect());
        Ok(Arc::new(InvocationScanExec::new(
            self.ctx.clone(),
            Plan {
                pred,
                order,
                path,
                fetch: limit,
                projection,
            },
        )))
    }
}

// ─────────────────────────────────────────────────────── ExecutionPlan

/// Leaf scanning `sys_invocation` — one partition for all local ranges.
pub struct InvocationScanExec {
    ctx: QueryCtx,
    plan: Plan,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for InvocationScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CairnScanExec")
            .field("path", &self.plan.path.name())
            .field("order", &self.plan.order)
            .field("fetch", &self.plan.fetch)
            .finish()
    }
}

impl InvocationScanExec {
    fn new(ctx: QueryCtx, plan: Plan) -> Self {
        let full = schema();
        let schema = Arc::new(full.project(&plan.projection).expect("valid projection"));
        let props = Arc::new(properties(&schema, plan.order));
        Self {
            ctx,
            plan,
            schema,
            props,
        }
    }

    fn with_plan(&self, plan: Plan) -> Self {
        Self::new(self.ctx.clone(), plan)
    }

    /// The chosen path — the query-plan shape tests read it.
    pub fn path(&self) -> &Path {
        &self.plan.path
    }
}

/// Declared order of the leaf: only when both the time column and `id` are in the projection.
fn properties(schema: &SchemaRef, order: Order) -> PlanProperties {
    let time = if order.transition() {
        "transitioned_at"
    } else {
        "created_at"
    };
    let desc = order.descending();
    let mut eq = EquivalenceProperties::new(schema.clone());
    if let (Ok(t), Ok(i)) = (schema.index_of(time), schema.index_of("id")) {
        let sort = |name: &str, idx: usize, descending: bool| PhysicalSortExpr {
            expr: Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>,
            options: SortOptions {
                descending,
                nulls_first: descending,
            },
        };
        eq = EquivalenceProperties::new_with_orderings(
            schema.clone(),
            [vec![sort(time, t, desc), sort("id", i, !desc)]],
        );
    }
    PlanProperties::new(
        eq,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    )
}

impl DisplayAs for InvocationScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CairnScanExec: table=sys_invocation, path={}, order=[{}]",
            self.plan.path.name(),
            self.plan.order.name()
        )?;
        if let Some(n) = self.plan.fetch {
            write!(f, ", fetch={n}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for InvocationScanExec {
    fn name(&self) -> &str {
        "CairnScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    // Does not declare `supports_limit_pushdown`: that flag means "the limit
    // goes **through** this node down to its child" — for a leaf the limit
    // would vanish. Left at the default, `LimitPushdown` calls `with_fetch` on the leaf itself.

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut plan = self.plan.clone();
        plan.fetch = limit;
        Some(Arc::new(self.with_plan(plan)))
    }

    fn fetch(&self) -> Option<usize> {
        self.plan.fetch
    }

    /// A non-default order (`transitioned_at`, or `ASC`) can be pushed into the
    /// leaf: change the scan axis / direction, no `SortExec` needed.
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let name = |e: &PhysicalSortExpr| {
            e.expr
                .as_ref()
                .downcast_ref::<Column>()
                .map(|c| c.name().to_string())
        };
        let Some(first) = order.first() else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        let desc = first.options.descending;
        let wanted = match name(first).as_deref() {
            Some("created_at") if desc => Order::CreatedDesc,
            Some("created_at") => Order::CreatedAsc,
            Some("transitioned_at") if desc => Order::TransitionedDesc,
            Some("transitioned_at") => Order::TransitionedAsc,
            _ => return Ok(SortOrderPushdownResult::Unsupported),
        };
        // The rest (if any) must be `id` in the opposite direction — the declared order within a timestamp.
        if let Some(second) = order.get(1) {
            if name(second).as_deref() != Some("id") || second.options.descending == desc {
                return Ok(SortOrderPushdownResult::Unsupported);
            }
        }
        if order.len() > 2 {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        let mut plan = self.plan.clone();
        plan.order = wanted;
        if plan.path != Path::Point {
            plan.path = choose(&self.ctx, &plan.pred, wanted);
        }
        Ok(SortOrderPushdownResult::Exact {
            inner: Arc::new(self.with_plan(plan)),
        })
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CairnScanExec has only partition 0, asked for {partition}"
            )));
        }
        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        let schema = self.schema.clone();
        let out_schema = schema.clone();
        let fut = async move {
            let rows = run(&ctx, &plan).await?;
            to_batch(&out_schema, &plan.projection, &rows)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut),
        )))
    }
}

// ─────────────────────────────────────────────────────── execution

/// One row of `sys_invocation`.
#[derive(Clone, Debug, Default)]
struct Row {
    id: InvocationId,
    tenant: String,
    service: String,
    handler: String,
    key: String,
    kind: u8,
    class: Option<Class>,
    /// Name of the detailed status (`running`/`suspended`) if it was read.
    status: Option<String>,
    root: bool,
    created_ms: u64,
    t: u64,
    meta: Option<InvocationMeta>,
    range: u32,
    extra: Extra,
}

/// Detail columns read (`complete`), each field one row of the range.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Extra {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<(u32, String)>,
    /// `(due, attempts)` of the `Retries` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry: Option<(u64, u32)>,
    /// `(code, message, time)` of the `LastFailure` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_failure: Option<(u32, String, u64)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    purge_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chain_len: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pinned: Option<String>,
    /// Number of journal entries. Absent on old nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_len: Option<u32>,
}

impl Row {
    fn ended_ms(&self) -> Option<u64> {
        self.class.filter(|c| c.is_ended()).map(|_| self.t)
    }

    fn duration_ms(&self) -> Option<i64> {
        self.ended_ms().map(|e| e as i64 - self.created_ms as i64)
    }

    fn time(&self, order: Order) -> u64 {
        if order.transition() {
            self.t
        } else {
            self.created_ms
        }
    }

    /// Every `Exact` condition (filter.rs) is re-applied here — DataFusion has
    /// removed their `FilterExec`.
    fn admits(&self, p: &Pred) -> bool {
        let has = |set: &Option<Vec<String>>, v: &str| {
            set.as_ref().is_none_or(|s| s.iter().any(|x| x == v))
        };
        let meta_has = |set: &Option<Vec<String>>, v: Option<String>| {
            set.as_ref()
                .is_none_or(|s| v.is_some_and(|v| s.contains(&v)))
        };
        let m = self.meta.as_ref();
        has(&p.ids, &id_hex(&self.id))
            && has(&p.tenants, &self.tenant)
            && has(&p.services, &self.service)
            && has(&p.handlers, &self.handler)
            && has(&p.keys, &self.key)
            && has(&p.kinds, kind_name(self.kind))
            && p.classes
                .as_ref()
                .is_none_or(|c| self.class.is_some_and(|x| c.contains(&x)))
            && p.root.as_ref().is_none_or(|r| r.contains(&self.root))
            && p.service_prefix
                .as_ref()
                .is_none_or(|s| self.service.starts_with(s.as_str()))
            && p.handler_prefix
                .as_ref()
                .is_none_or(|s| self.handler.starts_with(s.as_str()))
            && p.created.contains(self.created_ms)
            && p.transitioned.contains(self.t)
            && p.min_duration_ms
                .is_none_or(|d| self.duration_ms().is_some_and(|x| x >= d))
            && meta_has(&p.parent_ids, m.and_then(|m| m.parent).map(|x| id_hex(&x)))
            && meta_has(&p.schedules, m.and_then(|m| m.schedule.clone()))
            && meta_has(&p.deployments, m.and_then(|m| m.deployment.clone()))
            && meta_has(
                &p.restarted_from,
                m.and_then(|m| m.restarted_from).map(|x| id_hex(&x)),
            )
            && meta_has(&p.root_ids, m.and_then(|m| m.root_id).map(|x| id_hex(&x)))
    }
}

/// One row on the wire (remote scan).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireRow {
    id: String,
    tenant: String,
    service: String,
    handler: String,
    key: String,
    kind: u8,
    class: Option<String>,
    status: Option<String>,
    root: bool,
    created_ms: u64,
    t: u64,
    /// `InvocationMeta::encode`, hex.
    meta: Option<String>,
    range: u32,
    /// Absent on old nodes (lenient read).
    #[serde(default)]
    extra: Extra,
}

impl WireRow {
    fn of(r: &Row) -> Self {
        Self {
            id: id_hex(&r.id),
            tenant: r.tenant.clone(),
            service: r.service.clone(),
            handler: r.handler.clone(),
            key: r.key.clone(),
            kind: r.kind,
            class: r.class.map(|c| c.name().to_string()),
            status: r.status.clone(),
            root: r.root,
            created_ms: r.created_ms,
            t: r.t,
            meta: r.meta.as_ref().map(|m| hex(&m.encode())),
            range: r.range,
            extra: r.extra.clone(),
        }
    }

    fn into_row(self) -> Option<Row> {
        let meta = match self.meta {
            Some(h) => Some(InvocationMeta::decode(&unhex(&h)?)?),
            None => None,
        };
        let class = match self.class {
            Some(c) => Some(Class::from_name(&c)?),
            None => None,
        };
        Some(Row {
            id: id_from_hex(&self.id)?,
            tenant: self.tenant,
            service: self.service,
            handler: self.handler,
            key: self.key,
            kind: self.kind,
            class,
            status: self.status,
            root: self.root,
            created_ms: self.created_ms,
            t: self.t,
            meta,
            range: self.range,
            extra: self.extra,
        })
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn kind_name(code: u8) -> &'static str {
    cairn_engine::InvocationKind::from_code(code).name()
}

fn df_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

/// Tenants the query scans: the asker's tenant; for `cluster-admin` the
/// filtered tenant, or every tenant the range counters know (`None` = unknown — fallback).
fn tenants_of(ctx: &QueryCtx, pred: &Pred, h: &RangeReadHandle) -> Option<Vec<String>> {
    match (&ctx.tenant, &pred.tenants) {
        (TenantScope::One(t), Some(ts)) => Some(ts.iter().filter(|x| *x == t).cloned().collect()),
        (TenantScope::One(t), None) => Some(vec![t.clone()]),
        (TenantScope::All, Some(ts)) => Some(ts.clone()),
        (TenantScope::All, None) => {
            let mut ts: Vec<String> = h
                .counts()
                .snapshot()?
                .into_iter()
                .map(|(k, _)| k.tenant)
                .collect();
            ts.dedup();
            Some(ts)
        }
    }
}

/// Rows of the query: local ranges, then ranges on other nodes
/// ([`crate::remote`]); merged in the declared order, cut at `fetch`.
async fn run(ctx: &QueryCtx, plan: &Plan) -> Result<Vec<Row>> {
    let mut rows = local_rows(ctx, plan).await?;
    if ctx.remotes.is_empty() || plan.pred.impossible {
        return Ok(rows);
    }
    let path = match &plan.path {
        Path::Point => WirePath::Point,
        Path::Meta => WirePath::Meta,
        Path::Index(ix) => WirePath::Index {
            name: ix.name.to_string(),
            version: ix.version,
        },
    };
    let remote = crate::remote::fan_out(ctx, || Scan::Invocation {
        pred: Box::new(plan.pred.clone()),
        order: plan.order,
        path: path.clone(),
        fetch: plan.fetch,
        projection: plan.projection.clone(),
    })
    .await
    .map_err(df_err)?;
    for w in remote.into_iter().flat_map(|r| r.rows) {
        match w.into_row() {
            Some(r) if ctx.sees(&r.tenant) => rows.push(r),
            Some(_) => {}
            None => return Err(df_err("a remote node sent a broken row")),
        }
    }
    sort_rows(&mut rows, plan.order);
    if let Some(n) = plan.fetch {
        rows.truncate(n);
    }
    Ok(rows)
}

/// Remote-node side of [`crate::remote::serve`]: run the leaf on local ranges
/// with the plan the coordinating node sent. The path's descriptor is missing
/// (or has another version) here ⇒ scan metadata + `indexing`.
pub(crate) async fn serve_rows(
    ctx: &QueryCtx,
    pred: Pred,
    order: Order,
    path: WirePath,
    fetch: Option<usize>,
    projection: Vec<usize>,
) -> Result<Vec<WireRow>, crate::QueryError> {
    let full = schema().fields().len();
    if projection.iter().any(|c| *c >= full) {
        return Err(crate::QueryError::Internal("projection off schema".into()));
    }
    let path = match path {
        WirePath::Point => Path::Point,
        WirePath::Meta => Path::Meta,
        WirePath::Index { name, version } => {
            let mine = ctx
                .ranges
                .first()
                .map(|h| h.indexes())
                .unwrap_or(qi::QUERY_INDEXES);
            match mine.iter().find(|ix| ix.name == name) {
                Some(ix) if ix.version == version => Path::Index(ix),
                _ => {
                    for h in &ctx.ranges {
                        ctx.warn(Some(h.range().id.0), "indexing");
                    }
                    Path::Meta
                }
            }
        }
    };
    let plan = Plan {
        pred,
        order,
        path,
        fetch,
        projection,
    };
    let rows = local_rows(ctx, &plan)
        .await
        .map_err(crate::QueryError::from)?;
    Ok(rows.iter().map(WireRow::of).collect())
}

/// Leaf over the local ranges of `ctx`.
async fn local_rows(ctx: &QueryCtx, plan: &Plan) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    if plan.pred.impossible {
        return Ok(rows);
    }
    for h in &ctx.ranges {
        let range = h.range().id.0;
        let snap = match h.snapshot() {
            Ok(s) => s,
            Err(e) => {
                ctx.warn_partial(Some(range), &e);
                continue;
            }
        };
        let got = match &plan.path {
            Path::Point => point_rows(ctx, plan, &snap).await,
            Path::Meta => fallback_rows(ctx, plan, &snap).await,
            Path::Index(ix) => index_rows(ctx, plan, ix, &snap).await,
        };
        match got {
            Ok(r) => rows.extend(r),
            Err(e @ crate::QueryError::Budget(_)) => return Err(df_err(e)),
            Err(e) => ctx.warn_partial(Some(range), &e),
        }
    }
    sort_rows(&mut rows, plan.order);
    if let Some(n) = plan.fetch {
        rows.truncate(n);
    }
    complete(ctx, plan, &mut rows).await?;
    Ok(rows)
}

fn sort_rows(rows: &mut [Row], order: Order) {
    rows.sort_by(|a, b| {
        let (ta, tb) = (a.time(order), b.time(order));
        if order.descending() {
            tb.cmp(&ta).then(a.id.cmp(&b.id))
        } else {
            ta.cmp(&tb).then(b.id.cmp(&a.id))
        }
    });
}

/// Point read: metadata + status of each id belonging to this range.
async fn point_rows(
    ctx: &QueryCtx,
    plan: &Plan,
    snap: &RangeSnapshot,
) -> Result<Vec<Row>, crate::QueryError> {
    let mut out = Vec::new();
    for hex in plan.pred.ids.iter().flatten() {
        let Some(id) = id_from_hex(hex) else { continue };
        if !snap
            .handle()
            .range()
            .contains_key(cairn_store::partition_key_of(&id))
        {
            continue;
        }
        if let Some(r) = row_from_hot(ctx, snap, &id).await? {
            if ctx.sees(&r.tenant) && r.admits(&plan.pred) {
                out.push(r);
            }
        }
    }
    Ok(out)
}

/// A row built from metadata + status (point path, fallback, `ByAttr`).
async fn row_from_hot(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    id: &InvocationId,
) -> Result<Option<Row>, crate::QueryError> {
    ctx.charge(0, 2)?;
    let meta = snap
        .get(&qi::meta_key(id))
        .await?
        .and_then(|raw| InvocationMeta::decode(&raw));
    let status = snap.get(&cairn_store::keys::invocation_status(id)).await?;
    let (Some(meta), Some(raw)) = (meta, status) else {
        return Ok(None);
    };
    let at = InvocationStatus::decode_at(&raw);
    let Some(status) = InvocationStatus::decode(raw) else {
        return Ok(None);
    };
    let p = Placement::new(status.class(), at.unwrap_or(0), &meta);
    // The status row was read: the error code/message come with it, no further read.
    let failure = match &status {
        InvocationStatus::Failed { code, message } | InvocationStatus::Paused { code, message } => {
            Some((*code, message.clone()))
        }
        _ => None,
    };
    Ok(Some(Row {
        id: *id,
        tenant: meta.tenant.clone(),
        service: meta.service.clone(),
        handler: meta.handler.clone(),
        key: meta.key.clone(),
        kind: meta.kind,
        class: Some(p.class),
        status: Some(status.name().to_string()),
        root: meta.root,
        created_ms: meta.created_ms,
        t: p.t,
        range: snap.handle().range().id.0,
        meta: Some(meta),
        extra: Extra {
            failure,
            ..Extra::default()
        },
    }))
}

/// Every stream (prefix + time bounds) of descriptor `ix` for this plan.
fn streams(ix: &QueryIndex, plan: &Plan, tenants: &[String]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let p = &plan.pred;
    let roots: Vec<bool> = p.root.clone().unwrap_or_else(|| vec![true, false]);
    let classes: Vec<Class> = p.classes.clone().unwrap_or_else(|| Class::ALL.to_vec());
    let mut attrs: Vec<(&str, String)> = Vec::new();
    for (name, vals) in [
        (attr::PARENT, &p.parent_ids),
        (attr::SCHEDULE, &p.schedules),
        (attr::DEPLOYMENT, &p.deployments),
        (attr::RESTARTED_FROM, &p.restarted_from),
        (attr::ROOT, &p.root_ids),
    ] {
        if let Some(vs) = vals {
            attrs.extend(vs.iter().map(|v| (name, v.clone())));
            break;
        }
    }
    let span = if ix.axis() == Some(qi::Axis::Transition) {
        p.transitioned
    } else {
        p.created
    };
    let mut prefixes: Vec<Vec<u8>> = Vec::new();
    for t in tenants {
        if ix.code == qi::BY_CREATED.code {
            for r in &roots {
                prefixes.extend(qi::scan_prefix(ix, &[Fixed::Tenant(t), Fixed::Root(*r)]));
            }
        } else if ix.code == qi::BY_SERVICE.code {
            for s in p.services.iter().flatten() {
                prefixes.extend(qi::scan_prefix(ix, &[Fixed::Tenant(t), Fixed::Service(s)]));
            }
        } else if ix.code == qi::BY_TRANSITION.code {
            for c in &classes {
                for r in &roots {
                    prefixes.extend(qi::scan_prefix(
                        ix,
                        &[Fixed::Tenant(t), Fixed::Class(*c), Fixed::Root(*r)],
                    ));
                }
            }
        } else if ix.code == qi::BY_ATTR.code {
            for (k, v) in &attrs {
                prefixes.extend(qi::scan_prefix(ix, &[Fixed::Tenant(t), Fixed::Attr(k, v)]));
            }
        }
    }
    prefixes
        .into_iter()
        .map(|pre| qi::time_bounds(&pre, span.lo, span.hi))
        .collect()
}

/// Index path over one range.
async fn index_rows(
    ctx: &QueryCtx,
    plan: &Plan,
    ix: &'static QueryIndex,
    snap: &RangeSnapshot,
) -> Result<Vec<Row>, crate::QueryError> {
    let range = snap.handle().range().id.0;
    let readable = snap.readable_indexes().await?;
    let tenants = tenants_of(ctx, &plan.pred, snap.handle());
    let (true, Some(tenants)) = (
        readable
            .iter()
            .any(|r| r.code == ix.code && r.version == ix.version),
        tenants,
    ) else {
        ctx.warn(Some(range), "indexing");
        return fallback_rows(ctx, plan, snap).await;
    };
    // A stream on the same axis as the order ⇒ stop at `fetch` matching rows per stream.
    let same_axis = (ix.axis() == Some(qi::Axis::Transition)) == plan.order.transition()
        && ix.code != qi::BY_ATTR.code;
    let stop = plan.fetch.filter(|_| same_axis);
    let descending_scan = !plan.order.descending();
    let mut out = Vec::new();
    for (lo, hi) in streams(ix, plan, &tenants) {
        let mut found = 0usize;
        let (mut lo, mut hi) = (lo, hi);
        loop {
            ctx.stats
                .seeks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // A stream that can stop at `fetch`: the first chunk is exactly that
            // big (the newest page reads `n` rows per stream), later chunks are full size.
            let want = match stop {
                Some(n) if found == 0 => n.clamp(1, BATCH),
                _ => BATCH,
            };
            let batch = snap
                .scan(lo.clone(), hi.clone(), want, descending_scan, true)
                .await?;
            ctx.charge(batch.len() as u64, 0)?;
            for (k, v) in &batch {
                let Some(ir) = qi::decode_row(ix, k, v) else {
                    continue;
                };
                let Some(row) = row_of(ctx, snap, ix, ir).await? else {
                    continue;
                };
                if ctx.sees(&row.tenant) && row.admits(&plan.pred) {
                    out.push(row);
                    found += 1;
                }
            }
            if batch.len() < want || stop.is_some_and(|n| found >= n) {
                break;
            }
            let last = batch.last().map(|(k, _)| k.to_vec()).unwrap_or_default();
            if descending_scan {
                hi = last;
            } else {
                lo = last;
                lo.push(0);
            }
        }
    }
    Ok(out)
}

/// A row from one index row; when the descriptor does not cover every column
/// (`ByAttr`), read status + metadata as well.
async fn row_of(
    ctx: &QueryCtx,
    snap: &RangeSnapshot,
    ix: &QueryIndex,
    r: IndexRow,
) -> Result<Option<Row>, crate::QueryError> {
    if ix.code == qi::BY_ATTR.code {
        return row_from_hot(ctx, snap, &r.id).await;
    }
    Ok(Some(Row {
        id: r.id,
        tenant: r.tenant.unwrap_or_default(),
        service: r.service.unwrap_or_default(),
        handler: r.handler.unwrap_or_default(),
        key: r.key.unwrap_or_default(),
        kind: r.kind.unwrap_or_default(),
        class: r.class,
        status: None,
        root: r.root.unwrap_or(true),
        created_ms: r.created_ms.unwrap_or_default(),
        t: r.t.unwrap_or_default(),
        meta: None,
        range: snap.handle().range().id.0,
        extra: Extra::default(),
    }))
}

/// Fallback when the descriptor is not `readable` on the range yet: scan the
/// metadata table (hot region, capped), then point-read the status.
///
/// The hot region interleaves every table by partition key, so the scan walks
/// it in chunks of [`BATCH`] keys and charges each chunk to the read budget
/// before reading the next: a query over budget stops after one chunk, and a
/// range's hot region is never held in memory as a whole.
async fn fallback_rows(
    ctx: &QueryCtx,
    plan: &Plan,
    snap: &RangeSnapshot,
) -> Result<Vec<Row>, crate::QueryError> {
    let range = snap.handle().range().id.0;
    let (mut lo, hi) = (vec![0x00], vec![0x01]);
    let mut scanned = 0usize;
    let mut out = Vec::new();
    loop {
        let want = BATCH.min(FALLBACK_SCAN_MAX - scanned);
        let batch = snap
            .scan(lo.clone(), hi.clone(), want, false, false)
            .await?;
        scanned += batch.len();
        ctx.stats
            .fallback_rows
            .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        ctx.charge(batch.len() as u64, 0)?;
        for (k, _) in &batch {
            let Some(id) = qi::parse_meta_key(k) else {
                continue;
            };
            if out.len() >= FALLBACK_MAX {
                ctx.warn(Some(range), "partial: the fallback scan hit its row cap");
                return Ok(out);
            }
            if let Some(r) = row_from_hot(ctx, snap, &id).await? {
                if ctx.sees(&r.tenant) && r.admits(&plan.pred) {
                    out.push(r);
                }
            }
        }
        if batch.len() < want {
            return Ok(out);
        }
        if scanned >= FALLBACK_SCAN_MAX {
            ctx.warn(Some(range), "partial: the fallback scan hit its row cap");
            return Ok(out);
        }
        lo = batch.last().map(|(k, _)| k.to_vec()).unwrap_or_default();
        lo.push(0);
    }
}

/// Extra reads for the selected rows (after the `fetch` cut): metadata when
/// its columns are projected, detailed status when `status` is projected and
/// the class is `active`. A list page that projects none of them reads nothing.
async fn complete(ctx: &QueryCtx, plan: &Plan, rows: &mut [Row]) -> Result<()> {
    let want_meta = plan.projection.iter().any(|c| col::META.contains(c));
    let want_status = plan.projection.contains(&col::STATUS);
    let wants = |g: &[usize]| plan.projection.iter().any(|c| g.contains(c));
    let want_detail = wants(&col::STATUS_DETAIL);
    let want_retry = wants(&col::RETRY);
    let want_last = wants(&col::LAST_FAILURE);
    let want_purge = wants(&[col::PURGE_AT]);
    let want_chain = wants(&[col::HASH_CHAIN_LEN]);
    let want_pin = wants(&[col::PINNED_DEPLOYMENT]);
    let want_jlen = wants(&[col::JOURNAL_LEN]);
    let want_extra =
        want_detail || want_retry || want_last || want_purge || want_chain || want_pin || want_jlen;
    if !want_meta && !want_status && !want_extra {
        return Ok(());
    }
    for r in rows.iter_mut() {
        let Some(h) = ctx.ranges.iter().find(|h| h.range().id.0 == r.range) else {
            continue;
        };
        let snap = h.snapshot().map_err(df_err)?;
        if want_meta && r.meta.is_none() {
            ctx.charge(0, 1).map_err(df_err)?;
            r.meta = snap
                .get(&qi::meta_key(&r.id))
                .await
                .map_err(df_err)?
                .and_then(|raw| InvocationMeta::decode(&raw));
        }
        // One point read per projected group.
        let get = |key: Vec<u8>| {
            let snap = &snap;
            async move {
                ctx.charge(0, 1).map_err(df_err)?;
                snap.get(&key).await.map_err(df_err)
            }
        };
        // Only class `failed`/`paused` carries an error code.
        let may_fail = !matches!(r.class, Some(Class::Active | Class::Completed));
        if want_detail && may_fail && r.extra.failure.is_none() {
            r.extra.failure = get(cairn_store::keys::invocation_status(&r.id))
                .await?
                .and_then(InvocationStatus::decode)
                .and_then(|s| match s {
                    InvocationStatus::Failed { code, message }
                    | InvocationStatus::Paused { code, message } => Some((code, message)),
                    _ => None,
                });
        }
        if want_retry && r.extra.retry.is_none() {
            r.extra.retry = get(cairn_store::keys::retry(&r.id))
                .await?
                .and_then(|v| cairn_lifecycle::RetryRow::decode_value(&v).ok())
                .map(|row| (row.due, row.attempt));
        }
        if want_last && r.extra.last_failure.is_none() {
            r.extra.last_failure = get(cairn_store::keys::last_failure(&r.id))
                .await?
                .and_then(|v| cairn_store::keys::LastFailure::decode(&v))
                .map(|f| (f.code, f.message, f.at_ms));
        }
        if want_purge && r.extra.purge_at.is_none() {
            r.extra.purge_at = get(cairn_store::keys::purge(&r.id))
                .await?
                .and_then(|v| cairn_store::keys::decode_due(&v).map(|(due, _)| due));
        }
        if want_chain && r.extra.chain_len.is_none() {
            r.extra.chain_len = get(cairn_store::keys::chain(&r.id))
                .await?
                .and_then(|v| cairn_store::keys::decode_chain(&v))
                .map(|(len, _)| len);
        }
        // The journal is append-only until purge (which deletes the whole journal
        // at once), so the index of the last entry + 1 is the length: one reverse
        // scan takes one key, without reading the whole journal (console question 32).
        if want_jlen && r.extra.journal_len.is_none() {
            ctx.charge(0, 1).map_err(df_err)?;
            let prefix = cairn_store::keys::journal_prefix(&r.id);
            let last = snap
                .scan(prefix.clone(), qi::prefix_end(&prefix), 1, true, true)
                .await
                .map_err(df_err)?;
            r.extra.journal_len = Some(match last.first() {
                Some((k, _)) if k.len() >= 4 => {
                    u32::from_be_bytes(k[k.len() - 4..].try_into().expect("4 byte")) + 1
                }
                _ => 0,
            });
        }
        if want_pin && r.extra.pinned.is_none() {
            r.extra.pinned = get(cairn_store::keys::pin(&r.id))
                .await?
                .map(|v| String::from_utf8_lossy(&v).into_owned());
        }
        if want_status && r.status.is_none() {
            r.status = match r.class {
                Some(Class::Active) => {
                    ctx.charge(0, 1).map_err(df_err)?;
                    snap.get(&cairn_store::keys::invocation_status(&r.id))
                        .await
                        .map_err(df_err)?
                        .and_then(InvocationStatus::decode)
                        .map(|s| s.name().to_string())
                }
                Some(c) => Some(c.name().to_string()),
                None => None,
            };
        }
    }
    Ok(())
}

fn to_batch(schema: &SchemaRef, projection: &[usize], rows: &[Row]) -> Result<RecordBatch> {
    let n = rows.len();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &c in projection {
        let strs = |f: &dyn Fn(&Row) -> Option<String>| -> ArrayRef {
            let mut b = StringBuilder::new();
            for r in rows {
                b.append_option(f(r));
            }
            Arc::new(b.finish())
        };
        let times = |f: &dyn Fn(&Row) -> Option<u64>| -> ArrayRef {
            let mut b = TimestampMillisecondBuilder::new().with_timezone("UTC");
            for r in rows {
                b.append_option(f(r).map(|t| t as i64));
            }
            Arc::new(b.finish())
        };
        let ints = |f: &dyn Fn(&Row) -> Option<i64>| -> ArrayRef {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(f(r));
            }
            Arc::new(b.finish())
        };
        let meta = |r: &Row| r.meta.clone();
        cols.push(match c {
            col::ID => strs(&|r| Some(id_hex(&r.id))),
            col::TENANT => strs(&|r| Some(r.tenant.clone())),
            col::SERVICE => strs(&|r| Some(r.service.clone())),
            col::HANDLER => strs(&|r| Some(r.handler.clone())),
            col::KEY => strs(&|r| Some(r.key.clone())),
            col::KIND => strs(&|r| Some(kind_name(r.kind).to_string())),
            col::CLASS => strs(&|r| Some(r.class.map_or("", Class::name).to_string())),
            col::STATUS => strs(&|r| {
                Some(
                    r.status
                        .clone()
                        .unwrap_or_else(|| r.class.map_or("", Class::name).to_string()),
                )
            }),
            col::ROOT => {
                let mut b = BooleanBuilder::new();
                for r in rows {
                    b.append_value(r.root);
                }
                Arc::new(b.finish())
            }
            col::CREATED_AT => times(&|r| Some(r.created_ms)),
            col::TRANSITIONED_AT => times(&|r| Some(r.t)),
            col::ENDED_AT => times(&|r| r.ended_ms()),
            col::DURATION_MS => {
                let mut b = Int64Builder::new();
                for r in rows {
                    b.append_option(r.duration_ms());
                }
                Arc::new(b.finish())
            }
            col::SCOPE => strs(&|r| meta(r).map(|m| m.scope).filter(|s| !s.is_empty())),
            col::PARENT_ID => strs(&|r| meta(r).and_then(|m| m.parent).map(|p| id_hex(&p))),
            col::SCHEDULE => strs(&|r| meta(r).and_then(|m| m.schedule)),
            col::IDEMPOTENCY_KEY => strs(&|r| meta(r).and_then(|m| m.idempotency_key)),
            col::DEPLOYMENT => strs(&|r| meta(r).and_then(|m| m.deployment)),
            col::RESTARTED_FROM => {
                strs(&|r| meta(r).and_then(|m| m.restarted_from).map(|p| id_hex(&p)))
            }
            col::TAGS => strs(&|r| {
                meta(r).filter(|m| !m.tags.is_empty()).map(|m| {
                    serde_json::Value::Object(
                        m.tags
                            .into_iter()
                            .map(|(k, v)| (k, serde_json::Value::String(v)))
                            .collect(),
                    )
                    .to_string()
                })
            }),
            col::RANGE => {
                let mut b = UInt32Builder::new();
                for r in rows {
                    b.append_value(r.range);
                }
                Arc::new(b.finish())
            }
            col::FAILURE_CODE => ints(&|r| r.extra.failure.as_ref().map(|f| i64::from(f.0))),
            col::FAILURE_MESSAGE => strs(&|r| r.extra.failure.as_ref().map(|f| f.1.clone())),
            col::ATTEMPT => ints(&|r| r.extra.retry.map(|x| i64::from(x.1))),
            col::RETRY_AT => times(&|r| r.extra.retry.map(|x| x.0)),
            col::LAST_FAILURE_CODE => {
                ints(&|r| r.extra.last_failure.as_ref().map(|f| i64::from(f.0)))
            }
            col::LAST_FAILURE_MESSAGE => {
                strs(&|r| r.extra.last_failure.as_ref().map(|f| f.1.clone()))
            }
            col::LAST_FAILURE_AT => times(&|r| r.extra.last_failure.as_ref().map(|f| f.2)),
            col::PURGE_AT => times(&|r| r.extra.purge_at),
            col::HASH_CHAIN_LEN => ints(&|r| r.extra.chain_len.map(i64::from)),
            col::PINNED_DEPLOYMENT => strs(&|r| r.extra.pinned.clone()),
            col::ROOT_ID => strs(&|r| meta(r).and_then(|m| m.root_id).map(|p| id_hex(&p))),
            col::JOURNAL_LEN => ints(&|r| r.extra.journal_len.map(i64::from)),
            col::LIMIT_KEY => strs(&|r| meta(r).and_then(|m| m.limit_key)),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "column {other} outside the schema"
                )))
            }
        });
    }
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )?)
}
