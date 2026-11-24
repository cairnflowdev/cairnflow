// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`POST /query`**: a read-only SQL statement over the ranges.
//!
//! - Roles: `admin` sees exactly the key's tenant; `cluster-admin` every tenant.
//! - Body `{sql, allow_partial?, timeout_ms?}` (`timeout_ms` ≤ 30 s).
//! - Returns `{columns, rows, partial, warnings, read, ms}`. The statement runs
//!   to completion (every batch in hand) **before** the status is sent: a
//!   mid-way error is a `4xx`/`5xx` with a body, never a truncated `200`.
//! - Admission: at most [`MAX_CONCURRENT_PER_TENANT`] concurrent statements per
//!   tenant (the next one waits up to [`TENANT_SLOT_WAIT`]), at most
//!   [`MAX_CONCURRENT`] concurrent statements per node, beyond that `429`;
//!   a token bucket per tenant in **read units plus run time**
//!   ([`TenantBudget`], [`charge`]), when empty
//!   `429`.
//! - A range not open on this node: ask the node leading it per the routing
//!   table through `POST /internal/query/scan` ([`HttpRemote`]); no leader, no
//!   address, no cluster secret, or a node that does not answer ⇒
//!   `partial` + the range name.

use {
    super::QueryRanges,
    crate::auth::Identity,
    axum::{extract::State, http::StatusCode, response::IntoResponse, Json},
    cairn_query::{Limits, QueryCtx, QueryError, TenantScope},
    cairn_store::ClusterConfig,
    serde::Deserialize,
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::Instant,
};

/// Maximum concurrent statements per node.
pub const MAX_CONCURRENT: usize = 4;

/// Maximum concurrent statements of **one** tenant on the node: one `view` key
/// cannot hold all [`MAX_CONCURRENT`] slots of the node.
pub const MAX_CONCURRENT_PER_TENANT: usize = 2;

/// A tenant's third statement waits this long at most for a tenant slot (the
/// console fires a few statements in parallel for one screen) before `429`.
pub const TENANT_SLOT_WAIT: Duration = Duration::from_secs(10);

/// Per-tenant query slots. One semaphore per tenant that has a statement;
/// removed once no one holds or waits on it.
pub struct TenantSlots {
    per_tenant: usize,
    inner: Mutex<BTreeMap<String, Arc<tokio::sync::Semaphore>>>,
}

/// The slot of one statement; returned on drop.
pub struct TenantSlot {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl TenantSlots {
    pub fn new(per_tenant: usize) -> Self {
        Self {
            per_tenant: per_tenant.max(1),
            inner: Mutex::default(),
        }
    }

    /// Asks for a slot for `tenant`, waiting at most `wait`. `None` = timed out.
    pub async fn take(&self, tenant: &str, wait: Duration) -> Option<TenantSlot> {
        let sem = {
            let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            // Remove semaphores nobody holds or waits on (only the map holds
            // them): the map does not grow with the number of tenants that ever asked.
            let per = self.per_tenant;
            m.retain(|_, s| Arc::strong_count(s) > 1 || s.available_permits() < per);
            Arc::clone(
                m.entry(tenant.to_string())
                    .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(per))),
            )
        };
        let permit = tokio::time::timeout(wait, sem.acquire_owned())
            .await
            .ok()?
            .ok()?;
        Some(TenantSlot {
            _permit: Some(permit),
        })
    }
}

/// Units charged to the tenant's bucket after a statement: read units **plus**
/// run time — one second of running costs exactly one second of the bucket's
/// refill, so a CPU-heavy statement that reads little cannot run back to back.
pub fn charge(units: u64, ms: u64, budget: &TenantBudget) -> u64 {
    units.saturating_add(ms.saturating_mul(budget.per_second) / 1_000)
}

/// Cap on the `timeout_ms` a caller may ask for.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-tenant token bucket in read units: one tenant cannot take all of the
/// node's reads with a series of statements each under the per-statement cap.
/// A statement is admitted only while the bucket is positive; the actual units
/// are charged after it runs (and may become debt — the next statement waits
/// for the bucket to refill).
#[derive(Clone, Copy, Debug)]
pub struct TenantBudget {
    /// Maximum read units that can accumulate.
    pub capacity: u64,
    /// Read units refilled per second.
    pub per_second: u64,
}

impl Default for TenantBudget {
    fn default() -> Self {
        Self {
            capacity: 5 * cairn_query::ctx::DEFAULT_READ_BUDGET,
            per_second: cairn_query::ctx::DEFAULT_READ_BUDGET,
        }
    }
}

/// Bucket name of `cluster-admin` (matches no tenant: `$` breaks the name rules).
const CLUSTER_BUCKET: &str = "$cluster";

struct Buckets {
    budget: TenantBudget,
    /// `tenant → (units left, time computed)`. The mutex is held only for the arithmetic.
    inner: Mutex<BTreeMap<String, (f64, Instant)>>,
}

impl Buckets {
    fn level(&self, tenant: &str, spend: u64) -> f64 {
        let now = Instant::now();
        let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cap = self.budget.capacity as f64;
        let (left, at) = m.entry(tenant.to_string()).or_insert((cap, now));
        let refill = now.duration_since(*at).as_secs_f64() * self.budget.per_second as f64;
        *left = (*left + refill).min(cap) - spend as f64;
        *at = now;
        *left
    }
}

/// Read units of **one store GET** on REST read paths that go straight to an
/// object (`GET /archive/{id}`, `GET /audit` — and `sys_audit`, which calls
/// `/audit` back), charged to the tenant's bucket like a `/query` statement.
/// One store GET costs orders of magnitude more than an index row: with the
/// default bucket (2·10⁵ units/s, cap 10⁶) that is ~200 GET/s per tenant, with
/// bursts of ~1 000.
pub const STORE_GET_UNITS: u64 = 1_000;

/// The node's query service.
#[derive(Clone)]
pub struct QueryService {
    runtime: Arc<QueryRuntime>,
    ranges: QueryRanges,
    cluster: ClusterConfig,
    limits: Limits,
    slots: Arc<tokio::sync::Semaphore>,
    /// Per-tenant slots: taken **before** the node's slot.
    tenant_slots: Arc<TenantSlots>,
    buckets: Arc<Buckets>,
    /// The node's ingress router — source of the view tables.
    router: super::views::RouterSlot,
    /// Child processes for the caller's statements. `None` = run in place on the
    /// query runtime.
    workers: Option<Arc<super::isolate::Pool>>,
}

/// Who wrote the statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Author {
    /// The caller (`POST /query`): runs in a child process when enabled.
    User,
    /// The server builds a fixed-shape statement (`GET /invocations`, tenant
    /// usage counts): runs in place.
    Server,
}

impl QueryService {
    pub fn new(
        ranges: QueryRanges,
        cluster: ClusterConfig,
        limits: Limits,
        budget: TenantBudget,
    ) -> Self {
        Self {
            runtime: Arc::new(QueryRuntime(None)),
            ranges,
            cluster,
            limits,
            slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT)),
            tenant_slots: Arc::new(TenantSlots::new(MAX_CONCURRENT_PER_TENANT)),
            buckets: Arc::new(Buckets {
                budget,
                inner: Mutex::default(),
            }),
            router: Default::default(),
            workers: None,
        }
    }

    /// The caller's statements run in child processes `cfg`. Needs a dedicated
    /// query runtime (call after [`Self::with_threads`] with a thread count
    /// > 0); without one they keep running in place, with a warning.
    pub fn with_workers(mut self, cfg: super::isolate::QueryWorkerConfig) -> Self {
        match self.runtime.handle() {
            Some(h) => {
                self.workers = Some(super::isolate::Pool::new(cfg, MAX_CONCURRENT, h));
            }
            // `node::start` refuses this configuration before getting here.
            None => tracing::error!(
                "query worker processes need the query runtime; /query is not isolated"
            ),
        }
        self
    }

    /// [`Self::with_workers`] when a configuration is given.
    pub fn pipe_workers(self, cfg: Option<super::isolate::QueryWorkerConfig>) -> Self {
        match cfg {
            Some(c) => self.with_workers(c),
            None => self,
        }
    }

    /// The child-process configuration in use (`None` = in place).
    pub fn workers(&self) -> Option<&super::isolate::QueryWorkerConfig> {
        self.workers.as_ref().map(|p| p.config())
    }

    /// A dedicated runtime of `threads` threads for queries; `0` = the node's
    /// runtime.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.runtime = Arc::new(QueryRuntime::new(threads));
        self
    }

    pub fn ranges(&self) -> &QueryRanges {
        &self.ranges
    }

    /// Charges `gets` store GETs ([`STORE_GET_UNITS`] each) to `tenant`'s read
    /// bucket. `false` = the bucket was empty **before** the charge — the caller
    /// returns `429` and reads nothing; the charge may become debt, like a
    /// `/query` statement that exceeds its estimate.
    pub(crate) fn spend_store_reads(&self, tenant: &str, gets: u64) -> bool {
        self.spend_read_units(tenant, gets.saturating_mul(STORE_GET_UNITS))
    }

    /// Charges `units` read units to `tenant`'s bucket (each chunk of
    /// `GET /schedules`). `false` = the bucket was empty **before** the charge —
    /// nothing is charged, the caller stops reading.
    pub(crate) fn spend_read_units(&self, tenant: &str, units: u64) -> bool {
        if self.buckets.level(tenant, 0) <= 0.0 {
            metrics::THROTTLED.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.buckets.level(tenant, units);
        true
    }

    /// Sets the ingress router (after construction) — view tables run `GET` on it.
    pub fn set_router(&self, r: std::sync::Weak<axum::Router>) {
        self.router.set(r);
    }
}

/// `cairnflow_query_*` metrics (no tenant label).
pub mod metrics {
    use super::*;
    pub static REQUESTS: AtomicU64 = AtomicU64::new(0);
    pub static CLIENT_ERRORS: AtomicU64 = AtomicU64::new(0);
    pub static SERVER_ERRORS: AtomicU64 = AtomicU64::new(0);
    pub static REJECTED: AtomicU64 = AtomicU64::new(0);
    pub static THROTTLED: AtomicU64 = AtomicU64::new(0);
    pub static PARTIAL: AtomicU64 = AtomicU64::new(0);
    pub static ROWS: AtomicU64 = AtomicU64::new(0);
    pub static READ_UNITS: AtomicU64 = AtomicU64::new(0);
    pub static MILLIS: AtomicU64 = AtomicU64::new(0);

    /// Prometheus lines of the counters above.
    pub fn render(out: &mut String) {
        use std::fmt::Write;
        for (name, help, v) in [
            (
                "cairnflow_query_requests_total",
                "/query requests received",
                &REQUESTS,
            ),
            (
                "cairnflow_query_client_errors_total",
                "/query requests that failed on the caller's side (4xx)",
                &CLIENT_ERRORS,
            ),
            (
                "cairnflow_query_server_errors_total",
                "/query requests that failed on the server's side (5xx)",
                &SERVER_ERRORS,
            ),
            (
                "cairnflow_query_rejected_total",
                "/query requests refused for too many concurrent queries",
                &REJECTED,
            ),
            (
                "cairnflow_query_throttled_total",
                "/query requests refused because the tenant ran out of read units",
                &THROTTLED,
            ),
            (
                "cairnflow_query_partial_total",
                "/query answers missing a range or an index still being built",
                &PARTIAL,
            ),
            ("cairnflow_query_rows_total", "/query rows returned", &ROWS),
            (
                "cairnflow_query_read_units_total",
                "/query read units (index rows + point reads)",
                &READ_UNITS,
            ),
            (
                "cairnflow_query_duration_ms_total",
                "Total /query run time, ms",
                &MILLIS,
            ),
            (
                "cairnflow_query_worker_stopped_total",
                "/query worker processes stopped before answering (over the deadline, over the memory limit, crashed or broke the protocol)",
                &crate::query::isolate::STOPPED,
            ),
        ] {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}",
                v.load(Ordering::Relaxed)
            );
        }
    }
}

/// The node's routing table and transport, for remote scans.
pub struct Peers {
    pub leaders: BTreeMap<u32, crate::routing::Leader>,
    pub me: Option<String>,
    pub client: reqwest::Client,
    pub secret: Option<crate::auth::ClusterSecret>,
}

/// Another node, asked through its internal port (HMAC-signed; the tenant is
/// in the signed body). One ask is capped at [`MAX_TIMEOUT`]; the statement's
/// deadline wraps it.
struct HttpRemote {
    node: String,
    addr: String,
    ranges: Vec<u32>,
    client: reqwest::Client,
    secret: crate::auth::ClusterSecret,
}

#[async_trait::async_trait]
impl cairn_query::remote::RemoteScan for HttpRemote {
    fn node(&self) -> &str {
        &self.node
    }

    fn ranges(&self) -> &[u32] {
        &self.ranges
    }

    async fn scan(
        &self,
        req: cairn_query::remote::ScanRequest,
    ) -> Result<cairn_query::remote::ScanResponse, QueryError> {
        let url = format!("{}/internal/query/scan", self.addr.trim_end_matches('/'));
        let rb = self.client.post(url).timeout(MAX_TIMEOUT).json(&req);
        let resp = self
            .secret
            .send(&self.client, rb)
            .await
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let code = resp.status();
        if code == StatusCode::UNPROCESSABLE_ENTITY {
            let v: serde_json::Value = resp.json().await.unwrap_or_default();
            return Err(QueryError::Budget(
                v["error"].as_str().unwrap_or("query_budget").to_string(),
            ));
        }
        if !code.is_success() {
            return Err(QueryError::Internal(format!("node returned {code}")));
        }
        resp.json()
            .await
            .map_err(|e| QueryError::Internal(e.to_string()))
    }
}

/// `POST /internal/query/scan` — the remote node's side: runs the leaf on the
/// ranges it has open. Accepts only requests signed with the cluster secret
/// (internal port).
pub async fn internal_scan(
    State(st): State<crate::ingress::AppState>,
    Json(req): Json<cairn_query::remote::ScanRequest>,
) -> axum::response::Response {
    let Some(svc) = st.query().cloned() else {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "query layer is not enabled on this node",
        );
    };
    let job = cairn_query::remote::serve(svc.ranges.handles(), req);
    let served = svc
        .runtime
        .run(job)
        .await
        .unwrap_or_else(|e| Err(QueryError::Internal(format!("scan stopped: {e}"))));
    match served {
        Ok(resp) => Json(resp).into_response(),
        Err(e @ QueryError::Budget(_)) => err(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    /// Values for `$1…$n`: string, integer, bool, `null`.
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    #[serde(default = "yes")]
    pub allow_partial: bool,
    pub timeout_ms: Option<u64>,
}

/// JSON ⇒ [`cairn_query::Param`]. Floats, arrays, objects ⇒ `Err` (400): four
/// types are enough for the console's filters, and equality on floats is a trap.
fn params_of(values: &[serde_json::Value]) -> Result<Vec<cairn_query::Param>, String> {
    use {cairn_query::Param, serde_json::Value as V};
    values
        .iter()
        .enumerate()
        .map(|(i, v)| match v {
            V::String(s) => Ok(Param::Str(s.clone())),
            V::Bool(b) => Ok(Param::Bool(*b)),
            V::Null => Ok(Param::Null),
            V::Number(n) => n
                .as_i64()
                .map(Param::Int)
                .ok_or_else(|| format!("params[{i}] = {n}: only i64 integers are accepted")),
            _ => Err(format!(
                "params[{i}]: only strings, integers, bool or null are accepted"
            )),
        })
        .collect()
}

fn yes() -> bool {
    true
}

/// JSON error body. A `500` carries no detail (bucket, host, object path) —
/// only a code to look up in the log ([`crate::ingress::opaque_internal`]).
pub(crate) fn err(code: StatusCode, msg: impl Into<String>) -> axum::response::Response {
    let msg = msg.into();
    let msg = if code == StatusCode::INTERNAL_SERVER_ERROR {
        crate::ingress::opaque_internal(&msg)
    } else {
        msg
    };
    (code, Json(serde_json::json!({ "error": msg }))).into_response()
}

/// A finished statement: `columns`/`rows` are JSON bytes (from a child process
/// they are passed through as is, no tree is built on the node).
pub(crate) struct Answer {
    pub columns: bytes::Bytes,
    pub rows: bytes::Bytes,
    pub row_count: u64,
    /// Every warning, including ranges not open / not reachable recorded before the run.
    pub warnings: Vec<cairn_query::Warning>,
    /// Read units (counted by the node).
    pub read: u64,
    pub ms: u64,
}

impl Answer {
    /// Rows as a JSON tree — for callers inside the server (small, fixed-shape statements).
    pub fn rows(&self) -> Result<Vec<serde_json::Value>, String> {
        serde_json::from_slice(&self.rows).map_err(|e| e.to_string())
    }
}

/// The ranges of a statement per the current routing table.
struct Topology {
    /// Ranges open on this node.
    handles: Vec<cairn_store::range_read::RangeReadHandle>,
    /// Other nodes leading the rest.
    remotes: Vec<HttpRemote>,
    /// Ranges nobody can read, with the reason (`partial`).
    lost: Vec<(u32, &'static str)>,
}

impl QueryService {
    /// The ranges of a statement: ranges open on this node (exactly as in the
    /// current table), nodes leading the rest, ranges nobody can read.
    fn topology(&self, st: &crate::ingress::AppState) -> Topology {
        // Only ranges exactly as in the current table (id and bounds): the
        // parent range of a split/merge that is still registered (not yet
        // sealed) must not be read together with the child ranges — rows
        // would be duplicated.
        let table = self.cluster.partitions();
        let handles: Vec<_> = self
            .ranges
            .handles()
            .into_iter()
            .filter(|h| {
                let r = h.range();
                self.cluster
                    .range(r.id)
                    .is_some_and(|e| e.start == r.start && e.end == r.end)
            })
            .collect();
        let peers = st.query_peers();
        let mut far: BTreeMap<String, (String, Vec<u32>)> = BTreeMap::new();
        let mut lost = Vec::new();
        for p in table {
            if handles.iter().any(|h| h.range().id == p) {
                continue;
            }
            match peers.leaders.get(&p.0) {
                Some(l) if Some(&l.node_id) != peers.me.as_ref() && peers.secret.is_some() => {
                    match &l.address {
                        Some(addr) => far
                            .entry(l.node_id.clone())
                            .or_insert_with(|| (addr.clone(), Vec::new()))
                            .1
                            .push(p.0),
                        None => {
                            lost.push((p.0, "partial: the leader did not advertise an address"))
                        }
                    }
                }
                _ => lost.push((p.0, "partial: the range has no readable leader")),
            }
        }
        let remotes = match &peers.secret {
            Some(secret) => far
                .into_iter()
                .map(|(node, (addr, ranges))| HttpRemote {
                    node,
                    addr,
                    ranges,
                    client: peers.client.clone(),
                    secret: secret.clone(),
                })
                .collect(),
            None => Vec::new(),
        };
        Topology {
            handles,
            remotes,
            lost,
        }
    }

    /// Admits and runs a statement: concurrency slots, the tenant's bucket,
    /// caps, metrics. `Err` is the HTTP status and message.
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is one aspect of the statement"
    )]
    pub(crate) async fn execute(
        &self,
        st: &crate::ingress::AppState,
        author: Author,
        tenant: TenantScope,
        credentials: super::views::Credentials,
        sql: &str,
        params: &[cairn_query::Param],
        timeout_ms: Option<u64>,
    ) -> Result<Answer, (StatusCode, String)> {
        // Statement shape before any slot: a long statement or a long operator
        // chain holds no slot and never reaches DataFusion. Only the token part
        // here — one pass, no recursion; the AST part runs with the statement
        // (on the query runtime or in the child process).
        if let Err(e) = cairn_query::check_sql_text(sql) {
            metrics::CLIENT_ERRORS.fetch_add(1, Ordering::Relaxed);
            return Err((StatusCode::BAD_REQUEST, e.to_string()));
        }
        let bucket = match &tenant {
            TenantScope::All => CLUSTER_BUCKET.to_string(),
            TenantScope::One(t) => t.clone(),
        };
        // The tenant's slot first: a tenant's third statement waits — holding
        // none of the node's slots while it waits.
        let Some(_tenant_slot) = self.tenant_slots.take(&bucket, TENANT_SLOT_WAIT).await else {
            metrics::REJECTED.fetch_add(1, Ordering::Relaxed);
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "{MAX_CONCURRENT_PER_TENANT} queries of this tenant are already running on this node"
                ),
            ));
        };
        let Ok(_slot) = self.slots.clone().try_acquire_owned() else {
            metrics::REJECTED.fetch_add(1, Ordering::Relaxed);
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                format!("{MAX_CONCURRENT} queries are already running on this node"),
            ));
        };
        if self.buckets.level(&bucket, 0) <= 0.0 {
            metrics::THROTTLED.fetch_add(1, Ordering::Relaxed);
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "query_budget: the tenant has used up its read units, retry later".into(),
            ));
        }
        let topo = self.topology(st);
        let mut limits = self.limits;
        if let Some(ms) = timeout_ms {
            limits.deadline = Duration::from_millis(ms).min(MAX_TIMEOUT);
        }
        let views: Arc<dyn cairn_query::view::ViewSource> = Arc::new(super::views::RouterSource {
            slot: self.router.clone(),
            credentials,
        });
        let started = tokio::time::Instant::now();
        let (result, read, warnings) = match (&self.workers, author) {
            (Some(pool), Author::User) => {
                self.isolated(pool, topo, tenant, views, sql, params, limits)
                    .await
            }
            _ => {
                self.in_process(topo, tenant, views, sql, params, limits)
                    .await
            }
        };
        // A failed statement (including one over a cap) has still read:
        // charge the bucket by the actual amount — read units plus run time.
        let ms = started.elapsed().as_millis() as u64;
        self.buckets
            .level(&bucket, charge(read, ms, &self.buckets.budget));
        metrics::MILLIS.fetch_add(ms, Ordering::Relaxed);
        let out = result.map_err(|e| {
            let code = match &e {
                QueryError::Budget(_) => StatusCode::UNPROCESSABLE_ENTITY,
                e if e.is_client() => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            if code.is_client_error() {
                metrics::CLIENT_ERRORS.fetch_add(1, Ordering::Relaxed);
            } else {
                metrics::SERVER_ERRORS.fetch_add(1, Ordering::Relaxed);
            }
            (code, e.to_string())
        })?;
        if !warnings.is_empty() {
            metrics::PARTIAL.fetch_add(1, Ordering::Relaxed);
        }
        metrics::READ_UNITS.fetch_add(read, Ordering::Relaxed);
        Ok(Answer {
            columns: out.0,
            rows: out.1,
            row_count: out.2,
            warnings,
            read,
            ms,
        })
    }

    /// Statement on the node's query runtime. Returns (columns, rows, row
    /// count), read units, every warning (including ranges not open, recorded
    /// before the run).
    async fn in_process(
        &self,
        topo: Topology,
        tenant: TenantScope,
        views: Arc<dyn cairn_query::view::ViewSource>,
        sql: &str,
        params: &[cairn_query::Param],
        limits: Limits,
    ) -> Ran {
        let remotes = topo
            .remotes
            .into_iter()
            .map(|r| Arc::new(r) as Arc<dyn cairn_query::remote::RemoteScan>)
            .collect();
        let ctx = QueryCtx::new(tenant, topo.handles)
            .with_remotes(remotes)
            .with_views(views);
        for (p, why) in topo.lost {
            ctx.warn(Some(p), why);
        }
        let warnings_ctx = ctx.clone();
        let stats = ctx.stats.clone();
        let result = self
            .runtime
            .run({
                let sql = sql.to_string();
                let params = params.to_vec();
                async move { cairn_query::run_with_params(ctx, &sql, &params, limits).await }
            })
            .await
            .unwrap_or_else(|e| Err(QueryError::Internal(format!("query stopped: {e}"))))
            .and_then(|out| {
                let rows =
                    cairn_query::answer::rows_json(&out.batches).map_err(QueryError::Internal)?;
                let columns = cairn_query::answer::columns_json(&out.schema, &out.batches);
                let raw = |r: serde_json::Result<Vec<u8>>| {
                    r.map(bytes::Bytes::from)
                        .map_err(|e| QueryError::Internal(e.to_string()))
                };
                Ok((
                    raw(serde_json::to_vec(&columns))?,
                    raw(serde_json::to_vec(&rows))?,
                    rows.len() as u64,
                ))
            });
        (result, stats.units(), warnings_ctx.warnings())
    }

    /// Statement in a child process: the node serves scans and view tables for
    /// it, and enforces tenant, ranges and read budget itself.
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is one aspect of the statement"
    )]
    async fn isolated(
        &self,
        pool: &Arc<super::isolate::Pool>,
        topo: Topology,
        tenant: TenantScope,
        views: Arc<dyn cairn_query::view::ViewSource>,
        sql: &str,
        params: &[cairn_query::Param],
        limits: Limits,
    ) -> Ran {
        use super::isolate::{Relay, Serve};
        use cairn_query::worker::Target;
        // The child's path chooser uses the local ranges' counts, like an
        // in-place coordinating node (RAM, no store reads).
        let class_counts = cairn_query::invocation::class_counts(&QueryCtx::new(
            tenant.clone(),
            topo.handles.clone(),
        ));
        let mut targets = Vec::new();
        if !topo.handles.is_empty() {
            let ranges = topo.handles.iter().map(|h| h.range().id.0).collect();
            targets.push((
                Target {
                    node: "local".into(),
                    ranges,
                },
                Serve::Local(topo.handles),
            ));
        }
        for r in topo.remotes {
            targets.push((
                Target {
                    node: r.node.clone(),
                    ranges: r.ranges.clone(),
                },
                Serve::Far(Arc::new(r)),
            ));
        }
        let job = Relay {
            tenant: tenant.clone(),
            class_counts,
            targets,
            views: Some(views),
            sql: sql.to_string(),
            params: params.to_vec(),
            limits,
        };
        let pool = Arc::clone(pool);
        let out = self
            .runtime
            .run(async move { super::isolate::relay(&pool, job).await })
            .await;
        // Deduplicate with a set (O(n log n)); the child's warning count is
        // already capped (`isolate::check_answer`).
        let mut warnings: std::collections::BTreeSet<(Option<u32>, String)> = topo
            .lost
            .into_iter()
            .map(|(p, why)| (Some(p), why.to_string()))
            .collect();
        let out = match out {
            Ok(o) => o,
            Err(e) => {
                return (
                    Err(QueryError::Internal(format!("query stopped: {e}"))),
                    0,
                    to_warnings(warnings),
                )
            }
        };
        warnings.extend(out.warnings.into_iter().map(|w| (w.range, w.reason)));
        let result = out.result.map(|a| {
            // Warnings the child declares — the child can only speak about the caller's own statement.
            warnings.extend(a.warnings.into_iter().map(|w| (w.range, w.reason)));
            (a.columns, a.rows, a.row_count)
        });
        (result, out.read, to_warnings(warnings))
    }
}

fn to_warnings(
    set: std::collections::BTreeSet<(Option<u32>, String)>,
) -> Vec<cairn_query::Warning> {
    set.into_iter()
        .map(|(range, reason)| cairn_query::Warning { range, reason })
        .collect()
}

/// Outcome of a statement: (columns, rows, row count) or an error, read units, warnings.
type Ran = (
    Result<(bytes::Bytes, bytes::Bytes, u64), QueryError>,
    u64,
    Vec<cairn_query::Warning>,
);

/// Tenant scope of an identity.
pub(crate) fn scope_of(who: &Identity) -> TenantScope {
    if who.all_tenants() {
        TenantScope::All
    } else {
        TenantScope::One(who.tenant.as_str().to_string())
    }
}

pub(crate) fn warnings_json(ws: &[cairn_query::Warning]) -> Vec<serde_json::Value> {
    ws.iter()
        .map(|w| serde_json::json!({ "range": w.range, "reason": w.reason }))
        .collect()
}

/// `POST /query`.
pub async fn post_query(
    State(st): State<crate::ingress::AppState>,
    axum::Extension(who): axum::Extension<Identity>,
    headers: axum::http::HeaderMap,
    ext: axum::http::Extensions,
    Json(req): Json<QueryRequest>,
) -> axum::response::Response {
    metrics::REQUESTS.fetch_add(1, Ordering::Relaxed);
    let Some(svc) = st.query().cloned() else {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "query layer is not enabled on this node",
        );
    };
    let params = match params_of(&req.params) {
        Ok(p) => p,
        Err(e) => {
            metrics::CLIENT_ERRORS.fetch_add(1, Ordering::Relaxed);
            return err(StatusCode::BAD_REQUEST, e);
        }
    };
    let auth = super::views::Credentials::of_request(&headers, st.auth(), &ext);
    let answer = match svc
        .execute(
            &st,
            Author::User,
            scope_of(&who),
            auth,
            &req.sql,
            &params,
            req.timeout_ms,
        )
        .await
    {
        Ok(a) => a,
        Err((code, msg)) => return err(code, msg),
    };
    let partial = !answer.warnings.is_empty();
    if partial && !req.allow_partial {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "answer is missing ranges ({}) and allow_partial = false",
                answer
                    .warnings
                    .iter()
                    .filter_map(|w| w.range.map(|r| r.to_string()))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        );
    }
    metrics::ROWS.fetch_add(answer.row_count, Ordering::Relaxed);
    // The `200` body: `columns`/`rows` are written as raw bytes — from a child
    // process they are a slice of the frame it sent, not copied, not re-serialised.
    let tail = serde_json::json!({
        "partial": partial,
        "warnings": warnings_json(&answer.warnings),
        "read": answer.read,
        "ms": answer.ms,
    })
    .to_string();
    let chunks = [
        bytes::Bytes::from_static(b"{\"columns\":"),
        answer.columns,
        bytes::Bytes::from_static(b",\"rows\":"),
        answer.rows,
        bytes::Bytes::from_static(b","),
        // The `{…}` of the tail, without the opening brace.
        bytes::Bytes::from(tail).slice(1..),
    ];
    let stream = futures::stream::iter(chunks.map(Ok::<_, std::convert::Infallible>));
    (
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        )],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

/// Default thread count of the query runtime.
pub const QUERY_THREADS: usize = 2;

/// Stack per query-runtime thread: DataFusion planning recurses with
/// expression depth, and a stack overflow aborts the whole node. The main
/// guard is `cairn_query::check_sql`; a large stack is the second safety margin
/// (tokio's default is 2 MiB — measured: dies at 2 000 operators). Only virtual
/// address space for [`QUERY_THREADS`] threads, pages committed on touch.
pub const QUERY_STACK: usize = 64 << 20;

/// The query layer's dedicated runtime: DataFusion operators and the scans
/// served for other nodes run on its few threads, so a heavy statement does not
/// take the engine's and ingress's threads. `None` = run on the calling runtime
/// (deterministic simulation, `query_threads = 0`).
pub struct QueryRuntime(Option<tokio::runtime::Runtime>);

impl QueryRuntime {
    /// Handle of the dedicated runtime (`None` when running on the node's runtime).
    pub fn handle(&self) -> Option<tokio::runtime::Handle> {
        self.0.as_ref().map(|rt| rt.handle().clone())
    }

    fn new(threads: usize) -> Self {
        if threads == 0 {
            return Self(None);
        }
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_stack_size(QUERY_STACK)
            .thread_name("cairn-query")
            .enable_all()
            .build()
        {
            Ok(rt) => Self(Some(rt)),
            Err(e) => {
                tracing::warn!(error = %e, "cannot build the query runtime, running on the node's runtime");
                Self(None)
            }
        }
    }

    /// Runs `fut` on this runtime. Dropping the returned future (client closed
    /// the connection, deadline passed) cancels the running work — the snapshot
    /// is released as when running in place.
    async fn run<F>(&self, fut: F) -> Result<F::Output, tokio::task::JoinError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
        impl<T> Drop for AbortOnDrop<T> {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        impl<T> std::future::Future for AbortOnDrop<T> {
            type Output = Result<T, tokio::task::JoinError>;
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                std::pin::Pin::new(&mut self.0).poll(cx)
            }
        }
        match &self.0 {
            Some(rt) => AbortOnDrop(rt.spawn(fut)).await,
            None => Ok(fut.await),
        }
    }
}

impl Drop for QueryRuntime {
    fn drop(&mut self) {
        // Dropping a runtime inside an async context panics: shut down in the background.
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    /// Per-tenant slots: two statements of `acme` run, the third waits and
    /// times out; `bravo` still gets a slot; once one slot is returned, `acme`'s
    /// waiter goes through.
    #[tokio::test]
    async fn tenant_slots_cap_one_tenant() {
        use super::*;
        let s = Arc::new(TenantSlots::new(2));
        let w = Duration::from_millis(50);
        let a1 = s.take("acme", w).await.expect("1");
        let _a2 = s.take("acme", w).await.expect("2");
        assert!(s.take("acme", w).await.is_none(), "acme's third statement");
        let _b = s.take("bravo", w).await.expect("another tenant");
        let waiter = {
            let s = Arc::clone(&s);
            tokio::spawn(async move { s.take("acme", Duration::from_secs(5)).await.is_some() })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(a1);
        assert!(waiter.await.unwrap(), "slot back ⇒ waiter passes");
    }

    /// Run time goes into the bucket: one second of running = one second of the
    /// bucket's refill, added to the read units.
    #[test]
    fn runtime_is_charged() {
        use super::*;
        let b = TenantBudget {
            capacity: 1_000_000,
            per_second: 200_000,
        };
        assert_eq!(charge(10, 0, &b), 10);
        assert_eq!(charge(10, 1_000, &b), 200_010);
        assert_eq!(charge(0, 250, &b), 50_000);
    }
}
