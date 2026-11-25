// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `NodeConfig`: everything needed to build a node.

use super::*;

/// Everything needed to build a node. The defaults of [`NodeConfig::new`]
/// match the binary's defaults, except `port` (`0` — the OS picks a port, for
/// tests).
///
/// Test tooling does not leak into the release build: without the `testkit`
/// feature `NodeConfig` **has no** `net_filter` field. The two doctests below
/// only exist when the feature is off: the first must *not* compile, the
/// second (same shape, different field) proves the first fails precisely
/// because of that field.
///
#[cfg_attr(not(feature = "testkit"), doc = "```compile_fail")]
#[cfg_attr(
    not(feature = "testkit"),
    doc = "fn release_build_has_no_net_filter(cfg: cairn_server::node::NodeConfig) {"
)]
#[cfg_attr(not(feature = "testkit"), doc = "    let _ = cfg.net_filter;")]
#[cfg_attr(not(feature = "testkit"), doc = "}")]
#[cfg_attr(not(feature = "testkit"), doc = "```")]
#[cfg_attr(not(feature = "testkit"), doc = "")]
#[cfg_attr(not(feature = "testkit"), doc = "```")]
#[cfg_attr(
    not(feature = "testkit"),
    doc = "fn same_shape_other_field(cfg: cairn_server::node::NodeConfig) {"
)]
#[cfg_attr(not(feature = "testkit"), doc = "    let _ = cfg.migrations;")]
#[cfg_attr(not(feature = "testkit"), doc = "}")]
#[cfg_attr(not(feature = "testkit"), doc = "```")]
#[cfg_attr(not(feature = "testkit"), doc = "")]
#[cfg_attr(
    not(feature = "testkit"),
    doc = "The same goes for the simulated S3 store profile: the release build has no"
)]
#[cfg_attr(
    not(feature = "testkit"),
    doc = "`store_profile` module, so there is no path that reads `CAIRNFLOW_STORE_PROFILE`."
)]
#[cfg_attr(not(feature = "testkit"), doc = "")]
#[cfg_attr(not(feature = "testkit"), doc = "```compile_fail")]
#[cfg_attr(
    not(feature = "testkit"),
    doc = "let _ = cairn_server::store_profile::ENV;"
)]
#[cfg_attr(not(feature = "testkit"), doc = "```")]
#[derive(Clone)]
pub struct NodeConfig {
    pub stores: ObjectStores,
    pub mode: NodeMode,
    pub host: String,
    /// Host advertised to peers and clients (the session lease, `leader` of a
    /// `503`) instead of `host` (`--advertise-host`). `None` ⇒ advertise the
    /// bind address itself. Already checked by
    /// [`crate::node::check_advertise_host`].
    pub advertise_host: Option<String>,
    /// `0` ⇒ the OS picks the port; the real address is in [`RunningNode::addr`].
    pub port: u16,
    /// The **internal** port (`/internal/*`, called only by other nodes), same
    /// `host`. `0` ⇒ the OS picks; the real address is in
    /// [`RunningNode::internal_addr`] and is the address the node advertises
    /// to peers.
    pub internal_port: u16,
    /// API keys and the cluster secret. **Off** by default (in-process tests);
    /// on by default in the binary.
    pub auth: crate::auth::AuthConfig,
    /// Background loop interval for timers and retries — a **safety net**: the
    /// background loop wakes at the nearest deadline of the schedule table, the
    /// interval only catches what a deadline hint missed. `None` = background
    /// loop off entirely (tests drive time themselves through `/admin/tick`).
    pub tick_interval: Option<Duration>,
    pub limits: TurnLimits,
    /// How long (ms) to keep finished invocations before cleanup. `None` =
    /// forever. This is also the dedupe window of idempotency keys.
    pub retention_ms: Option<u64>,
    /// Archive the journal + final state as an `archive/…` object before
    /// cleanup. On by default.
    pub archive: bool,
    /// `--archive-artifacts`: archiving also copies artifact bytes to
    /// `archive/<tenant>/<date>/<id>.artifacts/<sha>`. Off by default: the
    /// archive keeps only refs.
    pub archive_artifacts: bool,
    /// How long `/invoke` waits at most for the invocation to stop. `None` = do not wait.
    pub invoke_wait: Option<Duration>,
    /// Cap on `?wait_ms=` on every attach (`--attach-max-wait-ms`): long-poll
    /// up to `min(wait_ms, cap)`.
    pub attach_max_wait: Duration,
    /// Smallest period of an `everyMs` schedule (`--schedule-min-period-ms`):
    /// sub-minute schedules are expensive, so there is a floor.
    pub schedule_min_period: Duration,
    /// Floor of the `/invoke` polling grid while the range's waiter registry
    /// is being fed — **tests only** (with a grid sparser than the wait limit,
    /// only the outcome notifier gets the answer back before the deadline).
    /// The dense grid of a range nobody leads has no floor. No command-line
    /// flag. `None` = 1 s grid.
    pub invoke_poll_floor: Option<Duration>,
    /// Follower reader refresh period. Floor 50ms.
    pub reader_poll: Duration,
    /// Refresh period of the cluster-level registries (deployments, keys,
    /// tenants, captures, range windows) from the bucket. Floor 100ms. Only a
    /// **safety net**: a node writing a registry notifies peers at once over
    /// the internal port ([`crate::registry_sync`]). The quarantine list
    /// follows `min(this interval, 30 s)`. A tick where the registry generation
    /// did not change costs a single GET.
    pub deploy_refresh: Duration,
    /// Interval recounting per-tenant usage (live invocations, artifact bytes)
    /// for the `max-live=`/`max-artifact-bytes=` caps. Runs only when some
    /// tenant carries one of the two caps. No command-line flag: tests lower
    /// it.
    pub tenant_usage_every: Duration,
    /// Interval at which permission denials are merged into audit records.
    /// Default 60 s; no command-line flag: tests lower it.
    pub audit_denials_every: Duration,
    /// Interval of the full reconciliation of the cluster-level registries and
    /// the quarantine list **regardless of** the registry generation
    /// (`control/registry-gen`, [`crate::registry_gen`]): a safety net for a
    /// writer that died between its main write and the generation bump, or a
    /// binary that does not know about generations. Default
    /// [`crate::registry_gen::DEFAULT_RECONCILE`] (10 minutes).
    pub registry_reconcile: Duration,
    /// An odd ("writing") registry generation standing longer than this makes
    /// the coordinator remove the writer's token — the writer died between the
    /// main write and the commit. Default [`crate::registry_gen::DEFAULT_STUCK`]
    /// (2 minutes; tests lower it).
    pub registry_stuck: Duration,
    /// Interval of the session's **owner-record watch loop** (layer 3 of LS-6,
    /// [`cairn_store::LeaseConfig::with_owner_watch`]). `None` = the default of
    /// `LeaseConfig`.
    pub owner_watch: Option<Duration>,
    /// Interval for flushing per-tenant metering counts to the bucket. Floor 100 ms.
    pub metering_flush: Duration,
    /// D-d: the loop removing the Dbs of retired ranges that no live range
    /// references any more — interval, `min_age`, dry run
    /// ([`crate::gc::GcConfig`]). `None` = **off** (default) — it deletes
    /// data; turn it on after chaos and simulation runs are clean on your
    /// store.
    pub gc_retired_dbs: Option<crate::gc::GcConfig>,
    /// **Safety net** of the **tombstone-on-behalf** loop (SM-11,
    /// [`crate::gc::OrphanRetirer`]): ranges that left the table whose owner
    /// died before writing the tombstone, plus the orphan children of a
    /// rollback (SM-14). Only the D-d runner ([`crate::gc::is_runner`]). The
    /// loop runs on kicks (`node/setup.rs`), with a default safety-net interval
    /// of [`crate::gc::DEFAULT_RETIRE_ORPHANS_EVERY`] (one hour). On by
    /// default, independent of `gc_retired_dbs`: this loop deletes no retired
    /// Db. `None` = both kicks and the safety net off.
    pub retire_orphans_every: Option<Duration>,
    /// Interval of the **intent reconciliation** loop (SM-15,
    /// [`crate::split::IntentReconciler`]), same runner as the loop above.
    /// Default [`crate::split::DEFAULT_RECONCILE_INTENTS_EVERY`] (15 s; each
    /// round is one GET of `cluster.conf`). `None` = off. It used to share the
    /// interval of `retire_orphans_every`.
    pub reconcile_intents_every: Option<Duration>,
    /// Interval of the **state scrubber** ([`crate::scrub::Scrubber`]):
    /// `verify` running in the background, dry — metrics and logs only. Only
    /// the D-d runner. On by default (600 s); `None` = off.
    pub scrub_every: Option<Duration>,
    /// The scrubber's request budget per hour (`--scrub-requests-per-hour`):
    /// `scrub_every` is the **fastest** interval; a round costing `C` requests
    /// makes the next round wait at least `C / budget` hours. `None` = the
    /// default ([`crate::scrub::default_budget`]: the cost of one round over 64
    /// ranges per `scrub_every`); `Some(0)` = no cap.
    pub scrub_requests_per_hour: Option<u64>,
    /// The trash ([`cairn_store::trash`]): how long entries are kept before the
    /// D-d runner's cleanup loop may delete them for real (after checking
    /// again). `None` = no cleanup — the trash only grows.
    pub trash_keep: Option<Duration>,
    /// Tenant purge ([`crate::tenant_purge`]): how long a `deleting` tenant is
    /// kept reversible before the D-d runner deletes it for real. `None` = the
    /// purge loop is off — nothing is deleted.
    pub tenant_purge_keep: Option<Duration>,
    /// Interval of the artifact sweep ([`crate::artifact_sweep`]). `None` = off
    /// — artifacts are never deleted. Also off when `trash_keep` is `None`.
    pub artifact_sweep: Option<Duration>,
    /// Minimum age of an object before the sweep touches its directory.
    pub artifact_min_age: Duration,
    /// Interval of the value sweep ([`crate::value_sweep`]). `None` = off —
    /// segments are never deleted. Also off when `trash_keep` is `None`.
    pub value_sweep: Option<Duration>,
    /// Minimum age of a segment before the sweep touches it.
    pub value_min_age: Duration,
    /// `T_put` (AP-9): the time limit from an artifact PUT to the commit of its
    /// row; past it, PUT again. Must be ≤ `artifact_min_age / 2`.
    pub artifact_put_deadline: Duration,
    /// WAL flush interval. `None` = keep the interval already set on `stores`.
    pub flush_interval: Option<Duration>,
    /// Number of threads of the engine pool. `None` = number of cores — see
    /// [`crate::engine_pool`].
    pub engine_threads: Option<usize>,
    /// Knobs of the shared WAL (W, S, window, object cap, T, PUT cap).
    pub node_wal: NodeWalConfig,
    /// The node's default slow-log threshold; a tenant sets its own through
    /// `PUT /admin/tenants/{tenant}`.
    pub slow_log: Duration,
    /// The migration registry the node runs. Always
    /// [`cairn_store::migrate::MIGRATIONS`] outside tests; tests inject their
    /// own registry instead of adding fake entries to the real one.
    pub migrations: &'static [cairn_store::migrate::Migration],
    /// Network filter for every request this node sends to another node — only
    /// with the `testkit` feature. `None` = the network is open.
    #[cfg(feature = "testkit")]
    pub net_filter: Option<Arc<dyn crate::netfilter::NetFilter>>,
    /// Runs every engine on the `LocalSet` of the caller of `start` and
    /// SlateDB's background work on the caller's runtime, instead of the
    /// process's thread pool + background runtime ([`EnginePool::inline`]).
    /// Only for deterministic simulation; `start` must run inside a
    /// `LocalSet`. Default `false`.
    #[cfg(feature = "testkit")]
    pub inline_engines: bool,
    /// The coordinator marks suspects in `view`. `false` only to test the
    /// remaining safety net when suspect marks are off — a sparse tracker.
    /// Only with the `testkit` feature; default `true`.
    #[cfg(feature = "testkit")]
    pub view_suspects: bool,
    /// Automatic split by load and size. Default `suggest`: only suggest.
    pub auto_split: crate::autosplit::AutoSplitConfig,
    /// Standalone children after a split: compact a whole range standing on a
    /// deep or old ancestor chain; the depth cap of automatic splitting.
    pub lineage: crate::lineage::LineageConfig,
    /// The node's slowness score. A node standing alone never releases
    /// (LS-11).
    pub slow: crate::slow::SlowConfig,
    /// Quarantine of poisonous handlers: the node's default mode, rules and
    /// TTL; a tenant overrides them with `PUT /admin/tenants/{tenant}`.
    pub quarantine: crate::quarantine::QuarantineConfig,
    /// The quiet-renewal gate of a **new bucket** (`--quiet-lease`): a node
    /// building an empty bucket pre-writes `control/quiet-lease` according to
    /// this choice — delegated by default. A bucket that already has a cluster
    /// ignores it (a `debug` log if different).
    pub quiet_lease: cairn_store::quiet_lease::Bootstrap,
    /// Streams and SSE: the node's RAM cap, SSE connection cap, keepalive,
    /// maximum lifetime.
    pub streams: crate::streams::StreamConfig,
    /// HTTP header read limit of both ports (`--http-header-timeout-ms`).
    /// hyper's HTTP/1 has no separate idle timeout: the same timer also runs
    /// while a keep-alive connection waits for the next request, so this number
    /// is both the slowloris limit and the idle timeout — it must be larger
    /// than the idle timeout of the proxy in front.
    pub http_header_timeout: Duration,
    /// Limit on each `/query` statement.
    pub query_limits: cairn_query::Limits,
    /// Per-tenant token bucket of `/query`, in read units.
    pub query_tenant_budget: crate::query::http::TenantBudget,
    /// Default caps for fields missing from the `tenants` registry: empty = a
    /// missing field means no cap; the SaaS profile sets the beta plan's
    /// defaults.
    pub tenant_defaults: cairn_engine::TenantLimit,
    /// Per-key (or per-SSO-user) request rate cap at ingress, GCRA `N/S`.
    /// `None` = no cap.
    pub key_rate: Option<crate::tenants::DeployRate>,
    /// Number of threads of the query layer's own runtime: DataFusion operators
    /// and scans serving other nodes run there, without competing for threads
    /// with the engines. `0` = run on the node's runtime (deterministic
    /// simulation).
    pub query_threads: usize,
    /// Child processes for the caller's `/query` statements
    /// (`--query-isolation`). `None` = run in place on the query runtime (the
    /// default of the library and tests; the binary chooses in `main.rs`).
    /// Needs `query_threads > 0`; deterministic simulation always runs in
    /// place.
    pub query_worker: Option<crate::query::QueryWorkerConfig>,
    /// The node's SST disk cache (`--disk-cache-dir`). `None` = off (the
    /// default; never on in deterministic simulation — real disk I/O).
    pub disk_cache: Option<cairn_store::disk_cache::DiskCacheConfig>,
    /// The web console on the ingress port (`--no-ui`, `--ui-base-path`). On by
    /// default at `/ui`, as in the binary.
    pub ui: crate::ui::UiConfig,
    /// Egress policy for requests made on behalf of tenants (`--egress-*`).
    /// The default lets everything through — self-hosting as before.
    pub egress: crate::egress::EgressPolicy,
    /// The cell's identity master (`CAIRNFLOW_IDENTITY_MASTER`). `None` ⇒
    /// requests to the SDK are not signed — self-hosting as before.
    pub identity_master: Option<crate::identity::Master>,
    /// `POST /deployments` refuses an SDK that does not declare
    /// `requestIdentity` (`--deploy-require-identity`, on in the SaaS
    /// profile). Off by default.
    pub deploy_require_identity: bool,
    /// Choice of transport mode when registering a deployment
    /// (`--deploy-transport`). Default `declared`: the mode the SDK declares.
    pub deploy_transport: crate::discovery::TransportPolicy,
    /// An `https://` endpoint whose SDK declares `BIDI_STREAM` is registered as
    /// `REQUEST_RESPONSE`, unless the request states `transport` explicitly
    /// (`--deploy-https-request-response`, on in the SaaS profile). Off by
    /// default.
    pub deploy_https_request_response: bool,
    /// New deployments do not record `rrCompression` even when the SDK
    /// declares it — `REQUEST_RESPONSE` bodies go uncompressed
    /// (`--deploy-no-rr-compression`). Off by default: compress when the SDK
    /// declares it.
    pub deploy_no_rr_compression: bool,
    /// `POST /deployments` refuses a `REQUEST_RESPONSE` endpoint whose SDK does
    /// not declare `requestCompression: gzip` (`--deploy-require-compression`,
    /// on in the SaaS profile). Off by default.
    pub deploy_require_compression: bool,
    /// The read path of segmented values for SDK v6 (`--value-*`). The default
    /// `auto` with no flags ⇒ `inline`.
    pub values: crate::value_urls::ValueConfig,
}

impl NodeConfig {
    pub fn new(stores: ObjectStores, mode: NodeMode) -> Self {
        Self {
            stores,
            mode,
            host: "127.0.0.1".into(),
            advertise_host: None,
            port: 0,
            internal_port: 0,
            auth: crate::auth::AuthConfig::default(),
            tick_interval: Some(Duration::from_millis(200)),
            limits: TurnLimits::default(),
            retention_ms: Some(24 * 60 * 60 * 1000),
            archive: true,
            archive_artifacts: false,
            invoke_wait: Some(Duration::from_secs(5)),
            attach_max_wait: DEFAULT_ATTACH_MAX_WAIT,
            schedule_min_period: DEFAULT_SCHEDULE_MIN_PERIOD,
            invoke_poll_floor: None,
            reader_poll: Duration::from_millis(500),
            deploy_refresh: DEFAULT_DEPLOY_REFRESH,
            tenant_usage_every: crate::tenant_usage::DEFAULT_EVERY,
            audit_denials_every: crate::audit::DENIALS_EVERY,
            registry_reconcile: crate::registry_gen::DEFAULT_RECONCILE,
            registry_stuck: crate::registry_gen::DEFAULT_STUCK,
            owner_watch: None,
            metering_flush: Duration::from_secs(60),
            gc_retired_dbs: None,
            retire_orphans_every: Some(crate::gc::DEFAULT_RETIRE_ORPHANS_EVERY),
            reconcile_intents_every: Some(crate::split::DEFAULT_RECONCILE_INTENTS_EVERY),
            scrub_every: Some(crate::scrub::DEFAULT_EVERY),
            scrub_requests_per_hour: None,
            trash_keep: Some(cairn_store::trash::DEFAULT_KEEP),
            tenant_purge_keep: Some(cairn_store::tenant_purge::KEEP),
            artifact_sweep: Some(crate::artifact_sweep::DEFAULT_EVERY),
            artifact_min_age: crate::artifact_sweep::DEFAULT_MIN_AGE,
            value_sweep: Some(crate::value_sweep::DEFAULT_EVERY),
            value_min_age: crate::value_sweep::DEFAULT_MIN_AGE,
            artifact_put_deadline: cairn_store::artifact::T_PUT,
            flush_interval: None,
            engine_threads: None,
            node_wal: NodeWalConfig::default(),
            slow_log: crate::slowlog::DEFAULT_THRESHOLD,
            migrations: cairn_store::migrate::MIGRATIONS,
            #[cfg(feature = "testkit")]
            net_filter: None,
            #[cfg(feature = "testkit")]
            inline_engines: false,
            #[cfg(feature = "testkit")]
            view_suspects: true,
            auto_split: crate::autosplit::AutoSplitConfig::default(),
            lineage: crate::lineage::LineageConfig::default(),
            slow: crate::slow::SlowConfig::default(),
            quarantine: crate::quarantine::QuarantineConfig::default(),
            quiet_lease: cairn_store::quiet_lease::Bootstrap::default(),
            streams: crate::streams::StreamConfig::default(),
            http_header_timeout: DEFAULT_HTTP_HEADER_TIMEOUT,
            query_limits: cairn_query::Limits::default(),
            query_tenant_budget: Default::default(),
            tenant_defaults: cairn_engine::TenantLimit::default(),
            key_rate: None,
            query_threads: crate::query::http::QUERY_THREADS,
            query_worker: None,
            disk_cache: None,
            ui: crate::ui::UiConfig::default(),
            egress: crate::egress::EgressPolicy::default(),
            identity_master: None,
            deploy_require_identity: false,
            deploy_transport: Default::default(),
            deploy_https_request_response: false,
            deploy_no_rr_compression: false,
            deploy_require_compression: false,
            values: Default::default(),
        }
    }
}

/// Maximum length of `--node-id`.
pub const NODE_ID_MAX: usize = 64;

/// A `--node-id` set by the operator: the node id goes into object keys
/// (lease, session, shared WAL), so only letters, digits, `-`, `_`, `.` are
/// accepted, 1…[`NODE_ID_MAX`] ASCII characters. `.` is for DNS names and
/// Helm's `$(POD_NAME)`. A generated id (`NodeMode::node_id` absent) does not
/// go through here — cairn builds it.
pub fn check_node_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > NODE_ID_MAX
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        || id.starts_with('.')
    {
        return Err(format!(
            "invalid node id {id:?}: 1…{NODE_ID_MAX} letters, digits, `-`, `_`, `.` \
             (must not start with `.`)"
        ));
    }
    Ok(())
}

/// Default HTTP header read limit: 75 s — above the 60 s idle timeout of
/// common proxies (AWS ALB; nginx `keepalive_timeout` 75 s is the cap), so the
/// proxy closes idle connections before we do and there is no connection-reuse
/// race.
pub const DEFAULT_HTTP_HEADER_TIMEOUT: Duration = Duration::from_secs(75);

/// Default cap of `?wait_ms=` on attach: 60 s — the idle limit of AWS ALB and
/// nginx. Clients default to 25 s, under the 30 s limit of the GCP LB.
pub const DEFAULT_ATTACH_MAX_WAIT: Duration = Duration::from_secs(60);

/// Default floor of `everyMs`: 10 s.
pub const DEFAULT_SCHEDULE_MIN_PERIOD: Duration = Duration::from_secs(10);
