// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `cairnflow` binary — one node.
//!
//! ```sh
//! cairnflow --data-dir ./data --port 8080
//! ```
//!
//! Or point straight at object stores, with the two tiers separate (D7):
//!
//! ```sh
//! cairnflow --main-store s3://cairn-sst/prod --wal-store s3://cairn-wal --port 8080
//! ```

use {
    cairn_server::{
        actor::TurnLimits,
        node::{self, NodeConfig, NodeMode},
    },
    cairn_store::{ObjectStores, StoreLocation},
    std::{path::PathBuf, time::Duration},
};

/// The default cap of one graceful shutdown.
///
/// Shutdown seals each range (waiting for what is pending to become durable, then
/// flushing the WAL) and closes the session; on a real object store each step is about a
/// hundred milliseconds. 20 s is many times that, yet still under the familiar grace
/// period of orchestrators (Kubernetes waits 30 s, then `SIGKILL`) — exiting by itself
/// first means the log can still say why.
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

struct Args {
    /// The main store: SSTs + manifest. The manifest holds `writer_epoch`.
    main_store: String,
    /// The WAL store. `None` ⇒ shares the main store (the dev setup).
    wal_store: Option<String>,
    /// `--wal-atomic-put true|false`: declares the atomic PUT capability of the WAL tier.
    /// `None` ⇒ by scheme (`cairn_store::atomic_put_by_scheme`) — for `s3://` only on
    /// checked stores (AWS S3, Tigris, SeaweedFS); on another store the node does not
    /// start until it is declared (`store_check.rs`).
    wal_atomic_put: Option<bool>,
    /// `--disk-cache-dir`: the node's SST disk cache. `None` = off — the binary needs no
    /// disk.
    disk_cache_dir: Option<std::path::PathBuf>,
    /// `--disk-cache-bytes`: the disk cache cap (default 16 GiB).
    disk_cache_bytes: u64,
    /// `--cluster` / `CAIRNFLOW_CLUSTER`: **no longer has any effect** — every node runs
    /// the lease layer, a node without peers is a one-node cluster. Still accepted so old
    /// commands and environment variables keep working; it only leaves a note on the
    /// "mode" line at startup.
    cluster: bool,
    /// The node's identity in the cluster. `None` ⇒ generated per process.
    node_id: Option<String>,
    lease_ttl_ms: u64,
    /// The follower reader's refresh interval — the staleness floor of follower reads.
    reader_poll_ms: u64,
    /// The refresh interval of the deployment registry from the bucket — the registration
    /// propagation delay.
    deploy_refresh_ms: u64,
    /// The number of partitions when the bucket is initialised. Fixed after that — changed by
    /// split/merge.
    partitions: u16,
    port: u16,
    host: String,
    /// The host advertised to peers instead of `--host`. `None` = advertise exactly the
    /// bind address.
    advertise_host: Option<String>,
    /// The background loop tick for timers and retries — a safety net: timers fire at
    /// their due time, the tick only catches what slips through. `0` turns the background
    /// loop off entirely (tests control time themselves).
    tick_interval_ms: u64,
    /// How long finished invocations are kept before purging. `0` = keep forever.
    ///
    /// This is also **the idempotency dedupe window**: after the purge, a call with the
    /// same idempotency key is a new invocation.
    retention_ms: u64,
    /// The number of client-initiated turns running at once on a partition.
    ///
    /// Each turn in flight is a connection to the customer's deployment. Applies to every
    /// turn of the partition — including turns handed to the scheduler by timers,
    /// retries, recovery or the drainer.
    max_concurrent_turns: usize,
    /// The cap on waiting for the SDK within a turn. `0` = off (not recommended).
    ///
    /// When it runs out the turn is dropped and rescheduled with backoff — the journal
    /// intact, losing exactly the work of the unfinished turn, just like a `kill -9` in
    /// the middle of a turn.
    inactivity_timeout_ms: u64,
    /// The upper cap of the `inactivityTimeout` a contract declares, ms.
    inactivity_timeout_max_ms: u64,
    /// The cap on running turns of one tenant on the node. `0` = no cap.
    max_turns_per_tenant: usize,
    /// The node's replay RAM budget, bytes. `0` = off.
    replay_ram_budget_bytes: u64,
    /// Default caps for absent fields of the `tenants` registry: empty = absent means no
    /// cap; the SaaS profile sets [`saas_tenant_defaults`].
    tenant_defaults: cairn_engine::TenantLimit,
    /// The per-key request rate cap `N/S`. `None` = no cap; the SaaS profile sets
    /// [`SAAS_KEY_RATE`].
    key_rate: Option<cairn_server::tenants::DeployRate>,
    /// The cap on waiting for a slot in the flow control queue, ms. `0` = wait forever.
    queue_wait_ms: u64,
    /// The L0 brake: the number of L0 views at which braking starts, `0` = off.
    l0_soft: usize,
    /// The L0 brake: the maximum probability of refusing new work, in `[0, 1)`.
    l0_max_reject: f64,
    /// How long `/invoke` waits at most for the invocation to stop for good. `0` = no wait.
    invoke_wait_ms: u64,
    attach_max_wait_ms: u64,
    schedule_min_period_ms: u64,
    /// The HTTP header read cap of both ports, ms.
    http_header_timeout_ms: u64,
    /// Where a caller's `/query` statement runs.
    query_isolation: QueryIsolation,
    /// The `cairn-query-worker` binary (`--query-worker`); absent = next to this binary.
    query_worker: Option<std::path::PathBuf>,
    /// The memory cap of each `/query` child process, MiB. `None` = the default
    /// (`DEFAULT_WORKER_MEMORY_MB`; `--profile saas`: [`SAAS_QUERY_WORKER_MEMORY_MB`]).
    query_worker_memory_mb: Option<u64>,
    /// The node is not dumpable (`PR_SET_DUMPABLE 0`, Linux): processes of the same user —
    /// including `/query` children — cannot read `/proc/<node>/environ` or `mem`, and there
    /// are no core dumps. Only `--profile saas` turns it on.
    non_dumpable: bool,
    /// The cap on long-lived connections (SSE, attach) per tenant per node. `0` = no cap.
    max_open_streams_per_tenant: usize,
    /// The node's default slow log threshold, ms.
    slow_log_ms: u64,
    /// The default cut window of the shared WAL (`NodeWalConfig::window` without
    /// `--wal-window-ms`) — on a fast store, the latency floor of back-to-back commits.
    /// SlateDB's `flush_interval` takes this number too, but it does not run on the commit
    /// path with the shared WAL. See [`cairn_store::DEFAULT_FLUSH_INTERVAL`].
    flush_interval_ms: u64,
    /// The number of threads of the engine pool. `0` = the number of cores.
    engine_threads: usize,
    /// The shared WAL knobs. Window `None` = follow `flush_interval_ms`.
    node_wal: cairn_store::NodeWalConfig,
    wal_window_ms: Option<u64>,
    /// The total cap of one graceful shutdown after `SIGTERM`/`SIGINT`, ms. Past the cap
    /// it exits at once like `kill -9` — peers take the ranges after the TTL.
    shutdown_timeout_ms: u64,
    /// The internal port (`/internal/*`, called only by other nodes). `None` = follow
    /// `port`: `port + 1`, or `0` (system-chosen) when `port` is `0`.
    internal_port: Option<u16>,
    /// `--no-auth`: turns API keys off — dev only.
    no_auth: bool,
    /// `CAIRNFLOW_CLUSTER_SECRET`: a cluster secret outside the bucket. `None` ⇒ the
    /// `auth/cluster-secret` object in the bucket (the first node creates it).
    cluster_secret: Option<String>,
    /// `--profile saas` (`CAIRNFLOW_PROFILE`): safe defaults for a shared cell, applied and
    /// checked in [`Parsing::finish`].
    profile: Option<Profile>,
    /// `--deploy-require-identity`: `POST /deployments` refuses SDKs that do not declare
    /// `requestIdentity` (do not verify the cell's signature).
    deploy_require_identity: bool,
    /// `--deploy-transport declared|auto`: chooses the transport mode at deployment
    /// registration. Default `declared` (the original behaviour).
    deploy_transport: cairn_server::discovery::TransportPolicy,
    /// `--deploy-https-request-response` (on in the SaaS profile): an `https://` endpoint
    /// declaring `BIDI_STREAM` is registered as `REQUEST_RESPONSE` when the request does
    /// not state `transport`.
    deploy_https_request_response: bool,
    /// `--deploy-no-rr-compression`: new deployments do not record `rrCompression` even
    /// when the SDK declares it — `REQUEST_RESPONSE` bodies go uncompressed.
    deploy_no_rr_compression: bool,
    /// `--deploy-require-compression` (on in the SaaS profile): `POST /deployments`
    /// refuses `REQUEST_RESPONSE` endpoints whose SDK does not declare
    /// `requestCompression: gzip` — the cell pays the egress for every replay.
    deploy_require_compression: bool,
    /// `--value-read-path`, `--value-presign-ttl-s`, `--value-public-endpoint`,
    /// `--value-public-url`.
    values: cairn_server::value_urls::ValueConfig,
    /// `--no-ui`: do not serve the web console.
    no_ui: bool,
    /// `--ui-base-path`: the web console's base path, checked when the flag is read.
    ui_base_path: String,
    /// `--ui-cookie-secure on|off`: the session cookie carries `Secure`. `None` = follow
    /// `--host` (on when not loopback).
    ui_cookie_secure: Option<bool>,
    /// `--ui-env-label`: the environment label in the console's top bar.
    ui_env_label: Option<String>,
    /// `--egress-deny-private`, `--egress-require-https`, `--egress-allow-cidr`,
    /// `--egress-deny-cidr`: the policy of requests on behalf of a tenant. The default
    /// lets everything through (self-hosting, e2e calling `localhost`).
    egress: cairn_server::egress::EgressPolicy,
    /// `CAIRNFLOW_IDENTITY_MASTER`: the cell's identity master, ≥ 32 bytes. `None` ⇒
    /// requests to the SDK are not signed.
    identity_master: Option<cairn_server::identity::Master>,
    /// `--trust-proxy <cidr>[,…]` (repeatable): proxies trusted for `X-Forwarded-*`.
    trust_proxy: cairn_server::auth::TrustedProxies,
    /// `--gc-retired-dbs`: turns D-d on — cleans the Dbs of retired ranges every 10
    /// minutes. Off by default: it deletes data. `--gc-retired-dbs=plan` only logs the
    /// plan.
    gc_retired_dbs: Option<cairn_server::gc::GcMode>,
    /// `--gc-every-s N`: the D-d round interval (default 600).
    gc_every_s: u64,
    /// `--gc-min-age-s N`: the minimum tombstone age before deletion (default 1800).
    gc_min_age_s: u64,
    /// `--retire-orphans-every-s N`: the safety-net tick of the loop writing tombstones on
    /// behalf of others / cleaning orphan children (SM-11, SM-14; the loop runs when
    /// nudged); `0` = off.
    retire_orphans_every_s: u64,
    /// `--reconcile-intents-every-s N`: the intent reconcile loop interval (SM-15);
    /// `0` = off.
    reconcile_intents_every_s: u64,
    /// `--scrub-every-s N`: the state scrubber interval, dry — metrics and logs only;
    /// `0` = off.
    scrub_every_s: u64,
    /// `--scrub-requests-per-hour N`: the scrubber's request budget; `None` = the default
    /// by interval, `0` = no cap.
    scrub_requests_per_hour: Option<u64>,
    /// `--trash-keep-s N`: keep trash entries N seconds before the purge loop may really
    /// delete them (default 7 days; 0 = never purge).
    trash_keep_s: u64,
    /// `--value-sweep-every-s N`: the interval of the orphan segment sweep (default 3600;
    /// 0 = off, segments are never deleted).
    value_sweep_every_s: u64,
    /// `--value-min-age-s N`: the minimum age of a segment before the sweep touches it
    /// (default 3600; ≥ 2 × T_PUT = 600 — AP-9).
    value_min_age_s: u64,
    /// `--tenant-purge-keep-s N`: a `deleting` tenant stays reversible for N seconds
    /// before the runner really deletes (default 7 days; 0 = the purge loop is off — the
    /// tenant stays `deleting` forever, nothing deleted).
    tenant_purge_keep_s: u64,
    /// `--no-archive`: purge without archiving.
    no_archive: bool,
    /// `--no-rr-delta-replay`: `REQUEST_RESPONSE` turns always replay in full, even with
    /// an SDK declaring `replayCache` on wire v5.
    no_rr_delta_replay: bool,
    /// `--no-user-otlp` (`CAIRNFLOW_NO_USER_OTLP`): do not export user spans/`$log` to
    /// OTLP; system spans are still exported.
    user_otlp: cairn_server::otel::UserExport,
    /// `--archive-artifacts`.
    archive_artifacts: bool,
    /// `--auto-split …`.
    auto_split: cairn_server::autosplit::AutoSplitConfig,
    /// `--lineage-…`.
    lineage: cairn_server::lineage::LineageConfig,
    /// `--slow-shed on|off`: a slow node gives up leadership by itself. On by default; off
    /// still measures and publishes the slowness score.
    slow_shed: bool,
    /// `--quarantine MODE`, `--quarantine-rule A/I/S`, `--quarantine-ttl-s N`: poison
    /// handler quarantine. Default `observe`.
    quarantine: cairn_server::quarantine::QuarantineConfig,
    /// `--quiet-lease off|solo|delegation` (`CAIRNFLOW_QUIET_LEASE`): the quiet renewal
    /// gate written up front when this node **creates a new bucket**. Default
    /// `delegation`; a bucket that already has a cluster ignores it.
    quiet_lease: cairn_store::quiet_lease::Bootstrap,
}

/// `--data-dir DIR` is a shortcut for `--main-store file://DIR`.
///
/// Kept because it is the original way of calling the node and appears in the docs and
/// the tests. Converted into an absolute URI rather than keeping a relative path:
/// `LocalFileSystem` anchors to the working directory, so a relative path would mean
/// "where the data is" depends on where the command was typed.
fn data_dir_uri(dir: &PathBuf) -> Result<String, String> {
    let abs = if dir.is_absolute() {
        dir.clone()
    } else {
        std::env::current_dir()
            .map_err(|e| format!("cannot read the working directory: {e}"))?
            .join(dir)
    };
    Ok(format!("file://{}", abs.display()))
}

fn parse_args() -> Result<Args, String> {
    parse_args_from(
        &|name: &str| std::env::var(name).ok(),
        std::env::args().skip(1).collect(),
    )
}

/// [`parse_args`] over a given environment variable source and `argv` (without the
/// program name) — so it can be tested.
///
/// Order: defaults ([`Parsing::defaults`]), then environment variables per the [`ENV`]
/// table, then flags per the [`FLAGS`] table — flags win over environment variables.
fn parse_args_from(
    env: &dyn Fn(&str) -> Option<String>,
    argv: Vec<String>,
) -> Result<Args, String> {
    let mut p = Parsing::defaults();
    for var in ENV {
        if let Some(v) = env(var.name) {
            (var.apply)(&mut p, &v)?;
        }
    }

    let mut i = 0;
    while i < argv.len() {
        let take = |i: usize| -> Result<String, String> {
            argv.get(i + 1)
                .cloned()
                .ok_or_else(|| format!("{} is missing a value", argv[i]))
        };
        let a = argv[i].as_str();
        if let Some(flag) = FLAGS.iter().find(|f| f.name == a) {
            match flag.apply {
                Apply::Switch(apply) => {
                    apply(&mut p);
                    i += 1;
                }
                Apply::Value(apply) => {
                    apply(&mut p, a, &take(i)?)?;
                    i += 2;
                }
            }
            continue;
        }
        // `--auto-split on` or `--auto-split=on`, and its knobs.
        if cairn_server::autosplit::AutoSplitConfig::owns(a.split('=').next().unwrap_or(a)) {
            match a.split_once('=') {
                Some((flag, v)) => {
                    p.args.auto_split.set(flag, v)?;
                    i += 1;
                }
                None => {
                    p.args.auto_split.set(a, &take(i)?)?;
                    i += 2;
                }
            }
            continue;
        }
        // `--lineage-compact-depth 2` hay `--lineage-compact-depth=2`.
        if cairn_server::lineage::LineageConfig::owns(a.split('=').next().unwrap_or(a)) {
            match a.split_once('=') {
                Some((flag, v)) => {
                    p.args.lineage.set(flag, v)?;
                    i += 1;
                }
                None => {
                    p.args.lineage.set(a, &take(i)?)?;
                    i += 2;
                }
            }
            continue;
        }
        if a == "--help" || a == "-h" {
            print_help();
            std::process::exit(0);
        }
        if a == "--version" || a == "-V" {
            println!("{}", cairn_server::version_line("cairnflow"));
            std::process::exit(0);
        }
        return Err(format!("unknown argument: {a}"));
    }
    p.finish()
}

/// The arguments being read: [`Args`] plus the two sources of the main store, resolved
/// in [`Parsing::finish`].
struct Parsing {
    main_store: Option<String>,
    data_dir: Option<PathBuf>,
    /// `main_store` here is not used until `finish`.
    args: Args,
}

impl Parsing {
    fn defaults() -> Self {
        Self {
            main_store: None,
            data_dir: None,
            args: Args {
                main_store: String::new(),
                wal_store: None,
                wal_atomic_put: None,
                disk_cache_dir: None,
                disk_cache_bytes: cairn_store::disk_cache::DEFAULT_MAX_BYTES,
                cluster: false,
                node_id: None,
                lease_ttl_ms: 10_000,
                reader_poll_ms: 500,
                deploy_refresh_ms: 30_000,
                partitions: 1,
                port: 8080,
                host: "127.0.0.1".into(),
                advertise_host: None,
                tick_interval_ms: 200,
                // Default 24 hours, not forever: on an object store, every invocation
                // that ever ran is a key living forever in compaction — the bucket grows
                // with *history* rather than with load, and self-hosters find out through
                // the bill.
                retention_ms: 24 * 60 * 60 * 1000,
                max_concurrent_turns: cairn_server::actor::DEFAULT_MAX_CONCURRENT_TURNS,
                inactivity_timeout_ms: cairn_server::actor::DEFAULT_SDK_TIMEOUT.as_millis() as u64,
                inactivity_timeout_max_ms: cairn_server::discovery::INACTIVITY_TIMEOUT_MAX_MS,
                max_turns_per_tenant: 0,
                replay_ram_budget_bytes: cairn_server::replay_budget::DEFAULT_REPLAY_RAM_BUDGET,
                tenant_defaults: cairn_engine::TenantLimit::default(),
                key_rate: None,
                queue_wait_ms: cairn_server::turn::DEFAULT_QUEUE_WAIT.as_millis() as u64,
                l0_soft: cairn_server::l0::DEFAULT_L0_SOFT,
                l0_max_reject: cairn_server::l0::DEFAULT_L0_MAX_REJECT,
                invoke_wait_ms: 5_000,
                attach_max_wait_ms: cairn_server::node::DEFAULT_ATTACH_MAX_WAIT.as_millis() as u64,
                http_header_timeout_ms: cairn_server::node::DEFAULT_HTTP_HEADER_TIMEOUT.as_millis()
                    as u64,
                query_isolation: QueryIsolation::Auto,
                query_worker: None,
                query_worker_memory_mb: None,
                non_dumpable: false,
                max_open_streams_per_tenant: cairn_server::streams::StreamConfig::default()
                    .tenant_max,
                schedule_min_period_ms: cairn_server::node::DEFAULT_SCHEDULE_MIN_PERIOD.as_millis()
                    as u64,
                slow_log_ms: cairn_server::slowlog::DEFAULT_THRESHOLD.as_millis() as u64,
                flush_interval_ms: cairn_store::DEFAULT_FLUSH_INTERVAL.as_millis() as u64,
                engine_threads: 0,
                node_wal: cairn_store::NodeWalConfig::default(),
                wal_window_ms: None,
                shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT.as_millis() as u64,
                internal_port: None,
                no_auth: false,
                cluster_secret: None,
                profile: None,
                deploy_require_identity: false,
                deploy_transport: Default::default(),
                deploy_https_request_response: false,
                deploy_no_rr_compression: false,
                deploy_require_compression: false,
                values: Default::default(),
                no_ui: false,
                ui_base_path: cairn_server::ui::DEFAULT_BASE_PATH.into(),
                ui_cookie_secure: None,
                egress: Default::default(),
                identity_master: None,
                ui_env_label: None,
                trust_proxy: Default::default(),
                gc_retired_dbs: None,
                gc_every_s: cairn_server::gc::DEFAULT_EVERY.as_secs(),
                gc_min_age_s: cairn_server::gc::DEFAULT_MIN_AGE.as_secs(),
                retire_orphans_every_s: cairn_server::gc::DEFAULT_RETIRE_ORPHANS_EVERY.as_secs(),
                reconcile_intents_every_s: cairn_server::split::DEFAULT_RECONCILE_INTENTS_EVERY
                    .as_secs(),
                scrub_every_s: cairn_server::scrub::DEFAULT_EVERY.as_secs(),
                scrub_requests_per_hour: None,
                trash_keep_s: cairn_store::trash::DEFAULT_KEEP.as_secs(),
                value_sweep_every_s: cairn_server::value_sweep::DEFAULT_EVERY.as_secs(),
                value_min_age_s: cairn_server::value_sweep::DEFAULT_MIN_AGE.as_secs(),
                tenant_purge_keep_s: cairn_store::tenant_purge::KEEP.as_secs(),
                no_archive: false,
                no_rr_delta_replay: false,
                user_otlp: Default::default(),
                archive_artifacts: false,
                auto_split: cairn_server::autosplit::AutoSplitConfig::default(),
                lineage: cairn_server::lineage::LineageConfig::default(),
                slow_shed: true,
                quarantine: cairn_server::quarantine::QuarantineConfig::default(),
                quiet_lease: cairn_store::quiet_lease::Bootstrap::default(),
            },
        }
    }

    fn finish(self) -> Result<Args, String> {
        // `--main-store` wins over `--data-dir` when both are given: it is the general
        // form, and silently picking the other would put the data where the person typing
        // did not intend.
        let main_store = match (self.main_store, &self.data_dir) {
            (Some(uri), _) => uri,
            (None, Some(dir)) => data_dir_uri(dir)?,
            (None, None) => data_dir_uri(&PathBuf::from("./cairnflow-data"))?,
        };
        let mut args = Args {
            main_store,
            ..self.args
        };
        if args.profile == Some(Profile::Saas) {
            apply_saas(&mut args)?;
        }
        // Requiring SDKs to declare compression while dropping compression for every new
        // deployment: every `REQUEST_RESPONSE` registration would be uncompressed, and the
        // compression rule would mean nothing.
        if args.deploy_require_compression && args.deploy_no_rr_compression {
            return Err(
                "--deploy-require-compression and --deploy-no-rr-compression cannot be used together (profile saas turns on the first)"
                    .to_string(),
            );
        }
        Ok(args)
    }
}

/// The profile of `--profile`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Profile {
    /// A cell shared by many tenants.
    Saas,
}

/// The default memory cap of each `/query` child under `--profile saas`.
const SAAS_QUERY_WORKER_MEMORY_MB: u64 = 256;

/// `--query-isolation`: whether a caller's `/query` statement runs in a
/// `cairn-query-worker` child process or inside the node process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryIsolation {
    /// A child process when the worker binary is found, otherwise in place.
    Auto,
    /// Always a child process; a missing worker binary ⇒ refuse to start.
    Process,
    /// Inside the node process (the behaviour before query isolation existed).
    Inline,
}

fn query_isolation(flag: &str, v: &str) -> Result<QueryIsolation, String> {
    match v {
        "auto" => Ok(QueryIsolation::Auto),
        "process" => Ok(QueryIsolation::Process),
        "inline" => Ok(QueryIsolation::Inline),
        _ => Err(format!("{flag} must be auto, process or inline")),
    }
}

/// The `/query` child process configuration per `--query-isolation` (at startup — needs
/// the file system). `process` without the binary found ⇒ refuse to start.
fn resolve_query_worker(
    a: &Args,
) -> Result<Option<cairn_server::query::QueryWorkerConfig>, String> {
    let cfg = |path| cairn_server::query::QueryWorkerConfig {
        path,
        memory_bytes: a
            .query_worker_memory_mb
            .unwrap_or(cairn_server::query::DEFAULT_WORKER_MEMORY_MB)
            << 20,
    };
    match a.query_isolation {
        QueryIsolation::Inline => Ok(None),
        QueryIsolation::Auto => Ok(query_worker_path(a).map(cfg)),
        QueryIsolation::Process => query_worker_path(a).map(cfg).map(Some).ok_or_else(|| {
            format!(
                "--query-isolation process: cannot find the {} binary (put it next to cairnflow, or pass --query-worker PATH / CAIRNFLOW_QUERY_WORKER)",
                cairn_query::worker::BINARY
            )
        }),
    }
}

/// `PR_SET_DUMPABLE 0` (Linux): `/proc/<pid>/{environ,mem,…}` owned by root, no `ptrace`
/// by the same user, no core dumps. Other systems: does nothing.
fn make_non_dumpable() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    // SAFETY: `prctl` with constant arguments.
    unsafe {
        // Read the flag back: could not set it ⇒ do not start.
        if libc::prctl(libc::PR_SET_DUMPABLE, 0) != 0 || libc::prctl(libc::PR_GET_DUMPABLE) != 0 {
            return Err(format!(
                "--profile saas: cannot make the node non-dumpable (prctl: {})",
                std::io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

/// The `/query` worker binary: `--query-worker`, or `cairn-query-worker` next to the
/// running binary.
fn query_worker_path(a: &Args) -> Option<std::path::PathBuf> {
    match &a.query_worker {
        Some(p) => Some(p.clone()),
        None => std::env::current_exe()
            .ok()?
            .parent()
            .map(|d| d.join(cairn_query::worker::BINARY)),
    }
    .filter(|p| p.is_file())
}

fn profile(flag: &str, v: &str) -> Result<Profile, String> {
    match v {
        "saas" => Ok(Profile::Saas),
        _ => Err(format!("{flag}: '{v}' — only saas is accepted")),
    }
}

/// `--profile saas`: adds no new behaviour, only turns on the full set of safe flags for
/// a shared cell and **refuses to start** when a condition is missing. Every missing
/// condition is reported at once — the operator fixes them in one go, not one restart
/// per error.
///
/// Turns on: `--egress-deny-private`, `--egress-require-https`, `--ui-cookie-secure on`,
/// `--no-user-otlp`, `--deploy-require-identity`, `--deploy-https-request-response`,
/// `--deploy-require-compression`. Requires: `CAIRNFLOW_CLUSTER_SECRET` ≥ 32 bytes (the
/// cluster secret never lives in the bucket), `CAIRNFLOW_IDENTITY_MASTER`, at least one
/// `--egress-deny-cidr` (the cell's own ingress address). Refuses: `--no-auth`,
/// `--ui-cookie-secure off`.
/// The SaaS profile's default caps for absent fields of the `tenants` registry: the
/// hosted plan sets every cap, but a missing line (a new tenant, a hand-edited registry,
/// a changed plan) must not mean "no cap" on a shared cell. `max-payload`,
/// `max-journal` follow the hosted beta plan.
fn saas_tenant_defaults() -> cairn_engine::TenantLimit {
    cairn_engine::TenantLimit {
        max_payload: Some(1 << 20),
        max_journal: Some(10_000),
        max_live: Some(1_000),
        max_deployments: Some(20),
        max_schedules: Some(50),
        max_timers: Some(1_000),
        max_streams: Some(64),
        max_state_keys: Some(10_000),
        max_artifact_bytes: Some(1 << 30),
        max_journal_bytes: Some(32 << 20),
        // The per-value logical cap of the hosted beta plan.
        max_value_bytes: Some(64 << 20),
        ..Default::default()
    }
}

/// The SaaS profile's per-key request rate cap: a burst of 1000, then 1000 requests per
/// second — enough for any real client, while stopping a key firing cheap commands
/// (`cancel`, `signal`) at the speed of a loop.
const SAAS_KEY_RATE: &str = "1000/1";

fn apply_saas(a: &mut Args) -> Result<(), String> {
    let mut missing = Vec::new();
    if a.no_auth {
        missing
            .push("--no-auth is not allowed: a shared cell always requires an API key".to_string());
    }
    if a.ui_cookie_secure == Some(false) {
        missing.push(
            "--ui-cookie-secure off is not allowed: session cookies must only travel over TLS"
                .to_string(),
        );
    }
    match a.cluster_secret.as_deref() {
        None => missing.push(format!(
            "missing CAIRNFLOW_CLUSTER_SECRET (≥ {} bytes, the same on every node): the cluster secret must not live in the bucket — generate it with `openssl rand -hex 32`",
            cairn_server::auth::MIN_ENV_SECRET
        )),
        Some(s) if s.len() < cairn_server::auth::MIN_ENV_SECRET => missing.push(format!(
            "CAIRNFLOW_CLUSTER_SECRET is too short ({} bytes, need ≥ {})",
            s.len(),
            cairn_server::auth::MIN_ENV_SECRET
        )),
        Some(_) => {}
    }
    if a.identity_master.is_none() {
        missing.push(format!(
            "missing {} (≥ 32 bytes, outside the bucket): customer endpoints must be able to verify that requests come from the cell",
            cairn_server::identity::MASTER_ENV
        ));
    }
    if a.egress.deny.is_empty() {
        missing.push(
            "missing --egress-deny-cidr: the cell's own ingress addresses (IP/CIDR), so customers cannot register an endpoint that points back into the cell"
                .to_string(),
        );
    }
    // `0` turns off the SDK wait cap: a turn hung at the edge would hold its turn slot
    // forever, with no error and no retry.
    // A tenant's SQL does not run inside the node process of a shared cell: a poisonous
    // statement only kills its own child process.
    if a.query_isolation == QueryIsolation::Inline {
        missing.push(
            "--query-isolation inline is not allowed: a tenant's SQL must run in a cairn-query-worker process, so a hostile query cannot stop the node"
                .to_string(),
        );
    }
    if a.inactivity_timeout_ms == 0 {
        missing.push(
            "--inactivity-timeout-ms 0 is not allowed: an SDK wait without a limit really hangs when the edge between the cell and the SDK stops answering (same for CAIRNFLOW_INACTIVITY_TIMEOUT_MS)"
                .to_string(),
        );
    }
    if !missing.is_empty() {
        return Err(format!(
            "--profile saas refuses to start:\n  - {}",
            missing.join("\n  - ")
        ));
    }
    a.query_isolation = QueryIsolation::Process;
    // A ~1 GB beta machine: 4 children × 256 MiB; children carry `oom_score_adj` 1000 so
    // the OOM killer takes a child before the node.
    a.query_worker_memory_mb
        .get_or_insert(SAAS_QUERY_WORKER_MEMORY_MB);
    // `/query` children run as the same user: with the node not dumpable, a child cannot
    // read `/proc/<node>/environ` (cluster secret, store keys). Self-hosting keeps the
    // debug/core dump behaviour as before.
    a.non_dumpable = true;
    a.egress.deny_private = true;
    a.egress.require_https = true;
    a.ui_cookie_secure = Some(true);
    a.user_otlp = cairn_server::otel::UserExport::Off;
    a.deploy_require_identity = true;
    a.deploy_https_request_response = true;
    a.deploy_require_compression = true;
    // Segmented values are read directly from a publicly reachable store when
    // `--value-read-path auto`.
    a.values.prefer_direct = true;
    // One tenant does not hold more than ¼ of the node's turn slots: one tenant's hung
    // endpoint or long model step does not block other tenants.
    if a.max_turns_per_tenant == 0 {
        a.max_turns_per_tenant = (a.max_concurrent_turns / 4).max(1);
    }
    // The upper cap of the `inactivityTimeout` a contract declares: 15 minutes instead of
    // 1 hour — the SDK declares 15 minutes when the user declares nothing.
    a.inactivity_timeout_max_ms = a
        .inactivity_timeout_max_ms
        .min(SAAS_INACTIVITY_TIMEOUT_MAX_MS);
    // The per-key rate cap.
    if a.key_rate.is_none() {
        a.key_rate = cairn_server::tenants::DeployRate::parse(SAAS_KEY_RATE);
    }
    // Every absent cap of the `tenants` registry has a default.
    if a.tenant_defaults.is_empty() {
        a.tenant_defaults = saas_tenant_defaults();
    }
    Ok(())
}

/// The upper cap of `inactivityTimeout` in the SaaS profile.
const SAAS_INACTIVITY_TIMEOUT_MAX_MS: u64 = 15 * 60 * 1000;

/// One environment variable: sets a default before the flags are read.
///
/// A broken number in an environment variable **is ignored** (the default is kept), not
/// reported — unlike flags. That is the long-standing behaviour; the table keeps it.
struct EnvVar {
    name: &'static str,
    apply: fn(&mut Parsing, &str) -> Result<(), String>,
}

/// Sets `slot` if `v` parses, otherwise leaves it as is.
fn lenient<T: std::str::FromStr>(slot: &mut T, v: &str) {
    if let Ok(n) = v.parse() {
        *slot = n;
    }
}

/// In exactly the old order: environment variable errors are reported in this order.
const ENV: &[EnvVar] = &[
    EnvVar {
        name: cairn_server::identity::MASTER_ENV,
        apply: |p, v| {
            // Set but short ⇒ do not start: signing with a weak secret is worse than not
            // signing (customers think they are protected).
            p.args.identity_master = Some(cairn_server::identity::Master::new(v.as_bytes())?);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_CLUSTER_SECRET",
        apply: |p, v| {
            p.args.cluster_secret = Some(v.into()).filter(|s: &String| !s.is_empty());
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_PROFILE",
        apply: |p, v| {
            p.args.profile = Some(profile("CAIRNFLOW_PROFILE", v)?);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_MAIN_STORE",
        apply: |p, v| {
            p.main_store = Some(v.into());
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_WAL_STORE",
        apply: |p, v| {
            p.args.wal_store = Some(v.into());
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_DATA_DIR",
        apply: |p, v| {
            p.data_dir = Some(PathBuf::from(v));
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_PORT",
        apply: |p, v| {
            lenient(&mut p.args.port, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_HOST",
        apply: |p, v| {
            p.args.host = v.into();
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_ADVERTISE_HOST",
        apply: |p, v| {
            p.args.advertise_host = Some(
                cairn_server::node::check_advertise_host(v)
                    .map_err(|e| format!("CAIRNFLOW_ADVERTISE_HOST {v:?}: {e}"))?,
            );
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_TICK_MS",
        apply: |p, v| {
            lenient(&mut p.args.tick_interval_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_CLUSTER",
        apply: |p, _| {
            p.args.cluster = true;
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_NODE_ID",
        apply: |p, v| {
            p.args.node_id = Some(v.into());
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_LEASE_TTL_MS",
        apply: |p, v| {
            lenient(&mut p.args.lease_ttl_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_PARTITIONS",
        apply: |p, v| {
            lenient(&mut p.args.partitions, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_READER_POLL_MS",
        apply: |p, v| {
            lenient(&mut p.args.reader_poll_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_DEPLOY_REFRESH_MS",
        apply: |p, v| {
            lenient(&mut p.args.deploy_refresh_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_RETENTION_MS",
        apply: |p, v| {
            lenient(&mut p.args.retention_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_MAX_CONCURRENT_TURNS",
        apply: |p, v| {
            lenient(&mut p.args.max_concurrent_turns, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_MAX_TURNS_PER_TENANT",
        apply: |p, v| {
            lenient(&mut p.args.max_turns_per_tenant, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_KEY_RATE",
        apply: |p, v| {
            p.args.key_rate = cairn_server::tenants::DeployRate::parse(v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_REPLAY_RAM_BUDGET_BYTES",
        apply: |p, v| {
            lenient(&mut p.args.replay_ram_budget_bytes, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_QUEUE_WAIT_MS",
        apply: |p, v| {
            lenient(&mut p.args.queue_wait_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_L0_SOFT",
        apply: |p, v| {
            lenient(&mut p.args.l0_soft, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_L0_MAX_REJECT",
        apply: |p, v| {
            lenient(&mut p.args.l0_max_reject, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_INACTIVITY_TIMEOUT_MS",
        apply: |p, v| {
            lenient(&mut p.args.inactivity_timeout_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_INACTIVITY_TIMEOUT_MAX_MS",
        apply: |p, v| {
            lenient(&mut p.args.inactivity_timeout_max_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_INVOKE_WAIT_MS",
        apply: |p, v| {
            lenient(&mut p.args.invoke_wait_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_ATTACH_MAX_WAIT_MS",
        apply: |p, v| {
            lenient(&mut p.args.attach_max_wait_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_QUERY_WORKER",
        apply: |p, v| {
            p.args.query_worker = Some(v.into());
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_HTTP_HEADER_TIMEOUT_MS",
        apply: |p, v| {
            lenient(&mut p.args.http_header_timeout_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_MAX_OPEN_STREAMS_PER_TENANT",
        apply: |p, v| {
            lenient(&mut p.args.max_open_streams_per_tenant, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_SCHEDULE_MIN_PERIOD_MS",
        apply: |p, v| {
            lenient(&mut p.args.schedule_min_period_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_SLOW_LOG_MS",
        apply: |p, v| {
            lenient(&mut p.args.slow_log_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_FLUSH_INTERVAL_MS",
        apply: |p, v| {
            lenient(&mut p.args.flush_interval_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_ENGINE_THREADS",
        apply: |p, v| {
            lenient(&mut p.args.engine_threads, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_WAL_KIND",
        apply: |_, v| check_wal_kind(v),
    },
    EnvVar {
        name: "CAIRNFLOW_SHUTDOWN_TIMEOUT_MS",
        apply: |p, v| {
            lenient(&mut p.args.shutdown_timeout_ms, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_INTERNAL_PORT",
        apply: |p, v| {
            p.args.internal_port = v.parse().ok();
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_NO_AUTH",
        apply: |p, _| {
            p.args.no_auth = true;
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_NO_UI",
        apply: |p, _| {
            p.args.no_ui = true;
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_UI_BASE_PATH",
        apply: |p, v| ui_base_path(p, "CAIRNFLOW_UI_BASE_PATH", v),
    },
    EnvVar {
        name: "CAIRNFLOW_UI_COOKIE_SECURE",
        apply: |p, v| {
            p.args.ui_cookie_secure = Some(parse_on_off("CAIRNFLOW_UI_COOKIE_SECURE", v)?);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_UI_ENV_LABEL",
        apply: |p, v| ui_env_label(p, "CAIRNFLOW_UI_ENV_LABEL", v),
    },
    EnvVar {
        name: "CAIRNFLOW_TRUST_PROXY",
        apply: |p, v| trust_proxy(p, "CAIRNFLOW_TRUST_PROXY", v),
    },
    EnvVar {
        name: "CAIRNFLOW_GC_RETIRED_DBS",
        apply: |p, v| {
            p.args.gc_retired_dbs = match v {
                "1" | "true" => Some(cairn_server::gc::GcMode::Delete),
                "plan" => Some(cairn_server::gc::GcMode::Plan),
                _ => None,
            };
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_GC_EVERY_S",
        apply: |p, v| {
            lenient(&mut p.args.gc_every_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_GC_MIN_AGE_S",
        apply: |p, v| {
            lenient(&mut p.args.gc_min_age_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_RETIRE_ORPHANS_EVERY_S",
        apply: |p, v| {
            lenient(&mut p.args.retire_orphans_every_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_RECONCILE_INTENTS_EVERY_S",
        apply: |p, v| {
            lenient(&mut p.args.reconcile_intents_every_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_TRASH_KEEP_S",
        apply: |p, v| {
            lenient(&mut p.args.trash_keep_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_TENANT_PURGE_KEEP_S",
        apply: |p, v| {
            lenient(&mut p.args.tenant_purge_keep_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_SCRUB_EVERY_S",
        apply: |p, v| {
            lenient(&mut p.args.scrub_every_s, v);
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_SCRUB_REQUESTS_PER_HOUR",
        apply: |p, v| {
            if let Ok(n) = v.parse() {
                p.args.scrub_requests_per_hour = Some(n);
            }
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_NO_ARCHIVE",
        apply: |p, _| {
            p.args.no_archive = true;
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_NO_USER_OTLP",
        apply: |p, _| {
            p.args.user_otlp = cairn_server::otel::UserExport::Off;
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_AUTO_SPLIT",
        apply: |p, v| p.args.auto_split.set("--auto-split", v),
    },
    EnvVar {
        name: "CAIRNFLOW_SLOW_SHED",
        apply: |p, v| {
            p.args.slow_shed = parse_on_off("CAIRNFLOW_SLOW_SHED", v)?;
            Ok(())
        },
    },
    EnvVar {
        name: "CAIRNFLOW_QUARANTINE",
        apply: |p, v| {
            p.args.quarantine.mode = quarantine_mode("CAIRNFLOW_QUARANTINE", v)?;
            Ok(())
        },
    },
    // An unknown value is an error, not ignored like a broken number: a typo must not
    // silently build a cluster with delegation.
    EnvVar {
        name: "CAIRNFLOW_QUIET_LEASE",
        apply: |p, v| {
            p.args.quiet_lease = quiet_lease("CAIRNFLOW_QUIET_LEASE", v)?;
            Ok(())
        },
    },
];

/// A command-line flag. `--auto-split…` is not here: it accepts the `--flag=value` form
/// too and is read by [`cairn_server::autosplit::AutoSplitConfig::set`].
struct Flag {
    name: &'static str,
    apply: Apply,
}

enum Apply {
    /// A bare flag, no value.
    Switch(fn(&mut Parsing)),
    /// A flag with exactly one value (the next cell); takes the flag name to report errors.
    Value(fn(&mut Parsing, &str, &str) -> Result<(), String>),
}

/// The number of a flag; broken ⇒ `"<flag> is not a number"`.
fn number<T: std::str::FromStr>(flag: &str, v: &str) -> Result<T, String> {
    v.parse().map_err(|_| format!("{flag} is not a number"))
}

/// Like [`number`], plus the ≥ 1 check with the error `"<flag> must be ≥ 1"`.
fn at_least_one<T: std::str::FromStr + PartialEq + From<u8>>(
    flag: &str,
    v: &str,
) -> Result<T, String> {
    let n: T = number(flag, v)?;
    if n == T::from(0) {
        return Err(format!("{flag} must be ≥ 1"));
    }
    Ok(n)
}

const FLAGS: &[Flag] = &[
    Flag {
        name: "--data-dir",
        apply: Apply::Value(|p, _, v| {
            p.data_dir = Some(PathBuf::from(v));
            Ok(())
        }),
    },
    Flag {
        name: "--main-store",
        apply: Apply::Value(|p, _, v| {
            p.main_store = Some(v.into());
            Ok(())
        }),
    },
    Flag {
        name: "--wal-store",
        apply: Apply::Value(|p, _, v| {
            p.args.wal_store = Some(v.into());
            Ok(())
        }),
    },
    Flag {
        name: "--port",
        apply: Apply::Value(|p, f, v| {
            p.args.port = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--host",
        apply: Apply::Value(|p, _, v| {
            p.args.host = v.into();
            Ok(())
        }),
    },
    Flag {
        name: "--advertise-host",
        apply: Apply::Value(|p, f, v| {
            p.args.advertise_host = Some(
                cairn_server::node::check_advertise_host(v)
                    .map_err(|e| format!("{f} {v:?}: {e}"))?,
            );
            Ok(())
        }),
    },
    Flag {
        name: "--internal-port",
        apply: Apply::Value(|p, f, v| {
            p.args.internal_port = Some(number(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--no-auth",
        apply: Apply::Switch(|p| p.args.no_auth = true),
    },
    Flag {
        name: "--no-ui",
        apply: Apply::Switch(|p| p.args.no_ui = true),
    },
    Flag {
        name: "--profile",
        apply: Apply::Value(|p, f, v| {
            p.args.profile = Some(profile(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--deploy-require-identity",
        apply: Apply::Switch(|p| p.args.deploy_require_identity = true),
    },
    Flag {
        name: "--deploy-transport",
        apply: Apply::Value(|p, f, v| {
            p.args.deploy_transport = cairn_server::discovery::TransportPolicy::parse(v)
                .ok_or_else(|| format!("{f}: '{v}' — only declared or auto is accepted"))?;
            Ok(())
        }),
    },
    Flag {
        name: "--deploy-https-request-response",
        apply: Apply::Switch(|p| p.args.deploy_https_request_response = true),
    },
    Flag {
        name: "--deploy-no-rr-compression",
        apply: Apply::Switch(|p| p.args.deploy_no_rr_compression = true),
    },
    Flag {
        name: "--deploy-require-compression",
        apply: Apply::Switch(|p| p.args.deploy_require_compression = true),
    },
    Flag {
        name: "--value-read-path",
        apply: Apply::Value(|p, f, v| {
            p.args.values.read_path = v.parse().map_err(|e| format!("{f}: {e}"))?;
            Ok(())
        }),
    },
    Flag {
        name: "--value-presign-ttl-s",
        apply: Apply::Value(|p, f, v| {
            p.args.values.ttl = Some(std::time::Duration::from_secs(number(f, v)?));
            Ok(())
        }),
    },
    Flag {
        name: "--value-public-endpoint",
        apply: Apply::Value(|p, _, v| {
            p.args.values.public_endpoint = Some(v.to_string()).filter(|s| !s.is_empty());
            Ok(())
        }),
    },
    Flag {
        name: "--value-public-url",
        apply: Apply::Value(|p, _, v| {
            p.args.values.public_url = Some(v.to_string()).filter(|s| !s.is_empty());
            Ok(())
        }),
    },
    Flag {
        name: "--egress-deny-private",
        apply: Apply::Switch(|p| p.args.egress.deny_private = true),
    },
    Flag {
        name: "--egress-require-https",
        apply: Apply::Switch(|p| p.args.egress.require_https = true),
    },
    Flag {
        name: "--egress-allow-cidr",
        apply: Apply::Value(|p, f, v| {
            p.args.egress.allow.push(cidr(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--egress-deny-cidr",
        apply: Apply::Value(|p, f, v| {
            p.args.egress.deny.push(cidr(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--ui-base-path",
        apply: Apply::Value(ui_base_path),
    },
    Flag {
        name: "--ui-cookie-secure",
        apply: Apply::Value(|p, f, v| {
            p.args.ui_cookie_secure = Some(parse_on_off(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--ui-env-label",
        apply: Apply::Value(ui_env_label),
    },
    Flag {
        name: "--trust-proxy",
        apply: Apply::Value(trust_proxy),
    },
    Flag {
        name: "--slow-shed",
        apply: Apply::Value(|p, f, v| {
            p.args.slow_shed = parse_on_off(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--quarantine",
        apply: Apply::Value(|p, f, v| {
            p.args.quarantine.mode = quarantine_mode(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--quarantine-rule",
        apply: Apply::Value(|p, f, v| {
            p.args.quarantine.rule = cairn_server::quarantine::Rule::parse(v).ok_or_else(|| {
                format!("{f}: '{v}' — format attempts/invocations/seconds, e.g. 20/5/300")
            })?;
            Ok(())
        }),
    },
    Flag {
        name: "--quarantine-ttl-s",
        apply: Apply::Value(|p, f, v| {
            p.args.quarantine.ttl = Duration::from_secs(at_least_one(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--gc-retired-dbs",
        apply: Apply::Switch(|p| p.args.gc_retired_dbs = Some(cairn_server::gc::GcMode::Delete)),
    },
    Flag {
        name: "--gc-retired-dbs=plan",
        apply: Apply::Switch(|p| p.args.gc_retired_dbs = Some(cairn_server::gc::GcMode::Plan)),
    },
    Flag {
        name: "--gc-every-s",
        apply: Apply::Value(|p, f, v| {
            p.args.gc_every_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--gc-min-age-s",
        apply: Apply::Value(|p, f, v| {
            p.args.gc_min_age_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--retire-orphans-every-s",
        apply: Apply::Value(|p, f, v| {
            p.args.retire_orphans_every_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--reconcile-intents-every-s",
        apply: Apply::Value(|p, f, v| {
            p.args.reconcile_intents_every_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--trash-keep-s",
        apply: Apply::Value(|p, f, v| {
            p.args.trash_keep_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--value-sweep-every-s",
        apply: Apply::Value(|p, f, v| {
            p.args.value_sweep_every_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--value-min-age-s",
        apply: Apply::Value(|p, f, v| {
            let s: u64 = number(f, v)?;
            let floor = 2 * cairn_store::artifact::T_PUT.as_secs();
            if s < floor {
                return Err(format!(
                    "{f}: {s} is below {floor} (2 × the PUT-to-commit deadline): a segment PUT just before a slow commit would be swept as an orphan"
                ));
            }
            p.args.value_min_age_s = s;
            Ok(())
        }),
    },
    Flag {
        name: "--tenant-purge-keep-s",
        apply: Apply::Value(|p, f, v| {
            p.args.tenant_purge_keep_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--scrub-every-s",
        apply: Apply::Value(|p, f, v| {
            p.args.scrub_every_s = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--scrub-requests-per-hour",
        apply: Apply::Value(|p, f, v| {
            p.args.scrub_requests_per_hour = Some(number(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--no-archive",
        apply: Apply::Switch(|p| p.args.no_archive = true),
    },
    Flag {
        name: "--no-rr-delta-replay",
        apply: Apply::Switch(|p| p.args.no_rr_delta_replay = true),
    },
    Flag {
        name: "--no-user-otlp",
        apply: Apply::Switch(|p| p.args.user_otlp = cairn_server::otel::UserExport::Off),
    },
    Flag {
        name: "--archive-artifacts",
        apply: Apply::Switch(|p| p.args.archive_artifacts = true),
    },
    Flag {
        name: "--tick-interval-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.tick_interval_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--cluster",
        apply: Apply::Switch(|p| p.args.cluster = true),
    },
    Flag {
        name: "--node-id",
        apply: Apply::Value(|p, _, v| {
            p.args.node_id = Some(v.into());
            Ok(())
        }),
    },
    Flag {
        name: "--partitions",
        apply: Apply::Value(|p, f, v| {
            p.args.partitions = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--lease-ttl-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.lease_ttl_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--reader-poll-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.reader_poll_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--deploy-refresh-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.deploy_refresh_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--retention-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.retention_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--max-concurrent-turns",
        apply: Apply::Value(|p, f, v| {
            p.args.max_concurrent_turns = at_least_one(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--max-turns-per-tenant",
        apply: Apply::Value(|p, f, v| {
            p.args.max_turns_per_tenant = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--key-rate",
        apply: Apply::Value(|p, f, v| {
            p.args.key_rate =
                Some(cairn_server::tenants::DeployRate::parse(v).ok_or_else(|| {
                    format!("{f} wants N/S (N requests per S seconds, both ≥ 1), got {v:?}")
                })?);
            Ok(())
        }),
    },
    Flag {
        name: "--replay-ram-budget-bytes",
        apply: Apply::Value(|p, f, v| {
            p.args.replay_ram_budget_bytes = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--queue-wait-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.queue_wait_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--l0-soft",
        apply: Apply::Value(|p, f, v| {
            p.args.l0_soft = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--l0-max-reject",
        apply: Apply::Value(|p, f, v| {
            let r: f64 = number(f, v)?;
            if !(0.0..1.0).contains(&r) {
                return Err(format!(
                    "{f} must be in [0, 1): the throttle never blocks everything"
                ));
            }
            p.args.l0_max_reject = r;
            Ok(())
        }),
    },
    Flag {
        name: "--inactivity-timeout-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.inactivity_timeout_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--inactivity-timeout-max-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.inactivity_timeout_max_ms = at_least_one(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--slow-log-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.slow_log_ms = at_least_one(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--invoke-wait-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.invoke_wait_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--schedule-min-period-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.schedule_min_period_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--attach-max-wait-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.attach_max_wait_ms = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--query-isolation",
        apply: Apply::Value(|p, f, v| {
            p.args.query_isolation = query_isolation(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--query-worker",
        apply: Apply::Value(|p, _, v| {
            p.args.query_worker = Some(v.into());
            Ok(())
        }),
    },
    Flag {
        name: "--query-worker-memory-mb",
        apply: Apply::Value(|p, f, v| {
            p.args.query_worker_memory_mb = Some(at_least_one(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--http-header-timeout-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.http_header_timeout_ms = at_least_one(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--max-open-streams-per-tenant",
        apply: Apply::Value(|p, f, v| {
            p.args.max_open_streams_per_tenant = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--flush-interval-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.flush_interval_ms = at_least_one(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--engine-threads",
        apply: Apply::Value(|p, f, v| {
            p.args.engine_threads = number(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--wal-kind",
        apply: Apply::Value(|_, _, v| check_wal_kind(v)),
    },
    Flag {
        name: "--wal-stripes",
        apply: Apply::Value(|p, f, v| {
            p.args.node_wal.stripes = positive(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--wal-inflight",
        apply: Apply::Value(|p, f, v| {
            p.args.node_wal.inflight = positive(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--wal-linger-us",
        apply: Apply::Value(|p, f, v| {
            let us: u64 = number(f, v)?;
            let max = cairn_store::MAX_LINGER;
            if us > max.as_micros() as u64 {
                return Err(format!(
                    "{f} must be ≤ {} ({} ms)",
                    max.as_micros(),
                    max.as_millis()
                ));
            }
            p.args.node_wal.linger = Duration::from_micros(us);
            Ok(())
        }),
    },
    Flag {
        name: "--wal-window-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.wal_window_ms = Some(positive(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--wal-object-cap-bytes",
        apply: Apply::Value(|p, f, v| {
            p.args.node_wal.object_cap = positive(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--wal-age-flush-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.node_wal.frame_age_flush = Duration::from_millis(positive(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--wal-atomic-put",
        apply: Apply::Value(|p, f, v| {
            p.args.wal_atomic_put = Some(match v {
                "true" => true,
                "false" => false,
                _ => return Err(format!("{f}: '{v}' — only true or false is accepted")),
            });
            Ok(())
        }),
    },
    Flag {
        name: "--disk-cache-dir",
        apply: Apply::Value(|p, f, v| {
            if v.is_empty() {
                return Err(format!("{f}: empty path"));
            }
            p.args.disk_cache_dir = Some(v.into());
            Ok(())
        }),
    },
    Flag {
        name: "--disk-cache-bytes",
        apply: Apply::Value(|p, f, v| {
            p.args.disk_cache_bytes = positive(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--quiet-lease",
        apply: Apply::Value(|p, f, v| {
            p.args.quiet_lease = quiet_lease(f, v)?;
            Ok(())
        }),
    },
    Flag {
        name: "--wal-put-timeout-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.node_wal.put_timeout = Duration::from_millis(positive(f, v)?);
            Ok(())
        }),
    },
    Flag {
        name: "--shutdown-timeout-ms",
        apply: Apply::Value(|p, f, v| {
            p.args.shutdown_timeout_ms = positive(f, v)?;
            Ok(())
        }),
    },
];

/// The UI base path is checked right when the flag is read: wrong means the node does not start.
fn ui_base_path(p: &mut Parsing, flag: &str, v: &str) -> Result<(), String> {
    cairn_server::ui::check_base_path(v).map_err(|e| format!("{flag}: {e}"))?;
    p.args.ui_base_path = v.into();
    Ok(())
}

fn ui_env_label(p: &mut Parsing, flag: &str, v: &str) -> Result<(), String> {
    cairn_server::ui::check_env_label(v).map_err(|e| format!("{flag}: {e}"))?;
    p.args.ui_env_label = Some(v.into());
    Ok(())
}

fn trust_proxy(p: &mut Parsing, flag: &str, v: &str) -> Result<(), String> {
    let t = cairn_server::auth::TrustedProxies::parse(v).map_err(|e| format!("{flag}: {e}"))?;
    p.args.trust_proxy.extend(t);
    Ok(())
}

fn quiet_lease(flag: &str, v: &str) -> Result<cairn_store::quiet_lease::Bootstrap, String> {
    cairn_store::quiet_lease::Bootstrap::parse(v)
        .ok_or_else(|| format!("{flag}: '{v}' — only off, solo or delegation is accepted"))
}

fn quarantine_mode(flag: &str, v: &str) -> Result<cairn_server::quarantine::Mode, String> {
    cairn_server::quarantine::Mode::parse(v)
        .ok_or_else(|| format!("{flag}: '{v}' — only off, observe, cooldown or hold is accepted"))
}

/// A CIDR (`10.0.0.0/8`, `fd00::/8`) or a bare IP (≡ `/32`, `/128`).
fn cidr(flag: &str, v: &str) -> Result<ipnet::IpNet, String> {
    v.parse::<ipnet::IpNet>()
        .or_else(|_| v.parse::<std::net::IpAddr>().map(ipnet::IpNet::from))
        .map_err(|_| format!("{flag}: '{v}' is not a CIDR or IP"))
}

/// `--help`/`-h`.
fn print_help() {
    println!(
        "cairnflow [--data-dir DIR | --main-store URI [--wal-store URI]]\n\
         \x20         [--node-id ID] [--lease-ttl-ms N]\n\
         \x20         [--port N] [--internal-port N] [--host ADDR] [--advertise-host ADDR]\n\
         \x20         [--tick-interval-ms N]\n\
         \x20         [--no-auth] [--no-ui] [--ui-base-path /ui] [--gc-retired-dbs[=plan]]\n\
         \x20         [--egress-deny-private] [--egress-require-https]\n\
         \x20         [--egress-allow-cidr CIDR]... [--egress-deny-cidr CIDR]...\n\
         \n\
         EGRESS (requests made on behalf of a tenant: discovery, SDK calls, test replay).\n\
         Redirects are not followed and HTTP(S)_PROXY is not read. --egress-deny-private\n\
         blocks loopback, private networks, link-local/metadata, CGNAT, special ranges\n\
         and IPv4-embedded IPv6 (checks the URL, every IP DNS returns, and the socket's\n\
         real peer). --egress-allow-cidr opens one network on purpose; --egress-deny-cidr\n\
         (the cluster's own ingress/ALB addresses) wins over everything, even with\n\
         deny-private off. --egress-require-https accepts only https://. Refused ⇒\n\
         400 egress_denied. Forwarding to the leader and asking sibling nodes do not go\n\
         through this policy.\n\
         \n\
         SAAS PROFILE (CAIRNFLOW_PROFILE=saas): --profile saas turns on\n\
         --egress-deny-private, --egress-require-https, --ui-cookie-secure on,\n\
         --no-user-otlp, --deploy-require-identity, --deploy-https-request-response,\n\
         --deploy-require-compression;\n\
         requires CAIRNFLOW_CLUSTER_SECRET (≥ 32 bytes, never stored in the bucket),\n\
         CAIRNFLOW_IDENTITY_MASTER and at least one --egress-deny-cidr; refuses\n\
         --no-auth, --ui-cookie-secure off, --inactivity-timeout-ms 0.\n\
         A missing requirement ⇒ the node does not start. --deploy-require-identity\n\
         (usable on its own): POST /deployments refuses SDKs that do not declare\n\
         requestIdentity: v1. --deploy-require-compression (usable on its own): POST\n\
         /deployments refuses REQUEST_RESPONSE endpoints whose SDK does not declare\n\
         requestCompression: gzip (400 compression_required); not together with\n\
         --deploy-no-rr-compression.\n\
         \n\
         LARGE VALUES (wire v6 SDKs): --value-read-path auto|direct|proxy|inline\n\
         (default auto). Segmented state values go to the SDK as references plus a\n\
         short-lived URL; the SDK fetches the bytes itself. direct: a presigned GET of\n\
         the s3:// main store (--value-public-endpoint: the store endpoint the app\n\
         sees). proxy: <--value-public-url>/values/v1/<token>, served by any node of\n\
         the cluster (default: the bound ingress address when it is not 0.0.0.0).\n\
         inline: the node assembles values as for older SDKs. auto: direct with\n\
         --value-public-endpoint (or under --profile saas) on s3://, else proxy with\n\
         --value-public-url, else inline. --value-presign-ttl-s N (default 60, max\n\
         3600) bounds every URL.\n\
         \n\
         TRANSPORT MODE: --deploy-transport declared|auto\n\
         (default declared: the mode the SDK declares). auto: when the SDK declares\n\
         BIDI_STREAM, probe /discover over h2 once at registration; if h2 cannot reach\n\
         the SDK (a proxy that only speaks h1) ⇒ the deployment is pinned to\n\
         REQUEST_RESPONSE. POST /deployments {{\"transport\": \"auto\"}} picks it for one\n\
         registration.\n\
         --deploy-https-request-response: an https:// endpoint that declares\n\
         BIDI_STREAM is registered as REQUEST_RESPONSE when the request does not name a\n\
         transport (TLS edges usually speak h1 to the app and buffer bodies — BIDI\n\
         hangs there); {{\"transport\": \"declared\"}} keeps BIDI_STREAM for platforms that\n\
         speak h2 all the way to the app.\n\
         \n\
         REQUEST IDENTITY towards the SDK: CAIRNFLOW_IDENTITY_MASTER (≥ 32 bytes,\n\
         outside the bucket) ⇒ every /discover and /invoke carries x-cairn-jwt-v1\n\
         signed with Ed25519 by the tenant's key (derived from the master). Public\n\
         keys: GET /identity. Rotate: cairn-ctl identity publish|activate|retire\n\
         <tenant>. Unset ⇒ no signing.\n\
         \n\
         WEB CONSOLE at <url>/ui/ (static files embedded in the binary, loaded without\n\
         a key; data still needs a key). --ui-base-path P (CAIRNFLOW_UI_BASE_PATH)\n\
         changes /ui; X-Forwarded-Prefix is not read — behind a proxy with a prefix,\n\
         set this flag to match. --no-ui (CAIRNFLOW_NO_UI) turns it off. The console\n\
         signs in with an API key once and exchanges it for a 12-hour HttpOnly session\n\
         cookie signed with the cluster secret; --ui-cookie-secure on|off\n\
         (CAIRNFLOW_UI_COOKIE_SECURE) sets the Secure flag — on by default when --host\n\
         is not loopback (behind a TLS proxy).\n\
         --ui-env-label L (CAIRNFLOW_UI_ENV_LABEL) shows an environment badge in the\n\
         console's top bar (e.g. prod, staging) — 1..24 ASCII characters.\n\
         --trust-proxy CIDR[,CIDR] (CAIRNFLOW_TRUST_PROXY, repeatable): peers in these\n\
         ranges are trusted TLS proxies — X-Forwarded-Host/-Proto are used for the\n\
         Origin check and the Secure flag, X-Forwarded-For for per-IP sign-in limits.\n\
         Unset ⇒ no X-Forwarded-* header is trusted.\n\
         \n\
         AUTH (on by default). Every route except /health requires `Authorization:\n\
         Bearer <key>`. Bootstrap key: the CAIRNFLOW_ADMIN_KEY variable (role\n\
         cluster-admin, tenant default, stored nowhere); create other keys with POST\n\
         /admin/keys {{tenant, role}} where role is invoke | view | admin | provision |\n\
         cluster-admin (view and provision need the `cairn-ctl cluster\n\
         enable-key-roles` gate once, otherwise 409 key_roles_disabled). No key at all\n\
         ⇒ the node does not start, unless --no-auth (CAIRNFLOW_NO_AUTH) — dev only.\n\
         Metrics-only token: CAIRNFLOW_METRICS_KEY (≥ 32 bytes, different from the\n\
         bootstrap key) — role metrics, GET /metrics only, for Prometheus.\n\
         Hash-only keys (the node keeps no plaintext):\n\
         CAIRNFLOW_ADMIN_KEY_SHA256 (64 hex = sha256 of the cluster-admin key, instead\n\
         of CAIRNFLOW_ADMIN_KEY — do not set both) and\n\
         CAIRNFLOW_PROVISION_KEY_SHA256 (control plane key: role provision, tenant\n\
         default, not in the registry, cannot sign in to the console). Each key has its\n\
         own secret; a malformed or duplicate hash ⇒ the node does not start.\n\
         Rotate/revoke: set the new hash and restart every node.\n\
         \n\
         --internal-port N port for other nodes (/internal/*: cross-range messages,\n\
         range handover), default --port + 1 (0 if --port 0). Do not expose it outside\n\
         the cluster network. Every request there is HMAC-signed with the cluster\n\
         secret: the CAIRNFLOW_CLUSTER_SECRET variable (≥ 32 bytes, the same on every\n\
         node), or — when unset — the auth/cluster-secret object in the bucket, created\n\
         by the first node.\n\
         \n\
         URI: file:///some/path · memory:// · s3://bucket/prefix\n\
         --wal-store moves the WAL tier to another store (proposal D7). The prefix\n\
         follows --main-store: SlateDB puts the WAL at <root>/wal/ on the WAL store.\n\
         \n\
         Every node runs the lease tier: several nodes pointing at one bucket give each\n\
         range one leader; other nodes answer 503 NotLeader with directions; a single\n\
         node is a one-node cluster. Several nodes need a store that can CAS on etag\n\
         (S3-compatible). file:// (--data-dir) runs only ONE node: the node holds an\n\
         exclusive lock on the directory. --cluster (CAIRNFLOW_CLUSTER) is still\n\
         accepted but has no effect. --lease-ttl-ms N (default 10000).\n\
         \n\
         --quiet-lease off|solo|delegation (CAIRNFLOW_QUIET_LEASE) the quiet renewal\n\
         gate the node writes when it CREATES A NEW BUCKET: delegation (default) = a\n\
         single node renews with GET, a multi-node cluster delegates renewal to the\n\
         coordinator; solo = single node only; off = renew with PUT as before. Not\n\
         enough evidence yet that the gate is done (cluster status prints what is\n\
         missing). Bucket that already has a cluster: ignored — change it with\n\
         cairn-ctl cluster enable-/disable-quiet-lease.\n\
         \n\
         --partitions N fixes the partition count. Written to the bucket at the first\n\
         initialization and CANNOT be changed by the flag afterwards (change it with\n\
         split/merge).\n\
         \n\
         --retention-ms N keeps finished invocations for N milliseconds, then cleans\n\
         them up (journal + state + idempotency key). Default 86400000 (24h);\n\
         0 = keep forever. This is also the idempotency key dedupe window.\n\
         A handler that declares journalRetention in its manifest overrides it.\n\
         Before cleanup, the journal + final state are saved as a compressed object\n\
         archive/<tenant>/<yyyy-mm-dd>/<id> (GET /archive/{{id}}); --no-archive\n\
         (CAIRNFLOW_NO_ARCHIVE) turns archiving off.\n\
         \n\
         INCREMENTAL REPLAY (wire v5): an SDK that declares replayCache keeps the\n\
         journal of running invocations; REQUEST_RESPONSE turns send only new entries,\n\
         the SDK compares a digest and answers 409 on mismatch, and the turn is resent\n\
         in full. --no-rr-delta-replay turns it off (every turn replays in full, like\n\
         wire v4).\n\
         \n\
         OTLP: build with --features otel and set CAIRNFLOW_OTEL_ENDPOINT ⇒ exports\n\
         system spans, plus user spans ($trace) and logs ($log).\n\
         --no-user-otlp (CAIRNFLOW_NO_USER_OTLP) exports system spans only — for shared\n\
         cells, where the operator's collector must not receive data written by\n\
         customers.\n\
         \n\
         --gc-retired-dbs turns on the pass that deletes Dbs of retired ranges (after\n\
         split/merge) that no live range references any more; only the leader of the\n\
         lowest range runs it. Off by default. --gc-retired-dbs=plan only logs the\n\
         plan. --gc-every-s N interval (default 600); --gc-min-age-s N minimum\n\
         tombstone age (default 1800). GET /admin/gc/plan shows the plan;\n\
         PUT /admin/gc/barriers/{{id}} {{holder, protects, ttlMs, reason}} keeps the Dbs of\n\
         some ranges (or \"all\") for a bounded time.\n\
         --retire-orphans-every-s N safety net of the pass that writes tombstones on\n\
         behalf of ranges that left the table when their owner died before writing\n\
         one, and cleans up orphan children of a rollback (default 3600; 0 = off). The\n\
         pass runs right away when nudged: a split/merge just finished or rolled back,\n\
         a session just ended, a node just became the runner. Independent of\n\
         --gc-retired-dbs: this pass deletes no retired Db.\n\
         --reconcile-intents-every-s N interval of the intent reconcile pass: rolls\n\
         back unfinished splits/merges that no coordinator holds any more (two\n\
         consecutive passes; default 15; 0 = off).\n\
         Every cairn data deletion path (D-d, split/merge rollback, dead shared WAL\n\
         logs) moves objects to <prefix>/trash/<date>/<id>/ instead of deleting;\n\
         --trash-keep-s N keeps them N seconds (default 604800 = 7 days), then the D-d\n\
         runner deletes them for real after checking again (0 = never delete).\n\
         cairn-ctl trash list|restore lists and restores them.\n\
         --value-sweep-every-s N interval of the sweep that moves segmented-value\n\
         objects without a holder to the trash (default 3600; 0 = off).\n\
         --value-min-age-s N a segment younger than N seconds is never touched\n\
         (default 3600, at least 600).\n\
         --tenant-purge-keep-s N a tenant in deleting (PUT /admin/tenants/{{t}}/state)\n\
         stays reversible for N seconds, then the D-d runner deletes its rows, objects\n\
         and configuration for real (default 604800 = 7 days; 0 = never delete).\n\
         --scrub-every-s N state scrubber interval (default 600; 0 = off): verify runs\n\
         in the background on the D-d runner, dry — only the cairnflow_scrub_findings\n\
         metric and logs, it repairs nothing. This is the fastest interval:\n\
         --scrub-requests-per-hour N caps the scrubber's object store requests; a round\n\
         costing C requests is followed by a pause of at least C/N hours, so the\n\
         interval stretches as the number of ranges grows and every round stays\n\
         complete (default: the cost of a round over 64 ranges every\n\
         --scrub-every-s, 6000 at 600 s; 0 = no cap).\n\
         \n\
         --max-concurrent-turns N number of client-initiated turns running at once on\n\
         the whole node (default 32), split evenly across the ranges the node leads.\n\
         Each turn is one connection to your deployment; lower it if the SDK cannot\n\
         cope, raise it if handlers mostly wait on I/O. Background work (timers,\n\
         retries, recovery) runs sequentially on its own and does not count towards\n\
         this cap.\n\
         --max-turns-per-tenant N cap on turns of ONE tenant running at once on the\n\
         whole node (default 0 = no cap; --profile saas: a quarter of\n\
         --max-concurrent-turns, at least 1). A tenant whose endpoint hangs, or whose\n\
         steps run long, holds at most its share; other tenants keep getting turns.\n\
         --key-rate N/S        cap requests of ONE API key (or SSO user) on this node:\n\
         bursts of N, then one every S/N seconds (default: no cap; --profile saas:\n\
         1000/1). Over the cap: 429 key_rate_limited with Retry-After.\n\
         --replay-ram-budget-bytes N  journal bytes the running SDK turns of this\n\
         node may hold in RAM at once (default 536870912 = 512 MiB, 0 = no budget).\n\
         A turn that does not fit is rescheduled after 1 s without calling your\n\
         deployment; its attempt is not counted. One tenant holds at most a quarter\n\
         (its first turn always runs); turns with journals up to min(1 MiB, N/64)\n\
         always run. A turn always runs when nothing else holds the budget.\n\
         \n\
         --queue-wait-ms N     limit on waiting for a slot in the queue; when it runs\n\
         \x20                     out the client gets 429 and the turn is rescheduled (0 = wait forever)\n\
         --l0-soft N           soft throttle before the L0 wall: from N L0 views (cap 8)\n\
         \x20                     refuses NEW work with some probability via 429 range_overloaded,\n\
         \x20                     without creating an invocation (default 5; 0 = off)\n\
         --l0-max-reject P     maximum refusal probability, at 7 views or more (default 0.9)\n\
         --inactivity-timeout-ms N drops a turn if the SDK makes no progress within\n\
         N milliseconds (default 60000; 0 = off). A dropped turn is rescheduled with\n\
         backoff, the journal is NOT lost. BIDI_STREAM restarts the timer on every\n\
         frame; REQUEST_RESPONSE has no signal mid-turn, so it is a cap on the whole\n\
         turn. The only legitimate silence within a turn is a ctx.run closure —\n\
         ctx.sleep and ctx.call end the turn instead of holding the connection — so\n\
         this really is 'the longest ctx.run closure you allow'. Longer work belongs in\n\
         a separate invocation started with ctx.send.\n\
         --inactivity-timeout-max-ms N upper bound for the inactivityTimeout a\n\
         handler declares (default 3600000 = one hour; --profile saas: at most 900000\n\
         = 15 minutes). A longer declared value is clamped to it.\n\
         \n\
         --invoke-wait-ms N POST /invoke waits up to N milliseconds for the invocation\n\
         to stop before answering (default 5000; 0 = answer right after the first\n\
         turn). When it runs out: 202 with the id — 202 can still happen, but it no\n\
         longer depends on which partition the callee hashes to. To fire and forget,\n\
         append /send to the invoke path: it answers as soon as the invocation is\n\
         durable, without waiting for it to run.\n\
         \n\
         --attach-max-wait-ms N cap of ?wait_ms= on POST\n\
         /invocations/{{id}}/attach and /workflow/.../attach (default 60000,\n\
         env CAIRNFLOW_ATTACH_MAX_WAIT_MS). Without wait_ms, attach waits like\n\
         --invoke-wait-ms.\n\
         \n\
         --query-isolation MODE  where POST /query runs SQL the caller wrote: process\n\
         (a cairn-query-worker child process per query, killed at the query deadline\n\
         or its memory limit — a hostile query cannot stop the node), inline (inside\n\
         the node process), or auto (default: process when cairn-query-worker sits\n\
         next to this binary, else inline with a warning). --profile saas requires\n\
         process.\n\
         --query-worker PATH   the cairn-query-worker binary (env CAIRNFLOW_QUERY_WORKER;\n\
         default: next to this binary).\n\
         --query-worker-memory-mb N  memory limit of each query worker process\n\
         (default 512; --profile saas: 256); a query over it gets 422 query_budget.\n\
         \n\
         --http-header-timeout-ms N  limit on reading HTTP headers on both ports\n\
         (default 75000, env CAIRNFLOW_HTTP_HEADER_TIMEOUT_MS). Also the idle limit of\n\
         keep-alive connections: set it higher than the idle timeout of the proxy in\n\
         front.\n\
         \n\
         --max-open-streams-per-tenant N  cap on long-lived connections (SSE streams,\n\
         waiting attaches) of one tenant on the node (default 256; 0 = no cap; env\n\
         CAIRNFLOW_MAX_OPEN_STREAMS_PER_TENANT). Over it ⇒ 429\n\
         too_many_open_streams.\n\
         \n\
         --schedule-min-period-ms N  smallest period of an everyMs schedule in\n\
         PUT /schedules/{{name}} (default 10000, env\n\
         CAIRNFLOW_SCHEDULE_MIN_PERIOD_MS).\n\
         \n\
         --slow-log-ms N    runs longer than N milliseconds leave a slow log line\n\
         (target cairnflow::slow_log; default 5000, env\n\
         CAIRNFLOW_SLOW_LOG_MS). A tenant sets its own threshold with\n\
         PUT /admin/tenants/{{tenant}} {{\"slowMs\": n}}. Cap 10 lines/s.\n\
         \n\
         --quarantine off|observe|cooldown|hold quarantines poison handlers (env\n\
         CAIRNFLOW_QUARANTINE; default observe: only logs, metrics, /stats).\n\
         cooldown = concurrency cap of 1 per range; hold = NEW invocations start paused\n\
         576 (never cancelled). A tenant sets its own with PUT /admin/tenants/\n\
         {{tenant}} {{\"quarantine\": \"hold\"}}. Lifted by: expiry, a new deployment,\n\
         DELETE /admin/quarantine/{{tenant}}/{{service}}/{{handler}}.\n\
         --quarantine-rule A/I/S  rule: >= A failed attempts across >= I invocations,\n\
         0 successes within S seconds (default 20/5/300).\n\
         --quarantine-ttl-s N  new entries live N seconds (default 3600).\n\
         \n\
         --flush-interval-ms N cut window of the per-node shared WAL (default\n\
         10; --wal-window-ms overrides): the minimum gap between two WAL object cuts of\n\
         one stream — not SlateDB's flush interval, which is not on the commit path\n\
         with the shared WAL. On fast stores (memory://, file://) it is the latency\n\
         floor of back-to-back commits; on S3 a PUT (tens of ms) is longer than the\n\
         window, so it hardly binds. Smaller ⇒ faster back-to-back commits but, under\n\
         heavy load, more WAL objects (at most one PUT per window per stream) — on S3\n\
         those are billed requests. Measure with bench/latency-floor before\n\
         tuning.\n\
         \n\
         --engine-threads N number of threads running the engines of all ranges\n\
         (default 0 = number of cores). Each thread runs many ranges; a range lives\n\
         entirely on one thread. More than the number of cores adds no CPU, only\n\
         context switches.\n\
         \n\
         WAL: every range uses the per-node shared WAL — WAL PUTs scale with nodes,\n\
         not ranges. SlateDB's native WAL is gone: native-WAL ranges of an old bucket\n\
         are flushed and moved to the shared WAL when opened (migration step\n\
         wal/shared; old binaries cannot run on that bucket again). --wal-kind shared\n\
         (or CAIRNFLOW_WAL_KIND) is still accepted and changes nothing; --wal-kind\n\
         slatedb is refused at startup, before touching the bucket. Shared WAL knobs:\n\
         --wal-stripes N (S, default 2), --wal-inflight N\n\
         (W, PUTs in flight per stream, default 1), --wal-window-ms N (default from\n\
         --flush-interval-ms), --wal-object-cap-bytes N (default 1 MiB),\n\
         --wal-age-flush-ms N (T, default 60000), --wal-linger-us N (L, default\n\
         1000: the first row after an idle spell waits L more before the cut, batching\n\
         commits that arrive close together — fewer PUTs under light load; never cuts\n\
         more often than the window; 0 = cut immediately as before; cap 100000),\n\
         --wal-put-timeout-ms N (default 2000; the node fails stop after 3 timeouts).\n\
         --wal-atomic-put true|false declares that WAL store PUTs are atomic: true\n\
         means an ambiguous log PUT is resolved by reading back; a store that can tear\n\
         objects (MinIO EC, quorum) must set false. Without the flag: memory:// and\n\
         file:// are true, unknown schemes are false; s3:// is true only on verified\n\
         stores (AWS S3, Tigris, SeaweedFS — detected by endpoint or Server header);\n\
         for any other s3:// store (MinIO, Garage, Ceph...) the node refuses to start\n\
         until the flag is set.\n\
         \n\
         --disk-cache-dir DIR turns on the node's SST disk cache (off by default):\n\
         copies of every range's SSTs under DIR, capped by --disk-cache-bytes N\n\
         (default 16 GiB). WAL and manifests never go there. DIR holds customer data:\n\
         use an encrypted volume and wipe it when the node leaves the cluster\n\
         (docs/self-hosting.md).\n\
         \n\
         --shutdown-timeout-ms N cap of a graceful shutdown (default 20000).\n\
         SIGTERM/SIGINT ⇒ the node stops accepting connections, seals each range and\n\
         closes its session, so other nodes take the ranges right away instead of\n\
         waiting for the lease to expire (TTL). Over the cap, or on a second signal,\n\
         it exits immediately like kill -9.\n\
         \n\
         --auto-split off|suggest|on (CAIRNFLOW_AUTO_SPLIT; default suggest)\n\
         splits hot or large ranges automatically. suggest only proposes at GET\n\
         /ranges/suggestions; on really splits — at most one unfinished split/merge in\n\
         the whole cluster, cooldown per range, once a minute per node. Knobs:\n\
         --auto-split-hot-share F (0.5, share of the node's turn slots),\n\
         --auto-split-hot-secs N (60 consecutive 1 s windows),\n\
         --max-range-bytes N (8 GiB, visible part), --auto-split-cooldown-ms N\n\
         (600000), --auto-split-node-interval-ms N (60000),\n\
         --auto-split-size-check-bytes N (64 MiB), --auto-split-size-check-ms N\n\
         (300000), --auto-split-max-ranges N (128; load splits stop once the table has\n\
         that many ranges, 0 = no cap). GET /ranges/stats shows load per range. Merges are only ever\n\
         suggested (reason merge, never done automatically): two adjacent ranges both\n\
         cold for --auto-split-merge-cold-ms N (21600000 = 6 h; 0 = no merge\n\
         suggestions), together under a quarter of --max-range-bytes, neither in a\n\
         split cooldown, ancestor depth at most 1.\n\
         \n\
         --lineage-compact-depth N (2; 0 = off) a range this node leads that still\n\
         reads SSTs of N or more ancestor ranges after splits, or of an ancestor\n\
         retired longer than --lineage-compact-age-ms N (86400000), gets one full\n\
         compaction so it stops standing on them; one range at a time, skipped\n\
         while L0 is throttled. --lineage-compact-every-ms N (60000),\n\
         --lineage-compact-retry-ms N (21600000). --lineage-auto-split-max-depth N\n\
         (3; 0 = no cap): auto-split refuses a range that deep; manual split warns.\n\
         \n\
         --slow-shed on|off (CAIRNFLOW_SLOW_SHED; default on) a node much slower than\n\
         the cluster (slowness score 100 for two 10 s rounds and p99 PUT > 2× the\n\
         median of the other nodes) sheds its leadership, one node at a time; it\n\
         recovers after 5 minutes with a score < 10. off still measures and publishes\n\
         the score (cairnflow_slow_score).\n\
         \n\
         --version, -V prints the version, data=<data format>, knows=<number of\n\
         migration steps the binary knows>; a cairn-ctl used with this cluster should\n\
         have the same knows=."
    );
}

fn parse_on_off(flag: &str, v: &str) -> Result<bool, String> {
    match v {
        "on" | "1" | "true" => Ok(true),
        "off" | "0" | "false" => Ok(false),
        other => Err(format!("{flag}: '{other}' — only on or off is accepted")),
    }
}

/// `--wal-kind` / `CAIRNFLOW_WAL_KIND`: the shared WAL is the only kind. `shared` is
/// accepted and changes nothing; `slatedb` is refused **before the binary touches the
/// bucket** — continuing would run the `wal/shared` migration step, which raises
/// `min_binary`, and the operator could no longer roll back to an old binary.
fn check_wal_kind(v: &str) -> Result<(), String> {
    match v {
        "shared" => Ok(()),
        "slatedb" => Err(
            "--wal-kind slatedb: SlateDB's native WAL is gone — every range uses the shared WAL, old native-WAL ranges are migrated when opened; drop this flag (or CAIRNFLOW_WAL_KIND)"
                .into(),
        ),
        other => Err(format!("--wal-kind: '{other}' — only shared is accepted")),
    }
}

/// A positive integer of a flag.
fn positive<T: std::str::FromStr + PartialOrd + Default>(flag: &str, v: &str) -> Result<T, String> {
    match v.parse::<T>() {
        Ok(n) if n > T::default() => Ok(n),
        _ => Err(format!("{flag} must be a number ≥ 1")),
    }
}

/// Turns on structured logging for the whole process.
///
/// `RUST_LOG` filters as usual (default `info`). `CAIRNFLOW_LOG=json` switches to
/// line-delimited JSON for log collectors; the default is text for people reading a
/// terminal. This is operational output — the `println!` lines at startup stay, they are
/// the answer for the person who just typed the command, not logs.
/// Logs go to stderr; OTLP trace export is added when built with `--features otel` and
/// `CAIRNFLOW_OTEL_ENDPOINT` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) is set.
///
/// `user` (`--no-user-otlp`, SaaS profile): whether user spans and `$log` are exported
/// too; system spans do not depend on it.
#[cfg_attr(not(feature = "otel"), allow(unused_variables))]
fn init_tracing(user: cairn_server::otel::UserExport) {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("CAIRNFLOW_LOG").is_ok_and(|v| v.eq_ignore_ascii_case("json"));
    let fmt = if json {
        tracing_subscriber::fmt::layer().json().boxed()
    } else {
        tracing_subscriber::fmt::layer().boxed()
    };
    let registry = tracing_subscriber::registry().with(filter).with(fmt);
    let endpoint = std::env::var("CAIRNFLOW_OTEL_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok();
    #[cfg(feature = "otel")]
    if let Some(endpoint) = endpoint {
        match cairn_server::otel::layer(&endpoint) {
            Ok((layer, provider)) => {
                // The provider lives for the whole process (kept in `otel`); flushed at shutdown.
                drop(provider);
                for e in cairn_server::otel::export_user_data(&endpoint, user) {
                    eprintln!("cairnflow: {e}");
                }
                registry.with(layer).init();
                return;
            }
            Err(e) => eprintln!("cairnflow: cannot build the OTLP exporter for {endpoint}: {e}"),
        }
    }
    #[cfg(not(feature = "otel"))]
    if endpoint.is_some() {
        eprintln!(
            "cairnflow: an OTLP endpoint is set but the binary was built without --features otel — ignored"
        );
    }
    registry.init();
}

#[tokio::main]
async fn main() {
    // Read the flags before logging: `--no-user-otlp` decides which exporter is built at
    // init. With broken flags the default log is still on to print the error as before.
    let args = parse_args();
    init_tracing(
        args.as_ref()
            .map_or_else(|_| Default::default(), |a| a.user_otlp),
    );
    // Do not return `Result` from `main`. Rust prints the error value with **`Debug`**,
    // so a multi-line message written for operators would come out as `"...\nmissing..."`
    // — newlines as two characters, and a structured error as struct syntax. Every
    // guidance message here (the D12 gate, the CAS gate, a mismatched partition count) is
    // multi-line and written for 3 a.m.
    let r = run(args).await;
    cairn_server::otel::shutdown();
    if let Err(e) = r {
        eprintln!("cairnflow: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Result<Args, String>) -> Result<(), node::Error> {
    let args = args?;
    if args.non_dumpable {
        make_non_dumpable()?;
    }
    let query_worker = resolve_query_worker(&args)?;
    // The node's replay RAM budget: set before any range runs a turn.
    cairn_server::replay_budget::NODE.set_cap(args.replay_ram_budget_bytes);

    let main = StoreLocation::parse(&args.main_store)?;
    let wal = args
        .wal_store
        .as_deref()
        .map(StoreLocation::parse)
        .transpose()?;
    // The warm-machine cross-check mode is on by default in `testkit` builds; benches on
    // an S3 profile turn it off with `CAIRNFLOW_WARM_CHECK=0` to measure exactly the
    // product path. Release builds do not have that mode.
    #[cfg(feature = "testkit")]
    if std::env::var("CAIRNFLOW_WARM_CHECK").as_deref() == Ok("0") {
        cairn_engine::set_warm_check(false);
    }
    // The simulated S3 store profile: only `testkit` builds read `CAIRNFLOW_STORE_PROFILE`;
    // release builds do not have this path.
    #[cfg(feature = "testkit")]
    let (main, wal) = {
        use cairn_server::store_profile::wrap_from_env;
        let fail = |e: String| -> ! {
            eprintln!("cairnflow: {e}");
            std::process::exit(2)
        };
        let main = wrap_from_env("main", main).unwrap_or_else(|e| fail(e));
        let wal = wal.map(|w| wrap_from_env("wal", w).unwrap_or_else(|e| fail(e)));
        (main, wal)
    };
    // The chaos `--slow-node`: every PUT of the node waits on the `node::put_delay`
    // failpoint, turned on/off at runtime through `/admin/failpoints`. Only `failpoints`
    // builds. A transparent layer (it only waits): it keeps `local_dir`, so a `--cluster`
    // node on `file://` still gets the directory lock and the simulated CAS.
    #[cfg(feature = "failpoints")]
    let (main, wal) = {
        let slow = |loc: StoreLocation| {
            loc.wrap_transparent(cairn_store::failpoint::DelayStore::wrap(
                std::sync::Arc::clone(loc.store()),
                "node::put_delay",
                cairn_store::failpoint::DelayOps::Writes,
            ))
        };
        (slow(main), wal.map(slow))
    };
    // Counting requests to the store (`cairnflow_store_requests_total`): the outermost
    // layer of the location, below the SST disk cache — only real requests to the store.
    let metered = |loc: StoreLocation| {
        loc.wrap_transparent(cairn_store::metered::MeteredStore::wrap(
            std::sync::Arc::clone(loc.store()),
        ))
    };
    let (main, wal) = (metered(main), wal.map(metered));
    let stores = match wal {
        Some(wal) => ObjectStores::split(main, wal)?,
        None => ObjectStores::single(main),
    }
    .with_wal_atomic_put(args.wal_atomic_put);
    let main_uri = stores.main().uri().to_string();
    let wal_atomic_put = stores.wal().atomic_put();
    // One path for every node: with `--cluster` or not, the node runs the lease layer;
    // alone on the bucket it is a one-node cluster.
    let mode = NodeMode {
        partitions: args.partitions,
        lease_ttl: Duration::from_millis(args.lease_ttl_ms),
        node_id: args.node_id.clone(),
    };
    let tick_interval =
        (args.tick_interval_ms > 0).then(|| Duration::from_millis(args.tick_interval_ms));

    // The TLS roots of the tenant client: only `testkit` builds read
    // `CAIRNFLOW_TESTKIT_TLS_ROOTS` — tests build a fake `https://` edge with their own
    // CA. Release builds always use the system root store.
    #[cfg_attr(not(feature = "testkit"), allow(unused_mut))]
    let mut egress = args.egress.clone();
    #[cfg(feature = "testkit")]
    if let Ok(path) = std::env::var(cairn_server::egress::TLS_ROOTS_ENV) {
        let pem = std::fs::read(&path).map_err(|e| format!("{path}: {e}"))?;
        egress.tls_roots =
            reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| format!("{path}: {e}"))?;
    }

    let mut running = node::start(NodeConfig {
        stores,
        mode,
        host: args.host.clone(),
        advertise_host: args.advertise_host.clone(),
        port: args.port,
        internal_port: args.internal_port.unwrap_or(if args.port == 0 {
            0
        } else {
            args.port.saturating_add(1)
        }),
        auth: cairn_server::auth::AuthConfig {
            required: !args.no_auth,
            admin_key: std::env::var("CAIRNFLOW_ADMIN_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            cluster_secret: args.cluster_secret.clone(),
            metrics_key: std::env::var("CAIRNFLOW_METRICS_KEY")
                .ok()
                .filter(|k| !k.is_empty()),
            // Variable present but empty ⇒ as if absent; present but broken ⇒ do not start
            // (`EnvKeys::resolve`).
            admin_key_sha256: std::env::var(cairn_server::auth::ADMIN_KEY_SHA256_ENV)
                .ok()
                .filter(|k| !k.trim().is_empty()),
            provision_key_sha256: std::env::var(cairn_server::auth::PROVISION_KEY_SHA256_ENV)
                .ok()
                .filter(|k| !k.trim().is_empty()),
        },
        tick_interval,
        limits: TurnLimits {
            max_concurrent_turns: args.max_concurrent_turns,
            // The node's default cap does not exceed the cell's upper cap either.
            sdk_timeout: (args.inactivity_timeout_ms > 0).then(|| {
                Duration::from_millis(
                    args.inactivity_timeout_ms
                        .min(args.inactivity_timeout_max_ms),
                )
            }),
            queue_wait: (args.queue_wait_ms > 0).then(|| Duration::from_millis(args.queue_wait_ms)),
            l0_throttle: cairn_server::l0::L0ThrottleConfig {
                soft: args.l0_soft,
                max_reject: args.l0_max_reject,
                ..Default::default()
            },
            rr_delta_replay: !args.no_rr_delta_replay,
            max_turns_per_tenant: (args.max_turns_per_tenant > 0)
                .then_some(args.max_turns_per_tenant),
            sdk_timeout_max: Duration::from_millis(args.inactivity_timeout_max_ms),
            ..TurnLimits::default()
        },
        // `0` = keep forever (retention off).
        retention_ms: (args.retention_ms > 0).then_some(args.retention_ms),
        archive: !args.no_archive,
        archive_artifacts: args.archive_artifacts,
        invoke_wait: (args.invoke_wait_ms > 0).then(|| Duration::from_millis(args.invoke_wait_ms)),
        attach_max_wait: Duration::from_millis(args.attach_max_wait_ms),
        schedule_min_period: Duration::from_millis(args.schedule_min_period_ms),
        invoke_poll_floor: None,
        reader_poll: Duration::from_millis(args.reader_poll_ms),
        deploy_refresh: Duration::from_millis(args.deploy_refresh_ms),
        tenant_usage_every: cairn_server::tenant_usage::DEFAULT_EVERY,
        audit_denials_every: cairn_server::audit::DENIALS_EVERY,
        registry_reconcile: cairn_server::registry_gen::DEFAULT_RECONCILE,
        registry_stuck: cairn_server::registry_gen::DEFAULT_STUCK,
        owner_watch: None,
        metering_flush: Duration::from_secs(60),
        gc_retired_dbs: args.gc_retired_dbs.map(|mode| cairn_server::gc::GcConfig {
            every: Duration::from_secs(args.gc_every_s),
            min_age: Duration::from_secs(args.gc_min_age_s),
            mode,
        }),
        retire_orphans_every: (args.retire_orphans_every_s > 0)
            .then(|| Duration::from_secs(args.retire_orphans_every_s)),
        reconcile_intents_every: (args.reconcile_intents_every_s > 0)
            .then(|| Duration::from_secs(args.reconcile_intents_every_s)),
        scrub_every: (args.scrub_every_s > 0).then(|| Duration::from_secs(args.scrub_every_s)),
        scrub_requests_per_hour: args.scrub_requests_per_hour,
        trash_keep: (args.trash_keep_s > 0).then(|| Duration::from_secs(args.trash_keep_s)),
        tenant_purge_keep: (args.tenant_purge_keep_s > 0)
            .then(|| Duration::from_secs(args.tenant_purge_keep_s)),
        artifact_sweep: Some(cairn_server::artifact_sweep::DEFAULT_EVERY),
        artifact_min_age: cairn_server::artifact_sweep::DEFAULT_MIN_AGE,
        value_sweep: (args.value_sweep_every_s > 0)
            .then(|| Duration::from_secs(args.value_sweep_every_s)),
        value_min_age: Duration::from_secs(args.value_min_age_s),
        artifact_put_deadline: cairn_store::artifact::T_PUT,
        flush_interval: Some(Duration::from_millis(args.flush_interval_ms)),
        engine_threads: (args.engine_threads > 0).then_some(args.engine_threads),
        node_wal: cairn_store::NodeWalConfig {
            window: Duration::from_millis(args.wal_window_ms.unwrap_or(args.flush_interval_ms)),
            ..args.node_wal.clone()
        },
        slow_log: Duration::from_millis(args.slow_log_ms),
        migrations: cairn_store::migrate::MIGRATIONS,
        // The binary never filters the network; the field only exists in test builds.
        #[cfg(feature = "testkit")]
        net_filter: None,
        #[cfg(feature = "testkit")]
        inline_engines: false,
        #[cfg(feature = "testkit")]
        view_suspects: true,
        auto_split: args.auto_split.clone(),
        lineage: args.lineage.clone(),
        slow: cairn_server::slow::SlowConfig {
            shed: args.slow_shed,
            ..Default::default()
        },
        quarantine: args.quarantine,
        quiet_lease: args.quiet_lease,
        streams: cairn_server::streams::StreamConfig {
            tenant_max: args.max_open_streams_per_tenant,
            ..Default::default()
        },
        http_header_timeout: Duration::from_millis(args.http_header_timeout_ms),
        query_limits: cairn_query::Limits::default(),
        query_tenant_budget: Default::default(),
        tenant_defaults: args.tenant_defaults,
        key_rate: args.key_rate,
        query_threads: cairn_server::query::http::QUERY_THREADS,
        query_worker: query_worker.clone(),
        disk_cache: args.disk_cache_dir.clone().map(|dir| {
            cairn_store::disk_cache::DiskCacheConfig {
                dir,
                max_bytes: args.disk_cache_bytes,
            }
        }),
        ui: cairn_server::ui::UiConfig {
            enabled: !args.no_ui,
            base_path: args.ui_base_path.clone(),
            cookie_secure: args.ui_cookie_secure,
            trust_proxy: args.trust_proxy.clone(),
            env_label: args.ui_env_label.clone(),
            ..Default::default()
        },
        egress,
        identity_master: args.identity_master.clone(),
        deploy_require_identity: args.deploy_require_identity,
        deploy_transport: args.deploy_transport,
        deploy_https_request_response: args.deploy_https_request_response,
        deploy_no_rr_compression: args.deploy_no_rr_compression,
        deploy_require_compression: args.deploy_require_compression,
        values: args.values.clone(),
    })
    .await?;

    // Print the port **actually** assigned, not the port requested.
    println!("cairnflow listening on {}", running.url());
    println!("  internal:    {}", running.internal_url());
    println!(
        "  auth:        {}",
        if args.no_auth {
            "OFF (--no-auth) — dev only"
        } else {
            "API key (Bearer)"
        }
    );
    if args.profile == Some(Profile::Saas) {
        println!(
            "  profile:     saas — strict egress, https:// only, deployments must verify request identity, https:// endpoints declaring BIDI register as REQUEST_RESPONSE, no user OTLP export, SQL in worker processes, node not dumpable (Linux)"
        );
    }
    println!(
        "  console:     {}",
        if args.no_ui || !cfg!(feature = "ui") {
            "off (--no-ui, or built without the ui feature)".to_string()
        } else if cairn_server::ui::built() {
            format!("{}{}/", running.url(), args.ui_base_path)
        } else {
            format!(
                "{}{}/ — placeholder page, binary built without ui/dist",
                running.url(),
                args.ui_base_path
            )
        }
    );
    println!(
        "  query:       {}",
        match &query_worker {
            Some(w) => format!(
                "SQL runs in {} processes (memory limit {} MiB each)",
                w.path.display(),
                w.memory_bytes >> 20
            ),
            None if args.query_isolation == QueryIsolation::Inline => {
                "SQL runs inside the node process (--query-isolation inline)".to_string()
            }
            None => format!(
                "SQL runs inside the node process — {} not found next to the binary; a hostile query can stop the node",
                cairn_query::worker::BINARY
            ),
        }
    );
    if query_worker.is_none() && args.query_isolation == QueryIsolation::Auto {
        tracing::warn!(
            "{} not found next to the binary: /query runs SQL inside the node process",
            cairn_query::worker::BINARY
        );
    }
    println!("  main store:  {main_uri}");
    println!(
        "  WAL store:   {} (atomic PUT: {}{})",
        match args.wal_store {
            Some(ref uri) => uri.as_str(),
            None => "(same as main store)",
        },
        if wal_atomic_put { "yes" } else { "no" },
        if args.wal_atomic_put.is_some() {
            ", --wal-atomic-put"
        } else {
            ", default — s3:// only on verified stores"
        }
    );
    match &args.disk_cache_dir {
        Some(dir) => println!(
            "  disk cache:  {} (cap {} MiB)",
            dir.display(),
            args.disk_cache_bytes >> 20
        ),
        None => println!("  disk cache:  off"),
    }
    println!("  partition:   {}", running.cluster.partition_count());
    println!(
        "  mode:        cluster (node {}), lease TTL {}ms{}",
        running.node_id,
        args.lease_ttl_ms,
        if args.cluster {
            " — --cluster has no effect any more: every node runs the lease tier"
        } else {
            ""
        }
    );
    println!(
        "  tick:        {}",
        match tick_interval {
            Some(d) => format!("{}ms", d.as_millis()),
            None => "off".into(),
        }
    );
    // **Graceful shutdown.** Without catching signals, `SIGTERM` kills the process by
    // default: no seal, no session close — peers have to wait for our lease to expire
    // (≥ TTL) before taking the ranges, so every deploy or scale-down would freeze every
    // range of the node for that many seconds.
    tokio::select! {
        r = running.wait() => return r,
        sig = shutdown_signal() => {
            tracing::info!(signal = sig?, "signal received — shutting down gracefully, handing over every range");
        }
    }
    let ceiling = Duration::from_millis(args.shutdown_timeout_ms);
    let started = tokio::time::Instant::now();
    tokio::select! {
        () = running.shutdown() => {
            tracing::info!(ms = started.elapsed().as_millis() as u64, "shut down gracefully");
        }
        // Shutdown is cleanup, not a place allowed to hang: a store that cannot flush ⇒
        // past the cap we exit like `kill -9` — peers take the ranges after the TTL, just
        // like before this step existed, never worse.
        () = tokio::time::sleep(ceiling) => {
            eprintln!("cairnflow: graceful shutdown exceeded {ceiling:?} — exiting now, peers take the ranges after the TTL");
            std::process::exit(1);
        }
        // A second signal (Ctrl-C again): the operator does not want to wait.
        sig = shutdown_signal() => {
            eprintln!("cairnflow: received {} again — exiting now, peers take the ranges after the TTL", sig?);
            std::process::exit(1);
        }
    }
    Ok(())
}

/// Waits for `SIGTERM` (an orchestrator, `kill`) or `SIGINT` (Ctrl-C). Returns the signal name.
///
/// Registered when the future is first polled; from then on the process no longer dies
/// by these two signals' default — the call site must exit by itself.
#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<&'static str> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    Ok(tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    })
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<&'static str> {
    tokio::signal::ctrl_c().await?;
    Ok("Ctrl-C")
}

#[cfg(test)]
mod tests {
    //! The table tests of `parse_args`: defaults, environment variables, each flag, errors.
    //! Every parse runs on its own thread with a cap — a flag that forgets to advance the
    //! cursor loops forever rather than failing, so a cap is needed to see it.

    use {
        super::*,
        cairn_server::{autosplit::AutoSplitMode, gc::GcMode, quarantine::Mode},
    };

    fn parse(env: &[(&str, &str)], argv: &[&str]) -> Result<Args, String> {
        let env: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let argv: Vec<String> = argv.iter().map(|a| a.to_string()).collect();
        let shown = argv.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let lookup = |k: &str| env.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
            let _ = tx.send(parse_args_from(&lookup, argv));
        });
        rx.recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("parse_args hangs with {shown:?}"))
    }

    fn ok(argv: &[&str]) -> Args {
        parse(&[], argv).unwrap_or_else(|e| panic!("{argv:?}: {e}"))
    }

    /// An environment sufficient for `--profile saas`.
    const SAAS_ENV: &[(&str, &str)] = &[
        (
            "CAIRNFLOW_CLUSTER_SECRET",
            "cf_test_cluster_secret_profile_saas_0123456789",
        ),
        (
            cairn_server::identity::MASTER_ENV,
            "cf_test_identity_master_profile_saas_0123",
        ),
    ];

    /// `--profile saas` turns on the full set of safe flags — even when the person typing
    /// sets none of them — and `CAIRNFLOW_PROFILE=saas` is equivalent.
    #[test]
    fn saas_profile_turns_on_every_safe_default() {
        use cairn_server::otel::UserExport;
        for (env, argv) in [
            (
                SAAS_ENV.to_vec(),
                vec!["--profile", "saas", "--egress-deny-cidr", "203.0.113.7"],
            ),
            (
                [SAAS_ENV, &[("CAIRNFLOW_PROFILE", "saas")]].concat(),
                vec!["--egress-deny-cidr", "203.0.113.7"],
            ),
        ] {
            let a = parse(&env, &argv).unwrap_or_else(|e| panic!("{argv:?}: {e}"));
            assert_eq!(a.profile, Some(Profile::Saas));
            assert!(a.egress.deny_private && a.egress.require_https);
            assert_eq!(a.egress.deny.len(), 1);
            assert_eq!(a.ui_cookie_secure, Some(true));
            assert_eq!(a.user_otlp, UserExport::Off);
            assert!(a.deploy_require_identity);
            assert!(a.deploy_https_request_response);
            // RR body compression follows what the SDK declares, and the SDK must declare it.
            assert!(!a.deploy_no_rr_compression);
            assert!(a.deploy_require_compression);
            assert!(a.values.prefer_direct);
            assert!(!a.no_auth);
            assert!(a.cluster_secret.is_some() && a.identity_master.is_some());
            // The tenant's SQL runs in a child process.
            assert_eq!(a.query_isolation, QueryIsolation::Process);
            assert_eq!(a.query_worker_memory_mb, Some(SAAS_QUERY_WORKER_MEMORY_MB));
            assert!(a.non_dumpable);
        }
        // No profile: no flag turns on by itself.
        let a = parse(SAAS_ENV, &[]).expect("no profile");
        assert!(!a.egress.deny_private && !a.egress.require_https);
        assert!(!a.deploy_require_identity);
        assert!(!a.deploy_https_request_response);
        assert!(!a.deploy_require_compression);
        assert!(!a.values.prefer_direct);
        assert_eq!(a.user_otlp, UserExport::On);
        assert_eq!(a.ui_cookie_secure, None);
        assert_eq!(a.query_isolation, QueryIsolation::Auto);
        assert_eq!(a.query_worker_memory_mb, None);
        assert!(!a.non_dumpable);
    }

    /// `--query-isolation`: three values, unknown ⇒ an error; `--profile saas` refuses
    /// `inline`; `--query-worker`/`CAIRNFLOW_QUERY_WORKER` and the memory cap; `process`
    /// without the binary found ⇒ a startup error.
    #[test]
    fn query_isolation_flags() {
        for (v, want) in [
            ("auto", QueryIsolation::Auto),
            ("process", QueryIsolation::Process),
            ("inline", QueryIsolation::Inline),
        ] {
            assert_eq!(ok(&["--query-isolation", v]).query_isolation, want);
        }
        let Err(e) = parse(&[], &["--query-isolation", "thread"]) else {
            panic!("--query-isolation thread accepted")
        };
        assert!(e.contains("auto, process or inline"), "{e}");
        let Err(e) = parse(
            SAAS_ENV,
            &[
                "--profile",
                "saas",
                "--egress-deny-cidr",
                "203.0.113.7",
                "--query-isolation",
                "inline",
            ],
        ) else {
            panic!("--profile saas accepts --query-isolation inline")
        };
        assert!(e.contains("--query-isolation inline is not allowed"), "{e}");
        let a = ok(&["--query-worker", "/x/w", "--query-worker-memory-mb", "256"]);
        assert_eq!(
            a.query_worker.as_deref(),
            Some(std::path::Path::new("/x/w"))
        );
        assert_eq!(a.query_worker_memory_mb, Some(256));
        let a = parse(&[("CAIRNFLOW_QUERY_WORKER", "/y/w")], &[]).unwrap();
        assert_eq!(
            a.query_worker.as_deref(),
            Some(std::path::Path::new("/y/w"))
        );
        assert!(parse(&[], &["--query-worker-memory-mb", "0"]).is_err());
        // No binary: `process` fails, `auto` falls back to in place.
        let mut a = ok(&["--query-worker", "/nonexistent/cairn-query-worker"]);
        assert!(resolve_query_worker(&a).unwrap().is_none());
        a.query_isolation = QueryIsolation::Process;
        let e = resolve_query_worker(&a).unwrap_err();
        assert!(
            e.contains("cannot find the cairn-query-worker binary"),
            "{e}"
        );
    }

    /// `--profile saas`: the memory cap of each `/query` child defaults to 256 MiB (a ~1 GB
    /// beta machine); self-hosting keeps 512; an explicit flag wins over both.
    #[test]
    fn saas_profile_caps_query_workers_at_256_mib() {
        let worker = std::env::current_exe().expect("exe");
        let worker = worker.to_str().expect("utf-8");
        let saas = [
            "--profile",
            "saas",
            "--egress-deny-cidr",
            "203.0.113.7",
            "--query-worker",
            worker,
        ];
        let mem = |env: &[(&str, &str)], argv: &[&str]| {
            let a = parse(env, argv).unwrap_or_else(|e| panic!("{argv:?}: {e}"));
            resolve_query_worker(&a)
                .expect("resolve")
                .expect("has a worker")
                .memory_bytes
                >> 20
        };
        assert_eq!(mem(SAAS_ENV, &saas), 256);
        assert_eq!(mem(&[], &["--query-worker", worker]), 512);
        let explicit = [&saas[..], &["--query-worker-memory-mb", "384"]].concat();
        assert_eq!(mem(SAAS_ENV, &explicit), 384);
    }

    /// `--profile saas` refuses to start when a condition is missing, and states **every**
    /// missing condition at once.
    #[test]
    fn saas_profile_refuses_missing_conditions() {
        let deny = ["--egress-deny-cidr", "203.0.113.7"];
        let short = [
            ("CAIRNFLOW_CLUSTER_SECRET", "tiny"),
            (cairn_server::identity::MASTER_ENV, SAAS_ENV[1].1),
        ];
        /// (environment, argv, fragments that must appear in the error).
        type Case<'a> = (&'a [(&'a str, &'a str)], Vec<&'a str>, &'a [&'a str]);
        let cases: Vec<Case> = vec![
            (
                &[],
                vec!["--profile", "saas"],
                &[
                    "missing CAIRNFLOW_CLUSTER_SECRET",
                    "missing CAIRNFLOW_IDENTITY_MASTER",
                    "missing --egress-deny-cidr",
                ],
            ),
            (
                &short,
                [&["--profile", "saas"][..], &deny].concat(),
                &["CAIRNFLOW_CLUSTER_SECRET is too short (4 bytes"],
            ),
            (
                &SAAS_ENV[..1],
                [&["--profile", "saas"][..], &deny].concat(),
                &["missing CAIRNFLOW_IDENTITY_MASTER"],
            ),
            (
                SAAS_ENV,
                [&["--profile", "saas", "--no-auth"][..], &deny].concat(),
                &["--no-auth is not allowed"],
            ),
            (
                SAAS_ENV,
                [
                    &["--ui-cookie-secure", "off", "--profile", "saas"][..],
                    &deny,
                ]
                .concat(),
                &["--ui-cookie-secure off is not allowed"],
            ),
            (
                SAAS_ENV,
                vec!["--profile", "bogus"],
                &["--profile: 'bogus'"],
            ),
        ];
        for (env, argv, want) in cases {
            let e = match parse(env, &argv) {
                Ok(_) => panic!("{argv:?}: must be refused"),
                Err(e) => e,
            };
            for w in want {
                assert!(e.contains(w), "{argv:?}: missing {w:?} in:\n{e}");
            }
        }
    }

    /// `--profile saas` refuses `--inactivity-timeout-ms 0`: `0` turns the SDK wait cap
    /// off, so a turn hung at the edge (BIDI behind a body-buffering proxy) would hold its
    /// turn slot forever, with no error and no retry. The environment variable too; a
    /// positive value is accepted.
    #[test]
    fn saas_profile_refuses_zero_inactivity_timeout() {
        let deny = ["--egress-deny-cidr", "203.0.113.7"];
        let argv = [
            &["--profile", "saas", "--inactivity-timeout-ms", "0"][..],
            &deny,
        ]
        .concat();
        let e = parse(SAAS_ENV, &argv).err().expect("must be refused");
        assert!(
            e.contains("--inactivity-timeout-ms 0 is not allowed"),
            "{e}"
        );
        let env = [SAAS_ENV, &[("CAIRNFLOW_INACTIVITY_TIMEOUT_MS", "0")]].concat();
        let argv = [&["--profile", "saas"][..], &deny].concat();
        assert!(parse(&env, &argv).is_err(), "environment variable = 0");
        let argv = [
            &["--profile", "saas", "--inactivity-timeout-ms", "5000"][..],
            &deny,
        ]
        .concat();
        let a = parse(SAAS_ENV, &argv).expect("positive cap");
        assert_eq!(a.inactivity_timeout_ms, 5000);
    }

    /// `--profile saas` has a default cap for every absent field of the `tenants`
    /// registry; without the profile it does not.
    #[test]
    fn saas_profile_defaults_every_absent_tenant_cap() {
        let deny = ["--egress-deny-cidr", "203.0.113.7"];
        let a = parse(SAAS_ENV, &[&["--profile", "saas"][..], &deny].concat()).unwrap();
        let d = a.tenant_defaults;
        assert_eq!(d, saas_tenant_defaults());
        assert_eq!(
            (
                d.max_live,
                d.max_journal_bytes,
                d.max_artifact_bytes,
                d.max_deployments,
                d.max_value_bytes
            ),
            (
                Some(1_000),
                Some(32 << 20),
                Some(1 << 30),
                Some(20),
                Some(64 << 20)
            )
        );
        let a = parse(&[], &[]).unwrap();
        assert!(a.tenant_defaults.is_empty());
        assert!(a.key_rate.is_none(), "self-hosted: no per-key rate cap");
        let a = parse(SAAS_ENV, &[&["--profile", "saas"][..], &deny].concat()).unwrap();
        assert_eq!(
            a.key_rate,
            cairn_server::tenants::DeployRate::parse("1000/1")
        );
        let a = parse(
            SAAS_ENV,
            &[&["--profile", "saas", "--key-rate", "50/1"][..], &deny].concat(),
        )
        .unwrap();
        assert_eq!(a.key_rate, cairn_server::tenants::DeployRate::parse("50/1"));
    }

    /// `--profile saas` splits the turn slots and clamps the SDK wait cap: undeclared, the
    /// per-tenant turn cap = ¼ of the node's slots (floor 1) and the upper cap of
    /// `inactivityTimeout` 15 minutes; declared, the declared value wins, except a wait cap
    /// above 15 minutes (clamped). Without the profile: no per-tenant cap, a 1 hour wait
    /// cap.
    #[test]
    fn saas_profile_shares_turns_and_caps_inactivity() {
        let deny = ["--egress-deny-cidr", "203.0.113.7"];
        let saas = |extra: &[&str]| {
            parse(
                SAAS_ENV,
                &[&["--profile", "saas"][..], &deny, extra].concat(),
            )
            .unwrap_or_else(|e| panic!("{extra:?}: {e}"))
        };
        let a = saas(&[]);
        assert_eq!(a.max_turns_per_tenant, a.max_concurrent_turns / 4);
        assert_eq!(a.inactivity_timeout_max_ms, 900_000);
        assert_eq!(
            saas(&["--max-concurrent-turns", "2"]).max_turns_per_tenant,
            1,
            "floor 1"
        );
        assert_eq!(
            saas(&["--max-turns-per-tenant", "3"]).max_turns_per_tenant,
            3
        );
        assert_eq!(
            saas(&["--inactivity-timeout-max-ms", "3600000"]).inactivity_timeout_max_ms,
            900_000,
            "clamped to 15 minutes"
        );
        assert_eq!(
            saas(&["--inactivity-timeout-max-ms", "300000"]).inactivity_timeout_max_ms,
            300_000
        );
        let a = parse(SAAS_ENV, &[]).expect("no profile");
        assert_eq!(a.max_turns_per_tenant, 0);
        assert_eq!(a.inactivity_timeout_max_ms, 3_600_000);
    }

    /// **The native WAL is gone**: `--wal-kind slatedb` (or `CAIRNFLOW_WAL_KIND=slatedb`)
    /// is refused at startup with the reason — before the binary touches the bucket, so the
    /// operator can still roll back to an old binary (the `wal/shared` step raises
    /// `min_binary`, after which there is no rolling back). `shared` is still accepted and
    /// changes nothing.
    #[test]
    fn native_wal_kind_is_rejected() {
        let want = "--wal-kind slatedb: SlateDB's native WAL is gone — \
                    every range uses the shared WAL, old native-WAL ranges are migrated when \
                    opened; drop this flag (or CAIRNFLOW_WAL_KIND)";
        assert_eq!(
            parse(&[], &["--wal-kind", "slatedb"]).err().as_deref(),
            Some(want)
        );
        assert_eq!(
            parse(&[("CAIRNFLOW_WAL_KIND", "slatedb")], &[])
                .err()
                .as_deref(),
            Some(want)
        );
        ok(&["--wal-kind", "shared"]);
        assert!(parse(&[("CAIRNFLOW_WAL_KIND", "shared")], &[]).is_ok());
    }

    /// `--wal-atomic-put`: absent ⇒ by the WAL store's scheme; only `true`/`false` are
    /// accepted — a declared capability, never guessed.
    #[test]
    fn wal_atomic_put_is_declared_explicitly() {
        assert_eq!(ok(&[]).wal_atomic_put, None);
        assert_eq!(
            ok(&["--wal-atomic-put", "false"]).wal_atomic_put,
            Some(false)
        );
        assert_eq!(ok(&["--wal-atomic-put", "true"]).wal_atomic_put, Some(true));
        assert_eq!(
            parse(&[], &["--wal-atomic-put", "maybe"]).err().as_deref(),
            Some("--wal-atomic-put: 'maybe' — only true or false is accepted")
        );
    }

    /// **The gate of a new bucket**: `--quiet-lease off|solo|delegation` (or
    /// `CAIRNFLOW_QUIET_LEASE`), default `delegation`; unknown values are refused, also in
    /// the environment variable (a typo must not silently turn delegation on).
    #[test]
    fn quiet_lease_flag_picks_the_new_bucket_gate() {
        use cairn_store::quiet_lease::Bootstrap;
        assert_eq!(ok(&[]).quiet_lease, Bootstrap::Delegation);
        for (v, want) in [
            ("off", Bootstrap::Off),
            ("solo", Bootstrap::Solo),
            ("delegation", Bootstrap::Delegation),
        ] {
            assert_eq!(ok(&["--quiet-lease", v]).quiet_lease, want);
            let a = parse(&[("CAIRNFLOW_QUIET_LEASE", v)], &[]).unwrap();
            assert_eq!(a.quiet_lease, want);
        }
        assert_eq!(
            parse(
                &[("CAIRNFLOW_QUIET_LEASE", "off")],
                &["--quiet-lease", "solo"]
            )
            .unwrap()
            .quiet_lease,
            Bootstrap::Solo,
            "the flag wins over the environment variable"
        );
        assert_eq!(
            parse(&[], &["--quiet-lease", "on"]).err().as_deref(),
            Some("--quiet-lease: 'on' — only off, solo or delegation is accepted")
        );
        assert!(parse(&[("CAIRNFLOW_QUIET_LEASE", "yes")], &[]).is_err());
    }

    #[test]
    fn defaults_without_env_or_flags() {
        let a = ok(&[]);
        assert!(a.main_store.starts_with("file://"), "{}", a.main_store);
        assert!(
            a.main_store.ends_with("/cairnflow-data"),
            "{}",
            a.main_store
        );
        assert_eq!(a.wal_store, None);
        assert!(!a.cluster);
        assert_eq!(a.node_id, None);
        assert_eq!(a.lease_ttl_ms, 10_000);
        assert_eq!(a.reader_poll_ms, 500);
        assert_eq!(a.deploy_refresh_ms, 30_000);
        assert_eq!(a.partitions, 1);
        assert_eq!(a.port, 8080);
        assert_eq!(a.host, "127.0.0.1");
        assert_eq!(a.tick_interval_ms, 200);
        assert_eq!(a.retention_ms, 24 * 60 * 60 * 1000);
        assert_eq!(
            a.max_concurrent_turns,
            cairn_server::actor::DEFAULT_MAX_CONCURRENT_TURNS
        );
        assert_eq!(
            a.inactivity_timeout_ms,
            cairn_server::actor::DEFAULT_SDK_TIMEOUT.as_millis() as u64
        );
        assert_eq!(
            a.queue_wait_ms,
            cairn_server::turn::DEFAULT_QUEUE_WAIT.as_millis() as u64
        );
        assert_eq!(a.l0_soft, cairn_server::l0::DEFAULT_L0_SOFT);
        assert_eq!(a.l0_max_reject, cairn_server::l0::DEFAULT_L0_MAX_REJECT);
        assert_eq!(a.invoke_wait_ms, 5_000);
        assert_eq!(
            a.slow_log_ms,
            cairn_server::slowlog::DEFAULT_THRESHOLD.as_millis() as u64
        );
        assert_eq!(
            a.flush_interval_ms,
            cairn_store::DEFAULT_FLUSH_INTERVAL.as_millis() as u64
        );
        assert_eq!(a.engine_threads, 0);
        let wal = cairn_store::NodeWalConfig::default();
        assert_eq!(a.node_wal.stripes, wal.stripes);
        assert_eq!(a.node_wal.inflight, wal.inflight);
        assert_eq!(a.node_wal.object_cap, wal.object_cap);
        assert_eq!(a.node_wal.linger, wal.linger);
        assert_eq!(a.node_wal.frame_age_flush, wal.frame_age_flush);
        assert_eq!(a.node_wal.put_timeout, wal.put_timeout);
        assert_eq!(a.wal_window_ms, None);
        assert_eq!(
            a.shutdown_timeout_ms,
            DEFAULT_SHUTDOWN_TIMEOUT.as_millis() as u64
        );
        assert_eq!(a.internal_port, None);
        assert!(!a.no_auth);
        assert!(!a.no_ui);
        assert_eq!(a.ui_base_path, "/ui");
        assert_eq!(a.ui_cookie_secure, None);
        assert_eq!(a.gc_retired_dbs, None);
        assert_eq!(a.gc_every_s, cairn_server::gc::DEFAULT_EVERY.as_secs());
        assert_eq!(a.gc_min_age_s, cairn_server::gc::DEFAULT_MIN_AGE.as_secs());
        assert_eq!(
            a.retire_orphans_every_s,
            cairn_server::gc::DEFAULT_RETIRE_ORPHANS_EVERY.as_secs()
        );
        assert_eq!(
            a.reconcile_intents_every_s,
            cairn_server::split::DEFAULT_RECONCILE_INTENTS_EVERY.as_secs()
        );
        assert_eq!(
            a.scrub_every_s,
            cairn_server::scrub::DEFAULT_EVERY.as_secs()
        );
        assert_eq!(a.scrub_requests_per_hour, None);
        assert_eq!(a.trash_keep_s, cairn_store::trash::DEFAULT_KEEP.as_secs());
        assert!(!a.no_archive);
        assert_eq!(a.user_otlp, cairn_server::otel::UserExport::On);
        assert_eq!(
            a.auto_split,
            cairn_server::autosplit::AutoSplitConfig::default()
        );
        assert!(a.slow_shed);
        assert_eq!(
            a.quarantine,
            cairn_server::quarantine::QuarantineConfig::default()
        );
    }

    #[test]
    fn env_sets_defaults_and_bad_numbers_fall_back() {
        let a = parse(
            &[
                ("CAIRNFLOW_MAIN_STORE", "memory://"),
                ("CAIRNFLOW_WAL_STORE", "memory://wal"),
                ("CAIRNFLOW_PORT", "9000"),
                ("CAIRNFLOW_HOST", "0.0.0.0"),
                ("CAIRNFLOW_TICK_MS", "nope"),
                ("CAIRNFLOW_CLUSTER", ""),
                ("CAIRNFLOW_NODE_ID", "n1"),
                ("CAIRNFLOW_LEASE_TTL_MS", "3000"),
                ("CAIRNFLOW_PARTITIONS", "4"),
                ("CAIRNFLOW_READER_POLL_MS", "77"),
                ("CAIRNFLOW_DEPLOY_REFRESH_MS", "88"),
                ("CAIRNFLOW_RETENTION_MS", "0"),
                ("CAIRNFLOW_MAX_CONCURRENT_TURNS", "0"),
                ("CAIRNFLOW_QUEUE_WAIT_MS", "11"),
                ("CAIRNFLOW_L0_SOFT", "3"),
                ("CAIRNFLOW_L0_MAX_REJECT", "0.5"),
                ("CAIRNFLOW_INACTIVITY_TIMEOUT_MS", "12"),
                ("CAIRNFLOW_INVOKE_WAIT_MS", "13"),
                ("CAIRNFLOW_SLOW_LOG_MS", "14"),
                ("CAIRNFLOW_FLUSH_INTERVAL_MS", "15"),
                ("CAIRNFLOW_ENGINE_THREADS", "2"),
                ("CAIRNFLOW_WAL_KIND", "shared"),
                ("CAIRNFLOW_SHUTDOWN_TIMEOUT_MS", "16"),
                ("CAIRNFLOW_INTERNAL_PORT", "9100"),
                ("CAIRNFLOW_NO_AUTH", "x"),
                ("CAIRNFLOW_NO_UI", "1"),
                ("CAIRNFLOW_UI_BASE_PATH", "/c"),
                ("CAIRNFLOW_UI_COOKIE_SECURE", "on"),
                ("CAIRNFLOW_GC_RETIRED_DBS", "plan"),
                ("CAIRNFLOW_GC_EVERY_S", "17"),
                ("CAIRNFLOW_GC_MIN_AGE_S", "18"),
                ("CAIRNFLOW_RETIRE_ORPHANS_EVERY_S", "19"),
                ("CAIRNFLOW_RECONCILE_INTENTS_EVERY_S", "21"),
                ("CAIRNFLOW_NO_ARCHIVE", "1"),
                ("CAIRNFLOW_NO_USER_OTLP", "1"),
                ("CAIRNFLOW_AUTO_SPLIT", "on"),
                ("CAIRNFLOW_SLOW_SHED", "off"),
                ("CAIRNFLOW_QUARANTINE", "hold"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(a.main_store, "memory://");
        assert_eq!(a.wal_store.as_deref(), Some("memory://wal"));
        assert_eq!(a.port, 9000);
        assert_eq!(a.host, "0.0.0.0");
        assert_eq!(a.tick_interval_ms, 200, "a broken number in env ⇒ default");
        assert!(a.cluster, "presence is enough");
        assert_eq!(a.node_id.as_deref(), Some("n1"));
        assert_eq!(a.lease_ttl_ms, 3000);
        assert_eq!(a.partitions, 4);
        assert_eq!(a.reader_poll_ms, 77);
        assert_eq!(a.deploy_refresh_ms, 88);
        assert_eq!(a.retention_ms, 0);
        assert_eq!(a.max_concurrent_turns, 0, "env does not check ≥ 1");
        assert_eq!(a.queue_wait_ms, 11);
        assert_eq!(a.l0_soft, 3);
        assert_eq!(a.l0_max_reject, 0.5);
        assert_eq!(a.inactivity_timeout_ms, 12);
        assert_eq!(a.invoke_wait_ms, 13);
        assert_eq!(a.slow_log_ms, 14);
        assert_eq!(a.flush_interval_ms, 15);
        assert_eq!(a.engine_threads, 2);
        assert_eq!(a.shutdown_timeout_ms, 16);
        assert_eq!(a.internal_port, Some(9100));
        assert!(a.no_auth);
        assert!(a.no_ui);
        assert_eq!(a.ui_base_path, "/c");
        assert_eq!(a.ui_cookie_secure, Some(true));
        assert_eq!(a.gc_retired_dbs, Some(GcMode::Plan));
        assert_eq!(a.gc_every_s, 17);
        assert_eq!(a.gc_min_age_s, 18);
        assert_eq!(a.retire_orphans_every_s, 19);
        assert_eq!(a.reconcile_intents_every_s, 21);
        assert!(a.no_archive);
        assert_eq!(a.user_otlp, cairn_server::otel::UserExport::Off);
        assert_eq!(a.auto_split.mode, "on".parse::<AutoSplitMode>().unwrap());
        assert!(!a.slow_shed);
        assert_eq!(a.quarantine.mode, Mode::Hold);

        for (v, want) in [
            ("1", Some(GcMode::Delete)),
            ("true", Some(GcMode::Delete)),
            ("yes", None),
        ] {
            let a = parse(&[("CAIRNFLOW_GC_RETIRED_DBS", v)], &[]).unwrap();
            assert_eq!(a.gc_retired_dbs, want, "{v}");
        }
        for (k, v, e) in [
            (
                "CAIRNFLOW_WAL_KIND",
                "x",
                "--wal-kind: 'x' — only shared is accepted",
            ),
            (
                "CAIRNFLOW_SLOW_SHED",
                "maybe",
                "CAIRNFLOW_SLOW_SHED: 'maybe' — only on or off is accepted",
            ),
            (
                "CAIRNFLOW_QUARANTINE",
                "nope",
                "CAIRNFLOW_QUARANTINE: 'nope' — only off, observe, cooldown or hold is accepted",
            ),
        ] {
            assert_eq!(parse(&[(k, v)], &[]).err().as_deref(), Some(e), "{k}");
        }
        assert!(parse(&[("CAIRNFLOW_AUTO_SPLIT", "bad")], &[]).is_err());
    }

    /// Each flag: argv, then a check on `Args`. The last flag of each argv is `--port 1` —
    /// reaching it means the flag before advanced the cursor by the right number of cells.
    #[test]
    fn each_flag_sets_its_field_and_advances() {
        type Check = fn(&Args) -> bool;
        let cases: &[(&[&str], Check)] = &[
            (&["--data-dir", "/d"], |a| a.main_store == "file:///d"),
            (&["--main-store", "memory://"], |a| {
                a.main_store == "memory://"
            }),
            (&["--wal-store", "memory://w"], |a| {
                a.wal_store.as_deref() == Some("memory://w")
            }),
            (&["--host", "h"], |a| a.host == "h"),
            (&["--advertise-host", "fdaa::3"], |a| {
                a.advertise_host.as_deref() == Some("fdaa::3")
            }),
            (&["--advertise-host", "[fdaa::3]"], |a| {
                a.advertise_host.as_deref() == Some("fdaa::3")
            }),
            (&["--advertise-host", "fly-local-6pn"], |a| {
                a.advertise_host.as_deref() == Some("fly-local-6pn")
            }),
            (&["--internal-port", "7"], |a| a.internal_port == Some(7)),
            (&["--no-auth"], |a| a.no_auth),
            (&["--no-ui"], |a| a.no_ui),
            (&["--deploy-require-identity"], |a| {
                a.deploy_require_identity
            }),
            (&["--deploy-transport", "auto"], |a| {
                a.deploy_transport == cairn_server::discovery::TransportPolicy::Auto
            }),
            (&["--deploy-https-request-response"], |a| {
                a.deploy_https_request_response
            }),
            (&["--deploy-require-compression"], |a| {
                a.deploy_require_compression
            }),
            (&["--egress-deny-private"], |a| a.egress.deny_private),
            (&["--egress-require-https"], |a| a.egress.require_https),
            (&["--egress-allow-cidr", "10.1.0.0/16"], |a| {
                a.egress.allow == ["10.1.0.0/16".parse::<ipnet::IpNet>().unwrap()]
            }),
            (&["--egress-deny-cidr", "203.0.113.7"], |a| {
                a.egress.deny == ["203.0.113.7/32".parse::<ipnet::IpNet>().unwrap()]
            }),
            (&["--ui-cookie-secure", "off"], |a| {
                a.ui_cookie_secure == Some(false)
            }),
            (&["--trust-proxy", "10.0.0.0/8"], |a| {
                a.trust_proxy == cairn_server::auth::TrustedProxies::parse("10.0.0.0/8").unwrap()
            }),
            (&["--ui-env-label", "staging-eu"], |a| {
                a.ui_env_label.as_deref() == Some("staging-eu")
            }),
            (&["--ui-base-path", "/console"], |a| {
                a.ui_base_path == "/console"
            }),
            (&["--slow-shed", "off"], |a| !a.slow_shed),
            (&["--quarantine", "cooldown"], |a| {
                a.quarantine.mode == Mode::Cooldown
            }),
            (&["--quarantine-rule", "3/2/10"], |a| {
                a.quarantine.rule.failed_attempts == 3
                    && a.quarantine.rule.failed_invocations == 2
                    && a.quarantine.rule.window == Duration::from_secs(10)
            }),
            (&["--quarantine-ttl-s", "9"], |a| {
                a.quarantine.ttl == Duration::from_secs(9)
            }),
            (&["--gc-retired-dbs"], |a| {
                a.gc_retired_dbs == Some(GcMode::Delete)
            }),
            (&["--gc-retired-dbs=plan"], |a| {
                a.gc_retired_dbs == Some(GcMode::Plan)
            }),
            (&["--gc-every-s", "5"], |a| a.gc_every_s == 5),
            (&["--gc-min-age-s", "6"], |a| a.gc_min_age_s == 6),
            (&["--retire-orphans-every-s", "0"], |a| {
                a.retire_orphans_every_s == 0
            }),
            (&["--reconcile-intents-every-s", "0"], |a| {
                a.reconcile_intents_every_s == 0
            }),
            (&["--scrub-every-s", "0"], |a| a.scrub_every_s == 0),
            (&["--scrub-requests-per-hour", "0"], |a| {
                a.scrub_requests_per_hour == Some(0)
            }),
            (&["--trash-keep-s", "0"], |a| a.trash_keep_s == 0),
            (&["--value-sweep-every-s", "0"], |a| {
                a.value_sweep_every_s == 0
            }),
            (&["--value-min-age-s", "600"], |a| a.value_min_age_s == 600),
            (&["--value-read-path", "proxy"], |a| {
                a.values.read_path == cairn_server::value_urls::ReadPath::Proxy
            }),
            (&["--value-presign-ttl-s", "30"], |a| {
                a.values.ttl == Some(Duration::from_secs(30))
            }),
            (&["--tenant-purge-keep-s", "60"], |a| {
                a.tenant_purge_keep_s == 60
            }),
            (&["--no-archive"], |a| a.no_archive),
            (&["--no-rr-delta-replay"], |a| a.no_rr_delta_replay),
            (&["--no-user-otlp"], |a| {
                a.user_otlp == cairn_server::otel::UserExport::Off
            }),
            (&["--tick-interval-ms", "0"], |a| a.tick_interval_ms == 0),
            (&["--cluster"], |a| a.cluster),
            (&["--node-id", "x"], |a| a.node_id.as_deref() == Some("x")),
            (&["--partitions", "8"], |a| a.partitions == 8),
            (&["--lease-ttl-ms", "2"], |a| a.lease_ttl_ms == 2),
            (&["--reader-poll-ms", "3"], |a| a.reader_poll_ms == 3),
            (&["--deploy-refresh-ms", "4"], |a| a.deploy_refresh_ms == 4),
            (&["--retention-ms", "0"], |a| a.retention_ms == 0),
            (&["--max-concurrent-turns", "5"], |a| {
                a.max_concurrent_turns == 5
            }),
            (&["--queue-wait-ms", "0"], |a| a.queue_wait_ms == 0),
            (&["--l0-soft", "0"], |a| a.l0_soft == 0),
            (&["--l0-max-reject", "0.25"], |a| a.l0_max_reject == 0.25),
            (&["--inactivity-timeout-ms", "0"], |a| {
                a.inactivity_timeout_ms == 0
            }),
            (&["--invoke-wait-ms", "0"], |a| a.invoke_wait_ms == 0),
            (&["--flush-interval-ms", "3"], |a| a.flush_interval_ms == 3),
            (&["--engine-threads", "4"], |a| a.engine_threads == 4),
            (&["--wal-stripes", "3"], |a| a.node_wal.stripes == 3),
            (&["--wal-inflight", "2"], |a| a.node_wal.inflight == 2),
            (&["--wal-linger-us", "500"], |a| {
                a.node_wal.linger == Duration::from_micros(500)
            }),
            // 0 is still accepted: linger off, cut at once as before the default changed.
            (&["--wal-linger-us", "0"], |a| a.node_wal.linger.is_zero()),
            (&["--wal-window-ms", "6"], |a| a.wal_window_ms == Some(6)),
            (&["--wal-object-cap-bytes", "1000"], |a| {
                a.node_wal.object_cap == 1000
            }),
            (&["--wal-age-flush-ms", "7"], |a| {
                a.node_wal.frame_age_flush == Duration::from_millis(7)
            }),
            (&["--wal-put-timeout-ms", "8"], |a| {
                a.node_wal.put_timeout == Duration::from_millis(8)
            }),
            (&["--shutdown-timeout-ms", "9"], |a| {
                a.shutdown_timeout_ms == 9
            }),
            (&["--auto-split", "on"], |a| {
                a.auto_split.mode == "on".parse::<AutoSplitMode>().unwrap()
            }),
            (&["--auto-split=off"], |a| {
                a.auto_split.mode == "off".parse::<AutoSplitMode>().unwrap()
            }),
            (&["--max-range-bytes", "1024"], |a| {
                a.auto_split.max_range_bytes == 1024
            }),
        ];
        for (flags, check) in cases {
            let mut argv: Vec<&str> = flags.to_vec();
            argv.extend(["--port", "1"]);
            let a = ok(&argv);
            assert!(check(&a), "{flags:?}");
            assert_eq!(a.port, 1, "{flags:?}: the next flag was not read");
        }
    }

    /// `--slow-log-ms N` (N ≥ 1) did not advance the cursor — `cairnflow --slow-log-ms 100`
    /// looped forever at 100 % CPU and never started.
    #[test]
    fn slow_log_ms_advances() {
        let a = ok(&["--slow-log-ms", "100", "--port", "1"]);
        assert_eq!(a.slow_log_ms, 100);
        assert_eq!(a.port, 1);
    }

    /// An identity master shorter than 32 bytes: the node refuses to start instead of
    /// signing with a weak secret.
    #[test]
    fn short_identity_master_refuses_to_start() {
        let e = parse(&[("CAIRNFLOW_IDENTITY_MASTER", "too-short")], &[]).err();
        assert!(
            e.as_deref()
                .is_some_and(|e| e.contains("CAIRNFLOW_IDENTITY_MASTER")),
            "{e:?}"
        );
        let long = "m".repeat(32);
        assert!(parse(&[("CAIRNFLOW_IDENTITY_MASTER", &long)], &[]).is_ok());
    }

    #[test]
    fn main_store_wins_over_data_dir() {
        let a = ok(&["--data-dir", "/d", "--main-store", "memory://"]);
        assert_eq!(a.main_store, "memory://");
        let a = parse(&[("CAIRNFLOW_DATA_DIR", "/e")], &[]).unwrap();
        assert_eq!(a.main_store, "file:///e");
    }

    #[test]
    fn errors_keep_their_wording() {
        let cases: &[(&[&str], &str)] = &[
            (&["--bogus"], "unknown argument: --bogus"),
            (
                &["--deploy-require-compression", "--deploy-no-rr-compression"],
                "--deploy-require-compression and --deploy-no-rr-compression cannot be used \
                 together (profile saas turns on the first)",
            ),
            (
                &["--value-min-age-s", "599"],
                "--value-min-age-s: 599 is below 600 (2 × the PUT-to-commit deadline): a segment \
                 PUT just before a slow commit would be swept as an orphan",
            ),
            (&["--port"], "--port is missing a value"),
            (&["--port", "x"], "--port is not a number"),
            (&["--internal-port", "x"], "--internal-port is not a number"),
            (
                &["--advertise-host", "0.0.0.0"],
                "--advertise-host \"0.0.0.0\": unspecified address — peers cannot reach it; set \
                 the node's own IP",
            ),
            (
                &["--advertise-host", "::"],
                "--advertise-host \"::\": unspecified address — peers cannot reach it; set the \
                 node's own IP",
            ),
            (
                &["--advertise-host", "http://10.0.0.1:8080"],
                "--advertise-host \"http://10.0.0.1:8080\": only a host name or IP is accepted \
                 — no scheme, port or path",
            ),
            (
                &["--ui-env-label", " prod"],
                "--ui-env-label: environment label accepts only printable ASCII, no \
                 leading/trailing whitespace",
            ),
            (
                &["--ui-base-path", "/invocations"],
                "--ui-base-path: --ui-base-path \"/invocations\" collides with the API route \
                 /invocations — pick another name (default /ui)",
            ),
            (
                &["--max-concurrent-turns", "0"],
                "--max-concurrent-turns must be ≥ 1",
            ),
            (
                &["--l0-max-reject", "1"],
                "--l0-max-reject must be in [0, 1): the throttle never blocks everything",
            ),
            (
                &["--wal-stripes", "0"],
                "--wal-stripes must be a number ≥ 1",
            ),
            (
                &["--wal-inflight", "x"],
                "--wal-inflight must be a number ≥ 1",
            ),
            (
                &["--quarantine-ttl-s", "0"],
                "--quarantine-ttl-s must be ≥ 1",
            ),
            (
                &["--quarantine-ttl-s", "x"],
                "--quarantine-ttl-s is not a number",
            ),
            (
                &["--quarantine", "nope"],
                "--quarantine: 'nope' — only off, observe, cooldown or hold is accepted",
            ),
            (
                &["--quarantine-rule", "x"],
                "--quarantine-rule: 'x' — format attempts/invocations/seconds, e.g. 20/5/300",
            ),
            (
                &["--wal-kind", "x"],
                "--wal-kind: 'x' — only shared is accepted",
            ),
            (
                &["--slow-shed", "maybe"],
                "--slow-shed: 'maybe' — only on or off is accepted",
            ),
            (
                &["--auto-split=bad"],
                "--auto-split \"bad\": choose off, suggest or on",
            ),
            (
                &["--auto-split", "bad"],
                "--auto-split \"bad\": choose off, suggest or on",
            ),
            (
                &["--flush-interval-ms", "0"],
                "--flush-interval-ms must be ≥ 1",
            ),
            (&["--slow-log-ms", "0"], "--slow-log-ms must be ≥ 1"),
            (&["--slow-log-ms", "x"], "--slow-log-ms is not a number"),
            (&["--gc-every-s", "x"], "--gc-every-s is not a number"),
            (
                &["--wal-age-flush-ms", "0"],
                "--wal-age-flush-ms must be a number ≥ 1",
            ),
            (
                &["--shutdown-timeout-ms", "0"],
                "--shutdown-timeout-ms must be a number ≥ 1",
            ),
            (
                &["--engine-threads", "x"],
                "--engine-threads is not a number",
            ),
        ];
        for (argv, want) in cases {
            assert_eq!(parse(&[], argv).err().as_deref(), Some(*want), "{argv:?}");
        }
    }

    /// `--wal-linger-us` is capped (`cairn_store::MAX_LINGER`, 100 ms): larger values are
    /// refused at startup, like `--max-concurrent-turns 0` — before, every `u64` was
    /// accepted, and the cut deadline `head row + L` overflowed `Instant`.
    #[test]
    fn wal_linger_above_cap_is_rejected() {
        let a = parse(&[], &["--wal-linger-us", "100000"]).expect("at the cap it is accepted");
        assert_eq!(a.node_wal.linger, Duration::from_millis(100));
        for v in ["100001", "18446744073709551615"] {
            assert_eq!(
                parse(&[], &["--wal-linger-us", v]).err().as_deref(),
                Some("--wal-linger-us must be ≤ 100000 (100 ms)"),
                "{v}"
            );
        }
    }
}
