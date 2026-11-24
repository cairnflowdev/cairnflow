// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The phases of `start`: store gates, bind, lease, migration, auth, routing, background loops, HTTP.

use super::*;

/// The store gates, **before anything else**.
///
/// Transient I/O errors are retried within the boot limit; a gate only fails on the
/// store's **answer**, never on a 503.
pub(super) async fn pass_gates(
    stores: &ObjectStores,
    boot: &crate::boot::BootClock,
) -> Result<(), Error> {
    // **The D12 gate, before anything else.** SlateDB's fence against zombie leaders is
    // the object store's conditional PUT, not `writer_epoch`. A store that swallows that
    // condition runs the system with no fence at all, and it fails silently. So failing
    // the gate means **not starting**, not warning and carrying on — there is no flag to
    // skip it.
    stores.assert_conditional_put_within(&boot.policy()).await?;
    // Second gate: the lease layer renews by CAS on the etag, a different capability
    // from `PutMode::Create` above. Block it here instead of letting it blow up at the first
    // renewal, a few seconds after the node has started taking traffic. A local directory
    // passes thanks to CAS emulated under a lock ([`ObjectStores::exclusive_local`]).
    stores
        .assert_compare_and_swap_within(&boot.policy())
        .await?;
    // Third gate: an `s3://` store without a declared `atomic_put` only runs with the
    // "atomic" default on a store verified for S3-2 — on a store that tears objects,
    // reading back an ambiguous log PUT of the shared WAL loses data. An unverified store
    // ⇒ no start, like the two gates above; no skip flag other than declaring
    // `--wal-atomic-put` explicitly.
    let why = crate::store_check::vet_wal_atomic_put(stores.wal()).await?;
    tracing::info!(
        atomic_put = stores.wal().atomic_put(),
        reason = %why,
        "atomic PUT capability of the WAL tier"
    );
    // The format mark, also before anything writes to the bucket. Without this step a
    // cluster node on a bucket of the wrong format still starts, takes leases, then loops
    // forever on a store-open error — the error only shows in each partition's log.
    boot.run(&cairn_store::retry::ops::BOOT_FORMAT, || {
        stores.ensure_format()
    })
    .await?;
    Ok(())
}

/// The two bound HTTP ports, and their advertised addresses.
///
/// The listener is `None` **only** in deterministic simulation (`testkit`, with the
/// in-process network `LocalNet` installed): the node opens no real TCP port — see
/// [`bind_listeners`]. Outside of that always `Some`.
pub(super) struct Listeners {
    ingress: Option<tokio::net::TcpListener>,
    pub(super) addr: SocketAddr,
    pub(super) ingress_url: String,
    internal: Option<tokio::net::TcpListener>,
    pub(super) internal_addr: SocketAddr,
    pub(super) advertised: String,
}

/// Bind **before** building the engines, because the advertised address in the lease
/// must be the REAL port.
///
/// `port = 0`, letting the system pick the port, is the only way to run many instances on
/// one machine, and with it advertising `http://host:0` means every other node knows which
/// partitions we hold but cannot send us anything. A silent failure: a single node still
/// runs correctly, only a cluster exposes it.
///
/// `advertise` (`--advertise-host`): the host advertised instead of the bind address — the
/// node binds `0.0.0.0`/`::` (so the platform's proxy can reach it) but peers must call it
/// on a private IP (e.g. the platform's private network). The port is still the **real**
/// bound port. Absent ⇒ advertise exactly the bind address, as before.
pub(super) async fn bind_listeners(
    host: &str,
    advertise: Option<&str>,
    port: u16,
    internal_port: u16,
) -> Result<Listeners, Error> {
    // Deterministic simulation: the thread's in-process network hands out two fake
    // addresses from its counter (same build order ⇒ same addresses — system-picked ports
    // differ between runs, and the advertised address decides peer order), and the node
    // binds **no** real TCP port: a real listener opened in the sim receives stray requests
    // from other processes on the machine (nextest runs every binary at once), which get
    // into the sim's runtime and scramble its schedule.
    #[cfg(feature = "testkit")]
    if let Some((addr, internal_addr)) = crate::netfilter::LocalNet::allocate_pair() {
        let _ = (host, advertise, port, internal_port);
        return Ok(Listeners {
            ingress: None,
            addr,
            ingress_url: format!("http://{addr}"),
            internal: None,
            internal_addr,
            advertised: format!("http://{internal_addr}"),
        });
    }
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    let addr = listener.local_addr()?;
    let url = |a: SocketAddr| match advertise {
        Some(h) => format!("http://{}:{}", bracketed(h), a.port()),
        None => format!("http://{a}"),
    };
    let ingress_url = url(addr);
    // Internal port: `/internal/*` only here, and **this** is the address advertised to
    // peers — inter-range messages, hand-over requests, deployment counts. The ingress
    // address is advertised alongside, but only to route clients.
    let internal_listener =
        tokio::net::TcpListener::bind(format!("{host}:{internal_port}")).await?;
    let internal_addr = internal_listener.local_addr()?;
    let advertised = url(internal_addr);
    Ok(Listeners {
        ingress: Some(listener),
        addr,
        ingress_url,
        internal: Some(internal_listener),
        internal_addr,
        advertised,
    })
}

/// Host in a URL: an IPv6 literal needs `[]`.
fn bracketed(h: &str) -> String {
    if h.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{h}]")
    } else {
        h.to_string()
    }
}

/// Check `--advertise-host`: one host (a name or an IP, IPv6 with or without `[]`), no
/// scheme, no port, no path, and **not** an unspecified address — advertising `0.0.0.0`
/// is a silent failure (no peer can reach it), exactly what this flag exists to avoid.
/// Returns the normalised host (IPv6 without `[]`); `Err` = the reason, the caller adds
/// the flag name.
pub fn check_advertise_host(h: &str) -> Result<String, String> {
    let bad = |why: &str| Err(why.to_string());
    let (bare, bracketed) = match h.strip_prefix('[').and_then(|x| x.strip_suffix(']')) {
        Some(x) => (x, true),
        None => (h, false),
    };
    if bare.is_empty() {
        return bad("empty");
    }
    if bracketed && bare.parse::<std::net::Ipv6Addr>().is_err() {
        return bad("`[]` only wraps an IPv6 address");
    }
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        if ip.is_unspecified() {
            return bad("unspecified address — peers cannot reach it; set the node's own IP");
        }
        return Ok(ip.to_string());
    }
    let host_char = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '.';
    if !bare.chars().all(host_char) {
        return bad("only a host name or IP is accepted — no scheme, port or path");
    }
    Ok(bare.to_string())
}

/// The node's lease configuration, TTL, and PUT meter.
pub(super) fn lease_setup(
    mode: &NodeMode,
    ports: &Listeners,
    knows: u32,
    owner_watch: Option<Duration>,
    node_wal: &mut NodeWalConfig,
    slow: &crate::slow::SlowConfig,
) -> (LeaseConfig, Duration, cairn_store::health::SlowMeter) {
    let NodeMode {
        lease_ttl, node_id, ..
    } = mode;
    let ttl = *lease_ttl;
    let mut cfg = LeaseConfig::new()
        .with_address(ports.advertised.clone())
        .with_ingress(ports.ingress_url.clone())
        .with_ttl(ttl)
        // Renew at 1/5 TTL: the self-demotion deadline is at 2/3 TTL, so this tick
        // gives us three attempts before reaching it. One miss from a network blip must
        // not turn into a failover.
        .with_renew_interval(ttl / 5)
        // Observe other nodes' sessions at 1/3 TTL: each round is one GET per peer —
        // when idle that is most of the cluster's read cost. It only changes when a dead
        // node is **detected** (at most TTL/3 − TTL/5 later), not who holds authority.
        .with_observe_interval(ttl / 3)
        // Sessions run on the process's multi-threaded runtime, **not** on any partition's
        // current_thread thread: a busy partition (or a synchronous block inside an
        // engine) must not make the whole node miss its renewal tick.
        .with_runtime(tokio::runtime::Handle::current())
        .with_knows(knows);
    if let Some(id) = node_id.clone() {
        cfg = cfg.with_node_id(id);
    }
    if let Some(every) = owner_watch {
        cfg = cfg.with_owner_watch(every);
    }
    // The node's PUT meter: session renewals and the shared WAL's PUT attempts are
    // measured in one place.
    let meter = cairn_store::health::SlowMeter::new(cfg.node_id.clone(), slow.slow_put);
    node_wal.meter = Some(meter.clone());
    (cfg.with_meter(meter.clone()), ttl, meter)
}

/// **Cluster-level migration**, after the format mark and before every other object:
/// `cluster-version` newer than the book ⇒ no start; missing steps ⇒ one node takes the
/// `upgrading` lock and runs them, other nodes wait (if the lock holder's session dies
/// they take over). With a complete book it is a single GET — an absent
/// `cluster-version` ≡ 0, format 9 buckets pass.
///
/// **A new bucket**: the creator writes the `control/quiet-lease` gate up front from
/// `quiet_lease` (`--quiet-lease`, delegation by default) — for `s3://`
/// after the G18 probe ([`cairn_store::migrate::NewCluster`]). An existing bucket: the
/// flag is ignored, logged at `debug` when it differs from the current gate.
pub(super) async fn migrate_cluster(
    stores: &ObjectStores,
    session: &Session,
    migrations: &'static [cairn_store::migrate::Migration],
    quiet_lease: cairn_store::quiet_lease::Bootstrap,
    ttl: Duration,
    boot: &crate::boot::BootClock,
) -> Result<(), Error> {
    // Registry generation 0 for a new bucket: without it readers treat absent as
    // "unknown" and run a full registry round every tick until the first registry
    // write.
    let gen = crate::registry_gen::RegistryGen::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
        Some(session.clone()),
    );
    let seed_gen: cairn_store::migrate::FreshHook = Arc::new(move || {
        let gen = gen.clone();
        Box::pin(async move {
            if gen
                .create_initial()
                .await
                .map_err(|e| cairn_store::StoreError::Io(e.to_string()))?
            {
                tracing::info!("new bucket: creating control/registry-gen generation 0");
            }
            Ok(())
        })
    });
    let new = cairn_store::migrate::NewCluster {
        quiet_lease,
        probe_reader: probe_reader(stores),
        on_fresh: vec![seed_gen],
    };
    // Repeatable: dying (or failing) midway means the next run sees our own `upgrading`
    // lock and continues from the unfinished step (`migrate::run_cluster`).
    boot.run(&cairn_store::retry::ops::BOOT_MIGRATE, || {
        cairn_store::migrate::run_cluster_with(
            stores.main().store(),
            stores.main().prefix(),
            cairn_store::migrate::Runner::Session(session),
            migrations,
            ttl / 5,
            &new,
        )
    })
    .await?;
    // One GET at start-up, only to report an ignored flag; on failure, never mind.
    if let Ok(gate) =
        cairn_store::quiet_lease::read(stores.main().store(), stores.main().prefix()).await
    {
        let have =
            cairn_store::quiet_lease::Bootstrap::of(gate.as_ref().and_then(|(q, _)| q.as_ref()));
        if have != quiet_lease {
            tracing::debug!(
                flag = quiet_lease.as_str(),
                gate = have.as_str(),
                "--quiet-lease only takes effect when creating a new bucket — this bucket already has another gate, ignored (change it with cairn-ctl cluster enable-/disable-quiet-lease)"
            );
        }
    }
    Ok(())
}

/// The read client for the new-bucket creator's G18 probe: `s3://` only, and it is
/// **the node's own client** — no second client built from the URI as `cairn-ctl` does:
/// the node's store may be wrapped (an S3 emulation profile, failpoints) or injected
/// (tests), and a client rebuilt from the URI would then point elsewhere. What the probe
/// must catch — a caching proxy or a weakly consistent store in front of the bucket —
/// sits between every client and the bucket, so the same client still sees the stale
/// `304`. `memory://` and `file://` are in-process stores (G18 pinned in
/// `library_contracts.rs`) — not probed.
fn probe_reader(stores: &ObjectStores) -> Option<Arc<dyn slatedb::object_store::ObjectStore>> {
    let main = stores.main();
    main.uri()
        .starts_with("s3://")
        .then(|| Arc::clone(main.store()))
}

/// API key store: load it, then block start-up if auth is on without any key —
/// nobody could call anything, and that error must show up when the command is typed.
pub(super) async fn resolve_auth(
    stores: &ObjectStores,
    auth: &crate::auth::AuthConfig,
    boot: &crate::boot::BootClock,
) -> Result<crate::auth::Auth, Error> {
    // A corrupt hash or two roles with the same secret ⇒ no start.
    let env = crate::auth::EnvKeys::resolve(auth)?;
    let keys = crate::auth::Keys::new(
        Some((Arc::clone(stores.main().store()), stores.main().prefix())),
        auth.admin_key.as_deref(),
    )
    .with_metrics_key(auth.metrics_key.as_deref())
    .with_env_hashes(&env);
    boot.run_text(&cairn_store::retry::ops::BOOT_AUTH, || keys.refresh())
        .await?;
    if auth.required && keys.is_empty() {
        return Err(
            "auth is on but the cluster has no API key yet: set CAIRNFLOW_ADMIN_KEY (bootstrap key, role cluster-admin) or its hash in CAIRNFLOW_ADMIN_KEY_SHA256, or the control plane key hash in CAIRNFLOW_PROVISION_KEY_SHA256, then create keys via POST /admin/keys — or run --no-auth for a dev environment"
                .into(),
        );
    }
    if !auth.required {
        tracing::warn!(
            "AUTH OFF (--no-auth): anyone who reaches the ingress port can call every API, including cluster administration — dev only"
        );
    }
    Ok(crate::auth::Auth {
        required: auth.required,
        keys,
        // Attached in `node::start` once the registry change notifier exists.
        registry: None,
        // Attached in `node::start` together with the UI configuration.
        sessions: None,
        // Attached in `node::start` together with the egress policy.
        oidc: None,
        // Attached in `node::start` together with the audit sink.
        denials: None,
        // Attached in `node::start` once the tenants book has been read.
        tenant_states: None,
        // Attached in `node::start` from `NodeConfig::key_rate`.
        key_rate: None,
        bearer_failures: crate::auth::bearer_failure_limiter(),
    })
}

/// The routing table must exist **before** the engines: the transport uses it to know
/// where to send inter-partition messages. It is built from the `owns` hints in the
/// nodes' sessions — no channel of its own to build and to break, and its cost does not
/// grow with the number of partitions. A node on its own as well: it sees its own
/// session.
pub(super) fn build_routing(
    session: &Session,
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    ttl: Duration,
    tasks: &mut Tasks,
) -> RoutingTable {
    let routing = RoutingTable::new();
    // Rebuilt whenever the tracker reports a change; `ttl` is only a safety net.
    tasks.push(routing.clone().follow(
        session.clone(),
        Arc::clone(stores.main().store()),
        stores.main().prefix().clone(),
        cluster.clone(),
        ttl,
    ));
    routing
}

/// Poison-handler quarantine: a cluster-level list, loaded before building the engines so
/// the very first invocation already sees `hold`. Detection runs every second on the
/// numbers of the ranges this node leads (the same tick as the summary collection);
/// re-read from the bucket on a peer's notification and on the deployment registry
/// refresh tick; after writing, peers are notified through `sync`.
pub(super) async fn start_quarantine(
    handler_stats: &crate::stats::HandlerStats,
    sync: &crate::registry_sync::RegistrySync,
    tenant_settings: &crate::tenants::TenantSettings,
    stores: &ObjectStores,
    quarantine: crate::quarantine::QuarantineConfig,
    deploy_refresh: Duration,
    tasks: &mut Tasks,
) {
    let q = handler_stats.quarantine();
    q.configure(quarantine);
    q.attach(crate::quarantine::QuarantineStore::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
    ));
    q.attach_sync(sync.clone());
    if let Err(e) = q.refresh().await {
        tracing::warn!(error = %e, "cannot read quarantine/ — the quarantine list stays empty until the next tick");
    }
    let settings = tenant_settings.clone();
    let policy = move |tenant: &str| {
        let (mode, rule) = settings.quarantine(tenant);
        (
            mode.unwrap_or(quarantine.mode),
            rule.unwrap_or(quarantine.rule),
            quarantine.ttl,
        )
    };
    tasks.push(
        q.spawn(
            handler_stats.clone(),
            policy,
            Duration::from_secs(1),
            deploy_refresh
                .min(QUARANTINE_REFRESH_MAX)
                .max(Duration::from_millis(100)),
        ),
    );
}

/// One engine per range, each competing for its own lease.
///
/// The node opens engines for **every** range of the table, not only those it wins: a
/// losing engine answers `NotLeader` and stands by, and it must be there to take
/// leadership when the holding node dies. The price is one thread per range — folding
/// them onto fewer threads is a later step.
///
/// **The engine set follows the table**: a new range appears ⇒ add an engine,
/// a range disappears ⇒ its engine exits for good. At start-up we build for the table as
/// it is; later it is the job of the `follow_table` loop.
///
/// `start` waits for no lease take: the node may be a follower and must still serve
/// HTTP. A command to a range nobody leads yet is held by ingress (bounded) until there
/// is a leader (`AppState::ask_led`).
pub(super) async fn spawn_engines(
    spawner: RangeSpawner,
    cluster: &ClusterConfig,
    ttl: Duration,
    tasks: &mut Tasks,
) -> Result<Partitions, Error> {
    let handles = Partitions::default();
    for partition in cluster.partitions() {
        handles.insert(
            partition,
            spawner.spawn(partition, Readiness::Immediately).await?,
        );
    }
    tasks.push(follow_table(spawner, handles.clone(), ttl));
    Ok(handles)
}

/// The state scrubber ([`crate::scrub`]): dry, on the same runner as D-d, and asking at
/// the same tick as D-d: it asks the runner every [`crate::gc::RUNNER_CHECK_EVERY`] and
/// only rests **after** a round that ran — a node that just started or just became
/// scrubber runs within seconds, without waiting a whole tick. It rests `every`, or longer
/// per the request budget (`budget`, [`crate::scrub::next_wait`]).
#[allow(clippy::too_many_arguments, reason = "passed straight from NodeConfig")]
pub(super) fn spawn_scrub(
    scrub_every: Option<Duration>,
    budget: Option<u64>,
    gc_retired_dbs: Option<&crate::gc::GcConfig>,
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    handles: &Partitions,
    last: crate::scrub::ScrubLast,
    tasks: &mut Tasks,
) {
    if let Some(every) = scrub_every {
        let every = every.max(Duration::from_secs(1));
        let budget = budget.unwrap_or_else(|| crate::scrub::default_budget(every));
        let cfg = crate::scrub::ScrubConfig {
            every,
            gc_min_age: gc_retired_dbs
                .filter(|g| g.mode == crate::gc::GcMode::Delete)
                .map(|g| g.min_age),
        };
        let stores = stores.clone();
        let cluster = cluster.clone();
        let handles = handles.clone();
        tasks.push(tokio::spawn(cairn_store::io_source::tagged(
            "scrub",
            async move {
                let mut scrubber = crate::scrub::Scrubber::default();
                let check = every.min(crate::gc::RUNNER_CHECK_EVERY);
                let mut wait = check;
                loop {
                    cairn_store::pace::beat(wait).await;
                    if !crate::gc::is_runner(&cluster, &handles).await {
                        wait = check;
                        continue;
                    }
                    crate::scrub::stats::note_runner(crate::scrub::wall_ms());
                    match scrubber.run_once(&stores, &cfg).await {
                        Ok(round) => last.record(&round),
                        Err(e) => {
                            tracing::warn!(error = %e, "state scrubber failed, retrying on the next tick")
                        }
                    }
                    wait = crate::scrub::next_wait(every, budget, scrubber.last_requests());
                    crate::scrub::stats::WAIT_MS
                        .store(wait.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
                    if wait > every {
                        tracing::debug!(
                            request = scrubber.last_requests(),
                            budget = budget,
                            wait_s = wait.as_secs(),
                            "state scrubber: interval stretched by the request budget"
                        );
                    }
                }
            },
        )));
    }
}

/// The tenant purge round ([`crate::tenant_purge::Purger`]): on the D-d runner, at a
/// `keep / 8` tick clamped to [1 s, 5 min], and **right away** when the cluster-level
/// registry changes (`RegistrySync::tenant_purge_notified` — `PUT …/state` on this node
/// or a peer's notification): phase A's hold entries appear, and are removed when it is
/// reversed, within one round after `PUT …/state` instead of up to 5 minutes. With no
/// tenant `deleting`, one round is one GET of the `tenants` book and one LIST of `trash/`.
/// While a full compaction it requested is not finished, the tick is at most
/// [`crate::tenant_purge::COMPACTION_POLL`] (G31). `None` = off.
pub(super) fn spawn_tenant_purge(
    purger: Option<Arc<crate::tenant_purge::Purger>>,
    sync: &crate::registry_sync::RegistrySync,
    cluster: &ClusterConfig,
    handles: &Partitions,
    tasks: &mut Tasks,
) {
    let Some(purger) = purger else {
        return;
    };
    let every = (purger.keep / 8).clamp(Duration::from_secs(1), Duration::from_secs(300));
    let sync = sync.clone();
    let cluster = cluster.clone();
    let handles = handles.clone();
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "gc.tenant_purge",
        async move {
            let check = every.min(crate::gc::RUNNER_CHECK_EVERY);
            let mut wait = check;
            loop {
                tokio::select! {
                    () = cairn_store::pace::beat(wait) => {}
                    () = sync.tenant_purge_notified() => {}
                }
                if !crate::gc::is_runner(&cluster, &handles).await {
                    wait = check;
                    continue;
                }
                match purger.run_once().await {
                    Ok(rounds) => {
                        for r in rounds.iter().filter(|r| r.blocked.is_some()) {
                            tracing::debug!(tenant = %r.tenant, phase = %r.phase, reason = ?r.blocked, "tenant purge: not finished yet");
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "tenant purge pass failed, retrying on the next tick"),
                }
                wait = if purger.awaiting_compaction() {
                    every.min(crate::tenant_purge::COMPACTION_POLL)
                } else {
                    every
                };
            }
        },
    )));
}

/// The trash purge round ([`crate::gc::purge_trash_once`]): the D-d runner really deletes
/// entries past `keep` after checking again. Enabled independently of the D-d flag —
/// split/merge roll-backs and the shared WAL put things in the trash even when D-d is off.
/// Tick `keep / 8`, clamped to [1 s, 1 h]; asks the runner like D-d: the first round
/// right when the node starts or takes the role.
/// It only changes **when** the round runs — each entry is still only deleted after
/// being checked again (rule 5).
pub(super) fn spawn_trash_purge(
    keep: Option<Duration>,
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    handles: &Partitions,
    tasks: &mut Tasks,
) {
    let Some(keep) = keep else {
        return;
    };
    let every = (keep / 8).clamp(Duration::from_secs(1), Duration::from_secs(3600));
    let stores = stores.clone();
    let cluster = cluster.clone();
    let handles = handles.clone();
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "gc.trash",
        async move {
            let check = every.min(crate::gc::RUNNER_CHECK_EVERY);
            let mut wait = check;
            loop {
                cairn_store::pace::beat(wait).await;
                if !crate::gc::is_runner(&cluster, &handles).await {
                    wait = check;
                    continue;
                }
                wait = match crate::gc::purge_trash_once(&stores, &cluster, keep).await {
                    // The round stopped at its limit (`PURGE_ROUND_MAX_OBJECTS`): entries past
                    // due remain — the next round after one role-check tick, re-reading the durable facts.
                    Ok(round) if round.capped => check,
                    Ok(_) => every,
                    Err(e) => {
                        tracing::warn!(error = %e, "trash cleanup pass failed, retrying on the next tick");
                        every
                    }
                };
            }
        },
    )));
}

/// The audit reconcile round ([`crate::audit::reconcile_intents`]): on the D-d runner, every
/// [`AUDIT_RECONCILE_EVERY`] — an intent left without an outcome (the node died between
/// intent and outcome, or the outcome PUT failed) gets the outcome `unknown` once it is
/// [`crate::audit::RECONCILE_AFTER_MS`] old. Asks the runner like D-d.
pub(super) fn spawn_audit_reconcile(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    handles: &Partitions,
    tasks: &mut Tasks,
) {
    let main = stores.main();
    let store = Arc::clone(main.store());
    let prefix = main.prefix().clone();
    let cluster = cluster.clone();
    let handles = handles.clone();
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "audit.reconcile",
        async move {
            let check = AUDIT_RECONCILE_EVERY.min(crate::gc::RUNNER_CHECK_EVERY);
            let mut wait = check;
            loop {
                cairn_store::pace::beat(wait).await;
                if !crate::gc::is_runner(&cluster, &handles).await {
                    wait = check;
                    continue;
                }
                let now = cairn_store::entropy::now_ms();
                match crate::audit::reconcile_intents(&store, &prefix, now).await {
                    Ok(r) if r.written > 0 => {
                        tracing::info!(written = r.written, "audit: intents without an outcome recorded as unknown");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "audit reconcile pass failed, retrying on the next tick");
                    }
                }
                wait = AUDIT_RECONCILE_EVERY;
            }
        },
    )));
}

/// Tick of [`spawn_audit_reconcile`].
pub(super) const AUDIT_RECONCILE_EVERY: Duration = Duration::from_secs(600);

/// The artifact sweep round ([`crate::artifact_sweep`]):
/// every node, each node only touches ids of the ranges it leads. `every_keep` = (tick,
/// retention); `None` ⇒ does not run.
pub(super) fn spawn_artifact_sweep(
    every_keep: Option<(Duration, Duration)>,
    min_age: Duration,
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    handles: &Partitions,
    tasks: &mut Tasks,
) {
    let Some((every, keep)) = every_keep else {
        return;
    };
    let main = stores.main();
    let artifacts =
        cairn_store::artifact::ArtifactStore::new(Arc::clone(main.store()), main.prefix());
    let trash = stores.main_trash();
    let cluster = cluster.clone();
    let handles = handles.clone();
    let every = every.max(Duration::from_millis(50));
    // Limit for the whole round: wide enough for MAX_PAGES pages, so one stuck round does
    // not block the next one forever.
    let bound = (every * 4).max(Duration::from_secs(300));
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "artifact.sweep",
        async move {
            // Where the previous round stopped (`artifact_sweep::Sweep::once`); lives
            // in RAM, a reopened node sweeps from the start.
            let mut cursor = None;
            loop {
                cairn_store::pace::beat(every).await;
                let sweep = crate::artifact_sweep::Sweep {
                    artifacts: &artifacts,
                    trash: &trash,
                    cluster: &cluster,
                    handles: &handles,
                    min_age,
                    keep,
                    now_ms: cairn_store::entropy::now_ms(),
                };
                match cairn_store::retry::deadline(
                    &cairn_store::retry::ops::ARTIFACT_SWEEP,
                    bound,
                    sweep.once(&mut cursor),
                )
                .await
                {
                    Ok(Ok(r)) if r != Default::default() => {
                        tracing::debug!(round = ?r, "artifact sweep pass")
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "artifact sweep pass failed, retrying on the next tick")
                    }
                    Err(e) => tracing::warn!(error = %e, "artifact sweep pass exceeded its limit"),
                }
            }
        },
    )));
}

/// The value sweep round ([`crate::value_sweep`]): like
/// [`spawn_artifact_sweep`] — every node, each node only touches owners of the ranges it
/// leads. `every_keep` = (tick, retention); `None` ⇒ does not run.
pub(super) fn spawn_value_sweep(
    every_keep: Option<(Duration, Duration)>,
    min_age: Duration,
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    handles: &Partitions,
    tasks: &mut Tasks,
) {
    let Some((every, keep)) = every_keep else {
        return;
    };
    let main = stores.main();
    let values = cairn_store::value::ValueStore::new(Arc::clone(main.store()), main.prefix());
    let trash = stores.main_trash();
    let cluster = cluster.clone();
    let handles = handles.clone();
    let every = every.max(Duration::from_millis(50));
    let bound = (every * 4).max(Duration::from_secs(300));
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "value.sweep",
        async move {
            let mut cursor = None;
            loop {
                cairn_store::pace::beat(every).await;
                let sweep = crate::value_sweep::Sweep {
                    values: &values,
                    trash: &trash,
                    cluster: &cluster,
                    handles: &handles,
                    min_age,
                    keep,
                    now_ms: cairn_store::entropy::now_ms(),
                };
                match cairn_store::retry::deadline(
                    &cairn_store::retry::ops::VALUE_SWEEP,
                    bound,
                    sweep.once(&mut cursor),
                )
                .await
                {
                    Ok(Ok(r)) if r != Default::default() => {
                        tracing::debug!(round = ?r, "value sweep pass")
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "value sweep pass failed, retrying on the next tick")
                    }
                    Err(e) => tracing::warn!(error = %e, "value sweep pass exceeded its limit"),
                }
            }
        },
    )));
}

/// The two cleanup rounds running on the leader of the lowest range ([`crate::gc::is_runner`]).
pub(super) fn spawn_gc_loops(
    gc_retired_dbs: Option<crate::gc::GcConfig>,
    retire_orphans_every: Option<Duration>,
    session: &Session,
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    handles: &Partitions,
    tasks: &mut Tasks,
) {
    // D-d, behind its flag: delete the Db of a retired range once no live range lists it
    // in `external_dbs` any more. Only the leader of the lowest range runs it
    // ([`crate::gc::is_runner`]).
    if let Some(gc) = gc_retired_dbs {
        let stores = stores.clone();
        let cluster = cluster.clone();
        let handles = handles.clone();
        tasks.push(tokio::spawn(cairn_store::io_source::tagged(
            "gc.dbs",
            async move {
                // Ask the runner on a short tick, only rest a full `every` after a round
                // that ran ([`crate::gc::RUNNER_CHECK_EVERY`]).
                let every = gc.every.max(Duration::from_secs(1));
                let check = every.min(crate::gc::RUNNER_CHECK_EVERY);
                let mut wait = check;
                loop {
                    cairn_store::pace::beat(wait).await;
                    if !crate::gc::is_runner(&cluster, &handles).await {
                        wait = check;
                        continue;
                    }
                    if let Err(e) = crate::gc::run_once(&stores, &cluster, &gc).await {
                        tracing::warn!(error = %e, "D-d pass failed, retrying on the next tick");
                    }
                    wait = every;
                }
            },
        )));
    }

    // Tombstones on behalf (SM-11): a range that left the table whose leader died between
    // the table commit and writing the tombstone is never taken again by anyone — the D-d
    // runner writes it instead, when provable. The same round also cleans up the orphan
    // children of a roll-back (SM-14): that is the roll-back path's job, not deleting retired
    // Dbs, so it is not behind the D-d flag.
    //
    // Runs on **nudges**, `every` is only a net:
    // (a) the table just finished a split/merge — commit or roll-back
    // ([`crate::gc::intent_settled`]); (b) the tracker saw another session end — a dead
    // owner may have left a tombstone undone; (c) the node just became the runner. A nudge
    // arriving while the node is not the runner is dropped: the real runner gets it too,
    // and whoever becomes runner later runs a round at once (c). A round left with work
    // after a possibly transient error retries with exponential backoff, from
    // [`crate::gc::ORPHAN_RETRY_FIRST`] up to the net.
    if let Some(every) = retire_orphans_every {
        let session = session.clone();
        let stores = stores.clone();
        let cluster = cluster.clone();
        let handles = handles.clone();
        // Nudge (b) rests on bucket facts: our tracker (full, the coordinator's, or
        // sparse) read/CASed the ended state, **or** a `view` reports a session ended for
        // the first time — the `view` carries the coordinator's tracker state, so it also
        // comes from the bucket. A nudge only opens a round; the round decides "dead" by
        // itself through `is_session_dead`.
        let mut ended = session.membership().ended();
        let mut tables = cluster.subscribe();
        tasks.push(tokio::spawn(cairn_store::io_source::tagged(
            "gc.orphans",
            async move {
                let mut retirer = crate::gc::OrphanRetirer::default();
                let every = every.max(Duration::from_millis(100));
                let check = every.min(crate::gc::RUNNER_CHECK_EVERY);
                let mut seen = tables.borrow_and_update().clone();
                let (mut was_runner, mut nudged) = (false, false);
                let mut due = tokio::time::Instant::now() + every;
                let mut retry: Option<cairn_store::pace::Backoff> = None;
                loop {
                    let wait = due
                        .saturating_duration_since(tokio::time::Instant::now())
                        .min(check);
                    // `biased`: the order only matters for a deterministic sim.
                    tokio::select! {
                        biased;
                        r = ended.changed() => {
                            if r.is_err() {
                                return;
                            }
                            nudged = true;
                        }
                        r = tables.changed() => {
                            if r.is_err() {
                                return;
                            }
                            let now = tables.borrow_and_update().clone();
                            nudged |= crate::gc::intent_settled(&seen, &now);
                            seen = now;
                        }
                        () = tokio::time::sleep(wait) => {}
                    }
                    if !crate::gc::is_runner(&cluster, &handles).await {
                        (was_runner, nudged) = (false, false);
                        continue;
                    }
                    let became = !std::mem::replace(&mut was_runner, true);
                    if !(became || nudged || tokio::time::Instant::now() >= due) {
                        continue;
                    }
                    nudged = false;
                    let round = retirer.run_once(&stores, &cluster, &session).await;
                    let pause = match round {
                        Ok(r) if !r.needs_retry() => {
                            retry = None;
                            every
                        }
                        Ok(r) => {
                            tracing::debug!(round = ?r, "tombstones on behalf of others: work left, retrying soon");
                            retry
                                .get_or_insert(cairn_store::pace::Backoff::new(
                                    crate::gc::ORPHAN_RETRY_FIRST.min(every),
                                    every,
                                ))
                                .next_pause()
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "tombstone-on-behalf pass failed, retrying soon");
                            retry
                                .get_or_insert(cairn_store::pace::Backoff::new(
                                    crate::gc::ORPHAN_RETRY_FIRST.min(every),
                                    every,
                                ))
                                .next_pause()
                        }
                    };
                    due = tokio::time::Instant::now() + pause;
                }
            },
        )));
    }
}

/// Per-tenant metering: each node flushes its own numbers, one object per
/// (tenant, hour) — nobody writes another node's object.
pub(super) fn spawn_metering(
    stores: &ObjectStores,
    node_id: &str,
    metering_flush: Duration,
    tasks: &mut Tasks,
) -> crate::metering::Flusher {
    let metering = crate::metering::Flusher::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix().clone(),
        node_id.to_string(),
    );
    {
        let metering = metering.clone();
        let every = metering_flush.max(Duration::from_millis(100));
        let (store, prefix) = (
            Arc::clone(stores.main().store()),
            stores.main().prefix().clone(),
        );
        tasks.push(tokio::spawn(cairn_store::io_source::tagged(
            "metering",
            async move {
                // The token price table: loaded at start, then on every flush tick.
                crate::pricing::refresh(&store, &prefix).await;
                loop {
                    cairn_store::pace::beat(every).await;
                    metering.flush().await;
                    crate::pricing::refresh(&store, &prefix).await;
                }
            },
        )));
    }
    metering
}

/// Durable statistics: on the same tick as metering.
pub(super) fn spawn_stats_history(
    stores: &ObjectStores,
    node_id: &str,
    stats: &crate::stats::HandlerStats,
    every: Duration,
    tasks: &mut Tasks,
) -> crate::stats_history::History {
    let history = crate::stats_history::History::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix().clone(),
        node_id.to_string(),
        stats.clone(),
    );
    let h = history.clone();
    let every = every.max(Duration::from_millis(100));
    tasks.push(tokio::spawn(cairn_store::io_source::tagged(
        "stats-history",
        async move {
            loop {
                cairn_store::pace::beat(every).await;
                h.flush().await;
            }
        },
    )));
    history
}

/// Build the router of both ports and run them. Returns the server task and its shutdown handle.
pub(super) fn serve_http(
    state: AppState,
    ports: Listeners,
    node_id: &str,
    header_timeout: Duration,
) -> (JoinHandle<std::io::Result<()>>, oneshot::Sender<()>) {
    let app = router(state.clone());
    // The view tables of `/query` run `GET`s on this very router. The query service
    // only holds a `Weak` (the router holds `AppState`, `AppState` holds the service):
    // the strong copy lives in the server task, shutting the server down frees the cycle.
    let view_router = Arc::new(app.clone());
    if let Some(q) = state.query() {
        q.set_router(Arc::downgrade(&view_router));
    }
    // `/internal/forward` runs forwarded requests on that same router — the same `Weak`,
    // for the same reason.
    state.ingress_slot().set(Arc::downgrade(&view_router));
    let internal_app = crate::ingress::internal_router(state);
    // Node-conditioned failpoints: the handlers of both ports run with this node as the
    // current node — split/merge, inbound, admin.
    let (app, internal_app) = if cairn_store::failpoint::enabled() {
        let scope = |node: Arc<str>| {
            axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let node = Arc::clone(&node);
                    async move { cairn_store::failpoint::in_node(&node, next.run(req)).await }
                },
            )
        };
        let node: Arc<str> = Arc::from(node_id);
        (
            app.layer(scope(Arc::clone(&node))),
            internal_app.layer(scope(node)),
        )
    } else {
        (app, internal_app)
    };
    // Deterministic simulation: the thread's in-process network (if any) takes both of the
    // node's ports — see `netfilter::LocalNet`.
    #[cfg(feature = "testkit")]
    {
        crate::netfilter::LocalNet::serve(&ports.addr.to_string(), app.clone());
        crate::netfilter::LocalNet::serve(&ports.internal_addr.to_string(), internal_app.clone());
    }
    let Listeners {
        ingress: listener,
        internal: internal_listener,
        ..
    } = ports;
    let (stop_server, stopped) = oneshot::channel::<()>();
    // Two listeners, one lifetime: shutting down shuts both, and one dying (an I/O
    // error) stops the whole node — a node without its internal port is no longer a
    // cluster member, even if ingress still answers.
    let server = tokio::spawn(async move {
        let _view_router = view_router;
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            let _ = stopped.await;
            let _ = stop_tx.send(true);
        });
        let (Some(listener), Some(internal_listener)) = (listener, internal_listener) else {
            // Deterministic simulation (see `Listeners`): no real port, the router only
            // lives in `LocalNet`; the server "runs" until told to shut down.
            let mut rx = stop_rx;
            let _ = rx.wait_for(|v| *v).await;
            return Ok(());
        };
        // Its own accept loop: HTTP/1 only, a header read limit, `ConnectInfo` for
        // per-IP limits, a bounded graceful shutdown — see `node/http.rs`.
        tokio::try_join!(
            super::http::serve(listener, app, header_timeout, stop_rx.clone()),
            super::http::serve(internal_listener, internal_app, header_timeout, stop_rx),
        )
        .map(|_| ())
    });
    (server, stop_server)
}
