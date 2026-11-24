// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `start`: the single assembly path of a node.

use super::*;

/// Build and run a node. Returns once the HTTP server accepts connections.
///
/// The phases run in exactly the order below, and the order is part of the contract: the
/// D12 gate and the format mark before any write, bind before the lease (the advertised
/// address must be the real port), the session before migration, migration before the range
/// table, auth before serving, the deployment registry before the engines, the engines before HTTP.
pub async fn start(cfg: NodeConfig) -> Result<RunningNode, Error> {
    let NodeConfig {
        stores,
        mode,
        host,
        advertise_host,
        port,
        internal_port,
        auth,
        tick_interval,
        limits,
        retention_ms: retention,
        archive,
        archive_artifacts,
        artifact_put_deadline,
        invoke_wait,
        attach_max_wait,
        schedule_min_period,
        invoke_poll_floor,
        reader_poll,
        deploy_refresh,
        tenant_usage_every,
        audit_denials_every,
        registry_reconcile,
        registry_stuck,
        owner_watch,
        metering_flush,
        gc_retired_dbs,
        retire_orphans_every,
        reconcile_intents_every,
        scrub_every,
        scrub_requests_per_hour,
        trash_keep,
        tenant_purge_keep,
        artifact_sweep,
        artifact_min_age,
        value_sweep,
        value_min_age,
        flush_interval,
        engine_threads,
        mut node_wal,
        slow_log,
        migrations,
        #[cfg(feature = "testkit")]
        net_filter,
        #[cfg(feature = "testkit")]
        inline_engines,
        #[cfg(feature = "testkit")]
        view_suspects,
        auto_split,
        lineage,
        slow,
        quarantine,
        quiet_lease,
        streams,
        http_header_timeout,
        query_limits,
        query_tenant_budget,
        tenant_defaults,
        key_rate,
        query_threads,
        query_worker,
        disk_cache,
        ui,
        egress,
        identity_master,
        deploy_require_identity,
        deploy_transport,
        deploy_https_request_response,
        deploy_no_rr_compression,
        deploy_require_compression,
        values,
    } = cfg;
    // A caller's query must run in a child process but the node has no query runtime to
    // serve it ⇒ do not start — never silently run it in place (a security requirement;
    // `--profile saas` has no fallback).
    {
        #[cfg(feature = "testkit")]
        let threads = if inline_engines { 0 } else { query_threads };
        #[cfg(not(feature = "testkit"))]
        let threads = query_threads;
        if query_worker.is_some() && threads == 0 {
            return Err(
                "a query worker is configured but the node has no query runtime (query_threads = 0): refusing to run SQL in process"
                    .into(),
            );
        }
    }
    let egress = Arc::new(egress);
    // Request identity: the per-tenant generation comes from the `tenants` book (on every
    // re-read), the key from the master — never from the bucket.
    let identity = crate::identity::Identity::new(identity_master);
    // Before any I/O: an operator-set node id or a bad UI base path is a configuration
    // error, the node does not start.
    if let Some(id) = &mode.node_id {
        check_node_id(id)?;
    }
    let ui_cfg = ui;
    let ui = crate::ui::Ui::build(&ui_cfg)?;
    // The last migration step number this binary knows (with this book) — the session's `knows=`.
    let knows = cairn_store::migrate::latest(migrations);
    // Local directory (`--data-dir`): an exclusive lock, CAS emulated within the process —
    // before any operation on the store, gates included.
    let (stores, local_lock) = stores.exclusive_local()?;
    let stores = match flush_interval {
        Some(every) => stores.with_flush_interval(every),
        None => stores,
    };
    // Disk cache: **after** the local CAS layer — the cache wraps the store it sees when
    // built, and Dbs go through the cache.
    let stores = match disk_cache {
        Some(c) => stores.with_disk_cache(c).await?,
        None => stores,
    };
    // Every range of the node runs on a fixed thread pool, and SlateDB's heavy
    // computation (compaction, GC) on the process's background runtime — the
    // number of threads no longer follows the number of ranges.
    // The turn limit is per node (D-c): the pool — one per node — carries it.
    #[cfg(feature = "testkit")]
    let (pool, stores) = if inline_engines {
        let rt = tokio::runtime::Handle::current();
        (EnginePool::inline(), stores.with_background_runtime(rt))
    } else {
        (
            EnginePool::new(engine_threads.unwrap_or_else(EnginePool::default_threads))?,
            stores.with_background_runtime(crate::engine_pool::background_runtime()?),
        )
    };
    #[cfg(not(feature = "testkit"))]
    let (pool, stores) = (
        EnginePool::new(engine_threads.unwrap_or_else(EnginePool::default_threads))?,
        stores.with_background_runtime(crate::engine_pool::background_runtime()?),
    );
    let pool = pool
        .with_turn_budget(limits.max_concurrent_turns)
        .with_tenant_turn_cap(limits.max_turns_per_tenant)
        .with_streams(&streams)
        .with_egress(Arc::clone(&egress))
        .with_identity(identity.clone());
    let query_ranges = pool.query_ranges().clone();
    let mut tasks = Tasks::default();
    let handler_stats = start_observe(&pool, &stores, slow_log, &mut tasks);

    // One shared limit for every transient I/O retry of the boot path.
    let boot = crate::boot::BootClock::start();
    pass_gates(&stores, &boot).await?;
    let ports = bind_listeners(&host, advertise_host.as_deref(), port, internal_port).await?;
    let (lease_cfg, ttl, meter) =
        lease_setup(&mode, &ports, knows, owner_watch, &mut node_wal, &slow);
    let node_id = lease_cfg.node_id.clone();
    let pool = pool.with_node(&node_id);

    // The local directory lock proves every session still `live` on the bucket belongs to
    // an earlier run that died: expire them at once, so their ranges can be taken right
    // away rather than after one TTL.
    if let Some(lock) = &local_lock {
        boot.run(&cairn_store::retry::ops::BOOT_SESSION, || {
            cairn_store::expire_sessions_under_lock(
                stores.main().store(),
                stores.main().prefix(),
                lock,
            )
        })
        .await?;
    }

    // **The node's session** (D20): one renewal for every partition. Opened before the
    // engines — if the session cannot be written the node has nothing to lead, and that is
    // a start-up error, not something to retry silently in each partition.
    let session = Session::establish_within(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
        lease_cfg.clone(),
        &boot.policy(),
    )
    .await?;
    // The session is open but start-up fails at a later step ⇒ close it, so a node that
    // never came up does not leave its renewal loop running forever in the process.
    let session_guard = CloseOnDrop(Some(session.clone()));
    // The incarnation the boot sequence reads the registry under (`RegistryGen::set_fresh_limit`).
    let boot_incarnation = session.current();
    migrate_cluster(&stores, &session, migrations, quiet_lease, ttl, &boot).await?;

    // The range table lives in the bucket, fixed at the very first initialisation. A node
    // that disagrees on the number of ranges at initialisation does not start — changing it
    // by a parameter once there is data loses data silently; changing it is the job of
    // split/merge, by CAS on this very object. So even a one-range node keeps its table on
    // the bucket: a table only in RAM cannot be split.
    let cluster = boot
        .run(&cairn_store::retry::ops::BOOT_TABLE, || {
            ClusterConfig::open(
                stores.main().store(),
                stores.main().prefix(),
                mode.partitions(),
            )
        })
        .await?;

    let registry = Registry::new();

    // **Auth**, before serving on any port (the ports are bound, no requests accepted yet):
    // the cluster secret (an environment variable or an object in the bucket), then the
    // key store — see [`resolve_auth`].
    let cluster_secret = boot
        .run_text(&cairn_store::retry::ops::BOOT_SECRET, || {
            crate::auth::ClusterSecret::resolve(
                stores.main().store(),
                stores.main().prefix(),
                auth.cluster_secret.as_deref(),
            )
        })
        .await?;
    #[cfg(feature = "testkit")]
    let cluster_secret = match net_filter {
        Some(f) => cluster_secret.with_net_filter(&node_id, f),
        None => cluster_secret,
    };
    let mut auth = resolve_auth(&stores, &auth, &boot).await?;
    // Console sessions: the signing key is derived from the cluster secret, so any node can
    // check a cookie another node signed.
    auth.sessions = Some(
        crate::auth::Sessions::new(
            &cluster_secret,
            ui_cfg.session_ttl,
            ui_cfg
                .cookie_secure
                .unwrap_or_else(|| !crate::ui::is_loopback(&host)),
        )
        .with_trusted_proxies(ui_cfg.trust_proxy.clone()),
    );
    // OIDC relying party: discovery, JWKS and tokens go through the node's egress policy —
    // the issuer is a URL set by someone with the `Provision` capability.
    auth.oidc = Some(crate::auth::oidc::Rp::new(Arc::clone(&egress)));
    // A range taker notifies the old owner — installed before any engine takes
    // anything.
    crate::owner_notice::install(&session, &cluster_secret);

    // **The range table can change at run time**: every holder of `cluster` sees the same
    // current table, and that table follows `cluster.conf` — one GET per TTL per node, not
    // per range. The table changes rarely (split/merge), and being one TTL late is only
    // late, not wrong — messages sent by the old table get `409` and are resent; a node that
    // splits/merges itself applies the new table at once.
    // The registry generation and the cluster coordinator: built early so the range table
    // follower can hear the version in `view` — only the coordinator polls `cluster.conf`
    // on a tick.
    let registry_gen = crate::registry_gen::RegistryGen::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
        Some(session.clone()),
    );
    registry_gen.set_stuck_after(registry_stuck);
    let control = crate::control::Control::new(
        session.clone(),
        cluster.clone(),
        ttl,
        Some(cluster_secret.clone()),
        Some(registry_gen.clone()),
    );
    #[cfg(feature = "testkit")]
    control.set_suspects(view_suspects);
    tasks.push(cluster.follow_with(ttl, control.table_hint()));

    // Shared WAL per incarnation — the only WAL of every range. The log is only opened
    // once some range is taken.
    // Net for a failed stream rotation: if other nodes can take the ranges, the rotated
    // slot waits in the background; if the whole bucket is slow, fail-stop as before.
    let members = session.membership();
    node_wal.rotation_net = Some(cairn_store::nodewal::RotationNet::new(move || {
        crate::slow::peers_can_serve(&members.peer_health())
    }));
    let wals = NodeWals::follow(&stores, &session, node_wal.clone());

    let routing = build_routing(&session, &stores, &cluster, ttl, &mut tasks);
    // A tenant's rate limit is split evenly across the nodes leading at least one range
    // — see `crate::rate::spawn_peer_count`.
    if let Some(budget) = pool.turn_budget() {
        tasks.push(crate::rate::spawn_peer_count(
            budget.tenants().clone(),
            routing.clone(),
            node_id.clone(),
        ));
    }

    // Leadership balancing: the nodes' sessions to count live nodes, the balancer to "hold
    // no more than the fair share, release when above". A node on its own has the whole
    // fair share.
    let balancer = Balancer::new(cluster.clone(), node_id.clone(), session.membership())
        .with_wal_gate(meter.clone())
        .with_registry_gate(registry_gen.clone());
    let transport = Transport::new(reqwest::Client::new(), routing.clone(), cluster.clone())
        .with_cluster_secret(cluster_secret.clone());
    let deployments =
        DeploymentStore::new(Arc::clone(stores.main().store()), stores.main().prefix());
    // Rule book: **one object** at cluster level, written by CAS on the etag. No second
    // store — every system that adds a disk is one more system the customer must operate.
    let rules = RuleStore::new(Arc::clone(stores.main().store()), stores.main().prefix());
    // One rule book read loop for the whole node, built **before** the engines: a range that
    // just became leader waits for its first round (bounded).
    refresh::spawn_rules_poller(&rules, &node_id, &mut tasks);
    // Cluster-level registry change notification: a registry write here ⇒ nudge the registry
    // refresh round (`RefreshLoop`) and the quarantine round, and notify peers; a peer's
    // notification ⇒ nudge those two rounds.
    // Registry generation: writers bump it, re-read rounds ask it.
    let registry_sync = crate::registry_sync::RegistrySync::new(
        Some(session.clone()),
        Some(cluster_secret.clone()),
        Some(rules.clone()),
    )
    .with_gen(registry_gen.clone(), registry_reconcile);
    auth.registry = Some(registry_sync.clone());
    // Data plane audit: the object sink and the loop writing refused requests, merged
    // every minute.
    let audit_sink = crate::audit::Sink::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix().clone(),
        &node_id,
    );
    let denials = Arc::new(crate::audit::Denials::default());
    auth.denials = Some(Arc::clone(&denials));
    auth.key_rate = key_rate.map(|r| Arc::new(crate::auth::KeyRate::new(r)));
    tasks.push(tokio::spawn(
        denials.run(audit_sink.clone(), audit_denials_every),
    ));
    // Archive before cleanup and tenants with the hash chain on — both at
    // cluster level, in the main store.
    let archive = archive.then(|| {
        cairn_store::archive::ArchiveStore::new(
            Arc::clone(stores.main().store()),
            stores.main().prefix(),
        )
    });
    // Artifacts: always in the main store; a range only uses them when the
    // `streams/table` gate is on at open.
    let artifacts = cairn_store::artifact::ArtifactStore::new(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
    );
    let tenant_settings = crate::tenants::TenantSettings::new(Some((
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
    )))
    .with_slow_log(handler_stats.slow_log().clone())
    .with_identity(identity.clone())
    .with_defaults(tenant_defaults);
    boot.run_text(&cairn_store::retry::ops::BOOT_TENANTS, || {
        tenant_settings.refresh()
    })
    .await?;
    auth.tenant_states = Some(tenant_settings.limits().clone());
    // The session written, `auth/keys` and `tenants` read, HTTP not served yet: tests
    // slip a registry write into this gap.
    cairn_store::failpoint!("node::start::after_registry_read");
    if let Err(e) = handler_stats.captures().refresh().await {
        tracing::warn!(error = %e, "cannot read captures/ — nothing captured until the next tick");
    }
    start_quarantine(
        &handler_stats,
        &registry_sync,
        &tenant_settings,
        &stores,
        quarantine,
        deploy_refresh,
        &mut tasks,
    )
    .await;
    // Per-range flush windows (D-h) — an absent object means every range follows the node.
    let range_windows = crate::windows::RangeWindows::new(Some((
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
    )));
    boot.run_text(&cairn_store::retry::ops::BOOT_WINDOWS, || {
        range_windows.refresh()
    })
    .await?;

    // Ranges that **this node's** split/merge coordinator is working on: the engine
    // factory does not roll back their intents (recovery).
    let active = crate::split::ActiveIntents::default();
    let range_stats = pool.range_stats().clone();
    let load_book = pool.load_book().clone();
    // The slowness score round looks at the node's turn slots to tell busy from slow;
    // the slot cell is built here, before the pool goes into the spawner.
    let slow_turns = pool.turn_budget().map(|b| b.node_turns());
    // The segmented value read path for SDK v6: after the cluster secret (the `proxy`
    // token key) and after bind (the default address of `proxy`).
    let value_urls = crate::value_urls::ValueUrls::build(
        &values,
        stores.main(),
        &cluster_secret,
        &ports.ingress_url,
    )?;
    tracing::info!(read_path = value_urls.name(), "value read path");
    let value_urls = Arc::new(value_urls);
    let pool = pool.with_value_urls(Arc::clone(&value_urls));
    // The node's segment cache limit: shared by the ranges and the ingress read path.
    let value_cache = cairn_store::artifact::CacheBudget::new(cairn_store::value::CACHE_NODE_BYTES);
    let spawner = RangeSpawner {
        active: active.clone(),
        migrations,
        cluster: cluster.clone(),
        stores: stores.clone(),
        lease_cfg: lease_cfg.clone(),
        balancer: balancer.clone(),
        session: session.clone(),
        wals: wals.clone(),
        windows: range_windows.clone(),
        routing: routing.clone(),
        registry: registry.clone(),
        transport: transport.clone(),
        rules: rules.clone(),
        tick_interval,
        limits,
        retention,
        archive: archive.clone(),
        archive_artifacts,
        artifact_put_deadline,
        artifacts,
        artifact_cache: cairn_store::artifact::CacheBudget::new(
            cairn_store::artifact::CACHE_NODE_BYTES,
        ),
        value_cache: value_cache.clone(),
        chained: tenant_settings.chained().clone(),
        tenant_limits: tenant_settings.limits().clone(),
        pool,
    };
    // The deployment registry is loaded **once, synchronously** before building the engines
    // and before `start` returns: the background refresh round (below) runs later, and its
    // first tick may come late — the node must not take work before it knows which deployment to call.
    let mut deploy_etags = cairn_store::entropy::HashMap::default();
    refresh_registry(&deployments, &registry, &mut deploy_etags).await;
    let handles = spawn_engines(spawner, &cluster, ttl, &mut tasks).await?;
    // The release-when-above loop: checks every TTL, releases only after two periods above,
    // one partition at a time. See `crate::balance` for why being slow is deliberate.
    tasks.push(balancer.clone().spawn_release_loop(handles.clone(), ttl));
    // Slowness score and self-shedding when slow: measure and publish the score every
    // round; a node clearly slower than the cluster takes the `shedding` marker, refuses to
    // take ranges and gradually hands over those it holds. A node on its own never sheds
    // (LS-11).
    tasks.push(crate::slow::spawn(
        slow.clone(),
        meter.clone(),
        slow_turns,
        Some(crate::slow::Shedder {
            session: session.clone(),
            balancer: balancer.clone(),
            handles: handles.clone(),
            marker: cairn_store::health::ShedMarker::new(
                Arc::clone(stores.main().store()),
                stores.main().prefix(),
                slow.marker_ttl,
            ),
        }),
    ));
    // Clock rate: a node whose monotonic clock runs at a different rate from the store's
    // time stops leading — releases its ranges, releases the coordinator role.
    // Only stores with a `Date` header (`s3://`); samples come from existing requests.
    if let Some(clock) = stores.main().store_clock() {
        tasks.push(crate::clock_rate::spawn(
            clock.clone(),
            crate::clock_rate::StepDown {
                session: session.clone(),
                balancer: balancer.clone(),
                handles: handles.clone(),
                meter: meter.clone(),
                control: control.clone(),
            },
        ));
    }

    let gc_min_age = gc_retired_dbs
        .as_ref()
        .map_or(crate::gc::DEFAULT_MIN_AGE, |g| g.min_age);
    let scrub_last = crate::scrub::ScrubLast::default();
    spawn_scrub(
        scrub_every,
        scrub_requests_per_hour,
        gc_retired_dbs.as_ref(),
        &stores,
        &cluster,
        &handles,
        scrub_last.clone(),
        &mut tasks,
    );
    spawn_trash_purge(trash_keep, &stores, &cluster, &handles, &mut tasks);
    spawn_audit_reconcile(&stores, &cluster, &handles, &mut tasks);
    spawn_artifact_sweep(
        artifact_sweep.zip(trash_keep),
        artifact_min_age,
        &stores,
        &cluster,
        &handles,
        &mut tasks,
    );
    spawn_value_sweep(
        value_sweep.zip(trash_keep),
        value_min_age,
        &stores,
        &cluster,
        &handles,
        &mut tasks,
    );
    spawn_gc_loops(
        gc_retired_dbs,
        retire_orphans_every,
        &session,
        &stores,
        &cluster,
        &handles,
        &mut tasks,
    );
    let metering = spawn_metering(&stores, &node_id, metering_flush, &mut tasks);
    let history = spawn_stats_history(
        &stores,
        &node_id,
        &handler_stats,
        metering_flush,
        &mut tasks,
    );
    // The control loop runs once `handles` exist — needed to know whether the node runs GC
    // (whoever takes the role first).
    for t in control.spawn(handles.clone()) {
        tasks.push(t);
    }
    // Close when uncertain: if the key registry has not been confirmed fresh for more than
    // two refresh ticks, requests authenticated by registry keys get `503`. Applies to a
    // lone node and `--data-dir` too (confirmed by a GET of its own generation). The first
    // mark is set right before the loop runs — the key registry was loaded synchronously at
    // boot, and building the engines above may take longer than two ticks.
    let refresh_every = deploy_refresh.max(Duration::from_millis(100));
    registry_gen.set_fresh_limit(refresh_every * 2, boot_incarnation);
    // Freshness also follows the node's session (`registry_gen::SessionGate`): a new
    // incarnation opened in-process (no boot sequence) serves registry keys only once it
    // has re-read the registry — nudge the refresh round as soon as one opens, instead of
    // waiting up to a whole refresh tick.
    {
        let mut live = session.live_changes();
        let sync = registry_sync.clone();
        let gen = registry_gen.clone();
        tasks.push(tokio::spawn(async move {
            while live.changed().await.is_ok() {
                if live.borrow_and_update().is_some() {
                    gen.note_session_change();
                    sync.poke_local();
                }
            }
        }));
    }
    // The logged-out sessions book has its own freshness: a failed read only closes
    // cookie sessions, not keys.
    auth.keys.oidc().set_sessions_fresh_limit(refresh_every * 2);
    tasks.push(
        RefreshLoop {
            deployments: deployments.clone(),
            registry: registry.clone(),
            handles: handles.clone(),
            every: refresh_every,
            keys: auth.keys.clone(),
            settings: tenant_settings.clone(),
            captures: handler_stats.captures().clone(),
            windows: range_windows.clone(),
            wals: wals.clone(),
            session: session.clone(),
            table: cluster.clone(),
            etags: deploy_etags,
            sync: registry_sync.clone(),
        }
        .spawn(),
    );

    // The follower read path. The poll period is the follower's staleness floor; each open
    // reader costs ~10 requests per period on the object store, and readers are only opened
    // for partitions someone asks about — a node on its own leads every range so it opens
    // no reader.
    let followers = Some({
        // A 50 ms floor: anything smaller is a busy loop on the object store, and none of
        // the target workloads needs fresher than that.
        // A node that becomes the range's leader ⇒ closes its reader.
        let (routing, me) = (routing.clone(), node_id.clone());
        FollowerReads::new(stores.clone(), reader_poll.max(Duration::from_millis(50)))
            .with_leader_check(move |p| routing.leader_of(p).is_some_and(|l| l.node_id == me))
    });

    let range_ops = crate::split::RangeOps {
        stats: range_stats,
        cluster: cluster.clone(),
        stores: stores.clone(),
        session: session.clone(),
        partitions: handles.clone(),
        routing: routing.clone(),
        active,
        client: reqwest::Client::new(),
        secret: cluster_secret.clone(),
        // Enough for a lease take (possibly waiting for the old owner of a range being
        // gathered to hand over) plus opening the Db and rebuilding the derived region.
        settle: ttl * 4 + Duration::from_secs(5),
        node_id: node_id.clone(),
        book: crate::split::OpBook::default(),
        lineage: lineage.clone(),
    };
    spawn_intent_reconciler(
        reconcile_intents_every,
        &range_ops,
        &cluster,
        &handles,
        &node_id,
        &mut tasks,
    );
    // Auto-split: the per-range load book carried by the pool to each range; the
    // background loop closes a window every second, then proposes/splits per the mode.
    // Child standalone after a split: compact a range standing on a deep or old ancestor
    // chain in full, one range at a time, reading writers from the load book.
    if lineage.enabled() {
        tasks.push(
            crate::lineage::Lineage::new(
                lineage,
                stores.clone(),
                cluster.clone(),
                load_book.clone(),
                handles.clone(),
            )
            .spawn(),
        );
    }
    let auto_split = crate::autosplit::AutoSplit::new(
        auto_split,
        load_book.clone(),
        limits.max_concurrent_turns,
        Some(range_ops.clone()),
    );
    tasks.push(auto_split.clone().spawn());
    // Tenant purge: the round on the D-d runner, and the batch-receiving half on every
    // node (an internal route).
    let purger = Arc::new(crate::tenant_purge::Purger {
        stores: stores.clone(),
        cluster: cluster.clone(),
        partitions: handles.clone(),
        routing: routing.clone(),
        secret: Some(cluster_secret.clone()),
        client: crate::invoker::internal_http(),
        settings: tenant_settings.clone(),
        keys: Some(auth.keys.clone()),
        rules: Some(rules.clone()),
        keep: tenant_purge_keep.unwrap_or(cairn_store::tenant_purge::KEEP),
        last: Arc::default(),
        compactions: Arc::default(),
        watches: Arc::default(),
        lots_per_range: crate::tenant_purge::LOTS_PER_RANGE,
        sweeps: Arc::default(),
        checks: Arc::default(),
        bases: Arc::default(),
    });
    super::setup::spawn_tenant_purge(
        tenant_purge_keep.map(|_| Arc::clone(&purger)),
        &registry_sync,
        &cluster,
        &handles,
        &mut tasks,
    );
    let state = AppState::new_in_cluster(
        handles.clone(),
        registry,
        cluster.clone(),
        routing.clone(),
        Some(deployments),
        followers,
    )
    .with_rules(Some(rules))
    .with_invoke_wait(invoke_wait)
    .with_attach_max_wait(attach_max_wait)
    .with_schedule_min_period(schedule_min_period)
    .with_invoke_poll_floor(invoke_poll_floor)
    .with_range_ops(Some(range_ops.clone()))
    .with_node_id(Some(node_id.clone()))
    .with_metering(
        Arc::clone(stores.main().store()),
        stores.main().prefix().clone(),
    )
    .with_value_cache(value_cache.clone())
    .with_history_flush(metering_flush)
    .with_windows(range_windows.clone())
    .with_auth(auth)
    .with_archive(archive)
    .with_tenant_settings(Some(tenant_settings))
    .with_tenant_purge(Some(purger))
    .with_audit(Some(audit_sink))
    .with_handler_stats(Some(handler_stats.clone()))
    .with_auto_split(Some(auto_split))
    .with_gc(Some(crate::gc::GcAdmin {
        stores: stores.clone(),
        cluster: cluster.clone(),
        min_age: gc_min_age,
    }))
    .with_cluster_secret(cluster_secret.clone())
    .with_value_urls(value_urls)
    .with_registry_sync(Some(registry_sync))
    .with_control(Some(control))
    .with_scrub_last(Some(scrub_last))
    .with_streams(streams)
    .with_egress(egress)
    .with_identity(identity)
    .with_deploy_require_identity(deploy_require_identity)
    .with_deploy_transport(deploy_transport)
    .with_deploy_https_request_response(deploy_https_request_response)
    .with_deploy_no_rr_compression(deploy_no_rr_compression)
    .with_deploy_require_compression(deploy_require_compression)
    .with_ui(ui)
    .with_env_label(ui_cfg.env_label.clone())
    .with_query(Some(
        crate::query::QueryService::new(
            query_ranges,
            cluster.clone(),
            query_limits,
            query_tenant_budget,
        )
        // Deterministic simulation runs everything on one runtime: no separate thread.
        .with_threads({
            #[cfg(feature = "testkit")]
            let threads = if inline_engines { 0 } else { query_threads };
            #[cfg(not(feature = "testkit"))]
            let threads = query_threads;
            threads
        })
        .pipe_workers(query_worker),
    ));
    let (addr, internal_addr) = (ports.addr, ports.internal_addr);
    // Per-tenant usage counting for soft limits: idle while no tenant carries
    // `max-live=`/`max-artifact-bytes=`.
    tasks.push(tokio::spawn(crate::tenant_usage::run(
        state.clone(),
        tenant_usage_every,
    )));
    let router = state.router().clone();
    let quarantine = handler_stats.quarantine().clone();
    let (server, stop_server) = serve_http(state, ports, &node_id, http_header_timeout);

    session_guard.disarm();
    Ok(RunningNode {
        addr,
        internal_addr,
        cluster_secret,
        node_id,
        partitions: handles,
        routing,
        cluster,
        session,
        ranges: range_ops,
        router,
        quarantine,
        balancer,
        load_book,
        tasks,
        metering,
        history,
        server: Some(server),
        stop_server: Some(stop_server),
        _local_lock: local_lock,
    })
}

/// Observability: per-handler summaries, the slow log, capturing the next run — one set
/// per node, the pool carries it to each range.
fn start_observe(
    pool: &EnginePool,
    stores: &ObjectStores,
    slow_log: Duration,
    tasks: &mut Tasks,
) -> crate::stats::HandlerStats {
    let handler_stats = pool.handler_stats().clone();
    handler_stats.slow_log().set_default(slow_log);
    handler_stats
        .captures()
        .attach(crate::capture::CaptureStore::new(
            Arc::clone(stores.main().store()),
            stores.main().prefix(),
        ));
    tasks.push(handler_stats.spawn_aggregator(Duration::from_secs(1)));
    handler_stats
}
