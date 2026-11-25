// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `RangeSpawner`: opens the Db and builds the engine of each range, following the range table.

use super::*;

/// The Db open of `partition` just failed: if it is because of the **closed mark**
/// (SM-17), consider whether it is a stale mark from a roll-back that died midway — the
/// roller CASed the intent away and died before removing the flag
/// ([`crate::split::reopen_sources`]). Only remove it when a **successful** table read shows
/// the range still in the table and without an intent: a committed range has left the
/// table, a range being split/merged still has an intent. The in-RAM table is never
/// enough — it is exactly what the closed mark distrusts. A failed read keeps the flag
/// (rule 5): the next open considers it again.
///
/// The close mark is read **before** the table, and the removal is conditioned on exactly
/// that epoch: an intent that reaches the table after the table read bumps the epoch with
/// its close, and this removal is fenced instead of removing its mark.
async fn heal_stale_close(stores: &ObjectStores, cluster: &ClusterConfig, partition: PartitionId) {
    let mark = match stores.range_db_close_mark(partition).await {
        Ok(Some(m)) if m.closed => m,
        _ => return,
    };
    // Tripwire/counter: an open meets a closed Db. With the old fences still on, this only
    // happens after a roll-back that died midway.
    cairn_store::failpoint!("node::open::closed_db");
    // The core decides (`Event::StaleClose`) on a table read **after** the mark.
    let view = cairn_split::View {
        entry: Some(match cluster.reload().await {
            Ok(_) => Ok(crate::split::core::entry(&cluster.table(), partition)),
            Err(_) => Err(cairn_split::ReadErr),
        }),
        ..cairn_split::View::default()
    };
    match cairn_split::step(&view, &cairn_split::Event::StaleClose) {
        cairn_split::Plan::KeepClosed => {}
        cairn_split::Plan::ReopenAtMark => {
            tracing::warn!(
                range = partition.0,
                "Db of a live range without an intent still has a closed mark — a stale mark of a roll-back that died midway; removing"
            );
            if let Err(e) = stores
                .reopen_range_db_at(partition, mark.writer_epoch)
                .await
            {
                tracing::warn!(range = partition.0, error = %e, "cannot remove the stale closed mark");
            }
        }
        other => unreachable!("core returned {other:?} for StaleClose"),
    }
}

/// Wait after taking a range before warming the disk cache: the open and the first turns
/// go first, the warm-up does not compete with them for bandwidth.
pub(super) const WARM_DELAY: Duration = Duration::from_secs(5);

/// Warm the disk cache for a range just taken only if the range is still in the table,
/// carries no intent and is not in the middle of a split/merge of this node.
pub(super) fn warm_allowed(in_table: bool, has_intent: bool, splitting_here: bool) -> bool {
    in_table && !has_intent && !splitting_here
}

/// One D-d round with the default configuration ([`crate::gc::GcConfig`]: really
/// deletes, `min_age` 30 minutes). Returns the number of Dbs deleted. See [`crate::gc::run_once`].
pub async fn gc_retired_dbs_once(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
) -> Result<usize, Error> {
    Ok(
        crate::gc::run_once(stores, cluster, &crate::gc::GcConfig::default())
            .await?
            .deleted
            .len(),
    )
}

/// Everything needed to build the engine of **one** range — kept after start-up, because
/// when the table changes the node must build more engines for new ranges.
#[derive(Clone)]
pub(super) struct RangeSpawner {
    pub(super) active: crate::split::ActiveIntents,
    /// The migration book — range-level steps run when a range opens.
    pub(super) migrations: &'static [cairn_store::migrate::Migration],
    pub(super) cluster: ClusterConfig,
    pub(super) stores: ObjectStores,
    pub(super) lease_cfg: LeaseConfig,
    pub(super) balancer: Balancer,
    pub(super) session: Session,
    pub(super) wals: NodeWals,
    pub(super) routing: RoutingTable,
    pub(super) registry: Registry,
    pub(super) transport: Transport,
    pub(super) rules: RuleStore,
    pub(super) tick_interval: Option<Duration>,
    pub(super) limits: TurnLimits,
    pub(super) retention: Option<u64>,
    pub(super) windows: crate::windows::RangeWindows,
    pub(super) archive: Option<cairn_store::archive::ArchiveStore>,
    pub(super) archive_artifacts: bool,
    pub(super) artifact_put_deadline: std::time::Duration,
    pub(super) artifacts: cairn_store::artifact::ArtifactStore,
    /// The node-wide limit on the artifact caches of every range.
    pub(super) artifact_cache: cairn_store::artifact::CacheBudget,
    /// The shared limit on the segment caches of every range.
    pub(super) value_cache: cairn_store::artifact::CacheBudget,
    pub(super) chained: cairn_engine::ChainedTenants,
    /// Per-tenant limits from the `tenants` book.
    pub(super) tenant_limits: cairn_engine::TenantLimitsSource,
    pub(super) pool: EnginePool,
}

impl RangeSpawner {
    /// The engine factory of range `partition`.
    ///
    /// The closure runs **every time** that range regains leadership, not only the first
    /// time: the old writer has been `seal`ed and has no way back. The engine's range is
    /// read from the table **at open** and then fixed for the engine's lifetime; a range
    /// that has left the table is not opened — the node is about to shut its engine down for good.
    fn factory(&self, partition: PartitionId) -> crate::actor::EngineFactory {
        let me = self.clone();
        Box::new(move || {
            let me = me.clone();
            Box::pin(cairn_store::io_source::tagged("range.open", async move {
                let gone = || {
                    EngineError::from(StoreError::BadClusterConfig {
                        reason: format!("range {} is no longer in the range table", partition.0),
                    })
                };
                if !me.cluster.contains(partition) {
                    return Err(gone());
                }
                // Take the lease BEFORE opening SlateDB. Otherwise `Db::open` bumps
                // `writer_epoch` and fences a perfectly healthy leader — every node that
                // restarts would kick the current leader out, though it has no authority
                // at all.
                let claim_started = tokio::time::Instant::now();
                let cfg = &me.lease_cfg;
                // An existing Db without an owner record yet (a bucket built by a
                // single-process node before owner records existed): the released record
                // carries the Db's WAL kind, before the take — otherwise the take would treat
                // the range as new and assign the node's default WAL kind. Only reads and then
                // `Create`s: retryable.
                open_io("owner record of an existing Db", None, || {
                    me.stores.adopt_unowned_db(partition, &cfg.node_id)
                })
                .await?;
                // Through the balancer rather than a direct `claim`: only competes while below
                // this node's fair share. Rescue grace period 2×TTL.
                let (grant, mut keeper) = cairn_store::io_source::tagged(
                    "range.claim",
                    me.balancer
                        .acquire(&me.session, partition, cfg.ttl * 2, cfg.renew_interval),
                )
                .await?;
                tracing::info!(
                    partition = partition.0,
                    term = grant.term(),
                    "became leader"
                );
                // We just wrote the lease — we know for sure who holds it without waiting for
                // the refresh round to read it again.
                me.routing.insert(
                    partition,
                    crate::routing::Leader {
                        node_id: cfg.node_id.clone(),
                        address: cfg.address.clone(),
                        ingress: cfg.ingress.clone(),
                        term: grant.term(),
                    },
                );
                // Test knob: turns off the old fences of the open — the SM-13 wait, the fresh
                // table (a load error swallowed as in the past), the quiesce tripwire — so the
                // test sees that the closed mark on the Db itself is enough. With the
                // `failpoints` feature off it is always `false`.
                let bypass = cairn_store::failpoint::fires("node::open::bypass_old_fences");
                // **SM-13**: the range is the quiesced source of a split/merge that a
                // coordinator **of this node** is still running — the source engine lost its
                // lease midway and just took it back. Opening the Db now would acknowledge
                // writes the (already cloned) children do not have, and the commit CAS would
                // still win. Wait for the coordinator to finish, holding the lease so that no
                // other node takes it and rolls the intent back; afterwards read a fresh table
                // as usual (commit ⇒ the range has left the table; roll-back ⇒ open as usual).
                //
                // Every decision of the open asks the core (`Event::Open`): the mark wait
                // (SM-13), the fresh table, rolling back/forward an intent nobody here
                // coordinates (SM-3).
                use cairn_split::{Event, Finder, Need, Plan, View};
                let ram = crate::split::core::entry(&me.cluster.table(), partition);
                let opening = |view: &View, quiesced_here: bool| {
                    cairn_split::step(
                        view,
                        &Event::Open {
                            quiesced_here,
                            active_here: me.active.contains(partition),
                            ram,
                        },
                    )
                };
                if !bypass
                    && opening(&View::default(), me.active.is_quiesced(partition))
                        == Plan::WaitUnquiesced
                {
                    me.active.wait_unquiesced(partition, Some(&keeper)).await?;
                }
                let open_started = tokio::time::Instant::now();
                let claim_ms = crate::range_stats::ms(open_started - claim_started);
                // A **fresh** table before opening the Db: a node holding an old table must not
                // open the Db of a range that has left the table (split/merge done but the
                // tombstone not yet written). One GET per leadership take — rare. A failed load
                // (after `open_io`'s attempts) **does not open**: the in-RAM table may be older
                // than a committed split/merge, and opening by it would acknowledge writes into a
                // Db about to retire. The leadership loop retries the whole open.
                //
                // **Recovery**: a range carrying the intent of a split/merge that no coordinator
                // of this node is running ⇒ the coordinator is dead (we just took the range from
                // it). The core decides (`Found(Opener)`) before opening — the parent range has
                // not been touched since the quiesce.
                let mut view = View::default();
                loop {
                    match opening(&view, false) {
                        Plan::Need(Need::Entry) => {
                            if bypass {
                                let _ = me.cluster.reload().await;
                            } else {
                                open_io("range table", Some(&keeper), || me.cluster.reload())
                                    .await?;
                            }
                            view.entry = Some(Ok(crate::split::core::entry(
                                &me.cluster.table(),
                                partition,
                            )));
                        }
                        Plan::Gone => return Err(gone()),
                        Plan::Resolve(_) => {
                            let intent = me
                                .cluster
                                .range(partition)
                                .and_then(|r| r.intent)
                                .expect("Resolve implies an intent");
                            let finder = Finder::Opener { active_here: false };
                            match crate::split::core::found_with(
                                &me.stores, partition, intent, finder,
                            )
                            .await
                            {
                                Plan::Abort { .. } => {
                                    tracing::warn!(
                                        range = partition.0,
                                        "range has the intent of an abandoned split/merge — rolling back"
                                    );
                                    crate::split::roll_back(
                                        &me.cluster,
                                        &me.stores,
                                        partition,
                                        crate::split::Who::Opener,
                                    )
                                    .await?;
                                    // The in-RAM table after the intent-removal CAS (or after
                                    // the roll-back round's re-read): no further read.
                                    view.entry = Some(Ok(crate::split::core::entry(
                                        &me.cluster.table(),
                                        partition,
                                    )));
                                }
                                // The op finished preparing ⇒ CAS the commit in place of the
                                // coordinator; the source leaves the table (the core returns `Gone` next round).
                                Plan::RollForward => {
                                    crate::split::roll_forward(
                                        &me.cluster,
                                        &me.stores,
                                        partition,
                                        intent,
                                    )
                                    .await?;
                                    view.entry = Some(Ok(crate::split::core::entry(
                                        &me.cluster.table(),
                                        partition,
                                    )));
                                }
                                other => unreachable!("core returned {other:?} for Found(Opener)"),
                            }
                        }
                        Plan::OpenDb => break,
                        other => unreachable!("core returned {other:?} for Open"),
                    }
                }
                let range = me.cluster.range(partition).ok_or_else(gone)?;
                // **Range-level migration**, the part before opening the Db: the cluster
                // version read fresh (one GET per leadership take — rare; if a newer binary
                // has upgraded the cluster the range does not open), and the range's `data`
                // from the owner record (keeper). A Db at a newer version than the binary ⇒
                // does not open.
                let (main_store, main_prefix) =
                    (me.stores.main().store(), me.stores.main().prefix());
                let knows = cairn_store::migrate::latest(me.migrations);
                let cv = open_io("cluster-version", Some(&keeper), || {
                    cairn_store::migrate::ensure_known(main_store, main_prefix, knows)
                })
                .await?;
                let cluster_version = cv.version;
                // The batching gate is read from this same GET: the engine keeps it for life,
                // never re-read on the turn path.
                let call_prefix = cv.call_prefix_on();
                let call_placement = cv.call_placement_on();
                let streams = cv.streams_on();
                let schedules = cv.schedules_on();
                let audit = cv.audit_on();
                // Segmented values: the range seals and writes `SegmentedState`/`ValueRefs`
                // only if the gate was on at open.
                let values = cv.values_on();
                let data = keeper.data();
                cairn_store::migrate::check_range_data(partition.0, data, knows)?;
                // Missing range-Db-level steps: they touch the manifest and the owner record,
                // so they run **before** opening the Db. The `data=` mark is written once, after
                // the in-Db part below. The `wal/shared` step opens a native-WAL Db: a stale
                // closed mark (SM-17) is removed as in the usual open below, then the leadership
                // loop retries.
                if let Err(e) = cairn_store::migrate::run_range_db(
                    &me.stores,
                    partition.0,
                    &mut keeper,
                    data,
                    cluster_version,
                    me.migrations,
                )
                .await
                {
                    heal_stale_close(&me.stores, &me.cluster, partition).await;
                    return Err(e.into());
                }
                // Every range opens on the shared WAL: native-WAL ranges have moved to the shared
                // WAL in the `wal/shared` step just above. A record still saying `slatedb` ⇒ the
                // cluster has not reached that step — do not open (the native WAL is only opened
                // to drain it, inside the migration step).
                match &keeper.wal().kind {
                    WalKind::Shared => {}
                    WalKind::SlateDb => {
                        return Err(StoreError::NativeWal {
                            range: partition.0,
                            version: cluster_version,
                        }
                        .into())
                    }
                    WalKind::Unknown(kind) => {
                        return Err(StoreError::UnknownWalKind {
                            range: partition.0,
                            kind: kind.clone(),
                        }
                        .into())
                    }
                }
                // A Db just born from a clone (split/merge) carries its parent's `wal_name` mark
                // — a native-WAL parent (an old bucket) has the mark switched to the shared WAL
                // when the Db's WAL is empty (the parent was sealed), otherwise the error goes up.
                // The replay mark of the manifest just read: a hint for the shared-WAL range
                // take, saving another manifest read.
                // Reads the WAL-kind mark in the manifest, rewrites it only if it differs — a
                // retry reads again before writing, so it is repeatable.
                let replay_hint = open_io("WAL kind mark", Some(&keeper), || {
                    me.stores.adopt_shared_wal(partition.0)
                })
                .await?;
                // SM-13 tripwire: getting here with a quiescing source means the coordinator
                // marked it **after** the wait above — do not open, take again from scratch
                // (a quiesce command arriving during the open got `NotLeader`, so the
                // coordinator rolls back). Tests arm `panic` or count with a callback.
                if !bypass && me.active.is_quiesced(partition) {
                    cairn_store::failpoint!("node::open::quiesced_source");
                    return Err(StoreError::Io(format!(
                        "range {} is the quiesced source of a running split/merge — not opening the Db",
                        partition.0
                    ))
                    .into());
                }
                let db = async {
                    let wal = me.wals.get(keeper.session())?;
                    // The range's own window: the stream cuts at the smallest
                    // among the ranges with frames waiting.
                    wal.set_range_window(partition.0, me.windows.get(partition.0));
                    Ok::<_, EngineError>(
                        acquire_shared_with_hint(
                            &me.stores,
                            &wal,
                            &mut keeper,
                            partition.0,
                            replay_hint,
                        )
                        .await?,
                    )
                }
                .await;
                let db = match db {
                    Ok(db) => db,
                    Err(e) => {
                        heal_stale_close(&me.stores, &me.cluster, partition).await;
                        return Err(e);
                    }
                };
                // From here the Db is open (with the shared WAL: on the node's log, the keeper
                // has attached the log). Every error exit below must **seal** the Db before
                // dropping the keeper: dropping it straight away is a dirty hand-over — the owner
                // record keeps a segment pointing at our live log, the next taker seals it, and the
                // whole node fail-stops along with every other range on that log stream. With a
                // deterministic error (a failed migration step, a corrupt row while building an
                // index) the nodes would kill each other in turn. `Db` is a shared handle: this
                // copy can still seal after the writer/engine has swallowed the other one.
                let opened_db = db.clone();
                let opened = async {
                    let mut writer = PartitionWriter::open(grant, db)
                        .await?
                        .with_db_stats(me.stores.take_db_stats(partition.0));
                    // The part after opening: the missing range-level steps, durable, then the
                    // `data=` mark written **last** on the owner record — dying in between means
                    // the next open runs them again (the steps are idempotent). Before the
                    // engine: no turns yet.
                    let migrated = cairn_store::migrate::run_range(
                        &mut writer,
                        data,
                        cluster_version,
                        me.migrations,
                    )
                    .await?;
                    if migrated != data {
                        keeper.set_data(migrated).await?;
                        tracing::info!(
                            range = partition.0,
                            from = data,
                            to = migrated,
                            "range migrated"
                        );
                    }
                    let engine = Engine::open_in_range(writer, me.cluster.clone(), range).await?;
                    Ok::<_, EngineError>(engine)
                }
                .await;
                let engine = match opened {
                    Ok(engine) => engine,
                    Err(e) => {
                        tracing::warn!(range = partition.0, error = %e, "range open failed after the Db was opened — sealing, then handing over");
                        seal_failed_open(partition, opened_db, &mut keeper).await;
                        return Err(e);
                    }
                };
                let open_db_ms = crate::range_stats::ms(open_started.elapsed());
                me.warm_disk_cache(partition);
                let engine = engine
                    .with_retention(me.retention)
                    // The query counters are only built when someone asks: no scan of all of
                    // `ByCreated` on every leader change.
                    .with_counts_on_demand(true)
                    .with_archive(me.archive)
                    .with_chained_tenants(me.chained)
                    .with_tenant_limits_source(me.tenant_limits)
                    .with_call_prefix(call_prefix)
                    .with_call_placement(call_placement)
                    .with_streams(streams)
                    .with_schedules(schedules)
                    .with_audit(audit)
                    .with_values(values)
                    .with_audit_store(Some(cairn_store::audit::AuditStore::new(
                        Arc::clone(me.stores.main().store()),
                        me.stores.main().prefix(),
                    )))
                    // The segment store: inert until the
                    // `values/segmented` gate is on (`with_values`).
                    .with_value_store(Some(
                        cairn_store::value::ValueStore::new(
                            Arc::clone(me.stores.main().store()),
                            me.stores.main().prefix(),
                        )
                        // The range's segment cache.
                        .with_cache(me.value_cache.clone()),
                    ))
                    .with_artifacts(streams.then(|| me.artifacts.clone()))
                    .with_archive_artifacts(me.archive_artifacts)
                    .with_artifact_put_deadline(me.artifact_put_deadline)
                    .with_artifact_cache(streams.then(|| me.artifact_cache.clone()));
                // The `cairnflow_schedules` gauge is counted in a background task at the start
                // of the term (`actor::count_schedules`), not on the path to the first command:
                // the empty index scan costs 17 ms of cold reads at H = 10⁶.
                // Measurements of this leadership take: the range's old row (the previous
                // take) is replaced in full, including `rebuild` when this take does not
                // rebuild.
                let rebuild = engine
                    .rebuilt()
                    .map(crate::range_stats::RebuildReport::from);
                me.pool.range_stats().update(partition.0, |t| {
                    t.claim_ms = Some(claim_ms);
                    t.open_db_ms = Some(open_db_ms);
                    t.rebuild = rebuild;
                    t.restore_ms = None;
                });
                Ok(Leadership {
                    engine,
                    keeper: Some(keeper),
                })
            }))
        })
    }

    /// **Warm the disk cache** when taking a range: a background loop on the background
    /// runtime, [`WARM_DELAY`] after the range has started serving, loading the manifest's
    /// L0 SSTs (byte and time limits in [`cairn_store::disk_cache`]). Does not block the
    /// open, does nothing when the cache is off, skips while the range is in a split window
    /// ([`warm_allowed`]): the blocks of a Db about to retire are not worth the disk.
    fn warm_disk_cache(&self, partition: PartitionId) {
        if self.stores.disk_cache().is_none() {
            return;
        }
        let (stores, active, cluster) = (
            self.stores.clone(),
            self.active.clone(),
            self.cluster.clone(),
        );
        self.stores.spawn_background(async move {
            cairn_store::pace::beat(WARM_DELAY).await;
            let entry = crate::split::core::entry(&cluster.table(), partition);
            if !warm_allowed(
                entry.in_table,
                entry.intent.is_some(),
                active.contains(partition) || active.is_quiesced(partition),
            ) {
                tracing::debug!(
                    range = partition.0,
                    "skipping disk cache warm-up: range in a split window"
                );
                return;
            }
            if let Some(n) = stores.warm_disk_cache(partition.0).await {
                tracing::debug!(range = partition.0, ssts = n, "disk cache warmed");
            }
        });
    }

    /// Build the engine thread of range `partition`.
    pub(super) async fn spawn(
        &self,
        partition: PartitionId,
        readiness: Readiness,
    ) -> Result<EngineHandle, Error> {
        let factory = self.factory(partition);
        let registry = self.registry.clone();
        let transport = self.transport.clone();
        let rules = self.rules.clone();
        // Wait for the engine to finish opening with `await`, without blocking the thread:
        // the caller's runtime keeps running meanwhile. Tests running `current_thread` need
        // this: a store with a blocking gate (testkit) can only open while that very runtime
        // is running.
        Ok(EngineHandle::spawn_in_pool(
            &self.pool,
            factory,
            registry,
            self.tick_interval,
            readiness,
            transport,
            self.limits,
            Some(rules),
        )
        .await?)
    }
}

/// The engine open failed **after** the Db was opened: flush the WAL and the memtable, close
/// the Db, then tell the keeper "writer sealed" — with the shared WAL, the owner record drops
/// the segment pointing at our log, so the hand-over (the keeper dropped by the caller) is a
/// **clean** hand-over and the next taker does not seal the node's live log. Bounded like the
/// seal when stepping down ([`crate::actor::seal_timeout`]). A failed or timed-out seal ⇒ a
/// dirty hand-over as before: the taker seals the log and the node fail-stops — exactly the
/// right behaviour when we cannot prove every frame of the range has reached L0.
async fn seal_failed_open(
    partition: PartitionId,
    db: slatedb::Db,
    keeper: &mut cairn_store::LeaseKeeper,
) {
    let bound = crate::actor::seal_timeout(false, keeper.wal_stall_max());
    let sealed = tokio::time::timeout(bound, async {
        for flush_type in [
            slatedb::config::FlushType::Wal,
            slatedb::config::FlushType::MemTable,
        ] {
            db.flush_with_options(slatedb::config::FlushOptions { flush_type })
                .await?;
        }
        db.close().await
    })
    .await;
    match sealed {
        Ok(Ok(())) => {
            if let Err(e) = keeper.writer_sealed().await {
                tracing::warn!(range = partition.0, error = %e, "seal after the failed open done but the clean owner record could not be written — the hand-over cleans the segment");
            }
        }
        Ok(Err(e)) => {
            tracing::warn!(range = partition.0, error = %e, "cannot seal the Db after the failed open — dirty hand-over");
        }
        Err(_) => {
            tracing::warn!(range = partition.0, limit = ?bound, "seal after the failed open exceeded its limit — dirty hand-over");
        }
    }
}

/// Retry policy of one I/O operation of the engine open: 8 attempts, backing off from 25 ms
/// doubling up to 1 s (jitter) — ~5 s of pauses for one operation. Each attempt is bounded by
/// [`OPEN_IO_ATTEMPT_BOUND`], the whole operation by [`OPEN_IO_TOTAL`].
const OPEN_IO_POLICY: cairn_store::retry::Policy =
    cairn_store::retry::Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8)
        .with_attempt_bound(OPEN_IO_ATTEMPT_BOUND)
        .with_total(OPEN_IO_TOTAL);

/// Limit on one attempt of [`open_io`]. Operations through SlateDB's `Admin` (the WAL kind
/// mark) retry object store errors **forever** in production — without this limit a long
/// store failure would hang the open while holding the lease. 10 s: one manifest read is a few
/// GETs; under the slow S3 profile of chaos runs (PUT +1.4 s) there is still room for tens
/// of them.
const OPEN_IO_ATTEMPT_BOUND: Duration = Duration::from_secs(10);

/// Limit on a whole [`open_io`] operation: past it the open fails, the leadership loop
/// releases the lease and takes it again with backoff — someone else gets a chance if only
/// this node's store is failing.
const OPEN_IO_TOTAL: Duration = Duration::from_secs(30);

/// One I/O operation of the engine open **after** the lease is taken and **before**
/// touching the shared WAL or opening the Db: retries I/O errors (503, network) with
/// backoff + jitter, **keeping** the keeper.
///
/// It used to be that one error failed the whole open: the keeper was dropped (one PUT to
/// release), a 0.5 s pause, then a take from scratch (one PUT to take, `term` + 1). Under a
/// long 503 spell (`s3_slowdown`) the open needs an unbroken run of GETs without errors, so it
/// almost never got through, and each failed round added two PUTs to the very throttling.
/// Retrying in place only takes again when really needed: a non-I/O error, attempts
/// exhausted, or the lease lost.
///
/// Only for reads, or read-before-write writes (repeatable). The later part — taking the
/// range on the shared WAL, opening the Db — does **not** go through here: a range take that
/// failed midway has changed the keeper's state (our WAL segment on the owner record), and
/// retrying it with the same keeper would replay our own live log (C1). That part has its own
/// retries per operation (`retry_io` of sealing, NW-3; SlateDB's object store retries).
///
/// The shared primitive `cairn_store::retry`: errors are classified by
/// `retry::store_verdict` (`Io`, `LeaseIo`, SlateDB's `Unavailable`),
/// a lost lease is the cancellation signal.
pub(super) async fn open_io<T, F, Fut>(
    what: &str,
    keeper: Option<&cairn_store::LeaseKeeper>,
    op: F,
) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StoreError>>,
{
    use tracing::Instrument as _;
    cairn_store::retry::run(
        &cairn_store::retry::ops::OPEN_IO,
        &OPEN_IO_POLICY,
        keeper,
        op,
    )
    .instrument(tracing::debug_span!("open_io", phep = what))
    .await
}

/// The **engine set follows the table** loop: every time the table changes — and every
/// `every`, as a net for a failed build — compare the engine set with the current table:
///
/// - a range disappeared ⇒ remove it from the set, then shut its engine down. That engine
///   **exits for good**: range ids are never reused, so it has nothing to come back to;
/// - a new range ⇒ build an engine, `Readiness::Immediately` (this loop must not stand
///   waiting for a lease take).
pub(super) fn follow_table(
    spawner: RangeSpawner,
    partitions: Partitions,
    every: Duration,
) -> JoinHandle<()> {
    tokio::spawn(cairn_store::io_source::tagged(
        "node.follow_table",
        async move {
            let mut changes = spawner.cluster.subscribe();
            loop {
                let table = changes.borrow_and_update().clone();
                reconcile(&spawner, &partitions, &table).await;
                // `biased`: a table change before the net tick; whichever branch fires,
                // the start of the loop reconciles the whole table again.
                tokio::select! {
                    biased;
                    r = changes.changed() => if r.is_err() { return },
                    _ = tokio::time::sleep(every) => {}
                }
            }
        },
    ))
}

async fn reconcile(spawner: &RangeSpawner, partitions: &Partitions, table: &PartitionTable) {
    let mut changed = false;
    // Retirement is irreversible (a tombstone): a range missing from the in-RAM table only
    // retires when the table **read from the bucket** does not have it either. The in-RAM
    // table may come from a peer's `409` body (D23). A failed read ⇒ no range retires in this
    // round (rule 5); the next round (a table change or the net tick) considers it again.
    let leaving = partitions
        .iter()
        .any(|(id, _)| !table.contains(PartitionId(id)));
    let stored = if leaving {
        match spawner.cluster.read_stored().await {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::warn!(error = %e, "cannot read the range table from the bucket — deferring range retirement");
                None
            }
        }
    } else {
        None
    };
    for (id, handle) in partitions.iter() {
        let p = PartitionId(id);
        if !table.contains(p) {
            // The core decides (`Event::LeftTable`) on the table read from the bucket.
            let view = cairn_split::View {
                entry: Some(match &stored {
                    None => Err(cairn_split::ReadErr),
                    Some(Some(st)) => Ok(crate::split::core::entry(st, p)),
                    Some(None) => Ok(cairn_split::Entry::OUT),
                }),
                ..cairn_split::View::default()
            };
            match cairn_split::step(&view, &cairn_split::Event::LeftTable) {
                cairn_split::Plan::Retire => {}
                cairn_split::Plan::Keep(cairn_split::Keep::InTable) => {
                    tracing::warn!(
                        range = id,
                        ram = table.version(),
                        bucket = stored
                            .as_ref()
                            .and_then(|s| s.as_ref())
                            .map(|s| s.version()),
                        "range missing from the in-RAM table but still in the bucket's table — not retiring"
                    );
                    continue;
                }
                cairn_split::Plan::Keep(_) => continue,
                other => unreachable!("core returned {other:?} for LeftTable"),
            }
        }
        if table.contains(p) {
            // The job of a dead range whose range is still in the table (a panic in the leadership
            // loop, swallowed by `LocalSet`): remove the dead handle so the loop below rebuilds the
            // engine. Without that every command to the range gets `Stopped` until the next
            // restart, and the balancer still counts the range as held.
            if handle.is_dead() && !handle.is_retiring() {
                tracing::error!(range = id, "the range's engine died — rebuilding");
                partitions.remove(p);
                spawner.balancer.forget(p);
                changed = true;
            }
            continue;
        }
        partitions.remove(p);
        spawner.balancer.forget(p);
        crate::metrics::schedules::forget_range(id);
        crate::metrics::query_index::forget_range(id);
        changed = true;
        tracing::info!(
            range = id,
            version = table.version(),
            "range left the table — retiring"
        );
        // A range leaving the table means only one thing — its split/merge is done, the id
        // never comes back — so its engine **retires**, not just shuts down: the leader writes
        // the tombstone (no node holding an old table can take the parent Db back), the engine
        // exits for good. Marked first: any command still arriving (routed by the old table)
        // gets `WrongRange` and goes looking for the new range (D23).
        handle.mark_retiring();
        // The tombstone is written by the engine's keeper; if the engine dies before that, the
        // tombstone-on-behalf round (SM-11) writes it instead.
        match tokio::time::timeout(SHUTDOWN_STEP, handle.retire()).await {
            Ok(_) => {}
            Err(_) => tracing::warn!(
                range = id,
                "the engine of a range that left the table did not answer the retire command"
            ),
        }
    }
    for p in table.partitions() {
        if partitions.contains(p) {
            continue;
        }
        // Do not wait for the engine to open: a lease take can be long, and this loop must not
        // stand still. A command to a new range before it has a leader is held by ingress
        // (bounded) — `AppState::ask_led`.
        match spawner.spawn(p, Readiness::Immediately).await {
            Ok(h) => {
                tracing::info!(
                    range = p.0,
                    version = table.version(),
                    "new range in the table — building an engine"
                );
                partitions.insert(p, h);
                changed = true;
            }
            Err(e) => {
                tracing::warn!(range = p.0, error = %e, "cannot build an engine for the new range, retrying next tick")
            }
        }
    }
    if changed {
        spawner.routing.request_refresh();
    }
}
