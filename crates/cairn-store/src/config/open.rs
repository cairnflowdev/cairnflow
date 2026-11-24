// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Opening a range's Db and readers (`open_db`, `build_db`, `open_reader*`).

use super::*;

/// The bound of one [`ObjectStores::detach_now`] pass.
pub const DETACH_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// How many times the object store retries each request of [`ObjectStores::detach_now`]
/// (SlateDB default: unbounded).
pub const DETACH_RETRIES: u32 = 3;

impl ObjectStores {
    /// Run **one** pass of SlateDB GC's `detach` task on the Db of range `range` right
    /// now (SM-24), instead of waiting for the Db's own GC tick (every `polls.gc`, 1 h,
    /// counted from when the Db was opened — measured: the depth reaches 0 after ~55
    /// minutes).
    ///
    /// Exactly the task the Db's GC runs (fork `garbage_collector/detach_gc.rs`), through
    /// the public API `GarbageCollectorBuilder` + `run_gc_once`, with every other task
    /// off: drop expired checkpoints from the manifest, then for each `external_dbs` entry
    /// whose `sst_ids` are empty in the current manifest **and** in every live checkpoint
    /// manifest that points at it: delete the final checkpoint on the ancestor, then drop
    /// the entry (manifest CAS — an open writer that sees the conflict reads again, as
    /// with its own GC). No condition changes; only **when** it runs. SlateDB's GC
    /// swallows per-task errors (it logs them): the result is read back with
    /// [`Self::range_external_paths`]. Object-store retries are bounded
    /// ([`DETACH_RETRIES`]); the whole pass is bounded by [`DETACH_BOUND`].
    pub async fn detach_now(&self, range: u32) -> Result<()> {
        let options = slatedb::config::GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: Some(Default::default()),
            object_store_max_retries: Some(DETACH_RETRIES),
            ..Default::default()
        };
        let mut builder = slatedb::GarbageCollectorBuilder::new(
            self.range_path(range),
            Arc::clone(self.main.store()),
        )
        .with_options(options);
        if let Some(seed) = crate::entropy::slatedb_seed() {
            builder = builder.with_seed(seed);
        }
        if let Some(clock) = crate::entropy::slatedb_clock() {
            builder = builder.with_system_clock(clock);
        }
        let gc = builder.build();
        crate::retry::deadline(
            &crate::retry::ops::LINEAGE_COMPACT,
            DETACH_BOUND,
            gc.run_gc_once(),
        )
        .await
        .map_err(|_| StoreError::Io(format!("detach range {range}: over {DETACH_BOUND:?}")))
    }

    /// Open SlateDB for a range (`range` = `RangeId.0`), with both tiers plugged in the
    /// right place.
    ///
    /// Checks the data format marker first (writing the marker if the bucket is empty) —
    /// see [`crate::DATA_FORMAT`].
    pub async fn open_db(&self, range: u32) -> Result<Db> {
        self.build_db(range, None).await
    }

    /// The WAL file id ranges that the latest manifest and every live checkpoint of the
    /// Db of range `range` reference (F1) — for the shared WAL's merged GC.
    ///
    /// Bounded by [`ADMIN_IO_BOUND`] (`admin_io`): `Admin`'s store retries without bound
    /// in production, and a hung read here once stalled the whole `gc_loop` (every I/O
    /// `.await` in a background loop must be bounded). Named differently from the SlateDB
    /// operation so that check 3 of `lint-sources.sh` only catches direct calls to
    /// `Admin::referenced_wal_ranges`.
    pub(crate) async fn stored_wal_refs(
        &self,
        range: u32,
    ) -> Result<Vec<slatedb::wal::WalFileRange>> {
        admin_io(self.admin(range).referenced_wal_ranges()).await
    }

    /// Compactor options of every range Db, for both build paths (inside `Settings` and
    /// `CompactorBuilder` on the background runtime).
    ///
    /// Incremental compaction (RFC 0035, SlateDB 0.18) stays **off** — SlateDB's
    /// default, pinned by G27 (`docs/reliability/irreversible.md` §0.3). Turning it on
    /// changes the order in which source SSTs are deleted (touching G8) and writes
    /// partial commits into the manifest that binaries before 0.18 do not understand: it
    /// is a decision of its own, not a config line.
    pub(crate) fn compactor_options(&self) -> slatedb::config::CompactorOptions {
        let mut compactor = slatedb::config::CompactorOptions {
            poll_interval: self.polls.compactor,
            worker_heartbeat_timeout: COMPACTION_HEARTBEAT_TIMEOUT,
            worker: Some(slatedb::config::CompactionWorkerOptions {
                compactions_poll_interval: self.polls.compaction_worker,
                heartbeat_interval: COMPACTION_HEARTBEAT,
                ..Default::default()
            }),
            // G33: a trivial move completes a compaction without rewriting a row, so it drops
            // no tombstone. A full compaction of a tenant purge must rewrite. Off is SlateDB's
            // default; cairn sets it so a change of default cannot turn it on.
            enable_trivial_move: false,
            ..Default::default()
        };
        if let Some(d) = self.compactor_checkpoint_lifetime {
            compactor.checkpoint_lifetime = d;
        }
        compactor
    }

    /// Open a range's Db; `shared` = shared WAL: the range's `WriterInit` and its
    /// `WalGc`. `None` = SlateDB's native WAL.
    pub(crate) async fn build_db(
        &self,
        range: u32,
        shared: Option<(
            Box<dyn slatedb::wal::WriterInit>,
            Arc<dyn slatedb::wal::WalGc>,
        )>,
    ) -> Result<Db> {
        crate::format::ensure(self.main.store(), self.main.prefix(), true).await?;
        let path = self.range_path(range);
        let compactor = self.compactor_options();
        let mut settings = slatedb::Settings {
            flush_interval: Some(self.flush_interval),
            manifest_poll_interval: self.polls.manifest,
            compactor_options: Some(compactor.clone()),
            // The shared WAL builds its GC with a separate builder (below) — with two
            // overlapping configs SlateDB warns on every open.
            garbage_collector_options: shared.is_none().then(|| gc_options(&self.polls)),
            ..Default::default()
        };
        // With a background runtime the compactor is built with a separate builder to pick
        // its runtime — and `compactor_options` must be empty, otherwise SlateDB warns
        // about two overlapping configs on every open.
        let compactor_builder = self.background.as_ref().map(|rt| {
            settings.compactor_options = None;
            // Newly compacted SSTs go into the disk cache right away (`cache_on_compaction`).
            let store = self.db_store();
            // Chaos `--l0-pressure`: the compactor is slowed by the
            // `compactor::io_delay` failpoint, toggled at run time. Only the path with a
            // background runtime (every real node) — a compactor built inside `Settings`
            // cannot be wrapped.
            #[cfg(feature = "failpoints")]
            let store: Arc<dyn ObjectStore> = crate::failpoint::DelayStore::wrap(
                store,
                "compactor::io_delay",
                crate::failpoint::DelayOps::ReadsAndWrites,
            );
            slatedb::CompactorBuilder::new(path.clone(), store)
                .with_options(compactor)
                .with_runtime(rt.clone())
        });
        let stats = crate::dbstats::DbStats::new();
        let mut builder = Db::builder(path, self.db_store())
            .with_settings(settings)
            .with_db_cache(Arc::clone(&self.db_cache), u64::from(range))
            .with_metrics_recorder(
                Arc::clone(&stats) as Arc<dyn slatedb_common::metrics::MetricsRecorder>
            );
        // Deterministic simulation: the Db's seed and clock come from the sim's
        // source of nondeterminism when there is one — otherwise SlateDB picks its own.
        if let Some(seed) = crate::entropy::slatedb_seed() {
            builder = builder.with_seed(seed);
        }
        if let Some(clock) = crate::entropy::slatedb_clock() {
            builder = builder.with_system_clock(clock);
        }
        if let Some(window) = boundary_trust_window(&self.polls) {
            builder = builder.with_boundary_trust_window(window);
        }
        if let Some(compactor_builder) = compactor_builder {
            builder = builder.with_compactor_builder(compactor_builder);
        }
        if let Some(rt) = &self.background {
            builder = builder.with_gc_runtime(rt.clone());
        }
        if let Some(wal) = &self.wal {
            builder = builder.with_wal_object_store(Arc::clone(wal.store()));
        }
        if let Some((init, wal_gc)) = shared {
            // The shared WAL has no WAL objects or fences under `<root>/wal/`: the Db's
            // GC only cleans manifests and SSTs; the shared log's objects are the job of
            // the node's merged GC.
            let mut opts = gc_options(&self.polls);
            opts.wal_fence_options = None;
            builder = builder.with_wal_writer(init).with_gc_builder(
                slatedb::GarbageCollectorBuilder::new(
                    self.range_path(range),
                    Arc::clone(self.main.store()),
                )
                .with_options(opts)
                .with_wal_gc(wal_gc),
            );
        }
        // SlateDB retries object store errors without bound: bound the whole open. Past
        // the bound ⇒ `Io`, the range claim fails, the keeper yields and the leadership
        // loop backs off — another node gets a chance. Dropping the future midway is safe:
        // the next open bumps `writer_epoch` and fences everything this one left behind.
        let stall = crate::failpoint::stall_if("store::open_db::stall", range);
        let build = Box::pin(builder.build());
        // Range label (testkit): SlateDB background tasks spawned during the open
        // inherit it — the per-range background cost.
        let db = crate::io_source::in_range(
            range,
            self.bounded_open(Box::pin(async move {
                stall.await;
                build.await
            })),
        )
        .await?;
        if let Ok(mut m) = self.db_stats.lock() {
            m.insert(range, stats);
        }
        Ok(db)
    }

    /// One SlateDB `build` (writer or reader) bounded by [`Self::open_bound`]. The
    /// caller passes a future already `Box::pin`ned: SlateDB's `build` future is large,
    /// and the engine thread (where the range claim runs) has a small stack — nesting
    /// one more level on the stack overflows it (debug build).
    async fn bounded_open<T>(
        &self,
        build: impl std::future::Future<Output = std::result::Result<T, slatedb::Error>>,
    ) -> Result<T> {
        crate::retry::deadline(&crate::retry::ops::OPEN_DB, self.open_bound, build)
            .await?
            .map_err(StoreError::from_slate)
    }

    /// The SlateDB metrics recorder of the Db of range `range` most recently opened
    /// through this instance (or a clone) — taken away, to be attached to the writer
    /// ([`crate::PartitionWriter::with_db_stats`]). `None` if already taken.
    pub fn take_db_stats(&self, range: u32) -> Option<Arc<crate::dbstats::DbStats>> {
        self.db_stats.lock().ok()?.remove(&range)
    }

    /// Open the **read** side of a partition — follower read.
    ///
    /// `DbReaderMode::FollowLatest`, on purpose: the reader **writes nothing** to the
    /// bucket, not even checkpoints. That is the same discipline as the follower having
    /// no write rights, and it is the mode that was measured. The price: there is no
    /// checkpoint holding its place, so GC can delete objects from under a reader that
    /// falls too far behind — then the read returns an error, and the error is the
    /// *right* thing to happen (better to refuse to serve than to answer wrong).
    ///
    /// `poll` is the manifest + WAL refresh period, i.e. the follower's **staleness
    /// floor**: there is no refresh-now hook in current SlateDB. Each poll round costs
    /// ~10 requests/partition — the caller picks the period by the number of partitions
    /// it opens.
    pub async fn open_reader(
        &self,
        range: u32,
        poll: std::time::Duration,
    ) -> Result<PartitionReader> {
        self.open_reader_with(range, poll, None).await
    }

    /// Like [`Self::open_reader`], but a range on the shared WAL reads its WAL through the
    /// node's tailers (`tailers`), in stream mode (F1). A range on the native WAL — or
    /// `tailers = None` — opens as before.
    ///
    /// In stream mode, `poll` is the tailer's period; the manifest polls per
    /// [`stream_manifest_poll`]: new writes arrive through the tailer, and when GC passes
    /// the point the reader needs the reader loads the manifest **at once**
    /// (`WalTruncated`, F1) — the periodic poll only picks up compaction results for the
    /// part read from SSTs, which does not need to be fresh.
    pub async fn open_reader_with(
        &self,
        range: u32,
        poll: std::time::Duration,
        tailers: Option<&crate::nodewal::NodeTailers>,
    ) -> Result<PartitionReader> {
        let shared = match tailers {
            Some(t) => t.reader(range).await?,
            None => None,
        };
        let Some(shared) = shared else {
            return self.open_reader_native(range, poll).await;
        };
        crate::format::ensure(self.main.store(), self.main.prefix(), false).await?;
        let builder = DbReader::builder(self.range_path(range), self.db_store())
            .with_db_cache(Arc::clone(&self.db_cache), u64::from(range))
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(DbReaderOptions {
                manifest_poll_interval: stream_manifest_poll(poll),
                wal_follow: slatedb::config::WalFollowMode::Stream,
                ..Default::default()
            })
            .with_wal_reader(Arc::new(shared.clone()));
        let builder = self.reader_trust(builder);
        Ok(PartitionReader::with_tail(
            crate::io_source::in_range(range, self.bounded_open(Box::pin(builder.build()))).await?,
            shared,
        ))
    }

    async fn open_reader_native(
        &self,
        range: u32,
        poll: std::time::Duration,
    ) -> Result<PartitionReader> {
        // Check the marker but **do not write**: a reader writes nothing to the bucket.
        crate::format::ensure(self.main.store(), self.main.prefix(), false).await?;
        let path = self.range_path(range);
        // Same shared cache, same `db_cache_id` as the range's writer: reader and
        // writer read the same SSTs of the same Db.
        let mut builder = DbReader::builder(path, self.db_store())
            .with_db_cache(Arc::clone(&self.db_cache), u64::from(range))
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(DbReaderOptions {
                manifest_poll_interval: poll,
                ..Default::default()
            });
        if let Some(wal) = &self.wal {
            builder = builder.with_wal_object_store(Arc::clone(wal.store()));
        }
        let builder = self.reader_trust(builder);
        Ok(PartitionReader::new(
            crate::io_source::in_range(range, self.bounded_open(Box::pin(builder.build()))).await?,
        ))
    }

    /// The reader's boundary trust window ([`boundary_trust_window`]), measured with the
    /// sim's SlateDB clock if there is one — the same clock as GC's `min_age`. The seed
    /// also comes from the sim: the reader retries object store errors with jitter drawn
    /// from its RNG (like `config::admin_builder`).
    fn reader_trust(
        &self,
        mut builder: slatedb::DbReaderBuilder<Path>,
    ) -> slatedb::DbReaderBuilder<Path> {
        if let Some(seed) = crate::entropy::slatedb_seed() {
            builder = builder.with_seed(seed);
        }
        if let Some(clock) = crate::entropy::slatedb_clock() {
            builder = builder.with_system_clock(clock);
        }
        if let Some(window) = boundary_trust_window(&self.polls) {
            builder = builder.with_boundary_trust_window(window);
        }
        builder
    }
}

/// The window during which a read of the latest manifest/`.compactions` may skip the GET
/// of `gc/*.boundary` when this very process has just confirmed that same version (fork
/// patch F7): **half of `gc_min_age`** (5 minutes ⇒ 150 s).
///
/// SlateDB's GC only deletes a manifest version `min_age` after it was created, and the
/// successor of the confirmed version is created after the confirmation; within the
/// window, a 404 on the probe for `id+1` proves the cached version is still the latest.
/// The other half of `min_age` is the clock-skew margin between the node running GC and
/// the object store. Every GC that cleans a range Db's manifests (the GC inside the Db,
/// the GC built separately for the shared WAL) uses the same `gc_min_age`
/// ([`gc_options`]); D-d, deleting the whole Db, behaves as before F7 (absent boundary
/// = 0). `gc_min_age` 0 (periods divided down for measurement) ⇒ no GET is skipped.
pub(crate) fn boundary_trust_window(polls: &PollIntervals) -> Option<std::time::Duration> {
    let window = polls.gc_min_age / 2;
    (!window.is_zero()).then_some(window)
}

/// Manifest poll period of a stream-mode reader: 20 tailer periods, within `[10 s, 30 s]`
/// — 10 s at the default 500 ms follower period, instead of a LIST + GET every 500 ms.
/// The reader still loads the manifest at once when it meets a truncation (F1), and SSTs
/// that a 30 s old manifest points at are not deleted by GC (`min_age` 5 minutes). One
/// case waits for a full period: a new iterator meets exactly the hole it just met (the
/// manifest covering the hole is not visible yet) — with the shared WAL that does not
/// happen, because GC only passes frames that the range's **stored** manifest already
/// covers.
pub(crate) fn stream_manifest_poll(poll: std::time::Duration) -> std::time::Duration {
    (poll * 20).clamp(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_secs(30),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **G27 — cairn does not turn on incremental compaction.**
    /// Every range Db builds its compactor from `compactor_options`; with any cairn
    /// config that field is `false`. Red ⇒ someone turned it on: that needs a decision of
    /// its own (order of source-SST deletion, G8; manifests with partial commits).
    #[test]
    fn g27_incremental_compaction_stays_off() {
        let mem = || StoreLocation::from_store(Arc::new(InMemory::new()), "", "memory://");
        let plain = ObjectStores::single(mem());
        let tuned = ObjectStores::single(mem())
            .with_polls(crate::config::DEFAULT_POLLS.scaled(2))
            .with_compactor_checkpoint_lifetime(std::time::Duration::from_secs(5));
        for stores in [plain, tuned] {
            assert!(
                !stores
                    .compactor_options()
                    .enable_experimental_incremental_compaction
            );
        }
    }

    /// **G33 — cairn does not turn on trivial moves.** A trivial move completes a
    /// compaction of non-overlapping SSTs by moving them into the destination run, without
    /// rewriting a row: it drops no tombstone, and a tenant purge's full compaction would
    /// prove nothing. Red ⇒ someone turned it on.
    #[test]
    fn g33_trivial_move_stays_off() {
        let mem = || StoreLocation::from_store(Arc::new(InMemory::new()), "", "memory://");
        let plain = ObjectStores::single(mem());
        let tuned = ObjectStores::single(mem())
            .with_polls(crate::config::DEFAULT_POLLS.scaled(2))
            .with_compactor_checkpoint_lifetime(std::time::Duration::from_secs(5));
        for stores in [plain, tuned] {
            assert!(!stores.compactor_options().enable_trivial_move);
        }
    }
}
