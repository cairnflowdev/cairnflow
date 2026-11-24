// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Which bucket to point at — and **checking that the bucket can uphold the fence**.
//!
//! At first SlateDB was built directly on `LocalFileSystem` in `main.rs`. That part was split
//! out: two separate store tiers (D7), addresses taken from configuration, and **a pass/fail
//! gate that runs before the database is opened**.
//!
//! ## Why there is a gate
//!
//! This system's fence against a zombie leader is **not** `writer_epoch`. Measured: SlateDB
//! PUTs every WAL SST with `PutMode::Create`, and `AlreadyExists` translates straight into
//! `Fenced`. So the last line of defence is **the object store's conditional PUT**. A store
//! that swallows that condition makes the single-writer guarantee silently disappear — and no
//! test at the upper layers catches it.
//!
//! Garage v1.0.1 fails this gate: the second PUT is accepted, the data is overwritten, and
//! end to end a zombie acks a write and then corrupts the WAL to the point where the DB cannot
//! be reopened.
//!
//! Hence D12: **a store that does not honour conditional PUT is refused outright, on both
//! tiers** — refusing to start, not warning and carrying on. The gate applies to the SST tier
//! *too* because the manifest, which holds `writer_epoch`, lives in the main store.

use {
    crate::{
        error::{Result, StoreError},
        partition_table::{PartitionKey, RangeId},
        reader::PartitionReader,
    },
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::{
        admin::{Admin, AdminBuilder},
        config::DbReaderOptions,
        db_cache::DbCache,
        object_store::{
            local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore, ObjectStoreExt,
            PutMode, PutOptions, PutPayload,
        },
        CloneSourceSpec, Db, DbReader, DbReaderMode,
    },
    std::{ops::Bound, sync::Arc},
};

mod location;
pub use location::*;
mod tuning;
pub use tuning::*;
mod gate;
mod open;
mod sizing;
pub use sizing::*;

mod range_db;
pub use range_db::*;

/// A full compaction requested through `Admin` and the newest `.compactions` version seen
/// that still held it ([`ObjectStores::full_compaction_done`] searches from there, G31).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionWatch {
    /// The compaction id (ULID text): what a clean range proof records.
    pub id: String,
    ulid: ulid::Ulid,
    seen: Option<u64>,
}

/// What [`ObjectStores::submit_full_compaction`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FullCompaction {
    /// The Db does not exist or has nothing to compact.
    Nothing,
    /// Not requested: a snapshot older than the flushed data is open, and a compaction now
    /// would keep what it needs (G32). Request again later.
    SnapshotOpen,
    /// Requested.
    Requested(CompactionWatch),
}

impl FullCompaction {
    /// The watch of a requested compaction; `None` for the other outcomes.
    pub fn requested(self) -> Option<CompactionWatch> {
        match self {
            Self::Requested(w) => Some(w),
            Self::Nothing | Self::SnapshotOpen => None,
        }
    }
}

/// The two store tiers (D7).
///
/// WAL and SST are opposite workloads — small objects written very often vs. large objects
/// written rarely — so they are allowed to live on two different systems. This is
/// configuration SlateDB already has (RFC 0009), not something we built.
///
/// **Shared root path.** SlateDB puts the WAL at `<root>/wal/` on the WAL store and the SSTs at
/// `<root>/compacted/` on the main store — the same `root`. So the prefix comes from the main
/// store's URI; a WAL store URI carrying a different prefix is a contradiction, and
/// [`Self::open`] refuses instead of silently picking one of the two.
#[derive(Clone)]
pub struct ObjectStores {
    main: StoreLocation,
    /// `None` ⇒ the WAL shares the main store (dev, one node).
    wal: Option<StoreLocation>,
    /// SlateDB's WAL flush interval — see [`DEFAULT_FLUSH_INTERVAL`].
    flush_interval: std::time::Duration,
    /// Intervals of SlateDB's background poll loops — see [`PollIntervals`].
    polls: PollIntervals,
    /// Block + metadata cache **shared** by every Db (and reader) opened through this value or
    /// its clones — see [`shared_db_cache`].
    db_cache: Arc<dyn DbCache>,
    /// Runtime for SlateDB's compaction and GC. `None` ⇒ the runtime of the caller of
    /// [`Self::open_db`] — see [`Self::with_background_runtime`].
    background: Option<tokio::runtime::Handle>,
    /// Lifetime of the checkpoint the compactor writes before each manifest update (SlateDB:
    /// 10 minutes). It pins the SSTs just merged — including SSTs inherited from a parent Db —
    /// so it is also the minimum delay of D-d. `None` = SlateDB's default.
    compactor_checkpoint_lifetime: Option<std::time::Duration>,
    /// SlateDB metrics recorder of the Db just opened, by range id — waiting for the writer
    /// opener to take it ([`Self::take_db_stats`]). Shared between clones.
    db_stats: Arc<std::sync::Mutex<std::collections::BTreeMap<u32, Arc<crate::dbstats::DbStats>>>>,
    /// Bound on the Db open path — see [`DEFAULT_OPEN_DB_BOUND`].
    open_bound: std::time::Duration,
    /// The node's SST disk cache — `None` = off (default).
    disk_cache: Option<crate::disk_cache::DiskCache>,
    /// Directory of the local main store whose lock [`Self::exclusive_local`] took — for the
    /// disk gauge of `/metrics`.
    data_dir: Option<std::path::PathBuf>,
    /// Classified trash entries ([`crate::trash::Seen`]): shared between clones, so every loop
    /// of the node reads each entry's `entry` only once.
    trash_seen: crate::trash::Seen,
}

impl ObjectStores {
    /// Both tiers on the same store — the `Dev / single node` shape.
    pub fn single(main: StoreLocation) -> Self {
        Self {
            main,
            wal: None,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            polls: DEFAULT_POLLS,
            db_cache: shared_db_cache(),
            background: None,
            compactor_checkpoint_lifetime: None,
            db_stats: Default::default(),
            open_bound: DEFAULT_OPEN_DB_BOUND,
            disk_cache: None,
            data_dir: None,
            trash_seen: Default::default(),
        }
    }

    /// **The only node on a local directory**: the main store is `file://` ⇒ take the exclusive
    /// lock on its directory (and on the WAL store's, if it is also local and a different
    /// directory), then wrap each local tier in [`crate::local::LocalCas`] — an in-process
    /// simulated CAS, correct because the lock keeps every other process out. A main store that
    /// is not local ⇒ left as is, `None`.
    ///
    /// The lock lives until [`crate::local::LocalLock`] is dropped. Already held ⇒ error.
    pub fn exclusive_local(self) -> Result<(Self, Option<crate::local::LocalLock>)> {
        let Some(main_dir) = self.main.local_dir.clone() else {
            return Ok((self, None));
        };
        let mut locks = vec![crate::local::DirLock::acquire(&main_dir)?];
        if let Some(wal_dir) = self.wal.as_ref().and_then(|w| w.local_dir.clone()) {
            if wal_dir != main_dir {
                locks.push(crate::local::DirLock::acquire(&wal_dir)?);
            }
        }
        let cas = |loc: StoreLocation| match loc.local_dir {
            Some(_) => loc.wrap(crate::local::LocalCas::wrap(Arc::clone(loc.store()))),
            None => loc,
        };
        let Self { main, wal, .. } = self.clone();
        Ok((
            Self {
                main: cas(main),
                wal: wal.map(cas),
                data_dir: Some(main_dir),
                ..self
            },
            Some(crate::local::LocalLock(locks)),
        ))
    }

    /// **Turns on the SST disk cache**: one [`crate::disk_cache::DiskCache`] over the
    /// **current** main store, shared by every Db, reader and compactor opened through this
    /// value and its clones. Call it **after** every wrapping of the main store
    /// ([`Self::exclusive_local`], failpoints): the cache wraps exactly the store it sees when
    /// built, and a Db going through the cache does not go through layers added afterwards.
    pub async fn with_disk_cache(
        mut self,
        config: crate::disk_cache::DiskCacheConfig,
    ) -> Result<Self> {
        self.disk_cache =
            Some(crate::disk_cache::DiskCache::open(config, Arc::clone(self.main.store())).await?);
        Ok(self)
    }

    /// The local data directory whose lock was taken ([`Self::exclusive_local`]), if any.
    pub fn data_dir(&self) -> Option<&std::path::Path> {
        self.data_dir.as_deref()
    }

    /// The node's disk cache, if on.
    pub fn disk_cache(&self) -> Option<&crate::disk_cache::DiskCache> {
        self.disk_cache.as_ref()
    }

    /// The main store **for a range's Db, reader and compactor**: through the disk cache if on.
    /// `Admin`, GC and all of cairn's own I/O use [`StoreLocation::store`] directly — the cache
    /// would skip them anyway (untagged reads), but not wrapping them is clearer.
    pub(crate) fn db_store(&self) -> Arc<dyn ObjectStore> {
        match &self.disk_cache {
            Some(c) => c.store(),
            None => Arc::clone(self.main.store()),
        }
    }

    /// Paths of the L0 SSTs of range `range`'s Db according to the latest manifest, newest
    /// first — the input of a disk cache warm-up. Empty if the Db has no manifest yet.
    pub async fn l0_sst_paths(&self, range: u32) -> Result<Vec<Path>> {
        let Some(m) = admin_io(self.admin(range).read_manifest(None)).await? else {
            return Ok(Vec::new());
        };
        let resolver = slatedb::PathResolver::new(self.range_path(range), &m);
        let mut out: Vec<Path> = Vec::new();
        for v in m.l0() {
            let p = resolver.sst_path(&v.sst.id);
            if !out.contains(&p) {
                out.push(p);
            }
        }
        Ok(out)
    }

    /// **Warms the disk cache** for range `range`: the manifest's L0 SSTs, newest first, up to
    /// [`crate::disk_cache::WARM_MAX_BYTES`]. No cache ⇒ nothing. Returns the number of SSTs
    /// loaded (`None` = no cache, or reading the manifest failed).
    pub async fn warm_disk_cache(&self, range: u32) -> Option<usize> {
        let cache = self.disk_cache.as_ref()?;
        let paths = self.l0_sst_paths(range).await.ok()?;
        let n = paths.len();
        cache
            .warm(paths, crate::disk_cache::WARM_MAX_BYTES)
            .await
            .then_some(n)
    }

    /// Runs `fut` on the background runtime ([`Self::with_background_runtime`]) if any,
    /// otherwise on the caller's runtime — background work must not sit on the engine thread
    /// (disk cache warm-up).
    pub fn spawn_background(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        match &self.background {
            Some(rt) => drop(rt.spawn(fut)),
            None => drop(tokio::spawn(fut)),
        }
    }

    /// Changes the compactor's checkpoint lifetime — see the field of the same name.
    pub fn with_compactor_checkpoint_lifetime(mut self, d: std::time::Duration) -> Self {
        self.compactor_checkpoint_lifetime = Some(d);
        self
    }

    /// Changes the bound on the Db open path — see [`DEFAULT_OPEN_DB_BOUND`].
    pub fn with_open_bound(mut self, bound: std::time::Duration) -> Self {
        self.open_bound = bound;
        self
    }

    /// Bound on the Db open path — see [`DEFAULT_OPEN_DB_BOUND`].
    pub fn open_bound(&self) -> std::time::Duration {
        self.open_bound
    }

    /// Changes SlateDB's background poll intervals. See [`DEFAULT_POLLS`].
    pub fn with_polls(mut self, polls: PollIntervals) -> Self {
        self.polls = polls;
        self
    }

    /// Uses `cache` instead of the self-built shared cache — see [`shared_db_cache`].
    pub fn with_db_cache(mut self, cache: Arc<dyn DbCache>) -> Self {
        self.db_cache = cache;
        self
    }

    /// Runs compaction and GC of every Db on `runtime` instead of the runtime of whoever opened
    /// the Db.
    ///
    /// The engines of many ranges share one `current_thread` thread: one compaction computing
    /// for a few hundred ms on that thread makes **every** range on it wait. Moving SlateDB's
    /// heaviest computation out to a separate multi-threaded runtime leaves the range's thread
    /// with only IO waits. The (light) compaction coordination loops still run next to the Db;
    /// only the execution moves here — an API SlateDB already has
    /// (`CompactorBuilder::with_runtime`, `DbBuilder::with_gc_runtime`).
    pub fn with_background_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.background = Some(runtime);
        self
    }

    /// Changes the WAL flush interval. See [`DEFAULT_FLUSH_INTERVAL`] about its cost.
    pub fn with_flush_interval(mut self, every: std::time::Duration) -> Self {
        self.flush_interval = every;
        self
    }

    /// Two separate tiers.
    ///
    /// `Err` if the two URIs have different prefixes: the root path is shared, so two
    /// different prefixes mean nothing, and silently picking one is how an operator ends up
    /// believing the WAL lives where it does not.
    pub fn split(main: StoreLocation, wal: StoreLocation) -> Result<Self> {
        if !wal.prefix.as_ref().is_empty() && wal.prefix != main.prefix {
            return Err(StoreError::BadStoreUri {
                uri: wal.uri.clone(),
                reason: format!(
                    "prefix {:?} differs from the main store's prefix {:?}; the two tiers share one root \
                     path (SlateDB puts the WAL at <root>/wal/), so either drop the prefix from the WAL URI or \
                     make both equal",
                    wal.prefix.as_ref(),
                    main.prefix.as_ref()
                ),
            });
        }
        Ok(Self {
            main,
            wal: Some(wal),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            polls: DEFAULT_POLLS,
            db_cache: shared_db_cache(),
            background: None,
            compactor_checkpoint_lifetime: None,
            db_stats: Default::default(),
            open_bound: DEFAULT_OPEN_DB_BOUND,
            disk_cache: None,
            data_dir: None,
            trash_seen: Default::default(),
        })
    }

    pub fn main(&self) -> &StoreLocation {
        &self.main
    }

    /// The main store's trash, sharing the node's memory of classified entries
    /// ([`crate::trash::Trash::held_entries`]).
    pub fn main_trash(&self) -> crate::trash::Trash {
        crate::trash::Trash::new(Arc::clone(self.main.store()), self.main.prefix())
            .with_seen(self.trash_seen.clone())
    }

    /// The WAL tier's store. Returns the main store if no split is configured.
    pub fn wal(&self) -> &StoreLocation {
        self.wal.as_ref().unwrap_or(&self.main)
    }

    /// Declares the `atomic_put` capability of the WAL tier (the main store when the WAL shares
    /// it) — `--wal-atomic-put`; `None` keeps the per-scheme default.
    pub fn with_wal_atomic_put(mut self, atomic: Option<bool>) -> Self {
        if let Some(a) = atomic {
            match &mut self.wal {
                Some(w) => {
                    w.atomic_put = a;
                    w.atomic_put_declared = true;
                }
                None => {
                    self.main.atomic_put = a;
                    self.main.atomic_put_declared = true;
                }
            }
        }
        self
    }

    /// Attaches a store clock sampler to the main store (tests — `s3://` has its own,
    /// [`StoreLocation::store_clock`]).
    pub fn with_store_clock(mut self, clock: Option<crate::store_clock::StoreClock>) -> Self {
        self.main.store_clock = clock;
        self
    }

    pub fn is_split(&self) -> bool {
        self.wal.is_some()
    }

    /// A copy whose every request to the store (main, and WAL if split) is also counted into
    /// `counter` ([`crate::metered::MeteredStore::counting`]) — to measure the cost of a
    /// background loop running on this copy (the state scrubber). The counting layer is
    /// transparent (keeps `local_dir`); the cache, intervals and other configuration are shared
    /// with the original.
    pub fn counting(&self, counter: &Arc<std::sync::atomic::AtomicU64>) -> Self {
        let wrap = |loc: &StoreLocation| {
            loc.wrap_transparent(crate::metered::MeteredStore::counting(
                Arc::clone(loc.store()),
                Arc::clone(counter),
            ))
        };
        Self {
            main: wrap(&self.main),
            wal: self.wal.as_ref().map(wrap),
            ..self.clone()
        }
    }

    /// Root path of a range's Db: `<prefix>/ranges/<id>` (D22). Ids are never reused, so the Db
    /// of a retired range is never opened by mistake for a new range.
    pub fn range_path(&self, range: u32) -> Path {
        let p = self.main.prefix.as_ref();
        if p.is_empty() {
            Path::from(format!("ranges/{range}"))
        } else {
            Path::from(format!("{p}/ranges/{range}"))
        }
    }

    // ─────────────────────────────────────────── split/merge primitives

    /// SlateDB Admin of a range's Db, with both tiers plugged in the right place.
    pub(crate) fn admin(&self, range: u32) -> Admin {
        self.admin_at(self.range_path(range))
    }

    pub(crate) fn admin_at(&self, path: Path) -> Admin {
        self.admin_with(path, None)
    }

    /// Admin with the WAL named `wal_name` (the marker in the manifest, F1): `None` is the
    /// native WAL, [`crate::SHARED_WAL_NAME`] is the per-node shared WAL.
    pub(crate) fn admin_with(&self, path: Path, wal_name: Option<&str>) -> Admin {
        let mut builder = admin_builder(path, Arc::clone(self.main.store()));
        if let Some(wal) = &self.wal {
            builder = builder.with_wal_object_store(Arc::clone(wal.store()));
        }
        if wal_name == Some(crate::SHARED_WAL_NAME) {
            builder = builder.with_wal_admin(Arc::new(crate::nodewal::SharedWalAdmin::new(
                Arc::clone(self.main.store()),
                self.main.prefix().clone(),
            )));
        }
        builder.build()
    }

    /// Requests a **full** compaction of the root tree of range `range`'s Db: every L0 and
    /// every sorted run merged into the **last** sorted run. Compacting into the last run drops
    /// the tombstones at the tail (G23, `docs/reliability/irreversible.md` §0.3), so logically
    /// deleted rows leave the SSTs — then SlateDB's GC deletes the old SSTs after
    /// `gc_min_age`. The compaction runs in the compactor of the process that has the Db open
    /// (G5); if nobody has it open it stays `Submitted` until the next open.
    ///
    /// **G32**: a compaction keeps every version an open snapshot needs, so the request waits
    /// while the newest manifest has `recent_snapshot_min_seq < last_l0_seq` (a snapshot older
    /// than flushed data is open): [`FullCompaction::SnapshotOpen`]. The worker takes its
    /// retention bound from a manifest it reads after it claims the job, and that bound does
    /// not go down; so once this manifest passes, every flushed version the job sees may go.
    /// Call it right after a flush ([`crate::PartitionWriter::flush_memtable_fresh`] always stores a
    /// fresh manifest).
    pub async fn submit_full_compaction(&self, range: u32) -> Result<FullCompaction> {
        use slatedb::compactor::{CompactionSpec, SourceId};
        let admin = self.admin(range);
        let Some(m) = admin_io(admin.read_manifest(None)).await? else {
            return Ok(FullCompaction::Nothing);
        };
        let mut sources: Vec<SourceId> = m.l0().iter().map(|v| SourceId::SstView(v.id)).collect();
        sources.extend(m.compacted().iter().map(|r| SourceId::SortedRun(r.id)));
        if sources.is_empty() {
            return Ok(FullCompaction::Nothing);
        }
        if m.recent_snapshot_min_seq() < m.last_l0_seq() {
            return Ok(FullCompaction::SnapshotOpen);
        }
        // `compacted` lists newest first: the last run is the bottom run. No run at all ⇒ a new
        // id above every existing id (the coordinator's rule).
        let destination = match m.compacted().last() {
            Some(r) => r.id,
            None => 0,
        };
        let spec = CompactionSpec::new(sources, destination);
        let c = admin_io(admin.submit_compaction(spec)).await?;
        Ok(FullCompaction::Requested(
            self.watch_compaction(range, c.id()).await?,
        ))
    }

    /// Starts to watch compaction `id` of range `range`'s Db: notes the newest `.compactions`
    /// version that holds it, the start of the search of [`Self::full_compaction_done`].
    pub async fn watch_compaction(&self, range: u32, id: ulid::Ulid) -> Result<CompactionWatch> {
        let latest = admin_io(self.admin(range).read_compactions(None)).await?;
        let seen = latest
            .filter(|v| v.recent_compactions().any(|c| c.id() == id))
            .map(|v| v.id());
        Ok(CompactionWatch {
            id: id.to_string(),
            ulid: id,
            seen,
        })
    }

    /// Whether the watched compaction has finished: `Some(true)` = `Completed`, `Some(false)` =
    /// failed **or unknown** (request again), `None` = waiting, running, or `Compacted` (the
    /// coordinator can still fail it when it validates the output against the manifest).
    ///
    /// **G31**: the ledger keeps the active jobs and the finished job with the highest id, so a
    /// job can leave it soon after it ends. The fork stores every finish once with its final
    /// status, so a job that left is looked up in the stored versions after the last one seen
    /// (a binary search: a job never comes back once it leaves). A version that GC already
    /// deleted ⇒ unknown. Poll well within `gc_min_age`.
    pub async fn full_compaction_done(
        &self,
        range: u32,
        watch: &mut CompactionWatch,
    ) -> Result<Option<bool>> {
        use slatedb::compactor::CompactionStatus as S;
        let admin = self.admin(range);
        let Some(latest) = admin_io(admin.read_compactions(None)).await? else {
            return Ok(Some(false));
        };
        let newest = latest.id();
        if let Some(c) = latest.recent_compactions().find(|c| c.id() == watch.ulid) {
            watch.seen = Some(newest);
            return Ok(match c.status() {
                S::Completed => Some(true),
                S::Failed => Some(false),
                _ => None,
            });
        }
        // `held` holds the job, `gone` does not.
        let Some(mut held) = watch.seen.filter(|&v| v < newest) else {
            return Ok(Some(false));
        };
        let mut gone = newest;
        let holds = |v: &slatedb::VersionedCompactions| {
            v.recent_compactions()
                .find(|c| c.id() == watch.ulid)
                .map(|c| c.status())
        };
        while gone - held > 1 {
            let mid = held + (gone - held) / 2;
            let Some(v) = admin_io(admin.read_compactions(Some(mid))).await? else {
                return Ok(Some(false));
            };
            if holds(&v).is_some() {
                held = mid;
            } else {
                gone = mid;
            }
        }
        let Some(v) = admin_io(admin.read_compactions(Some(held))).await? else {
            return Ok(Some(false));
        };
        Ok(Some(holds(&v) == Some(S::Completed)))
    }

    /// The WAL-kind marker in the stored manifest of a range's Db; `Ok(None)` both when the Db
    /// does not exist and when it uses the native WAL — distinguished by `Some(manifest)`.
    pub(crate) async fn manifest_wal_name(&self, range: u32) -> Result<Option<Option<String>>> {
        Ok(self.manifest_head(range).await?.map(|h| h.wal_name))
    }

    /// The WAL-kind marker and `replay_after_wal_id` in the latest stored manifest of a range's
    /// Db; `Ok(None)` when the Db does not exist. Marker `None` = native WAL.
    pub(crate) async fn manifest_head(&self, range: u32) -> Result<Option<ManifestHead>> {
        Ok(admin_io(self.admin(range).read_manifest(None))
            .await?
            .map(|m| ManifestHead {
                wal_name: m.wal_name().map(str::to_string),
                replay_after_wal_id: m.replay_after_wal_id(),
            }))
    }
}

/// The `AdminBuilder` of every `Admin` cairn builds: the seed and the clock come from the
/// nondeterminism source (`entropy`) if one is installed, like the `Db` in `open_db`. `Admin`
/// retries object store errors with jitter drawn from the builder's RNG (upstream #2141);
/// unseeded, that RNG draws from the operating system and the deterministic simulation's trace
/// diverges at the first retry. Outside the sim it is the bare builder.
pub(crate) fn admin_builder(path: Path, store: Arc<dyn ObjectStore>) -> AdminBuilder<Path> {
    let mut builder = AdminBuilder::new(path, store);
    if let Some(seed) = crate::entropy::slatedb_seed() {
        builder = builder.with_seed(seed);
    }
    if let Some(clock) = crate::entropy::slatedb_clock() {
        builder = builder.with_system_clock(clock);
    }
    builder
}

/// Bound on the Db open path: SlateDB's `build` (writer and reader) and the flush of replayed
/// rows when taking over a range on the shared WAL. Those operations retry object store errors
/// **forever** in production; without a bound a long store failure hangs the open while the
/// session keeps renewing — the node holds the lease of a range nobody serves, and no other
/// node can take it.
///
/// 60 s: an ordinary open is a few GETs and one manifest CAS, plus replay (possibly a few L0
/// flushes); even under the chaos runs' slow S3 profile there is plenty of headroom. It exceeds
/// the shared WAL's default `stall_max` (6 s) plus one memtable flush, so the flush of replayed
/// rows is only cut off when the log stream has also exceeded its budget.
pub const DEFAULT_OPEN_DB_BOUND: std::time::Duration = std::time::Duration::from_secs(60);

/// Bound on one SlateDB `Admin` operation. `Admin` retries object store errors **forever** in
/// production (`object_store_max_retries: None`, and `AdminBuilder` does not let it be set): a
/// long store failure would hang every background loop that reads manifests (auto split, GC
/// references, the scrubber, orphan child cleanup) with it. 30 s: one operation is a few
/// GETs/LISTs (a few PUTs when writing a manifest); equal to the S3 client's total bound
/// ([`S3_CLIENT_RETRY_TIMEOUT`]). Exceeding it is `Io` — every caller already treats a failed
/// read as "not provable" and **keeps** (delete only when provable).
pub const ADMIN_IO_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// One `Admin` operation bounded by [`ADMIN_IO_BOUND`]. The future is dropped when the bound is
/// exceeded: for writes (`set_wal_name`, `delete_checkpoint`, `create_detached_checkpoint`) the
/// outcome is ambiguous — those callers read back before the next write, or only leave a leak
/// (a checkpoint with a lifetime), nothing is lost.
pub(crate) async fn admin_io<T>(
    fut: impl std::future::Future<Output = std::result::Result<T, slatedb::Error>>,
) -> Result<T> {
    match crate::retry::deadline(&crate::retry::ops::SLATE_ADMIN, ADMIN_IO_BOUND, fut).await {
        Ok(r) => r.map_err(StoreError::from_slate),
        Err(e) => Err(e.into()),
    }
}

/// The head of a stored manifest that taking over a range needs before opening the Db.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManifestHead {
    pub wal_name: Option<String>,
    /// A value that only **increases** over time: a stale read is a lower bound of the value the
    /// following Db open sees.
    pub replay_after_wal_id: u64,
}

impl std::fmt::Debug for ObjectStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStores")
            .field("main", &self.main)
            .field("wal", &self.wal)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> StoreLocation {
        StoreLocation::from_store(Arc::new(InMemory::new()), "", "memory://")
    }

    #[test]
    fn db_path_split_by_prefix_and_id() {
        let ok = |p: &str, id| Some((p.to_string(), id));
        assert_eq!(split_range_db_path("ranges/3"), ok("", 3));
        assert_eq!(split_range_db_path("/a/ranges/3/"), ok("a", 3));
        assert_eq!(split_range_db_path("a//ranges/3"), ok("a", 3));
        assert_eq!(split_range_db_path("a/b/ranges/42"), ok("a/b", 42));
        assert_eq!(split_range_db_path("a/ranges/03"), None);
        assert_eq!(split_range_db_path("a/ranges/x"), None);
        assert_eq!(split_range_db_path("a/other/3"), None);
        assert_eq!(split_range_db_path("a/ranges/3/wal"), None);
        assert_eq!(normalize_path("/a//b/"), "a/b");
    }

    #[tokio::test]
    async fn memory_and_local_both_pass_gate() {
        ObjectStores::single(mem())
            .assert_conditional_put()
            .await
            .unwrap();

        let dir = tempdir();
        let local = StoreLocation::parse(&format!("file://{}", dir.display())).unwrap();
        ObjectStores::single(local)
            .assert_conditional_put()
            .await
            .unwrap();
    }

    /// A store that swallows the condition — exactly the behaviour measured on Garage v1.0.1.
    #[tokio::test]
    async fn store_swallowing_conditions_is_rejected() {
        let stores = ObjectStores::single(StoreLocation::from_store(
            Arc::new(unconditional::Unconditional::default()),
            "",
            "test://unconditional",
        ));
        let err = stores.assert_conditional_put().await.unwrap_err();
        assert!(
            matches!(err, StoreError::ConditionalPutUnsupported { .. }),
            "must be a gate error, got {err:?}"
        );
    }

    /// A WAL tier that fails the gate must also be refused, even if the main store passes: the
    /// manifest is on one side, the WAL SSTs on the other, and the fence needs **both**.
    #[tokio::test]
    async fn wal_tier_failing_gate_still_blocks() {
        let stores = ObjectStores::split(
            mem(),
            StoreLocation::from_store(
                Arc::new(unconditional::Unconditional::default()),
                "",
                "test://unconditional",
            ),
        )
        .unwrap();
        let err = stores.assert_conditional_put().await.unwrap_err();
        let StoreError::ConditionalPutUnsupported { tier, .. } = &err else {
            panic!("must be a gate error, got {err:?}");
        };
        assert_eq!(tier, "WAL");
    }

    #[test]
    fn wal_prefix_differing_from_main_is_rejected() {
        let main = StoreLocation::from_store(Arc::new(InMemory::new()), "a", "memory://a");
        let wal = StoreLocation::from_store(Arc::new(InMemory::new()), "b", "memory://b");
        assert!(ObjectStores::split(main, wal).is_err());
    }

    #[test]
    fn empty_wal_prefix_follows_main_store() {
        let main = StoreLocation::from_store(Arc::new(InMemory::new()), "a", "memory://a");
        let wal = StoreLocation::from_store(Arc::new(InMemory::new()), "", "memory://");
        let stores = ObjectStores::split(main, wal).unwrap();
        assert_eq!(stores.range_path(3).as_ref(), "a/ranges/3");
    }

    #[test]
    fn parse_uri() {
        let dir = tempdir();
        let local = StoreLocation::parse(&format!("file://{}", dir.display())).unwrap();
        assert!(local.prefix().as_ref().is_empty());

        assert!(StoreLocation::parse("without-scheme").is_err());
    }

    /// A fixed nondeterminism source for [`retry_time`]: same seed ⇒ same byte sequence.
    struct Seeded(std::cell::Cell<u64>);

    impl crate::entropy::Source for Seeded {
        fn now(&self) -> std::time::SystemTime {
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_767_225_600)
        }

        fn process_id(&self) -> u32 {
            1
        }

        fn fill(&self, buf: &mut [u8]) {
            for b in buf {
                let s = self
                    .0
                    .get()
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                self.0.set(s);
                *b = (s >> 56) as u8;
            }
        }
    }

    /// Which [`ObjectStores`] open [`retry_time`] measures.
    #[derive(Clone, Copy, Debug)]
    enum Opener {
        /// The `Admin` of `admin_with` (reading the manifest: `manifest_head`).
        Admin,
        /// The `DbReader` of `open_reader`.
        Reader,
    }

    /// The **virtual** time one `ObjectStores` open takes when the first three LISTs of
    /// `manifest/` fail — i.e. the sum of three backoffs of SlateDB's retry layer
    /// (`RetryingObjectStore`), under the nondeterminism source [`Seeded`] seeded with `seed`.
    fn retry_time(opener: Opener, seed: u64) -> std::time::Duration {
        use crate::testkit::{Action, FaultyStore, Op, PathMatch, Rule};
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let _src =
                crate::entropy::install(std::rc::Rc::new(Seeded(std::cell::Cell::new(seed))));
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            faulty.add_rule(Rule::new(
                Op::List,
                PathMatch::Contains("/manifest".into()),
                Action::Fail { times: Some(3) },
            ));
            let stores = ObjectStores::single(StoreLocation::from_store(
                faulty.clone() as Arc<dyn ObjectStore>,
                "",
                "memory://",
            ));
            let t0 = tokio::time::Instant::now();
            match opener {
                Opener::Admin => {
                    let head = stores.manifest_head(0).await.unwrap();
                    assert!(
                        head.is_none(),
                        "the Db does not exist yet it has a manifest"
                    );
                }
                Opener::Reader => {
                    // The Db does not exist: the reader returns an error after the LIST retries
                    // succeed — the result does not matter, only the timing.
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(60),
                        stores.open_reader(0, std::time::Duration::from_secs(1)),
                    )
                    .await;
                }
            }
            assert_eq!(
                faulty.injected().0,
                3,
                "{opener:?}: must go through all three failed LISTs"
            );
            t0.elapsed()
        })
    }

    /// Deterministic simulation (`tests/sim_cluster.rs`): the `Admin` and `DbReader` that
    /// `ObjectStores` builds take their seed from the installed nondeterminism source, like the
    /// `Db` (`open_db`). SlateDB's retry layer sprinkles jitter on the backoff with the
    /// builder's RNG (upstream #2141, which the fork includes); a builder that is not seeded
    /// draws that RNG from the operating system.
    ///
    /// 1. Install the [`Seeded`] source with seed 7; the first three LISTs of `manifest/` fail.
    /// 2. Open (`manifest_head` / `open_reader`): SlateDB retries three times, each sleeping
    ///    `[d, 2d]` on the virtual clock.
    /// 3. Do exactly the same on a new runtime.
    ///
    /// Must see: two runs with the same seed take the same virtual time — i.e. the sim's retry
    /// lands on the same virtual millisecond, and the sim trace does not diverge (seed 4 of
    /// `same_seed_same_trace` diverged at a retried GET of `gc/manifest.boundary`).
    #[test]
    fn admin_and_reader_retry_by_source_seed() {
        for opener in [Opener::Admin, Opener::Reader] {
            let a = retry_time(opener, 7);
            let b = retry_time(opener, 7);
            assert!(
                a >= std::time::Duration::from_millis(100 + 200 + 400),
                "{opener:?}: three backoffs but only {a:?}"
            );
            assert_eq!(a, b, "{opener:?}: same seed but different backoffs");
        }
    }

    fn tempdir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "cairnflow-cfg-test-{}-{}",
            crate::entropy::process_id(),
            crate::entropy::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// An object store that ignores `PutMode::Create` — simulates Garage v1.0.1.
    mod unconditional {
        use {
            futures::stream::BoxStream,
            slatedb::object_store::{
                memory::InMemory, path::Path, CopyOptions, GetOptions, GetResult, ListResult,
                MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
                PutPayload, PutResult, Result,
            },
            std::fmt,
        };

        /// Delegates everything to `InMemory`, except exactly one place: `put_opts` throws
        /// `PutOptions` away. That is exactly what Garage does.
        #[derive(Debug, Default)]
        pub struct Unconditional {
            inner: InMemory,
        }

        impl fmt::Display for Unconditional {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "Unconditional")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for Unconditional {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                _opts: PutOptions,
            ) -> Result<PutResult> {
                self.inner
                    .put_opts(location, payload, PutOptions::default())
                    .await
            }

            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOptions,
            ) -> Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(location, opts).await
            }

            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                self.inner.get_opts(location, options).await
            }

            fn delete_stream(
                &self,
                locations: BoxStream<'static, Result<Path>>,
            ) -> BoxStream<'static, Result<Path>> {
                self.inner.delete_stream(locations)
            }

            fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
                self.inner.copy_opts(from, to, options).await
            }
        }
    }
}
