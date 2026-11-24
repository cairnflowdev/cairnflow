// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! SlateDB parameters: poll periods, flush, compaction, GC, the shared block cache.

use super::*;

/// Block and SST metadata cache for the **whole node**, shared by every Db.
///
/// By default SlateDB builds, for **each** `Db`, a 512 MiB block cache and a 128 MiB
/// metadata cache (`DEFAULT_BLOCK_CACHE_CAPACITY`, `..._META_...`): the memory cap grows
/// with the number of ranges, 256 hot ranges is ~160 GiB. The shared cache keeps exactly
/// those two numbers but for the whole node — a node with one range costs the same as
/// before, a node with 256 ranges costs no more. Each Db keeps its own keys in the cache
/// via `db_cache_id` = range id ([`ObjectStores::open_db`]), so two ranges never read
/// each other's blocks by mistake; a hot range takes most of the cache by itself, idle
/// ranges give it back.
///
/// SlateDB 0.17 already has this API (`DbBuilder::with_db_cache`): the shared cache is a
/// matter of *using* it, not of patching.
pub fn shared_db_cache() -> Arc<dyn DbCache> {
    use slatedb::db_cache::{
        foyer::{FoyerCache, FoyerCacheOptions},
        SplitCache, DEFAULT_BLOCK_CACHE_CAPACITY, DEFAULT_META_CACHE_CAPACITY,
    };
    let foyer = |max_capacity| -> Arc<dyn DbCache> {
        Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity,
            ..Default::default()
        }))
    };
    Arc::new(
        SplitCache::new()
            .with_block_cache(Some(foyer(DEFAULT_BLOCK_CACHE_CAPACITY)))
            .with_meta_cache(Some(foyer(DEFAULT_META_CACHE_CAPACITY)))
            .build(),
    )
}

/// Periods of SlateDB's **background poll** loops on a partition that is leading.
///
/// This is most of the cost of an **idle** partition: with no writes there is no WAL
/// flush, but SlateDB still reads the manifest and the compaction queue periodically —
/// each one a paid request to the object store, multiplied by the number of partitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PollIntervals {
    /// The writer's `Settings::manifest_poll_interval`.
    pub manifest: std::time::Duration,
    /// `CompactorOptions::poll_interval`: the coordinator reads the manifest and decides
    /// whether compaction is needed.
    pub compactor: std::time::Duration,
    /// `CompactionWorkerOptions::compactions_poll_interval`: the in-process worker reads
    /// `.compactions` looking for work.
    pub compaction_worker: std::time::Duration,
    /// SlateDB's GC period for each Db (all directories) — see [`DEFAULT_GC_INTERVAL`].
    pub gc: std::time::Duration,
    /// The longest an idle Db's GC may skip its turn (fork: `idle_skip_max`) — see
    /// [`DEFAULT_GC_INTERVAL`].
    pub gc_idle_skip_max: std::time::Duration,
    /// GC's `min_age`: objects younger than this are not deleted. SlateDB's default
    /// (5 minutes); only changed when measuring with divided periods.
    pub gc_min_age: std::time::Duration,
}

impl PollIntervals {
    /// SlateDB 0.17's defaults — kept for comparison measurements.
    pub const SLATEDB: Self = Self {
        manifest: std::time::Duration::from_secs(1),
        compactor: std::time::Duration::from_secs(5),
        compaction_worker: std::time::Duration::from_secs(5),
        gc: std::time::Duration::from_secs(600),
        gc_idle_skip_max: std::time::Duration::ZERO,
        gc_min_age: std::time::Duration::from_secs(300),
    };

    /// Every period divided by `factor` — to **measure** the long-term cost of the safety
    /// nets (hourly, daily periods) in a window of a few dozen seconds and then multiply
    /// back. For measurement only; never run for real with divided periods.
    pub fn scaled(self, factor: u32) -> Self {
        let f = factor.max(1);
        Self {
            manifest: self.manifest / f,
            compactor: self.compactor / f,
            compaction_worker: self.compaction_worker / f,
            gc: self.gc / f,
            gc_idle_skip_max: self.gc_idle_skip_max / f,
            gc_min_age: self.gc_min_age / f,
        }
    }
}

/// Default WAL flush period.
///
/// **This is the latency floor of every commit**, so it is a number worth understanding
/// rather than an anonymous constant. `PartitionWriter::commit` waits for
/// `await_durable()`, and a write is only durable once the WAL is flushed; SlateDB
/// flushes periodically, so a commit costs half a period on average and a full period at
/// worst. SlateDB's default is 100ms, and with a few commits per invocation that is half
/// a second per call — measurable, and the largest part of the measured number
/// (`harness/bench`).
///
/// 10ms is the opposite trade-off: flushing more often means **batching less**, i.e.
/// more, smaller WAL objects on the store. On `file://` that part is almost free; on S3
/// they are paid requests (`docs/benchmarks.md` lists the drivers that measure it). Anyone
/// running production on a real object store should tune `--flush-interval-ms` after
/// reading their own numbers, not trust this one.
pub const DEFAULT_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// Cairn's default background poll periods: **every safety net of a range at 1 hour**,
/// an idle Db's GC skipping its turn for up to 24 hours — the long-term marginal cost of
/// an idle range is ~0.002 request/s.
///
/// Every background job of a range already has an **event-driven** path; polling only
/// catches what no event reports — things done by **another process**, which cairn does
/// not have:
///
/// - **Writer manifest — 1 h.** Compaction results arrive through the F2 signal. The
///   remaining job of this period used to be: an old writer standing still **detecting
///   by itself that it was fenced** when both layer 1 (clock-based validity) and layer 2
///   (the session renewal loop) fall short. That job now belongs to the session's
///   **owner-record watch** (`session/`, [`crate::LeaseConfig::with_owner_watch`]): the
///   notice from whoever takes the range, the coordinator's `view`, plus a
///   conditional-GET net every 1 minute (before that, a LIST of `owners/` every TTL);
///   when it sees the range has moved to another session it revokes the grant — as soon
///   as it is told (`failover.rs`) and without polling per range. In a single process
///   nobody else writes the Db, and the old writer's writes always fail the CAS
///   (`writer_epoch`). The writer's reads see a manifest up to 1 h late only when
///   **another** process changes the manifest — which does not happen in cairn; GC keeps
///   replaced SSTs for at least `min_age` + the compactor's checkpoint age (15 minutes),
///   and in-process compaction tells the writer at once.
/// - **Compaction coordinator — 1 h.** New L0 arrives through the signal; jobs of a dead
///   worker are reclaimed in the 1 s `commit_compacted` loop (F2). Polling only catches
///   compactions submitted by another process.
/// - **Compaction worker — 1 h.** New work arrives through the signal; polling only
///   catches work released by another worker — cairn only has the embedded worker.
/// - **GC — 1 h, skipping its turn when the Db has not changed** (fork `idle_skip_max`):
///   see [`DEFAULT_GC_INTERVAL`].
///
/// Cost per round (measured): manifest poll ~2 requests, coordinator ~3.4, worker ~1.9,
/// one GC pass ~23. An idle range, long term: (2 + 3.4 + 1.9)/3600 + 23/86 400 ≈ 0.0023
/// request/s — instead of ~0.064 at periods of 60 s / 300 s / 300 s / GC 1 h.
pub const DEFAULT_POLLS: PollIntervals = PollIntervals {
    manifest: std::time::Duration::from_secs(3600),
    compactor: std::time::Duration::from_secs(3600),
    compaction_worker: std::time::Duration::from_secs(3600),
    gc: DEFAULT_GC_INTERVAL,
    gc_idle_skip_max: std::time::Duration::from_secs(24 * 3600),
    gc_min_age: std::time::Duration::from_secs(300),
};

/// Heartbeat period of the in-process compaction worker: **2 s** (SlateDB: 10 s).
///
/// A compaction job of a process that died stays in `Running` until its heartbeat is
/// older than [`COMPACTION_HEARTBEAT_TIMEOUT`]; meanwhile the coordinator refuses every
/// other L0 compaction of the tree. A range reopened after failover whose L0 has already
/// hit the cap has **writes stalled** for that whole time (F2: reclaiming runs in the
/// 1 s `commit_compacted` loop, no longer waiting for the 300 s poll — but it still waits
/// for the heartbeat to expire). The heartbeat only runs while the worker holds a job:
/// a GET + CAS of `.compactions` every 2 s while compacting, nothing when idle.
pub const COMPACTION_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(2);

/// Heartbeat expiry of a compaction job: **10 s** (SlateDB: 30 s) — the bound of the
/// write stall after failover described above. Five heartbeat periods: an engine thread
/// busy for a few seconds (several ranges share a thread) does not get a live worker's
/// job wrongly reclaimed; and a wrong reclaim only wastes work anyway — the worker loses
/// ownership, its result is dropped, the job runs again; no data is wrong.
pub const COMPACTION_HEARTBEAT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// SlateDB's GC period for each Db: **1 hour** instead of the default 10 minutes — and an
/// **idle** Db **skips its turn** for up to 24 hours.
///
/// One GC pass of a Db costs **~23 requests** (~9 manifest GETs, ~10 other GETs, ~4
/// LISTs; measured) even with nothing to delete. At 1 hour that is ~0.006 request/s per
/// range — the largest of the remaining background loops. The fork
/// (`GarbageCollectorOptions::idle_skip_max`) lets a GC running in the same process as
/// the Db skip its turn when the Db's in-memory manifest has not changed since the
/// previous pass, no checkpoint has just expired, and the previous pass ran a full
/// `min_age` after the last change (so it has already deleted the youngest garbage). A
/// busy range still runs GC every hour; an idle range runs two passes after its last
/// change and then one pass every 24 hours: ~23/86 400 ≈ 0.0003 request/s.
///
/// The 1 hour period: garbage (SSTs replaced by compaction, WAL already in L0) lingers
/// at most an hour longer — negligible storage cost, the total number of delete requests
/// does not change. `min_age` keeps its default of 5 minutes.
pub const DEFAULT_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// SlateDB's default GC options, every enabled task running every `polls.gc`, an idle Db
/// skipping its turn for up to `polls.gc_idle_skip_max` (see [`DEFAULT_GC_INTERVAL`]).
pub(super) fn gc_options(polls: &PollIntervals) -> slatedb::config::GarbageCollectorOptions {
    let every = polls.gc;
    let mut o = slatedb::config::GarbageCollectorOptions {
        idle_skip_max: (!polls.gc_idle_skip_max.is_zero()).then_some(polls.gc_idle_skip_max),
        ..Default::default()
    };
    for dir in [
        &mut o.manifest_options,
        &mut o.wal_options,
        &mut o.compacted_options,
        &mut o.compactions_options,
    ]
    .into_iter()
    .flatten()
    {
        dir.min_age = polls.gc_min_age;
    }
    for dir in [
        &mut o.manifest_options,
        &mut o.wal_options,
        &mut o.wal_fence_options,
        &mut o.compacted_options,
        &mut o.compactions_options,
    ]
    .into_iter()
    .flatten()
    {
        dir.interval = Some(every);
    }
    if let Some(detach) = &mut o.detach_options {
        detach.interval = Some(every);
    }
    o
}
