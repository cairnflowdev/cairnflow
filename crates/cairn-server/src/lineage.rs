// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Standalone children after a split.**
//!
//! After a split, the child Db stands on the parent Db — and on every ancestor
//! of the parent: SlateDB copies the source's `external_dbs` as well — until
//! its own compaction rewrites all the inherited SSTs and SlateDB's GC
//! `detach` removes the entry. A **cold** child (no writes) never compacts: it
//! pins every ancestor indefinitely, D-d can delete none of them, and the
//! chain grows one deeper with every later split.
//!
//! A background loop per node ([`Lineage::tick`]): among the ranges this node
//! leads, a range standing on ≥ `depth` ancestors, or on an ancestor retired
//! longer than `ancestor_age`, gets a **full** compaction submitted
//! ([`ObjectStores::submit_full_compaction`], as the tenant purge does) and
//! then the compactor is woken. Once the compaction is done the loop
//! **detaches at once** ([`ObjectStores::detach_now`]) instead of waiting for
//! the Db's own 1 h GC tick, and reads the fresh `external_dbs` on the bucket
//! for the writer (the depth gauge does not wait for the 1 h manifest poll).
//! One compaction at a time per node, deepest range first; the whole tick is
//! yielded while one of the node's ranges is L0-throttled; ranges with a
//! split/merge in flight are skipped. I/O per tick is bounded
//! ([`cairn_store::retry::ops::LINEAGE_COMPACT`]).
//!
//! It writes nothing beyond the Db's own compaction log and what SlateDB's
//! detach task already writes (the Db's own manifest, the final checkpoint on
//! the ancestor — G6, G29), and it changes no deletion condition: D-d is
//! still the only one deleting ancestors, with the same proof. Cost: rewriting
//! the whole range (≤ `max_range_bytes`) once per cold child; after that at
//! most [`DETACH_FOR`] detach runs (~5 requests) and one manifest read per
//! tick.
//!
//! Automatic splitting refuses a range standing on ≥ `max_auto_split_depth`
//! ancestors (`RangeOps::split_blocker`); a manual split still works, with a
//! warning.

use {
    crate::{autosplit::LoadBook, partitions::Partitions},
    cairn_store::{ClusterConfig, ObjectStores, PartitionId, StoreError},
    std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration},
    tokio::time::Instant,
};

/// Knobs of the standalone-child enforcer.
#[derive(Clone, Debug, PartialEq)]
pub struct LineageConfig {
    /// A range standing on this many ancestors or more gets a full compaction.
    /// `0` = the whole loop is off.
    pub depth: usize,
    /// …or when an ancestor it stands on has been retired longer than this.
    pub ancestor_age: Duration,
    /// Interval of the loop.
    pub every: Duration,
    /// A range already compacted that still stands on ancestors (detaching is
    /// the SlateDB GC's job, at a later GC tick) is not asked to compact again
    /// before this long.
    pub retry_after: Duration,
    /// Automatic splitting (`--auto-split on`) refuses a range standing on this
    /// many ancestors or more; a manual split still works, with a warning. `0`
    /// = no block.
    pub max_auto_split_depth: usize,
}

pub const DEFAULT_DEPTH: usize = 2;
pub const DEFAULT_ANCESTOR_AGE: Duration = Duration::from_secs(24 * 3600);
pub const DEFAULT_EVERY: Duration = Duration::from_secs(60);
pub const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(6 * 3600);
pub const DEFAULT_MAX_AUTO_SPLIT_DEPTH: usize = 3;

impl Default for LineageConfig {
    fn default() -> Self {
        Self {
            depth: DEFAULT_DEPTH,
            ancestor_age: DEFAULT_ANCESTOR_AGE,
            every: DEFAULT_EVERY,
            retry_after: DEFAULT_RETRY_AFTER,
            max_auto_split_depth: DEFAULT_MAX_AUTO_SPLIT_DEPTH,
        }
    }
}

impl LineageConfig {
    /// Whether the loop runs.
    pub fn enabled(&self) -> bool {
        self.depth > 0
    }

    /// Sets a `--lineage-…` command-line flag (full flag name).
    pub fn set(&mut self, flag: &str, value: &str) -> Result<(), String> {
        let num = |v: &str| -> Result<u64, String> {
            v.parse::<u64>()
                .map_err(|_| format!("{flag} {v:?} is not a non-negative integer"))
        };
        match flag {
            "--lineage-compact-depth" => self.depth = num(value)? as usize,
            "--lineage-compact-age-ms" => self.ancestor_age = Duration::from_millis(num(value)?),
            "--lineage-compact-every-ms" => self.every = Duration::from_millis(num(value)?.max(1)),
            "--lineage-compact-retry-ms" => self.retry_after = Duration::from_millis(num(value)?),
            "--lineage-auto-split-max-depth" => self.max_auto_split_depth = num(value)? as usize,
            other => return Err(format!("unknown argument: {other}")),
        }
        Ok(())
    }

    /// Whether flag `flag` belongs to this loop — `main` passes them to [`Self::set`].
    pub fn owns(flag: &str) -> bool {
        flag.starts_with("--lineage-")
    }

    /// Whether automatic splitting is blocked at depth `depth`.
    pub fn blocks_auto_split(&self, depth: usize) -> bool {
        self.max_auto_split_depth > 0 && depth >= self.max_auto_split_depth
    }

    /// Whether a range standing on `depth` ancestors (`old_ancestor`: one of
    /// them has been retired longer than `ancestor_age`) is due for a full
    /// compaction, and why. Depth is checked first: the loop only reads the
    /// ancestors' tombstones when the depth is not enough
    /// ([`Self::needs_ancestor_age`]).
    pub fn due(&self, depth: usize, old_ancestor: bool) -> Option<Why> {
        if depth == 0 || !self.enabled() {
            None
        } else if depth >= self.depth {
            Some(Why::Depth)
        } else if old_ancestor {
            Some(Why::AncestorAge)
        } else {
            None
        }
    }

    /// At depth `depth`, whether [`Self::due`] needs the ancestors' age.
    pub fn needs_ancestor_age(&self, depth: usize) -> bool {
        self.enabled() && depth > 0 && depth < self.depth
    }
}

/// Why the loop requested a full compaction of a range (the `reason` label of
/// `cairnflow_lineage_compactions_submitted_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// Stands on ≥ `depth` ancestors.
    Depth,
    /// Stands on an ancestor retired longer than `ancestor_age`.
    AncestorAge,
}

impl Why {
    pub fn label(self) -> &'static str {
        match self {
            Self::Depth => "depth",
            Self::AncestorAge => "ancestor_age",
        }
    }
}

/// A range due for compaction in one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Due {
    pub range: u32,
    pub depth: usize,
    pub why: Why,
}

/// The range compacted in this tick: deepest first, at equal depth the smaller id.
pub fn pick(due: &[Due]) -> Option<Due> {
    due.iter()
        .copied()
        .max_by_key(|d| (d.depth, std::cmp::Reverse(d.range)))
}

// ─────────────────────────────────────────── the loop

/// Longest wait for a submitted full compaction to finish; past it, give up
/// (the range goes into `retry_after`) so the loop does not get stuck on one
/// range forever.
pub const COMPACTION_WAIT: Duration = Duration::from_secs(2 * 3600);

/// Maximum number of owner records of not-yet-known ancestors read per tick.
const OWNER_READS_PER_TICK: usize = 8;

/// After a full compaction finishes, the loop tries to **detach at once**
/// ([`ObjectStores::detach_now`]) on every tick for this long — enough for the
/// compactor's checkpoint (10 minutes) pinning the old manifest to expire —
/// and then leaves it to the Db's own GC.
pub const DETACH_FOR: Duration = Duration::from_secs(30 * 60);

/// Maximum number of ranges per tick that get an immediate detach attempt, and
/// a fresh depth read before requesting a compaction (each time: one detach
/// run of ~5 requests, one manifest read).
const FRESH_READS_PER_TICK: usize = 2;

/// Result of one tick — for logs and tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tick {
    /// No range needs compacting.
    Idle,
    /// One of the node's ranges is L0-throttled: yield the whole tick.
    Throttled,
    /// Just requested a full compaction of this range.
    Submitted(Due),
    /// This range's compaction has not finished yet.
    Waiting { range: u32 },
    /// This range's compaction has finished — detaching is the SlateDB GC's job.
    Compacted { range: u32 },
    /// The compaction failed, ran over its wait limit, or the node stopped leading the range: give up.
    Dropped { range: u32, why: &'static str },
}

struct Work {
    range: u32,
    watch: cairn_store::CompactionWatch,
    since: Instant,
}

/// The standalone-child enforcer of **one** node. Only the node's background loop holds it.
pub struct Lineage {
    cfg: LineageConfig,
    stores: ObjectStores,
    cluster: ClusterConfig,
    book: LoadBook,
    handles: Partitions,
    working: Option<Work>,
    /// Ranges not reconsidered before this mark (just compacted, compaction failed, nothing to compact).
    rest_until: BTreeMap<u32, Instant>,
    /// Retirement time of ancestors that already carry a tombstone — a
    /// tombstone is permanent (D22), so it is read once per ancestor.
    retired_since: BTreeMap<u32, u64>,
    /// Ranges whose full compaction just finished, awaiting detach — try at once every tick until this mark.
    detaching: BTreeMap<u32, Instant>,
}

impl Lineage {
    pub fn new(
        cfg: LineageConfig,
        stores: ObjectStores,
        cluster: ClusterConfig,
        book: LoadBook,
        handles: Partitions,
    ) -> Self {
        Self {
            cfg,
            stores,
            cluster,
            book,
            handles,
            working: None,
            rest_until: BTreeMap::new(),
            retired_since: BTreeMap::new(),
            detaching: BTreeMap::new(),
        }
    }

    /// The background loop: one tick every `every`, each tick bounded.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(cairn_store::io_source::tagged(
            "lineage.compact",
            async move {
                let bound = self.cfg.every.max(Duration::from_secs(60));
                loop {
                    cairn_store::pace::beat(self.cfg.every).await;
                    let now_ms = cairn_store::entropy::now_ms();
                    match cairn_store::retry::deadline(
                        &cairn_store::retry::ops::LINEAGE_COMPACT,
                        bound,
                        self.tick(Instant::now(), now_ms),
                    )
                    .await
                    {
                        Ok(Ok(Tick::Idle | Tick::Throttled | Tick::Waiting { .. })) => {}
                        Ok(Ok(t)) => tracing::debug!(tick = ?t, "lineage: one pass"),
                        Ok(Err(e)) => {
                            tracing::warn!(error = %e, "lineage compaction pass failed, retrying on the next tick")
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "lineage compaction pass exceeded its limit")
                        }
                    }
                }
            },
        ))
    }

    /// One tick at `now` (monotonic clock) / `now_ms` (wall clock, for
    /// tombstone age).
    pub async fn tick(&mut self, now: Instant, now_ms: u64) -> Result<Tick, StoreError> {
        let rows = self.book.leading_writers();
        self.rest_until
            .retain(|r, until| *until > now && rows.iter().any(|(l, ..)| l == r));
        self.detach_pending(&rows, now).await;
        if let Some(w) = &mut self.working {
            let range = w.range;
            if !rows.iter().any(|(r, ..)| *r == range) {
                self.working = None;
                return Ok(Tick::Dropped {
                    range,
                    why: "no longer led here",
                });
            }
            return Ok(
                match self
                    .stores
                    .full_compaction_done(range, &mut w.watch)
                    .await?
                {
                    None if now.duration_since(w.since) < COMPACTION_WAIT => {
                        Tick::Waiting { range }
                    }
                    None => self.finish(range, now, "compaction did not finish in time"),
                    Some(false) => self.finish(range, now, "compaction failed"),
                    Some(true) => {
                        self.working = None;
                        self.rest_until.insert(range, now + self.cfg.retry_after);
                        self.detaching.insert(range, now + DETACH_FOR);
                        stats::COMPACTED.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(
                            range,
                            "lineage: full compaction finished; detaching the ancestors now"
                        );
                        Tick::Compacted { range }
                    }
                },
            );
        }
        // Low priority: if any of the node's ranges is L0-throttled, background compaction yields.
        if rows.iter().any(|(_, _, g)| g.reject_ratio() > 0.0) {
            return Ok(Tick::Throttled);
        }
        let table = self.cluster.table();
        let mut reads = OWNER_READS_PER_TICK;
        let mut fresh_reads = FRESH_READS_PER_TICK;
        let mut due = Vec::new();
        for (range, health, _) in &rows {
            if self.rest_until.contains_key(range) {
                continue;
            }
            // A range with a split/merge in flight: do not touch it (the source
            // has quiesced, the child is cloning) — next round.
            match table.range(PartitionId(*range)) {
                Some(e) if e.intent.is_none() => {}
                _ => continue,
            }
            let Some(mut paths) = health.external_paths() else {
                continue;
            };
            let mut why = self.due_for(&paths, now_ms, &mut reads).await;
            // The writer's in-RAM manifest only changes when the writer polls
            // the manifest (1 h): the RAM copy is an **upper** bound (a Db's
            // `external_dbs` only shrinks, so a range not due by the RAM copy
            // is not due by the fresh one either). Due by the RAM copy ⇒ read
            // the fresh copy (bounded per tick) and decide again; past the
            // bound, go by the RAM copy — this can only compact needlessly,
            // never miss one. A range that is not due costs no request.
            if why.is_some() && fresh_reads > 0 {
                fresh_reads -= 1;
                if let Ok(Some(fresh)) = self.stores.range_external_paths(PartitionId(*range)).await
                {
                    health.note_external_paths(fresh);
                    paths = health.external_paths().unwrap_or_default();
                    why = self.due_for(&paths, now_ms, &mut reads).await;
                }
            }
            if let Some(why) = why {
                due.push(Due {
                    range: *range,
                    depth: paths.len(),
                    why,
                });
            }
        }
        let Some(pick) = pick(&due) else {
            return Ok(Tick::Idle);
        };
        let Due { range, depth, why } = pick;
        match self.stores.submit_full_compaction(range).await? {
            cairn_store::FullCompaction::Requested(watch) => {
                // Without a wake-up the compaction sits `Submitted` until the
                // coordinator's next poll (1 h, G24). If it fails, it is only
                // slower.
                if let Some(h) = self.handles.get(PartitionId(range)) {
                    if let Err(e) = h.wake_compactor().await {
                        tracing::debug!(range, error = %e, "lineage: could not wake the compactor — compaction waits for its poll");
                    }
                }
                match why {
                    Why::Depth => &stats::SUBMITTED_DEPTH,
                    Why::AncestorAge => &stats::SUBMITTED_AGE,
                }
                .fetch_add(1, Ordering::Relaxed);
                tracing::info!(range, depth, reason = why.label(), compaction = %watch.id, "lineage: full compaction so the range stops standing on its ancestors");
                self.working = Some(Work {
                    range,
                    watch,
                    since: now,
                });
                Ok(Tick::Submitted(pick))
            }
            // Nothing to compact but the entry remains: wait for GC to detach. A snapshot
            // older than the flushed data is open (G32): try again later.
            cairn_store::FullCompaction::Nothing | cairn_store::FullCompaction::SnapshotOpen => {
                self.rest_until.insert(range, now + self.cfg.retry_after);
                Ok(Tick::Idle)
            }
        }
    }

    /// Tries to **detach at once** the ranges whose full compaction just
    /// finished: the Db's own SlateDB GC detaches at its next tick (1 h from
    /// opening the Db — measured depth back to 0 after ~55 minutes). At most
    /// [`FRESH_READS_PER_TICK`] ranges per tick: one
    /// [`ObjectStores::detach_now`] run, then re-reading `external_dbs` on the
    /// bucket for the writer ([`cairn_store::WriterHealth::note_external_paths`]:
    /// the depth gauge does not wait for the writer's 1 h manifest poll). No
    /// ancestors left, past [`DETACH_FOR`], or the node stopped leading ⇒ stop.
    /// Errors are only logged: the Db's GC still runs as before.
    async fn detach_pending(
        &mut self,
        rows: &[(
            u32,
            cairn_store::WriterHealth,
            std::sync::Arc<crate::l0::L0Gauge>,
        )],
        now: Instant,
    ) {
        self.detaching
            .retain(|r, until| *until > now && rows.iter().any(|(l, ..)| l == r));
        let ranges: Vec<u32> = self
            .detaching
            .keys()
            .copied()
            .take(FRESH_READS_PER_TICK)
            .collect();
        for range in ranges {
            cairn_store::failpoint!("lineage::before_detach");
            // The source of a split/merge in flight (the intent on the table,
            // **re-read right here**): it has quiesced or is about to, the
            // child is cloning its manifest — the manifest must stand still
            // (G5). The list of led ranges read at the start of the tick may
            // be stale. Table read fails or the range left the table ⇒ skip
            // this turn, reconsider next tick.
            match self.cluster.read_stored().await {
                Ok(Some(t))
                    if t.range(PartitionId(range))
                        .is_some_and(|e| e.intent.is_none()) => {}
                Ok(None)
                    if self
                        .cluster
                        .table()
                        .range(PartitionId(range))
                        .is_some_and(|e| e.intent.is_none()) => {}
                Ok(_) => {
                    tracing::debug!(range, "lineage: range carries a split/merge intent — not detaching while it is quiesced");
                    continue;
                }
                Err(e) => {
                    tracing::debug!(range, error = %e, "lineage: cannot read the range table before detach — next tick");
                    continue;
                }
            }
            if let Err(e) = self.stores.detach_now(range).await {
                tracing::debug!(range, error = %e, "lineage: detach right after compaction failed — retrying next tick");
                continue;
            }
            let fresh = match self.stores.range_external_paths(PartitionId(range)).await {
                Ok(Some(p)) => p,
                Ok(None) => Vec::new(),
                Err(e) => {
                    tracing::debug!(range, error = %e, "lineage: could not read the manifest after detach");
                    continue;
                }
            };
            if let Some((_, health, _)) = rows.iter().find(|(r, ..)| *r == range) {
                health.note_external_paths(fresh.clone());
            }
            if fresh.is_empty() {
                self.detaching.remove(&range);
                stats::DETACHED.fetch_add(1, Ordering::Relaxed);
                tracing::info!(range, "lineage: range no longer stands on any ancestor");
            }
        }
    }

    /// Whether the range standing on `paths` is due for a full compaction ([`LineageConfig::due`]).
    async fn due_for(&mut self, paths: &[String], now_ms: u64, reads: &mut usize) -> Option<Why> {
        let depth = paths.len();
        if depth == 0 {
            return None;
        }
        let old =
            self.cfg.needs_ancestor_age(depth) && self.old_ancestor(paths, now_ms, reads).await;
        self.cfg.due(depth, old)
    }

    fn finish(&mut self, range: u32, now: Instant, why: &'static str) -> Tick {
        self.working = None;
        self.rest_until.insert(range, now + self.cfg.every * 10);
        tracing::warn!(
            range,
            reason = why,
            "lineage: dropping a full compaction, retrying later"
        );
        Tick::Dropped { range, why }
    }

    /// Whether an ancestor of this cluster in `paths` has been retired longer
    /// than `ancestor_age`. Unread tombstones are read (at most `reads` per
    /// tick); a failed read ⇒ treated as not old yet (read again next time).
    async fn old_ancestor(&mut self, paths: &[String], now_ms: u64, reads: &mut usize) -> bool {
        let age = self.cfg.ancestor_age.as_millis() as u64;
        for path in paths {
            let Some(a) = self.stores.own_range_of(path) else {
                continue;
            };
            let since = match self.retired_since.get(&a.0) {
                Some(s) => Some(*s),
                None if *reads > 0 => {
                    *reads -= 1;
                    let main = self.stores.main();
                    match cairn_store::inspect::owner_observed(main.store(), main.prefix(), a).await
                    {
                        Ok(Some(o)) if o.record.retired => {
                            let s = cairn_store::RetiredRange {
                                range: a,
                                retired_ms: o.record.retired_ms,
                                modified_ms: o.modified_ms,
                            }
                            .since_ms();
                            self.retired_since.insert(a.0, s);
                            Some(s)
                        }
                        Ok(_) => None,
                        Err(e) => {
                            tracing::debug!(range = a.0, error = %e, "lineage: could not read an ancestor's owner record");
                            None
                        }
                    }
                }
                None => None,
            };
            if since.is_some_and(|s| now_ms.saturating_sub(s) >= age) {
                return true;
            }
        }
        false
    }
}

/// Process counters (`/metrics`: `cairnflow_lineage_compactions_total`).
pub mod stats {
    use std::sync::atomic::AtomicU64;
    /// Full compactions requested because of depth ([`super::Why::Depth`]).
    pub static SUBMITTED_DEPTH: AtomicU64 = AtomicU64::new(0);
    /// Full compactions requested because of an old ancestor ([`super::Why::AncestorAge`]).
    pub static SUBMITTED_AGE: AtomicU64 = AtomicU64::new(0);
    /// Full compactions finished.
    pub static COMPACTED: AtomicU64 = AtomicU64::new(0);
    /// Ranges that the immediate detach after a full compaction brought back to depth 0.
    pub static DETACHED: AtomicU64 = AtomicU64::new(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lineage_defaults_are_pinned() {
        let c = LineageConfig::default();
        assert_eq!(c.depth, 2, "default depth ≥ 2");
        assert_eq!(c.ancestor_age, Duration::from_secs(24 * 3600), "> 24 h");
        assert_eq!(c.max_auto_split_depth, 3);
        assert!(c.enabled());
        assert!(c.blocks_auto_split(3) && !c.blocks_auto_split(2));
    }

    /// The loop's decision functions ([`LineageConfig::due`], [`pick`]) at the
    /// defaults: depth ≥ 2 fires **because of depth** even when no ancestor
    /// is old (and without reading tombstones); depth 1 fires only when an
    /// ancestor is old; depth 0 or the loop off never fires.
    #[test]
    fn depth_two_is_due_for_depth_alone() {
        let c = LineageConfig::default();
        assert_eq!(c.due(2, false), Some(Why::Depth));
        assert_eq!(c.due(3, true), Some(Why::Depth), "depth before age");
        assert!(!c.needs_ancestor_age(2), "enough depth, no tombstone read");
        assert_eq!(c.due(1, false), None);
        assert!(c.needs_ancestor_age(1));
        assert_eq!(c.due(1, true), Some(Why::AncestorAge));
        assert_eq!(c.due(0, true), None);
        let off = LineageConfig {
            depth: 0,
            ..LineageConfig::default()
        };
        assert_eq!(off.due(5, true), None);
        assert!(!off.needs_ancestor_age(1));
    }

    #[test]
    fn pick_takes_the_deepest_then_the_lowest_id() {
        let d = |range, depth, why| Due { range, depth, why };
        assert_eq!(pick(&[]), None);
        assert_eq!(
            pick(&[
                d(7, 1, Why::AncestorAge),
                d(9, 2, Why::Depth),
                d(4, 2, Why::Depth),
            ]),
            Some(d(4, 2, Why::Depth))
        );
    }

    #[test]
    fn flags_set_every_knob() {
        let mut c = LineageConfig::default();
        for (f, v) in [
            ("--lineage-compact-depth", "3"),
            ("--lineage-compact-age-ms", "1000"),
            ("--lineage-compact-every-ms", "200"),
            ("--lineage-compact-retry-ms", "5000"),
            ("--lineage-auto-split-max-depth", "0"),
        ] {
            assert!(LineageConfig::owns(f));
            c.set(f, v).unwrap();
        }
        assert_eq!(
            c,
            LineageConfig {
                depth: 3,
                ancestor_age: Duration::from_secs(1),
                every: Duration::from_millis(200),
                retry_after: Duration::from_secs(5),
                max_auto_split_depth: 0,
            }
        );
        assert!(!c.blocks_auto_split(99));
        assert!(c.set("--lineage-compact-depth", "x").is_err());
        assert!(!LineageConfig::owns("--auto-split"));
    }
}
