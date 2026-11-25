// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! SlateDB measurements for one range Db.
//!
//! SlateDB already has `l0_stall_count`, `backpressure_count` and `l0_sst_count` (the fork's
//! `db_stats.rs`), but only pushes them through a `MetricsRecorder`. The default is a no-op
//! recorder, so before this module cairn saw none of them.
//!
//! [`DbStats`] is a recorder that keeps exactly those three numbers (atomic) and returns a
//! no-op handle for every other one. Not `DefaultMetricsRecorder`: it keeps **every** number of
//! the Db (a few dozen atomics behind a `Mutex<Vec>`) for each range, while cairn reads only
//! three.
//!
//! One recorder per Db open: SlateDB registers handles when the Db is built, so the numbers
//! start at 0 in each leadership term.

use {
    slatedb_common::metrics::{
        CounterFn, GaugeFn, HistogramFn, MetricsRecorder, NoopMetricsRecorder, UpDownCounterFn,
    },
    std::sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
};

/// SlateDB metric names cairn reads — copies of the constants in `slatedb::db_stats` (that
/// module is not public).
const BACKPRESSURE_COUNT: &str = "slatedb.db.backpressure_count";
const L0_STALL_COUNT: &str = "slatedb.db.l0_stall_count";
const L0_SST_COUNT: &str = "slatedb.db.l0_sst_count";
const SORTED_RUN_COUNT: &str = "slatedb.db.sorted_run_count";
const TOTAL_MEM_SIZE_BYTES: &str = "slatedb.db.total_mem_size_bytes";
/// Accesses to the Db's (RAM) block cache, labels `entry_kind` and `result`
/// (`slatedb::db_cache::stats`) — cairn reads the `data_block` part.
const DB_CACHE_ACCESS: &str = "slatedb.db_cache.access_count";

#[derive(Default)]
struct Counter(AtomicU64);

impl CounterFn for Counter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Gauge(AtomicI64);

impl GaugeFn for Gauge {
    fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

/// The recorder of one range Db. `Clone` through `Arc`; read with [`Self::snapshot`].
#[derive(Default)]
pub struct DbStats {
    backpressure: Arc<Counter>,
    /// Both kinds (`num_ssts`, `num_ssts_per_key`) summed together: the operator asks "is the
    /// flusher stalled on L0", not which cap it stalled on.
    l0_stalls: Arc<Counter>,
    l0_ssts: Arc<Gauge>,
    sorted_runs: Arc<Gauge>,
    mem_bytes: Arc<Gauge>,
    block_hits: Arc<Counter>,
    block_misses: Arc<Counter>,
    /// Scan floors of the three ordered indexes (outbox, signal, inbound) — written by the
    /// writer, not by SlateDB. See [`SeqSlotSnapshot`].
    seq: [SeqSlot; 3],
    /// Time the range's engine is held, per holder ([`HOLDERS`]), µs — written by the actor
    /// (`Lease`), not by SlateDB.
    hold_us: [AtomicU64; HOLDERS.len()],
    /// The **longest** engine hold per holder, µs, since the Db was opened (to watch the bound
    /// on each single hold).
    hold_max_us: [AtomicU64; HOLDERS.len()],
    /// Number of engine holds per holder (how many times per second the engine is taken from
    /// the permit queue).
    holds: [AtomicU64; HOLDERS.len()],
    noop: NoopMetricsRecorder,
}

/// Who holds a range's engine, by task (the `holder` label of
/// `cairnflow_range_engine_hold_ms_total`). The index is the position in the array.
pub const HOLDERS: [&str; 11] = [
    "turn",
    "tick",
    "drain",
    "query_build",
    "hint_sweep",
    "recover",
    "rescue",
    "list",
    "command",
    // `restore_flow` on becoming leader — in chunks, inside the serve loop (before that it was
    // counted under `other`).
    "restore",
    "other",
];

/// Index of label `name` in [`HOLDERS`] (`other` if unknown).
pub const fn holder(name: &str) -> usize {
    let b = name.as_bytes();
    let mut i = 0;
    while i < HOLDERS.len() {
        let h = HOLDERS[i].as_bytes();
        if h.len() == b.len() {
            let mut j = 0;
            while j < h.len() && h[j] == b[j] {
                j += 1;
            }
            if j == h.len() {
                return i;
            }
        }
        i += 1;
    }
    HOLDERS.len() - 1
}

#[derive(Default)]
struct SeqSlot {
    floor: AtomicU64,
    below: AtomicU64,
    head: AtomicU64,
    scans: AtomicU64,
    frozen: AtomicU64,
}

/// Scan floor of one ordered index in the current writer term (`writer::scan::SeqFloor`). The
/// drainer opens each turn from `seq` 0 and relies on the floor to avoid walking over the
/// tombstones of drained rows: a large `head − floor` that stands still means every drain turn
/// walks over that many deleted rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeqSlotSnapshot {
    /// The in-RAM floor: open scans start here.
    pub floor: u64,
    /// Number of rows that may still be live below the floor (`below`).
    pub below: u64,
    /// The largest `seq` any scan of this term has seen.
    pub head: u64,
    /// Number of open scans.
    pub scans: u64,
    /// Scans that started at the floor and walked the whole span because `below` hit its cap
    /// and the floor did not advance.
    pub frozen: u64,
}

/// Snapshot of a Db's numbers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbStatsSnapshot {
    /// Times `db.write` had to wait because unflushed data exceeded `max_unflushed_bytes`.
    pub backpressure: u64,
    /// Times the flusher refused to upload a memtable because L0 was at its cap.
    pub l0_stalls: u64,
    /// Number of L0 SSTs according to SlateDB (gauge, updated on flush/compaction).
    pub l0_ssts: u64,
    /// Number of sorted runs below L0 (gauge, updated when the manifest is written). Many
    /// overlapping runs are read amplification: every point read/scan goes through every run.
    pub sorted_runs: u64,
    /// Memtable bytes (mutable + immutable) not yet flushed to L0.
    pub mem_bytes: u64,
    /// Data block reads that hit / miss the RAM block cache (node-wide) — a miss is a read down
    /// to the store (or the disk cache).
    pub block_hits: u64,
    pub block_misses: u64,
    /// In the order outbox, signal, inbound.
    pub seq: [SeqSlotSnapshot; 3],
    /// µs of engine hold per holder, in [`HOLDERS`] order.
    pub hold_us: [u64; HOLDERS.len()],
    /// µs of the longest hold per holder, in [`HOLDERS`] order.
    pub hold_max_us: [u64; HOLDERS.len()],
    /// Number of holds per holder, in [`HOLDERS`] order.
    pub holds: [u64; HOLDERS.len()],
}

impl DbStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> DbStatsSnapshot {
        DbStatsSnapshot {
            backpressure: self.backpressure.0.load(Ordering::Relaxed),
            l0_stalls: self.l0_stalls.0.load(Ordering::Relaxed),
            l0_ssts: self.l0_ssts.0.load(Ordering::Relaxed).max(0) as u64,
            sorted_runs: self.sorted_runs.0.load(Ordering::Relaxed).max(0) as u64,
            mem_bytes: self.mem_bytes.0.load(Ordering::Relaxed).max(0) as u64,
            block_hits: self.block_hits.0.load(Ordering::Relaxed),
            block_misses: self.block_misses.0.load(Ordering::Relaxed),
            seq: std::array::from_fn(|i| {
                let q = &self.seq[i];
                SeqSlotSnapshot {
                    floor: q.floor.load(Ordering::Relaxed),
                    below: q.below.load(Ordering::Relaxed),
                    head: q.head.load(Ordering::Relaxed),
                    scans: q.scans.load(Ordering::Relaxed),
                    frozen: q.frozen.load(Ordering::Relaxed),
                }
            }),
            hold_us: std::array::from_fn(|i| self.hold_us[i].load(Ordering::Relaxed)),
            hold_max_us: std::array::from_fn(|i| self.hold_max_us[i].load(Ordering::Relaxed)),
            holds: std::array::from_fn(|i| self.holds[i].load(Ordering::Relaxed)),
        }
    }

    /// One engine hold of `us` by holder `holder` ([`HOLDERS`]).
    pub fn note_hold(&self, holder: usize, us: u64) {
        if let Some(c) = self.hold_us.get(holder) {
            c.fetch_add(us, Ordering::Relaxed);
        }
        if let Some(m) = self.hold_max_us.get(holder) {
            m.fetch_max(us, Ordering::Relaxed);
        }
        if let Some(n) = self.holds.get(holder) {
            n.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// An open scan of the ordered index in slot `slot` has just finished: the floor and `below`
    /// after it, the largest `seq` seen, and whether the floor got stuck on the `below` cap.
    pub fn note_seq_scan(
        &self,
        slot: usize,
        floor: u64,
        below: usize,
        seen: Option<u64>,
        frozen: bool,
    ) {
        let Some(q) = self.seq.get(slot) else {
            return;
        };
        q.floor.store(floor, Ordering::Relaxed);
        q.below.store(below as u64, Ordering::Relaxed);
        if let Some(h) = seen {
            q.head.fetch_max(h, Ordering::Relaxed);
        }
        q.scans.fetch_add(1, Ordering::Relaxed);
        if frozen {
            q.frozen.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl MetricsRecorder for DbStats {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        match name {
            BACKPRESSURE_COUNT => self.backpressure.clone(),
            L0_STALL_COUNT => self.l0_stalls.clone(),
            DB_CACHE_ACCESS if labels.contains(&("entry_kind", "data_block")) => {
                if labels.contains(&("result", "hit")) {
                    self.block_hits.clone()
                } else {
                    self.block_misses.clone()
                }
            }
            _ => self.noop.register_counter(name, description, labels),
        }
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        match name {
            L0_SST_COUNT => self.l0_ssts.clone(),
            SORTED_RUN_COUNT => self.sorted_runs.clone(),
            TOTAL_MEM_SIZE_BYTES => self.mem_bytes.clone(),
            _ => self.noop.register_gauge(name, description, labels),
        }
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        self.noop
            .register_up_down_counter(name, description, labels)
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        self.noop
            .register_histogram(name, description, labels, boundaries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The constant names here must match SlateDB's — a mismatch is a metric silently stuck at 0.
    #[test]
    fn names_match_slatedb() {
        let s = DbStats::new();
        s.register_counter(BACKPRESSURE_COUNT, "", &[]).increment(2);
        s.register_counter(L0_STALL_COUNT, "", &[("type", "num_ssts")])
            .increment(1);
        s.register_counter(L0_STALL_COUNT, "", &[("type", "num_ssts_per_key")])
            .increment(1);
        s.register_gauge(L0_SST_COUNT, "", &[]).set(7);
        s.register_gauge(SORTED_RUN_COUNT, "", &[]).set(4);
        s.register_gauge(TOTAL_MEM_SIZE_BYTES, "", &[]).set(1 << 20);
        s.register_counter("slatedb.db.write_ops", "", &[])
            .increment(100);
        let block = |r| [("entry_kind", "data_block"), ("result", r)];
        s.register_counter(DB_CACHE_ACCESS, "", &block("hit"))
            .increment(5);
        s.register_counter(DB_CACHE_ACCESS, "", &block("miss"))
            .increment(3);
        s.register_counter(
            DB_CACHE_ACCESS,
            "",
            &[("entry_kind", "index"), ("result", "miss")],
        )
        .increment(9);
        assert_eq!(
            s.snapshot(),
            DbStatsSnapshot {
                backpressure: 2,
                l0_stalls: 2,
                l0_ssts: 7,
                sorted_runs: 4,
                mem_bytes: 1 << 20,
                block_hits: 5,
                block_misses: 3,
                seq: Default::default(),
                hold_us: Default::default(),
                hold_max_us: Default::default(),
                holds: Default::default(),
            }
        );
        // The real anchor: the fork's names and labels.
        let src = include_str!("../../../vendor/slatedb/slatedb/src/db_cache/mod.rs");
        assert!(src.contains("dbcache_stat_name!(\"access_count\")"));
        assert!(src.contains("(\"entry_kind\", \"data_block\"), (\"result\", \"hit\")"));
        let stats = include_str!("../../../vendor/slatedb/slatedb/src/db_stats.rs");
        assert!(stats.contains("SORTED_RUN_COUNT: &str = db_stat_name!(\"sorted_run_count\")"));
        assert!(
            stats.contains("TOTAL_MEM_SIZE_BYTES: &str = db_stat_name!(\"total_mem_size_bytes\")")
        );
    }
}

#[cfg(test)]
mod hold_tests {
    use super::*;

    #[test]
    fn holder_label_maps_to_index_and_unknown_to_other() {
        for (i, h) in HOLDERS.iter().enumerate() {
            assert_eq!(holder(h), i);
        }
        assert_eq!(holder("no such holder"), HOLDERS.len() - 1);
        let s = DbStats::new();
        s.note_hold(holder("tick"), 1_000);
        s.note_hold(holder("tick"), 500);
        s.note_hold(99, 7);
        assert_eq!(s.snapshot().hold_us[holder("tick")], 1_500);
        assert_eq!(s.snapshot().hold_max_us[holder("tick")], 1_000);
        assert_eq!(s.snapshot().holds[holder("tick")], 2);
    }
}
