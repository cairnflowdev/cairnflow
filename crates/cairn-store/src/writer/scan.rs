// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Scans: seq floors, scan counters, `Hinted`, `RebuildStats`, `Scan`.

use super::*;

/// The scan floor of an ordered index (outbox, signal, inbound) within **one term** of
/// the writer — so the flusher does not walk again over rows it already flushed.
///
/// The flusher opens each turn with a read from `seq` 0. Flushed rows are deleted, and a
/// deletion in an LSM is a tombstone that stays on the scan path until compaction removes
/// it; reading from 0 therefore costs O(rows ever flushed), not O(rows still pending) —
/// after a few tens of thousands of cross-range calls, each empty flush turn walked over
/// that many tombstones (CPU per call grew 4–8×).
///
/// **Invariant:** every **live** index row with `seq < floor` is in `below`. The converse
/// is not needed — `below` may keep dead rows, dropped when read. It holds thanks to two
/// places, both in the writer:
///
/// - `floor` only advances over a stretch that a scan **has just walked through** — every
///   live row in that stretch was just seen, and added to `below`. A scan and a `commit`
///   cannot interleave: the scan borrows `&self`, commit borrows `&mut self`.
/// - [`PartitionWriter::commit`] adds to `below` **every** index row written with
///   `seq < floor` — including a row whose `seq` was issued earlier but committed late,
///   and an outbox row rewritten with its old `seq` (D-O2). Nothing is assumed about the
///   order of issuing `seq` relative to the order of commits.
///
/// Lives in the writer's RAM; the value `min(floor, below)` is written durably with the
/// commit (`floors.rs`) and the **next** writer of the same Db opens with `floor` = that
/// value, `below` empty — the invariant holds because no live row is below it. A child
/// range after a split/merge, a just-rebuilt Db, or a writer whose durable floor mark does
/// not come from the writer right before it, starts from `floor = 0` and one full scan. A
/// writer does not live across two terms — a fenced Db can no longer be written — so no
/// row written by another node slips under the floor.
#[derive(Default)]
pub(super) struct SeqFloor {
    /// An open scan starts from here, not from the `seq` the caller asked for.
    pub(super) floor: u64,
    /// `(seq, index key)` of the rows **that may still be live** below `floor`.
    pub(super) below: std::collections::BTreeSet<(u64, bytes::Bytes)>,
}

impl SeqFloor {
    /// The three indexes with floors, by slot.
    pub(super) const INDEXES: [Index; 3] = [Index::Outbox, Index::Signal, Index::Inbound];

    /// Cap on the size of `below`. Past the cap the floor stops advancing (reads go on as
    /// before, from the current floor): thousands of rows stuck on a dead destination are
    /// worth one scan more than thousands of point reads per turn.
    pub(super) const BELOW_CAP: usize = 4096;

    pub(super) fn slot(ix: Index) -> Option<usize> {
        Self::INDEXES.iter().position(|i| *i == ix)
    }

    /// The key's slot if it is a row of an ordered index. Reads the first two bytes.
    pub(super) fn slot_of(key: &[u8]) -> Option<usize> {
        match key {
            [r, i, ..] if *r == keyspace::Region::Derived as u8 => {
                Self::INDEXES.iter().position(|ix| *ix as u8 == *i)
            }
            _ => None,
        }
    }
}

/// Result of [`PartitionWriter::seq_heads_unbounded`].
#[derive(Debug, Default)]
pub struct SeqHeads {
    /// Outbox rows still pending: `(seq, owner, (invocation, completion))`.
    pub outbox: Vec<(
        u64,
        crate::partition_table::PartitionKey,
        (InvocationId, u32),
    )>,
    /// The largest `seq` of outbox, signal, inbound — not below any floor of the three
    /// indexes but one; `0` if all three are empty and there is no floor yet.
    pub max: u64,
}

/// Number of keys the writer's scans have read, by keyspace **region**.
///
/// It exists so tests can say "this path does not scan region X" — `recover` only reads
/// the derived region, a rebuild does not read the bulk region. It counts keys rather than
/// requests: requests depend on the memtable, the cache and the SST layout, keys do not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanCounts {
    pub hot: u64,
    pub derived: u64,
    pub bulk: u64,
}

impl ScanCounts {
    pub fn since(&self, earlier: &ScanCounts) -> ScanCounts {
        ScanCounts {
            hot: self.hot - earlier.hot,
            derived: self.derived - earlier.derived,
            bulk: self.bulk - earlier.bulk,
        }
    }
}

/// A batch of [`PartitionWriter::retry_rows_between`]: retry rows
/// `(due, id, value part after the due time)` and the cursor to continue scanning
/// (`None` = done).
#[derive(Debug, Default)]
pub struct RetryBatch {
    pub rows: Vec<(u64, InvocationId, bytes::Bytes)>,
    pub next: Option<(u64, InvocationId)>,
}

/// Result of a read through an index: the rows still valid, and the index rows that are
/// **stale** (pointing at an entity row that no longer exists or does not match) for the
/// call site to clean with [`Mutations::forget_index_row`].
///
/// Index rows are hints: users always check the entity row. The read does not clean up by
/// itself because it has no write right — the writer only writes through `commit`.
#[derive(Debug)]
pub struct Hinted<T> {
    pub rows: Vec<T>,
    pub stale: Vec<Vec<u8>>,
    /// Filled only by scans **by due time** (timer, retry, delayed): the due time of the
    /// first index row **not** returned — the first row not yet due, or the first row past
    /// `limit`. `None` = no row left.
    ///
    /// Lets the background loop sleep until exactly that due time instead of until the
    /// next tick. Reads exactly one extra key, on the same blocks the scan just read — no
    /// extra seek. A hint like every index row: a stale row only wakes the background loop
    /// early once.
    pub next_due: Option<u64>,
}

impl<T> Default for Hinted<T> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            stale: Vec::new(),
            next_due: None,
        }
    }
}

/// Result of one [`PartitionWriter::rebuild_derived`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RebuildStats {
    /// Old index rows deleted.
    pub removed: usize,
    /// Hot-region rows scanned.
    pub scanned: usize,
    /// Rows written (new index rows, and entity rows that got a new `seq`).
    pub written: usize,
    /// Keys read by **every** scan of the rebuild, by region — `bulk` must be 0 (a rebuild
    /// does not read the bulk region).
    pub regions: ScanCounts,
    /// Bytes (key + value) of the hot-region scan.
    pub bytes: u64,
    /// Duration of the whole rebuild, from deleting the marker to writing the new one.
    pub elapsed: std::time::Duration,
}

/// A scan in progress, counting keys by region into the writer's counter.
pub(super) struct Scan<'a, I> {
    pub(super) it: I,
    pub(super) counts: &'a std::cell::Cell<ScanCounts>,
}

impl<I: RangeIter> Scan<'_, I> {
    pub(super) async fn next(&mut self) -> Result<Option<Row>> {
        let kv = self.it.next().await?;
        if let Some(kv) = &kv {
            let mut c = self.counts.get();
            match keyspace::Region::of_key(&kv.key) {
                Some(keyspace::Region::Hot) => c.hot += 1,
                Some(keyspace::Region::Derived) => c.derived += 1,
                Some(keyspace::Region::Bulk) => c.bulk += 1,
                None => {}
            }
            self.counts.set(c);
        }
        Ok(kv)
    }
}
