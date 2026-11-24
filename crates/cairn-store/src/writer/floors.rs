// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Durable progress floors (Temporal's "ack level").
//!
//! The scan floors of seven indexes — four by due time (timer, retry, purge, delayed) and
//! three ordered (outbox, signal, inbound, [`SeqFloor`]) — live in the writer's RAM and
//! die with its term. With nothing else, the next writer (leader change, recovery) scans
//! again from 0, through the tombstones of every row already processed until compaction
//! removes them — exactly when the store is being throttled (rapid leader changes during
//! splits/merges).
//!
//! Here the floors are **written durably** as one key of the derived region
//! ([`keyspace::Index::Floors`]), **inside an existing commit**: a commit that sees the
//! floors differ from the written value adds that key to its own batch — no extra commit,
//! no extra PUT. The fence is the Db's `writer_epoch`, like every other write.
//!
//! **Invariant (FL-18):** a durable floor never passes an unprocessed index row — no row
//! still on the store has a key below the floor (by due time), no **live** row has a
//! `seq` below the floor (ordered). It holds thanks to:
//!
//! - **Lowered in the same commit.** A commit writing a row below the floor lowers the RAM
//!   floor **before** building the value to write, so the batch carrying that row also
//!   carries the lowered floor — atomically.
//! - **Advanced later, not together.** The RAM floor only advances in a scan, over rows
//!   that **earlier** commits deleted; the advanced value is written in a **later** commit.
//!   The WAL is sequential: when a later commit is durable every earlier commit is
//!   durable. So the durable floor never runs ahead of the deletion that advanced it.
//! - **The `writer_epoch` stamp.** The value is only used when the stamp belongs to the
//!   writer **right before** (`stamp + 1 = current epoch`). Any other writer in between —
//!   an old binary that does not know this key, a tool opening the Db to write — may have
//!   written rows below the floor without lowering it; a mismatching stamp ⇒ drop the
//!   floors, scan from 0 as before. The new writer stamps again in its first commit.
//!
//! Read leniently: absent, corrupt, unknown version or mismatching stamp ⇒ every floor 0.
//! The derived region is outside every projection, so a child Db after a split/merge does
//! not see this key; [`PartitionWriter::rebuild_derived`] resets the floors to 0.

use super::*;

/// The four due-time indexes with floors, by slot. The keys of all four start with `due`
/// (u64 big-endian) right after the index key head.
pub(super) const DUE_INDEXES: [Index; 4] =
    [Index::Timer, Index::Retry, Index::Purge, Index::Delayed];

/// The slot of a due-time index.
pub(super) fn due_slot(ix: Index) -> usize {
    DUE_INDEXES
        .iter()
        .position(|i| *i == ix)
        .expect("only four due-time indexes have floors")
}

/// `(slot, due)` if `key` is a row of a due-time index. Reads the first ten bytes.
pub(super) fn due_of_index_key(key: &[u8]) -> Option<(usize, u64)> {
    match key {
        [r, i, rest @ ..] if *r == keyspace::Region::Derived as u8 && rest.len() >= 8 => {
            let slot = DUE_INDEXES.iter().position(|ix| *ix as u8 == *i)?;
            Some((slot, u64::from_be_bytes(rest[..8].try_into().ok()?)))
        }
        _ => None,
    }
}

/// The floor values of the seven indexes — what is written durably.
///
/// - `due[slot]`: no index row (live or stale) has a due time below the floor;
///   `u64::MAX` = an empty index.
/// - `seq[slot]`: no **live** index row has a `seq` below the floor — that is,
///   `min(floor, smallest seq of below)` of [`SeqFloor`].
/// - `high`: the largest `seq` ever written into an ordered index of this Db (a lower
///   bound — see [`SEQ_HIGH`]). Not a scan floor: it only seeds the `seq` counter.
/// - `queue`: the largest `seq` of the queue index that the writer **knows for sure** —
///   no live entry has a `seq` above it; `None` = unknown (see [`QUEUE_HIGH`]). It only
///   seeds the queue `seq` counter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Floors {
    pub due: [u64; 4],
    pub seq: [u64; 3],
    pub high: u64,
    pub queue: Option<u64>,
}

/// Code of the `high` pair in the floor value — outside every index code ([`Index`]), so a
/// binary predating it reads the new value and skips this pair (unknown codes are
/// skipped), while this binary reading an old value gets `high` = 0.
///
/// **Why it exists:** the `seq` counter is seeded at open from the live rows and the
/// durable floors; the durable floors are dropped when the stamp is not from the writer
/// right before (a writer opened, then died before its first commit), and if every row has
/// been flushed the counter goes back to 1 — new rows land below the whole band of the old
/// rows' tombstones, the floor only advances up to the last live row, so **every** later
/// flush turn walks through that band again until compaction removes it. `high` is read
/// **regardless of the stamp**: it is a lower bound of the `seq`s issued (only grows), so
/// seeding the counter from it never issues a `seq` smaller than the old path issued — a
/// higher counter is always safe (`seq` only needs to be unique and increase in creation
/// order). The scan floors are untouched: FL-18 holds unchanged.
pub(super) const SEQ_HIGH: u8 = 0xF0;

/// Code of the `queue` pair in the floor value — like [`SEQ_HIGH`], outside every index
/// code: a binary predating it skips this pair when reading and **does not write it back**
/// (it only encodes the pairs it knows), so the pair being present means the writer that
/// wrote the value is a binary that knows the rule below.
///
/// **Why it exists.** The queue `seq` counter (`Engine::next_queue_seq`) must be above the
/// `seq` of every live entry — otherwise a new entry would be ordered before an older
/// entry of the same queue (FL-5). It used to be seeded from a scan of the whole queue
/// index on becoming leader, before the first command; that scan walked through the
/// tombstones of every entry ever drained (O(history)). Now the VO key rebuild runs in
/// chunks **while** the range serves creation commands, so the counter must exist before
/// that rebuild finishes.
///
/// **Rule.** The writer only writes this pair when it knows the value for sure: loaded
/// from durable floors **with a matching stamp** (the writer right before knew it for sure
/// and kept it through every commit), or after a rebuild of the derived region (every
/// entry was rewritten), or after the engine's key rebuild has gone all the way through.
/// Every commit writing a queue index row raises it before building the floor value of
/// that very batch (`Mutations::queue_high`). Unlike `high`, this pair is **not** read
/// when the stamp mismatches: a writer in between (old binary, a tool) may have written
/// entries above it.
pub(super) const QUEUE_HIGH: u8 = 0xF1;

/// Version of the value layout. An unknown version ⇒ read as absent.
const VERSION: u8 = 1;

impl Floors {
    /// `[1] ‖ epoch u64 ‖ n u8 ‖ n × (index code u8 ‖ floor u64)`, big-endian.
    pub(super) fn encode(&self, epoch: u64) -> Vec<u8> {
        let pairs: Vec<(Index, u64)> = DUE_INDEXES
            .iter()
            .copied()
            .zip(self.due)
            .chain(SeqFloor::INDEXES.iter().copied().zip(self.seq))
            .collect();
        let extra = 1 + usize::from(self.queue.is_some());
        let mut b = Vec::with_capacity(10 + (pairs.len() + extra) * 9);
        b.push(VERSION);
        b.extend_from_slice(&epoch.to_be_bytes());
        b.push((pairs.len() + extra) as u8);
        for (ix, f) in pairs {
            b.push(ix as u8);
            b.extend_from_slice(&f.to_be_bytes());
        }
        b.push(SEQ_HIGH);
        b.extend_from_slice(&self.high.to_be_bytes());
        if let Some(q) = self.queue {
            b.push(QUEUE_HIGH);
            b.extend_from_slice(&q.to_be_bytes());
        }
        b
    }

    /// Reads leniently. `epoch` is the `writer_epoch` of the writer **being opened**: the
    /// value is only usable when written by the writer right before. Unknown index codes
    /// are skipped; an index missing from the value has floor 0.
    pub(super) fn decode(raw: &[u8], epoch: u64) -> Option<Floors> {
        Self::decode_stamped(raw)
            .and_then(|(stamp, f)| (stamp.checked_add(1) == Some(epoch)).then_some(f))
    }

    /// `(stamp, value)` — the stamp is **not** checked. Only for reading `high` and the
    /// lower bound of issued `seq`s ([`SEQ_HIGH`]); scan floors always go through
    /// [`Self::decode`].
    pub(super) fn decode_stamped(raw: &[u8]) -> Option<(u64, Floors)> {
        let (&version, rest) = raw.split_first()?;
        if version != VERSION {
            return None;
        }
        let stamp = u64::from_be_bytes(rest.get(..8)?.try_into().ok()?);
        let (&n, mut rest) = rest.get(8..)?.split_first()?;
        let mut out = Floors::default();
        for _ in 0..n {
            let pair = rest.get(..9)?;
            rest = &rest[9..];
            let f = u64::from_be_bytes(pair[1..].try_into().ok()?);
            if let Some(slot) = DUE_INDEXES.iter().position(|ix| *ix as u8 == pair[0]) {
                out.due[slot] = f;
            } else if let Some(slot) = SeqFloor::INDEXES.iter().position(|ix| *ix as u8 == pair[0])
            {
                out.seq[slot] = f;
            } else if pair[0] == SEQ_HIGH {
                out.high = f;
            } else if pair[0] == QUEUE_HIGH {
                out.queue = Some(f);
            }
        }
        Some((stamp, out))
    }

    /// The lower bound of the largest issued `seq` that this value attests: `high`, and
    /// each ordered floor minus one (a floor advances to the `seq` of a seen row plus one).
    pub(super) fn seq_lower_bound(&self) -> u64 {
        self.seq
            .iter()
            .map(|f| f.saturating_sub(1))
            .fold(self.high, u64::max)
    }
}

impl<S: RangeStore> PartitionWriter<S> {
    /// The current floors of the seven indexes — the value a commit would write.
    pub(super) fn floors_now(&self) -> Floors {
        let seq = self.seq_floors.borrow();
        Floors {
            due: std::array::from_fn(|i| self.due_floors[i].get()),
            seq: std::array::from_fn(|i| {
                let f = &seq[i];
                f.below.first().map_or(f.floor, |(s, _)| (*s).min(f.floor))
            }),
            high: self.seq_high.get(),
            queue: self.queue_seq_high(),
        }
    }

    /// The largest `seq` of the queue index that the writer knows for sure
    /// ([`QUEUE_HIGH`]): `None` = not known yet — the engine's queue `seq` counter must
    /// wait for the key rebuild to go all the way through.
    pub fn queue_seq_high(&self) -> Option<u64> {
        self.queue_high.get().map(|h| h.max(self.queue_seen.get()))
    }

    /// From now on the writer knows for sure: no live entry of the queue index has a `seq`
    /// above `seq` (merged with every `seq` this writer's commits have written). Called
    /// after a pass over every entry — the engine's VO key rebuild.
    pub fn learn_queue_high(&self, seq: u64) {
        let known = self.queue_high.get().unwrap_or(0);
        self.queue_high
            .set(Some(seq.max(known).max(self.queue_seen.get())));
    }

    /// A commit is about to write queue index rows up to `seq`.
    pub(super) fn note_queue_seq(&self, seq: u64) {
        if seq > self.queue_seen.get() {
            self.queue_seen.set(seq);
        }
    }

    /// The RAM floor of a due-time index after a scan that started at `start`: `first` is
    /// the due time of the first row the scan met (live or stale), `None` = it met no row
    /// before `empty` (the due time the scan reached).
    ///
    /// Nothing can be written during the scan (the engine holds the writer); if something
    /// were, the floor would have been lowered meanwhile — keep the lower one.
    pub(super) fn settle_due_floor(&self, slot: usize, start: u64, first: Option<u64>, empty: u64) {
        let lowered = self.due_floors[slot].get();
        let new = first.unwrap_or(empty);
        self.due_floors[slot].set(if lowered < start {
            lowered.min(new)
        } else {
            new
        });
    }

    /// Loads the durable floors at open — only when the derived region matches the stamp
    /// (no rebuild), and before any scan or commit of this writer.
    pub(super) async fn load_floors(&mut self) -> Result<()> {
        let Some(raw) = self.get(&keyspace::floors()).await? else {
            return Ok(());
        };
        // Lower bound of issued `seq`s: usable **even with a mismatching stamp** ([`SEQ_HIGH`]).
        if let Some((_, any)) = Floors::decode_stamped(&raw) {
            self.note_seq_high(any.seq_lower_bound());
        }
        let Some(f) = Floors::decode(&raw, self.epoch) else {
            tracing::debug!(
                epoch = self.epoch,
                "durable progress floors unusable (stamp mismatch or corrupt) — scanning from the start"
            );
            return Ok(());
        };
        for (cell, v) in self.due_floors.iter().zip(f.due) {
            cell.set(v);
        }
        // Matching stamp: the writer right before knew the queue `seq` for sure ([`QUEUE_HIGH`]).
        if let Some(q) = f.queue {
            self.learn_queue_high(q);
        }
        let mut seq = self.seq_floors.borrow_mut();
        for (s, v) in seq.iter_mut().zip(f.seq) {
            *s = SeqFloor {
                floor: v,
                below: Default::default(),
            };
        }
        Ok(())
    }

    /// Loads the lower bound of issued `seq`s from the durable floor row **regardless of
    /// the stamp** — for a rebuild of the derived region (`rebuild_derived`), which does
    /// not load the floors. An absent row (a child Db after a split/merge: the derived
    /// region is hidden) ⇒ nothing.
    pub(super) async fn note_floors_high(&self) -> Result<()> {
        if let Some(raw) = self.get(&keyspace::floors()).await? {
            if let Some((_, any)) = Floors::decode_stamped(&raw) {
                self.note_seq_high(any.seq_lower_bound());
            }
        }
        Ok(())
    }

    /// The largest known `seq` of the three ordered indexes, only increasing.
    pub(super) fn note_seq_high(&self, seq: u64) {
        if seq > self.seq_high.get() {
            self.seq_high.set(seq);
        }
    }

    /// Floors back to 0 (a rebuild of the derived region reissues `seq`s and rewrites every
    /// index). The next commit writes floors 0 stamped by this writer. `high` stays: it is
    /// not a scan floor, and issued `seq`s never go back.
    pub(super) fn reset_floors(&self) {
        for cell in &self.due_floors {
            cell.set(0);
        }
        // The queue index is about to be rewritten: known for sure again after the rebuild.
        self.queue_high.set(None);
        // The timer cursor points into the old index: the next round restarts at the floor.
        self.timer_resume.take();
        *self.seq_floors.borrow_mut() = Default::default();
        self.floors_written.set(None);
    }

    /// The current floor of a due-time index.
    #[cfg(any(test, feature = "testkit"))]
    pub fn due_floor(&self, ix: Index) -> u64 {
        self.due_floors[due_slot(ix)].get()
    }

    /// The floor (the value that would be written durably) of an ordered index.
    #[cfg(any(test, feature = "testkit"))]
    pub fn seq_floor(&self, ix: Index) -> u64 {
        self.floors_now().seq[SeqFloor::slot(ix).expect("not an ordered index")]
    }

    /// The FL-18 invariant on the store **right now**: every index row below the floor (by
    /// due time: every row; ordered: live rows) — empty if the invariant holds. Scans each
    /// index from the start, so for tests only.
    #[cfg(any(test, feature = "testkit"))]
    pub async fn rows_below_floors(&self) -> Result<Vec<(Index, u64)>> {
        let floors = self.floors_now();
        let mut out = Vec::new();
        for (slot, ix) in DUE_INDEXES.iter().enumerate() {
            let (lo, hi) = keyspace::index_bounds(*ix);
            let mut s = self.scan_range(lo, hi, false).await?;
            while let Some(kv) = s.next().await? {
                match due_of_index_key(&kv.key) {
                    Some((_, due)) if due < floors.due[slot] => out.push((*ix, due)),
                    _ => break,
                }
            }
        }
        for (slot, ix) in SeqFloor::INDEXES.iter().enumerate() {
            let (lo, hi) = keyspace::index_bounds(*ix);
            let mut s = self.scan_range(lo, hi, false).await?;
            while let Some(kv) = s.next().await? {
                let Some((seq, _)) = keyspace::parse_ix_seq(&kv.key, *ix) else {
                    continue;
                };
                if seq >= floors.seq[slot] {
                    break;
                }
                if self.seq_row(*ix, &kv.key).await?.is_some() {
                    out.push((*ix, seq));
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_reads_back_exactly_and_loosely() {
        let f = Floors {
            due: [1, u64::MAX, 3, 0],
            seq: [7, 8, 9],
            high: 77,
            queue: None,
        };
        let raw = f.encode(41);
        assert_eq!(
            Floors::decode(&raw, 42),
            Some(f),
            "stamp of the writer right before"
        );
        assert_eq!(Floors::decode(&raw, 43), None, "a writer in between");
        assert_eq!(Floors::decode(&raw, 41), None, "stamp of this very epoch");
        assert_eq!(Floors::decode(&raw[..raw.len() - 1], 42), None, "truncated");
        assert_eq!(Floors::decode(&[], 42), None);
        let mut v2 = raw.clone();
        v2[0] = 2;
        assert_eq!(Floors::decode(&v2, 42), None, "unknown version");
        // Unknown index codes are skipped; a missing index is 0.
        let mut odd = vec![VERSION];
        odd.extend_from_slice(&41u64.to_be_bytes());
        odd.push(2);
        odd.push(0xEE);
        odd.extend_from_slice(&5u64.to_be_bytes());
        odd.push(Index::Retry as u8);
        odd.extend_from_slice(&6u64.to_be_bytes());
        assert_eq!(
            Floors::decode(&odd, 42),
            Some(Floors {
                due: [0, 6, 0, 0],
                seq: [0, 0, 0],
                high: 0,
                queue: None,
            })
        );
        // `high` is readable even with a mismatching stamp; the lower bound includes the
        // ordered floors.
        let (stamp, any) = Floors::decode_stamped(&raw).expect("valid value");
        assert_eq!((stamp, any.high, any.seq_lower_bound()), (41, 77, 77));
        let low = Floors {
            seq: [100, 0, 3],
            ..Floors::default()
        };
        assert_eq!(low.seq_lower_bound(), 99);
    }

    /// The `queue` pair ([`QUEUE_HIGH`]): read back exactly; absent (a value from a binary
    /// predating it, or a writer that does not know for sure) ⇒ `None`; mismatching stamp
    /// ⇒ not used (unlike `high`).
    #[test]
    fn queue_seq_floor_used_only_when_head_matches() {
        let f = Floors {
            seq: [7, 8, 9],
            high: 77,
            queue: Some(1234),
            ..Floors::default()
        };
        let raw = f.encode(41);
        assert_eq!(Floors::decode(&raw, 42), Some(f));
        assert_eq!(Floors::decode(&raw, 43), None, "a writer in between");
        let unknown = Floors { queue: None, ..f };
        let old = unknown.encode(41);
        assert_eq!(Floors::decode(&old, 42).map(|f| f.queue), Some(None));
        assert_eq!(old.len() + 9, raw.len(), "exactly one extra pair");
        // `queue` is not a lower bound of the ordered `seq`s.
        assert_eq!(
            Floors::decode_stamped(&raw).map(|(_, f)| f.seq_lower_bound()),
            Some(77)
        );
    }

    #[test]
    fn due_of_index_key_per_table() {
        assert_eq!(
            due_of_index_key(&keyspace::ix_timer(9, &[1; 24], 2)),
            Some((0, 9))
        );
        assert_eq!(
            due_of_index_key(&keyspace::ix_retry(10, &[1; 24])),
            Some((1, 10))
        );
        assert_eq!(
            due_of_index_key(&keyspace::ix_purge(11, &[1; 24])),
            Some((2, 11))
        );
        assert_eq!(
            due_of_index_key(&keyspace::ix_delayed(12, &[1; 24], 3)),
            Some((3, 12))
        );
        assert_eq!(due_of_index_key(&keyspace::ix_invocation(&[1; 24])), None);
        assert_eq!(due_of_index_key(&keyspace::floors()), None);
    }
}
