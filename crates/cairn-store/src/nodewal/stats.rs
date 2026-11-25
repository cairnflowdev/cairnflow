// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared WAL counters printed on `/metrics`. Global per process like [`crate::failstop`]:
//! a multi-node harness in one process adds them up together.

use std::sync::atomic::{AtomicU64, Ordering};

/// Outcome of re-reading an ambiguous log PUT (`AlreadyExists` on any attempt, on an
/// `atomic_put` store) — `cairnflow_nodewal_put_resolved_total{outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PutResolved {
    /// The object at `seq` has exactly the bytes we sent: an earlier send of ours (or an
    /// automatic retry by object_store) arrived ⇒ ack as a success.
    Ours,
    /// The object at `seq` is not ours — a sealer's seal (or an unknown object) ⇒
    /// `Fenced`, as before.
    Theirs,
    /// `AlreadyExists` but the GET sees nothing ⇒ retry the `Create`.
    Absent,
    /// The GET failed (or timed out) until the `stall_max` budget ran out — whose object it
    /// is remains unknown. No ack, and not treated as sealed.
    Unknown,
}

impl PutResolved {
    /// Every label, in print order (all printed, even when 0).
    pub const ALL: [PutResolved; 4] = [
        PutResolved::Ours,
        PutResolved::Theirs,
        PutResolved::Absent,
        PutResolved::Unknown,
    ];

    /// The metric's `outcome` label.
    pub fn label(self) -> &'static str {
        match self {
            PutResolved::Ours => "ours",
            PutResolved::Theirs => "theirs",
            PutResolved::Absent => "absent",
            PutResolved::Unknown => "unknown",
        }
    }
}

static RESOLVED: [AtomicU64; PutResolved::ALL.len()] =
    [const { AtomicU64::new(0) }; PutResolved::ALL.len()];

pub(crate) fn note_resolved(o: PutResolved) {
    RESOLVED[o as usize].fetch_add(1, Ordering::Relaxed);
}

/// Number of re-reads of an ambiguous log PUT with outcome `o` in this process.
pub fn put_resolved(o: PutResolved) -> u64 {
    RESOLVED[o as usize].load(Ordering::Relaxed)
}

/// Why a stream (stripe) rotated to a new id — `cairnflow_nodewal_stripe_rotations_total{cause}`.
/// Before streams could rotate, the first two causes were a fail-stop of the whole node
/// (`wal_stall`); before a dirty yield rotated the stream, the third cause was a fail-stop
/// of the whole node when the taker sealed the log (`wal_fenced`, `c1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RotationCause {
    /// The log PUT did not finish within `stall_max` (hung, repeated errors).
    Stall,
    /// `AlreadyExists` and the re-read failed until `stall_max` ran out
    /// ([`PutResolved::Unknown`]).
    Unknown,
    /// A range of the stream yielded dirty (seal failed or timed out): the stream rotates
    /// before the owner record keeps a segment pointing at its log.
    DirtyYield,
}

impl RotationCause {
    pub const ALL: [RotationCause; 3] = [
        RotationCause::Stall,
        RotationCause::Unknown,
        RotationCause::DirtyYield,
    ];

    pub fn label(self) -> &'static str {
        match self {
            RotationCause::Stall => "stall",
            RotationCause::Unknown => "unknown",
            RotationCause::DirtyYield => "dirty_yield",
        }
    }
}

static ROTATIONS: [AtomicU64; RotationCause::ALL.len()] =
    [const { AtomicU64::new(0) }; RotationCause::ALL.len()];

pub(crate) fn note_rotation(c: RotationCause) {
    ROTATIONS[c as usize].fetch_add(1, Ordering::Relaxed);
}

/// Number of times a stream of the node rotated because of `c` in this process.
pub fn stripe_rotations(c: RotationCause) -> u64 {
    ROTATIONS[c as usize].load(Ordering::Relaxed)
}

/// Outcome of one full dead-log sweep round — `cairnflow_nodewal_sweep_rounds_total{outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SweepOutcome {
    /// Nothing left that the next round could do right away.
    Done,
    /// More logs than one batch, or a read/seal/purge failed for some log: the next round
    /// runs within seconds.
    Again,
    /// The round failed as a whole (the LIST of `nodewal/`).
    Failed,
}

impl SweepOutcome {
    pub const ALL: [SweepOutcome; 3] = [
        SweepOutcome::Done,
        SweepOutcome::Again,
        SweepOutcome::Failed,
    ];

    pub fn label(self) -> &'static str {
        match self {
            SweepOutcome::Done => "done",
            SweepOutcome::Again => "again",
            SweepOutcome::Failed => "failed",
        }
    }
}

static SWEEPS: [AtomicU64; SweepOutcome::ALL.len()] =
    [const { AtomicU64::new(0) }; SweepOutcome::ALL.len()];

static PURGED: AtomicU64 = AtomicU64::new(0);

pub(crate) fn note_sweep(o: SweepOutcome) {
    SWEEPS[o as usize].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn note_purged() {
    PURGED.fetch_add(1, Ordering::Relaxed);
}

/// Number of full dead-log sweep rounds with outcome `o` in this process.
pub fn sweep_rounds(o: SweepOutcome) -> u64 {
    SWEEPS[o as usize].load(Ordering::Relaxed)
}

/// Number of dead or rotated shared WAL logs purged (moved to the trash, seal kept) by
/// this process, by either the full sweep or the hint round.
pub fn logs_purged() -> u64 {
    PURGED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outcome_has_a_distinct_label_and_slot() {
        for (i, o) in PutResolved::ALL.iter().enumerate() {
            assert_eq!(*o as usize, i, "{o:?}");
        }
        let mut labels: Vec<_> = PutResolved::ALL.iter().map(|o| o.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), PutResolved::ALL.len());
        for (i, c) in RotationCause::ALL.iter().enumerate() {
            assert_eq!(*c as usize, i, "{c:?}");
        }
        for (i, o) in SweepOutcome::ALL.iter().enumerate() {
            assert_eq!(*o as usize, i, "{o:?}");
        }
    }
}
