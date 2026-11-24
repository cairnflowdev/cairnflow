// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Why an incarnation was dropped (fail-stop or session loss) — the baseline measurement
//! for fail-stop reliability (`cairnflow_failstop_total{cause}`).
//!
//! Every path that drops an incarnation goes through **one** place, `session::renew::lose`,
//! and that place counts exactly once per incarnation actually dropped (an incarnation
//! already lost earlier is not counted again). The cause is the **first** cause: the shared
//! WAL keeps the first reason (`nodewal::FailStop`), a later fail-stop of the same
//! incarnation does not change it.
//!
//! The counters are global per process (like `retry::ops`): a multi-node harness in one
//! process sums them together, a single-node binary gets exactly that node's counts.

use std::sync::atomic::{AtomicU64, Ordering};

/// Why the incarnation was dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cause {
    /// A shared-WAL log PUT hit a seal after an earlier attempt with an unknown outcome
    /// (error or `put_timeout` expired). On an `atomic_put` store this is only a real
    /// seal — our own write is read back and acked; an `atomic_put = false` store does not
    /// read back, so the label also covers our own write.
    WalAmbiguous,
    /// Seal on cairn's **first attempt**: the log is sealed. On an `atomic_put = false`
    /// store: also covers object_store's own 5xx retry layer having written first.
    WalFenced,
    /// The `stall_max` budget ran out while the PUT had not finished (hung, or fast errors
    /// in a row, including the `nodewal::stripe::put` failpoint), or an `AlreadyExists`
    /// whose read-back kept failing until the budget ran out (the "unknown" outcome of
    /// read-back resolution) — **and** the stripe could not rotate (normally only that
    /// stripe rotates and the node lives; `cairnflow_nodewal_stripe_rotations_total`).
    WalStall,
    /// The owner-record CAS was lost while claiming a range (`acquire_shared`, NW-5).
    OwnerCasLost,
    /// [`Cause::WalFenced`], [`Cause::WalAmbiguous`] or [`Cause::C1`] following a **dirty
    /// yield** by this same incarnation: a range stepped down while its owner record still
    /// pointed at the node's live log — the seal failed or exceeded its bound, or the clean
    /// owner-record CAS after the seal was lost ([`record_dirty_yield`]). The taker seals
    /// that log, or the node itself claims the range back and meets a segment pointing at
    /// its own log (C1). Previously these cases were mixed into the three labels above.
    DirtyYield,
    /// `wal_segment` points into this same incarnation's live log, which already has
    /// frames of the range (`NodeWal::take_over`, the C1 counterexample).
    C1,
    /// The renewal loop saw the session changed by someone else, marked as ended, or
    /// gone.
    SessionLost,
    /// No renewal arrived within ⅔·TTL, or a renewal returned after the deadline and the
    /// node demoted itself.
    SessionExpired,
    /// A path that cannot be classified: `NodeWal::fail_stop` from outside with no specific
    /// cause, or a death of the WAL stripe that fits none of the groups above. Must be 0
    /// in a soak; if non-zero, add a label.
    Other,
}

impl Cause {
    /// Every label, in the order printed at `/metrics` (all printed even when 0).
    pub const ALL: [Cause; 9] = [
        Cause::WalAmbiguous,
        Cause::WalFenced,
        Cause::WalStall,
        Cause::OwnerCasLost,
        Cause::DirtyYield,
        Cause::C1,
        Cause::SessionLost,
        Cause::SessionExpired,
        Cause::Other,
    ];

    /// The metric's `cause` label.
    pub fn label(self) -> &'static str {
        match self {
            Cause::WalAmbiguous => "wal_ambiguous",
            Cause::WalFenced => "wal_fenced",
            Cause::WalStall => "wal_stall",
            Cause::OwnerCasLost => "owner_cas_lost",
            Cause::DirtyYield => "dirty_yield",
            Cause::C1 => "c1",
            Cause::SessionLost => "session_lost",
            Cause::SessionExpired => "session_expired",
            Cause::Other => "other",
        }
    }

    fn index(self) -> usize {
        self as usize
    }

    /// Causes that an earlier dirty yield of the same incarnation can produce
    /// ([`Cause::DirtyYield`]): the live log sealed from outside, or a segment pointing at
    /// our own live log (C1).
    pub fn follows_dirty_yield(self) -> bool {
        matches!(self, Cause::WalFenced | Cause::WalAmbiguous | Cause::C1)
    }
}

impl std::fmt::Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

static COUNTS: [AtomicU64; Cause::ALL.len()] = [const { AtomicU64::new(0) }; Cause::ALL.len()];

/// Count one incarnation dropped because of `cause`. Only `session::renew::lose` calls it.
pub(crate) fn record(cause: Cause) {
    COUNTS[cause.index()].fetch_add(1, Ordering::Relaxed);
}

/// The number of incarnations dropped because of `cause` in this process.
pub fn count(cause: Cause) -> u64 {
    COUNTS[cause.index()].load(Ordering::Relaxed)
}

static DIRTY_YIELDS: AtomicU64 = AtomicU64::new(0);

/// Count one dirty yield (`cairnflow_dirty_yields_total`): a range stepped down while its
/// owner record may still point at the node's live shared-WAL log. Called from
/// `NodeWal::note_dirty_yield`.
pub(crate) fn record_dirty_yield() {
    DIRTY_YIELDS.fetch_add(1, Ordering::Relaxed);
}

/// The number of dirty yields in this process (all incarnations).
pub fn dirty_yields() -> u64 {
    DIRTY_YIELDS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_cause_has_a_distinct_label_and_slot() {
        let mut labels: Vec<_> = Cause::ALL.iter().map(|c| c.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), Cause::ALL.len());
        for (i, c) in Cause::ALL.iter().enumerate() {
            assert_eq!(c.index(), i, "{c} is not in its own slot");
        }
    }
}
