// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

use super::*;

/// A range's engine answering "not leader" / "stopped" / "wrong range" in the
/// middle of a split/merge (the other node just died, the range just changed owner)
/// is **transient** — `503`, the caller retries — not `500`.
#[test]
fn leadership_errors_are_retryable() {
    for e in [
        ActorError::NotLeader,
        ActorError::Stopped,
        ActorError::WrongRange,
    ] {
        let shown = e.to_string();
        assert!(
            matches!(RangeOpError::from(e), RangeOpError::Busy(_)),
            "{shown} must be Busy (503)"
        );
    }
}

/// A transient store I/O error, or a lease lost / a fence hit in the middle of a
/// split/merge (reading the owner record, taking a source while gathering, the table
/// CAS; the source engine changing owner during flush or quiesce) is **a legitimate
/// race or a flaky store**: the coordinator has rolled back, `503`, not `500`. A truly
/// abnormal error (a keyspace violation, a corrupt record) is still `500`.
#[test]
fn store_and_lease_errors_are_retryable() {
    let transient = || {
        [
            StoreError::Io("503 SlowDown".into()),
            StoreError::LeaseIo("PUT timed out".into()),
            StoreError::LeaseLost,
            StoreError::Fenced,
            StoreError::LeaseUnavailable { partition: 1 },
            StoreError::Closed,
        ]
    };
    for e in transient() {
        let shown = e.to_string();
        assert!(
            matches!(RangeOpError::from(e), RangeOpError::Busy(_)),
            "{shown} must be Busy (503)"
        );
    }
    for e in transient() {
        let e = ActorError::from(e);
        let shown = e.to_string();
        assert!(
            matches!(RangeOpError::from(e), RangeOpError::Busy(_)),
            "{shown} (through the engine) must be Busy (503)"
        );
    }
    let e = StoreError::RangeRetired { range: 1 };
    assert!(
        matches!(RangeOpError::from(e), RangeOpError::Conflict(_)),
        "range just retired ⇒ the table changed (409)"
    );
    for e in [
        StoreError::Corrupt("key outside the region".into()),
        StoreError::LeaseCorrupt { reason: "x".into() },
    ] {
        let shown = e.to_string();
        assert!(
            matches!(RangeOpError::from(e), RangeOpError::Failed(_)),
            "{shown} must still be Failed (500)"
        );
    }
}

/// **A failed `hold` does not remove the current holder's mark.** The
/// interleaving:
/// 1. Operation X (e.g. a manual merge of 0 + 1) holds {0, 1} and wins the intent CAS.
/// 2. Operation Y (e.g. an auto-split of 0) also `hold`s 0 on the same node, and loses.
/// 3. Y drops its guard.
///
/// Must see: after (3), 0 and 1 are still in the set — the node's `factory` does not
/// roll back X's intent. When the set was a bare set, (2) succeeded and (3) removed X's 0.
/// `hold` refuses a range that already has a holder, so each range has at most one
/// guard: the set is a counter with limit 1, no reference counting needed.
#[test]
fn failed_hold_never_releases_the_holders_ranges() {
    let set = ActiveIntents::default();
    let x = set
        .hold(&[PartitionId(0), PartitionId(1)])
        .expect("X can hold {0, 1}");
    for overlap in [&[PartitionId(0)][..], &[PartitionId(1), PartitionId(2)][..]] {
        let y = set.hold(overlap);
        assert!(y.is_none(), "Y holds {overlap:?} while X is holding");
        drop(y);
    }
    assert!(
        set.contains(PartitionId(0)) && set.contains(PartitionId(1)),
        "the failed hold removed X's mark"
    );
    assert!(
        !set.contains(PartitionId(2)),
        "the failed hold left its own mark behind"
    );
    let z = set.hold(&[PartitionId(2)]).expect("nobody holds range 2");
    drop(x);
    assert!(!set.contains(PartitionId(0)) && !set.contains(PartitionId(1)));
    assert!(set.contains(PartitionId(2)), "X's drop removed Z's mark");
    drop(z);
    assert!(!set.contains(PartitionId(2)));
}

/// The source range's seal timed out during quiesce (a slow store — the `overlap`
/// chaos run, PUTs under `ranges/` delayed by 1 s, saw `500 "cannot
/// close the range Db: seal exceeded 5s"`). The parent Db has not been cloned (SM-4) and
/// the intent has been rolled back, so this is **transient**: `503`, not `500` (SM-7).
#[test]
fn seal_failure_during_quiesce_is_retryable() {
    for e in [
        ActorError::SealFailed("seal exceeded 5s".into()),
        ActorError::SealFailed("a task still holds the engine".into()),
    ] {
        let shown = e.to_string();
        assert!(
            matches!(RangeOpError::from(e), RangeOpError::Busy(_)),
            "{shown} must be Busy (503)"
        );
    }
}
