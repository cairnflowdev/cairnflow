// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The op ledger's transition rules.

use crate::{Advance, Op, Phase};

const ALL: [Phase; 7] = [
    Phase::Intent,
    Phase::Closed,
    Phase::Cloned,
    Phase::Prepared,
    Phase::Committed,
    Phase::Aborting,
    Phase::Cleaned,
];

#[test]
fn ledger_is_created_only_at_intent() {
    for p in ALL {
        let want = if p == Phase::Intent {
            Advance::Write
        } else {
            Advance::Refused
        };
        assert_eq!(Phase::advance(None, p), want, "{p:?}");
    }
}

#[test]
fn ledger_transitions() {
    use Phase::*;
    // (from, to, allowed)
    let allowed = [
        (Intent, Closed),
        (Intent, Cloned),
        (Closed, Cloned),
        (Cloned, Prepared),
        (Prepared, Committed),
        (Intent, Aborting),
        (Closed, Aborting),
        (Cloned, Aborting),
        (Prepared, Aborting),
        (Aborting, Cleaned),
    ];
    for from in ALL {
        for to in ALL {
            let got = Phase::advance(Some(from), to);
            if from == to {
                assert_eq!(got, Advance::Already, "{from:?} → {to:?}");
            } else if allowed.contains(&(from, to))
                // the forward branch only goes up — phases may be skipped (the roller-forward).
                || (matches!(from, Intent | Closed | Cloned | Prepared)
                    && matches!(to, Closed | Cloned | Prepared | Committed)
                    && (to as u8) > (from as u8))
            {
                assert_eq!(got, Advance::Write, "{from:?} → {to:?}");
            } else {
                assert_eq!(got, Advance::Refused, "{from:?} → {to:?}");
            }
        }
    }
    // Outcomes are not overwritten; a roll-back does not overwrite a committed preparation.
    assert_eq!(Phase::advance(Some(Committed), Aborting), Advance::Refused);
    assert_eq!(Phase::advance(Some(Cleaned), Prepared), Advance::Refused);
    assert_eq!(Phase::advance(Some(Aborting), Prepared), Advance::Refused);
    assert_eq!(Phase::advance(Some(Aborting), Committed), Advance::Refused);
}

#[test]
fn ledger_key_is_the_first_child() {
    let s = Op::Split {
        source: 4,
        left: 9,
        right: 10,
    };
    let m = Op::Merge {
        lo: 4,
        hi: 5,
        into: 9,
    };
    assert_eq!(s.key(), 9);
    assert_eq!(m.key(), 9);
    assert!(Op::keys_of_child(10).contains(9));
    assert!(Op::keys_of_child(9).contains(9));
    assert_eq!(s.children().iter().collect::<Vec<_>>(), [9, 10]);
    assert_eq!(m.sources().iter().collect::<Vec<_>>(), [4, 5]);
}
