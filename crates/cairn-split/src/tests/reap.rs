// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! D-d: the plan and the proof right before deleting.

use super::{check, row};
use crate::{Event, Gate, Keep, Need, Pins, Plan, ReadErr, Refs, Retired, View};

const RETIRED: Retired = Retired {
    in_table: false,
    promised: false,
    tomb: true,
    db: true,
};

const CLEAR: Gate = Gate {
    table_version: 9,
    refs_version: 9,
    refs: Refs::Clear,
    refs_in_flight: true,
    barrier: false,
    moved: false,
};

/// The retired Db's manifest has no final checkpoint left.
const BARE: Pins = Pins {
    named: false,
    by_live: false,
    unnamed: false,
};

fn v(r: Retired, g: Option<Gate>) -> View {
    View {
        retired: Some(r),
        gate: g.map(Ok),
        pins: Some(Ok(BARE)),
        ..View::default()
    }
}

/// Like [`v`] with the checkpoints `pins` (`None` = the shell has not read them).
fn vp(pins: Option<Result<Pins, ReadErr>>) -> View {
    View {
        pins,
        ..v(RETIRED, Some(CLEAR))
    }
}

/// The checkpoint fence — the same in the plan (`pass` = `Eligible`) and in
/// the proof (`Delete`).
fn fence_rows(event: Event, pass: Plan) -> Vec<super::Row> {
    vec![
        row(
            "checkpoint fence: read the checkpoints",
            vp(None),
            event,
            Plan::Need(Need::Pins),
        )
        .with(|b| b.dd_no_checkpoint_fence = true, pass),
        row(
            "checkpoint fence: the Db's own manifest has no final checkpoint left ⇒ pass",
            vp(Some(Ok(BARE))),
            event,
            pass,
        ),
        row(
            "checkpoint fence: the scan is blind — a named checkpoint of a child that still has a Db ⇒ keep",
            vp(Some(Ok(Pins {
                named: true,
                ..BARE
            }))),
            event,
            Plan::Keep(Keep::NamedCheckpoint),
        )
        .with(|b| b.dd_no_checkpoint_fence = true, pass),
        row(
            "checkpoint fence: a live Db still has a named checkpoint while the scan says nobody ⇒ keep, tripwire",
            vp(Some(Ok(Pins {
                named: true,
                by_live: true,
                ..BARE
            }))),
            event,
            Plan::Keep(Keep::ProofsDisagree),
        )
        .with(|b| b.dd_no_checkpoint_fence = true, pass),
        row(
            "checkpoint fence: an unnamed checkpoint (before G15) ⇒ unknown, keep",
            vp(Some(Ok(Pins {
                unnamed: true,
                ..BARE
            }))),
            event,
            Plan::Keep(Keep::UnnamedCheckpoint),
        )
        .with(|b| b.dd_unnamed_as_clear = true, pass),
        row(
            "checkpoint fence: reading the manifest failed ⇒ keep",
            vp(Some(Err(ReadErr))),
            event,
            Plan::Keep(Keep::Unreadable),
        )
        .with(|b| b.dd_no_checkpoint_fence = true, pass),
    ]
}

#[test]
fn plan() {
    check(vec![
        row(
            "read the set",
            v(RETIRED, None),
            Event::ReapPlan,
            Plan::Need(Need::Gate),
        ),
        row(
            "nobody needs it ⇒ qualifies",
            v(RETIRED, Some(CLEAR)),
            Event::ReapPlan,
            Plan::Eligible,
        ),
        row(
            "no tombstone ⇒ not a candidate",
            v(
                Retired {
                    tomb: false,
                    ..RETIRED
                },
                Some(CLEAR),
            ),
            Event::ReapPlan,
            Plan::Keep(Keep::NotTomb),
        ),
        row(
            "mixed snapshots: the candidate filtered by a table other than the set's ⇒ keep",
            v(
                RETIRED,
                Some(Gate {
                    table_version: 10,
                    ..CLEAR
                }),
            ),
            Event::ReapPlan,
            Plan::Keep(Keep::StaleRefs),
        )
        .with(|b| b.dd_mixed_snapshots = true, Plan::Eligible),
        row(
            "N2: a set not adding children in flight ⇒ keep",
            v(
                RETIRED,
                Some(Gate {
                    refs_in_flight: false,
                    ..CLEAR
                }),
            ),
            Event::ReapPlan,
            Plan::Keep(Keep::StaleRefs),
        )
        .with(|b| b.dd_ignores_in_flight = true, Plan::Eligible),
        row(
            "SM-9: a live range stands on it ⇒ keep",
            v(
                RETIRED,
                Some(Gate {
                    refs: Refs::Referenced,
                    ..CLEAR
                }),
            ),
            Event::ReapPlan,
            Plan::Keep(Keep::Referenced),
        ),
        row(
            "child promised ⇒ keep",
            v(
                Retired {
                    promised: true,
                    ..RETIRED
                },
                Some(CLEAR),
            ),
            Event::ReapPlan,
            Plan::Keep(Keep::Promised),
        ),
        row(
            "checkpoint fence: the scan sees a reference ⇒ keep, no need to read the checkpoints",
            View {
                pins: None,
                ..v(
                    RETIRED,
                    Some(Gate {
                        refs: Refs::Referenced,
                        ..CLEAR
                    }),
                )
            },
            Event::ReapPlan,
            Plan::Keep(Keep::Referenced),
        ),
    ]);
    check(fence_rows(Event::ReapPlan, Plan::Eligible));
}

#[test]
fn prove() {
    check(vec![
        row(
            "nobody needs it ⇒ delete",
            v(RETIRED, Some(CLEAR)),
            Event::ReapProve,
            Plan::Delete,
        ),
        row(
            "the table changed after the gate's snapshot ⇒ keep this range, the round goes on",
            v(
                RETIRED,
                Some(Gate {
                    moved: true,
                    ..CLEAR
                }),
            ),
            Event::ReapProve,
            Plan::Keep(Keep::TableMoved),
        )
        .with(|b| b.dd_abandons_round = true, Plan::AbandonRound)
        .with(|b| b.dd_no_version_recheck = true, Plan::Delete),
        row(
            "proof recheck: the rebuilt proof sees a live range standing on it ⇒ keep",
            v(
                RETIRED,
                Some(Gate {
                    refs: Refs::Referenced,
                    ..CLEAR
                }),
            ),
            Event::ReapProve,
            Plan::Keep(Keep::Referenced),
        )
        .with(|b| b.dd_no_proof_recheck = true, Plan::Delete),
        row(
            "fence ⇒ keep, even before the proof recheck",
            v(
                RETIRED,
                Some(Gate {
                    barrier: true,
                    ..CLEAR
                }),
            ),
            Event::ReapProve,
            Plan::Keep(Keep::Barrier),
        )
        .with(|b| b.dd_no_proof_recheck = true, Plan::Keep(Keep::Barrier)),
        row(
            "proof recheck: the range back in the table? (impossible — but the proof does not trust the plan)",
            v(
                Retired {
                    in_table: true,
                    ..RETIRED
                },
                Some(CLEAR),
            ),
            Event::ReapProve,
            Plan::Keep(Keep::InTable),
        ),
    ]);
    check(fence_rows(Event::ReapProve, Plan::Delete));
}
