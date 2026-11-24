// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The coordinator: holding sources, the intent CAS, child-creation steps,
//! commit, rolling back in place, task death, failed roll-backs.

use super::{check, row};
use crate::{Cas, Event, Need, Phase, Plan, ReadErr, Stage, Standing, View, Who};

fn standing(s: Result<Standing, ReadErr>) -> View {
    View {
        standing: Some(s),
        ..View::default()
    }
}

const ROLL_BACK: Plan = Plan::RollBack {
    ledger: Some(Phase::Aborting),
    own_orphans: true,
};

#[test]
fn hold_and_intent_cas() {
    check(vec![
        row(
            "holds a source nobody holds yet",
            View::default(),
            Event::Hold { taken: false },
            Plan::Hold,
        ),
        row(
            "failed hold: the source already has a coordinator on this node — refuse, do not remove the holder's mark",
            View::default(),
            Event::Hold { taken: true },
            Plan::Refuse { release: false },
        )
        .with(
            |b| b.l13_failed_hold_releases = true,
            Plan::Refuse { release: true },
        ),
        row(
            "intent CAS won: ledger Intent (created), then go on",
            View::default(),
            Event::IntentCas(Cas::Applied),
            Plan::Go {
                ledger: Some(Phase::Intent),
            },
        )
        .with(|b| b.no_ledger = true, Plan::Go { ledger: None }),
        row(
            "SM-2: intent CAS lost — re-read, roll back if our intent got there",
            View::default(),
            Event::IntentCas(Cas::Lost),
            Plan::UndoStray,
        ),
        row(
            "SM-2: intent CAS unclear — same path",
            View::default(),
            Event::IntentCas(Cas::Unknown),
            Plan::UndoStray,
        ),
    ]);
}

#[test]
fn stages_check_the_bucket_before_creating_children() {
    let pending = standing(Ok(Standing::Pending));
    check(vec![
        row(
            "close mark: table not read yet ⇒ read it",
            View::default(),
            Event::Enter(Stage::Close),
            Plan::Need(Need::Standing),
        ),
        row(
            "close mark: the intent is still ours ⇒ ledger Closed before the mark",
            pending,
            Event::Enter(Stage::Close),
            Plan::Go {
                ledger: Some(Phase::Closed),
            },
        )
        .with(|b| b.before_close_mark = true, Plan::Skip)
        .with(|b| b.no_ledger = true, Plan::Go { ledger: None }),
        row(
            "first clone: ledger Cloned before the child Db",
            pending,
            Event::Enter(Stage::CloneFirst),
            Plan::Go {
                ledger: Some(Phase::Cloned),
            },
        ),
        row(
            "second clone: ledger unchanged",
            pending,
            Event::Enter(Stage::CloneNext),
            Plan::Go { ledger: None },
        ),
        row(
            "gathering the other range of a merge, the intent is still ours ⇒ keep waiting",
            pending,
            Event::Enter(Stage::Gather),
            Plan::Go { ledger: None },
        ),
        row(
            "gathering the other range of a merge, but the op was rolled back by someone else ⇒ stop (Busy), roll back",
            standing(Ok(Standing::Free { other: false })),
            Event::Enter(Stage::Gather),
            ROLL_BACK,
        ),
        row(
            "claiming children: ledger unchanged",
            pending,
            Event::Enter(Stage::Claim),
            Plan::Go { ledger: None },
        ),
        row(
            "commit: ledger Prepared before the commit CAS (no table read — the CAS is the check)",
            View::default(),
            Event::Enter(Stage::Commit),
            Plan::Go {
                ledger: Some(Phase::Prepared),
            },
        ),
        row(
            "D5: the intent was rolled back by someone else ⇒ roll back, create no children",
            standing(Ok(Standing::Free { other: false })),
            Event::Enter(Stage::CloneFirst),
            ROLL_BACK,
        )
        .with(
            |b| b.d5_recreates_children = true,
            Plan::Go {
                ledger: Some(Phase::Cloned),
            },
        ),
        row(
            "D5: rolled back by someone else, then a later op set an intent ⇒ roll back",
            standing(Ok(Standing::Free { other: true })),
            Event::Enter(Stage::Claim),
            ROLL_BACK,
        ),
        row(
            "D5: reading the table failed ⇒ roll back (Busy)",
            standing(Err(ReadErr)),
            Event::Enter(Stage::Close),
            ROLL_BACK,
        ),
    ]);
}

#[test]
fn commit_failure_and_task_death() {
    check(vec![
        row(
            "commit won: ledger Committed",
            View::default(),
            Event::CommitCas(Cas::Applied),
            Plan::Go {
                ledger: Some(Phase::Committed),
            },
        ),
        row(
            "commit lost: roll back in place (the roll-back round sees the op committed and stops)",
            View::default(),
            Event::CommitCas(Cas::Lost),
            ROLL_BACK,
        ),
        row(
            "commit unclear: roll back in place — the roll-back round re-reads the table",
            View::default(),
            Event::CommitCas(Cas::Unknown),
            ROLL_BACK,
        ),
        row(
            "D5: a step failed ⇒ Aborting, roll back, clean up the op's own children",
            View::default(),
            Event::Failed,
            ROLL_BACK,
        )
        .with(
            |b| b.d5_recreates_children = true,
            Plan::RollBack {
                ledger: Some(Phase::Aborting),
                own_orphans: false,
            },
        ),
        row(
            "L1: task died ⇒ cut the quiesce-holding cord (SM-15)",
            View::default(),
            Event::Vanished,
            Plan::Release,
        )
        .with(|b| b.l1_dies_with_request = true, Plan::GiveUp),
        row(
            "L4: rolling back in place failed ⇒ retry in the background",
            View::default(),
            Event::RollBackFailed(Who::Coordinator),
            Plan::Settle,
        )
        .with(|b| b.l4_roll_back_gives_up = true, Plan::GiveUp),
        row(
            "L3: intent CAS torn, re-read failed ⇒ retry in the background",
            View::default(),
            Event::RollBackFailed(Who::StrayUndo),
            Plan::Settle,
        )
        .with(|b| b.l3_stray_gives_up = true, Plan::GiveUp),
        row(
            "the background roll-back failed again ⇒ keep trying (capped in the shell, then the reconciliation round)",
            View::default(),
            Event::RollBackFailed(Who::Settle),
            Plan::Settle,
        ),
        row(
            "the source claimer's roll-back failed ⇒ the error goes up (the leadership loop retries the open)",
            View::default(),
            Event::RollBackFailed(Who::Opener),
            Plan::Fail,
        ),
    ]);
}
