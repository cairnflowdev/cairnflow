// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Non-coordinators: roll back or roll forward, the roll-back round, opening a
//! Db, self-removing the close mark, retirement.

use super::{check, row};
use crate::{
    Cas, Entry, Event, Finder, Need, Op, Phase, Plan, ReadErr, Reopen, Standing, View, Who,
};

const OP: Op = Op::Split {
    source: 1,
    left: 2,
    right: 3,
};
const ABORT: Plan = Plan::Abort {
    ledger: Some(Phase::Aborting),
};

fn ledger(p: Option<Phase>) -> View {
    View {
        ledger: Some(Ok(p)),
        ..View::default()
    }
}

fn prepared(ok: Result<bool, ReadErr>) -> View {
    View {
        ledger: Some(Ok(Some(Phase::Prepared))),
        prepared: Some(ok),
        ..View::default()
    }
}

const OPENER: Event = Event::Found(Finder::Opener { active_here: false });

#[test]
fn roll_back_or_forward() {
    check(vec![
        row(
            "SM-3: this node's coordinator still holds it ⇒ leave it",
            View::default(),
            Event::Found(Finder::Opener { active_here: true }),
            Plan::Leave,
        ),
        row(
            "read the ledger before deciding",
            View::default(),
            OPENER,
            Plan::Need(Need::Ledger),
        )
        .with(|b| b.no_roll_forward = true, ABORT)
        .with(|b| b.no_ledger = true, Plan::Abort { ledger: None }),
        row(
            "op without a ledger (old binary) ⇒ roll back, as before",
            ledger(None),
            OPENER,
            ABORT,
        ),
        row(
            "ledger before Prepared ⇒ roll back",
            ledger(Some(Phase::Cloned)),
            OPENER,
            ABORT,
        ),
        row(
            "ledger already Aborting ⇒ keep rolling back",
            ledger(Some(Phase::Aborting)),
            OPENER,
            ABORT,
        ),
        row(
            "reading the ledger failed ⇒ roll back (always safe: a CAS)",
            View {
                ledger: Some(Err(ReadErr)),
                ..View::default()
            },
            OPENER,
            ABORT,
        ),
        row(
            "ledger Prepared ⇒ check directly",
            ledger(Some(Phase::Prepared)),
            OPENER,
            Plan::Need(Need::Prepared),
        ),
        row(
            "Prepared, every child has a Db and an owner, the sources closed at the right epoch ⇒ roll forward",
            prepared(Ok(true)),
            OPENER,
            Plan::RollForward,
        )
        .with(|b| b.no_roll_forward = true, ABORT),
        row(
            "Prepared but the check finds something missing ⇒ roll back",
            prepared(Ok(false)),
            OPENER,
            ABORT,
        ),
        row(
            "Prepared but the check's read failed ⇒ roll back",
            prepared(Err(ReadErr)),
            OPENER,
            ABORT,
        ),
        row(
            "SM-15: reconciliation round, the source's leader says it holds the op ⇒ leave it",
            View::default(),
            Event::Found(Finder::Reconciler {
                held: Ok(true),
                unheld_before: true,
            }),
            Plan::Leave,
        ),
        row(
            "SM-15: cannot ask ⇒ taken as held",
            View::default(),
            Event::Found(Finder::Reconciler {
                held: Err(ReadErr),
                unheld_before: true,
            }),
            Plan::Leave,
        ),
        row(
            "SM-15: first time nobody holds it ⇒ remember",
            View::default(),
            Event::Found(Finder::Reconciler {
                held: Ok(false),
                unheld_before: false,
            }),
            Plan::Remember,
        ),
        row(
            "SM-15: two rounds in a row nobody holds it ⇒ roll back or forward like the source claimer",
            prepared(Ok(true)),
            Event::Found(Finder::Reconciler {
                held: Ok(false),
                unheld_before: true,
            }),
            Plan::RollForward,
        )
        .with(|b| b.l1_dies_with_request = true, Plan::Leave),
        row(
            "ctl: an owner is still alive, no --force ⇒ refuse",
            View::default(),
            Event::Found(Finder::Ctl {
                live_owner: true,
                force: false,
            }),
            Plan::Refused,
        ),
        row(
            "ctl: --force ⇒ roll back (the operator does not roll forward)",
            prepared(Ok(true)),
            Event::Found(Finder::Ctl {
                live_owner: true,
                force: true,
            }),
            ABORT,
        ),
    ]);
}

/// **`intent clear` reads the ledger.** An op at `Prepared` whose roll-forward
/// check passes ⇒ the commit may already have won (the table rebuilt from a
/// history older than the commit): rolling back would delete the Db of a child
/// that has accepted writes ⇒ refuse, unless `--force`. Reading the ledger or
/// the check fails ⇒ refuse (unknown means keep). A ledger before `Prepared`,
/// no ledger, or a failing check ⇒ roll back as before. Before the fix (flag
/// `ctl_clears_prepared`): every row rolls back, without reading the ledger.
#[test]
fn ctl_reads_the_ledger_before_clearing() {
    const CTL: Event = Event::Found(Finder::Ctl {
        live_owner: false,
        force: false,
    });
    check(vec![
        row(
            "ctl: read the ledger before rolling back",
            View::default(),
            CTL,
            Plan::Need(Need::Ledger),
        )
        .with(|b| b.ctl_clears_prepared = true, ABORT),
        row(
            "ctl: Prepared, check not read yet ⇒ read it",
            ledger(Some(Phase::Prepared)),
            CTL,
            Plan::Need(Need::Prepared),
        )
        .with(|b| b.ctl_clears_prepared = true, ABORT),
        row(
            "ctl: Prepared and every child in place ⇒ the commit may have won — refuse",
            prepared(Ok(true)),
            CTL,
            Plan::Refused,
        )
        .with(|b| b.ctl_clears_prepared = true, ABORT),
        row(
            "ctl: Prepared but the check fails ⇒ the commit cannot have won — roll back",
            prepared(Ok(false)),
            CTL,
            ABORT,
        ),
        row(
            "ctl: the check failed ⇒ refuse (unknown means keep)",
            prepared(Err(ReadErr)),
            CTL,
            Plan::Refused,
        )
        .with(|b| b.ctl_clears_prepared = true, ABORT),
        row(
            "ctl: reading the ledger failed ⇒ refuse (unknown means keep)",
            View {
                ledger: Some(Err(ReadErr)),
                ..View::default()
            },
            CTL,
            Plan::Refused,
        )
        .with(|b| b.ctl_clears_prepared = true, ABORT),
        row(
            "ctl: ledger before Prepared ⇒ roll back",
            ledger(Some(Phase::Cloned)),
            CTL,
            ABORT,
        ),
        row("ctl: no ledger ⇒ roll back", ledger(None), CTL, ABORT),
        row(
            "ctl: --force ⇒ roll back even though the commit may have won",
            prepared(Ok(true)),
            Event::Found(Finder::Ctl {
                live_owner: false,
                force: true,
            }),
            ABORT,
        ),
    ]);
}

fn standing(s: Standing) -> View {
    View {
        standing: Some(Ok(s)),
        ..View::default()
    }
}

#[test]
fn roll_back_loop() {
    check(vec![
        row(
            "SM-3: op still pending ⇒ read the mark, then the CAS dropping the intent",
            standing(Standing::Pending),
            Event::Clear,
            Plan::CasClear,
        )
        .with(|b| b.sm3_discard_first = true, Plan::DiscardFirst),
        row(
            "already committed ⇒ stop",
            standing(Standing::Committed),
            Event::Clear,
            Plan::Nothing,
        ),
        row(
            "SM-15: another intent on the source (a later op) ⇒ do not touch",
            standing(Standing::Free { other: true }),
            Event::Clear,
            Plan::Nothing,
        ),
        row(
            "already rolled back ⇒ stop",
            standing(Standing::Free { other: false }),
            Event::Clear,
            Plan::Nothing,
        ),
        row(
            "reading the table failed ⇒ error",
            View {
                standing: Some(Err(ReadErr)),
                ..View::default()
            },
            Event::Clear,
            Plan::Fail,
        ),
        row(
            "the CAS dropping the intent won ⇒ Cleaned before removing the mark and cleaning up children; removed per the mark",
            View::default(),
            Event::Cleared {
                cas: Cas::Applied,
                by: Who::Opener,
            },
            Plan::Cleanup {
                ledger: Some(Phase::Cleaned),
                reopen: Reopen::AtMark,
            },
        )
        .with(
            |b| b.r631_reopen_any = true,
            Plan::Cleanup {
                ledger: Some(Phase::Cleaned),
                reopen: Reopen::Any,
            },
        )
        .with(
            |b| b.ctl_reopen_any = true,
            Plan::Cleanup {
                ledger: Some(Phase::Cleaned),
                reopen: Reopen::AtMark,
            },
        ),
        row(
            "ctl: removes per the mark like every roller-back (before the core read the ledger: unconditionally)",
            View::default(),
            Event::Cleared {
                cas: Cas::Applied,
                by: Who::Ctl,
            },
            Plan::Cleanup {
                ledger: Some(Phase::Cleaned),
                reopen: Reopen::AtMark,
            },
        )
        .with(
            |b| b.ctl_reopen_any = true,
            Plan::Cleanup {
                ledger: Some(Phase::Cleaned),
                reopen: Reopen::Any,
            },
        ),
        row(
            "the table changed underfoot ⇒ re-read",
            View::default(),
            Event::Cleared {
                cas: Cas::Lost,
                by: Who::Reconciler,
            },
            Plan::Reread,
        ),
        row(
            "unclear ⇒ error",
            View::default(),
            Event::Cleared {
                cas: Cas::Unknown,
                by: Who::Coordinator,
            },
            Plan::Fail,
        ),
    ]);
}

fn entry(e: Result<Entry, ReadErr>) -> View {
    View {
        entry: Some(e),
        ..View::default()
    }
}

const IN: Entry = Entry {
    in_table: true,
    intent: None,
};
const OUT: Entry = Entry {
    in_table: false,
    intent: None,
};
const WITH_INTENT: Entry = Entry {
    in_table: true,
    intent: Some(OP),
};

fn open(quiesced_here: bool, active_here: bool, ram: Entry) -> Event {
    Event::Open {
        quiesced_here,
        active_here,
        ram,
    }
}

#[test]
fn opening_a_range_db() {
    check(vec![
        row(
            "D2/SM-13: the source is quiesced on this node ⇒ wait",
            View::default(),
            open(true, true, IN),
            Plan::WaitUnquiesced,
        )
        .with(|b| b.d2_ignores_quiesced = true, Plan::Need(Need::Entry)),
        row(
            "read the fresh table before opening",
            View::default(),
            open(false, false, IN),
            Plan::Need(Need::Entry),
        ),
        row(
            "D1: reading the fresh table failed ⇒ do not open",
            entry(Err(ReadErr)),
            open(false, false, IN),
            Plan::Retry,
        )
        .with(|b| b.d1_stale_table = true, Plan::OpenDb),
        row(
            "D1: the fresh table says the range left the table ⇒ do not open",
            entry(Ok(OUT)),
            open(false, false, IN),
            Plan::Gone,
        ),
        row(
            "SM-3: an intent nobody here coordinates ⇒ decide roll back/forward first",
            entry(Ok(WITH_INTENT)),
            open(false, false, IN),
            Plan::Resolve(OP),
        ),
        row(
            "the intent of this node's coordinator (not quiesced yet) ⇒ open",
            entry(Ok(WITH_INTENT)),
            open(false, true, IN),
            Plan::OpenDb,
        ),
        row(
            "live range, no intent ⇒ open",
            entry(Ok(IN)),
            open(false, false, OUT),
            Plan::OpenDb,
        ),
        row(
            "SM-17: a close mark on a live range without an intent ⇒ remove per the mark",
            entry(Ok(IN)),
            Event::StaleClose,
            Plan::ReopenAtMark,
        )
        .with(|b| b.r631_reopen_any = true, Plan::ReopenAny),
        row(
            "SM-17: the range still has an intent ⇒ keep the mark",
            entry(Ok(WITH_INTENT)),
            Event::StaleClose,
            Plan::KeepClosed,
        ),
        row(
            "SM-17: the range left the table (commit) ⇒ the mark stays forever",
            entry(Ok(OUT)),
            Event::StaleClose,
            Plan::KeepClosed,
        ),
        row(
            "SM-17: reading the table failed ⇒ keep the mark",
            entry(Err(ReadErr)),
            Event::StaleClose,
            Plan::KeepClosed,
        ),
        row(
            "R6.3-6: the bucket's table no longer has the range either ⇒ retire",
            entry(Ok(OUT)),
            Event::LeftTable,
            Plan::Retire,
        ),
        row(
            "R6.3-6: the RAM table (learned from a peer) says it left but the bucket still has it ⇒ keep",
            entry(Ok(IN)),
            Event::LeftTable,
            Plan::Keep(crate::Keep::InTable),
        )
        .with(|b| b.r636_retire_on_ram = true, Plan::Retire),
        row(
            "R6.3-6: reading the bucket failed ⇒ no range retires this round",
            entry(Err(ReadErr)),
            Event::LeftTable,
            Plan::Keep(crate::Keep::Unreadable),
        ),
    ]);
}
