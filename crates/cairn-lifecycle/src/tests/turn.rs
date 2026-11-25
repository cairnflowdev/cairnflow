// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! One turn: the gate (`gate_turn_table`, 18 rows), preflight
//! (`preflight_table`, 10 rows), ending (`settle_permit_table`, 6/8 rows), the
//! `Running` commit, throttling, errors, `apply_finish`.

use {
    super::*,
    crate::{
        backoff_ms, Attendance, Contract, Event, FailedAt, Finish, Need, OnMax, Outcome, Pause,
        Permit, Pinned, Plan, Planned, Release, Reply, RetryRow, RowChange, Seal, Side, Slot,
        Stage, StageChange, Status, Then, Turn, TurnErr, WakeKind, WakeWrite,
    },
    Status::{
        Completed as Done, Failed as Fail, Missing as Gone, Running as Run, Suspended as Susp,
    },
};

const PAUSE: Status = Status::Paused(Pause::Sdk);

fn slot(stage: Stage) -> Option<Slot> {
    Some(Slot {
        stage,
        exclusive: true,
    })
}

/// The actions of `decide_gate` (the decision-table version).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GateAction {
    Go,
    Ended { discard: bool },
    Paused { deliver: Option<WakeKind> },
}

/// `gate_turn_table` row for row, on both gates: before the permit
/// ([`Event::Drive`]) and after the permit ([`Event::PermitGranted`]).
#[test]
fn gate_turn_table() {
    use GateAction::*;
    const GO: GateAction = GateAction::Go;
    let wakes = [None, Some(WakeKind::Completion), Some(WakeKind::Signal)];
    let c = Some(WakeKind::Completion);
    let s = Some(WakeKind::Signal);
    #[rustfmt::skip]
    let rows: &[(&str, Status, Option<WakeKind>, GateAction)] = &[
        ("FL-10", Done,  None, Ended { discard: false }),
        ("FL-10", Done,  c,    Ended { discard: true }),
        ("FL-10", Done,  s,    Ended { discard: true }),
        ("FL-10", Fail,  None, Ended { discard: false }),
        ("FL-10", Fail,  c,    Ended { discard: true }),
        ("FL-10", Fail,  s,    Ended { discard: true }),
        ("FL-10", PAUSE, None, Paused { deliver: None }),
        ("FL-10", PAUSE, c,    Paused { deliver: c }),
        ("FL-10", PAUSE, s,    Paused { deliver: s }),
        ("FL-10", Run,   None, GO),
        ("FL-10", Run,   c,    GO),
        ("FL-10", Run,   s,    GO),
        ("FL-10", Susp,  None, GO),
        ("FL-10", Susp,  c,    GO),
        ("FL-10", Susp,  s,    GO),
        // No invocation: the decision table let it run and then `ensure_queued`
        // returned an error; the core returns `NotFound` at once (a difference,
        // the outcome is the same).
        ("—",     Gone,  None, GO),
        ("—",     Gone,  c,    GO),
        ("—",     Gone,  s,    GO),
    ];
    assert_eq!(rows.len(), 3 * 6, "the table covers the whole product");
    let _ = wakes;
    for (fl, status, wake, want) in rows {
        let turn = Turn {
            wake: *wake,
            ..Turn::PLAIN
        };
        let a = Answers {
            status: Some(*status),
            slot: Some(slot(Stage::Suspended)),
            retry: Some(None),
            journal_len: Some(4),
            ..Answers::default()
        };
        let before = ask_both(idle(), Event::Drive { turn }, a);
        // The `Running` commit differs from before the core in the watch row (`commit_running`).
        let after = if *want == GO {
            ask(idle(), Event::PermitGranted { turn }, a, crate::Bugs::NONE)
        } else {
            ask_both(idle(), Event::PermitGranted { turn }, a)
        };
        let ack = before.ram.duty == crate::Duty::Ack;
        match *want {
            GO if *status == Gone => {
                assert_eq!(before.then, Then::Reply(Reply::NotFound), "{fl}");
                assert_eq!(after.then, Then::Reply(Reply::NotFound), "{fl}");
            }
            GO => {
                assert_eq!(before.then, Then::RequestPermit, "{fl} {status:?}");
                assert!(!before.commits(), "{fl}: already in place ⇒ no write");
                assert!(matches!(after.then, Then::Preflight { .. }), "{fl}");
                assert_eq!(after.stage, StageChange::Move(Stage::Running), "FL-17");
            }
            Ended { discard } => {
                let w = if discard {
                    WakeWrite::Discard
                } else {
                    WakeWrite::None
                };
                assert_eq!(
                    (before.then, before.wake, ack),
                    (Then::Reply(Reply::Ended), w, true),
                    "{fl}"
                );
                // After the permit: leave the queue, release the VO lock too (`settle_permit` terminal).
                assert_eq!(
                    (after.wake, after.stage, after.ram.release),
                    (w, StageChange::Remove, Release::All),
                    "{fl}"
                );
            }
            Paused { deliver } => {
                let w = deliver.map_or(WakeWrite::None, WakeWrite::Deliver);
                assert_eq!(
                    (before.then, before.wake, ack),
                    (Then::Reply(Reply::Paused), w, true),
                    "{fl}"
                );
                assert!(
                    before.status.is_none() && before.stage == StageChange::Keep,
                    "{fl}: nothing changes before the permit"
                );
                // After the permit: stage `Paused`, keep the lock.
                assert_eq!(
                    (after.wake, after.stage, after.ram.release),
                    (w, StageChange::Move(Stage::Paused), Release::Running),
                    "{fl}"
                );
            }
        }
    }
}

/// `ensure_queued`: a missing queue entry ⇒ rebuilt at `Inbox` (FL-5).
#[test]
fn ensure_queued_rebuilds_entry() {
    let p = both(
        &idle().with_status(Run).with_slot(None),
        Event::Drive { turn: Turn::PLAIN },
    );
    assert_eq!(p.stage, StageChange::Insert(Stage::Inbox), "FL-5");
    assert_eq!(p.then, Then::RequestPermit);
}

fn contract(pinned: Pinned, max_attempts: Option<u32>, on_max: OnMax) -> Contract {
    Contract {
        pinned,
        max_attempts,
        on_max,
    }
}

/// `preflight_table` row for row (`retry_count` = attempts counted).
#[test]
fn preflight_table() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Pre {
        Run,
        NotFound,
        RetrySoon,
        Exhausted { max: u32, on_max: OnMax },
    }
    use {OnMax::*, Pinned::*, Pre::*};
    let v = |p, m, o, rc| (contract(p, m, o), rc);
    #[rustfmt::skip]
    let rows: &[(&str, (Contract, u32), Pre)] = &[
        // no deployment: the attempt cap is not considered
        ("—",     v(Undeclared, Some(0), Pause, 9), NotFound),
        ("—",     v(Undeclared, None,    Kill,  0), NotFound),
        ("FL-19", v(NotLoaded,  Some(0), Pause, 9), RetrySoon),
        ("FL-19", v(NotLoaded,  None,    Kill,  0), RetrySoon),
        // no cap declared: run forever
        ("FL-19", v(Found, None,    Pause, 1_000), Run),
        // under the cap: run; at the cap (turn max + 1): give up
        ("FL-19", v(Found, Some(3), Pause, 2), Run),
        ("FL-19", v(Found, Some(3), Pause, 3), Exhausted { max: 3, on_max: Pause }),
        ("FL-19", v(Found, Some(3), Kill,  3), Exhausted { max: 3, on_max: Kill }),
        ("FL-19", v(Found, Some(3), Kill,  4), Exhausted { max: 3, on_max: Kill }),
        ("FL-19", v(Found, Some(0), Pause, 0), Exhausted { max: 0, on_max: Pause }),
    ];
    assert_eq!(rows.len(), 10);
    for (fl, (c, rc), want) in rows {
        let a = Answers {
            contract: Some(*c),
            slot: Some(slot(Stage::Running)),
            retry: Some(Some(RetryRow::watch(66_000, rc + 1, 4))),
            journal_len: Some(4),
            ..Answers::default()
        };
        let e = Event::TurnStarted {
            turn: Turn::PLAIN,
            retry_count: *rc,
        };
        // RetrySoon refunds the attempt (changed with the core): compared separately below.
        let p = if *want == RetrySoon {
            ask(idle(), e, a, crate::Bugs::NONE)
        } else {
            ask_both(idle(), e, a)
        };
        let got = match p.then {
            Then::CallSdk { retry_count } => {
                assert_eq!(retry_count, *rc);
                assert_eq!(p.side, Some(Side::NewAttempt { attempt: rc + 1 }), "{fl}");
                Run
            }
            Then::Reply(Reply::NotFound) => NotFound,
            Then::Reply(Reply::Outcome) => {
                assert_eq!(p.stage, StageChange::Move(Stage::Suspended));
                RetrySoon
            }
            Then::Reply(Reply::Paused) => {
                assert_eq!(p.status, Some(Status::Paused(crate::Pause::MaxAttempts)));
                assert_eq!(p.side, Some(Side::Seal(Seal::Paused)));
                assert_eq!(
                    (p.stage, p.retry),
                    (StageChange::Move(Stage::Paused), RowChange::Clear)
                );
                Exhausted {
                    max: c.max_attempts.unwrap(),
                    on_max: c.on_max,
                }
            }
            Then::Reply(Reply::Ended) => {
                assert_eq!(p.status, Some(Status::Failed));
                assert_eq!(p.side, Some(Side::Seal(Seal::Terminal)));
                // One commit (before the core `fail_with` then `end_turn`).
                assert_eq!(
                    (p.stage, p.retry, p.ram.release),
                    (StageChange::Remove, RowChange::Clear, Release::All)
                );
                Exhausted {
                    max: c.max_attempts.unwrap(),
                    on_max: c.on_max,
                }
            }
            t => panic!("{t:?}"),
        };
        assert_eq!(got, *want, "{fl}: {c:?} rc {rc}");
    }
}

/// Preflight `RetrySoon` (deployment not loaded yet): the SDK is not called ⇒
/// the core refunds the attempt counted at `Running`. **Changed with the core**
/// (before it: a turn with a pre-planned retry kept the counted row; other
/// turns wrote `attempt 1`).
#[test]
fn retry_soon_refunds_attempt() {
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    let counted = RetryRow::retry(5_000, 3, 4, 0);
    let a = Answers {
        contract: Some(contract(Pinned::NotLoaded, None, OnMax::Pause)),
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(counted)),
        journal_len: Some(4),
        ..Answers::default()
    };
    #[rustfmt::skip]
    let rows: &[(&str, Turn, u32, RowChange, RowChange)] = &[
        // (FL, turn, rc, core, before the core)
        ("FL-19", planned,    2, RowChange::Set(RetryRow::retry(2_000, 2, 4, 1)), RowChange::Keep),
        ("FL-19", Turn::PLAIN, 0, RowChange::Set(RetryRow::retry(2_000, 0, 4, 1)), RowChange::Set(RetryRow::retry(2_000, 1, 4, 0))),
    ];
    for (fl, turn, rc, core, prod) in rows {
        let e = Event::TurnStarted {
            turn: *turn,
            retry_count: *rc,
        };
        let a = Answers {
            retry: Some(Some(if turn.planned.is_some() {
                counted
            } else {
                RetryRow::watch(66_000, 1, 4)
            })),
            ..a
        };
        assert_eq!(
            ask(idle(), e, a, crate::Bugs::NONE).retry,
            *core,
            "{fl} core"
        );
        assert_eq!(
            ask(idle(), e, a, crate::Bugs::BEFORE_STEP_3).retry,
            *prod,
            "{fl} before the core"
        );
    }
}

/// **Refused tenant** (FL-24): a tenant refused permits at preflight ⇒ the SDK
/// is not called; suspended until `now + the retry rhythm`, keeping the VO
/// lock, wake duty done, the attempt unchanged — refunding the attempt counted
/// at `Running` when the journal has not grown since the row's position,
/// counting again from the start when it has (an earlier sub-turn of the same
/// run made progress). The refusal comes before the contract: neither attempts
/// exhausted nor a missing deployment changes anything while refused. The
/// flag `refused_calls_sdk` is the behaviour before the fix: preflight ignored
/// the tenant state and called the SDK.
#[test]
fn refused_tenant_does_not_call_sdk() {
    const AFTER: u64 = 900_000;
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    #[rustfmt::skip]
    let rows: &[(&str, Turn, u32, RetryRow, u32, RetryRow)] = &[
        // (FL, turn, rc, row at preflight, journal length, row the core writes)
        // the run's first sub-turn: the journal has not grown since the `Running` commit
        ("FL-24", Turn::PLAIN, 0, RetryRow::watch(66_000, 1, 4),   4, RetryRow::retry(NOW + AFTER, 0, 4, 1)),
        ("FL-24", planned,     2, RetryRow::retry(5_000, 3, 4, 2), 4, RetryRow::retry(NOW + AFTER, 2, 4, 3)),
        // a follow-up sub-turn: the earlier sub-turn made progress ⇒ count again from the start
        ("FL-24", Turn::PLAIN, 3, RetryRow::watch(66_000, 4, 4),   6, RetryRow::retry(NOW + AFTER, 0, 6, 0)),
    ];
    for (fl, turn, rc, row, len, want) in rows {
        let e = Event::TurnStarted {
            turn: *turn,
            retry_count: *rc,
        };
        for pinned in [Pinned::Found, Pinned::Undeclared, Pinned::NotLoaded] {
            let a = Answers {
                contract: Some(contract(pinned, Some(*rc), OnMax::Kill)),
                slot: Some(slot(Stage::Running)),
                retry: Some(Some(*row)),
                journal_len: Some(*len),
                ..Answers::default()
            };
            let v = idle().with_tenant_refused(Some(AFTER));
            let p = ask(v, e, a, crate::Bugs::NONE);
            assert_eq!(p.need, None, "{fl} {pinned:?}");
            assert_eq!(p.then, Then::Reply(Reply::Throttled), "{fl} {pinned:?}");
            assert_eq!((p.status, p.side), (None, None), "{fl}: nothing new");
            assert_eq!(p.stage, StageChange::Move(Stage::Suspended), "{fl}");
            assert_eq!(p.retry, RowChange::Set(*want), "{fl} {pinned:?}");
            assert_eq!(
                (p.ram.release, p.ram.duty, p.ram.rescue),
                (Release::Running, crate::Duty::Ack, crate::Mark::Clear),
                "{fl}: keep the VO lock, the wake-up is already durable"
            );
            assert_eq!(
                ask(v, e, a, crate::Bugs::BEFORE_STEP_3),
                p,
                "{fl}: not a row changed with the core"
            );
            // Before the fix: preflight did not ask the tenant state.
            let before = crate::Bugs {
                refused_calls_sdk: true,
                ..crate::Bugs::NONE
            };
            assert_eq!(
                ask(v, e, a, before),
                ask(idle(), e, a, crate::Bugs::NONE),
                "{fl} {pinned:?}: flag refused_calls_sdk = as if not refused"
            );
        }
    }
    // Not refused: preflight as before (call the SDK while attempts remain).
    let a = Answers {
        contract: Some(contract(Pinned::Found, None, OnMax::Pause)),
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(RetryRow::watch(66_000, 1, 4))),
        journal_len: Some(4),
        ..Answers::default()
    };
    let e = Event::TurnStarted {
        turn: Turn::PLAIN,
        retry_count: 0,
    };
    let p = ask(idle().with_tenant_refused(None), e, a, crate::Bugs::NONE);
    assert_eq!(p.then, Then::CallSdk { retry_count: 0 });
}

/// `settle_permit_table`: 6/8 rows — `Throttled` and `AlreadyInvoked` are
/// outcomes of the shell, never reaching `end_turn` (throttling is
/// [`Event::Throttled`], with no permit that has `begin`).
#[test]
fn settle_permit_table() {
    #[rustfmt::skip]
    let rows: &[(&str, Option<Outcome>, bool, Stage)] = &[
        // the outcome could not be read: keep the VO lock
        ("FL-4", None,                                   false, Stage::Suspended),
        ("FL-4", Some(Outcome::Completed),               true,  Stage::Suspended),
        ("FL-4", Some(Outcome::Failed),                  true,  Stage::Suspended),
        ("FL-4", Some(Outcome::Paused),                  false, Stage::Paused),
        ("FL-4", Some(Outcome::Waiting),                 false, Stage::Suspended),
        ("FL-4", Some(Outcome::Retry { delay_ms: None }), false, Stage::Suspended),
    ];
    for (fl, outcome, terminal, stage) in rows {
        let planned = Turn::planned(Planned {
            attempt: 0,
            next_due: 5_000,
            journal_len: 4,
        });
        // A failed turn reads the status back (rule 7): still `running` ⇒ FL-4.
        let a = Answers {
            status: Some(Run),
            slot: Some(slot(Stage::Running)),
            retry: Some(Some(RetryRow::retry(5_000, 1, 4, 0))),
            journal_len: Some(4),
            ..Answers::default()
        };
        let e = match outcome {
            Some(o) => Event::TurnEnded {
                turn: planned,
                retry_count: 0,
                outcome: *o,
            },
            None => Event::TurnFailed {
                turn: planned,
                at: FailedAt::AfterRunning { retry_count: 0 },
                err: TurnErr::Other,
            },
        };
        let p = ask_both(idle(), e, a);
        let want_stage = if *terminal {
            StageChange::Remove
        } else {
            StageChange::Move(*stage)
        };
        let want_release = if *terminal {
            Release::All
        } else {
            Release::Running
        };
        assert_eq!(
            (p.stage, p.ram.release),
            (want_stage, want_release),
            "{fl}: {outcome:?}"
        );
    }
}

/// The `Running` commit (`begin_turn`, FL-17, FL-19): one row per turn.
/// **Changed with the core** (the reverse of B): the watch row carries
/// `attempt + 1` and the journal length, before the core `0 ‖ u32::MAX`.
#[test]
fn commit_running() {
    let watch_due = NOW + KNOBS.watch_after_ms;
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    #[rustfmt::skip]
    let rows: &[(&str, Turn, Option<RetryRow>, u32, RowChange, RowChange, u32)] = &[
        // (FL, turn, current row, len, core, before the core, rc)
        // a retry fires: rewrite the pre-planned row with attempt + 1, fires back to 0
        ("FL-19, FL-21", planned, Some(RetryRow::retry(5_000, 2, 4, 3)), 4,
            RowChange::Set(RetryRow::retry(5_000, 3, 4, 0)), RowChange::Set(RetryRow::retry(5_000, 3, 4, 0)), 2),
        // a retry fires while the journal grew during the permit wait: count again
        // at the new position (changed with the core — before it the fire's count
        // and position were kept)
        ("FL-19", planned, Some(RetryRow::retry(5_000, 2, 4, 1)), 5,
            RowChange::Set(RetryRow::retry(5_000, 1, 5, 0)), RowChange::Set(RetryRow::retry(5_000, 3, 4, 0)), 0),
        // a new turn: the watch row counts the first attempt
        ("FL-17, FL-19", Turn::PLAIN, None, 1,
            RowChange::Set(RetryRow::watch(watch_due, 1, 1)), RowChange::Set(RetryRow::legacy_watch(watch_due)), 0),
        // a `recover` turn after a turn that died midway: the watch row counted 1 ⇒ rc 1
        ("FL-19", Turn::PLAIN, Some(RetryRow::watch(9, 1, 4)), 4,
            RowChange::Set(RetryRow::watch(watch_due, 2, 4)), RowChange::Set(RetryRow::legacy_watch(watch_due)), 1),
        // the journal grew since then: count again
        ("FL-19", Turn::PLAIN, Some(RetryRow::watch(9, 1, 4)), 5,
            RowChange::Set(RetryRow::watch(watch_due, 1, 5)), RowChange::Set(RetryRow::legacy_watch(watch_due)), 0),
        // a turn carrying a wake-up / resume: count again
        // (the attempt's position is after the wake-up this commit writes: len + 1)
        ("FL-19", Turn::woken(WakeKind::Signal), Some(RetryRow::retry(9, 2, 4, 0)), 4,
            RowChange::Set(RetryRow::watch(watch_due, 1, 5)), RowChange::Set(RetryRow::legacy_watch(watch_due)), 0),
        ("FL-19", Turn { reset: true, ..Turn::PLAIN }, Some(RetryRow::retry(9, 2, 4, 0)), 4,
            RowChange::Set(RetryRow::watch(watch_due, 1, 4)), RowChange::Set(RetryRow::legacy_watch(watch_due)), 0),
    ];
    for (fl, turn, current, len, core, prod, rc) in rows {
        let a = Answers {
            status: Some(Run),
            slot: Some(slot(Stage::Suspended)),
            retry: Some(*current),
            journal_len: Some(*len),
            ..Answers::default()
        };
        let e = Event::PermitGranted { turn: *turn };
        let c = ask(idle(), e, a, crate::Bugs::NONE);
        let p = ask(idle(), e, a, crate::Bugs::BEFORE_STEP_3);
        assert_eq!(c.retry, *core, "{fl} core");
        assert_eq!(p.retry, *prod, "{fl} before the core");
        assert_eq!(c.stage, StageChange::Move(Stage::Running), "{fl}");
        assert_eq!(c.then, Then::Preflight { retry_count: *rc }, "{fl}");
        if turn.planned.is_some() && *len == 4 {
            assert_eq!(
                p.then, c.then,
                "{fl}: the retry turn at the same position as before the core"
            );
        }
        assert_eq!(
            c.wake,
            turn.wake.map_or(WakeWrite::None, WakeWrite::Deliver)
        );
    }
}

/// Throttling (`park_for_retry`, `drive_retry`'s `Throttled` branch): the turn
/// did not enter `Running` ⇒ it spends no attempt. **Changed with the core**
/// (suspicion B): before it, a turn without a pre-planned retry wrote
/// `attempt 1`.
#[test]
fn throttle_spends_no_attempt() {
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    let after = 300;
    #[rustfmt::skip]
    let rows: &[(&str, Turn, Option<RetryRow>, RowChange, RowChange)] = &[
        // the first turn is throttled: 0 attempts, 1 fire (the backoff step still goes up — FL-21)
        ("FL-19, suspicion B", Turn::PLAIN, None,
            RowChange::Set(RetryRow::retry(NOW + after, 0, 4, 1)), RowChange::Set(RetryRow::retry(NOW + after, 1, 4, 0))),
        // the `recover` turn of an invocation that counted 1 attempt (watch row): keep 1
        ("FL-19, suspicion B", Turn::PLAIN, Some(RetryRow::watch(9, 1, 4)),
            RowChange::Set(RetryRow::retry(NOW + after, 1, 4, 1)), RowChange::Set(RetryRow::retry(NOW + after, 1, 4, 0))),
        // a turn carrying a timer (a task waiting for the lock) is throttled: the timer is not written ⇒ the count stays
        ("FL-19", Turn::woken(WakeKind::Completion), Some(RetryRow::retry(9, 1, 4, 0)),
            RowChange::Set(RetryRow::retry(NOW + after, 1, 4, 1)), RowChange::Set(RetryRow::retry(NOW + after, 1, 4, 0))),
        // a pre-planned retry: the row stays as is (FL-19)
        ("FL-19", planned, Some(RetryRow::retry(5_000, 2, 4, 1)), RowChange::Keep, RowChange::Keep),
    ];
    for (fl, turn, current, core, prod) in rows {
        let a = Answers {
            retry: Some(*current),
            journal_len: Some(4),
            ..Answers::default()
        };
        let e = Event::Throttled {
            turn: *turn,
            after_ms: after,
        };
        let c = ask(idle(), e, a, crate::Bugs::NONE);
        assert_eq!(c.retry, *core, "{fl} core");
        assert_eq!(
            ask(idle(), e, a, crate::Bugs::BEFORE_STEP_3).retry,
            *prod,
            "{fl} before the core"
        );
        assert_eq!(
            (c.ram.duty, c.then),
            (crate::Duty::Ack, Then::Reply(Reply::Throttled))
        );
    }
}

/// Throttling a turn with a pre-planned retry: the row's due time becomes
/// `max(row.due, now + after_ms)` — the count and the fires stay as they are
/// (FL-19); if the due time does not change, nothing is written. Before, the
/// row kept the handler's backoff-step due time (possibly 1 s): a suspended
/// tenant cost ~330 commits/hour per invocation. The flag
/// `throttled_keeps_due` (and `BEFORE_STEP_3`) pins the old behaviour.
#[test]
fn throttle_pushes_due_of_planned_retry() {
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    let keeps = crate::Bugs {
        throttled_keeps_due: true,
        ..crate::Bugs::NONE
    };
    #[rustfmt::skip]
    let rows: &[(&str, u64, RetryRow, RowChange)] = &[
        // the old due time has passed (the fire just went off): move to `now + after`
        ("due time passed", 15 * 60_000, RetryRow::retry(NOW - 1, 2, 4, 1),
            RowChange::Set(RetryRow::retry(NOW + 15 * 60_000, 2, 4, 1))),
        // the old due time is later than `now + after`: the row stays as is (no needless write)
        ("old due time later", 300, RetryRow::retry(NOW + 10_000, 2, 4, 1), RowChange::Keep),
    ];
    for (what, after, current, core) in rows {
        let a = Answers {
            retry: Some(Some(*current)),
            journal_len: Some(4),
            ..Answers::default()
        };
        let e = Event::Throttled {
            turn: planned,
            after_ms: *after,
        };
        assert_eq!(
            ask(idle(), e, a, crate::Bugs::NONE).retry,
            *core,
            "{what} core"
        );
        assert_eq!(
            ask(idle(), e, a, keeps).retry,
            RowChange::Keep,
            "{what} flag"
        );
        assert_eq!(
            ask(idle(), e, a, crate::Bugs::BEFORE_STEP_3).retry,
            RowChange::Keep,
            "{what} before the core"
        );
    }
    // The row has disappeared (cleaned up midway): nothing is rebuilt.
    let a = Answers {
        retry: Some(None),
        journal_len: Some(4),
        ..Answers::default()
    };
    let e = Event::Throttled {
        turn: planned,
        after_ms: 300,
    };
    assert_eq!(ask(idle(), e, a, crate::Bugs::NONE).retry, RowChange::Keep);
}

/// Dropping a permit that has not `begin` (a throttle racing the grant, a
/// cancelled task): the VO lock held from before stays.
#[test]
fn drop_permit_not_yet_begun() {
    #[rustfmt::skip]
    let rows: &[(&str, Permit, Release, Release)] = &[
        // (FL, permit, core, planted f018_drop_held_lock)
        ("FL-6",  Permit::Waiting,                      Release::Claim,   Release::Claim),
        ("FL-4",  Permit::Granted { held_before: true }, Release::Running, Release::All),
        ("FL-6",  Permit::Granted { held_before: false }, Release::All,    Release::All),
    ];
    let bug = crate::Bugs {
        f018_drop_held_lock: true,
        ..crate::Bugs::NONE
    };
    for (fl, permit, core, f018) in rows {
        let mut at = view(Attendance {
            lock_held: false,
            permit: *permit,
            ..Attendance::IDLE
        });
        // Throttling a turn with a pre-planned retry reads the retry row; no
        // row ⇒ nothing is written.
        at.retry = Some(None);
        let planned = Turn::planned(Planned {
            attempt: 0,
            next_due: 5_000,
            journal_len: 4,
        });
        for e in [
            Event::PermitCancelled { turn: planned },
            Event::Throttled {
                turn: planned,
                after_ms: 100,
            },
        ] {
            assert_eq!(both(&at, e).ram.release, *core, "{fl} {e:?}");
            assert_eq!(
                run(&at, e, bug).ram.release,
                *f018,
                "{fl} f018_drop_held_lock {e:?}"
            );
        }
        // Cancelled: no ack — the rescue net is marked (FL-16).
        assert_eq!(
            both(&at, Event::PermitCancelled { turn: planned }).ram.duty,
            crate::Duty::Keep
        );
    }
}

/// Ending a turn with an outcome: `retry_if` + `end_turn` + `drive_retry`.
#[test]
fn turn_end_with_outcome() {
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    let base = Answers {
        slot: Some(slot(Stage::Running)),
        journal_len: Some(4),
        policy: Some(Some(700)),
        ..Answers::default()
    };
    let ended = |turn, rc, outcome| Event::TurnEnded {
        turn,
        retry_count: rc,
        outcome,
    };
    let retry = Outcome::Retry { delay_ms: None };
    #[rustfmt::skip]
    let rows: &[(&str, Event, Option<RetryRow>, RowChange, RowChange)] = &[
        // (FL, event, current row, core, before the core)
        // suspended / paused: remove the watch row (a new turn) or cancel the retry (a retry turn)
        ("FL-17", ended(Turn::PLAIN, 0, Outcome::Waiting), Some(RetryRow::watch(9, 1, 4)), RowChange::Clear, RowChange::Clear),
        ("FL-17", ended(planned, 2, Outcome::Waiting), Some(RetryRow::retry(5_000, 3, 4, 0)), RowChange::Clear, RowChange::Clear),
        ("FL-17", ended(Turn::PLAIN, 0, Outcome::Paused), Some(RetryRow::watch(9, 1, 4)), RowChange::Clear, RowChange::Clear),
        // finished: the row already went with `apply_finish`
        ("FL-17", ended(Turn::PLAIN, 0, Outcome::Completed), None, RowChange::Keep, RowChange::Keep),
        ("FL-17", ended(planned, 2, Outcome::Failed), Some(RetryRow::retry(5_000, 3, 4, 0)), RowChange::Clear, RowChange::Clear),
        // the SDK says retry, no due time: the handler's policy (changed with the core — before it backoff(0))
        ("FL-16", ended(Turn::PLAIN, 0, retry), Some(RetryRow::watch(9, 1, 4)),
            RowChange::Set(RetryRow::retry(NOW + 700, 1, 4, 0)), RowChange::Set(RetryRow::retry(NOW + backoff_ms(0), 1, 4, 0))),
        // the SDK gives a due time (`next_retry_delay`): that due time, once
        ("FL-16", ended(Turn::PLAIN, 0, Outcome::Retry { delay_ms: Some(42) }), Some(RetryRow::watch(9, 1, 4)),
            RowChange::Set(RetryRow::retry(NOW + 42, 1, 4, 0)), RowChange::Set(RetryRow::retry(NOW + 42, 1, 4, 0))),
        // a pre-planned retry deferred because busy: bring it back to the planned due time
        ("FL-5, FL-20", ended(planned, 2, retry), Some(RetryRow::retry(9_000, 3, 4, 0)),
            RowChange::Set(RetryRow::retry(5_000, 3, 4, 0)), RowChange::Set(RetryRow::retry(5_000, 3, 4, 0))),
        ("FL-5", ended(planned, 2, retry), Some(RetryRow::retry(5_000, 3, 4, 0)), RowChange::Keep, RowChange::Keep),
        ("FL-5", ended(planned, 2, retry), None, RowChange::Keep, RowChange::Keep),
    ];
    for (fl, e, current, core, prod) in rows {
        let a = Answers {
            retry: Some(*current),
            ..base
        };
        let c = ask(idle(), *e, a, crate::Bugs::NONE);
        assert_eq!(c.retry, *core, "{fl} core {e:?}");
        assert_eq!(
            ask(idle(), *e, a, crate::Bugs::BEFORE_STEP_3).retry,
            *prod,
            "{fl} before the core {e:?}"
        );
        assert_eq!(
            (c.ram.duty, c.ram.rescue),
            (crate::Duty::Ack, crate::Mark::Clear),
            "{fl}: the turn ran to the end"
        );
    }
}

/// The turn throws: keep the VO lock, **must** leave a retry (FL-4, FL-16).
#[test]
fn turn_throws() {
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    let a = Answers {
        status: Some(Run),
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(RetryRow::watch(9, 1, 4))),
        journal_len: Some(4),
        policy: Some(Some(700)),
        ..Answers::default()
    };
    // A turn with a pre-planned retry: the row is the retry the fire wrote
    // (`Retry`) — the watch row moved far out because of a parked ticket is in
    // `broken_ticket_turn_rewrites_deferred_watch_row`.
    let ap = Answers {
        retry: Some(Some(RetryRow::retry(5_000, 2, 4, 1))),
        ..a
    };
    let after = |turn, err| Event::TurnFailed {
        turn,
        at: FailedAt::AfterRunning { retry_count: 0 },
        err,
    };
    let before = |turn, err| Event::TurnFailed {
        turn,
        at: FailedAt::BeforeRunning,
        err,
    };
    #[rustfmt::skip]
    let rows: &[(&str, Event, RowChange, RowChange, crate::Duty)] = &[
        // (FL, event, core, before the core, duty)
        ("FL-16", after(Turn::PLAIN, TurnErr::Other),
            RowChange::Set(RetryRow::retry(NOW + 700, 1, 4, 0)), RowChange::Set(RetryRow::retry(NOW + backoff_ms(0), 1, 4, 0)), crate::Duty::Ack),
        ("FL-16", after(Turn::PLAIN, TurnErr::Fenced), RowChange::Keep, RowChange::Keep, crate::Duty::Ack),
        ("FL-19", after(planned, TurnErr::Other), RowChange::Keep, RowChange::Keep, crate::Duty::Ack),
        // `ensure_queued` failed (suspicion B, changed with the core): no attempt spent
        ("FL-19, suspicion B", before(Turn::PLAIN, TurnErr::Other),
            RowChange::Set(RetryRow::retry(NOW + 700, 1, 4, 1)), RowChange::Set(RetryRow::retry(NOW + backoff_ms(0), 1, 4, 0)), crate::Duty::Ack),
        ("FL-19", before(planned, TurnErr::Other), RowChange::Keep, RowChange::Keep, crate::Duty::Ack),
    ];
    for (fl, e, core, prod, duty) in rows {
        let a = match e {
            Event::TurnFailed { turn, .. } if turn.planned.is_some() => ap,
            _ => a,
        };
        let c = ask(idle(), *e, a, crate::Bugs::NONE);
        assert_eq!(c.retry, *core, "{fl} core {e:?}");
        assert_eq!(
            ask(idle(), *e, a, crate::Bugs::BEFORE_STEP_3).retry,
            *prod,
            "{fl} before the core {e:?}"
        );
        assert_eq!(c.ram.duty, *duty, "{fl}");
        if let Event::TurnFailed {
            at: FailedAt::AfterRunning { .. },
            ..
        } = e
        {
            assert_eq!(
                (c.stage, c.ram.release),
                (StageChange::Move(Stage::Suspended), Release::Running),
                "FL-4"
            );
        }
    }
    // The journal length cannot be read: keep the watch row, the duty drops (FL-16).
    let v = idle()
        .with_slot(slot(Stage::Running))
        .with_read_error(Need::JournalLen, crate::ReadErr::Other);
    let p = both(&v, after(Turn::PLAIN, TurnErr::Other));
    assert_eq!(
        (p.retry, p.ram.duty),
        (RowChange::Keep, crate::Duty::Keep),
        "FL-16"
    );
}

/// Preflight `NotFound` (no deployment declares the service): the turn ends as
/// `failed` 404 in **one** commit — the status, removing the queue entry,
/// deleting the retry row (the turn's own watch row) — matching the 404 the
/// caller already received. Before the fix: 404 was returned without writing
/// a status, the watch row stayed ⇒ `running` without an entry, looping with
/// backoff or stuck without a wake-up.
#[test]
fn preflight_notfound_ends_failed_404() {
    let a = Answers {
        contract: Some(contract(Pinned::Undeclared, Some(3), OnMax::Pause)),
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(RetryRow::watch(66_000, 1, 4))),
        journal_len: Some(4),
        ..Answers::default()
    };
    for turn in [
        Turn::PLAIN,
        Turn {
            planned: Some(Planned {
                next_due: 70_000,
                attempt: 1,
                journal_len: 4,
            }),
            ..Turn::PLAIN
        },
    ] {
        let e = Event::TurnStarted {
            turn,
            retry_count: 0,
        };
        let p = ask(idle(), e, a, crate::Bugs::NONE);
        assert_eq!(p.then, Then::Reply(Reply::NotFound), "{turn:?}");
        assert_eq!(
            (p.status, p.stage, p.retry, p.side),
            (
                Some(Fail),
                StageChange::Remove,
                RowChange::Clear,
                Some(Side::Seal(Seal::Terminal))
            ),
            "{turn:?}: one finishing commit"
        );
        assert_eq!(
            (p.ram.release, p.ram.duty),
            (Release::All, crate::Duty::Ack),
            "{turn:?}"
        );
        // Before the fix (a historical bug, flag `notfound_keeps_running`): no
        // status, the watch row stays.
        let old = ask(
            idle(),
            e,
            a,
            crate::Bugs {
                notfound_keeps_running: true,
                ..crate::Bugs::NONE
            },
        );
        assert_eq!(
            (old.status, old.stage, old.retry, old.then),
            (
                None,
                StageChange::Remove,
                RowChange::Keep,
                Then::Reply(Reply::NotFound)
            ),
            "{turn:?}"
        );
    }
}

/// `apply_finish`: the engine's finishing commit.
#[test]
fn engine_finish() {
    let a = Answers {
        status: Some(Run),
        slot: Some(slot(Stage::Running)),
        ..Answers::default()
    };
    #[rustfmt::skip]
    let rows: &[(&str, Finish, Option<Status>, StageChange, RowChange, Option<Side>, bool)] = &[
        ("FL-17", Finish::Completed, Some(Done), StageChange::Remove, RowChange::Clear, Some(Side::Seal(Seal::Terminal)), false),
        ("FL-17", Finish::Failed,    Some(Fail), StageChange::Remove, RowChange::Clear, Some(Side::Seal(Seal::Terminal)), false),
        ("FL-4",  Finish::Paused,    Some(PAUSE), StageChange::Keep,  RowChange::Keep,  Some(Side::Seal(Seal::Paused)), false),
        ("—",     Finish::Suspended { satisfied: false }, Some(Susp), StageChange::Keep, RowChange::Keep, None, false),
        ("—",     Finish::Suspended { satisfied: true },  Some(Run),  StageChange::Keep, RowChange::Keep, None, false),
        ("—",     Finish::Retry,     Some(Run),  StageChange::Keep,   RowChange::Keep,  None, false),
    ];
    for (fl, f, status, stage, retry, side, spawn) in rows {
        let p = ask_both(idle(), Event::Finished { finish: *f }, a);
        assert_eq!(
            (p.status, p.stage, p.retry, p.side, p.ram.spawn_successor),
            (*status, *stage, *retry, *side, *spawn),
            "{fl} {f:?}"
        );
    }
    // continue_as_new: the old run ends with its row (changed with the core — before it the row stayed)
    let e = Event::Finished {
        finish: Finish::ContinuedAsNew,
    };
    let c = ask(idle(), e, a, crate::Bugs::NONE);
    let p = ask(idle(), e, a, crate::Bugs::BEFORE_STEP_3);
    assert_eq!(
        (c.status, c.stage, c.retry, c.side),
        (
            Some(Done),
            StageChange::Remove,
            RowChange::Clear,
            Some(Side::Seal(Seal::Restarted))
        )
    );
    assert!(c.ram.spawn_successor);
    assert_eq!(p.retry, RowChange::Keep, "suspicion D (2) before the core");
    // A cancel slipped in during the turn: the outcome is durable, write nothing (FL-10).
    for s in [Done, Fail, Gone] {
        let p = ask_both(
            idle(),
            Event::Finished {
                finish: Finish::Completed,
            },
            Answers {
                status: Some(s),
                ..a
            },
        );
        assert_eq!(p, Plan::default(), "FL-10 {s:?}");
        // `continue_as_new` too: write nothing, **do not** create the new run —
        // the shell ends the turn like a cancelled run.
        let p = ask_both(
            idle(),
            Event::Finished {
                finish: Finish::ContinuedAsNew,
            },
            Answers {
                status: Some(s),
                ..a
            },
        );
        assert_eq!(p, Plan::default(), "FL-10 continue_as_new {s:?}");
        assert!(!p.ram.spawn_successor);
    }
}

/// The due time is `now + delay`, and the delay is set by the **tenant**: the
/// SDK's `next_retry_delay` or the manifest's `retryPolicy*Interval`. A delay
/// near `u64::MAX` must not overflow (debug: a panic on the engine thread;
/// release: wraps into the past ⇒ fires again on every tick) — the due time
/// saturates at `u64::MAX` (found by fuzzing).
#[test]
fn due_saturates_on_huge_delay() {
    const HUGE: u64 = u64::MAX;
    let a = Answers {
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(RetryRow::watch(9, 1, 4))),
        journal_len: Some(4),
        policy: Some(Some(HUGE)),
        status: Some(Run),
        ..Answers::default()
    };
    let ended = |outcome| Event::TurnEnded {
        turn: Turn::PLAIN,
        retry_count: 0,
        outcome,
    };
    #[rustfmt::skip]
    let rows: &[(&str, Event)] = &[
        ("SDK next_retry_delay", ended(Outcome::Retry { delay_ms: Some(HUGE) })),
        ("policy, the SDK gives no due time", ended(Outcome::Retry { delay_ms: None })),
        ("the turn throws after Running", Event::TurnFailed {
            turn: Turn::PLAIN, at: FailedAt::AfterRunning { retry_count: 0 }, err: TurnErr::Other,
        }),
        ("the turn throws before Running", Event::TurnFailed {
            turn: Turn::PLAIN, at: FailedAt::BeforeRunning, err: TurnErr::Other,
        }),
        ("throttled", Event::Throttled { turn: Turn::PLAIN, after_ms: HUGE }),
        ("retry tick fires", Event::RetryDue { row: RetryRow::retry(NOW, 1, 4, 0) }),
    ];
    // The old bug (`due_wraps`, a release build before the fix): the due time wraps to `NOW - 1`.
    let wraps = crate::Bugs {
        due_wraps: true,
        ..crate::Bugs::NONE
    };
    for (what, e) in rows {
        for (bugs, want) in [(crate::Bugs::NONE, HUGE), (wraps, NOW - 1)] {
            match ask(idle(), *e, a, bugs).retry {
                RowChange::Set(r) => assert_eq!(r.due, want, "{what} {bugs:?}: due {r:?}"),
                other => panic!("{what}: no retry row set: {other:?}"),
            }
        }
    }
}

/// A turn that does not finish: the SDK runs out of body/closes the stream
/// without `End`/`Suspension`/`Error` — the engine commits nothing, the durable
/// state is still `running`. The shell reports `Retry` (the same branch as
/// `FailureAction::Retry`): the core keeps the queue entry (back to
/// `Suspended`), releases the turn but not the VO lock, and sets a retry per
/// the policy. Reporting `Failed` (the shell flag `unfinished_turn_terminal`,
/// M4) makes the core treat the row as gone with the engine's finishing commit
/// — removing the entry, releasing every lock — for an invocation that has not
/// finished.
#[test]
fn unfinished_turn_is_failed_attempt() {
    let a = Answers {
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(RetryRow::watch(9, 1, 4))),
        journal_len: Some(4),
        policy: Some(Some(700)),
        ..Answers::default()
    };
    let ended = |outcome| Event::TurnEnded {
        turn: Turn::PLAIN,
        retry_count: 0,
        outcome,
    };
    let p = ask(
        idle(),
        ended(Outcome::Retry { delay_ms: None }),
        a,
        crate::Bugs::NONE,
    );
    assert_eq!(
        p.stage,
        StageChange::Move(Stage::Suspended),
        "keeps the queue entry"
    );
    assert_eq!(p.retry, RowChange::Set(RetryRow::retry(NOW + 700, 1, 4, 0)));
    assert_eq!(p.ram.release, Release::Running, "the VO lock remains");
    // The old mapping: a terminal outcome without the engine's finishing commit.
    let p = ask(idle(), ended(Outcome::Failed), a, crate::Bugs::NONE);
    assert_eq!(p.stage, StageChange::Remove, "terminal leaves the queue");
    assert_eq!(p.ram.release, Release::All);
}

/// **A failed turn whose finishing commit landed** (rule 7: an `Err` may have
/// written). The engine's `Completed`/`Failed` commit was applied — it removed the
/// queue entry and the retry row and wrote the reply — but returned an error
/// (failpoint `writer::commit::after_submit`), so the shell reports
/// `TurnFailed { AfterRunning }`. Must see: the core reads the status back and,
/// finding it terminal, settles like `TurnEnded` with a terminal outcome — VO lock
/// released (`Release::All`), no new retry row, the duty acked, streams sealed.
/// A status that is not terminal, or a status read that fails, keeps FL-4 (lock
/// kept, retry written). Before the fix (flag `ambiguous_finish_keeps_lock`):
/// the lock was kept and a retry row written for a finished invocation; the
/// retry cleared the row without freeing the lock, and the next call on the same
/// key waited for a leader change.
#[test]
fn failed_turn_whose_finish_landed_releases_the_lock() {
    let failed = Event::TurnFailed {
        turn: Turn::PLAIN,
        at: FailedAt::AfterRunning { retry_count: 0 },
        err: TurnErr::Other,
    };
    let a = |status, slot| Answers {
        status: Some(status),
        slot: Some(slot),
        retry: Some(None),
        journal_len: Some(4),
        policy: Some(Some(700)),
        ..Answers::default()
    };
    let before = crate::Bugs {
        ambiguous_finish_keeps_lock: true,
        ..crate::Bugs::NONE
    };
    for status in [Done, Fail] {
        // The landed commit removed the entry.
        let p = ask(idle(), failed, a(status, None), crate::Bugs::NONE);
        assert_eq!(p.need, None, "{status:?}");
        assert_eq!(p.ram.release, Release::All, "{status:?}: the lock is freed");
        assert_eq!(p.retry, RowChange::Keep, "{status:?}: no retry row");
        assert_eq!(p.ram.duty, crate::Duty::Ack, "{status:?}");
        assert_eq!(p.side, Some(Side::Seal(Seal::Terminal)), "{status:?}");
        let old = ask(idle(), failed, a(status, None), before);
        assert_eq!(
            old.ram.release,
            Release::Running,
            "before the fix {status:?}"
        );
        assert!(
            matches!(old.retry, RowChange::Set(_)),
            "before the fix {status:?}"
        );
    }
    // Not terminal: FL-4 as before.
    let p = ask(
        idle(),
        failed,
        a(Run, slot(Stage::Running)),
        crate::Bugs::NONE,
    );
    assert_eq!(p.ram.release, Release::Running);
    assert!(matches!(p.retry, RowChange::Set(_)), "{p:?}");
}

/// A **ticket's** turn with a pre-planned retry fails before rewriting the
/// row — reading the status at the gate after the permit fails
/// (`BeforeRunning`), or the `Running` commit returns an error before writing
/// (`AfterRunning`) — while the background tick has already moved its row into
/// a watch row due in `parked_defer_ms` (parked ticket). Must see: the row is
/// rewritten as `Retry` at the backoff step per `position` (keeping
/// `attempt`/`journal_len`/`fires`), the duty acked. Before the fix: the watch
/// row was kept for a day, the duty acked ⇒ nobody woke the invocation. Reading
/// the row fails ⇒ keep the row, the duty drops (rescue net). The row is
/// already `Retry` (the `Running` commit wrote it) ⇒ keep. The old behaviour is
/// pinned by the flag `planned_keeps_watch_row` (M4: `WAKE_BOUND`,
/// configuration `PARK_FAIL`).
#[test]
fn broken_ticket_turn_rewrites_deferred_watch_row() {
    let planned = Turn::planned(Planned {
        attempt: 2,
        next_due: 5_000,
        journal_len: 4,
    });
    let far = RetryRow {
        due: NOW + KNOBS.parked_defer_ms,
        kind: crate::RowKind::Watch,
        ..RetryRow::retry(0, 2, 4, 1)
    };
    let failed = |at| Event::TurnFailed {
        turn: planned,
        at,
        err: TurnErr::Other,
    };
    let ats = [
        FailedAt::BeforeRunning,
        FailedAt::AfterRunning { retry_count: 2 },
    ];
    let a = |row| Answers {
        status: Some(Run),
        slot: Some(slot(Stage::Running)),
        retry: Some(Some(row)),
        journal_len: Some(4),
        policy: Some(Some(700)),
        ..Answers::default()
    };
    #[rustfmt::skip]
    let rows: &[(&str, RetryRow, RowChange, crate::Duty)] = &[
        // (FL, current row, row written, duty)
        ("FL-16, FL-30", far, RowChange::Set(RetryRow::retry(NOW + 700, 2, 4, 1)), crate::Duty::Ack),
        ("FL-19", RetryRow::retry(5_000, 3, 4, 0), RowChange::Keep, crate::Duty::Ack),
    ];
    let before_1_3 = crate::Bugs {
        planned_keeps_watch_row: true,
        ..crate::Bugs::NONE
    };
    for (fl, row, want, duty) in rows {
        for at in ats {
            let p = ask(idle(), failed(at), a(*row), crate::Bugs::NONE);
            assert_eq!((p.retry, p.ram.duty), (*want, *duty), "{fl} {at:?} {row:?}");
            let old = ask(idle(), failed(at), a(*row), before_1_3);
            assert_eq!(
                (old.retry, old.ram.duty),
                (RowChange::Keep, crate::Duty::Ack),
                "before the fix {at:?} {row:?}"
            );
        }
    }
    // The row cannot be read: keep, the duty drops (FL-16).
    let v = idle()
        .with_slot(slot(Stage::Running))
        .with_read_error(Need::Retry, crate::ReadErr::Other);
    for at in ats {
        let p = run(&v, failed(at), crate::Bugs::NONE);
        assert_eq!(
            (p.retry, p.ram.duty),
            (RowChange::Keep, crate::Duty::Keep),
            "FL-16 {at:?}"
        );
    }
}
