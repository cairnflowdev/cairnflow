// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The background tick: `fire_retries` (`fire_retries_table`, 20 rows), the
//! timer of `tick` (`tick_timer_table`, 13 rows), the drainer's "write then
//! hand over", and the historical flags (fire-time counting, FL-20, FL-21,
//! suspicion A).

use {
    super::*,
    crate::{
        backoff_ms, Attendance, Event, Need, Plan, Planned, RetryRow, RowChange, Status, Turn,
        Waiting, WakeKind, WakeWrite,
    },
    Status::{
        Completed as Done, Failed as Fail, Missing as Gone, Running as Run, Suspended as Susp,
    },
};

const PAUSE: Status = Status::Paused(crate::Pause::InfraStuck);

/// The actions of `decide_retry` (the decision-table version), read back from the `Plan`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryAction {
    ReadStatus,
    ReadJournalLen,
    ReadPolicy {
        step: u32,
    },
    Defer {
        until: u64,
    },
    Cancel,
    Fire {
        attempt: u32,
        next_due: u64,
        len: u32,
        fires: u32,
    },
}

/// The view of `decide_retry` (the decision-table version).
#[derive(Clone, Copy, Debug)]
struct RetryView {
    busy: bool,
    attempt: u32,
    fires: u32,
    len_at_schedule: u32,
    status: Option<Status>,
    len_now: Option<u32>,
    policy: Option<Option<u64>>,
}

/// The due time of the due row in every row (does not affect the decision).
const DUE: u64 = 900;

fn retry_event(w: &RetryView) -> Event {
    Event::RetryDue {
        row: RetryRow::retry(DUE, w.attempt, w.len_at_schedule, w.fires),
    }
}

fn answers(w: &RetryView) -> Answers {
    Answers {
        status: w.status,
        journal_len: w.len_now,
        policy: w.policy,
        ..Answers::default()
    }
}

fn old_retry(p: &Plan, w: &RetryView) -> RetryAction {
    match p.need {
        Some(Need::Status) => return RetryAction::ReadStatus,
        Some(Need::JournalLen) => return RetryAction::ReadJournalLen,
        Some(Need::Policy { step }) => return RetryAction::ReadPolicy { step },
        Some(n) => panic!("decide_retry does not read {n:?}"),
        None => {}
    }
    assert!(!p.commits() || p.status.is_none() && p.wake == WakeWrite::None);
    match (p.retry, p.ram.handout) {
        (RowChange::Clear, None) => RetryAction::Cancel,
        (RowChange::Set(r), None) => {
            // Deferring keeps the counts as they are (`defer_retries`).
            assert_eq!(
                (r.attempt, r.journal_len, r.fires),
                (w.attempt, w.len_at_schedule, w.fires)
            );
            RetryAction::Defer { until: r.due }
        }
        (RowChange::Set(r), Some(t)) => {
            assert_eq!(
                t,
                Turn::planned(Planned {
                    attempt: r.attempt,
                    next_due: r.due,
                    journal_len: r.journal_len
                }),
                "the turn carries exactly the pre-planned retry"
            );
            RetryAction::Fire {
                attempt: r.attempt,
                next_due: r.due,
                len: r.journal_len,
                fires: r.fires,
            }
        }
        other => panic!("not an action of decide_retry: {other:?}"),
    }
}

fn busy_view(busy: bool) -> View {
    view(Attendance {
        lock_held: busy,
        ..Attendance::IDLE
    })
}

/// `fire_retries_table` row for row. `now` = 1 000, defer 5 000, the row
/// carries `attempt` 3, length when scheduled 10; `fires` 0 except in the last
/// rows (FL-21).
#[test]
fn fire_retries_table() {
    let w = |fires, len_now, policy| RetryView {
        busy: false,
        attempt: 3,
        fires,
        len_at_schedule: 10,
        status: Some(Run),
        len_now,
        policy,
    };
    let v = |busy, status, len_now, policy| RetryView {
        busy,
        attempt: 3,
        fires: 0,
        len_at_schedule: 10,
        status,
        len_now,
        policy,
    };
    use RetryAction::*;
    #[rustfmt::skip]
    let rows: &[(&str, RetryView, RetryAction)] = &[
        // busy: defer, read nothing (even when the other cells are present)
        ("FL-5",  v(true,  None,       None,     None),           Defer { until: 6_000 }),
        ("FL-5",  v(true,  Some(Run),  Some(10), Some(None)),     Defer { until: 6_000 }),
        // idle: read the status first
        ("FL-10", v(false, None,       None,     None),           ReadStatus),
        // stopped for good / gone / paused: cancel the row, read nothing more
        ("FL-10", v(false, Some(Gone), None,     None),           Cancel),
        ("FL-10", v(false, Some(Done), None,     None),           Cancel),
        ("FL-10", v(false, Some(Fail), None,     None),           Cancel),
        ("FL-10", v(false, Some(PAUSE),None,     None),           Cancel),
        // live: read the journal length (suspended fires too — suspicion E)
        ("FL-19", v(false, Some(Run),  None,     None),           ReadJournalLen),
        ("FL-19", v(false, Some(Susp), None,     None),           ReadJournalLen),
        // no progress: keep the count 3; with progress: count again from 0
        ("FL-19", v(false, Some(Run),  Some(10), None),           ReadPolicy { step: 3 }),
        ("FL-19", v(false, Some(Run),  Some(9),  None),           ReadPolicy { step: 3 }),
        ("FL-19", v(false, Some(Run),  Some(11), None),           ReadPolicy { step: 0 }),
        // the handler's policy beats the default backoff
        ("FL-19", v(false, Some(Run),  Some(10), Some(Some(700))), Fire { attempt: 3, next_due: 1_700, len: 10, fires: 1 }),
        ("FL-19", v(false, Some(Susp), Some(11), Some(Some(700))), Fire { attempt: 0, next_due: 1_700, len: 11, fires: 1 }),
        // no policy: backoff_ms(attempt)
        ("FL-19", v(false, Some(Run),  Some(10), Some(None)),     Fire { attempt: 3, next_due: 1_000 + backoff_ms(3), len: 10, fires: 1 }),
        ("FL-19", v(false, Some(Run),  Some(11), Some(None)),     Fire { attempt: 0, next_due: 1_000 + backoff_ms(0), len: 11, fires: 1 }),
        // FL-21: uncounted fires push the backoff step (and the policy step) up,
        // keeping the attempt count; with progress both go back to 0
        ("FL-21", w(2, Some(10), None),                           ReadPolicy { step: 5 }),
        ("FL-21", w(2, Some(10), Some(None)),                     Fire { attempt: 3, next_due: 1_000 + backoff_ms(5), len: 10, fires: 3 }),
        ("FL-21", w(2, Some(11), None),                           ReadPolicy { step: 0 }),
        ("FL-21", w(2, Some(11), Some(None)),                     Fire { attempt: 0, next_due: 1_000 + backoff_ms(0), len: 11, fires: 1 }),
    ];
    assert_eq!(rows.len(), 20, "all 20 rows of the decision table");
    for (fl, w, want) in rows {
        let p = ask_both(busy_view(w.busy), retry_event(w), answers(w));
        assert_eq!(old_retry(&p, w), *want, "{fl}: {w:?}");
    }
}

/// FL-20: busy also means a turn **handed over but not finished**
/// (`Runner::is_queued`) — a new row (the decision table merged both halves
/// into `busy`).
#[test]
fn fl20_delivered_turn_counts_as_busy() {
    let row = RetryRow::retry(DUE, 3, 10, 0);
    let queued = view(Attendance {
        queued: true,
        ..Attendance::IDLE
    });
    let p = both(&queued, Event::RetryDue { row });
    assert_eq!(
        p.retry,
        RowChange::Set(RetryRow { due: 6_000, ..row }),
        "FL-20: defer, do not hand over"
    );
    assert_eq!(p.ram.handout, None);
    // Planted FL-20: ask only the lock ⇒ read the status, then hand over a second turn.
    let bug = crate::Bugs {
        fl20_lock_only: true,
        ..crate::Bugs::NONE
    };
    let p = ask(
        queued,
        Event::RetryDue { row },
        Answers {
            status: Some(Run),
            journal_len: Some(10),
            policy: Some(None),
            ..Answers::default()
        },
        bug,
    );
    assert!(
        p.ram.handout.is_some(),
        "planted FL-20 hands over a duplicate"
    );
}

/// **Parked ticket**: the retry row of an invocation with a parked ticket is
/// deferred **far** into a watch row — one commit per ticket (not per
/// `defer_ms`), the next term's `recover` does not wait for it; keeps
/// `attempt`/`journal_len`/`fires`. An EXCLUSIVE queue's `Inbox` entry is
/// deferred as usual (`valid`: a watch row there marks a turn that has entered
/// `Running`).
#[test]
fn parked_ticket_row_deferred_into_watch_row() {
    let row = RetryRow::retry(DUE, 3, 10, 2);
    let parked = view(Attendance {
        queued: true,
        parked: true,
        ..Attendance::IDLE
    });
    let slot = |exclusive, stage| crate::Slot { stage, exclusive };
    // The core asks for the queue entry first.
    let p = run(&parked, Event::RetryDue { row }, crate::Bugs::NONE);
    assert_eq!(p.need, Some(Need::Slot));
    let far = RowChange::Set(RetryRow {
        due: NOW + KNOBS.parked_defer_ms,
        kind: crate::RowKind::Watch,
        ..row
    });
    let short = RowChange::Set(RetryRow { due: 6_000, ..row });
    for (s, want) in [
        (Some(slot(false, crate::Stage::Inbox)), far),
        (Some(slot(true, crate::Stage::Suspended)), far),
        (Some(slot(true, crate::Stage::Running)), far),
        (None, far),
        (Some(slot(true, crate::Stage::Inbox)), short),
    ] {
        let v = parked.with_slot(s);
        let p = run(&v, Event::RetryDue { row }, crate::Bugs::NONE);
        assert_eq!(p.retry, want, "{s:?}");
        assert_eq!(
            p.ram.handout, None,
            "a parked ticket is someone working: no hand-over"
        );
        // The behaviour before far deferral: deferred like an ordinary busy one.
        let old = crate::Bugs {
            parked_defer_short: true,
            ..crate::Bugs::NONE
        };
        assert_eq!(run(&v, Event::RetryDue { row }, old).retry, short);
    }
    // The far watch row still carries the counted attempts and the fires: the
    // next fire is at the right step.
    let RowChange::Set(deferred) = far else {
        unreachable!()
    };
    let p = ask_both(
        idle(),
        Event::RetryDue { row: deferred },
        Answers {
            status: Some(Run),
            journal_len: Some(10),
            policy: Some(None),
            ..Answers::default()
        },
    );
    assert_eq!(
        p.retry,
        RowChange::Set(RetryRow::retry(1_000 + backoff_ms(5), 3, 10, 3)),
        "FL-21: step attempt + fires"
    );
    // Planted: deferral clears `fires`.
    let bug = crate::Bugs {
        parked_defer_resets_fires: true,
        ..crate::Bugs::NONE
    };
    let v = parked.with_slot(None);
    assert!(matches!(
        run(&v, Event::RetryDue { row }, bug).retry,
        RowChange::Set(RetryRow { fires: 0, .. })
    ));
}

/// A watch row is deferred into an ordinary retry (`defer_retries` writes 13
/// bytes with flag 0) — kept as before the core; FL-17.
#[test]
fn watch_row_deferred_into_plain_retry() {
    let row = RetryRow::legacy_watch(DUE);
    let p = both(&busy_view(true), Event::RetryDue { row });
    assert_eq!(
        p.retry,
        RowChange::Set(RetryRow::retry(6_000, 0, u32::MAX, 0))
    );
    // Fire: an old watch row (`u32::MAX`) never "makes progress" ⇒ step 0.
    let p = ask_both(
        idle(),
        Event::RetryDue { row },
        Answers {
            status: Some(Run),
            journal_len: Some(7),
            policy: Some(None),
            ..Answers::default()
        },
    );
    assert_eq!(
        p.retry,
        RowChange::Set(RetryRow::retry(1_000 + backoff_ms(0), 0, 7, 1)),
        "FL-17: a watch row fires like a retry"
    );
}

/// Planted fire-time counting bugs and the FL-21 regression on the fire.
#[test]
fn fire_under_historical_flags() {
    let row = RetryRow::retry(DUE, 2, 10, 1);
    let a = Answers {
        status: Some(Run),
        journal_len: Some(10),
        policy: Some(None),
        ..Answers::default()
    };
    let e = Event::RetryDue { row };
    #[rustfmt::skip]
    let rows: &[(&str, crate::Bugs, RetryRow)] = &[
        // FL-19 + FL-21: the count stays, fires +1, step 3
        ("FL-19, FL-21", crate::Bugs::NONE, RetryRow::retry(1_000 + backoff_ms(3), 2, 10, 2)),
        // fire-time counting: the attempt is counted right at the fire
        ("count at fire", crate::Bugs { f023_count_at_fire: true, ..crate::Bugs::NONE },
                        RetryRow::retry(1_000 + backoff_ms(3), 3, 10, 2)),
        ("refund only throttled", crate::Bugs { f023_refund_only_throttled: true, ..crate::Bugs::NONE },
                        RetryRow::retry(1_000 + backoff_ms(3), 3, 10, 2)),
        // FL-21 regression: the fire is not counted
        ("FL-21",       crate::Bugs { fl21_no_fires: true, ..crate::Bugs::NONE },
                        RetryRow::retry(1_000 + backoff_ms(3), 2, 10, 1)),
    ];
    for (fl, bugs, want) in rows {
        let p = ask(idle(), e, a, *bugs);
        assert_eq!(p.retry, RowChange::Set(*want), "{fl}");
        // The turn always carries the count from **before** the fire (`ctx.attempt`).
        assert_eq!(
            p.ram.handout.and_then(|t| t.planned).map(|p| p.attempt),
            Some(2),
            "{fl}"
        );
    }
}

/// The actions of `decide_timer` (the decision-table version).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerAction {
    HandOff,
    ReadStatus,
    Drop,
    Deliver { run: bool },
}

fn old_timer(p: &Plan) -> TimerAction {
    if p.need == Some(Need::Status) {
        return TimerAction::ReadStatus;
    }
    assert_eq!(p.need, None);
    match p.wake {
        WakeWrite::None => {
            assert_eq!(p.ram.handout, Some(Turn::woken(WakeKind::Completion)));
            TimerAction::HandOff
        }
        WakeWrite::Discard => {
            assert_eq!(p.ram.handout, None);
            TimerAction::Drop
        }
        WakeWrite::Deliver(WakeKind::Completion) => {
            if let Some(t) = p.ram.handout {
                assert_eq!(
                    t,
                    Turn::PLAIN,
                    "completion written, the turn carries nothing"
                );
            }
            TimerAction::Deliver {
                run: p.ram.handout.is_some(),
            }
        }
        w => panic!("{w:?}"),
    }
}

/// `tick_timer_table` row for row.
#[test]
fn tick_timer_table() {
    let v = |busy, status| (busy, false, status);
    let q = |status| (false, true, status);
    use TimerAction::*;
    #[rustfmt::skip]
    let rows: &[(&str, (bool, bool, Option<Status>), TimerAction)] = &[
        ("FL-20", v(true,  None),        HandOff),
        ("FL-20", v(true,  Some(Done)),  HandOff),
        ("FL-10", v(false, None),        ReadStatus),
        ("FL-10", v(false, Some(Gone)),  Drop),
        ("FL-10", v(false, Some(Done)),  Drop),
        ("FL-10", v(false, Some(Fail)),  Drop),
        // paused: the completion is still written (the gate holds the turn back)
        ("FL-10", v(false, Some(PAUSE)), Deliver { run: true }),
        ("FL-10", v(false, Some(Run)),   Deliver { run: true }),
        ("FL-10", v(false, Some(Susp)),  Deliver { run: true }),
        // FL-20: a turn already handed over but not run ⇒ write, do not hand over another
        ("FL-20", q(None),               ReadStatus),
        ("FL-20", q(Some(Done)),         Drop),
        ("FL-20", q(Some(Susp)),         Deliver { run: false }),
        ("FL-20", q(Some(Run)),          Deliver { run: false }),
    ];
    assert_eq!(rows.len(), 13, "all 13 rows of the decision table");
    for (fl, (busy, queued, status), want) in rows {
        let at = view(Attendance {
            lock_held: *busy,
            queued: *queued,
            ..Attendance::IDLE
        });
        let a = Answers {
            status: *status,
            ..Answers::default()
        };
        let p = ask_both(at, Event::TimerDue { in_flight: false }, a);
        assert_eq!(
            old_timer(&p),
            *want,
            "{fl}: busy {busy} queued {queued} {status:?}"
        );
        // `stage_wake`: a suspended invocation that gets a wake-up can run.
        if let (Deliver { .. }, Some(Susp)) = (want, status) {
            assert_eq!(p.status, Some(Run), "{fl}");
        }
    }
}

/// A timer already handed to a task (`queue_timer` returned `false`) is not
/// handed over again; with suspicion A planted the timer hands over a turn even
/// though a turn is waiting.
#[test]
fn delivered_timer_and_suspect_a() {
    let busy = busy_view(true);
    let p = both(&busy, Event::TimerDue { in_flight: true });
    assert_eq!(p, Plan::default(), "FL-20: timer in flight");
    let queued = view(Attendance {
        queued: true,
        ..Attendance::IDLE
    })
    .with_status(Susp);
    let bug = crate::Bugs {
        a_timer_ignores_queued: true,
        ..crate::Bugs::NONE
    };
    let p = run(&queued, Event::TimerDue { in_flight: false }, bug);
    assert_eq!(
        p.ram.handout,
        Some(Turn::PLAIN),
        "suspicion A: duplicate hand-over"
    );
}

/// The drainer's "write then hand over" (`deliver_to_caller`,
/// `settle_signal_local`, `deliver_inbound_completion`): like a timer, except
/// that when busy the row stays. **Changed with the core**: before it, the turn
/// was handed over unconditionally (the same shape as suspicion A).
#[test]
fn drainer_writes_then_delivers() {
    for wake in [WakeKind::Completion, WakeKind::Signal] {
        // busy: no write, no hand-over
        let p = both(&busy_view(true), Event::CompletionArrived { wake });
        assert!(!p.commits() && p.ram.handout.is_none(), "FL-10");
        // stopped for good: drop the source row
        for s in [Gone, Done, Fail] {
            let p = both(&idle().with_status(s), Event::CompletionArrived { wake });
            assert_eq!(p.wake, WakeWrite::Discard, "FL-10 {s:?}");
        }
        // live: write; suspended ⇒ running (suspended: the wait tree is unknown)
        for s in [Run, Susp, PAUSE] {
            let v = idle().with_status(s).with_waiting(Waiting::Unknown);
            let p = both(&v, Event::CompletionArrived { wake });
            assert_eq!(p.wake, WakeWrite::Deliver(wake));
            assert_eq!(p.status, (s == Susp).then_some(Run));
            assert_eq!(p.ram.handout, Some(Turn::PLAIN));
        }
        // FL-20 (changed with the core): a turn handed over but not run ⇒ only write
        let queued = view(Attendance {
            queued: true,
            ..Attendance::IDLE
        })
        .with_status(Susp)
        .with_waiting(Waiting::Unknown);
        let core = run(
            &queued,
            Event::CompletionArrived { wake },
            crate::Bugs::NONE,
        );
        let prod = run(
            &queued,
            Event::CompletionArrived { wake },
            crate::Bugs::BEFORE_STEP_3,
        );
        assert_eq!(core.ram.handout, None, "core: FL-20");
        assert_eq!(
            prod.ram.handout,
            Some(Turn::PLAIN),
            "before the core: duplicate hand-over"
        );
        assert_eq!(core.wake, prod.wake);
    }
}

/// **Changed with wait trees.** The drainer has a completion for a `suspended`
/// invocation: the core asks for the last suspension's wait tree
/// (`Need::Waiting`); knowing for sure the completion does not satisfy the tree
/// ⇒ write the notification, **keep** `suspended`, hand over no turn. Satisfied
/// or unknown ⇒ as before. It does not ask when the invocation is not
/// suspended, for a signal, or when the lock is busy. Before wait trees
/// (`Bugs::BEFORE_3G_C`): wake on every completion. Two planted flags leave the
/// caller stuck (`m4.rs::matrix`): "unknown" taken as "unsatisfied", evaluated
/// without the new completion.
#[test]
fn drainer_wakes_only_when_wait_tree_satisfied() {
    use crate::Bugs;
    let susp = idle().with_status(Susp);
    let c = WakeKind::Completion;
    let arrive = Event::CompletionArrived { wake: c };
    // Suspended, tree not read ⇒ ask to read exactly that cell.
    assert_eq!(run(&susp, arrive, Bugs::NONE).need, Some(Need::Waiting));
    // Not suspended, a signal, busy ⇒ do not ask for the tree.
    for s in [Run, PAUSE] {
        assert_eq!(run(&idle().with_status(s), arrive, Bugs::NONE).need, None);
    }
    let sig = Event::CompletionArrived {
        wake: WakeKind::Signal,
    };
    assert_eq!(run(&susp, sig, Bugs::NONE).need, None);
    assert_eq!(run(&busy_view(true), arrive, Bugs::NONE).need, None);

    let stay =
        |p: &Plan| p.wake == WakeWrite::Deliver(c) && p.status.is_none() && p.ram.handout.is_none();
    let wakes = |p: &Plan| {
        p.wake == WakeWrite::Deliver(c)
            && p.status == Some(Run)
            && p.ram.handout == Some(Turn::PLAIN)
    };
    let not_yet = susp.with_waiting(Waiting::Known {
        before: false,
        after: false,
    });
    let now = susp.with_waiting(Waiting::Known {
        before: false,
        after: true,
    });
    let unknown = susp.with_waiting(Waiting::Unknown);
    // The core (= the product).
    assert!(
        stay(&run(&not_yet, arrive, Bugs::NONE)),
        "not satisfied: stay suspended"
    );
    assert!(wakes(&run(&now, arrive, Bugs::NONE)), "satisfied: wake");
    assert!(wakes(&run(&unknown, arrive, Bugs::NONE)), "unknown: wake");
    // A turn handed over but not run (FL-20): not satisfied still only writes.
    let queued = view(Attendance {
        queued: true,
        ..Attendance::IDLE
    })
    .with_status(Susp)
    .with_waiting(Waiting::Known {
        before: false,
        after: false,
    });
    assert!(stay(&run(&queued, arrive, Bugs::NONE)));
    // Before wait trees: every completion wakes, the tree is not asked.
    for v in [susp, not_yet, now, unknown] {
        assert!(wakes(&run(&v, arrive, Bugs::BEFORE_3G_C)), "{v:?}");
    }
    // Planted flags.
    let unknown_bug = Bugs {
        waiting_unknown_unsatisfied: true,
        ..Bugs::NONE
    };
    assert!(
        stay(&run(&unknown, arrive, unknown_bug)),
        "unknown ⇒ swallowed"
    );
    let without_new = Bugs {
        waiting_without_new: true,
        ..Bugs::NONE
    };
    assert!(
        stay(&run(&now, arrive, without_new)),
        "without the new completion ⇒ swallowed"
    );
}
