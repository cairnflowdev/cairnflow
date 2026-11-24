// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Creation and the admin paths: `create`, `cancel`, `pause_with`, `resume`, `restart`.

use {
    super::*,
    crate::{
        Event, Pause, Plan, Reply, RetryRow, RowChange, Seal, Side, Slot, Stage, StageChange,
        Status, Then, Turn, Vo,
    },
};

const SLOT: Option<Slot> = Some(Slot {
    stage: Stage::Suspended,
    exclusive: true,
});

#[test]
fn create_invocation() {
    // FL-14: a handler held in quarantine ⇒ created directly as `paused` 576 at `Inbox`.
    for (hold, status) in [
        (false, Status::Running),
        (true, Status::Paused(Pause::Quarantine)),
    ] {
        let p = both(&idle(), Event::Submitted { hold });
        assert_eq!(
            (p.status, p.stage),
            (Some(status), StageChange::Insert(Stage::Inbox)),
            "FL-5, FL-14"
        );
        assert_eq!(p.side, None, "creating as `paused` seals nothing");
    }
}

/// Admin cancel: one commit for the status, the retry row and the queue entry.
/// **Changed with the core** (suspicion D (1)): before the core the entry was
/// removed in a second commit (`release_queue_slot`).
#[test]
fn admin_cancel() {
    let a = Answers {
        slot: Some(SLOT),
        ..Answers::default()
    };
    for s in [
        Status::Running,
        Status::Suspended,
        Status::Paused(Pause::MaxAttempts),
    ] {
        let a = Answers {
            status: Some(s),
            ..a
        };
        let core = ask(idle(), Event::AdminCancel, a, crate::Bugs::NONE);
        assert_eq!(core.status, Some(Status::Failed), "FL-8 {s:?}");
        assert_eq!(
            (core.stage, core.retry, core.ram.vo),
            (StageChange::Remove, RowChange::Clear, Vo::Free),
            "suspicion D (1)"
        );
        assert_eq!(core.side, Some(Side::Seal(Seal::Terminal)));
        let prod = ask(idle(), Event::AdminCancel, a, crate::Bugs::BEFORE_STEP_3);
        assert_eq!(
            (prod.status, prod.stage, prod.retry, prod.then),
            (
                Some(Status::Failed),
                StageChange::Keep,
                RowChange::Clear,
                Then::Dequeue
            ),
            "before the core: two commits"
        );
        // the second commit
        let second = ask_both(idle(), Event::Dequeue, a);
        assert_eq!(
            (second.stage, second.ram.vo),
            (StageChange::Remove, Vo::Free),
            "FL-8"
        );
    }
    // cancelling again repairs an orphan entry
    for s in [Status::Completed, Status::Failed] {
        let p = ask_both(
            idle(),
            Event::AdminCancel,
            Answers {
                status: Some(s),
                ..a
            },
        );
        assert_eq!(
            (p.status, p.stage, p.then),
            (
                None,
                StageChange::Remove,
                Then::Reply(Reply::AlreadyFinished)
            ),
            "FL-8"
        );
        assert_eq!(p.side, None, "already sealed when it finished");
    }
    let p = ask_both(
        idle(),
        Event::AdminCancel,
        Answers {
            status: Some(Status::Missing),
            ..a
        },
    );
    assert_eq!(p, Plan::reply(Reply::NotFound));
}

/// `pause_with` (rescue net 577, poison inbound row 577): the stage stays as is
/// (suspicion C — harmless).
#[test]
fn pause_keeps_stage() {
    let reason = Pause::InfraStuck;
    #[rustfmt::skip]
    let rows: &[(&str, Status, Option<Status>, Option<Side>)] = &[
        ("FL-16", Status::Running,   Some(Status::Paused(reason)), Some(Side::Seal(Seal::Paused))),
        ("FL-15", Status::Suspended, Some(Status::Paused(reason)), Some(Side::Seal(Seal::Paused))),
        ("FL-15", Status::Paused(Pause::Sdk), Some(Status::Paused(reason)), None),
        ("—",     Status::Completed, None, None),
        ("—",     Status::Failed,    None, None),
        ("—",     Status::Missing,   None, None),
    ];
    for (fl, s, to, side) in rows {
        let p = both(&idle().with_status(*s), Event::Pause { reason });
        assert_eq!(
            (p.status, p.side, p.stage),
            (*to, *side, StageChange::Keep),
            "{fl} {s:?}"
        );
    }
}

/// `resume`: `paused → running`, the stage stays as is (suspicion C), then a
/// turn that counts again from the start.
#[test]
fn resume() {
    let p = both(
        &idle().with_status(Status::Paused(Pause::MaxAttempts)),
        Event::AdminResume,
    );
    assert_eq!(
        (p.status, p.stage, p.retry),
        (Some(Status::Running), StageChange::Keep, RowChange::Keep)
    );
    assert_eq!(
        p.then,
        Then::Drive(Turn {
            reset: true,
            ..Turn::PLAIN
        })
    );
    for s in [
        Status::Running,
        Status::Suspended,
        Status::Completed,
        Status::Failed,
    ] {
        assert_eq!(
            both(&idle().with_status(s), Event::AdminResume).then,
            Then::Reply(Reply::NotPaused),
            "{s:?}"
        );
    }
    assert_eq!(
        both(&idle().with_status(Status::Missing), Event::AdminResume).then,
        Then::Reply(Reply::NotFound)
    );
}

/// `restart`: the old run ends together with its stage and retry row
/// (**changed with the core**, suspicion D (2): before the core the row stayed
/// — harmless), returns the VO lock, creates the new run.
#[test]
fn restart() {
    let a = Answers {
        slot: Some(SLOT),
        ..Answers::default()
    };
    for s in [
        Status::Running,
        Status::Suspended,
        Status::Paused(Pause::Sdk),
    ] {
        let a = Answers {
            status: Some(s),
            ..a
        };
        let c = ask(idle(), Event::AdminRestart, a, crate::Bugs::NONE);
        assert_eq!(
            (c.status, c.stage, c.retry, c.ram.vo),
            (
                Some(Status::Completed),
                StageChange::Remove,
                RowChange::Clear,
                Vo::Free
            )
        );
        assert_eq!(
            (c.side, c.ram.spawn_successor),
            (Some(Side::Seal(Seal::Restarted)), true)
        );
        let p = ask(idle(), Event::AdminRestart, a, crate::Bugs::BEFORE_STEP_3);
        assert_eq!(p.retry, RowChange::Keep, "suspicion D (2) before the core");
    }
    // the old run has finished: only create the new run
    let p = ask_both(
        idle(),
        Event::AdminRestart,
        Answers {
            status: Some(Status::Completed),
            ..a
        },
    );
    assert_eq!(
        (p.status, p.side, p.ram.spawn_successor),
        (None, None, true)
    );
    let _ = RetryRow::legacy_watch(0);
}
