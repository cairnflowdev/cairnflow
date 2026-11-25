// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Leader changes and the rescue net: `restore_flow` (new rows), `recover`, and
//! `rescue_net_and_recover_table` (23 rows, `drainer.rs`).

use {
    super::*,
    crate::{
        Attendance, Event, Mark, Need, Pause, Plan, ReadErr, Reply, RetryRow, Slot, Stage,
        StageChange, Status, Turn, Vo,
    },
};

/// The actions of `decide_rescue` (the decision-table version).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RescueAction {
    ReadStatus,
    ReadPending,
    Skip,
    Later,
    Forget,
    Strike,
    Abort,
    PauseStuck,
    Requeue,
}

fn old_rescue(p: &Plan) -> RescueAction {
    use RescueAction::*;
    match p.need {
        Some(Need::Status) => return ReadStatus,
        Some(Need::Retry) => return ReadPending,
        Some(n) => panic!("{n:?}"),
        None => {}
    }
    if p.then == crate::Then::Reply(Reply::Abort) {
        return Abort;
    }
    if p.status == Some(Status::Paused(Pause::InfraStuck)) {
        assert_eq!(p.side, Some(crate::Side::Seal(crate::Seal::Paused)));
        return PauseStuck;
    }
    if let Some(t) = p.ram.handout {
        assert_eq!(t, Turn::PLAIN);
        return Requeue;
    }
    assert!(!p.commits());
    match p.ram.rescue {
        Mark::None => Skip,
        Mark::Later => Later,
        Mark::Forget => Forget,
        Mark::Strike => Strike,
        Mark::Clear => panic!("the reconciliation round does not break the chain"),
    }
}

/// `rescue_net_and_recover_table` row for row. `status: Ok(true)` = `running`;
/// `pending: Ok(true)` = there is a durable retry (watch rows not counted).
#[test]
fn rescue_net_and_recover_table() {
    use {ReadErr::*, RescueAction::*};
    type R = Option<Result<bool, ReadErr>>;
    let v = |attended: bool, marked: bool, stuck: bool, status: R, pending: R| {
        (attended, marked, stuck, status, pending)
    };
    #[rustfmt::skip]
    let rows: &[(&str, (bool, bool, bool, R, R), RescueAction)] = &[
        // someone is working: an id from a mark is reconsidered later, an id from
        // the scan is skipped
        ("FL-20", v(true,  true,  false, None,              None),              Later),
        ("FL-20", v(true,  true,  true,  Some(Ok(true)),    Some(Ok(false))),   Later),
        ("FL-20", v(true,  false, false, None,              Some(Ok(false))),   Skip),
        ("FL-20", v(true,  false, false, None,              None),              Skip),
        // an id from a mark: read the status first
        ("FL-16", v(false, true,  false, None,              None),              ReadStatus),
        ("FL-16", v(false, true,  false, Some(Ok(false)),   None),              Forget),
        ("FL-16", v(false, true,  true,  Some(Ok(false)),   None),              Forget),
        ("FL-16", v(false, true,  false, Some(Err(Fenced)), None),              Abort),
        ("FL-16", v(false, true,  false, Some(Err(Other)),  None),              Strike),
        ("FL-16", v(false, true,  false, Some(Ok(true)),    None),              ReadPending),
        // an id from the scan: already known to be running, do not read the status
        ("FL-16", v(false, false, false, None,              None),              ReadPending),
        // a durable retry
        ("FL-17", v(false, true,  false, Some(Ok(true)),    Some(Ok(true))),    Skip),
        ("FL-17", v(false, true,  true,  Some(Ok(true)),    Some(Ok(true))),    Skip),
        ("FL-17", v(false, false, false, None,              Some(Ok(true))),    Skip),
        ("FL-16", v(false, true,  false, Some(Ok(true)),    Some(Err(Fenced))), Abort),
        ("FL-16", v(false, false, false, None,              Some(Err(Fenced))), Abort),
        // no retry (or the read failed — running once too many beats forgetting)
        ("FL-16", v(false, true,  false, Some(Ok(true)),    Some(Ok(false))),   Requeue),
        ("FL-16", v(false, true,  false, Some(Ok(true)),    Some(Err(Other))),  Requeue),
        ("FL-16", v(false, false, false, None,              Some(Ok(false))),   Requeue),
        ("FL-16", v(false, false, false, None,              Some(Err(Other))),  Requeue),
        // over the threshold: paused 577 — only ids from a mark
        ("FL-16", v(false, true,  true,  Some(Ok(true)),    Some(Ok(false))),   PauseStuck),
        ("FL-16", v(false, true,  true,  Some(Ok(true)),    Some(Err(Other))),  PauseStuck),
        ("FL-16", v(false, false, true,  None,              Some(Ok(false))),   Requeue),
    ];
    assert_eq!(rows.len(), 23);
    for (fl, (attended, marked, stuck, status, pending), want) in rows {
        // `attended` = lock busy or a turn handed over — both ways mean busy.
        for at in [
            Attendance {
                lock_held: *attended,
                ..Attendance::IDLE
            },
            Attendance {
                queued: *attended,
                ..Attendance::IDLE
            },
        ] {
            let mut v = view(at);
            match status {
                None => {}
                Some(Ok(true)) => v.status = Some(Status::Running),
                Some(Ok(false)) => v.status = Some(Status::Suspended),
                Some(Err(e)) => v = v.with_read_error(Need::Status, *e),
            }
            match pending {
                None => {}
                Some(Ok(true)) => v.retry = Some(Some(RetryRow::retry(9, 1, 4, 0))),
                Some(Ok(false)) => v.retry = Some(None),
                Some(Err(e)) => v = v.with_read_error(Need::Retry, *e),
            }
            let p = both(
                &v,
                Event::RescueRound {
                    marked: *marked,
                    stuck: *stuck,
                },
            );
            assert_eq!(
                old_rescue(&p),
                *want,
                "{fl}: {:?}",
                (attended, marked, stuck, status, pending)
            );
        }
    }
    // A watch row is not a retry with a backoff step (FL-17): the rescue net and
    // `recover` do not wait for it.
    let v = idle().with_retry(Some(RetryRow::legacy_watch(9)));
    assert_eq!(
        old_rescue(&both(
            &v,
            Event::RescueRound {
                marked: false,
                stuck: false
            }
        )),
        Requeue,
        "FL-17"
    );
}

/// `recover`: one `running` invocation of the scan; the same rule as an
/// unmarked id (suspicion E: `suspended` does not run).
#[test]
fn recover_runs_only_running() {
    for s in [
        Status::Suspended,
        Status::Paused(Pause::Sdk),
        Status::Completed,
        Status::Failed,
        Status::Missing,
    ] {
        assert_eq!(
            both(&idle().with_status(s), Event::Recover),
            Plan::default(),
            "suspicion E {s:?}"
        );
    }
    let v = idle().with_status(Status::Running).with_retry(None);
    assert_eq!(
        both(&v, Event::Recover).ram.handout,
        Some(Turn::PLAIN),
        "FL-16"
    );
    // scanning the retry index failed ⇒ run (running once too many beats forgetting)
    let v = idle()
        .with_status(Status::Running)
        .with_read_error(Need::Retry, ReadErr::Other);
    assert_eq!(
        both(&v, Event::Recover).ram.handout,
        Some(Turn::PLAIN),
        "FL-16"
    );
}

/// `restore_flow` for one entry (FL-8).
#[test]
fn restore_flow_table() {
    let s = |stage, exclusive| Some(Slot { stage, exclusive });
    use {StageChange::*, Vo::Restore};
    #[rustfmt::skip]
    let rows: &[(&str, Option<Slot>, Option<Status>, StageChange, Vo)] = &[
        // (FL, entry, status (None = not read), stage, lock)
        // no entry: nothing
        ("FL-8",  None,                              None,                     Keep,                    Vo::Keep),
        // a locked queue: Running ⇒ Suspended, keep the lock
        ("locked queue", s(Stage::Running,   true),         Some(Status::Running),    Move(Stage::Suspended),  Restore),
        ("locked queue", s(Stage::Running,   true),         Some(Status::Suspended),  Move(Stage::Suspended),  Restore),
        ("FL-8",  s(Stage::Suspended, true),         Some(Status::Suspended),  Keep,                    Restore),
        ("FL-8",  s(Stage::Paused,    true),         Some(Status::Paused(Pause::Sdk)), Keep,            Restore),
        // suspicion C: a stage that disagrees with the status still keeps the lock
        ("FL-8",  s(Stage::Suspended, true),         Some(Status::Paused(Pause::InfraStuck)), Keep,     Restore),
        ("FL-8",  s(Stage::Paused,    true),         Some(Status::Running),    Keep,                    Restore),
        // an orphan entry of an invocation that stopped for good: clean up, not a lock
        ("FL-8",  s(Stage::Running,   true),         Some(Status::Completed),  Remove,                  Vo::Keep),
        ("FL-8",  s(Stage::Suspended, true),         Some(Status::Failed),     Remove,                  Vo::Keep),
        ("FL-8",  s(Stage::Paused,    true),         Some(Status::Missing),    Remove,                  Vo::Keep),
        // Inbox: holds nothing, does not read the status (even an orphan — suspicion D (1))
        ("FL-8",  s(Stage::Inbox,     true),         None,                     Keep,                    Vo::Keep),
        // a queue without a lock: Running ⇒ Inbox, do not read the status
        ("FL-8",  s(Stage::Running,   false),        None,                     Move(Stage::Inbox),      Vo::Keep),
        ("FL-8",  s(Stage::Suspended, false),        None,                     Keep,                    Vo::Keep),
    ];
    for (fl, slot, status, stage, vo) in rows {
        let mut v = idle().with_slot(*slot);
        if let Some(st) = status {
            v = v.with_status(*st);
        }
        let p = both(&v, Event::LeaderUp);
        assert_eq!(p.need, None, "{fl}: {slot:?} reads nothing more");
        assert_eq!(
            (p.stage, p.ram.vo),
            (*stage, *vo),
            "{fl}: {slot:?} {status:?}"
        );
    }
    // planted `f017_running_to_inbox`: a locked Running ⇒ Inbox, the lock is not rebuilt
    let bug = crate::Bugs {
        f017_running_to_inbox: true,
        ..crate::Bugs::NONE
    };
    let v = idle()
        .with_slot(s(Stage::Running, true))
        .with_status(Status::Running);
    let p = run(&v, Event::LeaderUp, bug);
    assert_eq!(
        (p.stage, p.ram.vo),
        (Move(Stage::Inbox), Vo::Keep),
        "planted f017_running_to_inbox"
    );
}
