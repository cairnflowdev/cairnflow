// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `valid(view)`: the table of valid states, including suspicion C's three
//! mismatched pairs (valid for a reason) and the ✘ pairs.

use crate::{valid, Invalid, Pause, RetryRow, Slot, Stage, Status, View};

fn v(status: Status, slot: Option<(Stage, bool)>, retry: Option<RetryRow>) -> View {
    super::idle()
        .with_status(status)
        .with_slot(slot.map(|(stage, exclusive)| Slot { stage, exclusive }))
        .with_retry(retry)
}

#[test]
fn valid_state_table() {
    use {Invalid::*, Status::*};
    let p3 = Some(RetryRow::retry(9, 1, 4, 0));
    let p3_uncounted = Some(RetryRow::retry(9, 0, 4, 1));
    let w = Some(RetryRow::watch(9, 1, 4));
    let legacy_w = Some(RetryRow::legacy_watch(9));
    let pause = Paused(Pause::InfraStuck);
    let x = |s| Some((s, true));
    let n = |s| Some((s, false));
    #[rustfmt::skip]
    let rows: &[(&str, View, Result<(), Invalid>)] = &[
        // running
        ("FL-17 uncovered", v(Running, x(Stage::Inbox), None),           Ok(())),
        ("suspicion B",     v(Running, x(Stage::Inbox), p3_uncounted),   Ok(())),
        ("FL-8",            v(Running, n(Stage::Inbox), w),              Ok(())),
        ("FL-8",            v(Running, n(Stage::Inbox), p3),             Ok(())),
        ("FL-17",           v(Running, x(Stage::Running), w),            Ok(())),
        ("FL-17",           v(Running, x(Stage::Running), p3),           Ok(())),
        ("FL-17",           v(Running, x(Stage::Running), None),         Err(RunningWithoutWake)),
        ("FL-4",            v(Running, x(Stage::Suspended), None),       Ok(())),
        ("FL-4",            v(Running, x(Stage::Suspended), w),          Ok(())),
        ("FL-4",            v(Running, x(Stage::Suspended), p3),         Ok(())),
        ("suspicion C k2",  v(Running, x(Stage::Paused), None),          Ok(())),
        ("—",               v(Running, None, None),               Err(LiveButUnqueued)),
        // The `Inbox` entry of a locked queue carries the marks of a turn that has
        // entered `Running`.
        ("locked queue",    v(Running, x(Stage::Inbox), w),              Err(EnteredButUnlocked)),
        ("locked queue",    v(Running, x(Stage::Inbox), legacy_w),       Err(EnteredButUnlocked)),
        ("locked queue",    v(Running, x(Stage::Inbox), p3),             Err(EnteredButUnlocked)),
        ("locked queue",    v(Suspended, x(Stage::Inbox), None),         Err(EnteredButUnlocked)),
        // suspended
        ("FL-4",            v(Suspended, x(Stage::Suspended), None),     Ok(())),
        ("suspicion E",     v(Suspended, x(Stage::Suspended), w),        Ok(())),
        ("FL-17",           v(Suspended, x(Stage::Running), w),          Ok(())),
        ("FL-8",            v(Suspended, n(Stage::Inbox), None),         Ok(())),
        ("—",               v(Suspended, x(Stage::Paused), None),        Err(SuspendedButPausedStage)),
        // paused
        ("FL-4",            v(pause, x(Stage::Paused), None),            Ok(())),
        ("suspicion C k1",  v(pause, x(Stage::Suspended), None),         Ok(())),
        ("suspicion C k3",  v(Paused(Pause::Quarantine), x(Stage::Inbox), None), Ok(())),
        ("FL-4",            v(pause, x(Stage::Running), w),              Ok(())),
        ("—",               v(pause, x(Stage::Paused), p3),              Ok(())),
        // finished: no entry, no row (the target of the core)
        ("FL-17",           v(Completed, None, None),             Ok(())),
        ("FL-17",           v(Failed, None, None),                Ok(())),
        ("—",               v(Missing, None, None),               Ok(())),
        ("suspicion D (1)", v(Failed, x(Stage::Inbox), None),            Err(EndedButQueued)),
        ("suspicion D (1)", v(Failed, n(Stage::Suspended), None),        Err(EndedButQueued)),
        ("FL-8",            v(Completed, x(Stage::Suspended), None),     Err(EndedButQueued)),
        ("suspicion D (2)", v(Completed, None, w),                Err(EndedButScheduled)),
        ("—",               v(Missing, x(Stage::Inbox), None),           Err(EndedButQueued)),
    ];
    for (fl, view, want) in rows {
        assert_eq!(valid(view), *want, "{fl}: {view:?}");
    }
    assert_eq!(valid(&super::idle()), Err(Unread));
}
