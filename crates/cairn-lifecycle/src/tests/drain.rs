// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The outbox drainer and the early reply: `outbox_row_table` (22 rows),
//! `call_row_table` (17 rows), `table_202_429_and_permit_wait_flip` (2 rows) —
//! row for row, on the copy in [`crate::drain`]. Outbox row routing keeps FL-15
//! (a dead target holds only its own rows, a busy invocation holds all its
//! later rows); the early reply keeps FL-12 (only budget means 429 / giving up
//! the wait slice).

use crate::{
    drain::*,
    Status::{
        Completed as Done, Failed as Fail, Missing as Gone, Running as Run, Suspended as Susp,
    },
};

const PAUSE: crate::Status = crate::Status::Paused(crate::Pause::Sdk);

#[test]
fn table_202_429_and_permit_wait_flip() {
    #[rustfmt::skip]
    let rows: &[(bool, EarlyReply, bool)] = &[
        // budget: 429, the background turn gives up the wait slice
        (true,  EarlyReply::Throttled, true),
        // VO lock / turn cap / not picked up yet: 202, the background turn keeps waiting
        (false, EarlyReply::Waiting,   false),
    ];
    for (budget, reply, give_up) in rows {
        assert_eq!(decide_early_reply(*budget), *reply, "{budget}");
        assert_eq!(decide_give_up_slice(*budget), *give_up, "{budget}");
    }
}

#[test]
fn outbox_row_table() {
    use {RowAction::*, RowKind::*, RowLock::*};
    let v = |kind, remote, dest_down, lock| RowView {
        kind,
        remote,
        dest_down,
        lock,
    };
    #[rustfmt::skip]
    let rows: &[(RowView, RowAction)] = &[
        // Call: always lock the caller; the target is known only after dispatch
        (v(Call,   false, false, Untried),  TryLock),
        (v(Call,   false, false, Busy),     Leave),
        (v(Call,   false, false, Held),     HandleCall),
        // `remote` of a Call is never true (the target is known only after
        // dispatch); even if it were, the path would not change
        (v(Call,   true,  false, Held),     HandleCall),
        // dest_down applies only when the target is remote
        (v(Call,   false, true,  Held),     HandleCall),
        // remote target: no lock; a dead target holds the row
        (v(Attach, true,  false, Unneeded), Send),
        (v(Attach, true,  true,  Unneeded), Leave),
        (v(Peek,   true,  false, Unneeded), Send),
        (v(Peek,   true,  true,  Unneeded), Leave),
        (v(Reply,  true,  false, Unneeded), Send),
        (v(Reply,  true,  true,  Unneeded), Leave),
        (v(Cancel, true,  false, Unneeded), Send),
        (v(Cancel, true,  true,  Unneeded), Leave),
        // a dead target wins even when the lock is already held
        (v(Reply,  true,  true,  Held),     Leave),
        // local: lock, then resolve
        (v(Attach, false, false, Untried),  TryLock),
        (v(Attach, false, false, Busy),     Leave),
        (v(Attach, false, false, Held),     Local),
        (v(Peek,   false, false, Held),     Local),
        (v(Reply,  false, false, Held),     Local),
        (v(Cancel, false, false, Held),     Local),
        (v(Cancel, false, false, Busy),     Leave),
        (v(Cancel, false, true,  Held),     Local),
    ];
    for (view, want) in rows {
        assert_eq!(decide_row(view), *want, "{view:?}");
    }
}

#[test]
fn call_row_table() {
    use CallAction::*;
    let v = |awaits_result, caller, dispatch| CallView {
        awaits_result,
        caller,
        dispatch,
    };
    let d = |remote_down, created| {
        Some(Some(CallDispatch {
            remote_down,
            created,
        }))
    };
    #[rustfmt::skip]
    let rows: &[(CallView, CallAction)] = &[
        // ctx.call: read the caller first
        (v(true,  None,        None),              ReadCaller),
        (v(true,  Some(Done),  None),              DropStale),
        (v(true,  Some(Fail),  None),              DropStale),
        // the caller is alive (including paused, and missing): dispatch
        (v(true,  Some(Run),   None),              Dispatch),
        (v(true,  Some(Susp),  None),              Dispatch),
        (v(true,  Some(PAUSE), None),              Dispatch),
        (v(true,  Some(Gone),  None),              Dispatch),
        // ctx.send: do not read the caller, run even when the caller has finished
        (v(false, None,        None),              Dispatch),
        (v(false, Some(Done),  None),              Dispatch),
        // could not be created: the row stays where it is
        (v(true,  Some(Run),   Some(None)),        Leave),
        (v(false, None,        Some(None)),        Leave),
        // local callee: hand over the id; hand over the callee only when just created
        (v(true,  Some(Run),   d(None, true)),     DeliverLocal { queue_callee: true }),
        (v(true,  Some(Run),   d(None, false)),    DeliverLocal { queue_callee: false }),
        (v(false, None,        d(None, true)),     DeliverLocal { queue_callee: true }),
        // remote callee: send; if that range died in this drain pass, leave it
        (v(true,  Some(Run),   d(Some(false), true)),  SendRemote),
        (v(true,  Some(Run),   d(Some(false), false)), SendRemote),
        (v(false, None,        d(Some(true), true)),   Leave),
    ];
    for (view, want) in rows {
        assert_eq!(decide_call(view), *want, "{view:?}");
    }
}
