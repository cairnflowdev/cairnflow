// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The decisions of the outbox drainer and of the permit-waiting turn — copied
//! verbatim from `turn/decide.rs` so their row-by-row table moves here complete.
//! They are not lifecycle state: they route an outbox row (`decide_row`,
//! `decide_call`) or choose 202/429 (`decide_early_reply`,
//! `decide_give_up_slice`). The shell was wired to these together with the core.
//!
//! The one difference from the original: the caller's status is
//! [`crate::Status`] instead of the server's `StatusKind`.

use crate::Status;

/// The client path's early reply when no permit arrives within `QUEUE_GRACE`
/// (`turn::drive_client`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EarlyReply {
    /// `202`: waiting for the VO lock or the turn cap — ordinary serialization.
    Waiting,
    /// `429`: the tenant is out of budget (counter or rate).
    Throttled,
}

/// `budget_blocked`: the scheduler is blocking the queue **because of budget**.
pub fn decide_early_reply(budget_blocked: bool) -> EarlyReply {
    if budget_blocked {
        EarlyReply::Throttled
    } else {
        EarlyReply::Waiting
    }
}

/// A background turn's permit-wait slice has run out
/// (`turn::acquire_turn_background`): `true` = give the turn up (schedule a
/// retry — [`crate::Event::Throttled`]). Only budget is worth giving up for —
/// for the VO lock or the turn cap, giving up makes nobody run sooner.
pub fn decide_give_up_slice(budget_blocked: bool) -> bool {
    budget_blocked
}

/// Outbox row kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowKind {
    Call,
    Attach,
    Peek,
    Reply,
    Cancel,
}

/// The local invocation lock the row needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowLock {
    /// The local part of the work writes to no invocation in this range.
    Unneeded,
    /// Needed, not tried yet.
    Untried,
    Held,
    /// Currently held (or the id was already busy earlier in this drain pass).
    Busy,
}

#[derive(Clone, Copy, Debug)]
pub struct RowView {
    pub kind: RowKind,
    /// The target is in another range, known without doing anything
    /// (`turn::remote_destination`); `Call` is always `false` — its target is
    /// known only after `dispatch_call`.
    pub remote: bool,
    /// Sending to that target already failed in this drain pass.
    pub dest_down: bool,
    pub lock: RowLock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowAction {
    /// The row stays where it is; the next pass retries.
    Leave,
    /// Try (without waiting) the lock of the row's local invocation.
    TryLock,
    /// A `Call` row: [`decide_call`].
    HandleCall,
    /// Send through the transport; forget the row once the other side is durable.
    Send,
    /// Resolve locally.
    Local,
}

/// Where an outbox row goes (`turn::drain_outbox`). A dead target holds only
/// its own rows; a busy invocation holds all its later rows (order is kept).
pub fn decide_row(v: &RowView) -> RowAction {
    if v.remote && v.dest_down {
        return RowAction::Leave;
    }
    match v.lock {
        RowLock::Untried => return RowAction::TryLock,
        RowLock::Busy => return RowAction::Leave,
        RowLock::Unneeded | RowLock::Held => {}
    }
    match (v.kind, v.remote) {
        (RowKind::Call, _) => RowAction::HandleCall,
        (_, true) => RowAction::Send,
        (_, false) => RowAction::Local,
    }
}

/// The callee of a `Call` row after `dispatch_call`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallDispatch {
    /// `None` = the callee is in this range; `Some(down)` = in another range,
    /// `down` if sending to that range already failed in this drain pass.
    pub remote_down: Option<bool>,
    /// This dispatch created the callee (rather than attaching to an existing one).
    pub created: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct CallView {
    /// The caller declared a result completion (`ctx.call`, not `ctx.send`).
    pub awaits_result: bool,
    /// `None` = not read yet.
    pub caller: Option<Status>,
    /// `None` = not dispatched yet; `Some(None)` = could not be created (the
    /// service is not yet in this node's registry).
    pub dispatch: Option<Option<CallDispatch>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallAction {
    ReadCaller,
    /// The caller has stopped for good while waiting for the result: nobody
    /// will read the result — drop the row, do not create the callee.
    DropStale,
    Dispatch,
    Leave,
    /// Hand the id to the caller and delete the row in the same commit; hand
    /// the callee to the scheduler if this pass created it.
    DeliverLocal {
        queue_callee: bool,
    },
    /// Send `StartCall`; hand the id to the caller only once the other side is durable.
    SendRemote,
}

pub fn decide_call(v: &CallView) -> CallAction {
    if v.awaits_result {
        match v.caller {
            None => return CallAction::ReadCaller,
            Some(Status::Completed | Status::Failed) => return CallAction::DropStale,
            Some(_) => {}
        }
    }
    let Some(dispatch) = v.dispatch else {
        return CallAction::Dispatch;
    };
    let Some(d) = dispatch else {
        return CallAction::Leave;
    };
    match d.remote_down {
        None => CallAction::DeliverLocal {
            queue_callee: d.created,
        },
        Some(true) => CallAction::Leave,
        Some(false) => CallAction::SendRemote,
    }
}
