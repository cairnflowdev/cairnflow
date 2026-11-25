// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cancel bell of a running turn.
//!
//! Cooperative cancellation ([`cancel_signal`](super::cancel_signal)) delivers the CANCEL
//! signal through the invocation lock — and the running turn holds that lock until it
//! stops: a whole `BIDI_STREAM` session, a whole chain of `REQUEST_RESPONSE` turns resolved
//! in place (waiting for the ack of each `ctx.run`). So a cancel that arrives mid-way only
//! reaches the SDK once the work is done — a chat turn runs to the end and spends tokens to
//! the end.
//!
//! This registry tells the running turn that someone asked to cancel it. The turn delivers
//! CANCEL into its own journal — at the turn boundary (`drive_one`) and right while waiting
//! on the SDK (`drive_bidi`) — and the normal path of `cancel_signal` arrives later, when the
//! lock is released, sees the signal identity already present and does not deliver it again
//! (the state machine deduplicates). The bell is RAM only: if it is lost (leader change, the
//! turn just stopped) the normal path still delivers — the bell only makes it arrive early.
//!
//! One range, one thread (actor `spawn_local`): `RefCell`/`Cell`, no locks.

use {
    super::*,
    std::{
        cell::{Cell, RefCell},
        collections::BTreeMap,
        rc::Rc,
    },
    tokio::sync::Notify,
};

/// Running turns of the range, by invocation.
#[derive(Default)]
pub struct LiveTurns {
    turns: RefCell<BTreeMap<InvocationId, Rc<Bell>>>,
}

#[derive(Default)]
struct Bell {
    asked: Cell<bool>,
    rung: Notify,
}

impl LiveTurns {
    /// A turn of `id` starts; the bell is removed when the guard drops. The invocation lock
    /// guarantees at most one turn of `id` at a time.
    pub(super) fn enter(&self, id: InvocationId) -> LiveTurn<'_> {
        let bell = Rc::new(Bell::default());
        self.turns.borrow_mut().insert(id, Rc::clone(&bell));
        LiveTurn {
            owner: self,
            id,
            bell,
        }
    }

    /// Someone asks to cancel `id`. `false` = no turn of `id` is running.
    pub(super) fn ask_cancel(&self, id: &InvocationId) -> bool {
        let Some(bell) = self.turns.borrow().get(id).cloned() else {
            return false;
        };
        bell.asked.set(true);
        bell.rung.notify_one();
        true
    }
}

/// Bell of one running turn.
pub(super) struct LiveTurn<'a> {
    owner: &'a LiveTurns,
    id: InvocationId,
    bell: Rc<Bell>,
}

impl LiveTurn<'_> {
    /// Take the cancel request (if any) — each request once.
    pub(super) fn take_cancel(&self) -> bool {
        self.bell.asked.replace(false)
    }

    /// Wait until there is a cancel request — **without** taking it
    /// ([`take_cancel`](Self::take_cancel)).
    /// Safe to cancel mid-way (`select!`).
    pub(super) async fn rung(&self) {
        while !self.bell.asked.get() {
            self.bell.rung.notified().await;
        }
    }
}

impl Drop for LiveTurn<'_> {
    fn drop(&mut self) {
        let mut turns = self.owner.turns.borrow_mut();
        if turns
            .get(&self.id)
            .is_some_and(|b| Rc::ptr_eq(b, &self.bell))
        {
            turns.remove(&self.id);
        }
    }
}

/// The CANCEL signal as `cancel_signal` delivers it — same identity, so the second delivery
/// (normal path, after the lock is released) is a no-op of the machine.
pub(super) fn cancel_notification() -> pb::SignalNotificationMessage {
    use pb::signal_notification_message::{Result as R, SignalId as S};
    pb::SignalNotificationMessage {
        signal_id: Some(S::Idx(pb::BuiltInSignal::Cancel as u32)),
        result: Some(R::Void(pb::Void {})),
    }
}
