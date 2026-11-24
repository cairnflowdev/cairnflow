// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `valid(view)` — the set of valid durable states of an invocation.
//!
//! This is the **target** set — every finishing path removes the queue entry
//! and the retry row in the same commit as the status, so `completed`/`failed`
//! have no entry or row left, not even transiently. Older products could still
//! produce a few ✘ pairs (marked "today" in the individual rules); the testkit
//! turns the check on only once the shell is on the core.
//!
//! Columns: status, queue stage, retry row (P = a retry row, W = a watch row).
//!
//! | Status | Stage | Row | Why valid |
//! |---|---|---|---|
//! | `running` | `Inbox` | —, P | just created; throttled, or `ensure_queued` failed before `Running` |
//! | `running` | `Inbox` (no lock) | W | a leader change in the middle of a turn of a queue without a lock |
//! | `running` | `Running` | W or P | a turn in flight (FL-17) |
//! | `running` | `Suspended` | —, W, P | `stage_wake`; a failed turn with a retry scheduled; a leader change in the middle of a turn with a lock |
//! | `running` | `Paused` | — | `resume` committed, `begin_turn` not yet |
//! | `suspended` | `Suspended`, `Running` | —, W, P | suspended; between `apply_finish` and `end_turn`; a leftover row |
//! | `suspended` | `Inbox` (no lock) | — / P / W | a leader change in the middle of a turn of a queue without a lock |
//! | `paused` | any stage | —, P, W | `apply_finish` Pause; `pause_stuck`/`poison_row` keep the stage; quarantine 576 + `Inbox`; the background tick cancels the row at its fire |
//! | `completed`/`failed`/absent | none | none | every finishing path |

use crate::{
    codec::RowKind,
    view::{Stage, Status, View},
};

/// Why a view is not valid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Invalid {
    /// The view lacks the `status`, `slot` or `retry` cell.
    Unread,
    /// The invocation has stopped for good (or is gone) yet still has a queue
    /// entry — an orphan entry (suspicion D (1); older products: death between
    /// the two commits of an admin cancel or a preflight `Kill`).
    EndedButQueued,
    /// Stopped for good yet still has a retry row (older products:
    /// `restart`/`continue_as_new`, suspicion D (2) — harmless, the background
    /// tick cancels it at its fire).
    EndedButScheduled,
    /// Live yet without a queue entry (older products: data from before flow
    /// control, rebuilt by `ensure_queued`; preflight `NotFound`).
    LiveButUnqueued,
    /// `suspended` at stage `Paused` — no path produces it.
    SuspendedButPausedStage,
    /// Stage `Running` without a durable wake-up (FL-17).
    RunningWithoutWake,
    /// An EXCLUSIVE queue's `Inbox` entry carries the marks of a turn that has
    /// entered `Running` — it read state without holding the VO lock.
    EnteredButUnlocked,
}

/// Whether the durable state in `view` belongs to the valid set.
pub fn valid(view: &View) -> Result<(), Invalid> {
    let (Some(status), Some(slot), Some(retry)) = (view.status, view.slot, view.retry) else {
        return Err(Invalid::Unread);
    };
    if !status.is_live() {
        if slot.is_some() {
            return Err(Invalid::EndedButQueued);
        }
        if retry.is_some() {
            return Err(Invalid::EndedButScheduled);
        }
        return Ok(());
    }
    let Some(slot) = slot else {
        return Err(Invalid::LiveButUnqueued);
    };
    if status == Status::Suspended && slot.stage == Stage::Paused {
        return Err(Invalid::SuspendedButPausedStage);
    }
    if slot.stage == Stage::Running && retry.is_none() {
        return Err(Invalid::RunningWithoutWake);
    }
    if slot.exclusive && slot.stage == Stage::Inbox {
        // The marks of a turn that has entered `Running`: suspended, a watch
        // row, or a counted attempt. A locked queue's entry leaves `Inbox` one
        // way only.
        let entered = status == Status::Suspended
            || retry.is_some_and(|r| r.kind == RowKind::Watch || r.attempt > 0);
        if entered {
            return Err(Invalid::EnteredButUnlocked);
        }
    }
    Ok(())
}
