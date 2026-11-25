// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `Plan`: what the core tells the shell to do.
//!
//! Three parts, in exactly the order the shell carries them out:
//! 1. **one commit** — [`Plan::status`], [`Plan::stage`], [`Plan::retry`],
//!    [`Plan::wake`] are written together; empty means no commit;
//! 2. **RAM work** after the commit — [`Ram`]: hand over a turn, release a
//!    permit/lock, ack the wake duty, the rescue-net ledger;
//! 3. **side-resource effects** [`Side`] (a no-op today) and **the next step**
//!    [`Then`].
//!
//! Or: [`Plan::need`] — "read X as well, then ask again"; every other part is
//! then empty.

use crate::{codec::RetryRow, event::Turn, event::WakeKind, view::Stage, view::Status};

/// A [`crate::View`] cell the core needs read as well.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Need {
    Status,
    Slot,
    Retry,
    JournalLen,
    Contract,
    /// Backoff per the handler's policy for step `step` (one GET of entry 0).
    Policy {
        step: u32,
    },
    /// The wait tree of the last suspension, evaluated with the completion
    /// that just arrived ([`crate::Waiting`]): the shell loads the caller's
    /// machine (warm machine or journal).
    Waiting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum StageChange {
    #[default]
    Keep,
    /// Write a new entry (`Engine::create`, `enqueue_existing`) at this stage,
    /// with a new `seq`.
    Insert(Stage),
    /// Move the existing entry to this stage.
    Move(Stage),
    /// Leave the queue.
    Remove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum RowChange {
    #[default]
    Keep,
    /// Overwrite the entity row (one row per invocation); the old index row
    /// becomes a stale row if the due time changes.
    Set(RetryRow),
    /// Delete the entity row (`drop_retry` / `cancel_retry`).
    Clear,
}

/// Write the wake-up into the journal (in the same commit as the rest).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum WakeWrite {
    #[default]
    None,
    /// Write the notification and delete the source row (timer, outbox, inbound).
    Deliver(WakeKind),
    /// The invocation has stopped for good: only delete the source row
    /// (`discard_source`, `drop_timer`, `drop_outbox`).
    Discard,
}

/// What to release of the permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Release {
    #[default]
    Keep,
    /// Withdraw the waiting request (`Core::cancel`); there is nothing to release.
    Claim,
    /// Release the counter + turn cap, **keeping the VO lock** (`release_running`, FL-4).
    Running,
    /// Release everything, including the VO lock (`release_all`).
    All,
}

/// The VO lock in the scheduler's ledger, outside the permit path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Vo {
    #[default]
    Keep,
    /// Rebuild it for this invocation (`Scheduler::restore_lock`).
    Restore,
    /// Return it (`release_lock` / `release_locks_of`).
    Free,
}

/// The task's wake duty (`WakeDuty`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Duty {
    /// No ack: dropped unacked ⇒ the rescue net is marked (the default nack).
    #[default]
    Keep,
    Ack,
}

/// The rescue-net ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Mark {
    #[default]
    None,
    /// The turn ran to the end: the chain of marks is broken (`Rescue::clear`).
    Clear,
    /// Busy and coming from a mark: reconcile again after a backoff.
    Later,
    /// Reading the status failed: one more strike.
    Strike,
    /// No work left: forget the mark.
    Forget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Ram {
    /// Hand a turn to `Runner` (the task waits for the invocation lock, then
    /// raises [`crate::Event::Drive`]).
    pub handout: Option<Turn>,
    pub release: Release,
    pub vo: Vo,
    pub duty: Duty,
    pub rescue: Mark,
    /// Create the successor (`restart`, `continue_as_new`), then hand it over.
    pub spawn_successor: bool,
}

/// Why seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Seal {
    /// To `completed`/`failed`.
    Terminal,
    /// To `paused`.
    Paused,
    /// The old run of `restart`/`continue_as_new`.
    Restarted,
}

/// An effect on the invocation's side resources.
///
/// There are no side resources today, so the shell carries it out as a no-op.
/// It is named from the start so that streams (and blobs) behind a feature
/// gate close or mark attempts **through the core**, rather than becoming a
/// direct primitive call outside the shell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Seal(Seal),
    /// A new attempt starts (the SDK is about to be called): attempt number
    /// `attempt` (from 1) since the journal last grew.
    NewAttempt {
        attempt: u32,
    },
}

/// The answer to the call site (condensed — the shell builds the real
/// `InvokeOutcome` from the store).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Reply {
    #[default]
    Nothing,
    /// Already finished: return the durable outcome.
    Ended,
    Paused,
    Throttled,
    NotFound,
    NotPaused,
    AlreadyFinished,
    Cancelled,
    Resumed,
    Restarted,
    /// The turn's outcome.
    Outcome,
    /// The turn was cancelled or failed; the shell returns the original error.
    Failed,
    /// Lost the right to write: stop the loop.
    Abort,
}

/// The next step of the task holding the invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Then {
    #[default]
    Done,
    /// Replied; the task is done.
    Reply(Reply),
    /// Request a permit (`acquire_turn*`).
    RequestPermit,
    /// `Running` is committed: run preflight ([`crate::Event::TurnStarted`]).
    Preflight { retry_count: u32 },
    /// Call the SDK with `ctx.attempt = retry_count`.
    CallSdk { retry_count: u32 },
    /// Run the turn (same task, under the lock it holds) — `resume`.
    Drive(Turn),
    /// A second commit: [`crate::Event::Dequeue`] (only with injected suspicion D(1)).
    Dequeue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Plan {
    pub need: Option<Need>,
    pub status: Option<Status>,
    pub stage: StageChange,
    pub retry: RowChange,
    pub wake: WakeWrite,
    pub ram: Ram,
    pub side: Option<Side>,
    pub then: Then,
}

impl Plan {
    pub const fn need(n: Need) -> Self {
        Plan {
            need: Some(n),
            status: None,
            stage: StageChange::Keep,
            retry: RowChange::Keep,
            wake: WakeWrite::None,
            ram: Ram {
                handout: None,
                release: Release::Keep,
                vo: Vo::Keep,
                duty: Duty::Keep,
                rescue: Mark::None,
                spawn_successor: false,
            },
            side: None,
            then: Then::Done,
        }
    }

    pub fn reply(r: Reply) -> Self {
        Plan {
            then: Then::Reply(r),
            ..Plan::default()
        }
    }

    /// Whether the durable part has anything to commit.
    pub fn commits(&self) -> bool {
        self.status.is_some()
            || self.stage != StageChange::Keep
            || self.retry != RowChange::Keep
            || self.wake != WakeWrite::None
    }
}
