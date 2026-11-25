// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `Event`: what just happened to an invocation, as the shell sees it.
//!
//! Each variant matches one state-transition site of the product; the "today"
//! column in each variant's doc is the product function that the shell
//! replaced with one call to [`crate::step`].

/// The wake-up that comes with a turn (`turn::Wake`): a completion (timer,
/// `ctx.call` result, inbound) or a signal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WakeKind {
    Completion,
    Signal,
}

/// The next retry that [`Event::RetryDue`] planned **before** the turn
/// (`PlannedRetry` today): once the turn enters `Running` it is rewritten with
/// `attempt + 1` (FL-19).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Planned {
    /// Attempts counted at fire time — the turn's `ctx.attempt`.
    pub attempt: u32,
    pub next_due: u64,
    pub journal_len: u32,
}

/// A turn the shell is running: replaces `(RetryOwner, retry_count, pending)`.
///
/// There is no `RetryOwner` any more: every turn follows the same attempt
/// rules. They differ only in whether the turn has a pre-planned retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Turn {
    /// The wake-up the turn carries (a timer handed to a task because the
    /// invocation was busy, an external signal). With it the journal will
    /// grow ⇒ retries are counted from 0 again.
    pub wake: Option<WakeKind>,
    /// A turn handed over by a retry fire.
    pub planned: Option<Planned>,
    /// `resume`: the operator lifted a pause ⇒ count again from the start.
    pub reset: bool,
}

impl Turn {
    pub const PLAIN: Turn = Turn {
        wake: None,
        planned: None,
        reset: false,
    };

    pub const fn planned(p: Planned) -> Self {
        Turn {
            wake: None,
            planned: Some(p),
            reset: false,
        }
    }

    pub const fn woken(w: WakeKind) -> Self {
        Turn {
            wake: Some(w),
            planned: None,
            reset: false,
        }
    }
}

/// The outcome the SDK reports for the turn (`InvokeOutcome` today, the part the SDK decides).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Completed,
    Failed,
    /// The SDK asks to stop (`Failed{action: Pause}`, 570…).
    Paused,
    /// Suspended, waiting for a durable event.
    Waiting,
    /// A retryable error; `delay_ms` = the SDK's `next_retry_delay`.
    Retry {
        delay_ms: Option<u64>,
    },
    /// `ctx.continueAsNew` — the old run ends, a new one is created.
    ContinuedAsNew,
}

impl Outcome {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Outcome::Completed | Outcome::Failed | Outcome::ContinuedAsNew
        )
    }
}

/// The engine's finishing commit in `Engine::step` (`engine/effects.rs::
/// apply_finish`), in the same commit as the SDK's last journal entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Finish {
    Completed,
    Failed,
    Paused,
    /// `satisfied`: what the SDK waits for is already in the journal ⇒ `Running` right away.
    Suspended {
        satisfied: bool,
    },
    /// `Failed{action: Retry}` ⇒ `Running` (the retry the turn writes when it ends).
    Retry,
    /// `Engine::restart(keep 1)` of `continue_as_new`.
    ContinuedAsNew,
}

/// Where the turn failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FailedAt {
    /// Before the `Running` commit (`ensure_queued` failed — `schedule_after_error`).
    BeforeRunning,
    /// After the `Running` commit (`drive_with_permit`, the `Err` branch).
    AfterRunning { retry_count: u32 },
}

/// The turn's error, condensed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TurnErr {
    /// Lost the right to write (`ActorError::is_fenced`).
    Fenced,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// Today `Engine::create` (from `turn::start_invocation`). `hold`: the
    /// handler is held in quarantine ⇒ created directly as `paused` 576 (FL-14).
    Submitted { hold: bool },
    /// A retry row is due — today `turn::fire_retries` (`decide_retry`).
    RetryDue { row: crate::RetryRow },
    /// A timer is due — today the timer branch of `turn::tick`
    /// (`decide_timer`). `in_flight`: this timer has already been handed to a
    /// task (`Runner::queue_timer` returned `false`).
    TimerDue { in_flight: bool },
    /// The drainer has a completion/signal for a local invocation — today
    /// `deliver_to_caller`, `settle_signal_local`,
    /// `deliver_inbound_completion` ("write then hand over").
    CompletionArrived { wake: WakeKind },
    /// The task has taken the invocation lock and starts the turn — today the
    /// start of `drive_untraced`: `gate_turn` (`decide_gate`) then `ensure_queued`.
    Drive { turn: Turn },
    /// The scheduler grants a permit — today `drive_with_permit` up to
    /// `begin_turn`: `gate_turn` a second time, then the `Running` commit.
    PermitGranted { turn: Turn },
    /// The permit wait ran out because of budget — today the `None` branch of
    /// `acquire_turn_background` → `park_for_retry` (+ the `Throttled`
    /// post-processing of `drive_retry`).
    Throttled { turn: Turn, after_ms: u64 },
    /// The task was cancelled while waiting for a permit (`JoinSet::shutdown`,
    /// `Claim::drop`, `Permit::unstarted`). No shell code runs besides `Drop` —
    /// the core only decides what to release.
    PermitCancelled { turn: Turn },
    /// The turn is in `Running`, about to call the SDK — today the preflight of
    /// `turn::drive_one` (`decide_preflight`).
    TurnStarted { turn: Turn, retry_count: u32 },
    /// The engine's finishing commit — today `apply_finish`.
    Finished { finish: Finish },
    /// The turn ran to the end — today `retry_if` + `end_turn_settle`
    /// (`decide_settle`) + the post-processing of `drive_retry`.
    TurnEnded {
        turn: Turn,
        retry_count: u32,
        outcome: Outcome,
    },
    /// The turn threw an error — today `schedule_after_error` / the `Err`
    /// branch of `drive_with_permit` (`retry_after_error`).
    TurnFailed {
        turn: Turn,
        at: FailedAt,
        err: TurnErr,
    },
    /// Start of a term, for one queue entry — today one chunk of
    /// `restore_flow` (`Engine::restore_chunk`, chunked).
    LeaderUp,
    /// The `running` scan — today `actor::recover`.
    Recover,
    /// An id in the reconciliation round — today `rescue_round` (`decide_rescue`).
    RescueRound { marked: bool, stuck: bool },
    /// Leave the queue and return the VO lock — today `release_queue_slot`.
    Dequeue,
    /// `turn::cancel` (`Engine::cancel` = `fail_with` 572).
    AdminCancel,
    /// `Engine::pause_with` from `pause_stuck` / `poison_row`. There is no admin
    /// `pause` endpoint today.
    Pause { reason: crate::Pause },
    /// `turn::resume`.
    AdminResume,
    /// `turn::restart` (`Engine::restart`).
    AdminRestart,
}
