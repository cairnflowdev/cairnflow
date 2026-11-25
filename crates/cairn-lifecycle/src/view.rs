// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `View`: what the shell can read for **one** invocation before asking the core.
//!
//! Every durable cell is an `Option` meaning "not read yet" (the pattern of
//! `turn/decide.rs`): the core returns [`crate::Need`] for exactly the missing
//! cell, the shell reads it and asks again. That way the core forces no read
//! that its branch does not need.

use crate::{codec::RetryRow, plan::Need};

/// The durable status, condensed (carries no result or message).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Status {
    /// No invocation has this id.
    Missing,
    Running,
    Suspended,
    Completed,
    Failed,
    Paused(Pause),
}

impl Status {
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Completed | Status::Failed)
    }

    /// Exists and has not finished.
    pub fn is_live(self) -> bool {
        matches!(
            self,
            Status::Running | Status::Suspended | Status::Paused(_)
        )
    }

    pub fn is_paused(self) -> bool {
        matches!(self, Status::Paused(_))
    }
}

/// Why `paused` (the codes of `cairn_engine`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pause {
    /// 575 `MAX_ATTEMPTS_EXHAUSTED` — preflight ran out of `retryPolicyMaxAttempts`.
    MaxAttempts,
    /// 576 — created directly as `paused` while the handler is held in quarantine (FL-14).
    Quarantine,
    /// 577 `INFRA_STUCK` — the rescue net (`pause_stuck`) or a poison inbound
    /// row (`poison_row`).
    InfraStuck,
    /// The SDK asks to stop (e.g. 570 `JOURNAL_MISMATCH`).
    Sdk,
}

impl Pause {
    pub const fn code(self) -> Option<u32> {
        match self {
            Pause::MaxAttempts => Some(575),
            Pause::Quarantine => Some(576),
            Pause::InfraStuck => Some(577),
            Pause::Sdk => None,
        }
    }
}

/// The stage of the queue entry (`cairn_store::Stage`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    Inbox,
    Running,
    Suspended,
    Paused,
}

impl Stage {
    /// The stages that hold the VO lock (EXCLUSIVE queues only): every stage
    /// except `Inbox` (`restore_flow`).
    pub fn holds_lock(self) -> bool {
        self != Stage::Inbox
    }
}

/// The invocation's place in the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Slot {
    pub stage: Stage,
    /// The queue has a VO lock (`QueueName::lock_name().is_some()`).
    pub exclusive: bool,
}

/// The invocation's flow-control permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Permit {
    #[default]
    None,
    /// A `Claim` is waiting in the scheduler's queue.
    Waiting,
    /// The scheduler has granted it, the turn has not `begin` yet
    /// (`Permit::unstarted`). `held_before`: the invocation held the VO lock
    /// **before** this request (a lock holder that suspended and woke up) —
    /// dropping the permit now may only release the counter.
    Granted { held_before: bool },
    /// The turn has `begin` (`TurnPermit`).
    Started,
}

/// **The one "busy" question**: built in one place in the shell, the core
/// decides from it. Before the core there were five ways of asking.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Attendance {
    /// The invocation lock (`Locks`) is held by **another** task — a turn in
    /// flight or waiting for a permit. For the background tick: `try_invocation`
    /// missed.
    pub lock_held: bool,
    /// There is a turn **handed over but not finished** (`Runner::is_queued`):
    /// its task may not have taken the lock yet.
    pub queued: bool,
    pub permit: Permit,
    /// The invocation holds the VO lock in the scheduler's ledger.
    pub vo_held: bool,
    /// The invocation's handed-over turn is a **parked ticket** in the
    /// scheduler's queue (`Scheduler::is_parked`): no task, no lock; the ticket
    /// carries the wake duty until it is granted (its turn rewrites the retry
    /// row at `PermitGranted`) or taken over (the turn that takes it rewrites
    /// it). Implies `queued`.
    pub parked: bool,
}

impl Attendance {
    /// Someone is working for this invocation (FL-20): handing over another turn would duplicate it.
    pub fn busy(&self) -> bool {
        self.lock_held || self.queued
    }

    pub const IDLE: Attendance = Attendance {
        lock_held: false,
        queued: false,
        permit: Permit::None,
        vo_held: false,
        parked: false,
    };
}

/// The wait tree of the last suspension, as the shell sees it when the
/// drainer has a completion for a `suspended` invocation.
///
/// The engine remembers the `Suspension` tree of the last suspension **in
/// RAM** (removed when a turn starts, lost on a leader change); the shell
/// evaluates it (`cairn_runtime::suspension_satisfied`) on the caller
/// machine's ledger of delivered completions — before and after the
/// completion that just arrived. The tree is a hint: without it the answer is
/// "unknown", and the core wakes the invocation as it did before wait trees.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Waiting {
    /// No tree (RAM lost, not suspended in this term, a turn has started).
    Unknown,
    /// A tree: whether it is satisfied with the delivered completions
    /// (`before`), and once the completion that just arrived is added (`after`).
    Known { before: bool, after: bool },
}

/// The invocation's deployment (`turn::pinned_deployment`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pinned {
    Found,
    /// No deployment declares the service.
    Undeclared,
    /// Pinned to a deployment the node's registry has not loaded yet.
    NotLoaded,
}

/// `retryPolicyOnMaxAttempts`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OnMax {
    Pause,
    Kill,
}

/// The part of the handler's contract that preflight reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Contract {
    pub pinned: Pinned,
    /// `retryPolicyMaxAttempts` — meaningful only when `Found`.
    pub max_attempts: Option<u32>,
    pub on_max: OnMax,
}

/// The answer to [`Need::Policy`]: backoff per the handler's policy for step
/// `step` (`Contract::retry_delay_ms(step + 1)`); `None` = the handler declares none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Policy {
    pub step: u32,
    pub delay_ms: Option<u64>,
}

/// A failed read the core must know about (rescue net, `recover`, retry after an error).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadErr {
    /// Lost the right to write: the term is about to end.
    Fenced,
    Other,
}

/// Node parameters (not state).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Knobs {
    /// How far to defer a busy invocation's retry (`turn::busy_defer_ms`).
    pub defer_ms: u64,
    /// The watch row fires after this long (`turn::watch_after_ms` =
    /// `sdk_timeout` + `WATCH_MARGIN`).
    pub watch_after_ms: u64,
    /// How far to defer the retry row of an invocation with a **parked
    /// ticket** ([`PARKED_DEFER_MS`]) — the ticket rewrites the row when it is
    /// granted, so this is only a net in case the ticket is lost unnoticed.
    pub parked_defer_ms: u64,
}

/// The default of [`Knobs::parked_defer_ms`]: one day. Each parked ticket costs
/// at most one deferral commit per day, not one commit per `defer_ms` (bench
/// `permit_queue_retry_rows`).
pub const PARKED_DEFER_MS: u64 = 24 * 3_600_000;

impl Knobs {
    /// The node's defaults: defer 10 s (`DEFAULT_QUEUE_WAIT`), watch 60 + 5 s,
    /// parked tickets one day.
    pub const DEFAULT: Knobs = Knobs {
        defer_ms: 10_000,
        watch_after_ms: 65_000,
        parked_defer_ms: PARKED_DEFER_MS,
    };
}

/// What the shell can read for one invocation. A `None` cell = not read yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct View {
    pub now_ms: u64,
    pub knobs: Knobs,
    pub status: Option<Status>,
    /// `Some(None)` = no queue entry.
    pub slot: Option<Option<Slot>>,
    /// `Some(None)` = no retry row.
    pub retry: Option<Option<RetryRow>>,
    pub journal_len: Option<u32>,
    pub contract: Option<Contract>,
    pub policy: Option<Policy>,
    /// The wait tree of the last suspension ([`Waiting`]) — read only by the
    /// drainer, only when the invocation is `suspended`.
    pub waiting: Option<Waiting>,
    pub attendance: Attendance,
    /// The read just requested ([`Need`]) failed: the shell sets this cell
    /// instead of the data cell, on the paths where the core knows how to
    /// treat a read error (rescue net, `recover`, retry after an error). Other
    /// paths return the error with `?` as before.
    pub read_error: Option<(Need, ReadErr)>,
    /// The invocation's tenant is refused permits (`suspended`, FL-24) **at the
    /// moment the shell asks**: retry after this many ms; `None` = not refused.
    /// Only preflight ([`crate::Event::TurnStarted`]) reads it — the shell asks
    /// right before every SDK turn, after every wait of the run. Not a durable
    /// cell: the node's scheduler answers, there is no [`Need`].
    pub tenant_refused_ms: Option<u64>,
}

impl View {
    pub const fn new(now_ms: u64, knobs: Knobs, attendance: Attendance) -> Self {
        Self {
            now_ms,
            knobs,
            status: None,
            slot: None,
            retry: None,
            journal_len: None,
            contract: None,
            policy: None,
            waiting: None,
            attendance,
            read_error: None,
            tenant_refused_ms: None,
        }
    }

    /// The tenant is refused permits, retry after `after_ms` — see
    /// [`Self::tenant_refused_ms`].
    pub fn with_tenant_refused(mut self, after_ms: Option<u64>) -> Self {
        self.tenant_refused_ms = after_ms;
        self
    }

    pub fn with_status(mut self, s: Status) -> Self {
        self.status = Some(s);
        self
    }

    pub fn with_slot(mut self, s: Option<Slot>) -> Self {
        self.slot = Some(s);
        self
    }

    pub fn with_retry(mut self, r: Option<RetryRow>) -> Self {
        self.retry = Some(r);
        self
    }

    pub fn with_journal_len(mut self, n: u32) -> Self {
        self.journal_len = Some(n);
        self
    }

    pub fn with_contract(mut self, c: Contract) -> Self {
        self.contract = Some(c);
        self
    }

    pub fn with_policy(mut self, step: u32, delay_ms: Option<u64>) -> Self {
        self.policy = Some(Policy { step, delay_ms });
        self
    }

    pub fn with_waiting(mut self, w: Waiting) -> Self {
        self.waiting = Some(w);
        self
    }

    pub fn with_read_error(mut self, need: Need, e: ReadErr) -> Self {
        self.read_error = Some((need, e));
        self
    }

    /// The error of the read `need`, if the shell reported one.
    pub(crate) fn failed(&self, need: Need) -> Option<ReadErr> {
        self.read_error.filter(|(n, _)| *n == need).map(|(_, e)| e)
    }
}
