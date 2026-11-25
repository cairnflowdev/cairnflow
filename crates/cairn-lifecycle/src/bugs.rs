// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flags that plant historical bugs into the core.
//!
//! Present only with the `mutations` feature (cairn-model enables it for M4)
//! and in this crate's own tests. The product build sees an empty [`Bugs`]:
//! every `bug!` branch compiles to `false` and is removed.
//!
//! Two kinds of flags, both **fixed bugs** — turned on, the core behaves like
//! the code before the fix; M4 must go red:
//! - historical bugs from before the core (FL-20, FL-21, suspicion A and the
//!   others below);
//! - product behaviour **from before the shell moved onto the core**, in the rows
//!   where the core chose the fixed behaviour (marked "changed with the core" in
//!   the tables) — each flag was turned off in the shell by a `test:`/`fix:`
//!   pair of that move; [`Bugs::BEFORE_STEP_3`] turns exactly these flags back
//!   on so the row-by-row tables pin the old behaviour as a historical bug.
//!
//! The product ([`Bugs::PRODUCTION`]) has been the core itself since that move:
//! the shell (`crates/cairn-server/src/turn/life.rs`) calls [`crate::step`],
//! with no flag.

#[cfg(any(test, feature = "mutations"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Bugs {
    // ── fixed bugs
    /// (before the fix that moves a `Running` entry holding the lock to
    /// `Suspended`): becoming leader sent an EXCLUSIVE queue's `Running` entry
    /// back to `Inbox` and did not rebuild the VO lock for it.
    pub f017_running_to_inbox: bool,
    /// (before the fix that releases only the permit): dropping a freshly
    /// granted permit (a throttle racing the grant) also released the VO lock
    /// held from before.
    pub f018_drop_held_lock: bool,
    /// (before the first fix, which refunds a throttled turn's attempt): the
    /// fire wrote `attempt + 1` before running; `Throttled` did not refund it.
    pub f023_count_at_fire: bool,
    /// (after that first fix, before attempts were counted only when a turn
    /// reaches `Running`): the fire wrote `attempt + 1`; only a turn
    /// returning `Throttled` was refunded — a turn cancelled while waiting for
    /// a permit was not.
    pub f023_refund_only_throttled: bool,
    /// **FL-20** (before the fix that stops background ticks dispatching a
    /// duplicate turn): the retry tick asked only the lock, not
    /// `Runner::is_queued`.
    pub fl20_lock_only: bool,
    /// **FL-21 regression** (after counting attempts only on `Running` and the
    /// FL-20 fix, before the backoff step counted every fire): the
    /// fire did not count `fires` — the backoff step stood still when the turn
    /// did not reach `Running`.
    pub fl21_no_fires: bool,
    /// **Suspicion A** (before the timer tick checked for a queued turn): a
    /// timer handed over a turn even when a handed-over turn had not run yet.
    pub a_timer_ignores_queued: bool,
    /// **Preflight `NotFound`** (fixed after a bug report): a service that no
    /// deployment declares ⇒ return 404 without writing a status; remove the
    /// queue entry, leave the watch row — `running` without an entry
    /// (`LiveButUnqueued`).
    pub notfound_keeps_running: bool,
    /// **Wait trees, "unknown" taken as "unsatisfied"** (injected, not a bug
    /// that shipped): the drainer has no wait tree (RAM lost, leader change) yet
    /// keeps `suspended` — the completion that satisfies the tree is swallowed,
    /// the caller is stuck forever.
    pub waiting_unknown_unsatisfied: bool,
    /// **Wait trees, evaluated without the new completion** (injected):
    /// evaluates the tree on the completion ledger **before** the completion
    /// that just arrived — the tree's last completion never wakes it.
    pub waiting_without_new: bool,
    /// **Due-time overflow** (found by fuzzing): the due time `now + delay` was
    /// added directly — a tenant-set delay (`next_retry_delay`, policy) near
    /// `u64::MAX` wrapped into the past (release build; debug panics). Only the
    /// row-by-row tables read it: M4 has no time domain (due times are not
    /// compared with a clock).
    pub due_wraps: bool,
    /// **Throttling keeps the old due time of a pre-planned retry**: a turn
    /// with a pre-planned retry that is `Throttled` kept the row as is — the due
    /// time is the handler's backoff step (possibly 1 s), so a suspended tenant
    /// cost up to ~330 commits/hour per invocation. The new rule is `due =
    /// max(row.due, now + after_ms)`. The row-by-row tables read it; M4 has no
    /// time domain.
    pub throttled_keeps_due: bool,
    /// **Refused tenant still calls the SDK** (found by a soak run): the tenant
    /// state was asked only when granting a permit. A turn granted before the
    /// suspension still called the SDK — even after tens of seconds waiting for
    /// durability/the engine, and every follow-up turn of the same run — after
    /// `PUT …/state suspended` had returned `204`: preflight ignored
    /// [`crate::View::tenant_refused_ms`], and the shell did not ask again right
    /// before the call (M4: the shell of `Pc::Ready`).
    pub refused_calls_sdk: bool,

    // ── product behaviour before the core (fixed when the shell moved onto it)
    /// The drainer's three "write then hand over" places handed over a turn
    /// unconditionally (the same shape as suspicion A) — changed with the core.
    pub drain_ignores_queued: bool,
    /// **Suspicion B**: `park_for_retry`/`schedule_after_error` wrote
    /// `attempt 1` for a turn that had not entered `Running` — changed with the
    /// core.
    pub b_count_before_running: bool,
    /// **The reverse of B**: the watch row carried `attempt` 0 — a `Drive`
    /// owner turn that entered `Running` and then died did not have its attempt
    /// counted — changed with the core.
    pub uncounted_watch: bool,
    /// A turn with a pre-planned retry whose journal grew since the fire (a
    /// wake-up arriving while the turn waited for a permit) still carried the
    /// fire's count and position — the attempt belonged to the old position
    /// (found by M4) — changed with the core.
    pub planned_stale_position: bool,
    /// Preflight `RetrySoon` (deployment not loaded yet) kept the attempt
    /// counted at `Running` although the SDK was not called — changed with the
    /// core.
    pub retry_soon_counts: bool,
    /// `retry_if`/`retry_after_error` ignored the handler's policy
    /// (`retryPolicyInitialInterval`) — changed with the core.
    pub policy_ignored: bool,
    /// **Suspicion D (1)**: an admin cancel removed the queue entry in a second
    /// commit — changed with the core.
    pub d1_cancel_two_commits: bool,
    /// **Suspicion D (2)**: `restart`/`continue_as_new` did not delete the
    /// retry row (harmless) — changed with the core.
    pub d2_restart_keeps_row: bool,

    // ── product behaviour before wait trees
    /// The drainer handed over a turn for a `suspended` invocation on **every**
    /// completion, without asking the wait tree — one SDK turn per completion
    /// (correct but slow).
    pub wake_ignores_waiting: bool,

    // ── injected bugs of the **shell** around durability: only M4 reads them,
    // the core has no branch for them (precedent: the two wait-tree flags above).
    /// The `200` of `/internal/inbound` goes out **before** the commit
    /// receiving the message is durable.
    pub ack_before_durable: bool,
    /// Calls the SDK before the `Running` commit (and every write before it) is
    /// durable.
    pub effect_before_durable: bool,
    /// Received-message dedup asks only the **durable** store, ignoring a
    /// message just received that is still in the non-durable tail.
    pub accept_checks_durable_only: bool,
    /// **A turn that does not finish is reported terminal** (found by fuzzing;
    /// shell, only M4 reads it): the SDK ends the turn without
    /// `End`/`Suspension`/`Error` — the engine commits nothing — yet
    /// `settle_turn` reports `Failed` to the core ⇒ the core removes the queue
    /// entry and releases the VO lock of an invocation that is still `running`
    /// (`LiveButUnqueued`). The product reports `Retry` (a failed attempt that
    /// can be retried).
    pub unfinished_turn_terminal: bool,

    // ── rebuilding VO locks on becoming leader: only M4 reads these, the core
    // has no branch for them — the shell's event order around
    // `Event::LeaderUp`.
    /// **Product behaviour before chunked restore** (correct but slow):
    /// `restore_flow` rebuilt every VO lock in **one** engine hold, before the
    /// first command (the old FL-8) — the range accepted no command until it
    /// was done. The product now rebuilds in chunks while serving commands,
    /// blocking only permit grants.
    pub restore_blocks_commands: bool,
    /// (injected, shell) The scheduler grants a permit **before** the lock
    /// rebuild has finished: another invocation takes the VO lock of a lock
    /// holder whose chunk has not been scanned yet.
    pub grant_before_restored: bool,
    /// (injected, shell) The rebuild chunk classifies entries in one engine
    /// hold and builds the RAM lock in the **next** hold (releasing the engine
    /// in between): a command slipping in between (a cancel) ends the
    /// invocation, and its lock is still rebuilt.
    pub restore_lock_after_release: bool,

    // ── the chunked drainer: only M4 reads these — the shell's event order
    // around the drainer's `Event::CompletionArrived`.
    /// (injected, shell) The drainer asks "is anyone working for the target"
    /// (lock, handed-over turn — `Attendance`) for a chunk's rows **before**
    /// releasing the engine between two chunks, then delivers the rows in the
    /// next hold by the old answer: a turn handed over in that window is not
    /// seen, and the wake-up hands over one more turn.
    pub drain_attendance_across_release: bool,

    // ── the permit-wait queue is data: only M4 reads these — the shell's event
    // order around a background turn's permit request.
    /// **Product behaviour before parked tickets** (correct but slow): a
    /// background turn waited for a permit in its task, **holding the
    /// invocation lock**; blocked by the budget, it gave up after a slice
    /// (`Event::Throttled`, a retry row) — every waiter went back to the engine
    /// on a tick. The product now: when blocked, the ticket sits in the
    /// scheduler's queue, with no task and no lock; granting a permit takes the
    /// invocation lock at the same time.
    pub permit_wait_in_task: bool,
    /// (injected, shell) A ticket is granted a permit while another task holds
    /// its invocation lock — and runs right away instead of handing it back
    /// (nack).
    pub parked_grant_ignores_lock: bool,
    /// (injected, shell) A grant that finds the invocation lock busy drops the
    /// ticket (the nack loses the id) instead of letting the ticket wait for the
    /// lock to be released.
    pub nack_drops_ticket: bool,
    /// (injected, shell) The ticket stays in the queue after the grant — it is
    /// granted a second time.
    pub parked_granted_twice: bool,
    /// (injected, shell) The turn of a freshly granted ticket uses the state
    /// read when the ticket was parked, without reading it again at the gate
    /// after the permit (`Event::PermitGranted`): the ticket of an invocation
    /// that finished or was cancelled while waiting still runs.
    pub parked_grant_skips_gate: bool,
    /// (injected, shell — found by M4 while building the parked-ticket shell) A
    /// new permit request for an invocation does **not** take over its parked
    /// ticket: the turn carrying a wake-up handed over while the ticket still
    /// held the lock (the gate) runs before the ticket, then the ticket — a
    /// pre-planned retry — counts the attempt again from 0 at the journal
    /// position the other turn already counted.
    pub adopt_keeps_ticket: bool,
    /// **Behaviour before far deferral of parked tickets** (correct but
    /// costly): the retry row of an invocation with a parked ticket was deferred
    /// like an ordinary busy one — `defer_ms` (10 s), one commit per ticket
    /// every 10 s, scaling with the number of tickets. Only the row-by-row
    /// tables read it (M4 has no time domain).
    pub parked_defer_short: bool,
    /// (injected) Deferring a parked ticket's row far out clears `fires` — the
    /// backoff step falls back after the next fire (FL-21).
    pub parked_defer_resets_fires: bool,

    // ── found in a pre-soak review: only M4 reads these — the shell's event
    // order around a granted ticket.
    /// **Product behaviour before the granted-ticket fix**: the permit of a
    /// granted ticket sat in the future of the spawned turn task, outside the
    /// scheduler's ledger — a new request for the same invocation (an external
    /// signal, a turn carrying a timer) took over the ticket without getting
    /// its permit, so it asked for **one more** permit ("already holds the
    /// lock"); the ticket's task returned its permit per `held_before = false`,
    /// `release_all` compared locks by id and released the VO lock the other
    /// permit was using.
    pub granted_out_of_ledger: bool,
    /// **Product behaviour before the failed-ticket fix** (core): a turn with
    /// a pre-planned retry that failed (`TurnFailed`, before or after
    /// `Running`) kept the row as is — even when the background tick had
    /// already moved it into a watch row due in a day because of a parked
    /// ticket. If the ticket's turn failed before rewriting the row, the
    /// `running` invocation was woken by nobody for a day (or until the next
    /// leader change).
    pub planned_keeps_watch_row: bool,
    /// **A failed turn whose finishing commit landed kept the VO lock** (chaos,
    /// failpoint `writer::commit::after_submit`; core table only — M4 leaves
    /// out commits that land and report an error): `TurnFailed { AfterRunning }`
    /// did not read the status back, so a turn whose `Completed`/`Failed`
    /// commit was applied but returned an error kept the lock (FL-4: "unknown
    /// where the turn stopped") and wrote a retry row; the retry found the
    /// invocation terminal and cleared the row without freeing the lock. The next
    /// call on the same virtual object key waited for the next leader change.
    pub ambiguous_finish_keeps_lock: bool,
}

#[cfg(any(test, feature = "mutations"))]
impl Bugs {
    pub const NONE: Bugs = Bugs {
        f017_running_to_inbox: false,
        f018_drop_held_lock: false,
        f023_count_at_fire: false,
        f023_refund_only_throttled: false,
        fl20_lock_only: false,
        fl21_no_fires: false,
        a_timer_ignores_queued: false,
        notfound_keeps_running: false,
        drain_ignores_queued: false,
        b_count_before_running: false,
        uncounted_watch: false,
        planned_stale_position: false,
        retry_soon_counts: false,
        policy_ignored: false,
        d1_cancel_two_commits: false,
        d2_restart_keeps_row: false,
        waiting_unknown_unsatisfied: false,
        waiting_without_new: false,
        due_wraps: false,
        throttled_keeps_due: false,
        refused_calls_sdk: false,
        wake_ignores_waiting: false,
        ack_before_durable: false,
        effect_before_durable: false,
        accept_checks_durable_only: false,
        unfinished_turn_terminal: false,
        restore_blocks_commands: false,
        grant_before_restored: false,
        restore_lock_after_release: false,
        drain_attendance_across_release: false,
        permit_wait_in_task: false,
        parked_grant_ignores_lock: false,
        nack_drops_ticket: false,
        parked_granted_twice: false,
        parked_grant_skips_gate: false,
        adopt_keeps_ticket: false,
        parked_defer_short: false,
        parked_defer_resets_fires: false,
        granted_out_of_ledger: false,
        planned_keeps_watch_row: false,
        ambiguous_finish_keeps_lock: false,
    };

    /// The product: since the shell moved onto the core it calls
    /// [`crate::step`] itself — no flag (`tests::production_is_the_core`).
    pub const PRODUCTION: Bugs = Bugs::NONE;

    /// The product **before** the core (the shell's own code, just before it
    /// moved onto [`crate::step`]): every "changed with the core" row back to
    /// the code of that time — so the tables and M4 pin the
    /// old behaviour as a historical bug.
    pub const BEFORE_STEP_3: Bugs = Bugs {
        drain_ignores_queued: true,
        b_count_before_running: true,
        uncounted_watch: true,
        planned_stale_position: true,
        retry_soon_counts: true,
        policy_ignored: true,
        d1_cancel_two_commits: true,
        d2_restart_keeps_row: true,
        throttled_keeps_due: true,
        ..Bugs::NONE
    };

    /// The product **before** wait trees: the drainer wakes on
    /// every completion — so the tables and M4 pin the old behaviour.
    pub const BEFORE_3G_C: Bugs = Bugs {
        wake_ignores_waiting: true,
        ..Bugs::NONE
    };

    /// The product **before** chunked restore: rebuild every VO lock before
    /// the first command — so M4 pins the old behaviour (correct but slow).
    pub const BEFORE_W5R: Bugs = Bugs {
        restore_blocks_commands: true,
        ..Bugs::NONE
    };

    /// The product **before** parked tickets: a background turn waits for a
    /// permit in its task, holding the invocation lock, and a throttled one
    /// schedules a retry — so M4 pins the old behaviour (correct but slow) and
    /// keeps covering the `Event::Throttled` path under interleavings.
    pub const BEFORE_W6Q: Bugs = Bugs {
        permit_wait_in_task: true,
        ..Bugs::NONE
    };
}

/// Product build: no flag.
#[cfg(not(any(test, feature = "mutations")))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Bugs;

#[cfg(not(any(test, feature = "mutations")))]
impl Bugs {
    pub(crate) const NONE: Bugs = Bugs;
}

/// `bug!(b.flag)` — a constant `false` when there are no flags.
macro_rules! bug {
    ($b:ident . $f:ident) => {{
        #[cfg(any(test, feature = "mutations"))]
        let on = $b.$f;
        #[cfg(not(any(test, feature = "mutations")))]
        let on = {
            let _ = $b;
            false
        };
        on
    }};
}
pub(crate) use bug;
