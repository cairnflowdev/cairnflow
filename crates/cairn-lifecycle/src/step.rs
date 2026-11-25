// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The transition table: `step(view, event) -> Plan`.
//!
//! Merges `decide_retry`, `decide_timer`, `decide_gate`, `decide_preflight`,
//! `decide_settle` (`turn/decide.rs`), `decide_rescue` (`drainer.rs`), the
//! `attempt`/`fires` rules of `retry_if`/`retry_after_error`/`park_for_retry`/
//! `RetryOwner::counted_at_start`, the post-processing of `drive_retry`, the
//! classification of `restore_flow`, `apply_finish`, and the admin paths.
//!
//! **One meaning for an attempt** — where the core differs from the product
//! before the shell moved onto it, each difference a flag of
//! `Bugs::BEFORE_STEP_3` (the product since that move is the core itself):
//! - an attempt is counted only at the `Running` commit ([`begin`]), for
//!   **every** turn: a turn with a pre-planned retry rewrites that row with
//!   `attempt + 1` (as before); other turns write a watch row **carrying**
//!   `attempt + 1` and the journal length (before: 0 and `u32::MAX` — a turn
//!   that died after `Running` was not counted);
//! - a turn that has not reached `Running` (throttled, `ensure_queued`
//!   failed) spends no attempt, it only counts one fire (`fires + 1`, FL-21);
//! - preflight `RetrySoon` refunds the attempt counted (the SDK was not called);
//! - the `retry_count` (`ctx.attempt`) of a turn without a pre-planned retry
//!   comes from the durable row ([`position`]), not the constant 0 of the
//!   `Drive` owner;
//! - a retry after an error follows the handler's policy;
//! - a turn with a pre-planned retry whose journal grew since the fire counts
//!   again at the new position (found by M4; before: it kept the fire's count
//!   and position).
//!
//! Two more places M4 found in the core's own first version (not in the
//! product): the watch row records the position **after** the wake-up that the
//! `Running` commit writes; a throttled turn carrying a wake-up does not count
//! the not-yet-written wake-up as progress.

use crate::{
    bugs::{bug, Bugs},
    codec::{RetryRow, RowKind},
    event::{Event, FailedAt, Finish, Outcome, Planned, Turn, TurnErr, WakeKind},
    plan::{
        Duty, Mark, Need, Plan, Release, Reply, RowChange, Seal, Side, StageChange, Then, Vo,
        WakeWrite,
    },
    view::{OnMax, Pause, Permit, Pinned, ReadErr, Stage, Status, View, Waiting},
};

/// The default backoff step when the handler declares no policy (`turn::RETRY_BASE_MS`).
pub const RETRY_BASE_MS: u64 = 100;
/// The default backoff cap (`turn::RETRY_CAP_MS`).
pub const RETRY_CAP_MS: u64 = 60_000;
/// Preflight `RetrySoon`: retry after this long (`turn::drive_one`).
pub const RETRY_SOON_MS: u64 = 1_000;

/// The default backoff for step `step` (`turn::backoff_ms`).
pub fn backoff_ms(step: u32) -> u64 {
    RETRY_BASE_MS
        .saturating_mul(1u64 << step.min(20))
        .min(RETRY_CAP_MS)
}

/// The lifecycle core: one event of one invocation, seen through `view`, into
/// a [`Plan`]. Pure: no I/O, no clock (the time is in `view.now_ms`), no
/// allocation.
pub fn step(view: &View, event: &Event) -> Plan {
    decide(view, event, &Bugs::NONE)
}

/// [`step`] with historical bugs planted (M4, `tests/m4.rs::matrix`).
#[cfg(any(test, feature = "mutations"))]
pub fn step_mutated(view: &View, event: &Event, bugs: &Bugs) -> Plan {
    decide(view, event, bugs)
}

/// Reads a view cell or returns the `Need` for it.
macro_rules! read {
    ($v:expr, $field:ident, $need:expr) => {
        match $v.$field {
            Some(x) => x,
            None => return Plan::need($need),
        }
    };
}

fn decide(v: &View, e: &Event, b: &Bugs) -> Plan {
    match *e {
        Event::Submitted { hold } => submitted(hold),
        Event::RetryDue { row } => retry_due(v, row, b),
        Event::TimerDue { in_flight } => timer_due(v, in_flight, b),
        Event::CompletionArrived { wake } => completion_arrived(v, wake, b),
        Event::Drive { turn } => drive(v, turn),
        Event::PermitGranted { turn } => permit_granted(v, turn, b),
        Event::Throttled { turn, after_ms } => throttled(v, turn, after_ms, b),
        Event::PermitCancelled { turn: _ } => permit_cancelled(v, b),
        Event::TurnStarted { turn, retry_count } => turn_started(v, turn, retry_count, b),
        Event::Finished { finish } => finished(v, finish, b),
        Event::TurnEnded {
            turn,
            retry_count,
            outcome,
        } => turn_ended(v, turn, retry_count, outcome, b),
        Event::TurnFailed { turn, at, err } => turn_failed(v, turn, at, err, b),
        Event::LeaderUp => leader_up(v, b),
        Event::Recover => recover(v),
        Event::RescueRound { marked, stuck } => rescue(v, marked, stuck),
        Event::Dequeue => dequeue(v),
        Event::AdminCancel => cancel(v, b),
        Event::Pause { reason } => pause(v, reason),
        Event::AdminResume => resume(v),
        Event::AdminRestart => restart(v, b),
    }
}

// ─────────────────────────────────────────────────────────────── shared

/// The current stage becomes `to`: `Move` if different, `Keep` if already
/// there or there is no entry (`end_turn`/`begin_turn` skip an invocation
/// without an entry).
fn move_to(slot: Option<crate::Slot>, to: Stage) -> StageChange {
    match slot {
        Some(s) if s.stage != to => StageChange::Move(to),
        _ => StageChange::Keep,
    }
}

fn remove(slot: Option<crate::Slot>) -> StageChange {
    match slot {
        Some(_) => StageChange::Remove,
        None => StageChange::Keep,
    }
}

/// `(attempt, fires)` of the turn at journal position `len_now`, read from the
/// durable row: with progress (the journal is longer than when scheduled) both
/// go back to 0 ("since last stored entry", `Engine::schedule_retry`); a turn
/// carrying a wake-up (the journal will grow) or `resume` (the operator lifted
/// a pause) also goes back to 0.
///
/// The first part is the `ctx.attempt` of a turn without a pre-planned retry.
/// Before the core it was the constant 0 (`RetryOwner::Drive`); they differ
/// only for the turn of an invocation with a counted watch row — the reverse
/// of suspicion B.
fn position(turn: Turn, row: Option<RetryRow>, len_now: u32) -> (u32, u32) {
    if turn.reset || turn.wake.is_some() {
        return (0, 0);
    }
    match row {
        Some(r) if len_now <= r.journal_len => (r.attempt, r.fires),
        _ => (0, 0),
    }
}

/// What to release when dropping a permit that has not `begin` (`Claim::drop`,
/// `Permit::unstarted`).
fn release_unstarted(permit: Permit, b: &Bugs) -> Release {
    match permit {
        Permit::None | Permit::Started => Release::Keep,
        Permit::Waiting => Release::Claim,
        // The VO lock held from before stays with its holder.
        Permit::Granted { held_before: true } if !bug!(b.f018_drop_held_lock) => Release::Running,
        Permit::Granted { .. } => Release::All,
    }
}

/// The backoff of step `step`: the handler's policy, otherwise the default.
fn policy_delay(v: &View, step: u32) -> Result<u64, Need> {
    match v.policy {
        Some(p) if p.step == step => Ok(p.delay_ms.unwrap_or_else(|| backoff_ms(step))),
        _ => Err(Need::Policy { step }),
    }
}

/// The due time `now + delay`. The delay may be set by the **tenant** — the
/// SDK's `next_retry_delay`, the manifest's `retryPolicy*Interval` —, so it
/// saturates at `u64::MAX` instead of overflowing (a debug build panics on the
/// engine thread, a release build wraps into the past ⇒ fires again on every
/// tick).
fn due_after(v: &View, delay: u64, b: &Bugs) -> u64 {
    if bug!(b.due_wraps) {
        v.now_ms.wrapping_add(delay)
    } else {
        v.now_ms.saturating_add(delay)
    }
}

macro_rules! delay {
    ($v:expr, $step:expr) => {
        match policy_delay($v, $step) {
            Ok(d) => d,
            Err(n) => return Plan::need(n),
        }
    };
}

// ─────────────────────────────────────────────────────────────── create

fn submitted(hold: bool) -> Plan {
    Plan {
        status: Some(if hold {
            Status::Paused(Pause::Quarantine)
        } else {
            Status::Running
        }),
        stage: StageChange::Insert(Stage::Inbox),
        then: Then::Reply(Reply::Nothing),
        ..Plan::default()
    }
}

// ─────────────────────────────────────────────────────────────── background tick

/// `decide_retry` (FL-5, FL-19, FL-20, FL-21) + `reschedule_retry` +
/// `queue_retry`.
fn retry_due(v: &View, row: RetryRow, b: &Bugs) -> Plan {
    let a = v.attendance;
    let busy = if bug!(b.fl20_lock_only) {
        a.lock_held
    } else {
        a.busy()
    };
    if busy && a.parked && !bug!(b.parked_defer_short) {
        // **Parked ticket**: the ticket's turn rewrites the row when it is
        // granted (`PermitGranted`), or the turn that takes the ticket over
        // rewrites it — deferring like an ordinary busy one (`defer_ms`) is one
        // commit per ticket per `defer_ms`, scaling with the number of tickets.
        // Defer **far** (`parked_defer_ms`) into a **watch** row, keeping
        // `attempt`/`journal_len`/`fires` (`position` does not read the kind,
        // FL-19/21): if the ticket is lost with the term, the next term's
        // `recover` does not wait for the watch row (`pending` counts only
        // `Retry`) and hands over a turn at once. Except an EXCLUSIVE queue's
        // `Inbox` entry: a watch row there marks a turn that has entered
        // `Running` (`valid`) — defer as usual.
        let slot = read!(v, slot, Need::Slot);
        if !slot.is_some_and(|s| s.exclusive && s.stage == Stage::Inbox) {
            return Plan {
                retry: RowChange::Set(RetryRow {
                    due: due_after(v, v.knobs.parked_defer_ms, b),
                    kind: RowKind::Watch,
                    fires: if bug!(b.parked_defer_resets_fires) {
                        0
                    } else {
                        row.fires
                    },
                    ..row
                }),
                ..Plan::default()
            };
        }
    }
    if busy {
        // `defer_retries` writes 13 bytes with flag 0: a deferred watch row
        // becomes an ordinary retry (`pending_retry` counts it from then on) —
        // kept as before.
        return Plan {
            retry: RowChange::Set(RetryRow {
                due: due_after(v, v.knobs.defer_ms, b),
                kind: RowKind::Retry,
                ..row
            }),
            ..Plan::default()
        };
    }
    let status = read!(v, status, Need::Status);
    if matches!(
        status,
        Status::Missing | Status::Completed | Status::Failed | Status::Paused(_)
    ) {
        return Plan {
            retry: RowChange::Clear,
            ..Plan::default()
        };
    }
    let len = read!(v, journal_len, Need::JournalLen);
    let (attempt, fires) = position(Turn::PLAIN, Some(row), len);
    let step = attempt.saturating_add(fires);
    let delay = delay!(v, step);
    let next_due = due_after(v, delay, b);
    // The attempt is counted at `Running`, not here (FL-19) — unless the
    // fire-time counting bugs are planted.
    let counted = if bug!(b.f023_count_at_fire) || bug!(b.f023_refund_only_throttled) {
        attempt + 1
    } else {
        attempt
    };
    let fires = if bug!(b.fl21_no_fires) {
        fires
    } else {
        fires.saturating_add(1)
    };
    Plan {
        retry: RowChange::Set(RetryRow::retry(next_due, counted, len, fires)),
        ram: crate::Ram {
            handout: Some(Turn::planned(Planned {
                attempt,
                next_due,
                journal_len: len,
            })),
            ..Default::default()
        },
        ..Plan::default()
    }
}

/// `decide_timer` (FL-10, FL-20) + `settle_completion`/`drop_timer`.
fn timer_due(v: &View, in_flight: bool, b: &Bugs) -> Plan {
    let a = v.attendance;
    if a.lock_held {
        // Busy: hand the timer to a task waiting for the lock, which carries it into the turn.
        return Plan {
            ram: crate::Ram {
                handout: (!in_flight).then_some(Turn::woken(WakeKind::Completion)),
                ..Default::default()
            },
            ..Plan::default()
        };
    }
    let status = read!(v, status, Need::Status);
    let run = !a.queued || bug!(b.a_timer_ignores_queued);
    write_then_deliver(status, WakeKind::Completion, run)
}

/// The drainer's "write then hand over". Lock busy ⇒ the row stays for the
/// next drain pass.
///
/// **Wake only once the wait tree is satisfied**: a completion for a
/// `suspended` invocation that the shell knows for sure does not satisfy the
/// last suspension's tree ⇒ write the notification, keep `suspended`, hand over
/// no turn. Unknown (no tree) ⇒ wake as before. This is enough for liveness
/// because `suspension_satisfied` is monotone in the set of delivered
/// completions: if the tree is not satisfied, one of its completions has not
/// arrived yet, and that completion — reaching the drainer like this one —
/// sees the tree satisfied (`after`), or sees no tree and wakes. Signals (and
/// timers, `TimerDue`) do not go through here: they always wake.
fn completion_arrived(v: &View, wake: WakeKind, b: &Bugs) -> Plan {
    let a = v.attendance;
    if a.lock_held {
        return Plan::reply(Reply::Nothing);
    }
    let status = read!(v, status, Need::Status);
    if status == Status::Suspended && wake == WakeKind::Completion && !bug!(b.wake_ignores_waiting)
    {
        let satisfied = match read!(v, waiting, Need::Waiting) {
            Waiting::Unknown => !bug!(b.waiting_unknown_unsatisfied),
            Waiting::Known { before, after } => {
                if bug!(b.waiting_without_new) {
                    before
                } else {
                    after
                }
            }
        };
        if !satisfied {
            return Plan {
                wake: WakeWrite::Deliver(wake),
                ..Plan::default()
            };
        }
    }
    let run = !a.queued || bug!(b.drain_ignores_queued);
    write_then_deliver(status, wake, run)
}

fn write_then_deliver(status: Status, wake: WakeKind, run: bool) -> Plan {
    match status {
        Status::Missing | Status::Completed | Status::Failed => Plan {
            wake: WakeWrite::Discard,
            ..Plan::default()
        },
        // `paused`: the wake-up is still written (the gate holds the turn back, FL-10).
        Status::Running | Status::Suspended | Status::Paused(_) => Plan {
            wake: WakeWrite::Deliver(wake),
            // `stage_wake`: a wake-up for a suspended invocation ⇒ it can run.
            status: (status == Status::Suspended).then_some(Status::Running),
            ram: crate::Ram {
                handout: run.then_some(Turn::PLAIN),
                ..Default::default()
            },
            ..Plan::default()
        },
    }
}

// ─────────────────────────────────────────────────────────────── one turn

/// The gate before the permit (`decide_gate`, FL-10), then `ensure_queued` (FL-5).
fn drive(v: &View, turn: Turn) -> Plan {
    let status = read!(v, status, Need::Status);
    let wake = turn.wake;
    match status {
        Status::Completed | Status::Failed => Plan {
            wake: if wake.is_some() {
                WakeWrite::Discard
            } else {
                WakeWrite::None
            },
            ram: ack(),
            then: Then::Reply(Reply::Ended),
            ..Plan::default()
        },
        Status::Paused(_) => Plan {
            wake: wake.map_or(WakeWrite::None, WakeWrite::Deliver),
            ram: ack(),
            then: Then::Reply(Reply::Paused),
            ..Plan::default()
        },
        // Before the core the gate let it run and then `ensure_queued` failed
        // (`load`) — an error, the wake duty dropped; the rescue net reads
        // `Missing` and forgets. The outcome is the same.
        Status::Missing => Plan {
            ram: ack(),
            then: Then::Reply(Reply::NotFound),
            ..Plan::default()
        },
        Status::Running | Status::Suspended => {
            let slot = read!(v, slot, Need::Slot);
            Plan {
                // Missing entry (data from before flow control): rebuild it at `Inbox`.
                stage: if slot.is_none() {
                    StageChange::Insert(Stage::Inbox)
                } else {
                    StageChange::Keep
                },
                then: Then::RequestPermit,
                ..Plan::default()
            }
        }
    }
}

fn ack() -> crate::Ram {
    crate::Ram {
        duty: Duty::Ack,
        ..Default::default()
    }
}

/// The gate after the permit (`decide_gate` + `settle_permit`), then the
/// `Running` commit (`Engine::begin_turn`, FL-17, FL-19).
fn permit_granted(v: &View, turn: Turn, b: &Bugs) -> Plan {
    let status = read!(v, status, Need::Status);
    let slot = read!(v, slot, Need::Slot);
    match status {
        Status::Completed | Status::Failed | Status::Missing => {
            return Plan {
                wake: if turn.wake.is_some() {
                    WakeWrite::Discard
                } else {
                    WakeWrite::None
                },
                stage: remove(slot),
                ram: crate::Ram {
                    release: Release::All,
                    duty: Duty::Ack,
                    ..Default::default()
                },
                then: Then::Reply(if status == Status::Missing {
                    Reply::NotFound
                } else {
                    Reply::Ended
                }),
                ..Plan::default()
            }
        }
        Status::Paused(_) => {
            return Plan {
                wake: turn.wake.map_or(WakeWrite::None, WakeWrite::Deliver),
                stage: move_to(slot, Stage::Paused),
                ram: crate::Ram {
                    release: Release::Running,
                    duty: Duty::Ack,
                    ..Default::default()
                },
                then: Then::Reply(Reply::Paused),
                ..Plan::default()
            }
        }
        Status::Running | Status::Suspended => {}
    }
    let len = read!(v, journal_len, Need::JournalLen);
    let (retry_count, row) = match turn.planned {
        Some(p) => {
            // The journal grew since the fire (a wake-up arrived while the turn
            // waited for a permit): the counted attempts belong to the old
            // position — count again from 0 at the new position (M4,
            // `ATTEMPTS`). Before the core: kept the fire's count and position.
            let (rc, at) = if len > p.journal_len && !bug!(b.planned_stale_position) {
                (0, len)
            } else {
                (p.attempt, p.journal_len)
            };
            let row = if bug!(b.f023_count_at_fire) || bug!(b.f023_refund_only_throttled) {
                // The fire already counted the attempt; `begin_turn` then writes nothing.
                RowChange::Keep
            } else {
                RowChange::Set(RetryRow::retry(p.next_due, rc + 1, at, 0))
            };
            (rc, row)
        }
        None => {
            let current = read!(v, retry, Need::Retry);
            // Before the core: the `Drive` owner was always 0.
            let (rc, _) = if bug!(b.uncounted_watch) {
                (0, 0)
            } else {
                position(turn, current, len)
            };
            let due = due_after(v, v.knobs.watch_after_ms, b);
            let watch = if bug!(b.uncounted_watch) {
                RetryRow::legacy_watch(due)
            } else {
                // The attempt's position is **after** the wake-up this commit
                // writes (one notification = one journal entry): if the turn
                // dies after the commit, the watch row's fire does not count the
                // wake-up as new progress (M4, `ATTEMPTS`).
                RetryRow::watch(due, rc + 1, len + u32::from(turn.wake.is_some()))
            };
            (rc, RowChange::Set(watch))
        }
    };
    Plan {
        stage: move_to(slot, Stage::Running),
        retry: row,
        // The wake-up the turn carries is in the journal before the SDK runs.
        wake: turn.wake.map_or(WakeWrite::None, WakeWrite::Deliver),
        then: Then::Preflight { retry_count },
        ..Plan::default()
    }
}

/// `park_for_retry` (+ `drive_retry`'s `Throttled` branch): the turn did not
/// enter `Running` ⇒ it spends no attempt (FL-19, suspicion B), only one fire.
fn throttled(v: &View, turn: Turn, after_ms: u64, b: &Bugs) -> Plan {
    let release = release_unstarted(v.attendance.permit, b);
    let done = |retry| Plan {
        retry,
        ram: crate::Ram {
            release,
            duty: Duty::Ack,
            ..Default::default()
        },
        then: Then::Reply(Reply::Throttled),
        ..Plan::default()
    };
    if turn.planned.is_some() {
        if bug!(b.f023_refund_only_throttled) {
            // The first fix: refund the attempt the fire counted.
            let row = read!(v, retry, Need::Retry);
            return done(match row {
                Some(r) if r.attempt > 0 => RowChange::Set(RetryRow {
                    attempt: r.attempt - 1,
                    ..r
                }),
                _ => RowChange::Keep,
            });
        }
        // A pre-planned retry still carries the old count — but its due time
        // moves to at least `now + after_ms`: keeping the handler's backoff-step
        // due time means firing again after 1 s for a turn the scheduler will
        // refuse again.
        if bug!(b.throttled_keeps_due) {
            return done(RowChange::Keep);
        }
        let due = due_after(v, after_ms, b);
        return done(match read!(v, retry, Need::Retry) {
            Some(r) if r.due < due => RowChange::Set(RetryRow { due, ..r }),
            _ => RowChange::Keep,
        });
    }
    let len = read!(v, journal_len, Need::JournalLen);
    let due = due_after(v, after_ms, b);
    if bug!(b.b_count_before_running) {
        return done(RowChange::Set(RetryRow::retry(due, 1, len, 0)));
    }
    let current = read!(v, retry, Need::Retry);
    done(RowChange::Set(not_running(turn, current, len, due)))
}

/// The retry row of a turn that did not reach `Running`: keeps the counted
/// attempts, adds one fire (FL-19, FL-21).
fn not_running(turn: Turn, current: Option<RetryRow>, len: u32, due: u64) -> RetryRow {
    // The wake-up the turn carries has **not** been written (only the
    // `Running` commit writes it): the journal has not grown, the counted
    // attempts are intact (M4, `ATTEMPTS`).
    let unwritten = Turn { wake: None, ..turn };
    let (attempt, fires) = position(unwritten, current, len);
    RetryRow::retry(due, attempt, len, fires.saturating_add(1))
}

/// The task was cancelled while waiting for a permit: RAM only. The
/// pre-planned retry (if any) stays as is with the old count (FL-19); the wake
/// duty drops unacked ⇒ the rescue net.
fn permit_cancelled(v: &View, b: &Bugs) -> Plan {
    Plan {
        ram: crate::Ram {
            release: release_unstarted(v.attendance.permit, b),
            ..Default::default()
        },
        then: Then::Reply(Reply::Failed),
        ..Plan::default()
    }
}

/// Preflight (`decide_preflight`) of a turn already in `Running`.
fn turn_started(v: &View, turn: Turn, retry_count: u32, b: &Bugs) -> Plan {
    // The tenant is refused permits **right now**: a permit granted before the tenant
    // was suspended does not let the turn call the SDK — checked even before the
    // contract: a suspended tenant changes nothing.
    if let Some(after_ms) = v.tenant_refused_ms {
        if !bug!(b.refused_calls_sdk) {
            return refused_turn(v, retry_count, after_ms, b);
        }
    }
    let c = read!(v, contract, Need::Contract);
    match c.pinned {
        // No deployment declares the service: finish as `failed` 404 —
        // matching the answer the caller gets — in **one** commit: the status,
        // removing the queue entry, deleting the retry row (the turn's own
        // watch row). Before the fix: 404 was returned without writing
        // anything, the watch row stayed ⇒ `running` without an entry, looping
        // or stuck (the "Preflight NotFound" bug report).
        Pinned::Undeclared => {
            let slot = read!(v, slot, Need::Slot);
            let ram = crate::Ram {
                release: Release::All,
                duty: Duty::Ack,
                rescue: Mark::Clear,
                ..Default::default()
            };
            if bug!(b.notfound_keeps_running) {
                return Plan {
                    stage: remove(slot),
                    ram,
                    then: Then::Reply(Reply::NotFound),
                    ..Plan::default()
                };
            }
            Plan {
                status: Some(Status::Failed),
                stage: remove(slot),
                retry: RowChange::Clear,
                ram,
                side: Some(Side::Seal(Seal::Terminal)),
                then: Then::Reply(Reply::NotFound),
                ..Plan::default()
            }
        }
        Pinned::NotLoaded => retry_soon(v, turn, retry_count, b),
        Pinned::Found => match c.max_attempts {
            Some(max) if retry_count >= max => exhausted(v, c.on_max),
            _ => Plan {
                then: Then::CallSdk { retry_count },
                side: Some(Side::NewAttempt {
                    attempt: retry_count + 1,
                }),
                ..Plan::default()
            },
        },
    }
}

/// Deployment not loaded yet: retry after 1 s, **refunding** the attempt
/// counted at `Running` (the SDK was not called).
fn retry_soon(v: &View, turn: Turn, retry_count: u32, b: &Bugs) -> Plan {
    let slot = read!(v, slot, Need::Slot);
    let len = read!(v, journal_len, Need::JournalLen);
    let due = due_after(v, RETRY_SOON_MS, b);
    let retry = if bug!(b.retry_soon_counts) {
        match turn.planned {
            // `drive_retry`'s `Retry` branch undoes the deferral; the counted row stays as is.
            Some(_) => RowChange::Keep,
            // `retry_if`: `attempt 1`.
            None => RowChange::Set(RetryRow::retry(due, 1, len, 0)),
        }
    } else {
        let row = read!(v, retry, Need::Retry);
        let fires = row.map_or(0, |r| r.fires);
        RowChange::Set(RetryRow::retry(
            due,
            retry_count,
            len,
            fires.saturating_add(1),
        ))
    };
    Plan {
        stage: move_to(slot, Stage::Suspended),
        retry,
        ram: crate::Ram {
            release: Release::Running,
            duty: Duty::Ack,
            rescue: Mark::Clear,
            ..Default::default()
        },
        then: Then::Reply(Reply::Outcome),
        ..Plan::default()
    }
}

/// The tenant is refused permits right before the SDK turn (FL-24): the SDK is
/// not called. Like [`retry_soon`] — suspended at `Suspended`, keeping the VO
/// lock, one retry row, wake duty done — but the due time is `now + after_ms`
/// (the retry rhythm of a suspended tenant; the postponed attempt is the one
/// **currently** due, not the next backoff step the pre-planned row carries)
/// and the reply is [`Reply::Throttled`].
///
/// The attempt **does not change** (FL-24 "a refused turn does not count an
/// attempt"): if the journal has not grown since the row's position (this
/// run's `Running` commit) ⇒ refund the attempt counted there, add one fire —
/// like [`retry_soon`]; if it has grown (an earlier sub-turn of the same run
/// made progress, or a wake-up was just written) ⇒ count again from the start
/// like any progress ([`position`]).
fn refused_turn(v: &View, retry_count: u32, after_ms: u64, b: &Bugs) -> Plan {
    let slot = read!(v, slot, Need::Slot);
    let len = read!(v, journal_len, Need::JournalLen);
    let row = read!(v, retry, Need::Retry);
    let due = due_after(v, after_ms, b);
    let (attempt, fires) = match row {
        Some(r) if len > r.journal_len => (0, 0),
        Some(r) => (retry_count, r.fires.saturating_add(1)),
        None => (retry_count, 1),
    };
    Plan {
        stage: move_to(slot, Stage::Suspended),
        retry: RowChange::Set(RetryRow::retry(due, attempt, len, fires)),
        ram: crate::Ram {
            release: Release::Running,
            duty: Duty::Ack,
            rescue: Mark::Clear,
            ..Default::default()
        },
        then: Then::Reply(Reply::Throttled),
        ..Plan::default()
    }
}

/// Attempts exhausted **before** calling the SDK: one commit for the status,
/// stage and row (before the core: three — `pause_with`/`fail_with`,
/// `end_turn`, `cancel_retry`).
fn exhausted(v: &View, on_max: OnMax) -> Plan {
    let slot = read!(v, slot, Need::Slot);
    match on_max {
        OnMax::Pause => Plan {
            status: Some(Status::Paused(Pause::MaxAttempts)),
            stage: move_to(slot, Stage::Paused),
            retry: RowChange::Clear,
            ram: crate::Ram {
                release: Release::Running,
                duty: Duty::Ack,
                rescue: Mark::Clear,
                ..Default::default()
            },
            side: Some(Side::Seal(Seal::Paused)),
            then: Then::Reply(Reply::Paused),
            ..Plan::default()
        },
        OnMax::Kill => Plan {
            status: Some(Status::Failed),
            stage: remove(slot),
            retry: RowChange::Clear,
            ram: crate::Ram {
                release: Release::All,
                duty: Duty::Ack,
                rescue: Mark::Clear,
                ..Default::default()
            },
            side: Some(Side::Seal(Seal::Terminal)),
            then: Then::Reply(Reply::Ended),
            ..Plan::default()
        },
    }
}

/// `apply_finish`: the engine's finishing commit, with the last journal entry.
fn finished(v: &View, finish: Finish, b: &Bugs) -> Plan {
    let status = read!(v, status, Need::Status);
    // A cancel slipped in during the turn: the outcome is durable, write nothing (FL-10).
    if status.is_terminal() || status == Status::Missing {
        return Plan::default();
    }
    let end = |to: Status, seal: Seal, keep_row: bool, spawn: bool| -> Plan {
        let slot = read!(v, slot, Need::Slot);
        Plan {
            status: Some(to),
            stage: remove(slot),
            retry: if keep_row {
                RowChange::Keep
            } else {
                RowChange::Clear
            },
            ram: crate::Ram {
                spawn_successor: spawn,
                ..Default::default()
            },
            side: Some(Side::Seal(seal)),
            ..Plan::default()
        }
    };
    match finish {
        Finish::Completed => end(Status::Completed, Seal::Terminal, false, false),
        Finish::Failed => end(Status::Failed, Seal::Terminal, false, false),
        Finish::ContinuedAsNew => end(
            Status::Completed,
            Seal::Restarted,
            bug!(b.d2_restart_keeps_row),
            true,
        ),
        Finish::Paused => Plan {
            status: Some(Status::Paused(Pause::Sdk)),
            side: (!status.is_paused()).then_some(Side::Seal(Seal::Paused)),
            ..Plan::default()
        },
        Finish::Suspended { satisfied } => Plan {
            status: Some(if satisfied {
                Status::Running
            } else {
                Status::Suspended
            }),
            ..Plan::default()
        },
        Finish::Retry => Plan {
            status: Some(Status::Running),
            ..Plan::default()
        },
    }
}

/// Ends the turn in one commit (`end_turn_settle`, `decide_settle`, FL-4,
/// FL-17) + `retry_if` + the post-processing of `drive_retry`.
fn turn_ended(v: &View, turn: Turn, retry_count: u32, outcome: Outcome, b: &Bugs) -> Plan {
    let slot = read!(v, slot, Need::Slot);
    let settled = |stage, retry, release| Plan {
        stage,
        retry,
        ram: crate::Ram {
            release,
            duty: Duty::Ack,
            rescue: Mark::Clear,
            ..Default::default()
        },
        then: Then::Reply(Reply::Outcome),
        ..Plan::default()
    };
    match outcome {
        // The row went with the engine's finishing commit; a pre-planned retry
        // is cancelled by `drive_retry`.
        o if o.is_terminal() => settled(
            remove(slot),
            if turn.planned.is_some() {
                RowChange::Clear
            } else {
                RowChange::Keep
            },
            Release::All,
        ),
        Outcome::Paused => settled(
            move_to(slot, Stage::Paused),
            RowChange::Clear,
            Release::Running,
        ),
        Outcome::Waiting => settled(
            move_to(slot, Stage::Suspended),
            RowChange::Clear,
            Release::Running,
        ),
        Outcome::Retry { delay_ms } => {
            let row = match turn.planned {
                // The turn took longer than the backoff: the background tick
                // already deferred the next retry — bring it back to the planned
                // due time (`undefer_retry`).
                Some(p) => {
                    let current = read!(v, retry, Need::Retry);
                    let to = p.next_due.max(v.now_ms);
                    match current {
                        Some(r) if r.kind == RowKind::Retry && r.due > to => {
                            RowChange::Set(RetryRow { due: to, ..r })
                        }
                        _ => RowChange::Keep,
                    }
                }
                None => {
                    let len = read!(v, journal_len, Need::JournalLen);
                    let delay = match delay_ms {
                        Some(d) => d,
                        None if bug!(b.policy_ignored) => backoff_ms(0),
                        None => delay!(v, retry_count),
                    };
                    RowChange::Set(RetryRow::retry(
                        due_after(v, delay, b),
                        retry_count + 1,
                        len,
                        0,
                    ))
                }
            };
            settled(move_to(slot, Stage::Suspended), row, Release::Running)
        }
        _ => unreachable!("covered by is_terminal"),
    }
}

/// The retry of a turn with a pre-planned retry that just failed: that retry
/// is its wake-up (FL-16, FL-19) — **unless** the background tick has already
/// moved the row far out into a watch row because of a parked ticket
/// (`retry_due`: `parked_defer_ms`, one day). A `planned` turn's row can only be
/// a watch row from that — its `Running` commit writes `Retry` —, and if the
/// ticket's turn fails before rewriting the row (reading the status at the gate
/// after the permit, or the `Running` commit failing before it wrote) nobody
/// wakes it for a day. Rewrite `Retry` at the backoff step per `position`,
/// keeping `attempt`/`journal_len`/`fires` (the fire was counted at
/// `RetryDue`).
///
/// `Err(None)`: the read failed — keep the row, the duty drops unacked (rescue net).
fn planned_rewake(v: &View, turn: Turn, b: &Bugs) -> Result<RowChange, Option<Plan>> {
    if bug!(b.planned_keeps_watch_row) {
        return Ok(RowChange::Keep);
    }
    if v.failed(Need::Retry).is_some() {
        return Err(None);
    }
    let Some(current) = v.retry else {
        return Err(Some(Plan::need(Need::Retry)));
    };
    let Some(r) = current.filter(|r| r.kind == RowKind::Watch) else {
        return Ok(RowChange::Keep);
    };
    if v.failed(Need::JournalLen).is_some() {
        return Err(None);
    }
    let Some(len) = v.journal_len else {
        return Err(Some(Plan::need(Need::JournalLen)));
    };
    let (attempt, fires) = position(Turn { wake: None, ..turn }, Some(r), len);
    let delay = policy_delay(v, attempt.saturating_add(fires)).map_err(|n| Some(Plan::need(n)))?;
    Ok(RowChange::Set(RetryRow {
        due: due_after(v, delay, b),
        kind: RowKind::Retry,
        ..r
    }))
}

/// The turn threw an error (`schedule_after_error`, the `Err` branch of
/// `drive_with_permit`). Where the turn stopped is unknown ⇒ keep the VO lock
/// (FL-4) and **must** leave a retry; if one cannot be set up, keep the watch
/// row and drop the duty (FL-16).
fn turn_failed(v: &View, turn: Turn, at: FailedAt, err: TurnErr, b: &Bugs) -> Plan {
    match at {
        FailedAt::BeforeRunning => {
            // Lost the right to write ⇒ the next term's `recover`.
            if err == TurnErr::Fenced {
                return Plan {
                    ram: ack(),
                    then: Then::Reply(Reply::Failed),
                    ..Plan::default()
                };
            }
            // The pre-planned retry is the retry — unless it was moved far out.
            if turn.planned.is_some() {
                return match planned_rewake(v, turn, b) {
                    Ok(retry) => Plan {
                        retry,
                        ram: ack(),
                        then: Then::Reply(Reply::Failed),
                        ..Plan::default()
                    },
                    Err(Some(need)) => need,
                    Err(None) => Plan::reply(Reply::Failed),
                };
            }
            if v.failed(Need::JournalLen).is_some() {
                return Plan::reply(Reply::Failed);
            }
            let len = read!(v, journal_len, Need::JournalLen);
            let row = if bug!(b.b_count_before_running) {
                RetryRow::retry(due_after(v, backoff_ms(0), b), 1, len, 0)
            } else {
                let current = read!(v, retry, Need::Retry);
                let (attempt, fires) = position(Turn { wake: None, ..turn }, current, len);
                let delay = delay!(v, attempt.saturating_add(fires));
                not_running(turn, current, len, due_after(v, delay, b))
            };
            Plan {
                retry: RowChange::Set(row),
                ram: ack(),
                then: Then::Reply(Reply::Failed),
                ..Plan::default()
            }
        }
        FailedAt::AfterRunning { retry_count } => {
            // Rule 7: the error may come from a commit that landed — the engine's
            // finishing commit applied (status terminal, entry and row removed, the
            // reply written) but returned an error. Read the status back: terminal ⇒
            // settle like `TurnEnded` with a terminal outcome, freeing the VO lock;
            // otherwise (or the read failed) "unknown where the turn stopped" (FL-4).
            if err != TurnErr::Fenced
                && !bug!(b.ambiguous_finish_keeps_lock)
                && v.failed(Need::Status).is_none()
                && read!(v, status, Need::Status).is_terminal()
            {
                let slot = read!(v, slot, Need::Slot);
                return Plan {
                    stage: remove(slot),
                    retry: if turn.planned.is_some() {
                        RowChange::Clear
                    } else {
                        RowChange::Keep
                    },
                    ram: crate::Ram {
                        release: Release::All,
                        duty: Duty::Ack,
                        rescue: Mark::Clear,
                        ..Default::default()
                    },
                    side: Some(Side::Seal(Seal::Terminal)),
                    then: Then::Reply(Reply::Outcome),
                    ..Plan::default()
                };
            }
            let slot = read!(v, slot, Need::Slot);
            let base = |retry, duty| Plan {
                stage: move_to(slot, Stage::Suspended),
                retry,
                ram: crate::Ram {
                    release: Release::Running,
                    duty,
                    ..Default::default()
                },
                then: Then::Reply(Reply::Failed),
                ..Plan::default()
            };
            if err == TurnErr::Fenced {
                return base(RowChange::Keep, Duty::Ack);
            }
            // The `planned` turn's `Running` commit wrote `Retry` ⇒ keep it; a
            // row that is still a watch row moved far out (the commit failed
            // before writing) ⇒ rewrite it.
            if turn.planned.is_some() {
                return match planned_rewake(v, turn, b) {
                    Ok(retry) => base(retry, Duty::Ack),
                    Err(Some(need)) => need,
                    Err(None) => base(RowChange::Keep, Duty::Keep),
                };
            }
            if v.failed(Need::JournalLen).is_some() {
                // The watch row is the retry; the rescue net is the fast path.
                return base(RowChange::Keep, Duty::Keep);
            }
            let len = read!(v, journal_len, Need::JournalLen);
            let delay = if bug!(b.policy_ignored) {
                backoff_ms(retry_count)
            } else {
                delay!(v, retry_count)
            };
            base(
                RowChange::Set(RetryRow::retry(
                    due_after(v, delay, b),
                    retry_count + 1,
                    len,
                    0,
                )),
                Duty::Ack,
            )
        }
    }
}

// ─────────────────────────────────────────────────────────────── leader change

/// `restore_flow` for one queue entry (FL-8): push `Running` out (with a lock
/// ⇒ `Suspended`, without ⇒ `Inbox`), rebuild the VO lock for a lock-holding
/// entry of a live invocation, clean up a lock-holding entry of a stopped
/// invocation.
fn leader_up(v: &View, b: &Bugs) -> Plan {
    let Some(slot) = read!(v, slot, Need::Slot) else {
        return Plan::default();
    };
    let f017 = bug!(b.f017_running_to_inbox) && slot.stage == Stage::Running;
    let pushed = match slot.stage {
        Stage::Running if slot.exclusive && !f017 => StageChange::Move(Stage::Suspended),
        Stage::Running => StageChange::Move(Stage::Inbox),
        _ => StageChange::Keep,
    };
    // Classify on the snapshot **before** the push: `Running` holds the lock.
    if !slot.exclusive || !slot.stage.holds_lock() || f017 {
        return Plan {
            stage: pushed,
            ..Plan::default()
        };
    }
    let status = read!(v, status, Need::Status);
    if status.is_live() {
        Plan {
            stage: pushed,
            ram: crate::Ram {
                vo: Vo::Restore,
                ..Default::default()
            },
            ..Plan::default()
        }
    } else {
        // An orphan entry of an invocation that stopped for good: not a lock.
        Plan {
            stage: StageChange::Remove,
            ..Plan::default()
        }
    }
}

/// `recover`: one `running` invocation of the scan. The same rule as an
/// unmarked id of the rescue net.
fn recover(v: &View) -> Plan {
    let status = read!(v, status, Need::Status);
    if status != Status::Running {
        return Plan::default();
    }
    rescue(v, false, false)
}

/// `decide_rescue` (FL-16).
fn rescue(v: &View, marked: bool, stuck: bool) -> Plan {
    let mark = |m| Plan {
        ram: crate::Ram {
            rescue: m,
            ..Default::default()
        },
        ..Plan::default()
    };
    // No duplicate turn: if someone is working, stop; an id from a mark is reconsidered later.
    if v.attendance.busy() {
        return mark(if marked { Mark::Later } else { Mark::None });
    }
    if marked {
        match v.failed(Need::Status) {
            Some(ReadErr::Fenced) => return Plan::reply(Reply::Abort),
            Some(ReadErr::Other) => return mark(Mark::Strike),
            None => {}
        }
        // Only `running` has work to run, like `recover`.
        if read!(v, status, Need::Status) != Status::Running {
            return mark(Mark::Forget);
        }
    }
    let pending = match v.failed(Need::Retry) {
        Some(ReadErr::Fenced) => return Plan::reply(Reply::Abort),
        // Running once too many beats forgetting.
        Some(ReadErr::Other) => false,
        None => read!(v, retry, Need::Retry).is_some_and(|r| r.kind == RowKind::Retry),
    };
    // The durable retry is what runs it next, at the right backoff step.
    if pending {
        return Plan::default();
    }
    if marked && stuck {
        return Plan {
            status: Some(Status::Paused(Pause::InfraStuck)),
            side: Some(Side::Seal(Seal::Paused)),
            ram: crate::Ram {
                rescue: Mark::Forget,
                ..Default::default()
            },
            ..Plan::default()
        };
    }
    Plan {
        ram: crate::Ram {
            handout: Some(Turn::PLAIN),
            ..Default::default()
        },
        ..Plan::default()
    }
}

// ─────────────────────────────────────────────────────────────── admin

fn dequeue(v: &View) -> Plan {
    let slot = read!(v, slot, Need::Slot);
    Plan {
        stage: remove(slot),
        ram: crate::Ram {
            vo: Vo::Free,
            ..Default::default()
        },
        then: Then::Reply(Reply::Cancelled),
        ..Plan::default()
    }
}

/// `turn::cancel`: the `failed` 572 status, the retry row and the queue entry
/// in **one** commit (suspicion D (1); before the core: two), then return the
/// VO lock.
fn cancel(v: &View, b: &Bugs) -> Plan {
    let status = read!(v, status, Need::Status);
    match status {
        Status::Missing => Plan::reply(Reply::NotFound),
        // Cancelling again repairs the orphan entry of an earlier cancel.
        Status::Completed | Status::Failed => {
            let p = dequeue(v);
            if p.need.is_some() {
                return p;
            }
            Plan {
                then: Then::Reply(Reply::AlreadyFinished),
                ..p
            }
        }
        Status::Running | Status::Suspended | Status::Paused(_) => {
            if bug!(b.d1_cancel_two_commits) {
                return Plan {
                    status: Some(Status::Failed),
                    retry: RowChange::Clear,
                    side: Some(Side::Seal(Seal::Terminal)),
                    then: Then::Dequeue,
                    ..Plan::default()
                };
            }
            let slot = read!(v, slot, Need::Slot);
            Plan {
                status: Some(Status::Failed),
                stage: remove(slot),
                retry: RowChange::Clear,
                ram: crate::Ram {
                    vo: Vo::Free,
                    ..Default::default()
                },
                side: Some(Side::Seal(Seal::Terminal)),
                then: Then::Reply(Reply::Cancelled),
                ..Plan::default()
            }
        }
    }
}

/// `Engine::pause_with` (`pause_stuck` 577, `poison_row` 577): the stage stays
/// as is (suspicion C, harmless).
fn pause(v: &View, reason: Pause) -> Plan {
    let status = read!(v, status, Need::Status);
    match status {
        Status::Running | Status::Suspended => Plan {
            status: Some(Status::Paused(reason)),
            side: Some(Side::Seal(Seal::Paused)),
            ..Plan::default()
        },
        Status::Paused(_) => Plan {
            status: Some(Status::Paused(reason)),
            ..Plan::default()
        },
        Status::Missing | Status::Completed | Status::Failed => Plan::default(),
    }
}

/// `turn::resume`: `paused → running`, the stage stays as is (suspicion C),
/// then run the turn under the same lock, counting retries again from the start.
fn resume(v: &View) -> Plan {
    let status = read!(v, status, Need::Status);
    match status {
        Status::Missing => Plan {
            ram: ack(),
            then: Then::Reply(Reply::NotFound),
            ..Plan::default()
        },
        Status::Paused(_) => Plan {
            status: Some(Status::Running),
            then: Then::Drive(Turn {
                reset: true,
                ..Turn::PLAIN
            }),
            ..Plan::default()
        },
        _ => Plan {
            ram: ack(),
            then: Then::Reply(Reply::NotPaused),
            ..Plan::default()
        },
    }
}

/// `turn::restart` (`Engine::restart`): an old run that has not finished ends
/// as `completed {"continuedAsNew"}` — stage and retry row (suspicion D (2);
/// before the core: not deleted) in the same commit — returning the VO lock;
/// then the new run is created and handed over.
fn restart(v: &View, b: &Bugs) -> Plan {
    let status = read!(v, status, Need::Status);
    let spawn = crate::Ram {
        spawn_successor: true,
        ..Default::default()
    };
    match status {
        Status::Missing => Plan::reply(Reply::NotFound),
        Status::Completed | Status::Failed => Plan {
            ram: spawn,
            then: Then::Reply(Reply::Restarted),
            ..Plan::default()
        },
        Status::Running | Status::Suspended | Status::Paused(_) => {
            let slot = read!(v, slot, Need::Slot);
            Plan {
                status: Some(Status::Completed),
                stage: remove(slot),
                retry: if bug!(b.d2_restart_keeps_row) {
                    RowChange::Keep
                } else {
                    RowChange::Clear
                },
                ram: crate::Ram {
                    vo: Vo::Free,
                    ..spawn
                },
                side: Some(Side::Seal(Seal::Restarted)),
                then: Then::Reply(Reply::Restarted),
                ..Plan::default()
            }
        }
    }
}
