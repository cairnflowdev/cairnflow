// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! # M4 — the invocation lifecycle on the core **itself**
//!
//! *The model is a hypothesis about the code, like a guide: it drifts. Unlike
//! M1–M3, M4's transition function **is** the real code: every decision goes
//! through [`cairn_lifecycle::step_mutated`]. The part the model writes itself
//! is only the shell (the event order of one turn, "store" and "RAM" as small
//! maps) and the environment (SDK, throttle, deaths). Changing the shell's event
//! order means fixing the shell here in the same commit.*
//!
//! Two invocations on the same queue — EXCLUSIVE (one VO lock) or not, per
//! [`Config::exclusive`]. Each invocation starts with a client turn (the
//! `/invoke` path, never throttled) or a `/send` turn (handed to `Runner`, can
//! be throttled). The SDK follows a script: a retryable failure at most `fails`
//! times, an infrastructure error after `Running` at most `errors` times,
//! suspending once to wait for a timer, one drainer completion, or **two**
//! drainer completions under an `ALL`/`FIRST` tree (`sleeps`, [`WakeBy::Two`] —
//! woken while the tree is not satisfied it suspends again on the same tree),
//! then done. The environment has budgets: throttling, leader changes (death:
//! RAM lost, durable state kept), admin cancel, `resume`, the `pause_stuck`
//! rescue net. Every path is finite, so "liveness" means: in a **quiet** state
//! (no action can change anything) every invocation has reached an outcome or
//! is `paused` for a reason ([`LIVE`]).
//!
//! ## State ↔ code
//!
//! | Model | Real |
//! |---|---|
//! | [`Inv::status`] | `invocation_status|id` (`cairn_lifecycle::Status`) |
//! | [`Inv::slot`] | the `VQueues` entry + `vqueue_of` |
//! | [`Inv::row`] | the entity row `retries|id` (decoded — `cairn_lifecycle::codec`) |
//! | [`Inv::len`] | journal length |
//! | [`Inv::timer`], [`Inv::completion`] | the `timers` row; the number of **visible** (memtable) inbound rows waiting for the drainer |
//! | [`Inv::asked`], [`Inv::recv`] | `call` commands in the journal whose results come back through the drainer; the dedup key of received messages (`keyspace::inbound`, IR-5) |
//! | [`Inv::issued`], [`Inv::acked`] | **the sender** (the callee's range): `Reply` outbox rows produced — after the call is durable (IR-1); deleted after `200` (IR-2) |
//! | [`State::durable`] | the durable prefix of the range WAL (`durable_seq`); `invs` is the visible store — SlateDB reads at level `Memory` |
//! | [`Task::fresh`] | the turn's `Running` commit is still in the non-durable tail |
//! | [`Inv::got`], [`Inv::two`] | drainer completions in the journal since the suspension; the two-completion `ALL`/`FIRST` tree the SDK suspended on (journal) |
//! | [`State::refused`] | the tenant of both invocations is refused permits (`suspended`) by the node's tenant-state source (`TenantLimitsSource`) — the scheduler grants no permit, the shell asks it at preflight and right before calling the SDK |
//! | [`State::hints`] | the wait tree of the last suspension in the engine's RAM (`cairn-engine/src/engine/warm.rs`): written after the suspending commit, removed when a turn loads the journal (`TurnStarted`), lost on a leader change |
//! | [`Task`] | a task running a turn: `Runner` (`run_dispatched`) or direct (`/invoke`, `/resume`, `/cancel`) |
//! | [`Task::lock`] | the invocation lock (`Locks`) |
//! | [`Pc::Waiting`], [`Pc::Granted`] | a waiting `Claim`; a `Permit` granted but not yet `begin` |
//! | [`Pc::Parked`] | a background turn's ticket in the scheduler's queue (`flow::Ticket`): no task, no invocation lock, still a handed-over turn (`Dispatched` lives in the ticket ⇒ `Runner::queued`, the wake duty) |
//! | [`Pc::Handed`] | a ticket that has been granted a permit (`Scheduler::deliver` → `take_ready` → the `run_granted` task spawned) whose turn has not taken the invocation lock yet — still `live` (`Scheduler::is_parked`), the permit is alive, the VO lock taken |
//! | [`Pc::Deferred`] | a nacked ticket (`Tickets::deferred`): permit returned, waiting for the invocation lock to be released |
//! | [`Pc::Stale`] | flag `granted_out_of_ledger`: the `run_granted` task of a ticket that a new request has adopted: the ticket is no longer `live`, the permit still sits in the task's future (the earlier shell) |
//! | [`State::vo`] | the VO lock (`flow::Core.rm.locks`) — one lock |
//! | [`State::marks`] | rescue-net marks (`Rescue`) |
//! | `Task::runner` + `Task::turn.wake == None` | a handed-over turn (`Runner::queued`) |
//! | [`Task::acked`] | the wake duty (`WakeDuty`) |
//! | [`State::up`], [`State::restored`], [`State::recovered`] | rebuilding the VO locks (`restore_flow`) has finished — the scheduler's grant gate is open (`Scheduler::open_gate`); its chunk has passed the invocation's entry; the term's `recover` has run |
//! | [`State::creates`] | `/invoke`/`/send` commands creating an invocation that have not arrived yet ([`Config::late`]) |
//! | [`Inv::calls`], [`Inv::lost`], [`Inv::entered`], [`Inv::finals`], [`Inv::last_attempt`], [`Inv::last_fire`] | ghost variables: SDK calls since the last progress; attempts counted at `Running` that died before calling the SDK (FL-19 allows it — including a turn that left preflight without calling the SDK through a write a leader change dropped); state read; number of finishing seals; the last `NewAttempt`; the backoff step of the last fire. With durability: ghost variables per store version (visible, durable); outside effects (`calls`, `entered`) count in every version |
//!
//! ## Actions ↔ core events ↔ real functions
//!
//! | Action | `Event` | Today |
//! |---|---|---|
//! | [`Action::TakeLock`] | — | `Locks::invocation` (a task waiting for the lock) |
//! | [`Action::Act`] at `Drive` | `Drive` | `drive_untraced`: `gate_turn`, `ensure_queued` |
//! | [`Action::Grant`] | — | `Provisional::secure` (grant a permit, take the VO lock). For a parked ticket: `Scheduler::deliver` hands ticket + permit to the serving loop ⇒ [`Pc::Handed`]; the grant does not look at the invocation lock (two steps, with a window in between) |
//! | [`Action::Act`] at `Handed` | — | the first poll of `run_granted`: synchronous `try_invocation` — free ⇒ `Scheduler::claim`, `Granted`; busy ⇒ `Scheduler::nack` (`Core::nack`): return the permit keeping the VO lock, the ticket waits for the lock to be released ([`Pc::Deferred`]) |
//! | [`Action::Act`] at `Deferred` | — | `Locks` nudges `Scheduler::lock_freed`, `requeue_freed`: the ticket asks again with the same `seq` ⇒ `Parked` — enabled when the invocation lock is free |
//! | [`Action::Act`] at `Stale` | — | the task of an adopted ticket polls for the first time: nack or `claim` (the ticket is no longer alive) — both return the permit per `held_before`, the task ends |
//! | [`Action::DrainNack`] | `CompletionArrived` | the drainer holds the target's invocation lock exactly when a granted ticket's turn polls: nack, then the drainer delivers the row (the ticket is still `live` ⇒ no turn handed over) |
//! | [`Action::Signal`] | `Drive` (a turn carrying a signal) | `deliver_signal_and_drive` (external signal, cooperative cancel): `Locks::invocation`, then `drive` — the permit request (`acquire_cooled`) first adopts the same invocation's ticket (`adopt`) |
//! | [`Action::GateFail`] | `TurnFailed { BeforeRunning }` (a `planned` turn) | reading the status at the start of `drive_with_permit` fails (`gate_status`, failpoint `turn::gate_turn`): `TurnPermit::drop` releases the permit keeping the VO lock; a turn with a pre-planned retry tells the core (`failed_before_running`), other turns drop unacked (the rescue net). A ticket's turn does not ack early (`drive_granted`) |
//! | `Then::RequestPermit` of a background turn | — | `turn::drive_dispatched`: the request in the same engine hold as the gate; granted at once ⇒ go on in the task, otherwise park a ticket (`Pc::Parked`, release the invocation lock). Every request first **adopts the live ticket** of the same invocation (`Scheduler::adopt`) — one entry per invocation in the queue; a granted ticket that has not taken the lock yet ([`Pc::Handed`]) hands its permit to the request (`Granted { held_before }` of that grant) |
//! | [`Action::Act`] at `Granted` | `PermitGranted` | `drive_with_permit` → `begin_turn` |
//! | [`Action::Throttle`] | `Throttled` | the earlier shell (`permit_wait_in_task`): the wait slice ran out because of the budget → `park_for_retry` / `drive_retry` (the slice check reads the scheduler's RAM, without taking the engine). Background turns are no longer throttled at the permit request; `Event::Throttled` remains for `resume`/external signals (`acquire_turn_background`) — M4 does not model their throttling |
//! | [`Action::Act`] at `Began` | `TurnStarted` | the preflight of `drive_one` (`View::tenant_refused_ms` from `permit::refused_ms`) |
//! | [`Action::Act`] at `Ready` | — | `turn/sdk.rs`: `sync` (empty tail), then send to the SDK — enabled when every write is durable. A tenant refused at that moment (`refused_now` in the closure calling the SDK) ⇒ `Step::Refused`, back to `Began` (preflight again) |
//! | [`Action::Refuse`], [`Action::Admit`] | — | `PUT /admin/tenants/{t}/state suspended`/`active` applied on the node (`TenantSettings::refresh` → a new generation of the source) |
//! | [`Action::Sdk`] | `Finished` / `TurnFailed` | the SDK runs; `apply_finish`; the `Err` branch. `SdkResult::Unfinished`: no event — the body/stream ended while the machine has no outcome (`settle_turn`, branch `None`) |
//! | [`Action::Act`] at `Finished` | `TurnEnded` | `retry_if` + `end_turn_settle` + `drive_retry` |
//! | [`Action::TickRetry`] | `RetryDue` | `fire_retries` (an invocation with a parked ticket: `Attendance::parked` — the core moves the row far out and turns it into a watch row); and `wake_lifted` (lifting a suspension; in batches of ≤ 256 rows per tick; also a leader that comes up after the lift, by the `refused-ms=`/`lifted-ms=` marks of the tenant registry) fires rows **before** their due time — the model has no time, so `TickRetry` is enabled whenever there is a row anyway |
//! | [`Action::TickTimer`] | `TimerDue` | the timer branch of `tick` |
//! | [`Action::Accept`] | — | `on_inbound_batch` → `accept_messages`: the receive commit (into the tail); a message already received (by the visible `recv`) adds no row |
//! | [`Action::Flush`] | — | the cut + PUT of the shared WAL: every submitted write becomes durable (NW-1). One flush of the whole tail covers every prefix — flushing right after that prefix's last write |
//! | [`Action::Ack`] | — | `finish_unexposed_then` → `200`: enabled when the receive commit of the queue head is durable (LF-4); the sender deletes the row |
//! | [`Action::Drain`] | `CompletionArrived` (+ `Need::Waiting` when `suspended`) | reads **visible** inbound rows, durable or not (an interleaving that exists anyway); `deliver_to_caller` & co. — a remote `Call` row: after the target range's ack, still one commit under the caller's lock. `Need::Waiting`: the shell evaluates the tree in RAM on the completions delivered, before/after the one just arrived (`turn/life.rs::Asker`); no tree ⇒ `Unknown`. **One row per step**, atomic (ask the core, commit, hand over the turn in the same hold); any action can slip in between two rows. Draining in chunks (`drain_inbound`/`drain_signals`/`drain_outbox` release the engine between two chunks, `actor/background.rs::drain_loop`) therefore adds no interleaving the model has not explored — the release point sits **between** two rows, not between asking the core and committing one row |
//! | [`Action::DrainSee`] | — | injected flag `drain_attendance_across_release`: ask `Attendance` for the row in the hold before the release point, deliver in the hold after by the old answer |
//! | [`Action::LeaderChange`] | — | leadership lost / death: `JoinSet::shutdown`, `Scheduler::reset`, RAM lost; the non-durable tail is lost (the store goes back to the durable prefix), the sender and effects already outside stay |
//! | [`Action::RestoreOne`] | `LeaderUp` | one chunk of `restore_flow`: `Engine::restore_chunk` reads the `Invocations` index (done ⇒ skipped) and the entries of unfinished ids, the core's `LeaderUp` per entry, `restore_stages` + `Scheduler::restore_lock` — one engine hold; commands, the drainer, background ticks, `recover` slip in between two chunks. The last chunk opens the grant gate |
//! | (choosing the next action) | — | the engine lock (`lease::EngineLock`): which task takes the engine next. Tokio's FIFO, except in the **start-of-term window** (until rebuilding the locks and the first `recover` are done): background work (`restore`, the drainer, background ticks, `recover`, the rescue net, index backfill) yields to commands waiting for the engine, capped at `lease::YIELD_MAX` takes per request; the drainer releases early at its existing release points between two rows when a command waits. The model chooses **every** enabled action at each step, so every engine-taking order has been explored: the priority rule only narrows the set of orders (the drainer's release points: `Action::Drain` is one row per step) — no new interleaving, no `Bugs` flag for the earlier shell |
//! | [`Action::Restore`] | `LeaderUp` | the earlier product (flag `restore_blocks_commands`): every entry in one hold, before the first command |
//! | [`Action::RestoreLock`] | — | injected flag `restore_lock_after_release`: the RAM lock is built in a hold after the classification |
//! | [`Action::Create`] | — | `turn::admit` (`Submitted`): `running`, an `Inbox` entry, one commit; an `/invoke` or `/send` task. The queue `seq` counter is primed from the durable mark (`floors::QUEUE_HIGH`) — M4 has no `seq` |
//! | [`Action::Recover`] | `Recover` | `actor::recover` |
//! | [`Action::Rescue`] | `RescueRound` | `rescue_round` (+ `pause_stuck`) |
//! | [`Action::Cancel`] → `Act` | `AdminCancel` (+ `Dequeue`) | `turn::cancel` |
//! | [`Action::Resume`] → `Act` | `AdminResume` → `Drive` | `turn::resume` |
//!
//! ## Properties
//!
//! - [`VALID`]: `cairn_lifecycle::valid` after every step, for every invocation.
//! - [`ONE_TURN`]: an invocation has at most one task holding its lock and one
//!   turn in the SDK.
//! - [`ONE_PERMIT`]: an invocation has at most one live permit — a granted
//!   ticket, the task of an adopted ticket, a turn from `Granted` until release.
//! - [`WAKE_BOUND`]: a `running` invocation with no task, no ticket, no rescue
//!   mark, whose term's `recover` has run and with no wake waiting for delivery
//!   ⇒ has a row due below [`PARKED_DEFER_MS`] — moving a parked ticket's row
//!   far out is nobody's retry. M4 has no time, so `LIVE` does not see a
//!   one-day row: it still fires.
//! - [`NO_DUPLICATE`]: at most one turn **carrying no wake** handed to `Runner`
//!   per invocation (FL-20) — a second turn is a redundant SDK call.
//! - [`VO_EXCLUSIVE`]: an EXCLUSIVE queue has at most one invocation that has
//!   read state and not finished (FL-4).
//! - [`ATTEMPTS`]: an attempt is counted ⇔ the SDK is called — at each SDK call
//!   and each preflight that gives up, `ctx.attempt` = SDK calls + deaths between
//!   the `Running` commit and the call (FL-19, and its reverse direction).
//! - [`SEAL`]: `Side::Seal` finishes exactly once for every finished invocation,
//!   never twice.
//! - [`NEW_ATTEMPT`]: `Side::NewAttempt` grows strictly between two progresses —
//!   at the real SDK call: a preflight the shell stops before the call (a
//!   refused tenant) shows no attempt number to the SDK.
//! - [`BACKOFF`]: between two fires with no counted attempt (`Running` commit)
//!   the backoff step grows strictly (FL-21) — fires do not grow without bound
//!   while the backoff stands still.
//! - [`LIVE`]: quiet state ⇒ every invocation is `completed`/`failed`, or
//!   `paused` 575 with enough attempts, or 577 from the rescue net, or waiting
//!   behind the VO lock of such an invocation (FL-4); and no message is stuck at
//!   the sender.
//! - [`NO_CALL_REFUSED`]: no SDK call while the tenant is refused permits —
//!   including a turn that got its permit before the suspension.
//! - [`ONE_DELIVERY`]: each call result is delivered at most once — delivered
//!   plus waiting rows never exceed the calls (IR-5).
//! - [`SOMETIMES_SERVED_RESTORING`]: a create or cancel command is served while
//!   rebuilding the VO locks has not finished — the model explores that gap.
//! - [`SOMETIMES_PARKED_LOCKED`]: a ticket is parked while another task holds
//!   its invocation lock — the model explores the gap where a ticket holds no lock.
//! - [`SOMETIMES_ADOPTED_HANDED`]: a new request adopts a **granted** ticket
//!   whose turn has not taken the lock yet — the model explores that window.
//! - [`SOMETIMES_EARLY_DRAIN`]: a state where the drainer has consumed a message
//!   whose receive commit is not durable yet — proof that the model explores
//!   that interleaving instead of stepping around it.
//!
//! ## Acceptance matrix (`tests/m4.rs::matrix`)
//!
//! Each flag of `cairn_lifecycle::Bugs` injects a bug into the **core**; the
//! checker must find a counterexample of exactly the property on the right.
//!
//! | Bug | Flag | Breaks |
//! |---|---|---|
//! | a running invocation sent back to its inbox (fixed: `Running` with the lock goes to `Suspended`) | `f017_running_to_inbox` | [`VO_EXCLUSIVE`] |
//! | a held VO lock dropped on throttle (fixed: dropping a permit releases only the permit) | `f018_drop_held_lock` | [`VO_EXCLUSIVE`] |
//! | attempts counted at fire time (before the first refund fix) | `f023_count_at_fire` | [`ATTEMPTS`] |
//! | refunding only throttled attempts (the first refund fix) | `f023_refund_only_throttled` | [`ATTEMPTS`] |
//! | FL-20 (the retry tick asking only the lock) | `fl20_lock_only` | [`NO_DUPLICATE`] |
//! | FL-21 regression (the fire not counting `fires`) | `fl21_no_fires` | [`BACKOFF`] |
//! | a timer ignoring a queued turn (suspicion A: the FL-20 shape at the timer tick) | `a_timer_ignores_queued` | [`NO_DUPLICATE`] |
//! | the same at the drainer (fixed with the lifecycle core) | `drain_ignores_queued` | [`NO_DUPLICATE`] |
//! | counting the attempt before `Running` (fixed with the lifecycle core) | `b_count_before_running` | [`ATTEMPTS`] |
//! | the reverse of the previous row (fixed with the lifecycle core) | `uncounted_watch` | [`ATTEMPTS`] |
//! | a pre-planned retry keeping its old position (fixed with the lifecycle core) | `planned_stale_position` | [`ATTEMPTS`] |
//! | cancel in two commits (fixed with the lifecycle core) | `d1_cancel_two_commits` | [`VALID`] |
//! | wait trees: "unknown" taken as "not satisfied" (injected) | `waiting_unknown_unsatisfied` | [`LIVE`] |
//! | wait trees: evaluating the tree without the new completion (injected) | `waiting_without_new` | [`LIVE`] |
//! | `200` before the receive commit is durable (injected, shell) | `ack_before_durable` | [`LIVE`] |
//! | calling the SDK before `Running` is durable (injected, shell) | `effect_before_durable` | [`ATTEMPTS`] |
//! | receive dedup only on the durable store (injected, shell) | `accept_checks_durable_only` | [`ONE_DELIVERY`] |
//! | an unfinished turn reported as terminal (shell) | `unfinished_turn_terminal` | [`VALID`] |
//! | a permit granted before the suspension still calls the SDK (core + shell) | `refused_calls_sdk` | [`NO_CALL_REFUSED`] |
//! | chunked restore: granting before rebuilding the locks finished (injected, shell) | `grant_before_restored` | [`VO_EXCLUSIVE`] |
//! | chunked restore: the RAM lock built in a hold after the classification (injected, shell) | `restore_lock_after_release` | [`LIVE`] |
//! | chunked drain: the drainer asks "is anyone on it" before the release point between two chunks, delivers after it (injected, shell) | `drain_attendance_across_release` | [`NO_DUPLICATE`] |
//! | parked tickets: a new request does not adopt the parked ticket (M4 found it while building the shell, then injected) | `adopt_keeps_ticket` | [`ATTEMPTS`] |
//! | parked tickets: granting a ticket while the invocation lock is busy (injected, shell) | `parked_grant_ignores_lock` | [`ONE_TURN`] |
//! | parked tickets: a nack loses the ticket (injected, shell) | `nack_drops_ticket` | [`LIVE`] |
//! | parked tickets: the ticket stays in the queue after the grant (injected, shell) | `parked_granted_twice` | [`NO_DUPLICATE`] |
//! | parked tickets: the ticket's turn skips the gate after the permit — a cancelled invocation's ticket still runs (injected, shell) | `parked_grant_skips_gate` | [`VALID`] |
//! | parked tickets: the retry tick moving a parked ticket's row far out clears `fires` (injected, core) | `parked_defer_resets_fires` | [`BACKOFF`] |
//! | a granted ticket's permit outside the ledger, a new request asks for another (the earlier shell) | `granted_out_of_ledger` | [`VO_EXCLUSIVE`] (and [`ONE_PERMIT`], even without a signal) |
//! | a failed `planned` turn keeping a watch row moved far out because of a parked ticket (the earlier core) | `planned_keeps_watch_row` | [`WAKE_BOUND`] |
//!
//! **The earlier shell broke three newer properties**: the permit outside the
//! ledger ([`ONE_PERMIT`], [`VO_EXCLUSIVE`]) — flag `granted_out_of_ledger`; it
//! breaks `ONE_PERMIT` even without a signal: a turn carrying a timer handed
//! over while the invocation lock is busy asks for a permit (`park` → `adopt`)
//! exactly when the same invocation's ticket has been granted. The far-out
//! watch row ([`WAKE_BOUND`], configuration `PARK_FAIL`) — core flag
//! `planned_keeps_watch_row`.
//!
//! `Bugs::BEFORE_3G_C` (`wake_ignores_waiting`: the drainer wakes on every
//! completion — the product before wait trees) keeps every property: right but
//! slow. `Bugs::BEFORE_W5R` (`restore_blocks_commands`: rebuild every lock
//! before the first command — the product before chunked restore) too.
//! `Bugs::BEFORE_W6Q` (`permit_wait_in_task`: a background turn waits for its
//! permit in the task, holding the invocation lock, and retries later when
//! throttled — the product before parked tickets) too; the four throttle-path
//! rows of the matrix (the held VO lock, counting at fire time, FL-21, counting
//! before `Running`) switch it on, because with parked tickets background turns
//! are not throttled at the permit request.
//!
//! **M4 found a flaw in the first parked-ticket shell** (fixed before landing,
//! flag `adopt_keeps_ticket`): a parked ticket releases the invocation lock, so
//! a turn carrying a timer that the background tick hands over while the
//! ticket's gate still holds the lock runs **before** the ticket; the ticket (a
//! pre-planned retry) then counts its attempts again from 0 at a journal
//! position the other turn has already counted. Fix: one entry per invocation in
//! the queue — a new request adopts the parked ticket.
//!
//! Outside the core, checked elsewhere:
//! - **Later arrivals jumping the lock holder** (a later arrival with a smaller
//!   `seq` going ahead of a lock holder waiting for a permit): the
//!   scheduler's grant order — `flow::Core`, not the lifecycle core. Covered by
//!   `crates/cairn-server/src/flow/tests/mod.rs::
//!   late_arrival_with_lower_seq_does_not_jump_lock_holder` and the FL-5 check at
//!   grant time of `flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`.
//! - **An idempotent resend waiting under the lock of its own waiting turn**
//!   (fixed by answering a resend with the same `Idempotency-Key` before taking
//!   the invocation lock): the ingress path, before the core (`admit`).
//! - `d2_restart_keeps_row` (harmless), `retry_soon_counts`, `policy_ignored`: M4
//!   has no restart, unloaded deployments or time — only the core's table pins them.
//!
//! **M4 found three flaws in the first version of the core** (fixed before
//! landing, one table row each): a watch row recorded the journal position
//! **before** the wake the same commit wrote; a turn with a pre-planned retry
//! whose journal grew while waiting for a permit kept the attempts of the old
//! position (today's product too — flag `planned_stale_position`); a throttled
//! turn carrying a timer treated the unwritten wake as progress and cleared the
//! counted attempts.
//!
//! ## Deliberately left out of the model
//!
//! - Serving order by `seq` (FL-5) and turn counters/caps (FL-3, FL-7): the
//!   scheduler here only has the VO lock; `flow/sim/` checks that part on the
//!   real `flow::Core`.
//! - Store read errors — except the status read at the gate after the permit
//!   ([`Action::GateFail`]) —, commits failing half-way (every commit is atomic —
//!   **not** durable at once: [`State::durable`]; a write losing its handle, LF-3,
//!   is absent), progress marks (FL-18), the L0 throttle (FL-11), quarantine
//!   (FL-14), restart, `continue_as_new`, unloaded deployments (`RetrySoon`),
//!   `Kill`: in the core's table, not in M4. An undeclared deployment
//!   (`NotFound`) is in: `Config::undeclared`.
//! - Time: the due time of retry/timer rows is not compared with a clock — every
//!   row can fire at any time (wider than reality, safe for safety properties).
//! - Parked tickets: the queue is **one** VO lock, gate and tenant — no turn cap,
//!   counter or `seq` order (`flow/sim/` checks them on the real `flow::Core`,
//!   tickets included). The order between a nacked ticket and a later `seq`
//!   (FL-5) therefore cannot be seen here. The ready queue (`Tickets::ready`,
//!   before `take_ready`) is not separated from the spawned task: `adopt` there
//!   returns the permit at once — the same outcome as [`Pc::Stale`] returning the
//!   permit at the next step. A client turn's or `resume`'s request adopts
//!   tickets like a background turn, but in M4 it never meets one (a client turn
//!   is only the first turn; `resume` needs `paused`); an external signal
//!   ([`Action::Signal`]) does. An external signal is not throttled at the
//!   permit request (its `Event::Throttled` is not modelled).

use {
    cairn_lifecycle::{
        self as lc, Attendance, Bugs, Contract, Event, FailedAt, Finish, Knobs, Need, OnMax,
        Outcome, Pause, Permit, Pinned, Plan, Release, RetryRow, RowChange, Seal, Side, Slot,
        Stage, StageChange, Status, Then, Turn, TurnErr, View, Vo, Waiting, WakeKind, WakeWrite,
        PARKED_DEFER_MS,
    },
    stateright::{Model, Property},
};

pub const N: usize = 2;

pub const VALID: &str = "valid(view) after every step";
pub const ONE_TURN: &str = "at most one lock holder and one SDK call per invocation";
pub const NO_DUPLICATE: &str = "at most one wake-less handed-out turn per invocation (FL-20, A)";
pub const VO_EXCLUSIVE: &str =
    "an EXCLUSIVE queue has at most one entered, unfinished invocation (FL-4)";
pub const ATTEMPTS: &str = "attempt counted <=> SDK called (FL-19, B, reverse B)";
pub const SEAL: &str = "Side::Seal of an ending exactly once per finished invocation";
pub const NEW_ATTEMPT: &str = "Side::NewAttempt strictly increases between progress";
pub const BACKOFF: &str =
    "backoff step strictly increases across fires without a counted attempt (FL-21)";
pub const LIVE: &str = "quiescent => every invocation finished or justifiably paused";
pub const SOMETIMES_DONE: &str = "sometimes: both invocations completed after a leader change";
pub const SOMETIMES_EXHAUSTED: &str = "sometimes: an invocation is paused 575 after max attempts";
pub const ONE_DELIVERY: &str =
    "each call result is delivered at most once (IR-5): delivered + pending rows <= calls";
pub const SOMETIMES_EARLY_DRAIN: &str =
    "sometimes: the drainer consumed a message whose accepting write is not durable";
pub const NO_CALL_REFUSED: &str =
    "no SDK call while the tenant is refused turns (H1), even with an earlier permit";
pub const SOMETIMES_SERVED_RESTORING: &str =
    "sometimes: a create or cancel was served while VO locks were being rebuilt";
pub const SOMETIMES_PARKED_LOCKED: &str =
    "sometimes: a parked ticket waits while another task holds its invocation lock";
pub const SOMETIMES_REFUSED_AFTER_PERMIT: &str =
    "sometimes: a turn that already held a permit was stopped before the SDK by a refusal";
pub const ONE_PERMIT: &str = "at most one live permit per invocation";
pub const WAKE_BOUND: &str =
    "a running invocation without a task or ticket has a wake within one backoff step or a rescue mark";
pub const SOMETIMES_ADOPTED_HANDED: &str =
    "sometimes: a new request adopted a ticket that was granted but had not taken its invocation lock";

/// Bits of properties recorded at transition time (cannot be read back from the state).
const V_VALID: u16 = 1;
const V_ATTEMPTS: u16 = 2;
const V_NEW_ATTEMPT: u16 = 4;
const V_BACKOFF: u16 = 8;
const V_VO: u16 = 16;
const V_REFUSED: u16 = 32;

/// The first turn of an invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Start {
    /// `/invoke`: a direct task holds the lock from creation, may wait for a
    /// permit as long as it takes (never throttled).
    Client,
    /// `/send`: handed to `Runner` — a background turn, can be throttled.
    Send,
}

/// The wake of a suspension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WakeBy {
    Timer,
    /// A completion through the drainer (`ctx.call` to another invocation, inbound).
    Drain,
    /// Both at once (`ctx.race(sleep, call)`): two wakes for one suspension —
    /// the shape of the timer-ignores-queued-turn bug.
    Both,
    /// Two drainer completions (`ctx.all`/`ctx.race` over two `ctx.call`s):
    /// `all` ⇒ an `ALL` tree needing both, otherwise `FIRST` needing one. An SDK
    /// called while the tree is not satisfied suspends again on the same tree.
    Two {
        all: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Config {
    pub exclusive: bool,
    pub start: [Start; N],
    /// Number of times the SDK returns `Retry` (a retryable failure) before it can progress.
    pub fails: [u8; N],
    /// Number of infrastructure errors after `Running` (`TurnFailed`).
    pub errors: [u8; N],
    /// Number of SDK turns that end without `End`/`Suspension`/`Error`: the
    /// machine has no outcome yet, the engine commits nothing.
    pub unfinished: [u8; N],
    /// The SDK suspends once waiting for this wake.
    pub sleeps: [Option<WakeBy>; N],
    pub max_attempts: Option<u32>,
    pub throttles: u8,
    pub leader_changes: u8,
    pub cancels: [bool; N],
    pub resumes: u8,
    /// Budget of the rescue net's `pause_stuck`.
    pub stuck: u8,
    /// No deployment declares the service (preflight `Pinned::Undeclared` ⇒
    /// `NotFound`) — for every invocation.
    pub undeclared: bool,
    /// The durability model: commits go into a non-durable tail, `Flush` is
    /// free, a leader change drops the tail. `false` ⇒ every commit is durable
    /// right after its action (the earlier model — a much smaller space;
    /// received messages still go through `Accept`/`Ack`).
    pub durable: bool,
    /// Number of times the tenant (of both invocations) is suspended and then
    /// lifted ([`Action::Refuse`], [`Action::Admit`]). `0` ⇒ the same space as before.
    pub refusals: u8,
    /// Invocations that do not exist at first: a create command ([`Action::Create`],
    /// `/invoke` or `/send` per [`Config::start`]) arrives **at any time** —
    /// including while the VO locks are being rebuilt after a leader change.
    pub late: [bool; N],
    /// An external signal (`POST …/signal`, cooperative cancel) to this
    /// invocation arrives **at any time** ([`Action::Signal`]): the task takes
    /// the invocation lock, then asks for a permit — adopting the same
    /// invocation's parked ticket.
    pub signals: [bool; N],
    /// Number of failed store reads at the gate after the permit
    /// ([`Action::GateFail`]): `engine.status` of `drive_with_permit` returns an
    /// error before the `Running` commit.
    pub gate_errors: u8,
}

impl Config {
    /// Nothing: two client turns, each SDK done at once.
    pub const QUIET: Config = Config {
        exclusive: true,
        start: [Start::Client, Start::Client],
        fails: [0, 0],
        errors: [0, 0],
        unfinished: [0, 0],
        sleeps: [None, None],
        max_attempts: None,
        throttles: 0,
        leader_changes: 0,
        cancels: [false, false],
        resumes: 0,
        stuck: 0,
        undeclared: false,
        durable: false,
        refusals: 0,
        late: [false, false],
        signals: [false, false],
        gate_errors: 0,
    };
}

/// Model configuration.
#[derive(Clone, Debug)]
pub struct M4 {
    pub bugs: Bugs,
    pub cfg: Config,
}

impl M4 {
    pub fn new(bugs: Bugs, cfg: Config) -> Self {
        M4 { bugs, cfg }
    }
}

/// The durable state (and ghost variables) of one invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Inv {
    pub status: Status,
    pub slot: Option<Slot>,
    pub row: Option<RetryRow>,
    pub len: u32,
    pub timer: bool,
    /// Number of **visible** (memtable) inbound rows waiting for the drainer to
    /// deliver to this invocation.
    pub completion: u8,
    /// Number of outgoing calls (`call` commands in the journal) whose results
    /// will come back through the drainer — written by the suspending commit.
    pub asked: u8,
    /// Number of messages (call results) received into the store in send order:
    /// message `m` received ⇔ `m < recv` — the dedup key of `accept_messages` (IR-5).
    pub recv: u8,
    /// **The sender** (the callee's range, durable, outside this range's
    /// store): messages produced — once the matching call is durable here (IR-1).
    pub issued: u8,
    /// **The sender**: messages that got `200` and had their outbox row deleted
    /// (IR-2). The sender's queue head is message number `acked`.
    pub acked: u8,
    /// Drainer completions delivered since the first suspension on the current
    /// tree (in the journal).
    pub got: u8,
    /// The two-completion wait tree ([`WakeBy::Two`]) the SDK is suspended on:
    /// the number of completions the tree needs (2 with `ALL`, 1 with `FIRST`).
    pub two: Option<u8>,
    // the rest of the environment
    pub fails: u8,
    pub errors: u8,
    pub unfinished: u8,
    pub sleep: Option<WakeBy>,
    // ghost variables
    pub calls: u32,
    pub lost: u32,
    pub entered: bool,
    pub finals: u8,
    pub last_attempt: u32,
    pub last_fire: Option<u32>,
}

impl Inv {
    /// Journal progress: retries counted from the start.
    fn progress(&mut self) {
        self.len += 1;
        self.calls = 0;
        self.lost = 0;
        self.last_attempt = 0;
        self.last_fire = None;
    }
}

/// Where a task is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pc {
    /// Handed over, not yet holding the invocation lock.
    Queued,
    /// Holds the lock, about to `Drive`.
    Drive,
    Waiting,
    /// A background turn's ticket parked in the scheduler's queue: no task, no
    /// invocation lock.
    Parked,
    /// A ticket **granted a permit** (`Scheduler::deliver` → `take_ready` →
    /// `run_granted` spawned) whose turn has not taken the invocation lock yet —
    /// the window between the two steps. The permit is alive, the VO lock taken
    /// (`held_before` at grant time), the ticket still `live`
    /// (`Attendance::parked`, a handed-over turn). Next step: take the
    /// invocation lock synchronously (free ⇒ `Granted`, busy ⇒ nack).
    Handed {
        held_before: bool,
    },
    /// A nacked ticket (the invocation lock was busy): permit returned, the
    /// ticket waits for the lock to be released (`Scheduler::lock_freed`), then
    /// asks again — back to [`Pc::Parked`].
    Deferred,
    /// Flag `granted_out_of_ledger` (the earlier shell): the task of a
    /// [`Pc::Handed`] ticket that a new request of the same invocation has
    /// adopted (`Scheduler::adopt`) — the permit sits in the task's future,
    /// outside the ledger's reach: the ticket is no longer `live`, the task still
    /// holds the permit until its first poll, then returns it (nack, or `claim`
    /// finding the ticket dead) per `held_before`. Today the permit stays in the
    /// ledger until `claim` and the new request takes it — this state does not exist.
    Stale {
        held_before: bool,
    },
    Granted {
        held_before: bool,
    },
    Began {
        rc: u32,
    },
    /// The core has decided to call the SDK (`Then::CallSdk`); waiting for the
    /// ticket to be durable (`turn/sdk.rs` `sync` before sending to the SDK).
    Ready {
        rc: u32,
    },
    InSdk {
        rc: u32,
    },
    Finished {
        rc: u32,
        outcome: Outcome,
    },
    Cancel,
    Dequeue,
    Resume,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Task {
    pub inv: u8,
    pub runner: bool,
    pub lock: bool,
    pub pc: Pc,
    pub turn: Turn,
    pub acked: bool,
    /// This turn's `Running` commit is still in the non-durable tail.
    pub fresh: bool,
}

impl Task {
    /// The ticket is still alive in the scheduler's ledger (`Scheduler::is_parked`).
    fn ticket(&self) -> bool {
        matches!(self.pc, Pc::Parked | Pc::Deferred | Pc::Handed { .. })
    }

    /// The task holds a live permit (`flow::Permit`) — granted, not released.
    fn permit(&self) -> bool {
        matches!(
            self.pc,
            Pc::Handed { .. }
                | Pc::Stale { .. }
                | Pc::Granted { .. }
                | Pc::Began { .. }
                | Pc::Ready { .. }
                | Pc::InSdk { .. }
                | Pc::Finished { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub invs: [Inv; N],
    pub tasks: Vec<Task>,
    pub vo: Option<u8>,
    pub marks: [bool; N],
    /// Rebuilding the term's VO locks has finished (the scheduler opens the grant gate).
    pub up: bool,
    /// The invocation's entry has been scanned by the lock rebuild in this term
    /// (the chunk containing it has passed).
    pub restored: [bool; N],
    /// Injected flag `restore_lock_after_release`: the chunk is classified (the
    /// lock must be built) but the RAM lock is not built yet.
    pub lock_pending: [bool; N],
    /// Injected flag `drain_attendance_across_release`: the `Attendance` the
    /// drainer asked for this invocation's row in the hold before the release point, not delivered yet.
    pub drain_seen: [Option<Attendance>; N],
    /// Create commands ([`Config::late`]) that have not arrived yet.
    pub creates: [bool; N],
    /// Ghost variable: a command (create, cancel) was served while the locks were
    /// being rebuilt ([`SOMETIMES_SERVED_RESTORING`]).
    pub served_restoring: bool,
    pub recovered: [bool; N],
    pub leader_changes: u8,
    pub throttles: u8,
    pub cancels: [bool; N],
    pub resumes: u8,
    pub stuck: u8,
    pub changed_leader: bool,
    /// The wait tree of the last suspension, **in the engine's RAM**: the number
    /// of drainer completions the tree needs to be satisfied. Lost on a leader
    /// change, removed when a turn starts (`load`).
    pub hints: [Option<u8>; N],
    /// The **durable** store, while writes are not durable yet: `invs` (the store
    /// part) is the visible store (memtable), `durable` the durable prefix. `None` = empty tail.
    pub durable: Option<[Inv; N]>,
    /// The tenant is refused permits: the scheduler grants none, the shell asks
    /// again at preflight and right before calling the SDK.
    pub refused: bool,
    /// Remaining [`Action::Refuse`] budget.
    pub refusals: u8,
    /// Ghost variable: a turn that **already had a permit** was stopped before the
    /// SDK because the tenant was refused ([`SOMETIMES_REFUSED_AFTER_PERMIT`]).
    pub stopped_by_refusal: bool,
    /// External signals ([`Config::signals`]) that have not arrived yet.
    pub signals: [bool; N],
    /// Remaining [`Action::GateFail`] budget.
    pub gate_errors: u8,
    /// Ghost variable: a new request adopted a **granted** ticket whose turn had
    /// not taken the lock yet ([`SOMETIMES_ADOPTED_HANDED`]).
    pub adopted_handed: bool,
    /// `V_*` bits of properties broken at transition time, per invocation (`<< 8`).
    pub viol: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SdkResult {
    Retry,
    Sleep,
    Complete,
    Error,
    /// A turn that ends without `End`/`Suspension`/`Error`.
    Unfinished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    TakeLock(usize),
    Act(usize),
    Grant(usize),
    /// The drainer holds the target's invocation lock (`claim`) exactly when a
    /// granted ticket's turn ([`Pc::Handed`]) takes that lock: the turn nacks,
    /// then the drainer delivers its row ([`Action::Drain`]).
    DrainNack(u8),
    /// The store read at the gate after the permit fails (`engine.status` of
    /// `drive_with_permit`, failpoint `turn::gate_turn`): the error goes straight
    /// out through `?`, no core event; `TurnPermit::drop` releases the permit keeping the VO lock.
    GateFail(usize),
    /// An external signal reaches the invocation ([`Config::signals`]).
    Signal(u8),
    Throttle(usize),
    Sdk(usize, SdkResult),
    TickRetry(u8),
    TickTimer(u8),
    Drain(u8),
    /// Injected flag `drain_attendance_across_release`: the drainer asks
    /// `Attendance` for this invocation's row, then releases the engine (delivers
    /// later at [`Action::Drain`]).
    DrainSee(u8),
    LeaderChange,
    /// Rebuild every lock in one hold, before the first command (flag
    /// `restore_blocks_commands` — the product before chunked restore).
    Restore,
    /// One chunk of the lock rebuild passes this invocation's entry.
    RestoreOne(u8),
    /// Injected flag `restore_lock_after_release`: build the RAM lock of a
    /// classified chunk in a later hold.
    RestoreLock(u8),
    /// A create command for the invocation ([`Config::late`]).
    Create(u8),
    Recover(u8),
    Rescue(u8, bool),
    Cancel(u8),
    Resume(u8),
    /// The receiver submits the receive commit of the sender's queue head.
    Accept(u8),
    /// The sender gets the `200` of its queue head.
    Ack(u8),
    /// Every submitted write becomes durable.
    Flush,
    /// The node applies the tenant suspension (`suspended`).
    Refuse,
    /// The node applies lifting the suspension (`active`).
    Admit,
}

/// The source of a wake that a plan writes or drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    None,
    Timer,
    Completion,
    /// An external signal carried by the turn — its content is in the command,
    /// not in any store row.
    Signal,
}

/// The due time of every retry is `0 + delay` (`View::now_ms` = 0): moving a
/// parked ticket's row far out gives it the due time [`PARKED_DEFER_MS`] (one
/// day) — [`WAKE_BOUND`] tells it apart from a due time ≤ one backoff step. Due
/// times are compared with a clock nowhere else.
const KNOBS: Knobs = Knobs {
    defer_ms: 0,
    watch_after_ms: 0,
    parked_defer_ms: PARKED_DEFER_MS,
};

impl M4 {
    fn exclusive_slot(&self, stage: Stage) -> Slot {
        Slot {
            stage,
            exclusive: self.cfg.exclusive,
        }
    }

    fn attendance(&self, s: &State, inv: u8, me: Option<usize>) -> Attendance {
        let others = s
            .tasks
            .iter()
            .enumerate()
            .filter(|(i, t)| t.inv == inv && Some(*i) != me);
        let mut a = Attendance::IDLE;
        for (_, t) in others {
            a.lock_held |= t.lock;
            a.queued |= t.runner;
            // A live ticket (`Scheduler::is_parked`: in the queue, granted but not
            // holding the lock yet, waiting for the lock): the retry tick moves its
            // row far out, as a watch row.
            a.parked |= t.ticket();
        }
        a.vo_held = s.vo == Some(inv);
        if let Some(i) = me {
            a.permit = match s.tasks[i].pc {
                Pc::Waiting => Permit::Waiting,
                Pc::Granted { held_before } => Permit::Granted { held_before },
                Pc::Began { .. } | Pc::Ready { .. } | Pc::InSdk { .. } | Pc::Finished { .. } => {
                    Permit::Started
                }
                _ => Permit::None,
            };
        }
        a
    }

    /// The shell: ask the core, answer every `Need` from the "store" (every read succeeds).
    fn ask(&self, s: &State, inv: u8, me: Option<usize>, e: Event) -> Plan {
        self.ask_as(s, inv, me, e, None, None)
    }

    /// Like [`Self::ask`], with a given answer (`Some`) to `Need::Status` —
    /// injected flags only.
    fn ask_status(
        &self,
        s: &State,
        inv: u8,
        me: Option<usize>,
        e: Event,
        status: Option<Status>,
    ) -> Plan {
        self.ask_as(s, inv, me, e, None, status)
    }

    /// Like [`Self::ask`], with an `Attendance` asked earlier (`Some`) instead of
    /// the current state's answer — injected flags only.
    fn ask_as(
        &self,
        s: &State,
        inv: u8,
        me: Option<usize>,
        e: Event,
        seen: Option<Attendance>,
        status: Option<Status>,
    ) -> Plan {
        let i = &s.invs[inv as usize];
        let attendance = seen.unwrap_or_else(|| self.attendance(s, inv, me));
        // The retry tick of a suspended tenant means nothing here (M4 has no time
        // domain): any value other than `None`.
        let mut v = View::new(0, KNOBS, attendance).with_tenant_refused(s.refused.then_some(1));
        for _ in 0..8 {
            let p = lc::step_mutated(&v, &e, &self.bugs);
            match p.need {
                None => return p,
                Some(Need::Status) => v.status = Some(status.unwrap_or(i.status)),
                Some(Need::Slot) => v.slot = Some(i.slot),
                Some(Need::Retry) => v.retry = Some(i.row),
                Some(Need::JournalLen) => v.journal_len = Some(i.len),
                Some(Need::Contract) => {
                    v.contract = Some(Contract {
                        pinned: if self.cfg.undeclared {
                            Pinned::Undeclared
                        } else {
                            Pinned::Found
                        },
                        max_attempts: self.cfg.max_attempts,
                        on_max: OnMax::Pause,
                    })
                }
                Some(Need::Policy { step }) => v = v.with_policy(step, None),
                Some(Need::Waiting) => v.waiting = Some(self.waiting(s, inv)),
            }
        }
        unreachable!("the core keeps asking for reads: {e:?}");
    }

    /// The shell of `Need::Waiting` (`turn/life.rs::Asker`): the RAM tree is
    /// evaluated on the delivered completions, before and after the one just
    /// arrived; no tree ⇒ unknown.
    fn waiting(&self, s: &State, inv: u8) -> Waiting {
        let got = s.invs[inv as usize].got;
        match s.hints[inv as usize] {
            None => Waiting::Unknown,
            Some(need) => Waiting::Known {
                before: got >= need,
                after: got.saturating_add(1) >= need,
            },
        }
    }

    /// Carry out the durable and RAM parts of `p` for `inv`; `me` is the running task.
    fn apply(&self, s: &mut State, inv: u8, p: &Plan, src: Source, me: Option<usize>) {
        let ix = inv as usize;
        let i = &mut s.invs[ix];
        if let Some(st) = p.status {
            i.status = st;
        }
        match p.stage {
            StageChange::Keep => {}
            StageChange::Insert(st) => i.slot = Some(self.exclusive_slot(st)),
            StageChange::Move(st) => {
                let slot = i.slot.as_mut().expect("Move needs an entry");
                slot.stage = st;
            }
            StageChange::Remove => i.slot = None,
        }
        match p.retry {
            RowChange::Keep => {}
            RowChange::Set(r) => i.row = Some(r),
            RowChange::Clear => i.row = None,
        }
        match p.wake {
            WakeWrite::None => {}
            WakeWrite::Deliver(_) => {
                match src {
                    Source::Timer => i.timer = false,
                    Source::Completion => {
                        i.completion -= 1;
                        i.got += 1;
                    }
                    Source::Signal => {}
                    Source::None => unreachable!("writing a wake without a source"),
                }
                i.progress();
            }
            WakeWrite::Discard => match src {
                Source::Timer => i.timer = false,
                Source::Completion => i.completion -= 1,
                Source::Signal => {}
                Source::None => unreachable!("dropping a wake without a source"),
            },
        }
        match p.side {
            None => {}
            Some(Side::Seal(Seal::Terminal | Seal::Restarted)) => i.finals += 1,
            Some(Side::Seal(Seal::Paused)) => {}
            // The SDK attempt **as seen**: recorded at the real call
            // ([`Self::call_sdk`], number `rc + 1` of `Pc::Ready`) — the shell may
            // stop the turn after preflight, before the call (a refused tenant),
            // and `life::side` does nothing with `NewAttempt`.
            Some(Side::NewAttempt { .. }) => {}
        }
        // RAM
        if p.ram.release == Release::All && s.vo == Some(inv) {
            s.vo = None;
        }
        match p.ram.vo {
            Vo::Keep => {}
            Vo::Restore => {
                if self.cfg.exclusive {
                    if s.vo.is_some_and(|h| h != inv) {
                        s.viol |= V_VO;
                    }
                    s.vo = Some(inv);
                }
            }
            Vo::Free => {
                if s.vo == Some(inv) {
                    s.vo = None;
                }
            }
        }
        match p.ram.rescue {
            lc::Mark::Clear | lc::Mark::Forget => s.marks[ix] = false,
            lc::Mark::None | lc::Mark::Later | lc::Mark::Strike => {}
        }
        if p.ram.duty == lc::Duty::Ack {
            if let Some(t) = me {
                s.tasks[t].acked = true;
            }
        }
        assert!(!p.ram.spawn_successor, "M4 has no restart");
        if let Some(turn) = p.ram.handout {
            s.tasks.push(Task {
                inv,
                runner: true,
                lock: false,
                pc: Pc::Queued,
                turn,
                // The pre-planned retry is durable: nobody has to remember it on its
                // behalf (`drive` acks right at the start for `RetryOwner::Caller`).
                acked: turn.planned.is_some(),
                fresh: false,
            });
        }
    }

    /// Task `t` is done: release the invocation lock; an unacked duty ⇒ a rescue mark.
    fn finish_task(&self, s: &mut State, t: usize) {
        let task = s.tasks.remove(t);
        if !task.acked {
            s.marks[task.inv as usize] = true;
        }
    }

    /// The task's next step per `then`.
    fn follow(&self, s: &mut State, t: usize, then: Then) {
        match then {
            Then::Done | Then::Reply(_) => self.finish_task(s, t),
            Then::RequestPermit => self.request_permit(s, t),
            Then::Preflight { retry_count } => {
                // The `Running` commit: one counted attempt — the backoff step is
                // measured again from here (FL-21 "the `Running` commit rewrites the 8-byte row").
                s.invs[s.tasks[t].inv as usize].last_fire = None;
                s.tasks[t].pc = Pc::Began { rc: retry_count };
                s.tasks[t].fresh = true;
            }
            Then::CallSdk { retry_count } => s.tasks[t].pc = Pc::Ready { rc: retry_count },
            Then::Drive(turn) => {
                s.tasks[t].pc = Pc::Drive;
                s.tasks[t].turn = turn;
            }
            Then::Dequeue => s.tasks[t].pc = Pc::Dequeue,
        }
    }

    /// Whether a permit request is granted at once: the VO lock is free (or its
    /// own), the tenant is not refused (FL-24), the grant gate is open (FL-8).
    fn grantable(&self, s: &State, inv: u8) -> bool {
        let free = !self.cfg.exclusive || s.vo.is_none_or(|h| h == inv);
        let gate = s.up || self.bugs.grant_before_restored;
        free && !s.refused && gate
    }

    /// Grant a permit to task `t` (`Provisional::secure`): take the VO lock.
    fn grant(&self, s: &mut State, t: usize) {
        let inv = s.tasks[t].inv;
        let held_before = s.vo == Some(inv);
        if self.cfg.exclusive {
            s.vo = Some(inv);
        }
        s.tasks[t].pc = Pc::Granted { held_before };
    }

    /// The core says request a permit (`Then::RequestPermit`). A client turn waits
    /// in its task, holding the invocation lock. A background turn
    /// (`turn::drive_dispatched`): the request and the grant are synchronous in the
    /// same engine hold as the gate — granted at once, it goes on in the task;
    /// otherwise it **parks a ticket** (`Pc::Parked`): the task ends, the
    /// invocation lock is released, the ticket is still a handed-over turn
    /// (`Runner::queued`). The earlier shell (`permit_wait_in_task`): waits in the
    /// task like a client turn.
    ///
    /// **One entry per invocation in the scheduler's queue**: a new request adopts
    /// the live ticket of the same invocation (`Scheduler::adopt`) — the new turn
    /// takes its place; an adopted ticket leaves no rescue mark (the new turn
    /// carries the wake duty). A **granted** ticket whose turn has not taken the
    /// lock yet ([`Pc::Handed`]): its permit stays in the ledger until `claim`, so
    /// the new request **takes that very permit** — `Granted { held_before }` of
    /// that grant, asking for no more. Flag `granted_out_of_ledger` (the earlier
    /// product): `adopt` cannot reach the permit in the spawned task's future —
    /// the task becomes [`Pc::Stale`], still holding the permit, and the new
    /// request asks for **another** permit.
    fn request_permit(&self, s: &mut State, mut t: usize) {
        let mut taken = None;
        if !self.bugs.adopt_keeps_ticket {
            let inv = s.tasks[t].inv;
            let mut i = 0;
            while i < s.tasks.len() {
                let o = s.tasks[i];
                if i == t || o.inv != inv {
                    i += 1;
                    continue;
                }
                match o.pc {
                    Pc::Parked | Pc::Deferred => {
                        s.tasks.remove(i);
                        if i < t {
                            t -= 1;
                        }
                    }
                    Pc::Handed { held_before } if self.bugs.granted_out_of_ledger => {
                        s.adopted_handed = true;
                        // The ticket leaves the ledger (no longer `live`, `Dispatched`
                        // acked); the permit stays with the task.
                        s.tasks[i].pc = Pc::Stale { held_before };
                        s.tasks[i].runner = false;
                        i += 1;
                    }
                    Pc::Handed { held_before } => {
                        s.adopted_handed = true;
                        // The ticket leaves the ledger with its permit: the permit moves to this request.
                        taken = Some(held_before);
                        s.tasks.remove(i);
                        if i < t {
                            t -= 1;
                        }
                    }
                    _ => i += 1,
                }
            }
        }
        let task = s.tasks[t];
        if let Some(held_before) = taken {
            s.tasks[t].pc = Pc::Granted { held_before };
        } else if !task.runner || self.bugs.permit_wait_in_task {
            s.tasks[t].pc = Pc::Waiting;
        } else if self.grantable(s, task.inv) {
            self.grant(s, t);
        } else {
            s.tasks[t].pc = Pc::Parked;
            s.tasks[t].lock = false;
        }
    }

    /// The scheduler grants a permit to parked ticket `t`: `secure` takes the VO
    /// lock, ticket + permit go into the ready queue, the serving loop spawns the
    /// turn — [`Pc::Handed`]. The turn takes the invocation lock at a later step
    /// ([`Self::claim_handed`]).
    fn grant_parked(&self, s: &mut State, t: usize) {
        if self.bugs.parked_granted_twice {
            // Injected: the ticket stays in the queue after the grant.
            let copy = s.tasks[t];
            s.tasks.push(copy);
        }
        let inv = s.tasks[t].inv;
        let held_before = s.vo == Some(inv);
        if self.cfg.exclusive {
            s.vo = Some(inv);
        }
        s.tasks[t].pc = Pc::Handed { held_before };
    }

    /// Return an **unused** permit of `inv` (`GiveBack`): `release_all`, unless
    /// the VO lock was held before the grant (`held_before`) — comparing locks by
    /// **invocation id**, not by permit (`ResourceManager::release_all`).
    fn give_back(&self, s: &mut State, inv: u8, held_before: bool) {
        if !held_before && s.vo == Some(inv) {
            s.vo = None;
        }
    }

    /// The turn of granted ticket `t` polls for the first time
    /// (`actor::run_granted`): take the invocation lock synchronously — free ⇒
    /// `claim`, go on from the gate after the permit (`Event::PermitGranted`);
    /// busy ⇒ nack ([`Self::nack`]).
    fn claim_handed(&self, s: &mut State, t: usize) {
        let Pc::Handed { held_before } = s.tasks[t].pc else {
            unreachable!()
        };
        let inv = s.tasks[t].inv;
        let lock_free = !s.tasks.iter().any(|o| o.inv == inv && o.lock);
        if !lock_free && !self.bugs.parked_grant_ignores_lock {
            self.nack(s, t);
            return;
        }
        s.tasks[t].lock = true;
        s.tasks[t].pc = Pc::Granted { held_before };
        // `drive_granted` does not ack early for a turn with a pre-planned retry:
        // the ticket's row may have been moved far out.
        s.tasks[t].acked = false;
        if self.bugs.parked_grant_skips_gate {
            // Injected: the gate after the permit uses the status read when the ticket parked (`running`).
            self.act_as(s, t, Some(Status::Running));
        }
    }

    /// **Nack** (`Scheduler::nack` → `Core::nack`): return the permit, **keep the
    /// VO lock** (`release_running` — earlier it returned per `held_before` at
    /// grant time, releasing the lock just taken: a later `seq` ran first; M4 has
    /// no `seq` so it cannot see that), the ticket waits for the invocation lock
    /// to be released, then asks again with the same `seq` ([`Pc::Deferred`]).
    /// Injected flag `nack_drops_ticket`: the ticket is dropped as if its work
    /// were done — no rescue mark.
    fn nack(&self, s: &mut State, t: usize) {
        debug_assert!(matches!(s.tasks[t].pc, Pc::Handed { .. }));
        if self.bugs.nack_drops_ticket {
            s.tasks.remove(t);
        } else {
            s.tasks[t].pc = Pc::Deferred;
        }
    }

    /// The task of an adopted ticket ([`Pc::Stale`]) polls for the first time:
    /// lock busy ⇒ nack, free ⇒ `claim` finds the ticket dead — both return the
    /// permit per `held_before` and end (the ticket was acked when adopted).
    fn stale_poll(&self, s: &mut State, t: usize) {
        let Pc::Stale { held_before } = s.tasks[t].pc else {
            unreachable!()
        };
        let inv = s.tasks[t].inv;
        s.tasks.remove(t);
        self.give_back(s, inv, held_before);
    }

    /// Call the SDK (`Pc::Ready` → `Pc::InSdk`): an outside effect — its ghost
    /// variable holds in **every** store version (visible and durable), because
    /// no leader change can take a call back.
    fn call_sdk(&self, s: &mut State, t: usize) {
        let Pc::Ready { rc } = s.tasks[t].pc else {
            unreachable!()
        };
        let ix = s.tasks[t].inv as usize;
        if s.refused {
            s.viol |= V_REFUSED << (8 * ix);
        }
        let i = &s.invs[ix];
        if rc != i.calls + i.lost {
            s.viol |= V_ATTEMPTS << (8 * ix);
        }
        // The preflight's `Side::NewAttempt { rc + 1 }` decided this call — the
        // attempt number the SDK sees, no leader change can take it back.
        if rc < i.last_attempt {
            s.viol |= V_NEW_ATTEMPT << (8 * ix);
        }
        let call = |i: &mut Inv| {
            i.calls += 1;
            i.last_fire = None;
            i.last_attempt = rc + 1;
        };
        call(&mut s.invs[ix]);
        if let Some(d) = s.durable.as_mut() {
            call(&mut d[ix]);
        }
        s.invs[ix].entered = true;
        s.tasks[t].pc = Pc::InSdk { rc };
    }

    /// Every submitted write becomes durable.
    fn flush(s: &mut State) {
        s.durable = None;
        for i in &mut s.invs {
            // The call is durable ⇒ the sender may send it (IR-1); its result
            // comes back to the callee's queue.
            i.issued = i.issued.max(i.asked);
        }
    }

    /// The durable store as its reader sees it: the durable prefix, or the
    /// visible store when the tail is empty.
    fn durable_inv(s: &State, ix: usize) -> &Inv {
        s.durable.as_ref().map_or(&s.invs[ix], |d| &d[ix])
    }

    fn turn_source(t: &Task) -> Source {
        match t.turn.wake {
            Some(WakeKind::Signal) => Source::Signal,
            Some(WakeKind::Completion) => Source::Timer,
            None => Source::None,
        }
    }

    fn act(&self, s: &mut State, t: usize) {
        self.act_as(s, t, None);
    }

    /// Like [`Self::act`]; `status = Some` replaces the store's answer to
    /// `Need::Status` — injected flags only.
    fn act_as(&self, s: &mut State, t: usize, status: Option<Status>) {
        let task = s.tasks[t];
        let inv = task.inv;
        let e = match task.pc {
            Pc::Drive => Event::Drive { turn: task.turn },
            Pc::Granted { .. } => Event::PermitGranted { turn: task.turn },
            Pc::Began { rc } => Event::TurnStarted {
                turn: task.turn,
                retry_count: rc,
            },
            Pc::Finished { rc, outcome } => Event::TurnEnded {
                turn: task.turn,
                retry_count: rc,
                outcome,
            },
            Pc::Cancel => Event::AdminCancel,
            Pc::Dequeue => Event::Dequeue,
            Pc::Resume => Event::AdminResume,
            Pc::Queued
            | Pc::Waiting
            | Pc::Parked
            | Pc::Deferred
            | Pc::Handed { .. }
            | Pc::Stale { .. }
            | Pc::Ready { .. }
            | Pc::InSdk { .. } => unreachable!(),
        };
        if task.pc == Pc::Cancel && !s.up {
            s.served_restoring = true;
        }
        if let Pc::Began { .. } = task.pc {
            // A turn loading the journal cold (`drive_one`: `load`) ⇒ the engine
            // removes the wait tree of the previous suspension.
            s.hints[inv as usize] = None;
        }
        let p = self.ask_status(s, inv, Some(t), e, status);
        self.apply(s, inv, &p, Self::turn_source(&task), Some(t));
        if let Pc::Began { rc } = task.pc {
            // Preflight gives up: the counted attempt must be one that was called.
            if p.status == Some(Status::Paused(Pause::MaxAttempts)) {
                let i = &s.invs[inv as usize];
                if rc != i.calls + i.lost {
                    s.viol |= V_ATTEMPTS << (8 * inv as usize);
                }
            }
        }
        if task.pc == Pc::Resume && p.status == Some(Status::Running) {
            // The operator lifts the pause: count again from the start.
            let i = &mut s.invs[inv as usize];
            i.calls = 0;
            i.lost = 0;
            i.last_attempt = 0;
            i.last_fire = None;
        }
        self.follow(s, t, p.then);
    }

    fn sdk(&self, s: &mut State, t: usize, r: SdkResult) {
        let task = s.tasks[t];
        let Pc::InSdk { rc } = task.pc else {
            unreachable!()
        };
        let inv = task.inv;
        let ix = inv as usize;
        let (finish, outcome) = match r {
            SdkResult::Error => {
                s.invs[ix].errors -= 1;
                let p = self.ask(
                    s,
                    inv,
                    Some(t),
                    Event::TurnFailed {
                        turn: task.turn,
                        at: FailedAt::AfterRunning { retry_count: rc },
                        err: TurnErr::Other,
                    },
                );
                self.apply(s, inv, &p, Source::None, Some(t));
                self.follow(s, t, p.then);
                return;
            }
            SdkResult::Unfinished => {
                s.invs[ix].unfinished -= 1;
                // The machine has no outcome ⇒ no `Finished`, no commit;
                // `settle_turn` reports one retryable failed attempt to the core.
                let outcome = if self.bugs.unfinished_turn_terminal {
                    Outcome::Failed
                } else {
                    Outcome::Retry { delay_ms: None }
                };
                s.tasks[t].pc = Pc::Finished { rc, outcome };
                return;
            }
            SdkResult::Retry => {
                s.invs[ix].fails -= 1;
                (Finish::Retry, Outcome::Retry { delay_ms: None })
            }
            SdkResult::Sleep => (Finish::Suspended { satisfied: false }, Outcome::Waiting),
            SdkResult::Complete => (Finish::Completed, Outcome::Completed),
        };
        let p = self.ask(s, inv, Some(t), Event::Finished { finish });
        self.apply(s, inv, &p, Source::None, Some(t));
        if r == SdkResult::Sleep {
            let i = &mut s.invs[ix];
            // The wait tree of this suspension: the number of drainer completions it needs.
            let need = match i.sleep.take() {
                // The `sleep`/`call` command into the journal in the same commit: a
                // timer row or a row waiting for the drainer.
                Some(by) => {
                    let need = match by {
                        WakeBy::Timer => {
                            i.timer = true;
                            u8::MAX
                        }
                        WakeBy::Drain => {
                            i.asked = 1;
                            1
                        }
                        WakeBy::Both => {
                            i.timer = true;
                            i.asked = 1;
                            1
                        }
                        WakeBy::Two { all } => {
                            i.asked = 2;
                            let need = if all { 2 } else { 1 };
                            i.two = Some(need);
                            need
                        }
                    };
                    i.got = 0;
                    i.progress();
                    need
                }
                // Suspending again on the same tree (`ctx.all` has only one new): no new command.
                None => i
                    .two
                    .expect("suspends again only while waiting on a two-completion tree"),
            };
            // The suspension's finishing commit (`apply_finish`) is done ⇒ the
            // engine remembers the tree.
            if p.status == Some(Status::Suspended) {
                s.hints[ix] = Some(need);
            }
        }
        s.tasks[t].pc = Pc::Finished { rc, outcome };
    }

    fn tick_retry(&self, s: &mut State, inv: u8) {
        let ix = inv as usize;
        let row = s.invs[ix].row.expect("has a row");
        let p = self.ask(s, inv, None, Event::RetryDue { row });
        if let (Some(turn), RowChange::Set(_)) = (p.ram.handout, p.retry) {
            if turn.planned.is_some() {
                // The backoff step of this fire (FL-21).
                let i = &s.invs[ix];
                let step = if i.len <= row.journal_len {
                    row.attempt + row.fires
                } else {
                    0
                };
                if i.last_fire.is_some_and(|prev| step <= prev) {
                    s.viol |= V_BACKOFF << (8 * ix);
                }
                s.invs[ix].last_fire = Some(step);
            }
        }
        self.apply(s, inv, &p, Source::None, None);
    }

    /// The first task of an invocation: `/invoke` holds the invocation lock from
    /// creation, `/send` is handed to `Runner`.
    fn first_task(&self, ix: usize) -> Task {
        match self.cfg.start[ix] {
            Start::Client => Task {
                inv: ix as u8,
                runner: false,
                lock: true,
                pc: Pc::Drive,
                turn: Turn::PLAIN,
                acked: false,
                fresh: false,
            },
            Start::Send => Task {
                inv: ix as u8,
                runner: true,
                lock: false,
                pc: Pc::Queued,
                turn: Turn::PLAIN,
                acked: false,
                fresh: false,
            },
        }
    }

    /// One chunk of the lock rebuild passes `inv`'s entry (`actor::restore_flow`
    /// in chunks): the chunk reads the `Invocations` index — finished (or not yet
    /// existing) invocations are not asked about —, asks the core's `LeaderUp`
    /// for the entry, writes the stage change and builds the RAM lock **in the
    /// same engine hold**.
    fn restore_one(&self, s: &mut State, inv: u8) {
        let ix = inv as usize;
        s.restored[ix] = true;
        if !s.invs[ix].status.is_live() {
            return;
        }
        let mut p = self.ask(s, inv, None, Event::LeaderUp);
        if self.bugs.restore_lock_after_release && p.ram.vo == Vo::Restore {
            // Injected: the lock is built in a later hold ([`Action::RestoreLock`]).
            p.ram.vo = Vo::Keep;
            s.lock_pending[ix] = true;
        }
        self.apply(s, inv, &p, Source::None, None);
    }

    /// The lock rebuild has finished: open the grant gate.
    fn maybe_up(s: &mut State) {
        if s.restored.iter().all(|r| *r) && !s.lock_pending.iter().any(|p| *p) {
            s.up = true;
        }
    }

    fn check_valid(&self, s: &mut State) {
        for ix in 0..N {
            let i = &s.invs[ix];
            let v = View::new(0, KNOBS, Attendance::IDLE)
                .with_status(i.status)
                .with_slot(i.slot)
                .with_retry(i.row);
            if lc::valid(&v).is_err() {
                s.viol |= V_VALID << (8 * ix);
            }
        }
    }

    /// Whether any action can change the state.
    fn quiescent(&self, s: &State) -> bool {
        let mut acts = Vec::new();
        self.actions(s, &mut acts);
        acts.into_iter().all(|a| self.next_state(s, a).is_none())
    }

    /// An acceptable outcome in a quiet state.
    fn settled(&self, s: &State, ix: usize) -> bool {
        let i = &s.invs[ix];
        // No message stuck at the sender.
        if i.acked < i.issued {
            return false;
        }
        match i.status {
            Status::Completed | Status::Failed => true,
            Status::Paused(Pause::MaxAttempts) => {
                self.cfg.max_attempts.is_some_and(|m| i.calls + i.lost >= m)
            }
            Status::Paused(Pause::InfraStuck) => true,
            _ => {
                // Waiting behind the VO lock of an invocation `paused` for a reason (FL-4).
                self.cfg.exclusive
                    && (0..N).any(|o| {
                        o != ix
                            && s.invs[o].status.is_paused()
                            && s.invs[o].slot.is_some_and(|sl| sl.stage.holds_lock())
                            && self.settled(s, o)
                    })
            }
        }
    }
}

/// The **store** part of one invocation — changing it is a write. Ghost variables
/// of outside effects (`calls`, `entered`, …), the environment and the sender do not count.
fn stored(i: &Inv) -> Stored {
    (
        (i.status, i.slot, i.row, i.len, i.timer, i.completion, i.got),
        (i.two, i.sleep, i.finals, i.asked, i.recv),
    )
}

type Stored = (
    (Status, Option<Slot>, Option<RetryRow>, u32, bool, u8, u8),
    (Option<u8>, Option<WakeBy>, u8, u8, u8),
);

fn bit(s: &State, v: u16) -> bool {
    s.viol & (v | v << 8) != 0
}

impl Model for M4 {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let mut tasks = Vec::new();
        let inv = |ix: usize| Inv {
            status: Status::Running,
            slot: Some(self.exclusive_slot(Stage::Inbox)),
            row: None,
            len: 1,
            timer: false,
            completion: 0,
            asked: 0,
            recv: 0,
            issued: 0,
            acked: 0,
            got: 0,
            two: None,
            fails: self.cfg.fails[ix],
            errors: self.cfg.errors[ix],
            unfinished: self.cfg.unfinished[ix],
            sleep: self.cfg.sleeps[ix],
            calls: 0,
            lost: 0,
            entered: false,
            finals: 0,
            last_attempt: 0,
            last_fire: None,
        };
        let mut invs = [inv(0), inv(1)];
        for (ix, i) in invs.iter_mut().enumerate() {
            if self.cfg.late[ix] {
                // Not yet created: no status, no entry, no task.
                i.status = Status::Missing;
                i.slot = None;
                continue;
            }
            tasks.push(self.first_task(ix));
        }
        vec![State {
            invs,
            tasks,
            vo: None,
            marks: [false; N],
            up: true,
            restored: [true; N],
            lock_pending: [false; N],
            drain_seen: [None; N],
            creates: self.cfg.late,
            served_restoring: false,
            recovered: [true; N],
            leader_changes: self.cfg.leader_changes,
            throttles: self.cfg.throttles,
            cancels: self.cfg.cancels,
            resumes: self.cfg.resumes,
            stuck: self.cfg.stuck,
            changed_leader: false,
            hints: [None; N],
            durable: None,
            refused: false,
            refusals: self.cfg.refusals,
            stopped_by_refusal: false,
            signals: self.cfg.signals,
            gate_errors: self.cfg.gate_errors,
            adopted_handed: false,
            viol: 0,
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        if !s.up {
            if self.bugs.restore_blocks_commands {
                // Before chunked restore: this phase reads no commands, one hold.
                out.push(Action::Restore);
                return;
            }
            // Chunked restore: the chunks follow index-key order — any order
            // relative to the invocations; commands, the drainer, background ticks
            // and `recover` slip in between two chunks.
            for ix in 0..N as u8 {
                if !s.restored[ix as usize] {
                    out.push(Action::RestoreOne(ix));
                }
                if s.lock_pending[ix as usize] {
                    out.push(Action::RestoreLock(ix));
                }
            }
        }
        for (t, task) in s.tasks.iter().enumerate() {
            let ix = task.inv as usize;
            let lock_free = !s.tasks.iter().any(|o| o.inv == task.inv && o.lock);
            match task.pc {
                Pc::Queued | Pc::Cancel | Pc::Resume if !task.lock => {
                    if lock_free {
                        out.push(Action::TakeLock(t));
                    }
                }
                Pc::Waiting => {
                    // A suspended tenant: the first tier of `poll_acquire` is
                    // `ResourceKind::Tenant` — no grant (FL-24). The lock rebuild
                    // has not finished: the scheduler's gate is closed (FL-8,
                    // `Scheduler::acquire` waits before entering the core).
                    if self.grantable(s, task.inv) {
                        out.push(Action::Grant(t));
                    }
                    // Throttle (the wait slice ran out because of the budget): only
                    // the earlier shell — background turns no longer wait in the task (a parked ticket never gives up).
                    if task.runner && s.throttles > 0 {
                        out.push(Action::Throttle(t));
                    }
                }
                Pc::Parked => {
                    // The grant does not look at the invocation lock: the ticket's
                    // turn takes it at a later step ([`Pc::Handed`]).
                    if self.grantable(s, task.inv) {
                        out.push(Action::Grant(t));
                    }
                }
                // The ticket waits for the invocation lock to be released, then asks again.
                Pc::Deferred => {
                    if lock_free {
                        out.push(Action::Act(t));
                    }
                }
                Pc::Granted { .. } => {
                    out.push(Action::Act(t));
                    if task.runner && s.throttles > 0 && self.bugs.permit_wait_in_task {
                        out.push(Action::Throttle(t));
                    }
                    if s.gate_errors > 0 {
                        out.push(Action::GateFail(t));
                    }
                }
                Pc::InSdk { .. } => {
                    let i = &s.invs[ix];
                    if i.fails > 0 {
                        out.push(Action::Sdk(t, SdkResult::Retry));
                    } else if i.sleep.is_some() || i.two.is_some_and(|need| i.got < need) {
                        out.push(Action::Sdk(t, SdkResult::Sleep));
                    } else {
                        out.push(Action::Sdk(t, SdkResult::Complete));
                    }
                    if i.errors > 0 {
                        out.push(Action::Sdk(t, SdkResult::Error));
                    }
                    if i.unfinished > 0 {
                        out.push(Action::Sdk(t, SdkResult::Unfinished));
                    }
                }
                _ => out.push(Action::Act(t)),
            }
        }
        for ix in 0..N as u8 {
            let i = &s.invs[ix as usize];
            if i.row.is_some() {
                out.push(Action::TickRetry(ix));
            }
            if i.timer {
                out.push(Action::TickTimer(ix));
            }
            if i.completion > 0 {
                if self.bugs.drain_attendance_across_release && s.drain_seen[ix as usize].is_none()
                {
                    out.push(Action::DrainSee(ix));
                } else {
                    out.push(Action::Drain(ix));
                    let handed = s
                        .tasks
                        .iter()
                        .any(|o| o.inv == ix && matches!(o.pc, Pc::Handed { .. }));
                    let lock_free = !s.tasks.iter().any(|o| o.inv == ix && o.lock);
                    if handed && lock_free {
                        out.push(Action::DrainNack(ix));
                    }
                }
            }
            if s.signals[ix as usize] {
                out.push(Action::Signal(ix));
            }
            if i.issued > i.acked {
                // The sender sends (again) its queue head until it gets `200`.
                out.push(Action::Accept(ix));
                let seen = if self.bugs.ack_before_durable {
                    i.recv
                } else {
                    Self::durable_inv(s, ix as usize).recv
                };
                if seen > i.acked {
                    out.push(Action::Ack(ix));
                }
            }
            if !s.recovered[ix as usize] {
                out.push(Action::Recover(ix));
            }
            if s.marks[ix as usize] {
                out.push(Action::Rescue(ix, false));
                if s.stuck > 0 {
                    out.push(Action::Rescue(ix, true));
                }
            }
            if s.cancels[ix as usize] {
                out.push(Action::Cancel(ix));
            }
            if s.resumes > 0 && i.status.is_paused() {
                out.push(Action::Resume(ix));
            }
            if s.creates[ix as usize] {
                out.push(Action::Create(ix));
            }
        }
        if s.leader_changes > 0 {
            out.push(Action::LeaderChange);
        }
        if s.durable.is_some() {
            out.push(Action::Flush);
        }
        if s.refused {
            out.push(Action::Admit);
        } else if s.refusals > 0 {
            out.push(Action::Refuse);
        }
    }

    fn next_state(&self, s0: &State, a: Action) -> Option<State> {
        let mut s = s0.clone();
        let before = s.invs;
        match a {
            Action::TakeLock(t) => {
                let task = &mut s.tasks[t];
                task.lock = true;
                if task.pc == Pc::Queued {
                    task.pc = Pc::Drive;
                }
            }
            // Calling the SDK demands a durable ticket: every submitted write must be
            // durable (`sync` captures the latest mark when asked — empty tail).
            Action::Act(t) if matches!(s.tasks[t].pc, Pc::Ready { .. }) => {
                if s.durable.is_some() && !self.bugs.effect_before_durable {
                    return None;
                }
                if s.refused && !self.bugs.refused_calls_sdk {
                    // The shell asks again right before the call, after waiting for
                    // durability: `Step::Refused` ⇒ `drive_one` reruns the preflight.
                    let Pc::Ready { rc } = s.tasks[t].pc else {
                        unreachable!()
                    };
                    s.tasks[t].pc = Pc::Began { rc };
                    s.stopped_by_refusal = true;
                } else {
                    self.call_sdk(&mut s, t);
                }
            }
            Action::Act(t) if matches!(s.tasks[t].pc, Pc::Handed { .. }) => {
                self.claim_handed(&mut s, t)
            }
            Action::Act(t) if matches!(s.tasks[t].pc, Pc::Stale { .. }) => {
                self.stale_poll(&mut s, t)
            }
            // The lock was just released (`Scheduler::requeue_freed`): the ticket asks again with the same `seq`.
            Action::Act(t) if s.tasks[t].pc == Pc::Deferred => s.tasks[t].pc = Pc::Parked,
            Action::Act(t) => self.act(&mut s, t),
            Action::Grant(t) if s.tasks[t].pc == Pc::Parked => self.grant_parked(&mut s, t),
            Action::Grant(t) => self.grant(&mut s, t),
            Action::GateFail(t) => {
                s.gate_errors -= 1;
                // `TurnPermit::drop` releases the permit keeping the VO lock (FL-4). A
                // turn with a pre-planned retry reports `TurnFailed { BeforeRunning }`
                // to the core (a watch row moved far out because of a parked ticket ⇒
                // the core writes a near retry again); other turns: the error goes
                // straight out, the duty drops unacked ⇒ a rescue mark.
                let task = s.tasks[t];
                if task.turn.planned.is_some() {
                    let p = self.ask(
                        &s,
                        task.inv,
                        Some(t),
                        Event::TurnFailed {
                            turn: task.turn,
                            at: FailedAt::BeforeRunning,
                            err: TurnErr::Other,
                        },
                    );
                    self.apply(&mut s, task.inv, &p, Source::None, Some(t));
                }
                self.finish_task(&mut s, t);
            }
            Action::Signal(inv) => {
                s.signals[inv as usize] = false;
                // `deliver_signal_and_drive`: take the invocation lock, then
                // `drive` with the signal as the wake the turn carries.
                s.tasks.push(Task {
                    inv,
                    runner: false,
                    lock: false,
                    pc: Pc::Queued,
                    turn: Turn::woken(WakeKind::Signal),
                    acked: false,
                    fresh: false,
                });
            }
            Action::Throttle(t) => {
                s.throttles -= 1;
                let task = s.tasks[t];
                let p = self.ask(
                    &s,
                    task.inv,
                    Some(t),
                    Event::Throttled {
                        turn: task.turn,
                        after_ms: 0,
                    },
                );
                self.apply(&mut s, task.inv, &p, Source::None, Some(t));
                self.follow(&mut s, t, p.then);
            }
            Action::Sdk(t, r) => self.sdk(&mut s, t, r),
            Action::TickRetry(inv) => self.tick_retry(&mut s, inv),
            Action::TickTimer(inv) => {
                let in_flight = s
                    .tasks
                    .iter()
                    .any(|t| t.inv == inv && t.turn.wake == Some(WakeKind::Completion));
                let p = self.ask(&s, inv, None, Event::TimerDue { in_flight });
                self.apply(&mut s, inv, &p, Source::Timer, None);
            }
            Action::Drain(inv) | Action::DrainNack(inv) => {
                // Injected flag: the "is anyone on it" answer asked in the hold before the release point.
                let seen = s.drain_seen[inv as usize].take();
                let p = self.ask_as(
                    &s,
                    inv,
                    None,
                    Event::CompletionArrived {
                        wake: WakeKind::Completion,
                    },
                    seen,
                    None,
                );
                self.apply(&mut s, inv, &p, Source::Completion, None);
                if let Action::DrainNack(_) = a {
                    // A granted ticket's turn polls while the drainer holds the
                    // target's lock (the grant came from a release in another range,
                    // a rate tick, lifting a suspension): nack — **after** the drainer
                    // has asked "is anyone on it" (the ticket is there ⇒ no turn handed over).
                    if let Some(t) = s
                        .tasks
                        .iter()
                        .position(|o| o.inv == inv && matches!(o.pc, Pc::Handed { .. }))
                    {
                        self.nack(&mut s, t);
                    }
                }
            }
            Action::DrainSee(inv) => {
                s.drain_seen[inv as usize] = Some(self.attendance(&s, inv, None));
            }
            Action::LeaderChange => {
                s.leader_changes -= 1;
                // The non-durable tail is lost; the new leader replays the durable
                // prefix. The sender and outside effects (the SDK has read state) are not taken back.
                if let Some(d) = s.durable.take() {
                    for (cur, old) in s.invs.iter_mut().zip(d) {
                        *cur = Inv {
                            fails: cur.fails,
                            errors: cur.errors,
                            unfinished: cur.unfinished,
                            entered: cur.entered,
                            issued: cur.issued,
                            acked: cur.acked,
                            ..old
                        };
                    }
                }
                for t in &s.tasks {
                    // An attempt counted at a **durable** `Running`, the SDK not called
                    // yet (FL-19: "dying after that point, the attempt has been counted").
                    if matches!(t.pc, Pc::Began { .. } | Pc::Ready { .. }) && !t.fresh {
                        s.invs[t.inv as usize].lost += 1;
                    }
                }
                s.tasks.clear();
                s.vo = None;
                s.marks = [false; N];
                s.up = false;
                s.restored = [false; N];
                s.lock_pending = [false; N];
                s.drain_seen = [None; N];
                // A create command whose commit was lost with the non-durable tail
                // was not answered (ack only after durability): the caller resends.
                for ix in 0..N {
                    if self.cfg.late[ix] && s.invs[ix].status == Status::Missing {
                        s.creates[ix] = true;
                    }
                }
                s.recovered = [false; N];
                s.hints = [None; N];
                s.changed_leader = true;
            }
            Action::Restore => {
                for inv in 0..N as u8 {
                    let p = self.ask(&s, inv, None, Event::LeaderUp);
                    self.apply(&mut s, inv, &p, Source::None, None);
                }
                s.restored = [true; N];
                s.up = true;
            }
            Action::RestoreOne(inv) => {
                self.restore_one(&mut s, inv);
                Self::maybe_up(&mut s);
            }
            Action::RestoreLock(inv) => {
                s.lock_pending[inv as usize] = false;
                if self.cfg.exclusive {
                    if s.vo.is_some_and(|h| h != inv) {
                        s.viol |= V_VO;
                    }
                    s.vo = Some(inv);
                }
                Self::maybe_up(&mut s);
            }
            Action::Create(inv) => {
                let ix = inv as usize;
                s.creates[ix] = false;
                if !s.up {
                    s.served_restoring = true;
                }
                // The core's `Submitted`: `running`, an `Inbox` entry — one commit.
                let i = &mut s.invs[ix];
                i.status = Status::Running;
                i.slot = Some(self.exclusive_slot(Stage::Inbox));
                s.tasks.push(self.first_task(ix));
            }
            Action::Recover(inv) => {
                s.recovered[inv as usize] = true;
                let p = self.ask(&s, inv, None, Event::Recover);
                self.apply(&mut s, inv, &p, Source::None, None);
            }
            Action::Rescue(inv, stuck) => {
                let p = self.ask(
                    &s,
                    inv,
                    None,
                    Event::RescueRound {
                        marked: true,
                        stuck,
                    },
                );
                if stuck {
                    // A round that does not choose `pause_stuck`: spends no budget (the
                    // same state as a round below the threshold).
                    p.status?;
                    s.stuck -= 1;
                }
                self.apply(&mut s, inv, &p, Source::None, None);
            }
            Action::Cancel(inv) => {
                s.cancels[inv as usize] = false;
                s.tasks.push(Task {
                    inv,
                    runner: false,
                    lock: false,
                    pc: Pc::Cancel,
                    turn: Turn::PLAIN,
                    acked: true,
                    fresh: false,
                });
            }
            Action::Accept(inv) => {
                let ix = inv as usize;
                let m = s.invs[ix].acked;
                // Dedup by a key derived from the content (IR-5): a message already
                // received — still a row or delivered — adds no row.
                let seen = if self.bugs.accept_checks_durable_only {
                    Self::durable_inv(&s, ix).recv
                } else {
                    s.invs[ix].recv
                };
                if m >= seen {
                    let i = &mut s.invs[ix];
                    i.completion += 1;
                    i.recv = i.recv.max(m + 1);
                }
            }
            Action::Ack(inv) => s.invs[inv as usize].acked += 1,
            Action::Flush => Self::flush(&mut s),
            // The tenant suspension/lift is applied on the node: a new generation of
            // the tenant-state source — the scheduler syncs at the next permit request, the shell at its next question.
            Action::Refuse => {
                s.refusals -= 1;
                s.refused = true;
            }
            Action::Admit => s.refused = false,
            Action::Resume(inv) => {
                s.resumes -= 1;
                s.tasks.push(Task {
                    inv,
                    runner: false,
                    lock: false,
                    pc: Pc::Resume,
                    turn: Turn::PLAIN,
                    acked: false,
                    fresh: false,
                });
            }
        }
        // A write: the store part of one invocation changes ⇒ the non-durable tail
        // starts from the snapshot before it (the durable prefix stays if the tail exists).
        if !matches!(a, Action::LeaderChange | Action::Flush)
            && s.durable.is_none()
            && before
                .iter()
                .zip(&s.invs)
                .any(|(was, now)| stored(was) != stored(now))
        {
            s.durable = Some(before);
        }
        // A turn with a **durable** `Running` leaves preflight without calling the
        // SDK (giving up, `NotFound`, …) through a write still in the tail: in the
        // durable version, the attempt is counted while the SDK was not called —
        // exactly the "died after `Running`" case of FL-19 if the tail is lost.
        if let Action::Act(t) = a {
            let task = s0.tasks[t];
            let to_sdk = s
                .tasks
                .get(t)
                .is_some_and(|n| n.inv == task.inv && matches!(n.pc, Pc::Ready { .. }));
            if matches!(task.pc, Pc::Began { .. }) && !task.fresh && !to_sdk {
                if let Some(d) = s.durable.as_mut() {
                    d[task.inv as usize].lost += 1;
                }
            }
        }
        // No durability model: everything is durable right after its action, and
        // call results reach the caller's queue at once (as in the earlier model):
        // with the receive commit durable at once, receiving and acking split or
        // merged give the same set of states — resending a received message is just a no-op.
        if !self.cfg.durable {
            Self::flush(&mut s);
            for i in &mut s.invs {
                while i.acked < i.issued {
                    let m = i.acked;
                    if m >= i.recv {
                        i.completion += 1;
                        i.recv = m + 1;
                    }
                    i.acked += 1;
                }
            }
        }
        if s.durable.is_none() {
            for t in &mut s.tasks {
                t.fresh = false;
            }
        }
        self.check_valid(&mut s);
        (s != *s0).then_some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(VALID, |_, s: &State| !bit(s, V_VALID)),
            Property::always(ONE_TURN, |_, s: &State| {
                (0..N as u8).all(|inv| {
                    let mine = || s.tasks.iter().filter(move |t| t.inv == inv);
                    mine().filter(|t| t.lock).count() <= 1
                        && mine().filter(|t| matches!(t.pc, Pc::InSdk { .. })).count() <= 1
                })
            }),
            Property::always(NO_DUPLICATE, |_, s: &State| {
                (0..N as u8).all(|inv| {
                    s.tasks
                        .iter()
                        .filter(|t| t.inv == inv && t.runner && t.turn.wake.is_none())
                        .count()
                        <= 1
                })
            }),
            Property::always(VO_EXCLUSIVE, |m: &M4, s: &State| {
                !bit(s, V_VO)
                    && (!m.cfg.exclusive
                        || s.invs
                            .iter()
                            .filter(|i| i.entered && i.status.is_live())
                            .count()
                            <= 1)
            }),
            Property::always(ONE_PERMIT, |_, s: &State| {
                (0..N as u8).all(|inv| {
                    s.tasks
                        .iter()
                        .filter(|t| t.inv == inv && t.permit())
                        .count()
                        <= 1
                })
            }),
            Property::always(WAKE_BOUND, |_, s: &State| {
                (0..N).all(|ix| {
                    let i = &s.invs[ix];
                    // Someone is on it (a task, a ticket), a rescue mark, the term's
                    // `recover` has not run, or a wake is waiting for delivery.
                    i.status != Status::Running
                        || s.tasks.iter().any(|t| t.inv as usize == ix)
                        || s.marks[ix]
                        || !s.recovered[ix]
                        || i.completion > 0
                        || i.timer
                        || i.row.is_some_and(|r| r.due < PARKED_DEFER_MS)
                })
            }),
            Property::sometimes(SOMETIMES_ADOPTED_HANDED, |_, s: &State| s.adopted_handed),
            Property::always(ATTEMPTS, |_, s: &State| !bit(s, V_ATTEMPTS)),
            Property::always(SEAL, |_, s: &State| {
                // Not yet created ([`Config::late`]) is not finished.
                s.invs.iter().all(|i| {
                    i.finals <= 1
                        && (i.status.is_live() || i.status == Status::Missing || i.finals == 1)
                })
            }),
            Property::always(NEW_ATTEMPT, |_, s: &State| !bit(s, V_NEW_ATTEMPT)),
            Property::always(BACKOFF, |_, s: &State| !bit(s, V_BACKOFF)),
            Property::always(LIVE, |m: &M4, s: &State| {
                !m.quiescent(s) || (0..N).all(|ix| m.settled(s, ix))
            }),
            Property::always(ONE_DELIVERY, |_, s: &State| {
                s.invs
                    .iter()
                    .all(|i| u16::from(i.got) + u16::from(i.completion) <= u16::from(i.asked))
            }),
            Property::sometimes(SOMETIMES_EARLY_DRAIN, |_, s: &State| {
                // More messages delivered (received, then consumed) than durable receipts.
                s.durable.as_ref().is_some_and(|d| {
                    s.invs
                        .iter()
                        .zip(d)
                        .any(|(i, d)| i.recv.saturating_sub(i.completion) > d.recv)
                })
            }),
            Property::always(NO_CALL_REFUSED, |_, s: &State| !bit(s, V_REFUSED)),
            Property::sometimes(SOMETIMES_REFUSED_AFTER_PERMIT, |_, s: &State| {
                s.stopped_by_refusal
            }),
            Property::sometimes(SOMETIMES_SERVED_RESTORING, |_, s: &State| {
                s.served_restoring
            }),
            // A parked ticket holds no invocation lock: another command (cancel)
            // holds its lock while the ticket waits — the model explores that gap.
            Property::sometimes(SOMETIMES_PARKED_LOCKED, |_, s: &State| {
                s.tasks
                    .iter()
                    .any(|p| p.pc == Pc::Parked && s.tasks.iter().any(|o| o.inv == p.inv && o.lock))
            }),
            Property::sometimes(SOMETIMES_DONE, |_, s: &State| {
                s.changed_leader && s.invs.iter().all(|i| i.status == Status::Completed)
            }),
            Property::sometimes(SOMETIMES_EXHAUSTED, |_, s: &State| {
                s.invs
                    .iter()
                    .any(|i| i.status == Status::Paused(Pause::MaxAttempts))
            }),
        ]
    }
}
