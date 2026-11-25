// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Running a turn: `gate_turn`, `drive*`, `after_run`, `capture_run`, `TurnPermit`,
//! `drive_with_permit`.

use super::*;

/// **Durable state gate** right before letting a turn run (lifecycle core `Event::Drive`
/// before the slot, `Event::PermitGranted` after the slot).
///
/// Every background path gets here through a decision made on data read before a point
/// where the engine is released — waiting for the invocation lock, waiting for a slot — so
/// by the time it arrives the invocation may have changed. Measured: a late cross-partition
/// completion for an invocation that had already finished made the SDK get called for one
/// more turn (`tests/stale_state.rs`).
///
/// - **Already ended**: drop the source row of the wake-up (if any) and return the durable
///   outcome — no turn runs. Waking a terminal invocation is rerunning it.
/// - **Paused**: the completion/signal is **written** into the journal, but no turn runs.
///   Paused means "waiting for a human"; only `/resume` lets it run on.
/// - **Missing**: the error `load` returns (`NoJournal`), nothing scheduled.
///
/// This function is the "don't run" part of the Plan: write (or drop) the wake-up the turn
/// carries, according to `wake`.
pub(super) async fn write_gate_wake(
    engine: &mut Lease,
    id: InvocationId,
    wake: lc::WakeWrite,
    pending: &mut Option<(Wake, Source)>,
) -> Result<()> {
    match wake {
        lc::WakeWrite::None => {}
        lc::WakeWrite::Discard => {
            let (wake, source) = pending.take().expect("Discard ⇒ a wake is pending");
            let cid = match &wake {
                Wake::Completion(c) => c.id,
                Wake::Signal(_) => 0,
            };
            engine.discard_source(&source, &id, cid).await?;
        }
        lc::WakeWrite::Deliver(_) => match pending.take().expect("Deliver ⇒ a wake is pending") {
            (Wake::Completion(c), source) => {
                settle_completion(engine, id, c, source).await?;
            }
            (Wake::Signal(sig), source) => {
                let mut inv = engine.load(id, params_for(&id)).await?;
                engine.step(&mut inv, Event::Start).await?;
                engine.deliver_signal(&mut inv, sig, source).await?;
            }
        },
    }
    Ok(())
}

/// The outcome the gate returns when it does not let the turn run, from the full state read.
pub(super) fn gate_outcome(asker: &life::Asker, plan: &lc::Plan) -> Result<InvokeOutcome> {
    match (plan.then, asker.status.clone().flatten()) {
        (lc::Then::Reply(lc::Reply::Ended), Some(InvocationStatus::Completed(v))) => {
            Ok(InvokeOutcome::Completed(v))
        }
        (lc::Then::Reply(lc::Reply::Ended), Some(InvocationStatus::Failed { code, message })) => {
            Ok(InvokeOutcome::Failed { code, message })
        }
        (lc::Then::Reply(lc::Reply::Paused), Some(InvocationStatus::Paused { code, message })) => {
            Ok(InvokeOutcome::Paused { code, message })
        }
        // No invocation: exactly the error `load` used to return — nothing scheduled for an
        // id that does not exist.
        (lc::Then::Reply(lc::Reply::NotFound), _) => {
            Err(cairn_engine::EngineError::NoJournal.into())
        }
        (then, status) => unreachable!("gate: {then:?} with {status:?}"),
    }
}

/// Run turns until the invocation stops for good or has to wait for something outside.
///
/// `pending` is the completion that just became ready (timer or outbox), if any.
///
/// `guard` is `id`'s lock, and the caller must already hold it — every caller has work to
/// do under that lock *before* driving (idempotency lookup, state check), so winning the
/// lock in here would be winning it one tick late.
///
/// Takes `&mut Option` rather than by value because this function **must be able to
/// release it**: moving on in an `EXCLUSIVE` queue means the next invocation's lock, and
/// holding two invocation locks at once is what rule 3 of [`crate::lease`] forbids.
///
/// `duty` is the turn's wake-up duty (nack by default): this function acks it at exactly
/// the places where the invocation already has a durable wake-up or no longer needs one;
/// every other exit — including a `?` nobody thought of, a panic, a cancellation — lets it
/// fall unacked, and the safety net is marked (FL-16).
///
/// `turn` is the turn according to the lifecycle core (`cairn_lifecycle::Turn`): whether
/// there is a reschedule set beforehand (`planned`), whether to count again from zero
/// (`reset`, `resume`). The wake-up it carries comes from `pending`. There is no
/// `RetryOwner` any more: every turn is subject to the same attempt rules.
pub async fn drive<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    pending: Option<(Wake, Source)>,
    guard: &mut Option<InvGuard<'c>>,
    turn: lc::Turn,
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    debug_assert_eq!(duty.id(), id, "wake duty belongs to this invocation");
    let turn = life::carrying(turn, pending.as_ref());
    // The next reschedule is already durable before the turn (a turn handed over by a
    // firing reschedule): nobody has to remember it on its behalf, even if the turn panics.
    if turn.planned.is_some() {
        duty.ack();
    }
    let (out, trace) =
        crate::stats::traced(drive_untraced(engine, ctx, id, pending, guard, turn, duty)).await;
    ack_unless_lost(&turn, &out, duty);
    after_run(engine, ctx, id, &out, &trace).await;
    out
}

/// An error that does not drop the wake-up (write authority lost — [`marks_rescue`]) ends
/// the wake-up duty: the next term's `recover` takes care of it.
fn ack_unless_lost<T>(turn: &lc::Turn, out: &Result<T>, duty: &mut WakeDuty) {
    if let Err(e) = out {
        if !marks_rescue(turn, e) {
            duty.ack();
        }
    }
}

pub(super) async fn drive_untraced<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    pending: Option<(Wake, Source)>,
    guard: &mut Option<InvGuard<'c>>,
    turn: lc::Turn,
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    debug_assert!(guard.is_some(), "drive requires this invocation's own lock");
    // **A task waiting for a slot must be small.** Each invocation the drainer emits is a
    // task alive for the whole wait for a slot, and its future is allocated in full at
    // spawn time with the size of its *largest* state. So every step before and after the
    // wait — the gate (reading state, the core, writing the wake-up), the reschedule on
    // hitting the cap, and the turn after the slot too — lives in its own box, allocated
    // only when that step is reached. Directly in this future there is only the wait for a
    // slot. No event order changes.
    let (slot, pending) =
        match Box::pin(gate_before_permit(engine, ctx, id, pending, turn, duty)).await? {
            Gate::Done(outcome) => return Ok(outcome),
            Gate::Queued(slot, pending) => (slot, pending),
        };
    let Some(permit) = acquire_turn_background(engine, ctx, &slot, id).await else {
        return Box::pin(throttled(engine, ctx, id, turn, &slot, duty)).await;
    };
    Box::pin(
        drive_with_permit(engine, ctx, id, permit, pending, turn, duty)
            .instrument(crate::otel::run_span(&id)),
    )
    .await
}

/// The wake-up and lifecycle-core turn of a turn `Runner` handed over: the reschedule set
/// beforehand (`planned`), or an undelivered timer the background tick met while the
/// invocation was busy (`timer`) — the task waits for the lock, then delivers it with the
/// turn.
pub fn dispatched_wake(
    engine: &Lease,
    req: &crate::drainer::RunRequest,
) -> (Option<(Wake, Source)>, lc::Turn) {
    match (req.planned, req.timer) {
        (Some(p), _) => (None, lc::Turn::planned(p)),
        (None, Some(t)) => {
            crate::metrics::latency::WAKEUP.observe(std::time::Duration::from_millis(
                engine.clock().wall_ms().saturating_sub(t.due_ms),
            ));
            (
                Some((
                    Wake::Completion(Completion {
                        id: t.completion_id,
                        result: CompletionResult::Timer,
                    }),
                    Source::Timer { due_ms: t.due_ms },
                )),
                lc::Turn::PLAIN,
            )
        }
        (None, None) => (None, lc::Turn::PLAIN),
    }
}

/// **A background turn handed over by `Runner`**: win the invocation lock, the gate before
/// the slot (`Event::Drive`), then ask for a slot — **synchronously, within the same engine
/// hold** as the gate. Granted right away ⇒ run the turn in this task like [`drive`];
/// otherwise **park a ticket** in the scheduler's line ([`crate::flow::Scheduler::park`]):
/// the task ends, the invocation lock is released, the ticket carries the "turn handed
/// over" mark and the wake-up duty (`done`). A slot granted to the ticket later runs the
/// turn in [`drive_granted`]. `Ok(None)` = the ticket is parked.
///
/// `admit` is the task's place at `Runner`'s intake gate ([`crate::drainer::ADMIT_CAP`]):
/// released before waiting for the invocation lock of a turn in flight, and as soon as the
/// slot ask is decided.
///
/// Unlike [`drive`] (client turns, `resume`, external signals): a ticket blocked on budget
/// or a suspended tenant **does not give up** — no `queue_wait` slices, no rescheduling
/// (`Event::Throttled`), no engine hold at all until it is granted.
pub async fn drive_dispatched<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    req: crate::drainer::RunRequest,
    done: crate::drainer::Dispatched,
    admit: crate::drainer::Admit,
) -> Result<Option<InvokeOutcome>> {
    let id = req.id;
    let mut admit = Some(admit);
    // A free lock (almost always): win it without releasing the engine. Busy (a turn of the
    // same invocation in flight — a timer delivered while busy): wait without holding a
    // place at the gate.
    let guard = match ctx.locks.try_invocation(id) {
        Some(g) => g,
        None => {
            admit = None;
            ctx.locks.invocation(engine, id).await
        }
    };
    let mut guard = Some(guard);
    let (pending, turn) = dispatched_wake(engine, &req);
    let turn = life::carrying(turn, pending.as_ref());
    let mut done = Some(done);
    // The next reschedule is already durable before the turn (a turn handed over by a
    // firing reschedule): nobody has to remember it on its behalf — like [`drive`].
    if turn.planned.is_some() {
        duty_of(&mut done).ack();
    }
    let (out, trace) = crate::stats::traced(dispatched_untraced(
        engine, ctx, req, pending, turn, &mut done, &mut guard, &mut admit,
    ))
    .await;
    let out = match out {
        Ok(None) => return Ok(None),
        Ok(Some(o)) => Ok(o),
        Err(e) => Err(e),
    };
    if let Some(d) = done.as_mut() {
        ack_unless_lost(&turn, &out, d.duty());
    }
    after_run(engine, ctx, id, &out, &trace).await;
    drop(guard);
    out.map(Some)
}

fn duty_of(done: &mut Option<crate::drainer::Dispatched>) -> &mut WakeDuty {
    done.as_mut().expect("ticket not parked yet").duty()
}

#[allow(clippy::too_many_arguments)]
async fn dispatched_untraced<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    req: crate::drainer::RunRequest,
    pending: Option<(Wake, Source)>,
    turn: lc::Turn,
    done: &mut Option<crate::drainer::Dispatched>,
    guard: &mut Option<InvGuard<'c>>,
    admit: &mut Option<crate::drainer::Admit>,
) -> Result<Option<InvokeOutcome>> {
    let id = req.id;
    debug_assert!(guard.is_some(), "drive requires this invocation's own lock");
    // Its own box: the task waiting for the engine stays small (FL-28).
    let (slot, pending) = match Box::pin(gate_before_permit(
        engine,
        ctx,
        id,
        pending,
        turn,
        duty_of(done),
    ))
    .await?
    {
        Gate::Done(outcome) => return Ok(Some(outcome)),
        Gate::Queued(slot, pending) => (slot, pending),
    };
    let cool = cool_key(engine, ctx, &slot, id).await;
    // The ask is decided right below: release the place at the intake gate.
    drop(admit.take());
    let ticket = crate::flow::Ticket::new(
        req,
        done.take().expect("ticket not parked yet"),
        slot.name.clone(),
        slot.seq,
        cool,
    );
    let (ticket, permit) = match ctx.scheduler.park(ticket) {
        crate::flow::Parked::Queued => {
            // The ticket sits in the line; the invocation lock is released with the task.
            drop(guard.take());
            return Ok(None);
        }
        crate::flow::Parked::Now(ticket, permit) => (ticket, permit),
    };
    crate::stats::note_permit_wait(std::time::Duration::ZERO);
    let (_, d) = ticket.into_parts();
    let duty = done.insert(d).duty();
    Box::pin(
        drive_with_permit(engine, ctx, id, permit, pending, turn, duty)
            .instrument(crate::otel::run_span(&id)),
    )
    .await
    .map(Some)
}

/// The turn of a **parked ticket just granted a slot**: the invocation lock was won
/// synchronously when the task started (`actor::run_granted`), the turn continues from the
/// gate after the slot (`Event::PermitGranted`) — an invocation that finished, was
/// cancelled or became `paused` while the ticket was parked does not run. Like [`drive`]
/// from the point where it has a slot.
#[allow(clippy::too_many_arguments)]
pub async fn drive_granted<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    permit: Permit,
    pending: Option<(Wake, Source)>,
    turn: lc::Turn,
    duty: &mut WakeDuty,
    waited: std::time::Duration,
) -> Result<InvokeOutcome> {
    debug_assert_eq!(duty.id(), id, "wake duty belongs to this invocation");
    let turn = life::carrying(turn, pending.as_ref());
    // **No early ack** even if the turn has a reschedule set beforehand: while the ticket
    // was parked, the background tick may have moved that row far out into a watch row (one
    // day) — the reschedule set beforehand is no longer a near wake-up. Every exit of
    // `drive_with_permit` acks by itself once there is a durable wake-up (end of turn, the
    // core's `TurnFailed` rewriting the row); the rest — a panic, a `?` nobody thought of —
    // falls unacked and marks the safety net (FL-16).
    let (out, trace) = crate::stats::traced(async {
        crate::stats::note_permit_wait(waited);
        Box::pin(
            drive_with_permit(engine, ctx, id, permit, pending, turn, duty)
                .instrument(crate::otel::run_span(&id)),
        )
        .await
    })
    .await;
    if out.as_ref().is_err_and(|e| e.is_fenced()) {
        // Write authority lost: the next term's `recover` takes care of it.
        duty.ack();
    }
    after_run(engine, ctx, id, &out, &trace).await;
    out
}

/// Reading the state at the gate after the slot — test point `turn::gate_turn` (only
/// `return`s the error, the engine is held).
async fn gate_status(engine: &mut Lease, id: &InvocationId) -> Result<Option<InvocationStatus>> {
    cairn_store::failpoint!("turn::gate_turn", err);
    Ok(engine.status(id).await?)
}

/// Outcome of the gate before the slot.
enum Gate {
    /// Do not run the turn (already ended, `paused`, missing).
    Done(InvokeOutcome),
    /// Has a place in the queue: go ask for a slot, carrying the undelivered wake-up.
    Queued(VQueueSlot, Option<(Wake, Source)>),
}

/// The gate before the slot (lifecycle core `Event::Drive`) and the place in the queue —
/// the first part of [`drive_untraced`], split out to live in its own box.
async fn gate_before_permit(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    pending: Option<(Wake, Source)>,
    turn: lc::Turn,
    duty: &mut WakeDuty,
) -> Result<Gate> {
    let mut pending = pending;
    // ── Gate before the slot (lifecycle core `Event::Drive`).
    // The engine is held — the point only `return`s the error, it does not stop (FL-16).
    cairn_store::failpoint!("turn::gate_turn", err);
    let status = engine.status(&id).await?;
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, lc::Attendance::IDLE).with_status(status);
    // After the state, the core only reads the queue entry — the part of `ensure_queued`:
    // if it fails, reschedule like every error before `Running`.
    let plan = match asker.ask(engine, ctx, id, lc::Event::Drive { turn }).await {
        Ok(p) => p,
        Err(e) => {
            if failed_before_running(engine, ctx, id, turn, &e).await {
                duty.ack();
            }
            return Err(e);
        }
    };
    if let lc::Then::Reply(_) = plan.then {
        // Already ended, `paused` or missing: nothing to wake.
        write_gate_wake(engine, id, plan.wake, &mut pending).await?;
        duty.ack();
        return gate_outcome(&asker, &plan).map(Gate::Done);
    }

    // ── Ask for a slot. This is the only place a turn starts, so it is the only place the
    //    concurrency cap can be enforced. Every background path (timer, retry, outbox
    //    drain, cross-partition message, recovery) goes through here — there is no back
    //    door that runs a turn without spending a slot.
    // An error here must also leave a reschedule — `drive` is the sole owner of
    // rescheduling after an error, callers do not schedule anything extra.
    let known = asker.slot.take().flatten();
    match queued_slot(engine, ctx, id, known).await {
        Ok(slot) => Ok(Gate::Queued(slot, pending)),
        Err(e) => {
            if failed_before_running(engine, ctx, id, turn, &e).await {
                duty.ack();
            }
            Err(e)
        }
    }
}

/// The wait cap was hit **because of budget** (counter or rate). The invocation is still in
/// the queue and still durable — the only thing dropped is this turn. Reschedule so nobody
/// has to remember it, then clear the way for others — out of tokens, reschedule exactly
/// when the bucket goes positive again.
async fn throttled(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    turn: lc::Turn,
    slot: &VQueueSlot,
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    let retry_after_ms = park_for_retry(engine, ctx, id, turn, throttle_ms(ctx, slot)).await?;
    duty.ack();
    crate::metrics::flow::bump(&crate::metrics::flow::THROTTLED);
    Ok(InvokeOutcome::Throttled { retry_after_ms })
}

/// [`drive`] for the **client** path: report early, then wait as long as it takes.
///
/// There is no slot wait cap here, and that is deliberate: this task only holds its own
/// invocation's lock and has a client waiting on it, so waiting is free for the rest of the
/// partition. The cap in [`drive`] is for `resume` and external signals, where a reschedule
/// is cheaper than holding the lock; turns run by the scheduler park tickets
/// ([`drive_dispatched`]).
pub(super) async fn drive_client<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    guard: &mut Option<InvGuard<'c>>,
    on_queued: impl FnOnce(InvokeOutcome, &Committed),
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    debug_assert_eq!(duty.id(), id, "wake duty belongs to this invocation");
    let (out, trace) = crate::stats::traced(drive_client_untraced(
        engine, ctx, id, guard, on_queued, duty,
    ))
    .await;
    ack_unless_lost(&lc::Turn::PLAIN, &out, duty);
    after_run(engine, ctx, id, &out, &trace).await;
    out
}

pub(super) async fn drive_client_untraced<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    guard: &mut Option<InvGuard<'c>>,
    on_queued: impl FnOnce(InvokeOutcome, &Committed),
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    debug_assert!(guard.is_some(), "drive requires this invocation's own lock");
    // Errors before `drive_with_permit` do not go through `schedule_after_error`: the
    // wake-up duty falls unacked and marks the safety net, lest the just-created invocation
    // sit in `running` for the whole term.
    let slot = ensure_queued(engine, ctx, id).await?;
    let permit = match acquire_turn(engine, ctx, &slot, id, Some(QUEUE_GRACE)).await {
        Some(p) => p,
        None => {
            // **Why it has to wait** decides what we tell the client. Waiting for a VO lock
            // is the normal serialisation of an object — `202`. Waiting for a counter means
            // the tenant is out of budget — `429`, and that is the only signal saying the
            // bill is hitting its cap. Merging the two swallows it.
            // Answering early is also **answering**: the client takes the id and looks it up
            // right after this line. This path sends straight into the `oneshot`, not through
            // `Shared::finish`, so it is the only place the durability step has to be called
            // by hand — and `on_queued` demands the ticket so it cannot be forgotten.
            let durable: Committed = async {
                // The engine is held — the point only `return`s the error, it does not stop
                // (FL-16).
                cairn_store::failpoint!("turn::client_sync", err);
                Ok::<_, ActorError>(engine.sync().await?)
            }
            .await?;
            let budget = ctx
                .scheduler
                .blocked_on(&slot.name)
                .is_some_and(|r| r.is_budget());
            on_queued(
                match lc::drain::decide_early_reply(budget) {
                    lc::drain::EarlyReply::Throttled => {
                        crate::metrics::flow::bump(&crate::metrics::flow::THROTTLED);
                        InvokeOutcome::Throttled {
                            retry_after_ms: throttle_ms(ctx, &slot),
                        }
                    }
                    lc::drain::EarlyReply::Waiting => InvokeOutcome::Waiting,
                },
                &durable,
            );
            match acquire_turn(engine, ctx, &slot, id, None).await {
                Some(p) => p,
                // `None` cannot be reached with a cap of `None`; but if it were, the
                // invocation would still be durable and still in the queue — nobody runs it,
                // so the wake-up duty falls unacked.
                None => return Ok(InvokeOutcome::Waiting),
            }
        }
    };
    // Its own box: see `drive_untraced`.
    Box::pin(
        drive_with_permit(engine, ctx, id, permit, None, lc::Turn::PLAIN, duty)
            .instrument(crate::otel::run_span(&id)),
    )
    .await
}

/// After a run: record the outcome in the per-handler summary, consider the slow log, and
/// capture if a capture request matches.
///
/// Only when the run **reached the SDK** (the trace has a handler key): a run that stopped
/// at the gate (already done, paused) or hit the slot wait cap is not a turn of any
/// handler.
pub(super) async fn after_run(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    out: &Result<InvokeOutcome>,
    trace: &crate::stats::TurnTrace,
) {
    use crate::stats::RunOutcome;
    // Suspended waiting for something outside: no step is running — its temporary stream is
    // released after a grace period if it stays idle (`RangeStreams::waiting`). Suspended
    // waiting for an ack does not end up `Waiting` (the turn carries on in place).
    if matches!(out, Ok(InvokeOutcome::Waiting)) {
        ctx.streams.waiting(&id);
    }
    let Some(key) = trace.key.as_ref() else {
        return;
    };
    let outcome = match out {
        Ok(InvokeOutcome::Completed(_)) => Some(RunOutcome::Completed),
        Ok(InvokeOutcome::Failed { .. }) => Some(RunOutcome::Failed),
        Ok(InvokeOutcome::Paused { .. }) => Some(RunOutcome::Paused),
        Ok(InvokeOutcome::Retry { .. }) => Some(RunOutcome::FailedAttempt),
        Ok(InvokeOutcome::Waiting) => Some(RunOutcome::Suspended),
        Ok(_) => None,
        // Losing write authority mid-turn is not the handler's fault.
        Err(e) if e.is_fenced() => None,
        Err(_) => Some(RunOutcome::FailedAttempt),
    };
    if let Some(o) = outcome {
        ctx.stats.outcome(key, &id, o);
    }
    ctx.stats
        .node()
        .slow_log()
        .consider(trace, key, &id, outcome);
    if let Some(o @ (RunOutcome::Completed | RunOutcome::Failed | RunOutcome::Paused)) = outcome {
        capture_run(engine, ctx, key, id, o).await;
    }
}

/// Capture the run of `id` that just stopped if a capture request matches. See
/// [`crate::capture`] for the win-then-write order.
///
/// Errors are only logged: capturing is diagnostics, it must not break the customer's turn.
pub(super) async fn capture_run(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    key: &crate::stats::HandlerKey,
    id: InvocationId,
    outcome: crate::stats::RunOutcome,
) {
    let caps = ctx.stats.node().captures();
    let Some(spec) = caps.matching(key) else {
        return;
    };
    let Some(store) = caps.store().cloned() else {
        return;
    };
    let Some(deployment) = ctx.deployments.get(&key.deployment).cloned() else {
        return;
    };
    let record = match engine.archive_snapshot(&id).await {
        Ok(Some(r)) => r,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(invocation = %hex(&id), error = %e, "capture: cannot read the journal");
            return;
        }
    };
    // The bundle leaves the node ⇒ what it contains must be durable first (rule 2 of `lease`).
    if let Err(e) = engine.sync().await {
        tracing::warn!(invocation = %hex(&id), error = %e, "capture: journal not durable yet");
        return;
    }
    let bundle = crate::capture::Bundle {
        meta: crate::capture::BundleMeta {
            capture: spec.id.clone(),
            captured_at_ms: engine.clock().wall_ms(),
            status: outcome.as_str().into(),
            contract: deployment.contract(&key.service, &key.handler),
            deployment,
        },
        record,
    };
    let tenant = key.tenant.clone();
    // The capture always carries the artifact bytes.
    let mut artifacts = Vec::new();
    if let Some(arts) = engine.artifact_store().cloned() {
        match engine.writer().artifacts_of_unbounded(&id).await {
            Ok(rows) => {
                for (sha, row) in rows {
                    let Some(row) = row else { continue };
                    let owner = cairn_store::artifact::Owner {
                        tenant: tenant.to_string(),
                        id,
                        epoch: row.epoch,
                    };
                    artifacts.push((sha, arts.path(&owner, &sha)));
                }
            }
            Err(e) => {
                tracing::warn!(invocation = %hex(&id), error = %e, "capture: cannot read artifact rows");
                return;
            }
        }
    }
    let written = engine
        .released(async |_: &Released| {
            store
                .write(&tenant, &spec.id, &id, &bundle, &artifacts)
                .await
        })
        .await;
    match written {
        Ok(true) => {
            caps.used(&tenant, &spec.id);
            tracing::info!(invocation = %hex(&id), capture = %spec.id, "run captured");
        }
        Ok(false) => caps.exhausted(&tenant, &spec.id),
        Err(e) => {
            tracing::warn!(invocation = %hex(&id), capture = %spec.id, error = %e, "capture failed")
        }
    }
}

/// A turn's slot, with its release path — so a turn **cancelled mid-way** does not leave the
/// slot stuck.
///
/// Mid-way cancellation is real and frequent: losing leadership calls `JoinSet::shutdown`,
/// and every task running a turn is cut at its nearest await point. Without this guard the
/// `Permit` on their stack would be dropped with nobody releasing it.
///
/// **Drop releases with `release_running`, not `release_all`** — and that difference is the
/// whole reason this guard lives here rather than in `Permit`: [`crate::flow::Permit`]
/// does not know where the turn stopped so it must not guess, whereas this place does
/// know. A turn cut mid-way may already have committed journal entries, so **keeping the
/// VO lock** is the safe side — just like a turn failed by an infrastructure error. The
/// lock is rebuilt correctly on the next becoming-leader (recovery rebuilds held VO locks),
/// whereas a stuck slot does not heal by itself.
pub(super) struct TurnPermit<'a> {
    scheduler: &'a Scheduler,
    permit: Option<Permit>,
}

impl<'a> TurnPermit<'a> {
    fn new(scheduler: &'a Scheduler, mut permit: Permit) -> Self {
        // The turn starts: from here on releasing is this guard's job, by where the turn stops.
        permit.begin();
        Self {
            scheduler,
            permit: Some(permit),
        }
    }

    /// Settle the run's CU into the tenant's bucket + `vt` — before releasing.
    pub(super) fn settle_cu(&self, cu: u64) {
        if let Some(p) = &self.permit {
            self.scheduler.settle(p, cu);
        }
    }

    /// Release counters + turn cap, **keep the VO lock**.
    pub(super) fn release_running(mut self) {
        if let Some(p) = self.permit.take() {
            self.scheduler.release_running(p);
        }
    }

    /// Release everything.
    pub(super) fn release_all(mut self) {
        if let Some(p) = self.permit.take() {
            self.scheduler.release_all(p);
        }
    }
}

impl Drop for TurnPermit<'_> {
    fn drop(&mut self) {
        if let Some(p) = self.permit.take() {
            self.scheduler.release_running(p);
        }
    }
}

/// Run one turn with a slot **already in hand**, and release it on every exit path.
#[allow(clippy::too_many_arguments)]
/// Records the duration of a run into the histogram when leaving scope.
pub(super) struct Timed(tokio::time::Instant);

impl Drop for Timed {
    fn drop(&mut self) {
        crate::metrics::latency::TURN.observe(self.0.elapsed());
        cairn_store::probe::mark("drive.end", self.0.elapsed().as_micros() as u64);
    }
}

pub(super) async fn drive_with_permit(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    permit: Permit,
    pending: Option<(Wake, Source)>,
    turn: lc::Turn,
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    // Time the whole run on every exit path — the guard records on drop.
    let _timed = Timed(tokio::time::Instant::now());
    cairn_store::probe::mark("drive.begin", u64::from(engine.partition().0));
    // From here on the slot is released on **every** exit path, including cancellation.
    let permit = TurnPermit::new(ctx.scheduler, permit);

    // Waiting for a slot releases the engine — check the durable state again once it is our
    // turn (lifecycle core `Event::PermitGranted`: the gate, then commit `Running`).
    let mut pending = pending;
    // The engine is held — the point only `return`s the error, it does not stop (FL-16). A
    // turn with a reschedule set beforehand tells the core (`TurnFailed { BeforeRunning }`):
    // its row may have been moved far out because of a parked ticket, and the core rewrites
    // the near reschedule. Other turns fall unacked ⇒ safety net, like every `?` before
    // `Running`.
    let status = match gate_status(engine, &id).await {
        Ok(s) => s,
        Err(e) => {
            if turn.planned.is_some() && failed_before_running(engine, ctx, id, turn, &e).await {
                duty.ack();
            }
            return Err(e);
        }
    };

    // Move to `Running` **before** calling the SDK: if the leader dies mid-turn, the sweep
    // on becoming leader finds the entry in `Running` and pushes it back to `Inbox`. With
    // the entry still in `Inbox` the new leader could not tell "never ran" from "half-run" —
    // and the two differ in that the latter may already have committed journal entries.
    //
    // **The same commit carries the turn's durable wake-up** (lifecycle core
    // `Event::PermitGranted`): a turn handed over by a firing reschedule rewrites the
    // reschedule set beforehand with `attempt + 1` — the attempt is counted here (FL-19);
    // other turns write a watch reschedule row. A turn that dies anywhere after this with
    // nobody rescheduling in time has that row fire after `sdk_timeout` + [`WATCH_MARGIN`]:
    // no RAM needed, no live process needed.
    //
    // The wake-up the turn carries (`plan.wake`) is **not** written in this commit:
    // `drive_one` writes it in the next commit, before calling the SDK — dying between the
    // two commits leaves its source (timer, outbox) intact and the turn's row does not count
    // it as progress (a deliberate deviation from writing everything in one commit).
    cairn_store::probe::mark("turn.begin", 0);
    let began = match begin_turn(engine, ctx, id, turn, status, &mut pending).await {
        Ok(Began::Done {
            outcome,
            release,
            ack,
        }) => {
            // Already ended or `paused`: nothing to wake; the slot is released even if the
            // stage commit fails (a stuck slot is lost forever).
            if ack {
                duty.ack();
            }
            permit.settle_cu(crate::stats::take_run_cu());
            release_permit(permit, release);
            return outcome;
        }
        Ok(Began::Run { row, retry_count }) => Ok((row, retry_count)),
        Err(e) => Err(e),
    };
    let begun = began.as_ref().ok().and_then(|b| b.0);
    // Test tripwire: a `?` that "forgets" to reschedule right after the `Running` commit —
    // only `return`s the error. The watch reschedule row is its durable wake-up; the unacked
    // wake-up duty is the fast path (safety net).
    cairn_store::failpoint!("turn::after_running", err);

    let (outcome, retry_count) = match began {
        Ok((_, rc)) => {
            cairn_store::probe::mark("turn.running", 0);
            let r = drive_one(engine, ctx, id, pending, turn, rc).await;
            cairn_store::probe::mark("turn.sdk_done", 0);
            (r, rc)
        }
        // The `Running` commit failed (ambiguous — it may have been written, AGENTS.md §1
        // rule 7): treat the turn as having entered `Running`, as before.
        Err(e) => (Err(e), turn.planned.map_or(0, |p| p.attempt)),
    };

    // Release **on every exit path**, including the error path. A turn failed by a fence
    // must also give its slot back; keeping it would leave the counter stuck until the next
    // leader change.
    //
    // **Ending a turn is one commit**: the stage (or removing the row), removing the turn's
    // durable wake-up, and the retry reschedule if any — if it fails nothing changes, the
    // turn's row is still there.
    match outcome {
        Ok(ran) => {
            // The turn got through: its streak of consecutive infrastructure failures is broken.
            ctx.rescue.clear(&id);
            let o = match ran {
                Ran::Outcome(o) => {
                    end_turn(engine, ctx, id, permit, turn, retry_count, &o, begun).await?;
                    o
                }
                Ran::Settled {
                    outcome,
                    plan,
                    asker,
                } => {
                    end_turn_with(engine, id, permit, &asker, &plan, &outcome, begun).await?;
                    outcome
                }
            };
            // The outcome is durable together with the reschedule (if any): done, suspended
            // waiting for a durable event, or with a durable retry.
            duty.ack();
            Ok(o)
        }
        Err(e) => {
            // We don't know where the turn stopped ⇒ pick the safe side: keep the VO lock,
            // release the slot. Releasing the lock of an invocation that may have written a
            // half-done journal lets the next one read state mid-way.
            //
            // ...and it **must leave a reschedule**, or nobody would ever come to release
            // that VO lock just kept. If no reschedule can be set (read failure) **keep** the
            // watch reschedule row: it is the reschedule.
            if failed_after_running(engine, ctx, id, permit, turn, retry_count, &e, begun).await {
                duty.ack();
            }
            Err(e)
        }
    }
}
