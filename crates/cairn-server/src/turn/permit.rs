// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Asking for a flow-control slot for a turn: queues, cool/park/throttle, `settle_permit`.

use super::*;

/// Margin of the watch row past the SDK wait cap: a turn that is still alive
/// has finished or hit its cap before the row fires; when it fires while the
/// turn still holds the lock, the background tick only moves it later.
pub const WATCH_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);

/// A turn's watch row fires after this long (ms).
pub(super) fn watch_after_ms(ctx: &Ctx<'_>) -> u64 {
    (ctx.sdk_timeout.unwrap_or(DEFAULT_SDK_TIMEOUT) + WATCH_MARGIN).as_millis() as u64
}

/// The place of an invocation in its queue, rebuilt if missing.
///
/// Missing is something that *should not* happen — [`Engine::create`] writes
/// the vqueue entry in the same commit as journal entry 0. But "should not" is
/// not "cannot": an invocation created by a binary from before flow control
/// has no entry. Running it without a queue means running it **without the VO
/// lock**, i.e. exactly what `EXCLUSIVE` promises will not happen. So this path
/// rebuilds the entry instead of skipping it.
pub(super) async fn ensure_queued(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
) -> Result<VQueueSlot> {
    // Holding the engine — the point only `return`s an error, never pauses.
    cairn_store::failpoint!("turn::ensure_queued", err);
    let known = engine.vqueue_of(&id).await?;
    rebuild_slot(engine, ctx, id, known).await
}

/// [`ensure_queued`] when the queue entry has already been read (`known`, the
/// core's gate — `Event::Drive` asks for `Need::Slot`): rebuilt if missing
/// (`StageChange::Insert`).
pub(super) async fn queued_slot(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    known: Option<VQueueSlot>,
) -> Result<VQueueSlot> {
    // Holding the engine — the point only `return`s an error, never pauses.
    cairn_store::failpoint!("turn::ensure_queued", err);
    rebuild_slot(engine, ctx, id, known).await
}

async fn rebuild_slot(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    known: Option<VQueueSlot>,
) -> Result<VQueueSlot> {
    if let Some(slot) = known {
        return Ok(slot);
    }
    let inv = engine.load(id, params_for(&id)).await?;
    // The limit key is not in the journal (it is not identity), so the rebuilt
    // entry is only subject to the scope's cap. That is a real loss and is
    // written down here rather than hidden: it only touches invocations of old
    // binaries.
    let queue = queue_name_on(
        pinned_deployment(ctx.deployments, &inv).ok().flatten(),
        &inv.target,
        None,
    );
    let seq = engine.next_queue_seq(&queue).await?;
    tracing::warn!(
        invocation = %hex(&id),
        queue = %queue,
        "invocation has no queue entry — rebuilt (data from before flow control?)"
    );
    life::enqueue(engine, id, &queue, seq).await?;
    Ok(VQueueSlot {
        name: queue.clone(),
        queue: queue.encode(),
        stage: Stage::Inbox,
        seq,
    })
}

/// Asks for a slot, with a cap. `None` = the wait cap passed.
///
/// Same mould as [`awaiting_sdk`], and for the same reason: "release the engine
/// while waiting" is the rule of [`crate::lease`], and a function cannot forget.
/// The list of its call sites is exactly the list of places where a turn starts.
pub(super) async fn acquire_turn(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    slot: &VQueueSlot,
    id: InvocationId,
    budget: Option<std::time::Duration>,
) -> Option<Permit> {
    // `slot.seq`, not arrival order: an invocation that suspended and woke up
    // still holds the VO lock, and queueing it **behind** a newcomer of the same
    // queue deadlocks that queue — the newcomer is waiting for exactly the lock
    // it holds.
    let cool = cool_key(engine, ctx, slot, id).await;
    let fut = ctx.scheduler.acquire_cooled(&slot.name, id, slot.seq, cool);
    let started = tokio::time::Instant::now();
    let got = match budget {
        None => Some(engine.wait(fut).await),
        Some(d) => engine.wait(tokio::time::timeout(d, fut)).await.ok(),
    };
    crate::stats::note_permit_wait(started.elapsed());
    got
}

/// Asks for a slot for a turn that runs through [`drive`] **inside its own
/// task** — now only turns with a caller: `resume`, external signals.
/// Background turns handed out by `Runner` (scheduler, recovery, retry, timer)
/// do not wait here: when not granted at once they park a ticket
/// ([`drive_dispatched`], FL-30) — no slices, no reschedule.
///
/// The `queue_wait` cap only makes sense when blocked **by a counter**: the
/// tenant is out of budget, holding the invocation longer than needed is a
/// waste, dropping the turn and rescheduling it is right. When blocked by **the
/// VO lock** (normal serialisation of an object) or **the turn cap** (the
/// partition is busy), giving up makes nobody run sooner — it only trades a
/// place in the queue (ordered by `seq`, the scheduler calls it when its turn
/// comes) for one reschedule commit + one re-fire commit every `queue_wait`,
/// for **every** invocation in the queue. A hot object with 6 000 commands
/// waiting is 100–180 commits/s competing for the engine thread with the very
/// command that is running. So: when a wait slice ends and the block is still
/// not because of a counter, keep waiting — like the client path — and write
/// nothing.
///
/// The price: a waiting invocation holds its own lock, so `cancel` on it has to
/// wait for its turn — exactly like the client path always has.
pub(super) async fn acquire_turn_background(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    slot: &VQueueSlot,
    id: InvocationId,
) -> Option<Permit> {
    let Some(slice) = ctx.queue_wait else {
        return acquire_turn(engine, ctx, slot, id, None).await;
    };
    // Suspended tenant: give up at once, without waiting out a slice — waiting
    // would not get a slot either, and the caller reschedules after one
    // reschedule tick (`throttle_ms`).
    if lc::drain::decide_give_up_slice(ctx.scheduler.refuses(&slot.name.tenant)) {
        crate::metrics::flow::bump(&crate::metrics::flow::BLOCKED_TENANT);
        return None;
    }
    // **One** slot request for the whole wait: keep the place in the queue
    // across every slice, never drop it and ask again (each such retry leaves a
    // dead entry behind).
    let cool = cool_key(engine, ctx, slot, id).await;
    let mut fut = Box::pin(ctx.scheduler.acquire_cooled(&slot.name, id, slot.seq, cool));
    let started = tokio::time::Instant::now();
    // **Do not take the engine between two slices**: the "give up because of
    // budget" check reads the scheduler's RAM state and needs no `Lease`. The
    // task only takes the engine back when granted a slot or when giving up — W
    // waiters (VO lock, turn cap) are no longer W engine acquisitions per slice.
    let (scheduler, queue) = (ctx.scheduler, &slot.name);
    let got = engine
        .wait(async {
            loop {
                if let Ok(p) = tokio::time::timeout(slice, &mut fut).await {
                    return Some(p);
                }
                // VO lock, turn cap, or just called but not yet received: one
                // more slice. No commit, no reschedule row.
                if lc::drain::decide_give_up_slice(
                    scheduler.blocked_on(queue).is_some_and(|r| r.is_budget()),
                ) {
                    return None;
                }
            }
        })
        .await;
    // Giving up was decided while the engine was released: the slot may have
    // arrived while taking the engine back — take it instead of returning to the
    // queue and rescheduling.
    let got = match got {
        Some(p) => Some(p),
        None => futures::FutureExt::now_or_never(&mut fut),
    };
    crate::stats::note_permit_wait(started.elapsed());
    got
}

/// The `cooldown` identity of the turn about to ask for a slot (FL-14), or
/// `None` — nearly always, and then it costs nothing beyond a version
/// comparison: only when the queue's `(tenant, service)` has a `cooldown` entry
/// is entry 0 read (one GET under the engine) to learn the handler and
/// deployment.
///
/// A read error ⇒ `None`: quarantine is damage control, not a correctness
/// condition — running one turn without a lowered cap beats failing a
/// customer's turn.
pub(super) async fn cool_key(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    slot: &VQueueSlot,
    id: InvocationId,
) -> Option<crate::flow::CoolKey> {
    let name = &slot.name;
    if !ctx.quarantine.cools(name.tenant.as_str(), &name.service) {
        return None;
    }
    let (target, pinned) = engine.head(&id).await.ok().flatten()?;
    // The deployment the turn will run on — the same way the per-handler summary sets its key.
    let deployment = match pinned {
        Some(d) => d,
        None => deployment_for(ctx.deployments, &target)
            .map(|d| d.id.clone())
            .unwrap_or_default(),
    };
    ctx.quarantine
        .cooled(
            tenant_of(&target),
            &target.service,
            &target.handler,
            &deployment,
        )
        .then(|| crate::stats::HandlerKey {
            tenant: target.tenant.to_string(),
            service: target.service,
            handler: target.handler,
            deployment,
        })
}

/// Reschedules an invocation just refused a slot, and says how long until then
/// — the core, `Event::Throttled`.
///
/// The turn has not entered `Running`, so it does not spend an attempt
/// (FL-19); a turn with a reschedule set beforehand does not add another — two
/// rows for one invocation mean two turns running together later, and the
/// backoff count would go back to 1, so it would never grow. The slot request
/// was withdrawn when the waiting future was dropped (`Claim::drop`).
pub(super) async fn park_for_retry(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    turn: lc::Turn,
    after_ms: u64,
) -> Result<u64> {
    let now = engine.clock().wall_ms();
    let attendance = lc::Attendance {
        permit: lc::Permit::Waiting,
        ..lc::Attendance::IDLE
    };
    let mut asker = life::Asker::new(now, ctx, attendance);
    let plan = asker
        .ask(engine, ctx, id, lc::Event::Throttled { turn, after_ms })
        .await?;
    life::commit(engine, id, &asker, &plan, None).await?;
    life::side(ctx.streams, &id, plan.side);
    Ok(after_ms)
}

/// The gate after the slot, then the `Running` commit of turn `turn` (core,
/// `Event::PermitGranted`): stage `Running` and the turn's durable wake-up (the
/// reschedule set beforehand rewritten with `attempt + 1`, or the watch row) in
/// one commit.
pub(super) enum Began {
    Run {
        /// The row this commit writes — the commit ending the turn removes or replaces it.
        row: Option<lc::RetryRow>,
        /// The turn's `ctx.attempt`.
        retry_count: u32,
    },
    /// The gate does not let it run (already finished, `paused`, absent): the
    /// carrying turn's wake-up has been written (or dropped), the stage
    /// committed (queue row withdrawn / `Paused`). The caller releases the slot
    /// per `release` — even when `outcome` is an error — and acks the wake-up
    /// duty if `ack`.
    Done {
        outcome: Result<InvokeOutcome>,
        release: lc::Release,
        ack: bool,
    },
}

pub(super) async fn begin_turn(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    turn: lc::Turn,
    status: Option<InvocationStatus>,
    pending: &mut Option<(Wake, Source)>,
) -> Result<Began> {
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, started()).with_status(status);
    if turn.planned.is_some() {
        // The row's **real** due time: while the turn waited for a slot, the
        // background tick may have moved the row — a busy holder
        // (`busy_defer_ms`) or a parked ticket (a one-day watch row). The
        // `Running` commit that rewrites the row must delete the index row at
        // that due time, not at the firing's `next_due`: otherwise a stale index
        // row stays until it comes due — a whole cluster of tickets at once,
        // one day later. On a read error, as before (`next_due`): a stale row
        // only costs space, whereas an error here would be treated as an
        // ambiguous `Running` commit.
        if let Ok(row) = engine.retry_row(&id).await {
            asker = asker.with_row(row);
        }
    }
    let plan = asker
        .ask(engine, ctx, id, lc::Event::PermitGranted { turn })
        .await?;
    let lc::Then::Preflight { retry_count } = plan.then else {
        if let Err(e) = write_gate_wake(engine, id, plan.wake, pending).await {
            // Like a read error before the turn: the slot is released on `Drop`
            // (keeping the VO lock), the wake-up duty is dropped unacked.
            return Ok(Began::Done {
                outcome: Err(e),
                release: lc::Release::Running,
                ack: false,
            });
        }
        let committed = life::commit(engine, id, &asker, &plan, None).await;
        life::side(ctx.streams, &id, plan.side);
        return Ok(Began::Done {
            outcome: committed.and_then(|()| gate_outcome(&asker, &plan)),
            release: plan.ram.release,
            ack: plan.ram.duty == lc::Duty::Ack,
        });
    };
    life::commit(engine, id, &asker, &plan, turn.planned.map(|p| p.next_due)).await?;
    life::side(ctx.streams, &id, plan.side);
    Ok(Began::Run {
        row: match plan.retry {
            lc::RowChange::Set(r) => Some(r),
            _ => None,
        },
        retry_count,
    })
}

/// Ends a turn that got to run in **one** commit (core, `Event::TurnEnded`):
/// stage (or withdraw the queue row), remove the turn's durable wake-up
/// (`begun`), the retry reschedule if any — then release the slot per the
/// outcome, **even when the commit fails** (a wrong stage only makes recovery
/// guess again, a stuck slot is lost forever).
#[allow(clippy::too_many_arguments)]
pub(super) async fn end_turn(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    permit: TurnPermit<'_>,
    turn: lc::Turn,
    retry_count: u32,
    outcome: &InvokeOutcome,
    begun: Option<lc::RetryRow>,
) -> Result<()> {
    if matches!(outcome, InvokeOutcome::Retry { .. }) && turn.planned.is_none() {
        // Holding the engine — the point only `return`s an error, never pauses (FL-16).
        cairn_store::failpoint!("turn::retry_if", err);
    }
    // 1 CU was charged up front when the slot was granted; now charge the rest
    // of the run's actual cost (CU of the SDK turns in the trace — 0 if the run
    // never reached the SDK ⇒ 1 refunded).
    permit.settle_cu(crate::stats::take_run_cu());
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, started());
    let event = lc::Event::TurnEnded {
        turn,
        retry_count,
        outcome: life::outcome_of(outcome),
    };
    let plan = match asker.ask(engine, ctx, id, event).await {
        Ok(p) => p,
        Err(e) => {
            release_permit(
                permit,
                if is_terminal(outcome) {
                    lc::Release::All
                } else {
                    lc::Release::Running
                },
            );
            return Err(e);
        }
    };
    // The error of the attempt that just failed: in the same commit as the next reschedule.
    let failure = match outcome {
        InvokeOutcome::Retry {
            failure: Some((code, message)),
            ..
        } => Some(cairn_store::keys::LastFailure::new(
            *code,
            message,
            retry_count,
            now,
        )),
        _ => None,
    };
    let ended = commit_end(engine, id, permit, &asker, &plan, begun, failure.as_ref()).await;
    if ended.is_ok() && plan.side.is_none() {
        // The SDK's outcome became durable in the turn — the engine's
        // `Effect::Finish`, core `Event::Finished` — so its `Side` is carried
        // out here: streams are sealed when the invocation finishes or is
        // `paused`.
        life::side(&asker.streams, &id, life::finished_side(outcome));
    }
    ended
}

/// Ends a turn with the Plan that preflight decided (`Ran::Settled`): the same
/// release order as [`end_turn`]. When the Plan changes status (out of
/// attempts), the status carries `outcome`'s code and message in the same
/// commit as the stage and the retry row.
pub(super) async fn end_turn_with(
    engine: &mut Lease,
    id: InvocationId,
    permit: TurnPermit<'_>,
    asker: &life::Asker,
    plan: &lc::Plan,
    outcome: &InvokeOutcome,
    begun: Option<lc::RetryRow>,
) -> Result<()> {
    permit.settle_cu(crate::stats::take_run_cu());
    let committed = match (plan.status, outcome) {
        (Some(lc::Status::Paused(_)), InvokeOutcome::Paused { code, message }) => {
            life::pause(engine, id, *code, message, asker, plan)
                .await
                .map(|_| ())
        }
        (Some(lc::Status::Failed), InvokeOutcome::Failed { code, message }) => {
            life::fail(engine, id, *code, message, asker, plan)
                .await
                .map(|_| ())
        }
        (None, _) => {
            let c = life::commit(engine, id, asker, plan, begun.map(|r| r.due)).await;
            if c.is_ok() {
                life::side(&asker.streams, &id, plan.side);
            }
            c
        }
        (status, o) => unreachable!("preflight: {status:?} with {o:?}"),
    };
    release_permit(permit, plan.ram.release);
    committed
}

async fn commit_end(
    engine: &mut Lease,
    id: InvocationId,
    permit: TurnPermit<'_>,
    asker: &life::Asker,
    plan: &lc::Plan,
    begun: Option<lc::RetryRow>,
    failure: Option<&cairn_store::keys::LastFailure>,
) -> Result<()> {
    let committed =
        life::commit_noting(engine, id, asker, plan, begun.map(|r| r.due), failure).await;
    release_permit(permit, plan.ram.release);
    committed?;
    life::side(&asker.streams, &id, plan.side);
    Ok(())
}

/// Tenant `tenant` is being refused slots (`suspended`) ⇒ reschedule after how
/// long — the suspended tenant's reschedule tick, floored at
/// [`THROTTLE_RETRY_AFTER_MS`]; `None` = not refused. The shell asks right
/// before every SDK turn, after every wait of the run: one generation
/// comparison against the tenant-state source, no I/O.
pub(super) fn refused_ms(ctx: &Ctx<'_>, tenant: &cairn_store::TenantId) -> Option<u64> {
    ctx.scheduler.refuses(tenant).then(|| {
        let wait = ctx
            .scheduler
            .throttle_after(&crate::flow::ResourceKind::Tenant(tenant.clone()))
            .unwrap_or_default();
        (wait.as_millis() as u64).max(THROTTLE_RETRY_AFTER_MS)
    })
}

/// How long until a turn just blocked by budget should be retried: the first
/// backoff step ([`THROTTLE_RETRY_AFTER_MS`]) for a counter; the time the
/// bucket turns positive again for a rate — saying a shorter number tells the
/// client to knock in vain.
pub(super) fn throttle_ms(ctx: &Ctx<'_>, slot: &VQueueSlot) -> u64 {
    // Suspended tenant: background turns give up before asking for a slot, so
    // `blocked_on` may not say anything yet — ask directly.
    let blocked = if ctx.scheduler.refuses(&slot.name.tenant) {
        Some(crate::flow::ResourceKind::Tenant(slot.name.tenant.clone()))
    } else {
        ctx.scheduler.blocked_on(&slot.name)
    };
    let wait = blocked
        .and_then(|r| ctx.scheduler.throttle_after(&r))
        .unwrap_or_default();
    (wait.as_millis() as u64).max(THROTTLE_RETRY_AFTER_MS)
}
