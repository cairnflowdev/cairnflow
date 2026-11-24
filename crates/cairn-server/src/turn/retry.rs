// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The retry schedule and the re-run paths: `schedule_*`, `restart`,
//! `continue_as_new`, `migrate`, `fire_retries`.

use super::*;

/// Whether error `e` of turn `turn` drops the wake-up — i.e. whether the turn's
/// wake-up duty ([`WakeDuty`]) is still pending.
///
/// No, when write rights are lost (the term is ending, the next term's
/// `recover` takes over), or when the turn has a reschedule set beforehand (a
/// turn handed out by a firing reschedule: the next reschedule was durable
/// before the turn). [`drive`] acks the duty by this rule; on every other error
/// path that leaves no reschedule, the duty is dropped unacked and the safety
/// net is marked (nack by default, FL-16).
pub(super) fn marks_rescue(turn: &lc::Turn, e: &crate::actor_types::ActorError) -> bool {
    turn.planned.is_none() && !e.is_fenced()
}

/// The turn's error, condensed for the core.
fn turn_err(e: &ActorError) -> lc::TurnErr {
    if e.is_fenced() {
        lc::TurnErr::Fenced
    } else {
        lc::TurnErr::Other
    }
}

/// Test point for the read done for the reschedule after an error: its error
/// becomes a journal-length read error that the core knows how to handle (keep
/// the watch row, duty dropped unacked). Only on turns the core will read for
/// (no reschedule set beforehand, write rights not lost) — like where the point
/// used to sit.
async fn schedule_fault(asker: &mut life::Asker, turn: &lc::Turn, err: &ActorError) {
    if turn.planned.is_some() || err.is_fenced() {
        return;
    }
    let r = async {
        cairn_store::failpoint!("turn::schedule_after_error", err);
        Ok::<_, ActorError>(())
    }
    .await;
    if let Err(e) = r {
        asker.fail(lc::Need::JournalLen, e);
    }
}

/// Reschedules a turn that **threw an error before the `Running` commit**
/// (`ensure_queued` failed) — the core, `Event::TurnFailed { at: BeforeRunning }`.
///
/// Previously nobody rescheduled a turn that threw `Err` (only the `Retry`
/// outcome had a reschedule): the invocation sat there until the next
/// leadership change; a single node never has one. **No reschedule once write
/// rights are lost** — the next term's `recover` is the right path; **every
/// error reschedules, without classification** (a retry with backoff is
/// visible, and `cancel` can stop it).
///
/// Returns no error: its own error must not mask the original one. Returns
/// `true` when the wake-up duty is done (reschedule written, or not needed);
/// otherwise the duty is still pending and the term's safety net is marked
/// (FL-16).
pub(super) async fn failed_before_running(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    turn: lc::Turn,
    err: &ActorError,
) -> bool {
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, lc::Attendance::IDLE).tolerant();
    schedule_fault(&mut asker, &turn, err).await;
    let event = lc::Event::TurnFailed {
        turn,
        at: lc::FailedAt::BeforeRunning,
        err: turn_err(err),
    };
    settle_failed(engine, ctx, id, &mut asker, event, None, None, None).await
}

/// A turn that threw an error **after** the `Running` commit — the core,
/// `Event::TurnFailed { at: AfterRunning }`. The core reads the status back first
/// (rule 7): the error may come from the engine's finishing commit that landed —
/// terminal ⇒ settle like a finished turn (VO lock released). Otherwise where the
/// turn stopped is unknown ⇒ keep the VO lock (FL-4), release the slot, and **a
/// reschedule must** be left behind; if one cannot be set up, keep the watch row
/// (it is the reschedule). If the commit ending the turn fails, the slot is still
/// released.
///
/// The error goes with the reschedule commit as the attempt's `lastFailure`,
/// code [`INFRA_FAILURE`] like the SDK wait cap: a broken node → SDK leg or an
/// SDK answering 5xx is an attempt that failed for infrastructure reasons, and
/// an `/invoke` that hit its wait cap and returned `202` has only this place
/// left to say why. Not when write rights are lost: nothing can be written.
#[allow(clippy::too_many_arguments)]
pub(super) async fn failed_after_running(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    permit: TurnPermit<'_>,
    turn: lc::Turn,
    retry_count: u32,
    err: &ActorError,
    begun: Option<lc::RetryRow>,
) -> bool {
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, started()).tolerant();
    schedule_fault(&mut asker, &turn, err).await;
    let event = lc::Event::TurnFailed {
        turn,
        at: lc::FailedAt::AfterRunning { retry_count },
        err: turn_err(err),
    };
    let failure = (!err.is_fenced()).then(|| {
        cairn_store::keys::LastFailure::new(INFRA_FAILURE, &err.tenant_text(), retry_count, now)
    });
    settle_failed(
        engine,
        ctx,
        id,
        &mut asker,
        event,
        Some(permit),
        begun.map(|r| r.due),
        failure.as_ref(),
    )
    .await
}

/// The attendance of a task holding a slot that has `begin`un.
pub(super) fn started() -> lc::Attendance {
    lc::Attendance {
        permit: lc::Permit::Started,
        ..lc::Attendance::IDLE
    }
}

#[allow(clippy::too_many_arguments)]
async fn settle_failed(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    asker: &mut life::Asker,
    event: lc::Event,
    permit: Option<TurnPermit<'_>>,
    old_due: Option<u64>,
    failure: Option<&cairn_store::keys::LastFailure>,
) -> bool {
    if let Some(p) = &permit {
        p.settle_cu(crate::stats::take_run_cu());
    }
    let plan = match asker.ask(engine, ctx, id, event).await {
        Ok(p) => p,
        Err(e) => {
            drop(permit);
            tracing::error!(error = %e, "could NOT reschedule after a failure — the rescue net will reconcile");
            return false;
        }
    };
    if asker.view.read_error.is_some() {
        tracing::error!(
            error = %asker.failed_text(),
            "cannot read the invocation to reschedule after a failure — the rescue net will reconcile"
        );
    }
    // The status read back says the turn's finishing commit landed (rule 7): the
    // invocation ended — no failure to note on it.
    let ended = asker.view.status.is_some_and(lc::Status::is_terminal);
    let failure = failure.filter(|_| !ended);
    let committed = life::commit_noting(engine, id, asker, &plan, old_due, failure).await;
    if let Some(p) = permit {
        release_permit(p, plan.ram.release);
    }
    match committed {
        Ok(()) => {
            life::side(ctx.streams, &id, plan.side);
            plan.ram.duty == lc::Duty::Ack
        }
        Err(e) => {
            // The last place that can **write** a durable reschedule for this
            // invocation, so it is an `error!`; if it still fails past here,
            // only the watch row (if any) and the in-RAM safety net remain.
            if matches!(plan.retry, lc::RowChange::Set(_)) {
                tracing::error!(error = %e, "could NOT reschedule after a failure — the rescue net will reconcile");
            }
            false
        }
    }
}

/// Releases a `begin`un slot per the Plan's `Release`.
pub(super) fn release_permit(permit: TurnPermit<'_>, r: lc::Release) {
    match r {
        lc::Release::All => permit.release_all(),
        // `Keep`/`Claim` do not occur with a `begin`un slot; `Drop` releases
        // like `release_running` — the safe side.
        lc::Release::Running | lc::Release::Keep | lc::Release::Claim => permit.release_running(),
    }
}

/// The id of `old`'s `restart-as-new` copy: **the same partition key** (the
/// journal is copied within the same Db), the rest random.
pub(super) fn successor_id(old: &InvocationId) -> InvocationId {
    let mut tail = [0u8; 16];
    cairn_store::entropy::fill(&mut tail);
    cairn_store::make_invocation_id(cairn_store::partition_key_of(old), tail)
}

/// `restart-as-new` from the admin API: the new copy keeps the first `keep`
/// entries of `old`, is pinned to `deployment` (default: the one `old` is
/// pinned to), and is then handed out to run. Under `old`'s lock; if `old` has
/// not finished, its VO lock is returned as with `cancel`.
pub async fn restart<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    old: InvocationId,
    keep: u32,
    deployment: Option<String>,
    patch: Option<admit::Patch>,
) -> Result<cairn_engine::RestartOutcome> {
    let mut guard = Some(ctx.locks.invocation(engine, old).await);
    let Some((target, pinned)) = engine.head(&old).await? else {
        return Ok(cairn_engine::RestartOutcome::NotFound);
    };
    let new = successor_id(&old);
    // The restart's audit row goes with the commit that writes the new copy's
    // status (`restart_in`).
    let audit = ctx.audit_begin(engine, new).await;
    // The next copy is new work: ask the shared admission path — today only
    // to refuse an input patch in front of the feature gate.
    match admit::admit(
        engine,
        ctx,
        admit::Source::Restart { patch },
        admit::Request {
            id: new,
            target: &target,
            limit_key: None,
            idempotency_key: None,
            input: Bytes::new(),
            log_level: None,
            tags: &[],
            sender: None,
            root_id: None,
            schedule: None,
        },
    )
    .await?
    {
        admit::Admission::Admitted => {}
        admit::Admission::Rejected(why) => return Err(ActorError::Rejected(why)),
        admit::Admission::Refused(s) => return Err(ActorError::TenantRefused(s)),
        _ => unreachable!("a Restart source returns only Admitted/Rejected/Refused"),
    }
    let to = deployment.or(pinned);
    let d = to.as_deref().and_then(|id| ctx.deployments.get(id));
    let queue = queue_name_on(
        d.or_else(|| deployment_for(ctx.deployments, &target)),
        &target,
        None,
    );
    let seq = engine.next_queue_seq(&queue).await?;
    // Core `Event::AdminRestart`: if the old copy has not finished, it ends as
    // `completed {"continuedAsNew"}` — stage, retry row — in the same commit
    // that creates the new copy; its VO lock is returned; then the new copy is
    // handed out.
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, lc::Attendance::IDLE);
    let plan = asker.ask(engine, ctx, old, lc::Event::AdminRestart).await?;
    if plan.then == lc::Then::Reply(lc::Reply::NotFound) {
        super::audit_end(engine, ctx, audit, &new, "not_found").await?;
        return Ok(cairn_engine::RestartOutcome::NotFound);
    }
    let outcome = life::restart(
        engine,
        old,
        new,
        keep,
        None,
        to.as_deref(),
        Some((&queue, seq)),
        &plan,
        ctx.streams,
    )
    .await?;
    super::audit_end(engine, ctx, audit, &new, "noop").await?;
    if let cairn_engine::RestartOutcome::Restarted { new } = &outcome {
        life::free_lock(ctx, old, &asker, &plan);
        drop(guard.take());
        ctx.runner.queue(*new);
    }
    Ok(outcome)
}

/// `ctx.continueAsNew(input)`: `inv`'s turn ends with a **new** invocation of
/// the same target whose journal only holds the new `Input` — running on the
/// service's **current revision** (the natural upgrade point of a long loop).
/// Callers, waiters and the idempotency key move to the new copy. The running
/// turn holds `inv`'s permit; releasing it (terminal outcome) returns the VO
/// lock to the new copy.
pub(super) async fn continue_as_new(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    inv: &Invocation,
    input: Bytes,
) -> Result<InvokeOutcome> {
    let target = &inv.target;
    let current = ctx
        .deployments
        .current_id(tenant_of(target), &target.service)
        .map(str::to_string);
    let queue = queue_name(ctx.deployments, target, None);
    let seq = engine.next_queue_seq(&queue).await?;
    let new = successor_id(&inv.id);
    // Core `Event::Finished { ContinuedAsNew }`: the old copy ends as
    // `completed {"continuedAsNew"}` — stage, the turn's retry row — in the same
    // commit that creates the new copy. If the old copy was cancelled in the
    // middle of the turn, the core writes nothing and does **not** create a new
    // copy (`spawn_successor`): the turn ends like the cancelled copy (the shell
    // used to create the new copy anyway).
    let now = engine.clock().wall_ms();
    // Test slot (`failpoints`): a durable admin cancel slipping in mid-turn — the
    // durable part of core `AdminCancel` (`failed` 572, entry withdrawn, rows
    // deleted); the VO lock is left for this turn to release per its outcome, as
    // when a real cancel arrives after the turn.
    if cairn_store::failpoint::fires("turn::continue_as_new::cancelled_mid_turn") {
        let mut cancel = life::Asker::new(now, ctx, lc::Attendance::IDLE);
        let p = cancel
            .ask(engine, ctx, inv.id, lc::Event::AdminCancel)
            .await?;
        life::fail(
            engine,
            inv.id,
            cairn_engine::CANCELLED,
            "invocation cancelled via the admin API (failpoint, mid-turn)",
            &cancel,
            &p,
        )
        .await?;
    }
    let mut asker = life::Asker::new(now, ctx, started());
    let plan = asker
        .ask(
            engine,
            ctx,
            inv.id,
            lc::Event::Finished {
                finish: lc::Finish::ContinuedAsNew,
            },
        )
        .await?;
    if !plan.ram.spawn_successor {
        return Ok(match asker.status.clone().flatten() {
            Some(InvocationStatus::Failed { code, message }) => {
                InvokeOutcome::Failed { code, message }
            }
            Some(InvocationStatus::Completed(v)) => InvokeOutcome::Completed(v),
            Some(InvocationStatus::Paused { code, message }) => {
                InvokeOutcome::Paused { code, message }
            }
            // Cleaned up mid-turn: nothing left to end.
            _ => InvokeOutcome::Failed {
                code: 404,
                message: "invocation no longer exists — no new run created".into(),
            },
        });
    }
    match life::restart(
        engine,
        inv.id,
        new,
        1,
        Some(input),
        current.as_deref(),
        Some((&queue, seq)),
        &plan,
        ctx.streams,
    )
    .await?
    {
        cairn_engine::RestartOutcome::Restarted { new } => {
            ctx.runner.queue(new);
            Ok(InvokeOutcome::Completed(
                Bytes::from(format!("{{\"continuedAsNew\":\"{}\"}}", hex(&new))).into(),
            ))
        }
        cairn_engine::RestartOutcome::WorkflowRun => Ok(InvokeOutcome::Failed {
            code: 400,
            message: "continueAsNew does not apply to a workflow run".into(),
        }),
        other => Ok(InvokeOutcome::Failed {
            code: 500,
            message: format!("continueAsNew failed: {other:?}"),
        }),
    }
}

/// Moves invocation `id` to deployment `to` — under its own lock, so no turn in
/// flight loads the old deployment midway and writes after the move.
pub async fn migrate<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    to: &str,
) -> Result<bool> {
    let _guard = ctx.locks.invocation(engine, id).await;
    let audit = ctx.audit_begin(engine, id).await;
    let moved = engine.migrate(&id, to).await?;
    super::audit_end(engine, ctx, audit, &id, if moved { "ok" } else { "noop" }).await?;
    Ok(moved)
}

/// Re-runs the invocations whose retry is due.
pub(super) async fn fire_retries<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    now_ms: u64,
) -> Result<usize> {
    // The engine **is held** here — pausing at this point pauses the whole range.
    cairn_store::failpoint!("turn::fire_retries", err);
    let rows = engine.due_retry_rows(now_ms, MAX_RETRIES_PER_TICK).await?;
    fire_rows(engine, ctx, now_ms, rows).await
}

/// Cap on index rows one lift tick reads (each row: one read of the entity row
/// and one tenant lookup, under the engine). For rows fired, the cap is
/// [`MAX_RETRIES_PER_TICK`], like a normal tick.
const MAX_LIFTED_SCAN_PER_TICK: usize = 4 * MAX_RETRIES_PER_TICK;

/// The unfinished work of lifting the reschedule rows of a tenant just unsuspended
/// on one range — RAM of the leader term, kept by the background loop between ticks.
#[derive(Debug, Default)]
pub struct LiftSweep {
    /// Unsuspended tenants whose reschedule interval has not been fully scanned.
    tenants: Vec<cairn_store::TenantId>,
    /// Lower bound (exclusive) of the scan interval; each tick scans from `max(now, it)`.
    from_ms: u64,
    /// Upper bound (inclusive) of the scan interval.
    until_ms: u64,
    /// The last index row considered: the next tick carries on scanning after it.
    after: Option<(u64, InvocationId)>,
    /// Rows already fired in this scan: a firing moves the row to the next
    /// step, sometimes still inside the interval and after the cursor — it must
    /// not fire twice.
    fired: std::collections::BTreeSet<InvocationId>,
}

impl LiftSweep {
    /// Adds a tenant just unsuspended, at `now_ms`, with the suspended tenant's
    /// reschedule tick `window`. The due-time interval to scan for each tenant:
    ///
    /// - the leader **saw** the unsuspension (`marks: None`):
    ///   `(now, now + window + 1 s]` — every reschedule row of the tenant within
    ///   one upcoming reschedule tick, including ordinary retries whose backoff
    ///   step falls there;
    /// - the registry's marks (`Some((refused, lifted))`): rows rescheduled
    ///   while suspended are due at `reschedule time + window`, with the
    ///   reschedule time in `[refused, lifted]`
    ///   (± [`crate::flow::LIFT_SLACK`]) ⇒ `[refused + window − slack, lifted +
    ///   window + slack]`, cut below at `now`. No `refused` ⇒ from `now`.
    ///   Upper bound already passed ⇒ dropped: every row of that wave is
    ///   already due, the normal tick fires them.
    ///
    /// The scan's interval is the union of the intervals. A scan in progress (if
    /// any) restarts from the beginning of the interval — the new tenant's rows
    /// may lie before the cursor; rows already fired still do not fire twice.
    pub fn add(
        &mut self,
        lifted: Vec<crate::flow::Lifted>,
        now_ms: u64,
        window: std::time::Duration,
    ) {
        let w = window.as_millis() as u64;
        let slack = crate::flow::LIFT_SLACK.as_millis() as u64;
        let mut added = false;
        for l in lifted {
            let (from, until) = match l.marks {
                None => (now_ms, now_ms.saturating_add(w).saturating_add(1_000)),
                Some((refused, lifted)) => {
                    let until = lifted.saturating_add(w).saturating_add(slack);
                    if until <= now_ms {
                        continue;
                    }
                    let from = refused
                        .filter(|r| *r <= lifted)
                        .map_or(now_ms, |r| r.saturating_add(w).saturating_sub(slack))
                        .max(now_ms);
                    (from, until)
                }
            };
            if self.tenants.is_empty() {
                (self.from_ms, self.until_ms) = (from, until);
            } else {
                self.from_ms = self.from_ms.min(from);
                self.until_ms = self.until_ms.max(until);
            }
            if !self.tenants.contains(&l.tenant) {
                self.tenants.push(l.tenant);
            }
            added = true;
        }
        if added {
            self.after = None;
        }
    }

    /// Nothing left to lift.
    pub fn is_idle(&self) -> bool {
        self.tenants.is_empty()
    }
}

/// **Unsuspension ⇒ turns rescheduled while suspended run at once**, in batches
/// capped per tick.
///
/// Background turns of a suspended tenant are refused slots by the scheduler
/// and rescheduled `SUSPENDED_RETRY` (15 minutes) ahead (`Event::Throttled`,
/// FL-24). Unsuspending only wakes the in-RAM queues, so those rows used to wait
/// out the whole tick. Here: every reschedule row of `sweep.tenants` on the
/// range with a due time in `(now, unsuspension time + reschedule tick + 1 s]`
/// fires through exactly the path of a due row (`Event::RetryDue`, the core is
/// unchanged) — including ordinary retries whose backoff step falls in that
/// window: unsuspension counts as one backoff wait served for every waiting
/// retry of the tenant (the next step still grows, `fires`). The core does not
/// know the time: M4 enables `RetryDue` whenever there is a row.
///
/// Each call (one background tick, holding the engine) reads at most
/// [`MAX_LIFTED_SCAN_PER_TICK`] index rows and fires at most
/// [`MAX_RETRIES_PER_TICK`] rows — like `fire_retries` — then leaves the cursor
/// in `sweep` for the next tick: unsuspending a tenant at its `max-live` cap
/// does not hold the range's engine longer than a normal tick. Returns `true`
/// when work remains (the background loop wakes again at once, as when
/// `fire_retries` hits its cap).
///
/// A leader that came up after the unsuspension (never saw the tenant
/// suspended) — or a range that changed leader mid-scan — lifts by the
/// `refused-ms=`/`lifted-ms=` marks of the tenant registry
/// ([`crate::flow::Lifted`]): the term's first sync sees the marks, and the
/// scan interval is the due-time interval the suspension may have rescheduled
/// ([`LiftSweep::add`]).
pub async fn wake_lifted<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    now_ms: u64,
    sweep: &mut LiftSweep,
) -> Result<bool> {
    if sweep.is_idle() {
        return Ok(false);
    }
    let until = sweep.until_ms;
    // Rows due ≤ `now`: the background tick fires them as usual.
    let from = sweep.from_ms.max(now_ms);
    let (scanned, more) = engine
        .retry_rows_between(from, sweep.after, until, MAX_LIFTED_SCAN_PER_TICK)
        .await?;
    let mut cursor = more;
    let mut prev = sweep.after;
    let mut rows = Vec::new();
    for (id, row) in scanned {
        if rows.len() == MAX_RETRIES_PER_TICK {
            // Cap reached: the next tick carries on scanning from the last row considered.
            cursor = prev;
            break;
        }
        prev = Some((row.due, id));
        if sweep.fired.contains(&id) {
            continue;
        }
        if engine
            .tenant_of(&id)
            .await?
            .is_some_and(|t| sweep.tenants.contains(&t))
        {
            rows.push((id, row));
        }
    }
    let ids: Vec<InvocationId> = rows.iter().map(|(id, _)| *id).collect();
    let n = rows.len();
    if n > 0 {
        cairn_store::failpoint!("turn::wake_lifted::step");
        for _ in &rows {
            cairn_store::failpoint!("turn::wake_lifted::row");
        }
        tracing::debug!(
            rows = n,
            "tenant unsuspended: firing its parked retries now"
        );
    }
    // On a failure mid-batch the cursor stays put: the next tick reconsiders exactly this interval.
    fire_rows(engine, ctx, now_ms, rows).await?;
    sweep.fired.extend(ids);
    match cursor {
        Some(c) => {
            sweep.after = Some(c);
            Ok(true)
        }
        None => {
            *sweep = LiftSweep::default();
            Ok(false)
        }
    }
}

/// Fires the reschedule rows `rows` as due rows (`Event::RetryDue`).
async fn fire_rows<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    now_ms: u64,
    rows: Vec<(InvocationId, lc::RetryRow)>,
) -> Result<usize> {
    let mut fired = 0;
    let mut busy = Vec::new();
    for (id, row) in rows {
        // Never wait for the lock: an invocation in the middle of a turn — or
        // waiting for a slot, still holding its lock — has its reschedule
        // **moved** by one slot-wait cap. Leaving it in place means every tick
        // rescans it along with every other busy row, one read each on the
        // engine thread, without making progress (a hot object with 7 000
        // commands waiting ⇒ commits waiting ~1.5 s).
        //
        // Busy also covers a turn **handed out but not finished**
        // (`Runner::is_queued`): its task may still be waiting for the engine,
        // not yet holding the lock — handing out another one would be a
        // duplicate turn (the same question as `recover` and the safety net ask,
        // `turn::attendance`).
        //
        // A parked ticket (FL-30) is such a holder, but its turn rewrites the
        // row when granted: the core moves its row **far** away (one day, as a
        // watch row), not by `busy_defer_ms` each time — otherwise every ticket
        // would cost one commit every 10 s, scaling with the number of tickets.
        let attendance = lc::Attendance {
            parked: ctx.scheduler.is_parked(&id),
            ..life::attendance(ctx.locks, ctx.runner, &id)
        };
        let r = async {
            let mut asker = life::Asker::new(now_ms, ctx, attendance).with_row(Some(row));
            let plan = asker
                .ask(engine, ctx, id, lc::Event::RetryDue { row })
                .await?;
            if let (lc::RowChange::Set(to), None) = (plan.retry, plan.ram.handout) {
                // Busy: move it — one commit for the whole tick.
                return Ok(Fired::Busy(to));
            }
            // Cancel (done, failed, gone, `paused` awaiting a human), or
            // reschedule the **next** attempt before running: a crash mid-turn
            // still leaves a reschedule. Same attempt count — an attempt is only
            // counted when the turn enters `Running`; a firing is counted here
            // (`fires`, FL-21).
            life::commit(engine, id, &asker, &plan, None).await?;
            life::side(ctx.streams, &id, plan.side);
            Ok::<_, ActorError>(match plan.ram.handout.and_then(|t| t.planned) {
                Some(p) => Fired::Fire(p),
                None => Fired::Cancelled,
            })
        }
        .await;
        match r {
            Ok(Fired::Fire(planned)) => {
                ctx.runner.queue_planned(id, planned);
                fired += 1;
            }
            Ok(Fired::Busy(to)) => {
                crate::metrics::flow::bump(&crate::metrics::flow::RETRY_BUSY);
                busy.push((id, Some(row.due), to));
            }
            Ok(Fired::Cancelled) => {}
            Err(e) if e.is_fenced() => return Err(e),
            Err(e) => {
                tracing::warn!(invocation = %hex(&id), error = %e, "retry schedule failed; the next tick retries")
            }
        }
    }
    // One commit for the whole tick, not one per busy row.
    if let Err(e) = life::defer(engine, &busy).await {
        if e.is_fenced() {
            return Err(e);
        }
        tracing::warn!(error = %e, row = busy.len(), "could not postpone the retry of a busy invocation; the next tick retries");
    }
    Ok(fired)
}

/// Where a due row goes.
enum Fired {
    Fire(lc::Planned),
    Busy(lc::RetryRow),
    Cancelled,
}

/// Backoff per the handler's policy for attempt `attempt + 1`, if the
/// invocation's deployment declares one — one GET of entry 0; retries are rare,
/// the cost is acceptable.
pub(super) async fn retry_policy(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: &InvocationId,
    attempt: u32,
) -> Result<Option<u64>> {
    Ok(match engine.head(id).await? {
        Some((target, pinned)) => {
            let d = match pinned.as_deref() {
                Some(pid) => ctx.deployments.get(pid),
                None => deployment_for(ctx.deployments, &target),
            };
            d.and_then(|d| {
                d.contract(&target.service, &target.handler)
                    .retry_delay_ms(attempt + 1)
            })
        }
        None => None,
    })
}

/// At most this many due reschedules are considered per tick. If there are
/// more, the returned due-time hint is already ≤ `now`, the background loop
/// wakes at once and carries on — each pass's work is capped, and the engine
/// thread is released between passes.
pub(super) const MAX_RETRIES_PER_TICK: usize = 256;

/// How far to move the reschedule of a busy invocation: one slot-wait cap — the
/// task holding the lock either gets its slot within that time, or hits the cap
/// and reschedules itself (overwriting this row). A 1 s floor for very short or
/// disabled caps.
pub(super) fn busy_defer_ms(ctx: &Ctx<'_>) -> u64 {
    ctx.queue_wait
        .unwrap_or(DEFAULT_QUEUE_WAIT)
        .as_millis()
        .clamp(1_000, 60_000) as u64
}

#[cfg(test)]
mod lift_tests {
    use {
        super::LiftSweep,
        crate::flow::{Lifted, LIFT_SLACK},
        cairn_store::TenantId,
        std::time::Duration,
    };

    const W: Duration = Duration::from_secs(15 * 60);

    fn ms(d: Duration) -> u64 {
        d.as_millis() as u64
    }

    fn t(name: &str) -> TenantId {
        TenantId::new(name).unwrap()
    }

    /// The leader saw the unsuspension: `(now, now + reschedule tick + 1 s]`.
    #[test]
    fn seen_lift_sweeps_one_window_ahead() {
        let mut s = LiftSweep::default();
        s.add(
            vec![Lifted {
                tenant: t("acme"),
                marks: None,
            }],
            1_000,
            W,
        );
        assert_eq!((s.from_ms, s.until_ms), (1_000, 1_000 + ms(W) + 1_000));
        assert!(!s.is_idle());
    }

    /// A leader that came up after the unsuspension: the due-time interval the
    /// suspension `[refused, lifted]` may have rescheduled, widened by
    /// `LIFT_SLACK` at both ends, cut below at `now`; a wave entirely in the
    /// past is dropped; no `refused` means from `now`; unioned with the
    /// interval of a leader that saw the unsuspension.
    #[test]
    fn marks_bound_the_sweep_to_the_suspension() {
        let (refused, lifted, now) = (100_000, 160_000, 170_000);
        let mut s = LiftSweep::default();
        s.add(
            vec![Lifted {
                tenant: t("acme"),
                marks: Some((Some(refused), lifted)),
            }],
            now,
            W,
        );
        assert_eq!(
            (s.from_ms, s.until_ms),
            (
                refused + ms(W) - ms(LIFT_SLACK),
                lifted + ms(W) + ms(LIFT_SLACK)
            )
        );
        // A wave entirely in the past: every row of it is already due — no scan.
        let mut old = LiftSweep::default();
        old.add(
            vec![Lifted {
                tenant: t("acme"),
                marks: Some((Some(refused), lifted)),
            }],
            lifted + ms(W) + ms(LIFT_SLACK),
            W,
        );
        assert!(old.is_idle());
        // The lower bound never goes back past `now`; no `refused` ⇒ from `now`.
        let late = refused + ms(W);
        let mut cut = LiftSweep::default();
        cut.add(
            vec![Lifted {
                tenant: t("acme"),
                marks: Some((Some(refused), lifted)),
            }],
            late,
            W,
        );
        assert_eq!(cut.from_ms, late);
        let mut bare = LiftSweep::default();
        bare.add(
            vec![Lifted {
                tenant: t("acme"),
                marks: Some((None, lifted)),
            }],
            now,
            W,
        );
        assert_eq!(bare.from_ms, now);
        // Union with another tenant's interval as seen by a leader that saw the unsuspension.
        s.add(
            vec![Lifted {
                tenant: t("beta"),
                marks: None,
            }],
            now,
            W,
        );
        assert_eq!(
            (s.from_ms, s.until_ms),
            (
                now,
                (now + ms(W) + 1_000).max(lifted + ms(W) + ms(LIFT_SLACK))
            )
        );
        assert_eq!(s.tenants, vec![t("acme"), t("beta")]);
    }
}
