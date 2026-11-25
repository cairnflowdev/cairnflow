// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The timer tick: `tick`, `purge_archived`.

use super::*;

/// Cap on timers per tick, like [`MAX_RETRIES_PER_TICK`]: N timers coming due
/// together (cron fan-out, piled up after the store or the leader was away)
/// must not become one O(N) engine hold. When the cap is hit, the background
/// loop's next due time is the next due row — it wakes again at once,
/// releasing the engine between batches — and the next batch reads on from
/// after this batch's last row (`PartitionWriter::due_timers`). An
/// `/admin/tick` also delivers at most that many per range; call it again to
/// deliver more.
///
/// [`MAX_RETRIES_PER_TICK`]: super::retry::MAX_RETRIES_PER_TICK
pub(super) const MAX_TIMERS_PER_TICK: usize = 256;

/// Fires the due timers (at most [`MAX_TIMERS_PER_TICK`]), then carries on
/// with the invocations concerned.
///
/// Returns the number of invocations that made progress.
pub async fn tick<'c>(engine: &mut Lease, ctx: &Ctx<'c>, now_ms: u64) -> Result<usize> {
    let due = engine.due_timers(now_ms, MAX_TIMERS_PER_TICK).await?;
    let mut advanced = 0;
    for (due_ms, id, completion_id) in due {
        // **Write, then hand out — do not run**, and never wait for the lock
        // (core, `Event::TimerDue`). There is no engine release point in the
        // whole tick, so two ticks (background and `/admin/tick`) can never
        // deliver the same timer.
        //
        // - The invocation is in the middle of a turn (its lock is held): hand
        //   the wake-up to the scheduler anyway rather than skipping it — a task
        //   may wait for the lock, a background tick may not. Skipping would bet
        //   on "the next tick", and an `/admin/tick` with a future `now_ms` has
        //   no next tick that reaches that mark. If the timer was already handed
        //   to a turn that has not finished, that is enough.
        // - Stopped for good (cancelled while sleeping): the timer is garbage —
        //   delivering the completion would rerun an invocation that has ended.
        //   Clean up the timer row.
        // - Otherwise: write the notification and delete the timer row in the
        //   same commit (the target is routed by entry 0 — the condition for a
        //   timer to survive a restart, INV-3), then hand out the turn — unless
        //   a turn was already handed out and has not run yet (FL-20): its task
        //   takes the lock after this tick (the tick holds the engine all the
        //   way to the hand-out) and reads a journal with the completion just
        //   written. Two timers of one invocation coming due in one tick is the
        //   most common case.
        let attendance = life::attendance(ctx.locks, ctx.runner, &id);
        let in_flight = ctx.runner.timer_in_flight(&id, completion_id);
        let r = async {
            let mut asker = life::Asker::new(now_ms, ctx, attendance);
            let plan = asker
                .ask(engine, ctx, id, lc::Event::TimerDue { in_flight })
                .await?;
            match plan.wake {
                lc::WakeWrite::None => {}
                lc::WakeWrite::Discard => engine.drop_timer(due_ms, &id, completion_id).await?,
                lc::WakeWrite::Deliver(_) => {
                    settle_completion(
                        engine,
                        id,
                        Completion {
                            id: completion_id,
                            result: CompletionResult::Timer,
                        },
                        Source::Timer { due_ms },
                    )
                    .await?
                }
            }
            Ok::<_, ActorError>(plan)
        }
        .await;
        match r {
            Ok(plan) => match plan.wake {
                // Busy: hand out with the timer (if already handed to an unfinished turn, that is enough).
                lc::WakeWrite::None => {
                    if plan.ram.handout.is_some() {
                        ctx.runner.queue_timer(id, completion_id, due_ms);
                    }
                    advanced += 1;
                }
                lc::WakeWrite::Deliver(_) => {
                    if plan.ram.handout.is_some() {
                        ctx.runner.queue(id);
                    }
                    advanced += 1;
                }
                lc::WakeWrite::Discard => {}
            },
            // One broken invocation must not block the timers of every other
            // invocation in the tick; with write rights lost, stop for good.
            Err(e) if e.is_fenced() => return Err(e),
            Err(e) => {
                tracing::warn!(invocation = %hex(&id), error = %e, "timer delivery failed; the next tick retries")
            }
        }
    }
    advanced += fire_retries(engine, ctx, now_ms).await?;
    // Signals and cross-partition messages are no longer drained here: the
    // partition's drainer does it when there is work. The background tick only
    // pokes it, below.
    // One-way calls whose time has come (`OneWayCall.invoke_time`): move them
    // to the outbox BEFORE draining, so they go in this tick rather than the
    // next. The background loop wakes at the earliest due time of the schedule
    // table, so a delayed send goes on time like a timer, without waiting for
    // a tick.
    const MAX_DELAYED_PER_TICK: usize = 64;
    engine.promote_delayed(now_ms, MAX_DELAYED_PER_TICK).await?;
    // Server-side schedules that are due: a same-range run goes into the queue
    // at once, a cross-range firing becomes a `Fire` row that the drainer
    // carries away below.
    advanced += super::schedule::fire_due(engine, ctx, now_ms).await?;
    // The background tick only **pokes**. Nobody is waiting for its answer, so
    // standing by to see whether the drainer is done would hold the engine for
    // nothing.
    ctx.drainer.nudge();

    // Purge invocations past their retention — last, and capped: purging is
    // idle work, it must not cut in front of urgent work, and a backlog of a
    // thousand rows after a long downtime must not take the engine thread for a
    // whole tick. Not added to `advanced`: purging is not "an invocation made
    // progress".
    const MAX_PURGES_PER_TICK: usize = 32;
    for (due, id) in engine.due_purges(now_ms, MAX_PURGES_PER_TICK).await? {
        // A failed archive (flaky store) ⇒ **no** purge this time: the due time
        // stays, the next tick retries. Purging without archiving loses the
        // journal for good.
        if let Err(e) = purge_archived(engine, &id, Some(due)).await {
            if e.is_fenced() {
                return Err(e);
            }
            tracing::warn!(invocation = %hex(&id), error = %e, "not archived yet — cleanup postponed");
        }
    }
    // Sealed streams past their expiry, temporary streams of a suspended
    // invocation sitting idle: RAM only, no I/O.
    ctx.streams.tick();
    // `logFlushMs`: a second path committed only to make streams durable
    // (AP-16). An empty batch — riders fill it; nobody waits for it to be durable.
    if ctx.streams.take_flush_due(now_ms) {
        match engine
            .writer_mut()
            .commit(cairn_store::Mutations::new())
            .await
        {
            Ok(()) => crate::metrics::agent::bump(&crate::metrics::agent::STREAM_FLUSH_COMMITS[1]),
            Err(e) if e.is_fenced() => {
                return Err(ActorError::from(cairn_engine::EngineError::from(e)))
            }
            Err(e) => tracing::debug!(error = %e, "logFlushMs stream flush failed — next tick"),
        }
    }
    Ok(advanced)
}

/// Purges `id` **after** archiving it, if the node has archiving on.
///
/// Reads the snapshot under the engine lock, PUTs the archive object
/// **outside** the lock ([`Lease::released`] — a PUT to the object store is
/// network I/O, the whole range's engine must not be held while waiting), then
/// takes the engine back to commit the deletion. Between the two, the
/// invocation cannot change: it has stopped for good (purged by due time, or by
/// hand — [`cairn_engine::Engine::purge_now`] refuses an unfinished invocation).
///
/// The object's date is the UTC date of the purge due time `due` (fixed across
/// every retry), or of the purge time for a manual purge. The PUT is `Create`;
/// if it already exists, [`cairn_store::archive::ArchiveStore::put`] reads it
/// back and compares: from this same run ⇒ done, from another run with the same
/// id ⇒ written to the next slot.
pub async fn purge_archived(engine: &mut Lease, id: &InvocationId, due: Option<u64>) -> Result<()> {
    if let Some(store) = engine.archive_store().cloned() {
        if let Some(mut rec) = engine.archive_snapshot(id).await? {
            let now = engine.clock().wall_ms();
            rec.purged_at_ms = now;
            let date_ms = due.unwrap_or(now);
            // `--archive-artifacts`: artifact bytes go into the archive
            // **before** the record — the record never points at something the
            // archive does not have. By default only refs (journal), 0 COPYs.
            let copies = match (engine.archive_artifacts(), engine.artifact_store().cloned()) {
                (true, Some(arts)) => {
                    let mut v = Vec::new();
                    for (sha, row) in engine.writer().artifacts_of_unbounded(id).await? {
                        let Some(row) = row else { continue };
                        let owner = cairn_store::artifact::Owner {
                            tenant: rec.tenant.as_str().to_string(),
                            id: *id,
                            epoch: row.epoch,
                        };
                        v.push((sha, arts.path(&owner, &sha)));
                    }
                    v
                }
                _ => Vec::new(),
            };
            let (tenant, inv) = (rec.tenant.clone(), *id);
            engine
                .released(async |_: &Released| {
                    for (sha, from) in &copies {
                        store
                            .copy_artifact(&tenant, &inv, date_ms, sha, from)
                            .await?;
                    }
                    store.put(&rec, date_ms).await
                })
                .await?;
        }
    }
    engine.purge(id, due).await?;
    Ok(())
}
