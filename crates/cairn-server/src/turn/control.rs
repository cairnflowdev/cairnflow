// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Control from outside: cancel, external signal, resume, releasing a queue slot.

use super::*;

/// **Cooperative** cancellation: deliver the CANCEL signal and let the SDK clean up and
/// finish by itself.
///
/// Differs from [`cancel`] (the hard level, 572) in who decides: here it is the *user's
/// code* — pending futures throw `TerminalError`, the handler gets to run its cleanup
/// (`try/finally`), and it ends with its own failure `Output`. The price: it needs a live
/// SDK that answers; if the SDK is dead the invocation keeps running — then the hard level
/// is the remaining path, and that is why the hard level is still the admin API's default.
///
/// Returns the state **after** delivering the signal and driving: `None` = no such
/// invocation.
pub async fn cancel_signal<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    duty: &mut WakeDuty,
) -> Result<Option<InvocationStatus>> {
    // The running turn holds the invocation lock until it stops — a whole BIDI session, a
    // whole chain of RR turns resolved in place. Ring the bell first: the turn delivers
    // CANCEL into its own journal right away, and the delivery below — when the lock is
    // released — finds the identity already present.
    ctx.live.ask_cancel(&id);
    external_signal(
        engine,
        ctx,
        id,
        cairn_engine::SignalId::Idx(pb::BuiltInSignal::Cancel as u32),
        CompletionPayload::Void,
        duty,
    )
    .await
}

/// Deliver a signal **from outside the system** to a waiting invocation.
///
/// The door for an outside system to wake a workflow: a webhook, a callback, a human
/// pressing an approve button. [`cancel_signal`] is now just the special case of this
/// function with `Idx(CANCEL)` — it already was "send a signal from outside", only boxed
/// into exactly one signal.
///
/// Returns `None` = no such invocation; a terminal state = returned as is, treated as a
/// no-op (delivering a notification to a finished invocation would run it again).
pub async fn external_signal<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    signal: cairn_engine::SignalId,
    value: CompletionPayload,
    duty: &mut WakeDuty,
) -> Result<Option<InvocationStatus>> {
    let Some(status) = engine.status(&id).await? else {
        duty.ack();
        return Ok(None);
    };
    if status.is_terminal() {
        duty.ack();
        return Ok(Some(status));
    }
    let ds = cairn_engine::DeliverSignal {
        target: id,
        // The sender is an outside system, not an invocation — sender/entry_index only
        // serve as the queue key, and a direct delivery (`Source::None`) has no queue row.
        // The real dedupe is in the machine: a signal identity already seen makes the later
        // delivery a no-op — so webhook retries are safe.
        sender: id,
        entry_index: 0,
        signal,
        result: value,
        // A human's signal: ingress has already checked the tenant (`ensure_visible`); this
        // path does not go through the gate of `settle_signal_local`.
        tenant: cairn_engine::wire_tenant(engine.tenant_of(&id).await?.as_ref()),
    };
    deliver_signal_and_drive(engine, ctx, ds, Source::None, duty).await?;
    // The end (if the SDK finishes within this turn) must reach the waiters immediately.
    wait_spawned_signals(engine, ctx).await?;
    sync_outbox_of(engine, ctx, id).await?;
    engine.status(&id).await.map_err(Into::into)
}

/// Name of the outcome of a human command for its own audit record commit (an action that
/// changed nothing): the outcome's `Debug`, lower case (`notfound`, `notpaused`…).
fn outcome_name(o: &impl std::fmt::Debug) -> &'static str {
    let s = format!("{o:?}").to_ascii_lowercase();
    match s.as_str() {
        "cancelled" | "resumed" | "purged" | "restarted" => "ok",
        s if s.contains("notfound") => "not_found",
        _ => "noop",
    }
}

/// Take an invocation out of `Paused` and run it on immediately.
///
/// It runs on **here** rather than leaving it to the background tick: the operator has just
/// fixed what made it stall and is watching the screen, so the answer must be the real
/// outcome, not "received, ask again later".
pub async fn resume<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    duty: &mut WakeDuty,
) -> Result<cairn_engine::ResumeOutcome> {
    let mut guard = Some(ctx.locks.invocation(engine, id).await);
    let audit = ctx.audit_begin(engine, id).await;
    // Lifecycle core `Event::AdminResume`: `paused → running` (stage kept as is — a
    // status/stage mismatch that is valid and harmless), then run the turn under the same
    // lock. A failed unstall (including an ambiguous one — `running` may have been written):
    // the wake-up duty falls unacked, the safety net looks at the durable state.
    let (outcome, turn) = life::resume(engine, ctx, id).await?;
    audit_end(engine, ctx, audit, &id, outcome_name(&outcome)).await?;
    let Some(turn) = turn else {
        duty.ack();
        return Ok(outcome);
    };
    // Count again from zero (`Turn::reset`): an operator unstalling means "retry from the
    // new state", and carrying the failure count of the old world would make the new turn
    // pay for an incident that has already been handled.
    drive(engine, ctx, id, None, &mut guard, turn, duty).await?;
    drop(guard.take());
    wait_spawned_signals(engine, ctx).await?;
    sync_outbox_of(engine, ctx, id).await?;
    Ok(outcome)
}

/// Cancel an invocation (admin API).
///
/// The engine does the durable part in one commit ([`Engine::cancel`]); here we do what
/// comes *after* it, in the same order as a normal end in [`drive`]: give the place in the
/// EXCLUSIVE queue to the next one, then drain the outbox so the cancel result reaches the
/// waiters immediately instead of waiting for the next background tick.
pub async fn cancel<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
) -> Result<cairn_engine::CancelOutcome> {
    // Cancelling writes a terminal state, so it must stand outside every turn of that very
    // invocation — otherwise a turn in flight would go on committing onto an invocation that
    // was just ended.
    let mut guard = Some(ctx.locks.invocation(engine, id).await);
    let audit = ctx.audit_begin(engine, id).await;
    // Lifecycle core `Event::AdminCancel`: state `failed` 572, retry row, queue entry, then
    // the VO lock is released. No need to look for "the next one" ourselves: the scheduler
    // holds the wait list of exactly that key, and releasing the lock wakes them. Order:
    // remove the rows (commit) **first**, release the lock (RAM) **after** — the other way
    // round, the next one could start its turn while the cancelled one's entry is still in
    // the lock-holding stage, which recovery on becoming leader would read as "two holders
    // at once".
    let outcome = life::cancel(engine, ctx, id, "invocation cancelled via the admin API").await?;
    audit_end(engine, ctx, audit, &id, outcome_name(&outcome)).await?;
    if !matches!(outcome, cairn_engine::CancelOutcome::Cancelled) {
        return Ok(outcome);
    }
    drop(guard.take());
    sync_outbox(engine, ctx).await?;
    Ok(outcome)
}
