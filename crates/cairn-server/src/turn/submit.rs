// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The running part of a just-admitted invocation (`finish_invocation*`) and
//! the creation path's deployment-registry readers. The admission part —
//! `start_invocation` and every check before it — is in [`super::admit`].

use super::*;

/// Tenant read from an outbox row / a cross-range message (`None` = `default`).
/// A broken value ⇒ `None` and a log line: no guessing — guessing `default`
/// would create invocations (and read state) in someone else's tenant.
pub(super) fn tenant_of_wire(
    raw: Option<&str>,
    who: &InvocationId,
) -> Option<cairn_store::TenantId> {
    match raw {
        None => Some(cairn_store::TenantId::default()),
        Some(t) => match cairn_store::TenantId::new(t) {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::error!(invocation = %hex(who), tenant = t, error = %e, "corrupt tenant on message — skipped");
                None
            }
        },
    }
}

/// The tenant of a target: the service registry belongs to the tenant — the
/// same service name in two tenants is two different deployments.
pub(super) fn tenant_of(target: &Target) -> &str {
    target.tenant.as_str()
}

/// The current revision of `target`'s service — for the **creation path** only.
/// An already created invocation runs on its own deployment ([`pinned_deployment`]).
pub(super) fn deployment_for<'a>(
    deployments: &'a Catalog,
    target: &Target,
) -> Option<&'a Deployment> {
    deployments.current(tenant_of(target), &target.service)
}

/// Whether there is any revision for `service` (in the same tenant as `target`).
pub(super) fn knows_service(deployments: &Catalog, target: &Target, service: &str) -> bool {
    deployments.current(tenant_of(target), service).is_some()
}

/// The deployment an already created invocation runs on: the one it was pinned to at creation.
///
/// - `Ok(Some)` — call exactly that one;
/// - `Ok(None)` — the invocation is not pinned (built by hand in tests): follow
///   the current revision, as before pinning existed;
/// - `Err(id)` — pinned to an id this node **has not loaded yet**: deployments
///   are immutable and cannot be removed while invocations are pinned to them,
///   so this can only be the registry refresh loop not yet having caught up
///   with a registration on another node — wait, do not fail.
pub(super) fn pinned_deployment<'a>(
    deployments: &'a Catalog,
    inv: &Invocation,
) -> std::result::Result<Option<&'a Deployment>, String> {
    match inv.deployment.as_deref() {
        Some(id) => deployments.get(id).map(Some).ok_or_else(|| id.to_string()),
        None => Ok(deployment_for(deployments, &inv.target)),
    }
}

/// Cap on the number of turns for one invocation.
///
/// Without a cap, an SDK that suspends and then makes no progress would spin
/// forever and take over the engine thread — the whole partition stalls
/// because of one broken invocation.
pub(super) const MAX_TURNS: usize = 256;

/// `random_seed` must be **stable across every replay**.
///
/// So it is derived from the invocation id, not generated fresh each turn.
/// Generating it fresh is the classic determinism bug: user code that seeds
/// `Math.random()` from it gets different results between two replays, and
/// the journal no longer matches.
pub(super) fn params_for(id: &InvocationId) -> StartParams {
    params_with_retry(id, 0)
}

/// The random seed of an invocation: hashes **the whole** id, with domain separation.
///
/// It used to be `id[..8]`. For ids created over HTTP, the first 8 bytes are 2
/// partition bytes + 6 bytes of the process's start mark — identical for every
/// invocation on the same partition in the same process, so every
/// `Math.random()` seeded from it produced the same sequence. Hashing the whole
/// id gives two different ids two different seeds, and the seed is still stable
/// across every replay (it depends only on the id).
pub(super) fn random_seed_of(id: &InvocationId) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::new()
        .chain_update(b"cairnflow-seed/1")
        .chain_update(id)
        .finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 is 32 bytes"))
}

/// Like [`params_for`], but also declares the number of retries so far.
///
/// The field `retry_count_since_last_stored_entry` **has been in
/// `StartMessage` since protocol v1** and `cairn-runtime` already emitted it —
/// but nobody filled it in, so it was always 0. An SDK therefore could not
/// count, and `maxRetryAttempts` could not be implemented: a tool calling a
/// permanently broken API would retry forever with a 60-second backoff, with no
/// way out other than `cancel`.
///
/// **The count resets on progress**, in the exact sense of "since last stored
/// entry": see [`cairn_engine::Engine::schedule_retry`].
pub(crate) fn params_with_retry(id: &InvocationId, retry_count: u32) -> StartParams {
    StartParams {
        debug_id: id.iter().map(|b| format!("{b:02x}")).collect(),
        random_seed: random_seed_of(id),
        retry_count_since_last_stored_entry: retry_count,
        ..Default::default()
    }
}

/// The grace period before telling the client "queued".
///
/// Almost every call contends for nothing and gets its slot **at once** in the
/// synchronous dispatch step of [`Scheduler::acquire`], i.e. before the `await`
/// even yields control. This period only covers the near misses; whoever has to
/// really wait gets a `202` right away instead of holding an HTTP connection.
pub const QUEUE_GRACE: std::time::Duration = std::time::Duration::from_millis(50);

/// Runs a just-created invocation until it stops for good or has to wait on the outside.
pub async fn finish_invocation<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    guard: &mut Option<InvGuard<'c>>,
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    finish_invocation_notifying(engine, ctx, id, guard, |_, _| {}, duty).await
}

/// Like [`finish_invocation`], but **reports early** if it has to queue.
///
/// This is the shape the `/invoke` door needs, and it is the old shape kept
/// intact: a call that has to wait for an `EXCLUSIVE` lock returns
/// `202 waiting` **at once**, without holding the request until its turn. What
/// differs is who runs it: previously the invocation in front had to drive the
/// next one by hand when it finished; now this task waits for its own slot and
/// the scheduler wakes it.
///
/// Thanks to that, "fairness" is real: whoever came first in the *durable
/// queue* is served first, not whoever the one just finishing happened to see.
pub async fn finish_invocation_notifying<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    id: InvocationId,
    guard: &mut Option<InvGuard<'c>>,
    on_queued: impl FnOnce(InvokeOutcome, &Committed),
    duty: &mut WakeDuty,
) -> Result<InvokeOutcome> {
    let outcome = drive_client(engine, ctx, id, guard, on_queued, duty).await?;

    // The reschedule was already set by `drive`; all that is left here is answering the client.
    if let InvokeOutcome::Retry { .. } = outcome {
        return Ok(outcome);
    }

    // Release the invocation lock **before** waiting for the drainer: the
    // drainer may need exactly this lock to deliver a result to this very
    // invocation (it only tries the lock, so holding it would only delay our
    // own work by one drain pass).
    drop(guard.take());

    // The work spawned by this invocation (`ctx.call`, `ctx.sendSignal`) must be
    // processed before answering the client — otherwise the client is told
    // "waiting" for something that is in fact ready to carry on right away. No
    // draining by ourselves: the partition's drainer does it, we only wait for it.
    wait_spawned_signals(engine, ctx).await?;
    sync_outbox_of(engine, ctx, id).await?;

    // Finished is finished: no drain makes an invocation that is done run
    // again.
    if !matches!(outcome, InvokeOutcome::Waiting) {
        return Ok(outcome);
    }

    // For `Waiting`, **the durable state must be asked again**, and the
    // condition must not be "did we manage to drain any row".
    //
    // That used to be the right measure when everything ran sequentially:
    // nobody else touched the queue, so draining nothing meant nothing changed.
    // Since turns run in parallel, a background tick may grab the drain lock
    // before us and complete exactly our outbox row — we would see "drained
    // nothing" and return `Waiting` for an invocation that is already
    // `completed`. The client still gets the right result on its next ask, but
    // it is a wrong answer at the moment it is given, and it depends on who runs
    // faster — the hardest kind of wrong to spot.
    Ok(match engine.status(&id).await? {
        Some(InvocationStatus::Completed(v)) => InvokeOutcome::Completed(v),
        Some(InvocationStatus::Failed { code, message }) => InvokeOutcome::Failed { code, message },
        Some(InvocationStatus::Paused { code, message }) => InvokeOutcome::Paused { code, message },
        _ => InvokeOutcome::Waiting,
    })
}
