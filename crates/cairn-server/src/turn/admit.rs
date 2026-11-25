// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The shared intake path: [`admit`].
//!
//! All **new** work — HTTP `/invoke` and `/send` (and every outside source going through
//! [`crate::ingress::Router`]), local child calls (outbox rows), cross-range `StartCall`
//! messages, a workflow's `run`, `restart` — asks here before an invocation exists.
//! [`start_invocation`] is private to this file (`scripts/lint-sources.sh` check 6 catches
//! calls to `start_invocation`/`create` outside it). Four questions, in order (after
//! question 0 — a `limited`/`suspended` tenant, or one that has hit `max-live=` ⇒
//! [`Admission::Refused`] for the ingress, `restart` and schedule sources):
//!
//! 1. **Can the source say this** — `Restart.patch` present ⇒ `400` while the feature gate
//!    is not open.
//! 2. **Does the work already exist** — a workflow's `run` has already run (the id is a
//!    function of `(scope, service, key)`), an idempotency key already points at a live
//!    invocation ([`holder_of`] — one lookup for every source). If so, answer (ingress) or
//!    attach the waiter to it (stream sources), create nothing.
//! 3. **L0 throttle** (FL-11): only blocks new work. The source says whether the throttle
//!    applies before or after question 2 — see [`Source`].
//! 4. **Create** — the `hold` quarantine (FL-14) and the rules (queues by limit key, read
//!    by the scheduler) are fixed in the same commit that creates.
//!
//! Invocation lock: ingress sources win (wait for) the id's lock, the `StartCall` source
//! tries the lock (busy ⇒ [`Admission::NotYet`], the row stays), local child calls take no
//! lock (the drainer is the outbox's only reader, the id is deterministic per row). A lock
//! won is handed back to the caller in [`Admission`].

use super::*;

/// The source of new work — it says **which throttle applies and where**.
///
/// The list is **open**. A future source already has a place in the contract but no
/// variant yet (no variant is added before the source exists):
///
/// - **`Kafka`**: each record is new work, `(topic, source partition, offset)` as the
///   idempotency key. L0 throttle like HTTP `send` (`Shed` ⇒ 429, nothing created); the
///   consumer **keeps the offset** — it does not commit the offset of a throttled record,
///   redelivers it after `retry_after`, and the redelivery matches thanks to the key. This
///   shape is already tested through [`crate::ingress::Router`]
///   (`tests/router_source.rs`).
pub enum Source<'a> {
    /// `POST /invoke`, `/send` (and outside sources through `ingress::Router`).
    ///
    /// The L0 throttle (`Throttle::admit_ingress`) **is decided when the actor receives the
    /// command** ([`Source::ingress`]), before any write, and **applied after** question 2:
    /// a resend of existing work is still answered as usual; new work ⇒
    /// [`Admission::Shed`] ⇒ `429 range_overloaded`, no id.
    Ingress { shed: bool },
    /// A local child call — an outbox row of this very range (`dispatch_call`).
    ///
    /// The L0 throttle (`Throttle::defer_dispatch`) applies **before** the idempotency
    /// lookup (the row is already durable, left for a later drain turn — no read at all on
    /// the congested range); only a workflow's `run` applies it **after** the "already
    /// exists" check (a shape that predates `admit`, kept as is).
    Call { reply_to: Option<ReplyTo> },
    /// A `StartCall` message from another range, already durable in `inbound`
    /// (`drain_inbound`).
    ///
    /// The L0 throttle (`Throttle::defer_inbound`) applies **before any read**; when
    /// throttled the id goes into `busy` so later messages for the same callee (signal,
    /// cancel) wait too. **Never** refused at `/internal/inbound`.
    Inbound {
        reply_to: Option<ReplyTo>,
        busy: &'a mut Busy,
    },
    /// `POST /invocations/{id}/restart` — the successor of an existing invocation.
    ///
    /// No L0 throttle, no quarantine (an admin command on existing work, as before
    /// `admit`). `patch` is the input replacing entry `n` ("fix it and rerun from step",
    /// behind a feature gate): present ⇒ [`Admission::Rejected`] (`400`) until the gate
    /// opens — this is the only place that has to change then.
    Restart { patch: Option<Patch> },
    /// A **same-range** firing of a server-side schedule: unkeyed target, deterministic id
    /// `(schedule, mark)` (`cairn_store::schedule_run_id`).
    ///
    /// The L0 throttle (`Throttle::defer_dispatch`, like a local child call) applies
    /// **before** the "already exists" check: when throttled that firing **creates
    /// nothing yet** and the schedule row does not move — the background loop fires that
    /// mark again on a later tick, it does not skip the mark. The id already exists (fired
    /// again after a crash, or a duplicate manual firing) ⇒ nothing created. No lock (new
    /// id, only the range's background loop creates it).
    Schedule,
}

impl Source<'static> {
    /// An ingress source with the L0 throttle decided **right now** — when the actor
    /// receives the `Invoke`/`Submit` command, before spawning a task and before any write.
    pub fn ingress(l0: &crate::l0::Throttle) -> Self {
        Source::Ingress {
            shed: !l0.admit_ingress(),
        }
    }
}

/// The input patch of `restart` (not enabled yet — [`Source::Restart`]).
#[derive(Clone, Debug)]
pub struct Patch {
    /// The entry replaced (0 = `Input`).
    pub entry: u32,
    pub input: Bytes,
}

/// The work asking to be admitted: invocation `id` is about to exist.
pub struct Request<'a> {
    pub id: InvocationId,
    pub target: &'a Target,
    /// Only affects queueing — not part of the identity.
    pub limit_key: Option<&'a LimitKey>,
    pub idempotency_key: Option<&'a str>,
    pub input: Bytes,
    /// The checked `cairnflow-log-level` header — ingress sources only; goes into entry 0.
    pub log_level: Option<&'a str>,
    /// A run fired by a schedule: the `cairnflow-schedule` header and — same range — the
    /// schedule row written in the creating commit (CG-12).
    pub schedule: Option<cairn_engine::ScheduleStamp<'a>>,
    /// Checked user tags — ingress and schedule sources only; go into the metadata and
    /// `ByAttr`. Call sources: the SDK's tags.
    pub tags: &'a [(String, String)],
    /// The sender of a one-way call and the root of the call tree — call sources only.
    pub sender: Option<InvocationId>,
    pub root_id: Option<InvocationId>,
}

/// The answer of [`admit`].
pub enum Admission<'c> {
    /// Just created and already queued — **one commit**. `held`: created directly in
    /// `Paused` because the handler is in the `hold` quarantine (FL-14); the caller does
    /// not run it. `guard`: the id's lock if the source wins locks.
    Created {
        guard: Option<InvGuard<'c>>,
        held: Option<InvokeOutcome>,
    },
    /// Ingress: the work already exists — answer the caller with this outcome, create nothing.
    Answered(InvokeOutcome),
    /// Stream source: the work already exists at `id` — the waiter has been attached to it
    /// (or has received the result/`573`), nothing created.
    Attached {
        id: InvocationId,
        guard: Option<InvGuard<'c>>,
    },
    /// L0 throttle: nothing created. Ingress ⇒ `Overloaded`; stream source ⇒ the row stays.
    Shed,
    /// Not decidable yet — the id lock is busy, or the service is not yet in the node's
    /// deployment registry (the `EXCLUSIVE` decision is fixed at creation, from the
    /// manifest). The row stays.
    NotYet,
    /// The source asks for something that does not exist yet (`400`).
    Rejected(String),
    /// The tenant is `limited`/`suspended` or has hit `max-live=`: its **new** work is
    /// refused — nothing created, nothing read. Only the ingress, `restart` and schedule
    /// sources; child calls and `StartCall` create as usual (running work runs to its
    /// outcome; for a `suspended` tenant the scheduler grants them no slot).
    Refused(cairn_engine::TenantRefusal),
    /// `restart` without a patch: carry on along the restart path.
    Admitted,
}

/// New work asking to enter this range. See the module docs.
pub async fn admit<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    source: Source<'_>,
    req: Request<'_>,
) -> Result<Admission<'c>> {
    // Question 0: does the tenant accept new work. The range's snapshot, copied when the
    // source changes generation — one atomic read, no shared lock. Stream sources (`Call`,
    // `Inbound`) do not ask: for them `admit` only has two "no" paths, both "the row stays,
    // retry later" — refusing for days that way would spin the outbox/inbound rows
    // endlessly.
    if matches!(
        source,
        Source::Ingress { .. } | Source::Restart { .. } | Source::Schedule
    ) {
        if let Some(why) = engine.tenant_refusal(&req.target.tenant) {
            return Ok(Admission::Refused(why));
        }
    }
    let workflow = kind_of(ctx.deployments, req.target) == InvocationKind::WorkflowRun;
    match source {
        Source::Restart { patch } => Ok(match patch {
            Some(p) => Admission::Rejected(format!(
                "restart with an input patch (entry {}) is not enabled yet; \
                 resend without a body",
                p.entry
            )),
            None => Admission::Admitted,
        }),
        Source::Ingress { shed } => admit_ingress(engine, ctx, shed, req).await,
        Source::Schedule => {
            if ctx.l0.defer_dispatch() {
                return Ok(Admission::Shed);
            }
            if engine.status(&req.id).await?.is_some() {
                return Ok(Admission::Attached {
                    id: req.id,
                    guard: None,
                });
            }
            create(engine, ctx, None, None, req, false).await
        }
        Source::Call { reply_to } => {
            if workflow {
                if engine.status(&req.id).await?.is_some() {
                    // The `573` receiver on the local path: the row is produced by the
                    // **caller** (a shape that predates `admit`).
                    let from = reply_to.as_ref().map(|r| r.caller).unwrap_or(req.id);
                    settle_workflow_duplicate(engine, &req, reply_to.as_ref(), from).await?;
                    return Ok(Admission::Attached {
                        id: req.id,
                        guard: None,
                    });
                }
                if ctx.l0.defer_dispatch() {
                    return Ok(Admission::Shed);
                }
                return create(engine, ctx, None, reply_to.as_ref(), req, true).await;
            }
            if ctx.l0.defer_dispatch() {
                return Ok(Admission::Shed);
            }
            let Some(idem) = req.idempotency_key else {
                // No idempotency key: the id deterministic per `(caller, completion)` is
                // enough for reprocessing the same outbox row not to spawn another
                // invocation.
                if engine.status(&req.id).await?.is_some() {
                    return Ok(Admission::Attached {
                        id: req.id,
                        guard: None,
                    });
                }
                return create(engine, ctx, None, reply_to.as_ref(), req, false).await;
            };
            if let Some(holder) = holder_of(engine, req.target, idem).await? {
                let id = holder.0;
                attach_waiter(engine, holder, reply_to.as_ref()).await?;
                return Ok(Admission::Attached { id, guard: None });
            }
            // `req.id` is already the id derived from the idempotency key.
            create(engine, ctx, None, reply_to.as_ref(), req, false).await
        }
        Source::Inbound { reply_to, busy } => {
            if ctx.l0.defer_inbound() {
                busy.insert(req.id);
                return Ok(Admission::Shed);
            }
            // **The workflow duplicate check is also resolved here** (target range): the
            // sender can only compute the id, it cannot read whether it already exists.
            if workflow {
                let Some(g) = try_lock(ctx, busy, req.id) else {
                    return Ok(Admission::NotYet);
                };
                if engine.status(&req.id).await?.is_some() {
                    // The `573` row is produced by the callee (a shape that predates `admit`).
                    let from = req.id;
                    settle_workflow_duplicate(engine, &req, reply_to.as_ref(), from).await?;
                    return Ok(Admission::Attached {
                        id: req.id,
                        guard: Some(g),
                    });
                }
                return create(engine, ctx, Some(g), reply_to.as_ref(), req, true).await;
            }
            // **Idempotency is resolved here, not on the caller's side**: the
            // `idempotency` table is state of the *target* range. Correct thanks to class 2
            // of `partition_for_target` — with no object key the range is derived from the
            // idempotency key itself, so every call with the same key reaches the same range.
            if let Some(idem) = req.idempotency_key {
                if let Some(holder) = holder_of(engine, req.target, idem).await? {
                    let id = holder.0;
                    attach_waiter(engine, holder, reply_to.as_ref()).await?;
                    return Ok(Admission::Attached { id, guard: None });
                }
            }
            // The lock covers both the "already exists?" check and the creation: two
            // duplicate `StartCall` messages (the sender resending after dying at exactly
            // the wrong spot) reaching two different drain turns would both see "not there"
            // and both ask for a place in the `EXCLUSIVE` queue.
            let Some(g) = try_lock(ctx, busy, req.id) else {
                return Ok(Admission::NotYet);
            };
            if engine.status(&req.id).await?.is_some() {
                return Ok(Admission::Attached {
                    id: req.id,
                    guard: Some(g),
                });
            }
            // A service not yet in this node's registry is **not created yet**: creating
            // before the registry catches up would create *without* queueing by object key.
            if !knows_service(ctx.deployments, req.target, &req.target.service) {
                return Ok(Admission::NotYet);
            }
            create(engine, ctx, Some(g), reply_to.as_ref(), req, false).await
        }
    }
}

/// Ingress source: question 2 first, throttle after (a resend is still answered).
async fn admit_ingress<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    shed: bool,
    req: Request<'_>,
) -> Result<Admission<'c>> {
    if shed {
        // Nothing to promise if it does not exist yet: return `Shed`, the caller answers now.
        return Ok(match existing_submission(engine, ctx, &req).await? {
            Some(o) => Admission::Answered(o),
            None => Admission::Shed,
        });
    }
    // A **busy** invocation lock = the invocation exists and its turn is holding the lock
    // — possibly for the whole wait for a VO lock behind thousands of the object's
    // commands. A resend only needs to know it exists: ask without the lock and answer
    // right away. A **free** lock (a new command — almost every time) goes straight down
    // the locked path: the lookup there is the only lookup. One extra lookup for every
    // command would double the read misses (lookups that find nothing) on the engine thread — on a just-merged range,
    // inheriting a dozen L0 views, enough to triple the whole range's tail latency.
    let guard = match ctx.locks.try_invocation(req.id) {
        Some(g) => g,
        None => {
            if let Some(o) = existing_submission(engine, ctx, &req).await? {
                return Ok(Admission::Answered(o));
            }
            ctx.locks.invocation(engine, req.id).await
        }
    };
    // Lock **before** the idempotency lookup, not just around `drive`: two requests with
    // the same idempotency key arriving at once carry the same `id` (the id is derived from
    // the key). Without the lock here both look up the table, both see "not there", both
    // create — and with an `EXCLUSIVE` handler both ask for a place in the queue, leaving an
    // orphan place nobody comes to claim.
    //
    // **One run per workflow id** must also be checked here, under the lock: two `run`
    // requests with the same key arriving at once both see "not there" in the unlocked
    // query, and the second one creates over the first one's entry 0. Only the "already
    // exists" answer of the unlocked query is trusted; a "not there" is asked again here.
    if let Some(o) = existing_submission(engine, ctx, &req).await? {
        return Ok(Admission::Answered(o));
    }
    // **A command resent with the same id**: the router resends after `Stopped` (the
    // range quiesced to split) with an id already fixed — the first attempt may have
    // committed. Commands without an idempotency key carry a random id, so only a resend
    // meets an existing id; without the check it would create over entry 0 and rerun the
    // invocation. Workflows and keyed commands are already resolved by
    // `existing_submission`. One point read, under the lock, only on the unkeyed creation
    // path.
    if req.idempotency_key.is_none() {
        if let Some(s) = engine.status(&req.id).await? {
            return Ok(Admission::Answered(answer_of(s)));
        }
    }
    create(engine, ctx, Some(guard), None, req, false).await
}

/// The client's answer for an invocation that already exists.
fn answer_of(status: InvocationStatus) -> InvokeOutcome {
    match status {
        InvocationStatus::Completed(v) => InvokeOutcome::Completed(v),
        InvocationStatus::Failed { code, message } => InvokeOutcome::Failed { code, message },
        InvocationStatus::Paused { code, message } => InvokeOutcome::Paused { code, message },
        // Still running: the client asks `/invocations/{id}` again.
        _ => InvokeOutcome::Waiting,
    }
}

/// Whether this ingress call **already has** an invocation (a workflow with the same id, or
/// an idempotency key already used) — and if so, what to answer the client.
///
/// Read-only, so it can be called **without the invocation lock**. `None` means nothing
/// exists yet: the caller wins the lock and asks again under it.
async fn existing_submission(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    req: &Request<'_>,
) -> Result<Option<InvokeOutcome>> {
    if kind_of(ctx.deployments, req.target) == InvocationKind::WorkflowRun
        && engine.status(&req.id).await?.is_some()
    {
        return Ok(Some(InvokeOutcome::AlreadyInvoked));
    }
    // Idempotency applies to the HTTP door too, not only `ctx.call`: a client retrying an
    // ingress request must not spawn a second invocation. Ingress has already derived `id`
    // from the key, so a duplicate call carries exactly the id of the invocation holding the
    // key — but the `idempotency` table is the source of truth (compared byte by byte), so
    // look it up rather than `status(id)`.
    let Some(idem) = req.idempotency_key else {
        return Ok(None);
    };
    Ok(holder_of(engine, req.target, idem)
        .await?
        .map(|(_, status)| answer_of(status)))
}

/// The **live** invocation holding idempotency key `idem` of `target` (at most one
/// invocation per `(tenant, scope, service, handler, key, idempotency_key)`), with its
/// state.
///
/// The `idempotency` table is the source of truth (keys compared **byte by byte**, not by
/// hash); the derived id is only a naming scheme — normally `holder == derived id`, they
/// only differ for data created by an old binary, and then the table wins. A mapping that
/// points at an invocation that no longer exists (cleaned up after retention) ⇒ `None`: the
/// key counts as unused, the creation path overwrites the mapping — attaching waiters to a
/// ghost invocation would hang them forever.
async fn holder_of(
    engine: &mut Lease,
    target: &Target,
    idem: &str,
) -> Result<Option<(InvocationId, InvocationStatus)>> {
    let Some(holder) = engine
        .idempotency_target(
            &target.tenant,
            target.scope(),
            &target.service,
            &target.handler,
            &target.object_key,
            idem,
        )
        .await?
    else {
        return Ok(None);
    };
    Ok(engine.status(&holder).await?.map(|s| (holder, s)))
}

/// A duplicate call (stream source): "attach to the running invocation and get the same
/// result" — **no new invocation**, even when nobody waits for the result (the guarantee
/// is "at most one invocation per key", not "except for `ctx.send`").
///
/// Even when `holder` is the invocation this very row created on an earlier pass (died
/// between creating and delivering the id): attaching again is harmless, because results
/// are keyed by **waiter** `(caller, completion)` — duplicate addresses collapse into one;
/// if it was already delivered, `settled_completions` in the caller's range blocks the
/// second one.
async fn attach_waiter(
    engine: &mut Lease,
    (holder, status): (InvocationId, InvocationStatus),
    reply_to: Option<&ReplyTo>,
) -> Result<()> {
    let Some(r) = reply_to else {
        return Ok(());
    };
    match status {
        // Already done: put the result into the outbox instead of attaching — nobody will
        // read the waiter list any more.
        InvocationStatus::Completed(v) => {
            engine
                .enqueue_reply(&holder, &r.caller, r.completion_id, v.into_reply())
                .await?;
        }
        InvocationStatus::Failed { code, message } => {
            engine
                .enqueue_reply(
                    &holder,
                    &r.caller,
                    r.completion_id,
                    ReplyResult::Failure { code, message },
                )
                .await?;
        }
        // Still running: add the name to the waiter list.
        _ => engine.attach(&holder, &r.caller, r.completion_id).await?,
    }
    Ok(())
}

/// A workflow's `run` that already exists (stream source).
///
/// "Is the waiter recorded in entry 0 this very call" — the test that tells a real
/// duplicate apart from our own row being processed again (died between creating and
/// deleting the row). A real duplicate ⇒ the caller receives [`WORKFLOW_ALREADY_INVOKED`]
/// and is **not** attached to the running run: a second `run` call carries different input
/// that would be dropped — returning the first run's result as if it were the second's
/// would be a lie. `ctx.send` (nobody waiting) has nobody to tell.
async fn settle_workflow_duplicate(
    engine: &mut Lease,
    req: &Request<'_>,
    reply_to: Option<&ReplyTo>,
    from: InvocationId,
) -> Result<()> {
    let owner = engine.reply_to_of(&req.id).await?;
    let mine = owner.as_ref().map(|r| (r.caller, r.completion_id))
        == reply_to.map(|r| (r.caller, r.completion_id));
    if mine {
        return Ok(());
    }
    if let Some(r) = reply_to {
        engine
            .enqueue_reply(
                &from,
                &r.caller,
                r.completion_id,
                ReplyResult::Failure {
                    code: WORKFLOW_ALREADY_INVOKED,
                    message: format!(
                        "workflow {}/{} was already invoked",
                        req.target.service, req.target.object_key
                    ),
                },
            )
            .await?;
    }
    Ok(())
}

/// Question 4: create. `workflow`: idempotency-key dedupe (at most one invocation per key) does **not apply** to `run` — the
/// workflow key is already the dedupe key, and two stacked dedupe mechanisms only raise
/// the question "which one wins" (the HTTP door refuses outright; stream sources — nobody
/// to tell — drop it).
async fn create<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    guard: Option<InvGuard<'c>>,
    reply_to: Option<&ReplyTo>,
    req: Request<'_>,
    workflow: bool,
) -> Result<Admission<'c>> {
    let idem = if workflow { None } else { req.idempotency_key };
    let held = start_invocation(
        engine,
        ctx,
        req.id,
        req.target,
        req.limit_key,
        reply_to,
        idem,
        req.input,
        req.log_level,
        req.schedule,
        req.tags,
        (req.sender, req.root_id),
    )
    .await?;
    Ok(Admission::Created { guard, held })
}

/// Create the invocation and put it in the virtual queue — **one commit**.
///
/// **Why it must queue.** `EXCLUSIVE` means an object key has only one invocation running
/// at a time — even while it is suspended mid-way on a `ctx.sleep`. Without the lock a
/// read-modify-write spanning the suspension point loses updates: two invocations both
/// read the old value and both overwrite it. Once the manifest declares `EXCLUSIVE` that
/// is a promise, not a hint.
///
/// This function does **not** decide "run now or queue". That decision belongs to the
/// scheduler ([`crate::flow`]), where it weighs all three resources rather than just the
/// VO lock.
///
/// **The `hold` quarantine** (FL-14): when the identity `(tenant, service, handler, current
/// deployment)` is held, the invocation is created directly in `Paused` with code
/// [`cairn_engine::QUARANTINED`] — same commit, no turn runs, no VO lock is held. Returns
/// `Some` with that outcome; the caller does not run it (on the background path
/// [`gate_turn`] blocks it). Reading the list is one atomic comparison and one hash: no
/// shared lock.
///
/// Private to this file: every creation path goes through [`admit`] (check 6 of
/// `scripts/lint-sources.sh`).
#[allow(clippy::too_many_arguments)]
async fn start_invocation(
    engine: &mut Engine,
    ctx: &Ctx<'_>,
    id: InvocationId,
    target: &Target,
    limit_key: Option<&LimitKey>,
    reply_to: Option<&ReplyTo>,
    idempotency_key: Option<&str>,
    input: Bytes,
    log_level: Option<&str>,
    schedule: Option<cairn_engine::ScheduleStamp<'_>>,
    tags: &[(String, String)],
    (sender, root_id): (Option<InvocationId>, Option<InvocationId>),
) -> Result<Option<InvokeOutcome>> {
    let tags: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let queue = queue_name(ctx.deployments, target, limit_key);
    let seq = engine.next_queue_seq(&queue).await?;
    let deployment = ctx
        .deployments
        .current_id(tenant_of(target), &target.service);
    let held = ctx
        .quarantine
        .held(
            tenant_of(target),
            &target.service,
            &target.handler,
            deployment.unwrap_or_default(),
        )
        .map(|e| {
            format!(
                "QUARANTINED: handler {}/{} (deployment {}) quarantined since {} — {}. \
                 Deploy a fix, then resume",
                target.service,
                target.handler,
                deployment.unwrap_or("-"),
                e.since_ms,
                e.reason
            )
        });
    engine
        .create(
            id,
            target,
            CreateOptions {
                reply_to,
                idempotency_key,
                queue: Some((&queue, seq)),
                // The contract is stamped into entry 0 **at creation**, read from the
                // deployment registry exactly once. See [`InvocationKind`].
                kind: kind_of(ctx.deployments, target),
                // Bound to the current revision, for life.
                deployment,
                paused: held.as_deref().map(|m| (cairn_engine::QUARANTINED, m)),
                log_level,
                schedule,
                tags: &tags,
                sender,
                root_id,
            },
            params_for(&id),
            input,
        )
        .await?;
    crate::metering::invocation(target.tenant.as_str());
    Ok(held.map(|message| {
        crate::quarantine::HELD.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        InvokeOutcome::Paused {
            code: cairn_engine::QUARANTINED,
            message,
        }
    }))
}
