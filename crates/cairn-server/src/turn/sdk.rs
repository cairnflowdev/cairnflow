// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The two transports to the SDK: `drive_one`, `settle_turn`, `drive_rr`, `drive_bidi`.

use super::*;

/// Whether it has stopped for good.
///
/// `Waiting`, `Retry` and `Throttled` have **not** — the invocation is still
/// alive and still holds its place in the queue. `Paused` holds its place too:
/// it is waiting for a human, and releasing the lock would let another
/// invocation modify state it may be relying on.
pub(super) fn is_terminal(o: &InvokeOutcome) -> bool {
    matches!(
        o,
        InvokeOutcome::Completed(_) | InvokeOutcome::Failed { .. }
    )
}

/// What wakes an invocation: a completion (matched by id) or a signal (matched
/// by idx/name identity). Two different paths into the state machine — see
/// [`cairn_runtime::Event`] — but the same "delete the source in the same
/// commit" discipline.
pub enum Wake {
    Completion(Completion),
    Signal(pb::SignalNotificationMessage),
}

/// The outcome of [`drive_one`].
pub(super) enum Ran {
    /// The SDK (or preflight) reported an outcome; the commit ending the turn is
    /// decided by the core from it (`Event::TurnEnded`).
    Outcome(InvokeOutcome),
    /// Preflight has decided **the whole** commit ending the turn (core
    /// `Event::TurnStarted`): the SDK is not called. A rare path (deployment not
    /// yet loaded) — boxed to keep the enum small.
    Settled {
        outcome: InvokeOutcome,
        plan: lc::Plan,
        asker: Box<life::Asker>,
    },
}

/// Turn parameters taken from **the deployment the invocation is pinned to** —
/// set on **every** construction of the turn's machine (start of the turn, and
/// rebuilds mid-BIDI session): a machine freshly built from `load` does not
/// carry them, and one forgotten place is a turn running under the wrong rules.
///
/// - Wire v3: the keepalive tick reported to the SDK — a third of the cap, so
///   that two consecutive missed ticks still do not reach the cap. Only when
///   the deployment negotiated v3: a v2 SDK does not understand that field (and
///   the runtime never sends anything a deployment did not negotiate).
/// - The handler's retention (manifest) overrides the node's at the ending
///   commit.
/// - A `SHARED` handler of a virtual object only reads state (D-W5,
///   [`shared_object_handler`]) — the engine enforces it when entry 0 carries
///   the mark.
fn arm(inv: &mut Invocation, deployment: &Deployment, ctx: &Ctx<'_>) {
    inv.set_wire_v3(keepalive_for(deployment, ctx), false);
    inv.set_retention(
        deployment
            .contract(&inv.target.service, &inv.target.handler)
            .journal_retention,
    );
    if shared_object_handler(deployment, &inv.target) {
        inv.mark_shared_object_handler();
    }
    // SDKs before wire v6 receive values inline (the shell assembles them): the
    // engine ends the invocation when an assembled value exceeds `max-payload=`.
    if deployment.wire_version < crate::discovery::WIRE_SEGMENTED {
        inv.mark_inline_values();
    }
}

pub(super) async fn drive_one(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    mut pending: Option<(Wake, Source)>,
    turn: lc::Turn,
    retry_count: u32,
) -> Result<Ran> {
    // A turn failing midway (SDK, network) — the point only `return`s an error.
    cairn_store::failpoint!("turn::drive_one", err);
    let live = ctx.live.enter(id);
    for _ in 0..MAX_TURNS {
        // Someone asked to cancel while the previous turn ran (RR: each
        // `ctx.run` is a turn): CANCEL goes into the journal before the next
        // turn, like the caller's wake-up.
        if pending.is_none() && live.take_cancel() {
            pending = Some((Wake::Signal(live::cancel_notification()), Source::None));
        }
        // The completion is fed **after** the replay, not before.
        //
        // A state machine just loaded from the journal does not yet know which
        // completions are pending — it only learns that once the replay is done.
        // Feeding a completion into a machine that has not replayed makes it see
        // a notification for an undeclared id, i.e. a protocol violation, and
        // the invocation dies for an entirely wrong reason.
        //
        // Once fed, the journal has grown; redo the turn so the SDK receives that
        // notification too. Progress is guaranteed because the journal grew.
        if let Some((wake, source)) = pending.take() {
            let mut inv = engine.load(id, params_with_retry(&id, retry_count)).await?;
            engine.step(&mut inv, Event::Start).await?;
            match wake {
                Wake::Completion(completion) => {
                    engine.deliver(&mut inv, completion, source).await?;
                }
                Wake::Signal(s) => {
                    engine.deliver_signal(&mut inv, s, source).await?;
                }
            }
            continue;
        }

        let mut inv = engine.load(id, params_with_retry(&id, retry_count)).await?;

        let pinned = pinned_deployment(ctx.deployments, &inv);
        // ── The handler's contract, taken from **the invocation's deployment**:
        // changing a policy means registering a new deployment, a running
        // invocation keeps its own.
        let contract = pinned
            .as_ref()
            .ok()
            .copied()
            .flatten()
            .map(|d| d.contract(&inv.target.service, &inv.target.handler));
        // ── Preflight (core, `Event::TurnStarted`): out of attempts —
        // `retry_count` is the number of attempts counted since the journal last
        // grew — then give up **before** calling the SDK: attempt `max + 1` does
        // not run.
        let mut asker = life::Asker::new(engine.clock().wall_ms(), ctx, started());
        asker.view.contract = Some(life::preflight_contract(&pinned, &inv.target));
        // The tenant's state **now**, not at slot-grant time: the run may have
        // waited for durability, for the engine, or run child turns first.
        asker.view.tenant_refused_ms = super::permit::refused_ms(ctx, &inv.target.tenant);
        // The node's replay RAM budget: the turn reserves its durable journal
        // until the end of the turn, within the tenant's share. Not enough room
        // ⇒ the same "refuse before calling" path as a suspended tenant: the SDK
        // is not called, the run is rescheduled, the attempt count is unchanged.
        let _replay_hold = crate::replay_budget::NODE
            .try_hold_for(inv.target.tenant.as_str(), inv.journal_bytes());
        if _replay_hold.is_none() {
            asker.view.tenant_refused_ms = asker
                .view
                .tenant_refused_ms
                .or(Some(crate::replay_budget::REPLAY_RETRY_MS));
        }
        let plan = asker
            .ask(
                engine,
                ctx,
                id,
                lc::Event::TurnStarted { turn, retry_count },
            )
            .await?;
        match plan.then {
            lc::Then::CallSdk { .. } => life::side(ctx.streams, &id, plan.side),
            // Tenant suspended after the slot was granted: the core decides the
            // whole commit ending the turn — suspended until the reschedule
            // tick, VO lock kept, attempt count unchanged. The SDK is not called.
            lc::Then::Reply(lc::Reply::Throttled) => {
                let retry_after_ms = asker
                    .view
                    .tenant_refused_ms
                    .unwrap_or(THROTTLE_RETRY_AFTER_MS);
                return Ok(Ran::Settled {
                    outcome: InvokeOutcome::Throttled { retry_after_ms },
                    plan,
                    asker: Box::new(asker),
                });
            }
            // The core decides the whole commit ending the turn: `failed` 404,
            // queue entry withdrawn, retry row deleted — one commit (`life::fail`).
            lc::Then::Reply(lc::Reply::NotFound) => {
                return Ok(Ran::Settled {
                    outcome: InvokeOutcome::Failed {
                        code: 404,
                        message: format!("no deployment declares service {:?}", inv.target.service),
                    },
                    plan,
                    asker: Box::new(asker),
                })
            }
            // Retry with backoff, durable across restarts — the registry refresh
            // loop brings the deployment within a few seconds.
            lc::Then::Reply(lc::Reply::Outcome) => {
                tracing::info!(
                    invocation = %hex(&inv.id),
                    deployment = %pinned.as_ref().err().map_or("", String::as_str),
                    "the invocation's deployment is not loaded on this node yet — rescheduled"
                );
                // The core decides the whole commit ending the turn: the SDK was
                // not called, so the attempt counted at `Running` is given back.
                return Ok(Ran::Settled {
                    outcome: InvokeOutcome::Retry {
                        delay_ms: Some(lc::RETRY_SOON_MS),
                        failure: None,
                    },
                    plan,
                    asker: Box::new(asker),
                });
            }
            lc::Then::Reply(lc::Reply::Paused | lc::Reply::Ended) => {
                let max = asker
                    .view
                    .contract
                    .and_then(|c| c.max_attempts)
                    .unwrap_or_default();
                let code = cairn_engine::MAX_ATTEMPTS_EXHAUSTED;
                // The error of the last attempt — why it ran out of attempts. One
                // read, only on this rare branch.
                let message = match engine.last_failure(&id).await? {
                    Some(f) => format!(
                        "exhausted {max} attempts (retryPolicyMaxAttempts) — last error (attempt {}): \
                         {} {}",
                        f.attempt, f.code, f.message
                    ),
                    None => format!("exhausted {max} attempts (retryPolicyMaxAttempts)"),
                };
                // The core decides the whole commit ending the turn: the status
                // (`paused` 575 or `failed` 575), stage and retry row in **one**
                // commit (previously three: `pause_with`/`fail_with`, `end_turn`,
                // `cancel_retry`).
                return Ok(Ran::Settled {
                    outcome: if plan.then == lc::Then::Reply(lc::Reply::Paused) {
                        InvokeOutcome::Paused { code, message }
                    } else {
                        InvokeOutcome::Failed { code, message }
                    },
                    plan,
                    asker: Box::new(asker),
                });
            }
            other => unreachable!("TurnStarted: {other:?}"),
        }
        let (Ok(Some(deployment)), Some(contract)) = (pinned, contract) else {
            unreachable!("TurnStarted: CallSdk only with a deployment");
        };
        let with_timeout;
        let ctx = match contract.inactivity_timeout {
            Some(ms) => {
                with_timeout = Ctx {
                    // Also clamp contracts registered before the cap existed, by
                    // the cell's cap (SaaS profile: 15 minutes).
                    sdk_timeout: Some(
                        std::time::Duration::from_millis(ms).min(ctx.sdk_timeout_max),
                    ),
                    ..*ctx
                };
                &with_timeout
            }
            None => ctx,
        };

        // ── Turn parameters from the deployment: keepalive, retention, the
        // state-write rule of a VO's `SHARED` handler.
        arm(&mut inv, deployment, ctx);
        let mut opening = engine.step(&mut inv, Event::Start).await?;
        // The engine ends the invocation right at the opening batch (a replayed
        // segmented value over the cap for an SDK that reads inline): the SDK
        // is not called — the outcome goes through the turn's normal path.
        if inv.outcome().is_some() {
            if let Step::Done(outcome) = settle_turn(&inv) {
                return Ok(Ran::Outcome(outcome));
            }
        }
        // SDK ≤ v5 reads inline: assemble before encoding; v6 receives refs +
        // read URLs.
        super::values::prepare_down(
            engine,
            &ctx.clients.values,
            deployment.wire_version,
            &inv,
            &mut opening,
            super::values::Down::Opening,
        )
        .await?;

        // The mode is declared by the **deployment** at discovery, not chosen by
        // the node. A mixed cluster is normal: two deployments, two SDKs, two
        // modes, one engine.
        let tenant = inv.target.tenant.clone();
        // The per-handler summary key: the deployment is the one this turn
        // **runs on**, not the current revision.
        let key = crate::stats::HandlerKey {
            tenant: tenant.to_string(),
            service: inv.target.service.clone(),
            handler: inv.target.handler.clone(),
            deployment: deployment.id.clone(),
        };
        crate::stats::turn_started(&key);
        let started = tokio::time::Instant::now();
        let sdk_before = crate::stats::sdk_so_far();
        let span = crate::otel::turn_span(
            &inv.target.service,
            &inv.target.handler,
            match deployment.protocol_mode {
                ProtocolMode::RequestResponse => "REQUEST_RESPONSE",
                ProtocolMode::BidiStream => "BIDI_STREAM",
            },
        );
        let mut rt = super::rtrace::RuntimeTrace::new(
            contract.runtime_trace(),
            id,
            inv.target.tenant.clone(),
            retry_count,
            engine.clock().wall_ms(),
        );
        rt.begin(engine, ctx).await;
        // Request identity: `jti` binds the JWT to this very turn's `Start` —
        // the same id, the same `retry_count` that `Start` carries.
        let jti = ctx
            .clients
            .identity
            .signs()
            .then(|| crate::identity::invocation_jti(&id, retry_count));
        let jti = jti.as_deref();
        let driven = match deployment.protocol_mode {
            ProtocolMode::RequestResponse => {
                drive_rr(
                    engine, ctx, id, &mut inv, deployment, &opening, jti, &mut rt,
                )
                .instrument(span)
                .await
            }
            ProtocolMode::BidiStream => {
                drive_bidi(engine, ctx, deployment, inv, opening, jti, &mut rt, &live)
                    .instrument(span)
                    .await
            }
        };
        // Only a turn that reaches an outcome or a valid wait point is `ok` (a
        // child turn stopped because the tenant is suspended is not an error either).
        let turn_ok = matches!(
            driven,
            Ok(Step::Again
                | Step::Refused
                | Step::Done(InvokeOutcome::Completed(_) | InvokeOutcome::Waiting))
        );
        rt.end(engine, ctx, turn_ok).await;
        let step = driven?;
        // The SDK was not called ⇒ not a handler turn: no time counted, no CU.
        // The next round runs preflight, and the core suspends the run.
        if matches!(step, Step::Refused) {
            continue;
        }
        let took = started.elapsed();
        crate::metering::run_time(tenant.as_str(), took);
        crate::stats::turn_finished(ctx.stats, &key, took);
        let handler = crate::stats::sdk_so_far().saturating_sub(sdk_before);
        ctx.load
            .note_turn(cairn_store::partition_key_of(&id), took, handler);
        match step {
            // `Retry` carries no due time: the core reschedules per the handler's
            // policy at step `retry_count` (`Event::TurnEnded`; the SDK's
            // `next_retry_delay` wins).
            Step::Done(outcome) => return Ok(Ran::Outcome(outcome)),
            // What the SDK is waiting for is already in the journal: rerun the
            // turn so it sees it. On `BIDI_STREAM` this is already done inside
            // `drive_bidi` without opening a new connection — this branch is
            // only the path of
            Step::Again => continue,
            Step::Refused => unreachable!("handled by `continue` above"),
        }
    }
    Err(ActorError::Invoke(invoker::InvokeError::Transport(
        format!("invocation exceeded {MAX_TURNS} turns without stopping"),
    )))
}

/// Wire v4 side-channel frames (`StreamAppend`) that the turn layer consumes
/// without passing them to the state machine: records go into the range's
/// `StreamHub` — no `step`, no `sync` (AP-4). Only when the deployment
/// negotiated v4: below v4 the SDK must not send it, and the frame goes on to
/// the machine — 571 like an unknown type. `ArtifactPut` takes its own path
/// ([`artifact_frame`]).
///
/// A broken frame (cannot be decoded) is dropped and counted: an invocation
/// never fails because of a stream.
pub(super) async fn side_frame(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    deployment: &Deployment,
    inv: &Invocation,
    f: &cairn_protocol::Frame,
) -> bool {
    let id = &inv.id;
    if f.ty != cairn_protocol::MessageType::STREAM_APPEND
        || deployment.wire_version < crate::discovery::WIRE_STREAMS
    {
        return false;
    }
    let m = match cairn_protocol::decode(f) {
        Ok(Message::StreamAppend(m)) => m,
        _ => {
            crate::metrics::agent::bump(&crate::metrics::agent::STREAM_DROPPED);
            tracing::debug!(
                bytes = f.payload.len(),
                "corrupt StreamAppend frame — dropped"
            );
            return true;
        }
    };
    // The tenant's `max-streams=`: a new stream over the cap is dropped
    // **before** seeding (seeding creates the stream). The range's snapshot, no
    // shared lock.
    let limit = engine.tenant_limit(&inv.target.tenant);
    let cap = limit.max_streams;
    // The tenant's stream RAM share on the node (from a security review) —
    // bound before seeding or pushing.
    ctx.streams
        .bind_tenant(id, &inv.target.tenant, limit.max_stream_ram);
    if ctx.streams.over_user_cap(id, &m.name, cap) {
        crate::metrics::agent::add(
            &crate::metrics::agent::STREAM_DROPPED,
            m.records.len() as u64,
        );
        return true;
    }
    // A durable stream for the first time in this term: the next offset and the
    // continuation counter of the last chunk in the Db. On a read error the
    // batch is dropped — handing out offsets from 0 would overwrite durable chunks.
    if ctx.streams.needs_seed(id, &m.name, m.durable) {
        match engine.writer().stream_tail_unbounded(id, &m.name).await {
            Ok(tail) => ctx.streams.seed(id, &m.name, tail),
            Err(e) => {
                let n = m.records.len() as u64;
                crate::metrics::agent::add(&crate::metrics::agent::STREAM_DROPPED, n);
                tracing::debug!(error = %e, "cannot read the tail of the durable stream — batch dropped");
                return true;
            }
        }
    }
    // The handler's `logFlushMs`: a durable record schedules one flush on the
    // node's clock tick; the range's timer tick commits when it is due.
    if m.durable || crate::streams::is_system(&m.name) {
        if let Some(every) = deployment
            .contract(&inv.target.service, &inv.target.handler)
            .log_flush_ms
        {
            ctx.streams.arm_flush(every, engine.clock().wall_ms());
        }
    }
    let out = ctx.streams.append_capped(id, m, cap);
    if out.dropped > 0 {
        tracing::debug!(dropped = out.dropped, "stream records dropped by the limit");
    }
    // Tokens of the span just closed: into the in-RAM metering, flushed on the same tick.
    for (model, input, output) in &out.tokens {
        crate::metering::tokens(
            inv.target.tenant.as_str(),
            model.as_deref(),
            *input,
            *output,
        );
    }
    // Flush when full: a batch with only `Streams` rows — riders fill it — and
    // nobody waits for it to be durable. Between two `step`s, so it never
    // slips into a commit.
    if ctx.streams.wants_flush() {
        match engine
            .writer_mut()
            .commit(cairn_store::Mutations::new())
            .await
        {
            Ok(()) => crate::metrics::agent::bump(&crate::metrics::agent::STREAM_FLUSH_COMMITS[0]),
            Err(e) => {
                tracing::debug!(error = %e, "flushing a full durable stream failed — the next commit carries it")
            }
        }
    }
    true
}

/// Maximum PUTs of one artifact within one frame while they keep exceeding `T_put`.
const PUT_ROUNDS: u32 = 3;

/// The wire v4 `ArtifactPut` frame: PUT the object
/// `artifacts/<tenant>/<id>/<epoch>/<sha>` **with the engine released** (D16),
/// then commit the `Artifacts` row — all before handling the next frame (AP-1:
/// every entry pointing at the artifact comes after that commit in the same
/// writer, so it is durable after it). No durability wait here: the next
/// `sync` before anything is sent down to the SDK covers this commit.
///
/// `Ok(false)` = the frame goes on to the machine and is refused like an
/// unknown type (571, invocation `failed`): another frame, wire < v4, the
/// `streams/table` gate off, and a frame that **violates the protocol** — not
/// decodable, sha not matching the content, over
/// [`cairn_store::artifact::MAX_BYTES`]. Dropping it silently like a broken
/// `StreamAppend` is not allowed: a later ref would point at nothing. No
/// object and no row is written. A store error (PUT, reading/committing the
/// row) ⇒ a turn error, retried.
///
/// Epoch: that of the existing row of `id`, otherwise freshly drawn from
/// `entropy` (lazily, at the first artifact). If the row commit fails, the turn
/// fails; the retry does not see the row, so it draws another epoch — the
/// object just PUT becomes an orphan, which the sweep cleans up (AP-8).
pub(super) async fn artifact_frame(
    engine: &mut Lease,
    deployment: &Deployment,
    inv: &Invocation,
    f: &cairn_protocol::Frame,
) -> Result<bool> {
    if f.ty != cairn_protocol::MessageType::ARTIFACT_PUT
        || deployment.wire_version < crate::discovery::WIRE_STREAMS
    {
        return Ok(false);
    }
    let Some(store) = engine.artifact_store().cloned() else {
        return Ok(false);
    };
    let m = match cairn_protocol::decode(f) {
        Ok(Message::ArtifactPut(m)) => m,
        _ => return Ok(false),
    };
    // `max-artifact-bytes=`: the tenant has hit its cap ⇒ no PUT; the frame
    // goes to `Engine::step`, where the invocation ends with 579
    // `artifacts_limit` (a stable code, not 571 "unknown type").
    if engine.tenant_artifacts_full(&inv.target.tenant) {
        return Ok(false);
    }
    let sha = cairn_store::artifact::sha256(&m.content);
    if m.content.len() > cairn_store::artifact::MAX_BYTES || m.sha256.as_ref() != sha {
        tracing::debug!(
            bytes = m.content.len(),
            "ArtifactPut violates the protocol (sha mismatch or over the limit) — passed to the machine, 571"
        );
        return Ok(false);
    }
    let id = inv.id;
    let store_err =
        |e: cairn_store::StoreError| ActorError::from(cairn_engine::EngineError::from(e));
    let epoch = match engine
        .writer()
        .artifact_epoch(&id)
        .await
        .map_err(store_err)?
    {
        Some(e) => e,
        None => {
            let mut b = [0u8; 8];
            cairn_store::entropy::fill(&mut b);
            u64::from_be_bytes(b)
        }
    };
    let owner = cairn_store::artifact::Owner {
        tenant: inv.target.tenant.as_str().to_string(),
        id,
        epoch,
    };
    let size = m.content.len() as u64;
    // `T_put` (AP-9): the row must be committed within the deadline counted from
    // the **start** of the PUT (the object's age counts from then); past the
    // deadline, PUT again — overwriting, refreshing the age — before
    // committing. `min_age` ≥ 2 × `T_put`, so the sweep never sees an object
    // old enough while its row is still on its way to commit.
    let deadline = engine.artifact_put_deadline();
    for round in 0..PUT_ROUNDS {
        let started = engine.clock().mono();
        let content = m.content.clone();
        engine
            .released(async |_: &Released| {
                if round == 0 {
                    store.put(&owner, &sha, content).await
                } else {
                    store.refresh(&owner, &sha, content).await
                }
            })
            .await
            .map_err(store_err)?;
        crate::metrics::agent::bump(&crate::metrics::agent::ARTIFACT_PUTS);
        crate::metrics::agent::add(&crate::metrics::agent::ARTIFACT_PUT_BYTES, size);
        // The "object durable, row not yet committed" gap (AP-1): a crash here leaves an orphan.
        cairn_store::failpoint!("artifact::after_put", err);
        if engine.clock().mono().saturating_duration_since(started) <= deadline {
            break;
        }
        if round + 1 == PUT_ROUNDS {
            return Err(store_err(cairn_store::StoreError::Io(format!(
                "artifact: all {PUT_ROUNDS} PUTs exceeded T_put {deadline:?} — next attempt"
            ))));
        }
        tracing::debug!(
            round,
            "artifact: T_put exceeded between PUT and commit — PUT again"
        );
    }
    let mut muts = cairn_store::Mutations::new();
    // The row + the change to the range's artifact byte counter, reading the
    // prior state within the same engine hold as the commit.
    engine
        .stage_artifact_put(
            &mut muts,
            &id,
            &sha,
            &cairn_store::keys::ArtifactRow {
                epoch,
                size,
                content_type: m.content_type.clone(),
            },
        )
        .await?;
    engine.writer_mut().commit(muts).await.map_err(store_err)?;
    // Freshly PUT ⇒ straight into the range's cache.
    if let Some(cache) = engine.artifact_cache() {
        cache.insert(sha, m.content);
    }
    Ok(true)
}

/// The new copy's input from a `ContinueAsNew` frame.
pub(super) fn continue_input(f: &cairn_protocol::Frame) -> Result<Bytes> {
    match cairn_protocol::decode(f) {
        Ok(Message::ContinueAsNew(m)) => Ok(m.input),
        _ => Err(ActorError::Invoke(invoker::InvokeError::Transport(
            "corrupt ContinueAsNew frame".into(),
        ))),
    }
}

/// The keepalive tick for a turn on `deployment`: `cap / 3`, or `None` when
/// there is no cap or the deployment does not speak v3 yet.
pub(super) fn keepalive_for(deployment: &Deployment, ctx: &Ctx<'_>) -> Option<u64> {
    if deployment.wire_version < crate::discovery::WIRE_KEEPALIVE {
        return None;
    }
    ctx.sdk_timeout.map(|t| (t.as_millis() as u64 / 3).max(1))
}

/// The result of a turn: stopped for good, or rerun because what the SDK waits for is already there.
pub(super) enum Step {
    Done(InvokeOutcome),
    Again,
    /// The turn's tenant is refused slots right before a call out to the
    /// endpoint: no call. `drive_one` reruns preflight, and the core suspends
    /// the run until the suspended tenant's reschedule tick ([`refused_now`]).
    Refused,
}

/// Whether the turn's tenant is being refused slots **right now** — asked
/// right before every call out to the endpoint, after every wait: a slot
/// granted before a suspension must not call the SDK after the node has
/// applied the suspension (seen in a soak run: a 16 s durability wait before
/// the stream was opened). Counted in `cairnflow_flow_refused_at_call_total`.
fn refused_now(ctx: &Ctx<'_>, tenant: &cairn_store::TenantId) -> bool {
    let refused = ctx.scheduler.refuses(tenant);
    if refused {
        crate::metrics::flow::bump(&crate::metrics::flow::REFUSED_AT_CALL);
    }
    refused
}

/// The state machine's outcome after the SDK ends a turn, translated into a [`Step`].
///
/// A frame sent up by the SDK exceeds the frame cap (`FrameTooLarge`, 16 MiB): `(length, cap)`.
fn frame_too_large(e: &invoker::InvokeError) -> Option<(u32, u32)> {
    match e {
        invoker::InvokeError::Protocol(cairn_protocol::ProtocolError::FrameTooLarge {
            len,
            max,
        }) => Some((*len, *max)),
        _ => None,
    }
}

/// **An oversized frame from the SDK is a permanent error**: the same code run
/// again replays exactly that frame — retrying only repeats the closure's side
/// effects (a `ctx.run` returning a result that is too large) until
/// `maxAttempts` runs out, or forever. The invocation is stopped as when the
/// SDK itself reports an `Error` with behaviour `PAUSE` (571
/// `PROTOCOL_VIOLATION`): the turn takes the state machine's normal ending path
/// — commit `paused`, keep the VO lock, and a `resume` after the code is fixed
/// reruns from the journal.
async fn pause_oversized_frame(
    engine: &mut Lease,
    inv: &mut Invocation,
    len: u32,
    max: u32,
) -> Result<Step> {
    tracing::warn!(
        invocation = %cairn_store::id_hex(&inv.id),
        len,
        max,
        "SDK sent a frame over the frame limit — pausing instead of retrying"
    );
    let error = Message::Error(pb::ErrorMessage {
        code: PROTOCOL_VIOLATION,
        message: format!(
            "the SDK sent a {len}-byte frame, over the {max}-byte frame limit; retrying would run the same code again"
        ),
        behavior: pb::ErrorBehavior::Pause as i32,
        ..Default::default()
    });
    let mut buf = bytes::BytesMut::from(&error.encode(cairn_protocol::Flags::default())[..]);
    let frame = cairn_protocol::FrameDecoder::new()
        .decode(&mut buf)
        .ok()
        .flatten()
        .expect("one whole Error frame, just encoded");
    engine.step(inv, Event::FromSdk(frame)).await?;
    Ok(settle_turn(inv))
}

/// Shared by both transports — and it must be shared, because that is exactly
/// the promise "same journal, same semantics, only the I/O differs". A
/// turn-ending rule written twice is a rule that will diverge at the third edit.
pub(super) fn settle_turn(inv: &Invocation) -> Step {
    match inv.outcome() {
        // An `Output` carrying a failure is a FAILED ending (same reasoning as
        // the engine's `status_of`): returning "completed" with an empty output
        // would lie to the HTTP caller while the durable status records `failed`.
        Some(Outcome::Completed(o)) => Step::Done(match &o.result {
            // The machine has already refused `segments` from the SDK outside a
            // sealed frame; segmented ⇒ ref + source owner, like the engine's
            // `status_of`.
            Some(pb::output_command_message::Result::Value(v)) => {
                match cairn_engine::output_of(v, &inv.target, &inv.id) {
                    Ok(b) => InvokeOutcome::Completed(b),
                    Err(e) => InvokeOutcome::Failed {
                        code: cairn_protocol::PROTOCOL_VIOLATION,
                        message: e.to_string(),
                    },
                }
            }
            Some(pb::output_command_message::Result::Failure(f)) => InvokeOutcome::Failed {
                code: f.code,
                message: f.message.clone(),
            },
            None => InvokeOutcome::Completed(Bytes::new().into()),
        }),
        Some(Outcome::Failed {
            code,
            message,
            action,
        }) => {
            let (code, message) = (*code, message.clone());
            Step::Done(match action {
                // A retry does **not** rerun right here.
                //
                // Looping in place means calling the SDK again at once, up to
                // 256 times, with the very error that just happened. Return it so
                // the layer above schedules it, with backoff, durable across restarts.
                FailureAction::Retry { delay_ms } => InvokeOutcome::Retry {
                    delay_ms: *delay_ms,
                    failure: Some((code, message)),
                },
                FailureAction::Pause => InvokeOutcome::Paused { code, message },
                FailureAction::Fail => InvokeOutcome::Failed { code, message },
            })
        }
        Some(Outcome::Suspended(s)) => {
            // Is what the SDK is waiting for already in the journal?
            //
            // Yes ⇒ it just has not *seen* it yet: the notification was written
            // in this very turn, after the request sent down to the SDK was
            // already built (`ctx.get` is the typical case). Letting it see it
            // makes it carry on at once.
            //
            // No ⇒ it is waiting on the outside world — a timer, or the result
            // of a `ctx.call`. Calling it again only costs one more round trip to
            // get exactly the same suspension.
            //
            // Measured by "is what the SDK waits for already there", **not** by
            // "did the journal grow": the very command that causes the suspension
            // (`Sleep`, `Call`) also grows the journal, so counting entries would
            // make *every* suspension cost one extra call down to the SDK — right
            // on the hot path of the workloads this targets.
            // With a combinator tree, "enough to carry on" is decided by the
            // tree: `ALL` needs all of them, `FIRST` needs one. Waking the SDK
            // while the tree is not satisfied only gets exactly the same
            // suspension.
            let settled = inv.settled_completions();
            let signals = cairn_runtime::SettledSignals {
                signals: inv.settled_signals(),
            };
            if cairn_runtime::suspension_satisfied(s, &settled, signals) {
                Step::Again
            } else {
                Step::Done(InvokeOutcome::Waiting)
            }
        }
        // The SDK closed the connection without Output+End, without Suspension,
        // without Error — the service protocol says a turn must end with
        // exactly one of the three. A **retryable failed** attempt with
        // `lastFailure` 571, like an SDK that makes no progress
        // (`sdk_timed_out`): the machine has no outcome, so nothing is
        // committed, and reporting a terminal outcome to the core here would
        // withdraw the queue entry and release the VO lock of an invocation that
        // is still `running`. Retries are capped by the policy (`maxAttempts` ⇒
        // `paused` 575).
        None => Step::Done(InvokeOutcome::Retry {
            delay_ms: None,
            failure: Some((
                PROTOCOL_VIOLATION,
                "SDK ended the turn without Output+End, Suspension or Error".into(),
            )),
        }),
    }
}

/// One `REQUEST_RESPONSE` turn: send the whole journal, read the whole response.
///
/// **The engine is released while waiting for the SDK.** That is the whole
/// point of the split: a handler running for 30 seconds no longer holds the
/// engine thread for 30 seconds. `inv` crosses this release point safely
/// thanks to the invocation lock — no other turn can touch it and make the
/// in-RAM snapshot diverge from the journal.
#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_rr(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    inv: &mut Invocation,
    deployment: &Deployment,
    opening: &[Message],
    jti: Option<&str>,
    rt: &mut super::rtrace::RuntimeTrace,
) -> Result<Step> {
    crate::metrics::sdk::RR_TURNS.bump(engine.partition().0);
    // ── Wire v5: with a hint, send a delta — `Start.replay_from = B`,
    // `prefix_digest` recomputed over the **current** journal, only entries
    // `[B, N)`. Every ambiguity ⇒ `B = 0`, a body byte-identical to v4.
    let capable = super::replay::delta_capable(deployment);
    let enc = capable
        .then(|| cairn_runtime::replay::Opening::new(opening))
        .flatten();
    let now_ms = engine.clock().wall_ms();
    let plan = if capable {
        ctx.replay.plan(&id, deployment, enc.as_ref(), now_ms)
    } else {
        super::replay::Plan::default()
    };
    let full_body = || match &enc {
        Some(e) => e.body(0),
        None => invoker::encode_stream(opening),
    };
    let from = plan.from;
    let body = match &enc {
        Some(e) if from > 0 => e.body(from),
        _ => full_body(),
    };
    // The journal size net: bytes of the turn's replayed part, full or delta;
    // over 10 MiB ⇒ one `warn` per invocation per term — a rare event, not on
    // the turn's normal path.
    let journal_bytes = enc
        .as_ref()
        .map_or(body.len(), |e| e.prefix_bytes(e.known_entries())) as u64;
    crate::metrics::sdk::note_rr_journal(journal_bytes);
    if journal_bytes > crate::metrics::sdk::RR_JOURNAL_WARN_BYTES && ctx.replay.warn_once(&id) {
        tracing::warn!(
            invocation = %hex(&id),
            bytes = journal_bytes,
            "REQUEST_RESPONSE journal exceeds 10 MiB: every turn without a cache resends the whole journal — \
             move large payloads to ctx.artifact, or run BIDI_STREAM/replayCache"
        );
    }
    // `REQUEST_RESPONSE` has no progress signal midway — the SDK sends its whole
    // response once at the end. So in this mode the "no progress" cap equals the
    // cap for **the whole turn**, and that has to be said out loud: the same
    // number means two slightly different things in the two modes.
    let durable = engine.sync().await?;
    let signing = jti.map(|jti| invoker::Signing {
        identity: &ctx.clients.identity,
        tenant: inv.target.tenant.as_str(),
        jti,
        now_ms: engine.clock().wall_ms(),
    });
    let gzip = deployment.rr_compression.as_deref() == Some(crate::discovery::RR_GZIP);
    // In turn: a delta from `B`; a miss carrying a valid `k` the SDK still holds
    // ⇒ resend **once** from `k`; otherwise — or on a miss at that point too —
    // full. Every resend is immediate, with the same identity, does not count
    // as an attempt and writes nothing durable (like Temporal not bumping the
    // attempt when a sticky cache is lost).
    let full_len = enc.as_ref().map_or(0, |e| e.full_len()) as u64;
    let mut sent = body.len() as u64;
    let mut at = from;
    let mut next = Some(body);
    let mut misses = 0u32;
    let mut tried_held = false;
    let reply = loop {
        let this = next.take().expect("body of the next send");
        let Some(reply) = awaiting_sdk(engine, ctx, &durable, async |r: &Released| {
            // The engine is released: pausing here does not block the range.
            cairn_store::failpoint!("turn::sdk::before_call");
            // Every request is one rerun of the handler in the SDK — including a
            // resend after a cache miss: ask the tenant state right before it.
            if refused_now(ctx, &inv.target.tenant) {
                return None;
            }
            Some(
                invoker::call(
                    &ctx.clients.http,
                    &deployment.endpoint,
                    &inv.target.service,
                    &inv.target.handler,
                    deployment.wire_version,
                    this,
                    signing,
                    Some(inv.target.tenant.as_str()),
                    gzip,
                    &plan.affinity,
                    r,
                )
                .await,
            )
        })
        .await
        else {
            return Ok(sdk_timed_out(
                ctx,
                engine.partition(),
                &id,
                "REQUEST_RESPONSE",
            ));
        };
        let Some(reply) = reply else {
            return Ok(Step::Refused);
        };
        // A miss on a full turn (`at == 0`) does not take this branch: it is an SDK error.
        let Err(invoker::InvokeError::ReplayMiss { held }) = reply else {
            break reply;
        };
        if at == 0 {
            break Err(invoker::InvokeError::ReplayMiss { held });
        }
        misses += 1;
        crate::metrics::sdk::replay(crate::metrics::sdk::Replay::Miss);
        ctx.replay.forget(&id);
        // The "SDK answered a miss, not yet resent" gap: a crash here fails the
        // turn like a transport error — the next attempt replays in full (the
        // hint was dropped).
        if misses == 1 {
            cairn_store::failpoint!("rr_replay::after_miss", err);
        }
        // `k` is the SDK's claim: only used when 0 < k ≤ N, k ≠ the point that
        // just missed, and `h_k` computed over the **current** journal equals the
        // `h_k` the SDK reported.
        let resume = enc.as_ref().and_then(|e| {
            let (k, h) = held.filter(|_| !tried_held)?;
            (k > 0 && k <= e.known_entries() && k != at && e.digest(k) == Some(&h))
                .then(|| (k, e.body(k)))
        });
        let body = match resume {
            Some((k, b)) => {
                tried_held = true;
                at = k;
                b
            }
            None => {
                at = 0;
                full_body()
            }
        };
        sent += body.len() as u64;
        next = Some(body);
    };
    if from > 0 {
        ctx.replay.note(&deployment.id, sent, full_len, now_ms);
        if reply.is_ok() && at > 0 {
            crate::metrics::sdk::replay(if misses == 0 {
                crate::metrics::sdk::Replay::Hit
            } else {
                crate::metrics::sdk::Replay::Partial
            });
            if let Some(e) = &enc {
                crate::metrics::sdk::REPLAY_SAVED_BYTES.fetch_add(
                    e.prefix_bytes(at) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }
    } else {
        crate::metrics::sdk::replay(if plan.why_full == Some("disabled") {
            crate::metrics::sdk::Replay::Disabled
        } else {
            crate::metrics::sdk::Replay::Full
        });
    }
    let reply = match reply {
        Ok(r) => r,
        Err(e) => {
            ctx.replay.forget(&id);
            if let Some((len, max)) = frame_too_large(&e) {
                return pause_oversized_frame(engine, inv, len, max).await;
            }
            return Err(e.into());
        }
    };
    if capable {
        ctx.replay.record(
            &id,
            deployment,
            enc.as_ref(),
            reply.cache,
            reply.affinity,
            now_ms,
        );
    }
    let frames = reply.frames;

    for f in frames {
        // The turn already has an outcome (`End`, `Suspension`, `Error`): as in
        // `drive_bidi`, do not read further. A later frame reaching an already
        // `Closed` machine would become a second ending that gets committed (571)
        // while the in-RAM outcome is still the first — the durable state would
        // diverge from what the shell reports to the core.
        if inv.outcome().is_some() {
            tracing::debug!(message_type = %f.ty, "dropped frame after the turn outcome");
            break;
        }
        // Keepalive (v3) means nothing in a response body that was read in full.
        if f.ty == cairn_protocol::MessageType::KEEPALIVE {
            continue;
        }
        // ContinueAsNew (v3): ends the turn and the invocation. A new input over
        // `max-payload=` ⇒ let `engine.step` end the invocation with
        // `payload_too_large` (below), without building a new invocation.
        if f.ty == cairn_protocol::MessageType::CONTINUE_AS_NEW && !engine.frame_oversized(inv, &f)
        {
            let input = continue_input(&f)?;
            return Ok(Step::Done(continue_as_new(engine, ctx, inv, input).await?));
        }
        // Wire v4 side channel: does not enter the state machine. In RR the
        // response body is read in full at the end, so records are applied at
        // the end of the turn, in exactly their order in the body.
        if artifact_frame(engine, deployment, inv, &f).await? {
            continue;
        }
        if side_frame(engine, ctx, deployment, inv, &f).await {
            continue;
        }
        if f.ty.kind() == MessageKind::Command {
            crate::metering::steps(inv.target.tenant.as_str(), 1);
            crate::stats::note_step();
        }
        rt.frame(engine, ctx, inv, &f).await;
        let v6 = deployment.wire_version >= crate::discovery::WIRE_SEGMENTED;
        let out = match super::values::seal_frame(engine, inv, &f, v6).await? {
            Some(s) => {
                engine
                    .step_sealed_with(inv, s.frame, s.fresh, s.old_bytes)
                    .await?
            }
            None => engine.step(inv, Event::FromSdk(f)).await?,
        };
        // The service protocol: `0xFC00+` must be forwarded. Here **there is
        // nowhere to forward it to** — there is only one SDK at the other end and
        // it just sent this, and the response body is not built yet so nothing
        // can be written back into it. Dropping it is right; dropping it
        // silently is not.
        for m in out {
            if m.kind() == MessageKind::Custom {
                tracing::warn!(
                    message_type = %m.message_type(),
                    "custom message dropped: REQUEST_RESPONSE has no forwarding path"
                );
            }
        }
    }
    Ok(settle_turn(inv))
}

/// One `BIDI_STREAM` turn: one h2c connection, both directions running in parallel.
///
/// Three things do **not** change compared with [`drive_rr`], and they are why this function is short:
///
/// - **The engine is still one thread, and still blocks while driving.** The
///   blocking point changes from "wait for the whole response" to "wait for
///   the next frame". No new invoker task, no completion-routing channel —
///   that is the ten-thousand-line road we do not need as long as we keep the
///   turn model.
/// - **The "durable before sent" rule is intact.** Every frame the SDK sends up
///   goes through `engine.step` with the same commit *before* any notification
///   is pushed down — that ordering lives in `step`, not here.
/// - **The journal does not change.** Exactly the same entries and order.
///
/// What changes: when the SDK suspends on something the runtime can resolve
/// locally, we do **not** close the stream. The state machine is rebuilt (it
/// went `Closed` at `Suspension`) but the replayed part is **not resent** —
/// the SDK keeps its own local journal and reruns the handler in RAM. That is
/// the whole saving: no HTTP round trip, no journal resend.
#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_bidi(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    deployment: &Deployment,
    mut inv: Invocation,
    opening: Vec<Message>,
    jti: Option<&str>,
    rt: &mut super::rtrace::RuntimeTrace,
    live: &live::LiveTurn<'_>,
) -> Result<Step> {
    let id = inv.id;
    crate::metrics::sdk::BIDI_STREAMS.bump(engine.partition().0);
    let body = invoker::encode_stream(&opening);
    let durable = engine.sync().await?;
    let signing = jti.map(|jti| invoker::Signing {
        identity: &ctx.clients.identity,
        tenant: inv.target.tenant.as_str(),
        jti,
        now_ms: engine.clock().wall_ms(),
    });
    let Some(session) = awaiting_sdk(engine, ctx, &durable, async |r: &Released| {
        // The engine is released: pausing here does not block the range.
        cairn_store::failpoint!("turn::sdk::before_call");
        if refused_now(ctx, &inv.target.tenant) {
            return None;
        }
        Some(
            invoker::open_bidi(
                &ctx.clients.bidi,
                &deployment.endpoint,
                &inv.target.service,
                &inv.target.handler,
                deployment.wire_version,
                body,
                signing,
                Some(inv.target.tenant.as_str()),
                r,
            )
            .await,
        )
    })
    .await
    else {
        return Ok(sdk_timed_out(ctx, engine.partition(), &id, "BIDI_STREAM"));
    };
    let Some(session) = session else {
        return Ok(Step::Refused);
    };
    let mut session = session?;

    // Frames read but not yet processed — the rest of a batch when the turn
    // ended mid-batch (the SDK kept writing on the same stream); the next turn
    // on this stream reads them first.
    let mut queued: std::collections::VecDeque<cairn_protocol::Frame> = Default::default();
    for _ in 0..MAX_TURNS {
        loop {
            // Waiting for a frame is where a bidi turn stands the longest — the
            // SDK is running user code. Release the engine here, and **only**
            // here and in the `send` below: never between `step` and its commit,
            // because the engine's `pending_*_removals` cannot tolerate anyone
            // slipping in.
            //
            // The cap is recomputed from **every** frame, so in this mode it
            // truly means "no progress", not "the whole turn": a handler that
            // sends steadily may run as long as it likes.
            if queued.is_empty() {
                // The cancellation bell also wakes this wait: `next_frame` keeps
                // what it has read in `session`, so abandoning it midway loses no bytes.
                let Some(read) = awaiting_sdk_read(engine, ctx, async |r: &Released| {
                    tokio::select! {
                        f = session.next_frame(r) => Some(f),
                        () = live.rung() => None,
                    }
                })
                .await
                else {
                    return Ok(sdk_timed_out(ctx, engine.partition(), &id, "BIDI_STREAM"));
                };
                let Some(frame) = read else {
                    // Someone asked to cancel while the SDK runs user code: CANCEL
                    // goes into the journal and then down the open stream — the
                    // running closure sees it at once, without waiting for the
                    // turn to stop. Durable before sent, like every notification.
                    if live.take_cancel() {
                        let out = engine
                            .deliver_signal(&mut inv, live::cancel_notification(), Source::None)
                            .await?;
                        let durable = engine.sync().await?;
                        let Some(sent) =
                            awaiting_sdk(engine, ctx, &durable, async |r: &Released| {
                                session.send(&out, r).await
                            })
                            .await
                        else {
                            return Ok(sdk_timed_out(ctx, engine.partition(), &id, "BIDI_STREAM"));
                        };
                        sent?;
                    }
                    continue;
                };
                let frame = match frame {
                    Err(e) => match frame_too_large(&e) {
                        Some((len, max)) => {
                            return pause_oversized_frame(engine, &mut inv, len, max).await
                        }
                        None => return Err(e.into()),
                    },
                    Ok(f) => f,
                };
                let Some(frame) = frame else {
                    // The SDK closed the stream midway. The service protocol: a
                    // turn must end with exactly one of three messages, and
                    // "nothing at all" is not one of the three.
                    return Ok(settle_turn(&inv));
                };
                queued.push_back(frame);
                // ── Batched: every frame **that has arrived** at once — the SDK
                // writes `[Run, ProposeRunCompletion, Suspension]` in one go.
                match session.buffered() {
                    Ok(more) => queued.extend(more),
                    Err(e) => match frame_too_large(&e) {
                        Some((len, max)) => {
                            return pause_oversized_frame(engine, &mut inv, len, max).await
                        }
                        None => return Err(e.into()),
                    },
                }
            }
            // `step` frame by frame — the state machine still sees each one, in
            // order — then **one** durability wait for the whole batch, and only
            // then send. The rule does not change: nothing goes down to the SDK
            // before the commit holding it is durable; one flush tick just covers
            // several consecutive commits with nothing going out between them.
            let mut out = Vec::new();
            // A batch of only side-channel frames (keepalive, stream records)
            // writes nothing: no durability wait, no send (AP-4).
            let mut stepped = false;
            while let Some(frame) = queued.pop_front() {
                // Keepalive (v3): it has already done its job — the silence cap
                // is recomputed from every frame read. Does not enter the state machine.
                if frame.ty == cairn_protocol::MessageType::KEEPALIVE {
                    continue;
                }
                // Wire v4 side channel: no `step`, no `sync` — an SDK in the
                // middle of a `ctx.run` can send it too.
                if artifact_frame(engine, deployment, &inv, &frame).await? {
                    continue;
                }
                if side_frame(engine, ctx, deployment, &inv, &frame).await {
                    continue;
                }
                stepped = true;
                // ContinueAsNew (v3) ends the turn — and the invocation — here;
                // it does not enter the state machine.
                if frame.ty.kind() == MessageKind::Command {
                    crate::metering::steps(inv.target.tenant.as_str(), 1);
                    crate::stats::note_step();
                }
                if frame.ty == cairn_protocol::MessageType::CONTINUE_AS_NEW
                    && !engine.frame_oversized(&inv, &frame)
                {
                    let input = continue_input(&frame)?;
                    let durable = engine.sync().await?;
                    let Some(sent) = awaiting_sdk(engine, ctx, &durable, async |r: &Released| {
                        session.send(&out, r).await
                    })
                    .await
                    else {
                        return Ok(sdk_timed_out(ctx, engine.partition(), &id, "BIDI_STREAM"));
                    };
                    sent?;
                    return Ok(Step::Done(continue_as_new(engine, ctx, &inv, input).await?));
                }
                rt.frame(engine, ctx, &inv, &frame).await;
                let v6 = deployment.wire_version >= crate::discovery::WIRE_SEGMENTED;
                let stepped_out = match super::values::seal_frame(engine, &inv, &frame, v6).await? {
                    Some(s) => {
                        engine
                            .step_sealed_with(&mut inv, s.frame, s.fresh, s.old_bytes)
                            .await?
                    }
                    None => engine.step(&mut inv, Event::FromSdk(frame)).await?,
                };
                out.extend(stepped_out);
                if inv.outcome().is_some() {
                    break;
                }
            }
            if !stepped {
                continue;
            }
            // SDK ≤ v5 reads inline; v6 receives refs + URLs.
            super::values::prepare_down(
                engine,
                &ctx.clients.values,
                deployment.wire_version,
                &inv,
                &mut out,
                super::values::Down::Mid,
            )
            .await?;
            let durable = engine.sync().await?;
            // The handler is still running (the machine has no outcome): this
            // batch lets it take one more step. A tenant just suspended ⇒ stop at
            // the step boundary — the journal is durable up to here, the next run
            // replays from there —, like `REQUEST_RESPONSE`, where each `ctx.run`
            // ends a request.
            let running = inv.outcome().is_none();
            let Some(sent) = awaiting_sdk(engine, ctx, &durable, async |r: &Released| {
                if running && refused_now(ctx, &inv.target.tenant) {
                    return None;
                }
                Some(session.send(&out, r).await)
            })
            .await
            else {
                return Ok(sdk_timed_out(ctx, engine.partition(), &id, "BIDI_STREAM"));
            };
            let Some(sent) = sent else {
                return Ok(Step::Refused);
            };
            sent?;
            if inv.outcome().is_some() {
                break;
            }
        }

        match settle_turn(&inv) {
            Step::Done(o) => return Ok(Step::Done(o)),
            Step::Refused => unreachable!("settle_turn does not ask for the tenant state"),
            // Resolvable locally: rebuild the machine, **keep** the stream. The
            // SDK is waiting for a frame at the other end and will rerun the
            // handler itself when the notification arrives — it does not need,
            // and must not receive again, the replayed part.
            Step::Again => {
                crate::metrics::sdk::BIDI_RESUMES.bump(engine.partition().0);
                inv = engine.load(id, params_for(&id)).await?;
                arm(&mut inv, deployment, ctx);
                // `Start` + `known_entries` replayed entries are exactly what the
                // SDK already has in its local journal. Everything **after** that
                // part is notifications produced right in the Start step — the
                // replay task of a pending completion resolvable locally — and
                // the SDK does not have that yet, so it has to go down the wire.
                let replayed = 1 + inv.known_entries() as usize;
                let replay = engine.step(&mut inv, Event::Start).await?;
                let mut tail = replay[replayed.min(replay.len())..].to_vec();
                super::values::prepare_down(
                    engine,
                    &ctx.clients.values,
                    deployment.wire_version,
                    &inv,
                    &mut tail,
                    super::values::Down::Mid,
                )
                .await?;
                let durable = engine.sync().await?;
                let Some(sent) = awaiting_sdk(engine, ctx, &durable, async |r: &Released| {
                    // The engine is released. This tail makes the SDK rerun the
                    // handler in RAM — one call like opening a new stream: if the
                    // tenant is suspended, close the stream and send nothing.
                    cairn_store::failpoint!("turn::sdk::before_call");
                    if refused_now(ctx, &inv.target.tenant) {
                        return None;
                    }
                    Some(session.send(&tail, r).await)
                })
                .await
                else {
                    return Ok(sdk_timed_out(ctx, engine.partition(), &id, "BIDI_STREAM"));
                };
                let Some(sent) = sent else {
                    return Ok(Step::Refused);
                };
                sent?;
            }
        }
    }
    Err(ActorError::Invoke(invoker::InvokeError::Transport(
        format!("invocation exceeded {MAX_TURNS} turns on one stream without stopping"),
    )))
}
