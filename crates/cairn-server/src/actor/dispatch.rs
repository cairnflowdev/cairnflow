// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Commands of the serve loop: [`on_command`] dispatches commands, one `on_*` function per
//! [`Command`].
//!
//! Split out of [`serve`] so the `select!` loop only coordinates. The `on_*` function of a
//! command that wins the engine is a **task body** — an `async fn` taking [`Shared`] by
//! value, `spawn_local`ed exactly like the old `async move` block; anything that must be
//! decided when the command is *received* (the L0 throttle) is still decided in
//! [`on_command`], before spawning.

use super::*;

/// A command just received while leader.
///
/// Returns `Some(step)` if the command ends the serve loop (`Resign`, `Quiesce`, `Retire`,
/// `Shutdown`); `quiesce_grace` is then the cap on waiting for work in flight.
pub(super) fn on_command(
    sh: &Shared,
    tasks: &mut JoinSet<()>,
    range: &RangeEntry,
    retiring: &AtomicBool,
    cmd: Command,
    quiesce_grace: &mut Duration,
) -> Option<Step> {
    // **Is it our range** (D23) — checked here, in the actor, not only in HTTP: between
    // HTTP looking up the table and the command arriving here, the range may have started
    // retiring. An engine's range is fixed for its whole life, so the comparison needs no
    // table.
    if cmd.target_key().is_some() {
        if retiring.load(Ordering::Acquire) || !cmd.within(range) {
            cmd.reject_wrong_range();
            return None;
        }
        // Every keyed command of the range goes through here — count the load. The key sample
        // is taken when the turn ends, once the turn's duration is known.
        sh.load.note_op();
    }
    match cmd {
        // Audit note per task of an inner command: the task's own `Shared` carries it to
        // `turn::Ctx`.
        Command::Audited { note, inner } => {
            let mut audited = sh.clone();
            audited.audit = Some(Rc::new(*note));
            return on_command(&audited, tasks, range, retiring, *inner, quiesce_grace);
        }
        Command::Invoke {
            id,
            target,
            limit_key,
            idempotency_key,
            input,
            log_level,
            tags,
            reply,
        } => on_new_command(
            sh,
            tasks,
            NewInvocation {
                id,
                target,
                limit_key,
                idempotency_key,
                input,
                log_level,
                tags,
            },
            Reply::Invoke(reply),
        ),
        Command::Submit {
            id,
            target,
            limit_key,
            idempotency_key,
            input,
            log_level,
            tags,
            reply,
        } => on_new_command(
            sh,
            tasks,
            NewInvocation {
                id,
                target,
                limit_key,
                idempotency_key,
                input,
                log_level,
                tags,
            },
            Reply::Submit(reply),
        ),
        Command::Tick { now_ms, reply } => {
            tasks.spawn_local(on_tick(sh.clone(), now_ms, reply));
        }
        Command::DeploymentsChanged { reply } => on_deployments_changed(sh, tasks, reply),
        // The three read commands below are the reason for this whole change: they only need
        // the engine for a few milliseconds, and now they no longer have to wait for an SDK
        // call turn to finish before getting their turn.
        Command::Status { id, reply } => {
            tasks.spawn_local(on_status(sh.clone(), id, reply));
        }
        Command::Settled { id, reply } => {
            tasks.spawn_local(on_settled(sh.clone(), id, reply));
        }
        Command::View { id, target, reply } => {
            tasks.spawn_local(on_view(sh.clone(), id, target, reply));
        }
        Command::LastFailure { id, reply } => {
            tasks.spawn_local(on_last_failure(sh.clone(), id, reply));
        }
        Command::Head { id, reply } => {
            tasks.spawn_local(on_head(sh.clone(), id, reply));
        }
        Command::Journal { id, reply } => {
            tasks.spawn_local(on_journal(sh.clone(), id, reply));
        }
        Command::JournalPage {
            id,
            from,
            max_entries,
            max_bytes,
            reply,
        } => {
            let sh = sh.clone();
            tasks.spawn_local(async move {
                let engine = Lease::acquire(&sh.engine).await;
                let r = engine
                    .journal_page(&id, from, max_entries, max_bytes)
                    .await
                    .map_err(ActorError::from);
                sh.finish(reply, r, engine).await;
            });
        }
        Command::OpenStream { id, reply } => {
            tasks.spawn_local(on_open_stream(sh.clone(), id, reply));
        }
        Command::ReadStream {
            id,
            name,
            from,
            max_bytes,
            reply,
        } => {
            tasks.spawn_local(on_read_stream(sh.clone(), id, name, from, max_bytes, reply));
        }
        Command::Artifacts { id, reply } => {
            tasks.spawn_local(on_artifacts(sh.clone(), id, reply));
        }
        Command::ValuesReferenced {
            tenant,
            pk,
            owner,
            reply,
        } => {
            tasks.spawn_local(on_values_referenced(sh.clone(), tenant, pk, owner, reply));
        }
        #[cfg(feature = "testkit")]
        Command::RawCommit { muts, reply, .. } => {
            tasks.spawn_local(on_raw_commit(sh.clone(), muts, reply));
        }
        Command::ArtifactsUnreferenced { owner, reply } => {
            tasks.spawn_local(on_artifacts_unreferenced(sh.clone(), owner, reply));
        }
        Command::PurgeTenantRows {
            grant,
            from,
            budget,
            check,
            reply,
        } => {
            tasks.spawn_local(on_purge_tenant_rows(
                sh.clone(),
                grant,
                from,
                budget,
                check,
                reply,
            ));
        }
        Command::CompletePromise {
            tenant,
            scope,
            service,
            object_key,
            name,
            result,
            reply,
        } => {
            let promise = PromiseAddr {
                tenant,
                scope,
                service,
                object_key,
                name,
            };
            tasks.spawn_local(on_complete_promise(sh.clone(), promise, result, reply));
        }
        Command::Restart {
            id,
            keep,
            deployment,
            patch,
            reply,
        } => {
            tasks.spawn_local(on_restart(sh.clone(), id, keep, deployment, patch, reply));
        }
        Command::Migrate { id, to, reply } => {
            tasks.spawn_local(on_migrate(sh.clone(), id, to, reply));
        }
        Command::Resume { id, reply } => {
            tasks.spawn_local(on_resume(sh.clone(), id, reply));
        }
        Command::PeekPromise {
            tenant,
            scope,
            service,
            object_key,
            name,
            reply,
        } => {
            let promise = PromiseAddr {
                tenant,
                scope,
                service,
                object_key,
                name,
            };
            tasks.spawn_local(on_peek_promise(sh.clone(), promise, reply));
        }
        Command::ExternalSignal {
            id,
            signal,
            value,
            reply,
        } => {
            tasks.spawn_local(on_external_signal(sh.clone(), id, signal, value, reply));
        }
        Command::PinnedCounts { reply } => {
            tasks.spawn_local(on_pinned_counts(sh.clone(), reply));
        }
        Command::Schedule { op, reply } => {
            tasks.spawn_local(on_schedule(sh.clone(), op, reply));
        }
        Command::ListInvocations {
            limit,
            filter,
            detailed,
            reply,
        } => {
            tasks.spawn_local(crate::lease::holding_as(
                "list",
                on_list_invocations(sh.clone(), limit, filter, detailed, reply),
            ));
        }
        Command::Cancel { id, reply } => {
            tasks.spawn_local(on_cancel(sh.clone(), id, reply));
        }
        Command::Purge { id, reply } => {
            tasks.spawn_local(on_purge(sh.clone(), id, reply));
        }
        Command::CancelSignal { id, reply } => {
            tasks.spawn_local(on_cancel_signal(sh.clone(), id, reply));
        }
        Command::Inbound { msg, reply } => {
            tasks.spawn_local(on_inbound(sh.clone(), msg, reply));
        }
        Command::InboundBatch { msgs, reply } => {
            tasks.spawn_local(on_inbound_batch(sh.clone(), msgs, reply));
        }
        Command::Resign { reply } => {
            // Leave through exactly the door of "lost leadership": seal + release live in one
            // single place, in `leadership_loop` — and the answer is given there, **after**
            // the seal.
            return Some(Step::LostLeadership(AfterSeal::reply(reply)));
        }
        Command::Quiesce {
            grace,
            holder,
            reply,
        } => {
            *quiesce_grace = grace;
            return Some(Step::Quiesce(AfterSeal {
                holder,
                ..AfterSeal::reply(reply)
            }));
        }
        Command::Retire { reply } => {
            *quiesce_grace = RETIRE_GRACE;
            return Some(Step::Quiesce(AfterSeal {
                retire: vec![reply],
                ..AfterSeal::default()
            }));
        }
        Command::FlushMemtable { fresh, reply } => {
            tasks.spawn_local(on_flush_memtable(sh.clone(), fresh, reply));
        }
        Command::WakeCompactor { reply } => {
            tasks.spawn_local(on_wake_compactor(sh.clone(), reply));
        }
        // The two commands below do **not** spawn a task and do **not** win the engine: they
        // only read/write the scheduler's RAM, and the scheduler lives right on this thread.
        // Going around through `Lease` would mean queueing behind an SDK call turn to read
        // three numbers.
        Command::Flow { reply } => {
            let _ = reply.send(Ok(sh.scheduler.snapshot()));
        }
        Command::SetRules { book, reply } => {
            sh.scheduler.set_rules(*book);
            let _ = reply.send(Ok(()));
        }
        Command::Shutdown { reply } => {
            return Some(Step::Stop(AfterSeal {
                shutdowns: vec![reply],
                stop: true,
                ..AfterSeal::default()
            }));
        }
    }
    None
}

/// The shared part of `Invoke`/`Submit`: the new invocation the client asks to create.
pub(super) struct NewInvocation {
    id: InvocationId,
    target: Target,
    limit_key: Option<LimitKey>,
    idempotency_key: Option<String>,
    input: Bytes,
    log_level: Option<String>,
    tags: Vec<(String, String)>,
}

/// The address of an outside promise for `CompletePromise`/`PeekPromise`.
pub(super) struct PromiseAddr {
    tenant: TenantId,
    scope: Option<Scope>,
    service: String,
    object_key: String,
    name: String,
}

/// The two ingress doors that create work: the same intake path ([`turn::admit`]), they
/// differ in how they answer the caller.
pub(super) enum Reply {
    /// `/invoke`: run the first turn, then return the result — or answer early if it has to
    /// queue. A turn that fails (not a fence) after creation ⇒ `Waiting`, the door waits
    /// through the reschedule.
    Invoke(oneshot::Sender<Result<InvokeOutcome>>),
    /// `/send`: answer as soon as it is durable, run the first turn behind the caller's back.
    Submit(oneshot::Sender<Result<InvokeOutcome>>),
}

/// An `Invoke`/`Submit` command just received: the L0 throttle is decided **right on
/// receipt**, before any write ([`turn::admit::Source::ingress`]), then the task body is
/// spawned.
fn on_new_command(sh: &Shared, tasks: &mut JoinSet<()>, new: NewInvocation, reply: Reply) {
    let source = turn::admit::Source::ingress(&sh.l0);
    // The id's wake-up duty (nack by default): created before the spawn, so a panic or
    // cancellation anywhere in the task marks the safety net.
    let duty = crate::drainer::WakeDuty::new(Rc::clone(&sh.rescue), new.id);
    tasks.spawn_local(crate::lease::holding_as(
        "turn",
        on_new(sh.clone(), source, new, reply, duty),
    ));
}

/// `Invoke`/`Submit`: admit through [`turn::admit`], then run according to the door.
async fn on_new(
    sh: Shared,
    source: turn::admit::Source<'static>,
    new: NewInvocation,
    reply: Reply,
    mut duty: crate::drainer::WakeDuty,
) {
    use turn::admit::Admission as A;
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let ctx = sh.ctx(&deployments);
    let id = new.id;
    let admitted = turn::admit::admit(
        &mut engine,
        &ctx,
        source,
        turn::admit::Request {
            id,
            target: &new.target,
            limit_key: new.limit_key.as_ref(),
            idempotency_key: new.idempotency_key.as_deref(),
            input: new.input,
            log_level: new.log_level.as_deref(),
            schedule: None,
            tags: &new.tags,
            sender: None,
            root_id: None,
        },
    )
    .await;
    // The wake-up duty starts at `Created` (not held): an invocation that existed before (or
    // was held back, or nothing created) is not something this turn has to remember.
    let (mut guard, tx) = match (admitted, reply) {
        (Ok(A::Created { guard, held: None }), reply) => (guard, reply),
        (r, Reply::Invoke(tx) | Reply::Submit(tx)) => {
            duty.ack();
            // Keep the lock (if any) until the answer is done, like every other path.
            let (_guard, r) = match r {
                Ok(A::Created {
                    guard,
                    held: Some(o),
                }) => (guard, Ok(o)),
                Ok(A::Answered(o)) => (None, Ok(o)),
                // Nothing created: nothing to promise, so nothing to force durable — answer
                // right away.
                Ok(A::Shed) => {
                    let _ = tx.send(Err(ActorError::Overloaded {
                        retry_after_ms: sh.l0.config().retry_after.as_millis() as u64,
                    }));
                    return;
                }
                // The tenant accepts no new work: nothing created yet.
                Ok(A::Refused(s)) => {
                    let _ = tx.send(Err(ActorError::TenantRefused(s)));
                    return;
                }
                Ok(
                    A::Created { held: None, .. }
                    | A::Attached { .. }
                    | A::NotYet
                    | A::Rejected(_)
                    | A::Admitted,
                ) => {
                    unreachable!("an ingress source returns only Created/Answered/Shed")
                }
                Err(e) => (None, Err(e)),
            };
            return sh.finish(tx, r, engine).await;
        }
    };
    match tx {
        Reply::Invoke(tx) => {
            // Answer early **if it has to queue**, then carry on behind the caller's back.
            // That is the old shape of `/invoke`, and it is the right shape: a call waiting
            // for an `EXCLUSIVE` lock must not hold an HTTP connection until its turn.
            //
            // `RefCell` because `on_queued` is an `FnOnce` run *inside* the turn, while
            // `reply` must be usable on both exit paths — and only one of the two actually
            // touches it.
            let reply = Rc::new(std::cell::RefCell::new(Some(tx)));
            let early = Rc::clone(&reply);
            let r = turn::finish_invocation_notifying(
                &mut engine,
                &ctx,
                id,
                &mut guard,
                move |o, _durable| {
                    if let Some(tx) = early.borrow_mut().take() {
                        let _ = tx.send(Ok(o));
                    }
                },
                &mut duty,
            )
            .await;
            let pending = reply.borrow_mut().take();
            match pending {
                // The turn failed **after** the invocation was created: the node → SDK leg
                // broke, the SDK returned a 5xx, a write failed mid-turn. The turn has left a
                // reschedule (or a watch reschedule row, the safety net — `drive`), so for
                // the client this is "received, not done yet" like a retryable handler
                // error: `waiting`, then the door waits through the reschedule up to the
                // `--invoke-wait-ms` cap.
                //
                // Why this is not a lie: `finish` forces durability and passes the read fence
                // **before** `waiting` goes out — the durable ticket covers admit's creating
                // commit, so the invocation with that id really exists; if forcing
                // durability fails the client gets the durability error as before. No extra
                // SDK call: the answer changes, not the turn.
                //
                // Losing write authority keeps the error: `NotLeader` means something to the
                // client (ask the new leader again), and an old leader has nothing to
                // promise.
                Some(tx) => {
                    let r = match r {
                        Err(e) if !e.is_fenced() => {
                            tracing::warn!(error = %e, "first turn of /invoke failed, retrying");
                            Ok(InvokeOutcome::Waiting)
                        }
                        r => r,
                    };
                    sh.finish(tx, r, engine).await
                }
                // "Queued" was already answered; from here there is nobody left to report an
                // error to. Log it and leave it to the reschedule-and-rerun path.
                None => {
                    if let Err(e) = r {
                        if e.is_fenced() {
                            sh.fenced.notify_one();
                        } else {
                            tracing::warn!(error = %e, "/invoke turn failed after replying");
                        }
                    }
                }
            }
        }
        Reply::Submit(tx) => {
            // Answer **right here** — the invocation is durable (`finish` itself forced it),
            // so the id given to the client can be looked up. The running part happens
            // behind the caller's back.
            sh.finish_held(tx, Ok(InvokeOutcome::Waiting), &mut engine)
                .await;
            // From here there is nobody left to report an error to. Log it and leave it to
            // the reschedule-and-rerun path — the same place every other background path
            // relies on.
            if let Err(e) =
                turn::finish_invocation(&mut engine, &ctx, id, &mut guard, &mut duty).await
            {
                if e.is_fenced() {
                    sh.fenced.notify_one();
                } else {
                    tracing::warn!(error = %e, "first turn of /send failed, retrying");
                }
            }
        }
    }
}

async fn on_tick(sh: Shared, now_ms: u64, reply: oneshot::Sender<Result<usize>>) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::tick(&mut engine, &sh.ctx(&deployments), now_ms).await;
    sh.finish(reply, r, engine).await;
}

/// The first deployment may be what every unfinished invocation is waiting for to run on.
/// **Answer right away, reconcile later**: the sender of this command is the node's
/// background loop (registry refresh) or `POST /deployments` — none of them may stand
/// behind a scan of the whole range. The reconciliation skips invocations someone is
/// working on, so it does not hand over a duplicate of a running turn; if one is already
/// waiting to run, the new command merges into it.
fn on_deployments_changed(
    sh: &Shared,
    tasks: &mut JoinSet<()>,
    reply: oneshot::Sender<Result<()>>,
) {
    let _ = reply.send(Ok(()));
    if !sh.reconcile_pending.replace(true) {
        let sh = sh.clone();
        tasks.spawn_local(crate::lease::holding_as("recover", async move {
            recover(&sh).await
        }));
    }
}

async fn on_status(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine.status(&id).await.map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_settled(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine.status(&id).await.map_err(ActorError::from);
    match r {
        Ok(Some(
            s @ (InvocationStatus::Completed(_)
            | InvocationStatus::Failed { .. }
            | InvocationStatus::Paused { .. }),
        )) => sh.finish(reply, Ok(Some(s)), engine).await,
        // "Not done yet": the asker drops this answer and asks again — nobody sees it, so
        // there is no need to prove it is fresh. It still goes through the durable ticket
        // and `alive` like every answer.
        Ok(_) => sh.finish_unexposed(reply, Ok(None), engine).await,
        Err(e) => sh.finish(reply, Err(e), engine).await,
    }
}

async fn on_view(
    sh: Shared,
    id: InvocationId,
    target: bool,
    reply: oneshot::Sender<Result<InvocationView>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = async {
        // The metadata row is one GET (target, `createdAt`, tags), without reading entry 0
        // or the pin row: this answer does not need the deployment.
        let (head, created_ms, tags) = match engine.meta(&id).await? {
            Some(m) => (cairn_engine::target_of_meta(&m), Some(m.created_ms), m.tags),
            None => {
                let (h, c) = engine.target_created(&id).await?;
                (h, c, Vec::new())
            }
        };
        let target = if target { head } else { None };
        let (status, modified_ms) = match engine.status_meta(&id).await? {
            Some((s, m)) => (Some(s), m),
            None => (None, None),
        };
        let times = cairn_engine::Times {
            created_ms,
            modified_ms,
        };
        let last_failure = match status {
            Some(
                InvocationStatus::Running
                | InvocationStatus::Suspended
                | InvocationStatus::Paused { .. },
            ) => engine.last_failure(&id).await.ok().flatten(),
            _ => None,
        };
        let next_attempt_ms = match status {
            Some(InvocationStatus::Running) => engine.pending_retry(&id).await.ok().flatten(),
            _ => None,
        };
        Ok::<_, cairn_engine::EngineError>(InvocationView {
            target,
            status,
            last_failure,
            next_attempt_ms,
            times,
            tags,
        })
    }
    .await
    .map_err(ActorError::from);
    // One fence for all three reads: a proof issued after the last read covers every
    // earlier read in this turn.
    sh.finish(reply, r, engine).await;
}

async fn on_open_stream(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<StreamAccess>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = async {
        let target = engine.head(&id).await?.map(|(t, _)| t);
        let status = engine.status(&id).await?;
        Ok::<_, cairn_engine::EngineError>(StreamAccess {
            hub: Arc::clone(&sh.streams),
            target,
            status,
        })
    }
    .await
    .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_read_stream(
    sh: Shared,
    id: InvocationId,
    name: String,
    from: u64,
    max_bytes: usize,
    reply: oneshot::Sender<Result<DurablePage>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = async {
        let chunks = engine
            .writer()
            .stream_chunks_unbounded(&id, &name, from, max_bytes)
            .await?;
        let exists = !chunks.is_empty()
            || engine
                .writer()
                .stream_tail_unbounded(&id, &name)
                .await?
                .is_some();
        let status = engine.status(&id).await?;
        Ok::<_, cairn_engine::EngineError>(DurablePage {
            chunks,
            exists,
            status,
        })
    }
    .await
    .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_artifacts(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<ArtifactLookup>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let cache = engine.artifact_cache().cloned();
    let r = engine
        .writer()
        .artifacts_of_unbounded(&id)
        .await
        .map(|rows| ArtifactLookup { rows, cache })
        .map_err(|e| ActorError::from(cairn_engine::EngineError::from(e)));
    sh.finish(reply, r, engine).await;
}

async fn on_values_referenced(
    sh: Shared,
    tenant: String,
    pk: cairn_store::PartitionKey,
    owner: cairn_store::value::Owner,
    reply: oneshot::Sender<Result<ReferencedValues>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine
        .values_referenced(&tenant, pk, &owner)
        .await
        .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

#[cfg(feature = "testkit")]
async fn on_raw_commit(
    sh: Shared,
    muts: cairn_store::Mutations,
    reply: oneshot::Sender<Result<()>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let r = engine
        .writer_mut()
        .commit(muts)
        .await
        .map(|_| ())
        .map_err(|e| ActorError::from(cairn_engine::EngineError::from(e)));
    sh.finish(reply, r, engine).await;
}

async fn on_artifacts_unreferenced(
    sh: Shared,
    owner: cairn_store::artifact::Owner,
    reply: oneshot::Sender<Result<UnreferencedArtifacts>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine
        .artifacts_unreferenced(&owner)
        .await
        .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_purge_tenant_rows(
    sh: Shared,
    grant: cairn_store::proof::TenantPurgeGrant,
    from: Option<cairn_engine::TenantPurgeCursor>,
    budget: usize,
    check: bool,
    reply: oneshot::Sender<Result<cairn_engine::TenantRowsStep>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let r = if check {
        engine.find_tenant_rows(&grant, from, budget).await
    } else {
        engine.purge_tenant_rows(&grant, from, budget).await
    }
    .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_last_failure(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<Option<cairn_store::keys::LastFailure>>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine.last_failure(&id).await.map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_head(sh: Shared, id: InvocationId, reply: oneshot::Sender<Result<Option<Target>>>) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine
        .head(&id)
        .await
        .map(|h| h.map(|(t, _)| t))
        .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_journal(sh: Shared, id: InvocationId, reply: oneshot::Sender<Result<Vec<JournalRow>>>) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine.journal_entries(&id).await.map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_complete_promise(
    sh: Shared,
    promise: PromiseAddr,
    result: cairn_engine::ReplyResult,
    reply: oneshot::Sender<Result<cairn_engine::PromiseOutcome>>,
) {
    // Spend a slot: waking the waiters means running their turns, and every turn is a
    // connection to the SDK.
    let mut engine = Lease::acquire(&sh.engine).await;
    let r = async {
        let outcome = engine
            .complete_promise_external(
                &promise.tenant,
                promise.scope.as_ref(),
                &promise.service,
                &promise.object_key,
                &promise.name,
                result,
            )
            .await?;
        // Wait for the drain to finish: the waiters already have a result in the outbox, and
        // making them wait for the background tick would turn a synchronous door into a
        // "done but not visible yet" door.
        //
        // **Wait, do not drain ourselves.** Draining here would create a second reader,
        // i.e. reopen exactly the class of bugs the single reader closes — see
        // [`crate::drainer`].
        let deployments = sh.registry.snapshot();
        turn::sync_outbox(&mut engine, &sh.ctx(&deployments)).await?;
        Ok(outcome)
    }
    .await;
    sh.finish(reply, r, engine).await;
}

async fn on_restart(
    sh: Shared,
    id: InvocationId,
    keep: u32,
    deployment: Option<String>,
    patch: Option<turn::admit::Patch>,
    reply: oneshot::Sender<Result<cairn_engine::RestartOutcome>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::restart(
        &mut engine,
        &sh.ctx(&deployments),
        id,
        keep,
        deployment,
        patch,
    )
    .await;
    sh.finish(reply, r, engine).await;
}

async fn on_migrate(
    sh: Shared,
    id: InvocationId,
    to: String,
    reply: oneshot::Sender<Result<bool>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::migrate(&mut engine, &sh.ctx(&deployments), id, &to).await;
    sh.finish(reply, r, engine).await;
}

async fn on_resume(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<cairn_engine::ResumeOutcome>>,
) {
    // The wake-up duty of the turn that runs on (nack by default): a panic or an error
    // before the reschedule point marks the safety net.
    let mut duty = crate::drainer::WakeDuty::new(Rc::clone(&sh.rescue), id);
    // Spend a slot: once unstalled it runs on, i.e. calls the SDK.
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::resume(&mut engine, &sh.ctx(&deployments), id, &mut duty).await;
    sh.finish(reply, r, engine).await;
}

async fn on_peek_promise(
    sh: Shared,
    promise: PromiseAddr,
    reply: oneshot::Sender<Result<Option<cairn_engine::ReplyResult>>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine
        .peek_promise_external(
            &promise.tenant,
            promise.scope.as_ref(),
            &promise.service,
            &promise.object_key,
            &promise.name,
        )
        .await
        .map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_external_signal(
    sh: Shared,
    id: InvocationId,
    signal: cairn_engine::SignalId,
    value: cairn_engine::CompletionPayload,
    reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
) {
    let mut duty = crate::drainer::WakeDuty::new(Rc::clone(&sh.rescue), id);
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::external_signal(
        &mut engine,
        &sh.ctx(&deployments),
        id,
        signal,
        value,
        &mut duty,
    )
    .await;
    sh.finish(reply, r, engine).await;
}

async fn on_schedule(
    sh: Shared,
    op: crate::schedules::ScheduleOp,
    reply: oneshot::Sender<Result<crate::schedules::ScheduleAnswer>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = crate::schedules::run(&mut engine, &sh.ctx(&deployments), op).await;
    sh.finish(reply, r, engine).await;
}

async fn on_pinned_counts(
    sh: Shared,
    reply: oneshot::Sender<Result<std::collections::BTreeMap<String, usize>>>,
) {
    let engine = Lease::acquire(&sh.engine).await;
    let r = engine.pinned_counts().await.map_err(ActorError::from);
    sh.finish(reply, r, engine).await;
}

async fn on_list_invocations(
    sh: Shared,
    limit: usize,
    filter: cairn_engine::ListFilter,
    detailed: bool,
    reply: oneshot::Sender<Result<ListAnswer>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    // The asker has gone (the per-range cap of `local_invocations`, the fan-out cap, the
    // client disconnecting) while the command waited for the engine: release right away, do
    // not scan. The command only reads and nobody receives the answer; scanning would hold
    // the engine for nobody, and once a scan takes longer than the cap, each re-ask piles one
    // more scan into the range's line — every other command of the range queues behind that
    // heap.
    if reply.is_closed() {
        return;
    }
    cairn_store::failpoint!("actor::list::scan");
    let r = async {
        // In chunks, releasing the engine between two chunks: filtering by state alone has
        // no scan cap, and the whole index of a large range in one engine hold means seconds
        // that every turn of the range has to wait.
        let mut scan = cairn_engine::ListScan::default();
        loop {
            cairn_store::failpoint!("actor::list::chunk");
            engine
                .list_invocations_step(limit, &filter, &mut scan, cairn_engine::LIST_TAG_CHUNK)
                .await?;
            if scan.is_done() {
                break;
            }
            engine.wait(tokio::task::yield_now()).await;
            // The asker left mid-way: stop, as before the first chunk.
            if reply.is_closed() {
                return Ok(None);
            }
        }
        let listing = scan.finish(limit, &filter);
        let mut out = Vec::with_capacity(listing.rows.len());
        for (id, status, target, times) in listing.rows {
            let (deployment, attempt) = if detailed {
                engine.invocation_extras(&id).await?
            } else {
                (None, 0)
            };
            out.push(InvocationListing {
                id,
                status,
                target,
                deployment,
                attempt,
                times,
            });
        }
        Ok::<_, cairn_engine::EngineError>(Some(ListAnswer {
            rows: out,
            untimed: listing.untimed,
            capped: listing.capped,
        }))
    }
    .await
    .map_err(ActorError::from);
    let r = match r {
        Ok(None) => return,
        Ok(Some(a)) => Ok(a),
        Err(e) => Err(e),
    };
    sh.finish(reply, r, engine).await;
}

async fn on_cancel(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<cairn_engine::CancelOutcome>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::cancel(&mut engine, &sh.ctx(&deployments), id).await;
    sh.finish(reply, r, engine).await;
}

async fn on_purge(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<cairn_engine::PurgeOutcome>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    // A manual purge also archives first — the same path as purge by deadline, only without
    // a deadline (day = today).
    let deployments = sh.registry.snapshot();
    let ctx = sh.ctx(&deployments);
    let r = async {
        use cairn_engine::PurgeOutcome;
        // The audit row goes with the purge commit — archiving ran before with the engine
        // released, but no other commit touches a finished id.
        let audit = ctx.audit_begin(&mut engine, id).await;
        let (out, outcome) = match engine.status(&id).await? {
            None => (PurgeOutcome::NotFound, "not_found"),
            Some(s) if !s.is_terminal() => (PurgeOutcome::StillRunning, "noop"),
            Some(_) => {
                turn::purge_archived(&mut engine, &id, None).await?;
                (PurgeOutcome::Purged, "ok")
            }
        };
        turn::audit_end(&mut engine, &ctx, audit, &id, outcome).await?;
        Ok(out)
    }
    .await;
    sh.finish(reply, r, engine).await;
}

async fn on_cancel_signal(
    sh: Shared,
    id: InvocationId,
    reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
) {
    // Cooperative cancel *calls the SDK*, so it spends a slot like a normal turn — otherwise
    // a burst of cancel commands could get past the cap.
    let mut duty = crate::drainer::WakeDuty::new(Rc::clone(&sh.rescue), id);
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let r = turn::cancel_signal(&mut engine, &sh.ctx(&deployments), id, &mut duty).await;
    sh.finish(reply, r, engine).await;
}

async fn on_inbound(sh: Shared, msg: Box<InterPartition>, reply: oneshot::Sender<Result<()>>) {
    on_inbound_batch(sh, vec![*msg], reply).await
}

/// One message or one batch: **one** commit for every message, ack once after that commit
/// is durable.
async fn on_inbound_batch(
    sh: Shared,
    msgs: Vec<InterPartition>,
    reply: oneshot::Sender<Result<()>>,
) {
    let mut engine = Lease::acquire(&sh.engine).await;
    let r = engine
        .accept_messages(&msgs)
        .await
        .map_err(ActorError::from);
    // **Nudge the drainer as soon as the commit receiving the messages is submitted**,
    // before the durability wait below. The drainer cannot run yet — the engine is here —,
    // it runs when `Lease::sync` below releases the engine: it reads the inbound rows at
    // `Memory` level, creates invocations or delivers completions, and the next turn commits
    // `Running` — with the WAL's linger, in the same object as the receiving commit.
    // Correct because the drainer only *reads and writes* on not-yet-durable messages, it
    // does not *say* anything to the outside: every effect after that demands a ticket
    // covering the receiving commit (LF-5), and if the tail is lost it is lost together.
    // `200` still only goes out after the receiving commit is durable (LF-4). This
    // interleaving existed before (`failpoints.rs::drained_inbound_on_a_lost_leader_runs_once`);
    // M4 explores it (`SOMETIMES_EARLY_DRAIN`).
    if r.is_ok() {
        sh.drainer.nudge();
    }
    // `Ok` says "the message is in the range's durable history" — written by this turn
    // (durable, not fenced) or by an earlier receipt (a `Ready` ticket: every write is
    // durable). Durable history only grows, the new owner replays it, so that statement
    // holds regardless of the clock: no read fence needed. The receiver is the sender's
    // drain loop.
    sh.finish_unexposed_then(reply, r, engine, || {
        // One sample per durable inbound commit: the number of messages it carries.
        crate::metrics::interrange::BATCH.observe(msgs.len() as u64);
    })
    .await;
    // Process **when the message arrives**, without waiting for the background tick. Still
    // nudge after `200`: the early nudge may land in the middle of a running drain turn
    // (`dirty` keeps it), this is the net.
    sh.drainer.nudge();
}

async fn on_wake_compactor(sh: Shared, reply: oneshot::Sender<Result<()>>) {
    // Only flips a signal in SlateDB's RAM: no I/O while holding the engine.
    let engine = Lease::acquire(&sh.engine).await;
    engine.writer().wake_compactor();
    drop(engine);
    let _ = reply.send(Ok(()));
}

async fn on_flush_memtable(sh: Shared, fresh: bool, reply: oneshot::Sender<Result<()>>) {
    // Hold the engine across the flush: a commit slipping in between is still durable
    // (WAL), just not in L0 yet — `seal` takes care of that part.
    //
    // Bounded: while it is held, every turn of the range stands still, and SlateDB retries
    // SST PUTs forever. Past the cap, release the engine and return a transient error — the
    // coordinator rolls back; the flush keeps running inside SlateDB, only nobody waits for
    // it.
    let engine = Lease::acquire(&sh.engine).await;
    let writer = engine.writer();
    let flush = if fresh {
        futures::future::Either::Left(writer.flush_memtable_fresh())
    } else {
        futures::future::Either::Right(writer.flush_memtable())
    };
    let r = match cairn_store::retry::deadline(
        &cairn_store::retry::ops::FLUSH_MEMTABLE,
        FLUSH_MEMTABLE_BOUND,
        flush,
    )
    .await
    {
        Ok(r) => r.map_err(ActorError::from),
        Err(e) => Err(ActorError::Engine(EngineError::Store(e.into()))),
    };
    drop(engine);
    let _ = reply.send(r);
}
