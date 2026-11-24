// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Dispatching calls/workflows and delivering completions to the caller.

use {super::*, crate::metrics::interrange::KeyClass};

pub(super) struct Dispatch {
    pub(super) callee: InvocationId,
    /// `true` if this call created the invocation; `false` if it attached to an existing one.
    pub(super) created: bool,
    /// The target is in another partition — the call must go through [`Transport`] instead
    /// of being created locally.
    ///
    /// The id can still be computed right away (it is deterministic and already carries the
    /// target partition), but the caller is only given the `CallInvocationId` **after** the
    /// target partition confirms it has durably written the `StartCall` message — see the
    /// handling branch in [`drain_outbox`].
    pub(super) elsewhere: Option<cairn_store::PartitionId>,
    /// Key class of the call — the `key_class` label of `cairnflow_calls_total`.
    pub(super) class: KeyClass,
}

impl Dispatch {
    /// Count the call in `cairnflow_calls_total`.
    ///
    /// The caller counts **after** the id has been delivered to the caller, not at dispatch
    /// time: a row that stays (target broken, L0 throttle) and is dispatched again on a
    /// later turn is not counted twice.
    pub(super) fn count(&self) {
        use crate::metrics::interrange::{call, Placement};
        let placement = match self.elsewhere {
            None => Placement::Local,
            Some(_) => Placement::Remote,
        };
        call(placement, self.class);
    }
}

/// Key class of a call that is not a workflow's `run` — in the same order as
/// `partition_key_for_target`: scope, then object key, then idempotency key.
fn key_class(target: &Target, idempotency_key: Option<&str>) -> KeyClass {
    if target.scope().is_some() {
        KeyClass::Scoped
    } else if !target.object_key.is_empty() {
        KeyClass::Object
    } else if idempotency_key.is_some() {
        KeyClass::Idempotent
    } else {
        KeyClass::Unkeyed
    }
}

/// Partition key of the callee of `Call` row `c`, target `target`.
///
/// Keyed classes (scope, object key, idempotency key) follow today's rule — even if the row
/// carries `callee_pk`, which the engine never writes for those classes (IR-10). Unkeyed
/// class: the `callee_pk` of a v6 row if present (the prefix-grouping rule, fixed at write
/// time), otherwise `H(caller, completion)` — v5 rows use the old rule **forever**, so a
/// row written before the gate was enabled and drained again after it still yields the same
/// id (IR-9). A function of the row's content alone: it does not read the gate.
fn callee_key(c: &cairn_engine::OutboundCall, target: &Target) -> cairn_store::PartitionKey {
    cairn_store::partition_key_for_target(
        &target.tenant,
        target.scope(),
        &c.service,
        &target.object_key,
        c.idempotency_key.as_deref(),
    )
    .unwrap_or_else(|| {
        c.callee_pk
            .unwrap_or_else(|| cairn_store::partition_key_for_callee(&c.caller, c.id_completion))
    })
}

/// Error code for "this workflow has already run".
///
/// Extends the runtime's 57x range (570 journal mismatch, 571 protocol violation, 572
/// cancelled) rather than borrowing HTTP's 409: the one receiving it is *user code* waiting
/// on a `ctx.call`, and there the code must tell "the runtime refused" apart from a 409
/// returned by the target handler itself.
pub const WORKFLOW_ALREADY_INVOKED: u32 = 573;

/// The `run` call of a workflow — the id is a function of `(scope, service, key)`.
///
/// Three outcomes, and there is no fourth:
///
/// 1. **Not there yet** ⇒ create, exactly like any other call.
/// 2. **Already there, and the waiter recorded in entry 0 is this very caller** ⇒ our own
///    outbox row is being processed again (we died right between `create` and deleting the
///    row). Not a duplicate: silently take back the old id.
/// 3. **Already there, a different waiter** ⇒ a real duplicate. The caller receives
///    [`WORKFLOW_ALREADY_INVOKED`] and is **not** attached to the running turn.
///
/// The third is where workflows differ from idempotency, and on purpose: an idempotency key
/// attaches a duplicate call to the running invocation *because they carry the same input*
/// (the key is that promise). A second `run` call carries **different input** that would be
/// dropped — returning the first run's result as if it were the second's would be a lie.
pub(super) async fn dispatch_workflow_run(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    c: &cairn_engine::OutboundCall,
    target: &Target,
    limit_key: Option<&LimitKey>,
    callee: InvocationId,
) -> Result<Option<Dispatch>> {
    // Empty key ⇒ there is no workflow to run. Tell the caller right away instead of
    // creating a workflow with an empty name that every other `run` call would also hit.
    if target.object_key.is_empty() {
        if let Some(cid) = c.result_completion {
            engine
                .enqueue_reply(
                    &c.caller,
                    &c.caller,
                    cid,
                    ReplyResult::Failure {
                        code: 400,
                        message: format!(
                            "call to {}/{} has no workflow key — a workflow `run` \
                             needs a key",
                            c.service, c.handler
                        ),
                    },
                )
                .await?;
        }
        return Ok(Some(Dispatch {
            callee,
            created: false,
            elsewhere: None,
            class: KeyClass::Workflow,
        }));
    }

    if !engine.owns_id(&callee) {
        // Target in another partition: the id is already deterministic, so send it straight
        // away. The duplicate check runs in the target partition, where the workflow's state
        // lives.
        return Ok(Some(Dispatch {
            callee,
            created: false,
            elsewhere: Some(engine.cluster().partition_of_id(&callee)),
            class: KeyClass::Workflow,
        }));
    }

    // Already there (a real duplicate or our row being processed again), L0 throttle (like
    // `dispatch_call`: a new `run` in this range waits) and create: [`admit`].
    let reply_to = c.result_completion.map(|completion_id| ReplyTo {
        caller: c.caller,
        completion_id,
    });
    admitted(
        callee,
        KeyClass::Workflow,
        admit::admit(
            engine,
            ctx,
            admit::Source::Call { reply_to },
            admit::Request {
                id: callee,
                target,
                limit_key,
                idempotency_key: None,
                input: c.parameter.clone(),
                log_level: None,
                // Lineage of the call.
                tags: &c.lineage.tags,
                schedule: None,
                sender: Some(c.caller),
                root_id: c.lineage.root_id,
            },
        )
        .await?,
    )
}

/// The answer of [`admit`] for an outbox `Call` row whose target is in this range.
fn admitted(
    callee: InvocationId,
    class: KeyClass,
    a: admit::Admission<'_>,
) -> Result<Option<Dispatch>> {
    use admit::Admission as A;
    Ok(match a {
        // A quarantine outcome (`held`) changes nothing here: the invocation exists, the
        // scheduler blocks it (`gate_turn`).
        A::Created { .. } => Some(Dispatch {
            callee,
            created: true,
            elsewhere: None,
            class,
        }),
        A::Attached { id, .. } => Some(Dispatch {
            callee: id,
            created: false,
            elsewhere: None,
            class,
        }),
        // L0 throttle, or not decidable yet: the outbox row stays, a later drain turn does it.
        A::Shed | A::NotYet => None,
        A::Answered(_) | A::Rejected(_) | A::Refused(_) | A::Admitted => {
            unreachable!("a Call source never gets an ingress/restart reply")
        }
    })
}

/// Decide whether this call creates a new invocation or attaches to an existing one
/// (idempotency-key attach).
///
/// `Ok(None)` = **not decidable yet**: the service is not yet in the node's deployment
/// registry — and the `EXCLUSIVE` decision (queueing by object key) and the decision "is
/// this a workflow's `run`" (deterministic id) are both fixed at creation time, from the
/// manifest. Creating before the registry catches up means creating *without* queueing, or
/// creating with a non-deterministic id. The outbox row stays where it is; the registry
/// refresh loop brings the manifest within a few seconds and the next tick carries on.
pub(super) async fn dispatch_call(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    c: &cairn_engine::OutboundCall,
) -> Result<Option<Dispatch>> {
    // Scope and limit key travel with the outbox row (`CallCommandMessage` fields 7/8). A
    // corrupt value is **dropped** rather than failing the call: the outbox row may have
    // been written by another binary, and a call that loses its cap costs money, while a
    // call refused forever loses work.
    let scope = parse_ident(c.scope.as_deref(), "scope", &c.caller, Scope::new);
    let limit_key = parse_ident(
        c.limit_key.as_deref(),
        "limit key",
        &c.caller,
        LimitKey::parse,
    );
    // The call's tenant is the caller's tenant (the engine writes it into the outbox row).
    // Unlike scope above, a corrupt value must **not** be skipped: falling back to
    // `default` would create the invocation in someone else's tenant. The row stays.
    let Some(tenant) = tenant_of_wire(c.tenant.as_deref(), &c.caller) else {
        return Ok(None);
    };
    let target = Target {
        tenant,
        scope,
        service: c.service.clone(),
        handler: c.handler.clone(),
        object_key: c.object_key.clone(),
    };

    // Target not yet in this node's deployment registry ⇒ **not decidable yet**, even when
    // the target is in another partition.
    //
    // The cross-partition branch used to need no registry (id derived from the caller,
    // partition derived from the key). Workflows break that: the id of a `run` is a function
    // of `(scope, service, key)`, and only the registry can say whether this handler is
    // `run`. Guessing wrong once means sending a *non*-deterministic id, and the target
    // partition would create a second workflow for the same key — the one thing a workflow
    // promises never happens. Waiting a few seconds for the registry refresh loop is far
    // cheaper.
    if deployment_for(ctx.deployments, &target).is_none() {
        return Ok(None);
    }

    // ── Workflow: the identity of the run **is** `(scope, service, key)`.
    if kind_of(ctx.deployments, &target) == InvocationKind::WorkflowRun {
        // Empty key ⇒ no workflow; `dispatch_workflow_run` returns an error to the caller.
        // The id reported back to the caller then is the call's own deterministic id — like
        // an unkeyed call — not an id under a made-up pk (D-a).
        let callee = workflow_run_id(&target).unwrap_or_else(|| {
            // Empty key ⇒ the class below object key; the idempotency key is not part of
            // the identity of `run` (D-a).
            let key = callee_key(
                &cairn_engine::OutboundCall {
                    idempotency_key: None,
                    ..c.clone()
                },
                &target,
            );
            cairn_store::callee_invocation_id(key, &c.caller, c.id_completion)
        });
        return dispatch_workflow_run(engine, ctx, c, &target, limit_key.as_ref(), callee).await;
    }

    // Partition key of the **target**: by the call's keys if any (scope, object key,
    // idempotency key), otherwise by the rule fixed in the row ([`callee_key`]) —
    // deterministic in every case, so processing the same outbox row again yields the same
    // id in the same place. Independent of the number of partitions: if the key-range
    // table changes, the id stays the same.
    let key = callee_key(c, &target);

    // With an idempotency key the id is derived **from the key itself**: every caller with
    // the same key computes the same id, so the id returned to the caller (right below,
    // even when the target is in another partition) *is* the real id — no need for the
    // target partition to send it back. Without one, the id follows `(caller,
    // completion)`, enough for reprocessing the same outbox row not to spawn another
    // invocation.
    let callee = match c.idempotency_key.as_deref() {
        Some(idem) => cairn_store::idempotent_invocation_id(
            key,
            &target.tenant,
            target.scope(),
            &c.service,
            &c.handler,
            &c.object_key,
            idem,
        ),
        None => cairn_store::callee_invocation_id(key, &c.caller, c.id_completion),
    };
    let class = key_class(&target, c.idempotency_key.as_deref());
    if !engine.owns_key(key) {
        return Ok(Some(Dispatch {
            callee,
            created: false,
            elsewhere: Some(engine.cluster().partition_of_key(key)),
            class,
        }));
    }
    // L0 throttle, idempotency-key attach and create: [`admit`]. The throttle applies
    // before the lookup: a target in this range is new work for the very range that is
    // congested — leave the outbox row, a later drain turn does it. A target in another
    // range is throttled by that range on receipt (`drain_inbound`).
    let reply_to = c.result_completion.map(|completion_id| ReplyTo {
        caller: c.caller,
        completion_id,
    });
    admitted(
        callee,
        class,
        admit::admit(
            engine,
            ctx,
            admit::Source::Call { reply_to },
            admit::Request {
                id: callee,
                target: &target,
                limit_key: limit_key.as_ref(),
                idempotency_key: c.idempotency_key.as_deref(),
                input: c.parameter.clone(),
                log_level: None,
                // Lineage of the call.
                tags: &c.lineage.tags,
                schedule: None,
                sender: Some(c.caller),
                root_id: c.lineage.root_id,
            },
        )
        .await?,
    )
}

/// Deliver a completion to `caller` (waiting in this range) and delete the source outbox
/// row in the same commit.
///
/// The `caller`'s lock is held **and released** by the caller of this function — the
/// outbox drainer holds it across the per-target send too, shared by every row of the same
/// caller. `true` = the caller must hand `caller` to the scheduler **after** releasing the
/// lock: the task running it will ask for this very lock.
pub(super) async fn deliver_to_caller(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    _guard: &InvGuard<'_>,
    caller: InvocationId,
    completion: Completion,
    row: OutboxRef,
) -> Result<bool> {
    // The `caller`'s lock must be held **before** the state check, not only around the
    // write: if another turn slips in between "seen alive" and "deliver the completion" and
    // ends it, we would be delivering a notification to a terminal invocation — i.e.
    // rerunning something already done. The caller wins it with `try_invocation` before
    // any side effect of the row.
    //
    // The gap "callee already exists (locally, or the receiver has acked `StartCall`) but
    // the caller has not got the id yet": the `Call` row stays, a later drain turn
    // processes it again. Only `return` the error — under the engine we do not stop
    // (AGENTS §5).
    cairn_store::failpoint!(
        "turn::deliver_to_caller::call_id",
        matches!(completion.result, CompletionResult::CallInvocationId(_)),
        err
    );

    // The caller may already have ended (cancelled, or a `ctx.send` nobody waits on). Drop
    // the outbox row instead of letting it cycle forever.
    let mut arrival = arrived(
        engine,
        ctx,
        caller,
        lc::WakeKind::Completion,
        Some(completion.id),
    )
    .await?;
    if arrival.plan.wake == lc::WakeWrite::Discard {
        engine.drop_outbox(row).await?;
        return Ok(false);
    }

    // **Write, then hand over — do not run.** This is the other half of moving SDK turns
    // out of the drain loop: the drain loop used to run a whole SDK turn of the *caller*
    // right here, and the first half only took the *child's* turn out. Both halves have the
    // same shape, and this one is even easier to miss because it hides behind the word
    // `drive`.
    let inv = arrival.machine(engine, caller).await??;
    settle_into(
        engine,
        inv,
        completion,
        Source::Outbox { row },
        !arrival.stays,
    )
    .await?;
    Ok(arrival.plan.ram.handout.is_some())
}

/// Write a completion into the journal and cut the source row — **one commit, no SDK
/// turn**.
///
/// This is the *cheap* part of waking an invocation, split from the *expensive* part. It is
/// not a turn: no slot is asked for, no stage changes, no connection is opened.
/// `Event::Start` here is a **replay of the journal into the in-RAM machine**, not a
/// greeting sent down to the SDK — the messages it returns are dropped on purpose, because
/// nobody is listening.
///
/// The order "finish replaying, then load the completion" is mandatory, and the reason is
/// recorded at [`drive_one`]: a machine that has not replayed does not know which
/// completions are pending, so it treats the notification as being for an undeclared id —
/// a protocol violation, and the invocation dies for an entirely wrong reason.
///
/// **INV-5 lives in `Engine::deliver`**, not here: it stages the deletion of the source row
/// and rolls it into exactly the commit that writes the notification. Separating the two
/// would replay a completion that was already delivered — which exactly-once delivery
/// forbids.
pub(super) async fn settle_completion(
    engine: &mut Lease,
    id: InvocationId,
    completion: Completion,
    source: Source,
) -> Result<()> {
    let inv = replayed(engine, id).await??;
    settle_into(engine, inv, completion, source, true).await
}

/// `id`'s machine for one delivery by the drainer: the warm machine if the journal tail on
/// the store matches it, otherwise load the journal and replay (`Engine::settle_machine`).
///
/// An error of the **load** is returned separately (`Ok(Err)`): the inbound path treats it
/// as "no target" and drops the message; other errors (reading the tail, replay) are errors
/// of the drain turn like any other.
pub(super) async fn replayed(
    engine: &mut Lease,
    id: InvocationId,
) -> Result<std::result::Result<Invocation, cairn_engine::EngineError>> {
    use crate::metrics::settle;
    Ok(match engine.settle_machine(id, params_for(&id)).await? {
        Ok((inv, how)) => {
            settle::warm(how);
            Ok(inv)
        }
        Err(e) => {
            settle::warm(cairn_engine::WarmUse::Cold);
            Err(e)
        }
    })
}

/// Load `completion` into machine `inv` ([`replayed`]) — one commit together with deleting
/// the source row (`Engine::deliver`, INV-5) — then keep the machine for the next delivery.
///
/// Keep it **only** after `deliver` succeeds: an error (including an ambiguous commit —
/// AGENTS.md §1 rule 7) drops the machine with `inv`, and the cache holds no machine of
/// this id (taking is taking out).
///
/// `wake = false`: the core knows for sure the completion has not satisfied the wait tree
/// ⇒ write it while **keeping** `suspended`.
pub(super) async fn settle_into(
    engine: &mut Lease,
    mut inv: Invocation,
    completion: Completion,
    source: Source,
    wake: bool,
) -> Result<()> {
    use crate::metrics::settle;
    settle::bump(&settle::SETTLED);
    if wake {
        engine.deliver(&mut inv, completion, source).await?;
    } else {
        settle::bump(&settle::HELD);
        engine
            .deliver_staying_suspended(&mut inv, completion, source)
            .await?;
    }
    settle::evicted(engine.keep_warm(inv));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `key_class` label in exactly the pk derivation order of
    /// `partition_key_for_target`: scope beats object key, object key beats idempotency key.
    #[test]
    fn key_class_follows_derivation_order() {
        let target = |scope: Option<&str>, key: &str| Target {
            scope: scope.map(|s| Scope::new(s).expect("scope")),
            service: "S".into(),
            handler: "h".into(),
            object_key: key.into(),
            ..Target::default()
        };
        assert_eq!(
            key_class(&target(Some("t"), "k"), Some("i")),
            KeyClass::Scoped
        );
        assert_eq!(key_class(&target(None, "k"), Some("i")), KeyClass::Object);
        assert_eq!(
            key_class(&target(None, ""), Some("i")),
            KeyClass::Idempotent
        );
        assert_eq!(key_class(&target(None, ""), None), KeyClass::Unkeyed);
    }
}
