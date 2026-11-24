// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Interprets the `Effect`s of one [`Engine::step`] round: one function per `Task`, one for
//! `Effect::Finish`, plus the reads after the commit.
//!
//! Split out of `step`. Every function here only **gathers** into a
//! [`StepBatch`] — none of them commits; `step` commits exactly once per round,
//! so the contract "everything of a step goes into one commit" still lives in
//! one place.

use super::*;

/// What one round of [`Engine::step`] gathers before the commit.
pub(super) struct StepBatch {
    pub(super) muts: Mutations,
    pub(super) sends: Vec<Message>,
    /// Read tasks — done AFTER the commit, so the reads also see this step's own mutations.
    pub(super) reads: Vec<Task>,
    /// Completions whose result is known within this step (a promise, an
    /// attach to an already finished invocation...). Fed into the queue AFTER
    /// the commit, like `reads`: their notification must be in a commit after
    /// the mutation that produced it.
    pub(super) ready: Vec<Completion>,
    pub(super) finished: Option<ReplyResult>,
    /// The wait tree of the suspension this step writes as `suspended` — the
    /// engine remembers it after the commit.
    pub(super) suspended: Option<pb::SuspensionMessage>,
}

impl StepBatch {
    pub(super) fn new() -> Self {
        Self {
            muts: Mutations::new(),
            sends: Vec::new(),
            reads: Vec::new(),
            ready: Vec::new(),
            finished: None,
            suspended: None,
        }
    }
}

/// The owner of the state/promises of the object the invocation touches.
///
/// Only used in branches that touch the object — getting there means
/// `object` has a value (`step` has already filtered by D-a).
fn owner(object: Option<PartitionKey>, inv: &Invocation) -> Result<PartitionKey> {
    object.ok_or_else(|| no_owner(&inv.target))
}

impl<S: RangeStore> Engine<S> {
    /// Clears what earlier turns left waiting for the next commit.
    pub(super) fn stage_pending_removals(&mut self, muts: &mut Mutations) {
        // Fired timers are deleted in the very commit that writes their notification.
        for (due, id, cid) in self.pending_timer_removals.drain(..) {
            muts.remove_timer(due, &id, cid);
        }
        // Collect first, then remove from the map: `drain` borrows `self`, and
        // `stage_outbox_removal` needs `self` too.
        let outbox_done: Vec<OutboxRef> = self.pending_outbox_removals.drain(..).collect();
        for row in outbox_done {
            self.stage_outbox_removal(muts, row);
        }
        for key in self.pending_inbound_removals.drain(..) {
            muts.pop_inbound(key.owner, key.seq, key.kind, &key.id, key.completion_id);
        }
        for key in self.pending_signal_removals.drain(..) {
            muts.pop_signal_outbox(key.seq, &key.sender, key.entry_index);
        }
        // Placed **before** the step's effects: if this very step ends the
        // invocation, the ending status is written later, under the same key,
        // and wins.
        for w in self.pending_wakes.drain(..) {
            w.stage(muts);
        }
    }

    /// One `Effect` of the state machine.
    pub(super) async fn apply_effect(
        &mut self,
        inv: &mut Invocation,
        object: Option<PartitionKey>,
        fx: Effect,
        b: &mut StepBatch,
    ) -> Result<()> {
        match fx {
            Effect::Persist { entry_index, entry } => {
                let raw = entry.encode();
                b.muts.append_journal(&inv.id, entry_index, &raw);
                inv.journal_bytes += raw.len() as u64;
                // The holders `Invocation(id)` of every ref in the entry: the
                // journal holds the segments until `purge`.
                let new_bytes =
                    self.stage_entry_holders(&inv.target, &inv.id, &entry, &mut b.muts)?;
                // The entry's billing units: in the same batch as the entry —
                // counted exactly once, when the batch is durable. Segment bytes
                // just sealed for this entry count like bytes carried in it.
                b.muts.meter_units(
                    inv.target.tenant.as_str(),
                    crate::units::entry_units_with(&entry, new_bytes),
                );
                // Extends the chain in **the same commit** as the entry.
                if let Some((len, h)) = inv.chain {
                    let h = cairn_store::archive::chain_step(&h, entry_index, &raw);
                    inv.chain = Some((len + 1, h));
                    b.muts.set_chain(&inv.id, len + 1, &h);
                }
            }
            Effect::Send(m) => b.sends.push(m),
            Effect::Task(t) => self.apply_task(inv, object, t, b).await?,
            // Per the protocol, the runtime forwards the bytes as is, without
            // parsing — and **does not journal** them. It is not an entry: a
            // third party's extension has no right to shift the index of every
            // command after it.
            //
            // The engine only queues it on the outgoing stream; whether it has
            // anywhere to go is the transport's business. `BIDI_STREAM` does
            // (the stream is open), `REQUEST_RESPONSE` does not — and that
            // place says out loud what it just dropped, instead of dropping it
            // silently and letting the third party believe their extension is
            // working.
            Effect::ForwardCustom { ty, payload } => {
                b.sends.push(Message::Custom { ty, payload });
            }
            Effect::Finish(o) => self.apply_finish(inv, o, b).await?,
        }
        Ok(())
    }

    /// One `Task` of the state machine — one function per variant.
    async fn apply_task(
        &mut self,
        inv: &Invocation,
        object: Option<PartitionKey>,
        t: Task,
        b: &mut StepBatch,
    ) -> Result<()> {
        match t {
            Task::WriteState { key, value } => {
                // The "one of two rows" invariant. Remove the segmented row
                // and the old holders **first**: a reused ref has the same
                // holder key, so the write that comes after must win.
                let mut held = self.segmented_refs_of(&inv.target, &key).await?;
                // A reused ref may also be in the holder `Invocation(id)` of
                // the invocation itself: a v6 SDK remembers `refs[0..k]` after
                // a `set`, and the shell may have merged them out of the row.
                // Only scanned when there is a ref outside the current row.
                if let Some(cairn_protocol::value::Logical::Segmented { refs, .. }) = &value {
                    let outside = refs.iter().any(|r| {
                        !self.sealed_fresh.contains(&(r.sha256, r.gen))
                            && !held
                                .iter()
                                .any(|h| h.sha == r.sha256 && h.gen == r.gen && h.len == r.len)
                    });
                    if outside {
                        held.extend(self.invocation_held(&inv.target, &inv.id).await?);
                    }
                }
                self.stage_drop_segmented(&mut b.muts, &inv.target, &key)
                    .await?;
                match &value {
                    Some(cairn_protocol::value::Logical::Segmented { .. }) => {
                        let v = value.as_ref().expect("matched Some above");
                        self.stage_write_segmented(&mut b.muts, &inv.target, &key, v, &held)?;
                    }
                    Some(cairn_protocol::value::Logical::Inline(v)) => {
                        task_write_state(inv, object, &mut b.muts, &key, Some(v))?
                    }
                    None => task_write_state(inv, object, &mut b.muts, &key, None)?,
                }
            }
            Task::ScheduleTimer {
                completion_id,
                wake_up_time,
            } => {
                b.muts.add_timer(wake_up_time, &inv.id, completion_id, b"");
                self.next_due.offer(wake_up_time);
            }
            // An outgoing call must be durable before anyone sends it.
            Task::Invoke {
                result_completion_id,
                invocation_id_completion_id,
                service,
                handler,
                key,
                parameter,
                idempotency_key,
                invoke_time,
                scope,
                limit_key,
                spread,
                tags,
            } => {
                // Lineage: the callee's root is the caller's root — its
                // `root_id`, or the caller itself when it is the root. One
                // point read of the caller's metadata per outgoing call.
                let root_id = self
                    .meta(&inv.id)
                    .await?
                    .and_then(|m| m.root_id)
                    .unwrap_or(inv.id);
                let lineage = crate::lineage::Lineage {
                    root_id: Some(root_id),
                    tags: crate::lineage::Lineage::checked_tags(&tags),
                };
                let mut call = OutboundCall {
                    caller: inv.id,
                    id_completion: invocation_id_completion_id,
                    result_completion: result_completion_id,
                    service,
                    handler,
                    object_key: key,
                    parameter,
                    invoke_time,
                    idempotency_key,
                    // The SDK declares no scope ⇒ **inherit the
                    // caller's scope**. That is the right default:
                    // a workflow running in a tenant's scope means
                    // everything it calls belongs to that tenant
                    // too, and making users repeat `scoped(...)` on
                    // every call is making them remember for the
                    // system.
                    scope: scope.or_else(|| inv.target.scope.as_ref().map(|s| s.to_string())),
                    limit_key,
                    // The tenant is **always** the caller's, there
                    // is nothing for the SDK to choose: there are no
                    // cross-tenant calls.
                    tenant: (!inv.target.tenant.is_default())
                        .then(|| inv.target.tenant.to_string()),
                    callee_pk: None,
                    lineage,
                };
                call.callee_pk = Self::callee_pk_for(inv, &call, spread);
                let entry = OutboxEntry::Call(call);
                self.task_invoke(
                    inv,
                    &mut b.muts,
                    invoke_time,
                    invocation_id_completion_id,
                    entry,
                )
                .await?;
            }
            // A signal must be durable before anyone carries it off —
            // the same contract as `Invoke`, only a different table
            // (see `keyspace::Table::SignalOutbox` for the key).
            Task::SendSignal {
                target_invocation_id,
                signal,
                result,
                entry_index,
            } => {
                self.task_send_signal(
                    inv,
                    &mut b.muts,
                    &target_invocation_id,
                    &signal,
                    result.as_ref(),
                    entry_index,
                )
                .await?
            }
            // Reads need not write anything — but they must run AFTER
            // the commit, so they also see this step's own mutations.
            Task::ReadState { .. } | Task::ReadStateKeys { .. } => b.reads.push(t),
            Task::ClearAllState => self.task_clear_all_state(inv, object, &mut b.muts).await?,
            Task::PromiseGet { completion_id, key } => {
                self.task_promise_get(inv, object, b, completion_id, &key)
                    .await?
            }
            Task::PromisePeek { completion_id, key } => {
                self.task_promise_peek(inv, object, b, completion_id, &key)
                    .await?
            }
            Task::PromiseComplete {
                completion_id,
                key,
                result,
            } => {
                self.task_promise_complete(inv, object, b, completion_id, &key, &result)
                    .await?
            }
            Task::Attach {
                completion_id,
                target_invocation_id,
            } => {
                self.task_attach(inv, b, completion_id, &target_invocation_id)
                    .await?
            }
            Task::PeekOutput {
                completion_id,
                target_invocation_id,
            } => {
                self.task_peek_output(inv, b, completion_id, &target_invocation_id)
                    .await?
            }
        }
        Ok(())
    }

    /// The placement rule of a call's callee (IR-9): `Some(pk)` = the
    /// prefix-grouping rule (a v6 row), `None` = the existing rule, computed
    /// at drain time.
    ///
    /// 1. **Only the unkeyed tier** (IR-10): a scope (including an inherited
    ///    one), object key or idempotency key ⇒ `None`. The
    ///    state/promises/idempotency of keyed tiers live under `owner_pk` by
    ///    the existing rule; changing the rule would mean moving data. A
    ///    workflow's `run` always has a key (no key is rejected), so it is here
    ///    too — the engine need not know the handler kind.
    /// 2. **The rule belongs to the caller**: the `cairnflow-call-prefix` mark
    ///    in its entry 0 (`Invocation::calls`), written in the commit that
    ///    created it according to the range's gate at the time. A pure
    ///    function of the call's content and one durable, immutable thing —
    ///    it does **not** read the gate now, the outbox row, or the D-O2 map.
    ///    The state machine replays `Task::Invoke` on every replay; every
    ///    rewrite therefore yields exactly the same row, even when the old row
    ///    is gone (deleted, or the map off after a failed commit) and the gate
    ///    has changed midway.
    /// 3. **`placement: "spread"`** (field 13 of the command, kept byte for
    ///    byte in the journal): a caller carrying the `cairnflow-call-placement`
    ///    mark ⇒ `None` (the old rule, spread). No mark ⇒ ignore `spread` —
    ///    exactly the rule a gate-step-4 binary applies (it does not know the
    ///    field), so a journal that binary has processed does not yield a
    ///    different row here. Keyed ⇒ ignored (point 1 decides first): an
    ///    inherited scope is invisible to the SDK, a rejection would be a
    ///    surprise, and the keyed tier does not group anyway.
    fn callee_pk_for(inv: &Invocation, call: &OutboundCall, spread: bool) -> Option<PartitionKey> {
        let keyed =
            call.scope.is_some() || !call.object_key.is_empty() || call.idempotency_key.is_some();
        let spread = spread && inv.calls.placement;
        (!keyed && !spread && inv.calls.prefix).then(|| {
            cairn_store::partition_key_for_callee_prefix(
                partition_key_of(&inv.id),
                &inv.id,
                call.id_completion,
            )
        })
    }

    /// `Task::Invoke`: the outbox row (or the `delayed` row if it is not time
    /// yet) of an outgoing call.
    async fn task_invoke(
        &mut self,
        inv: &Invocation,
        muts: &mut Mutations,
        invoke_time: u64,
        invocation_id_completion_id: u32,
        entry: OutboxEntry,
    ) -> Result<()> {
        // Deterministic key ⇒ replaying the task overwrites, it does
        // not spawn another call.
        if invoke_time > 0 {
            // `OneWayCall.invoke_time`: not yet time ⇒ not in the
            // outbox yet — it sits in the `delayed` table, and the
            // tick loop moves it over when due. A replay after the
            // row has moved only rewrites a harmless delayed row:
            // when due it becomes an outbox row with the same key
            // as the one already processed — an overwrite or an
            // idempotent dispatch, no extra call spawned.
            muts.add_delayed(
                invoke_time,
                &inv.id,
                invocation_id_completion_id,
                &entry.encode(),
            );
            self.next_due.offer(invoke_time);
        } else {
            self.stage_outbox_for(
                inv.id,
                muts,
                partition_key_of(&inv.id),
                (inv.id, invocation_id_completion_id),
                &entry.encode(),
            )
            .await?;
        }
        Ok(())
    }

    /// `Task::SendSignal`: the signal-outbox row.
    async fn task_send_signal(
        &mut self,
        inv: &Invocation,
        muts: &mut Mutations,
        target_invocation_id: &str,
        signal: &pb::send_signal_command_message::SignalId,
        result: Option<&pb::send_signal_command_message::Result>,
        entry_index: u32,
    ) -> Result<()> {
        match unhex(target_invocation_id) {
            Some(target) => {
                let msg = InterPartition::Signal(DeliverSignal {
                    target,
                    sender: inv.id,
                    entry_index,
                    signal: signal_id_of(signal),
                    result: signal_payload_of(result),
                    // The target range only delivers to a target in the same tenant.
                    tenant: crate::wire_tenant(Some(&inv.target.tenant)),
                });
                let seq = self.next_message_seq().await?;
                muts.push_signal_outbox(seq, &inv.id, entry_index, &msg.encode());
            }
            // `SendSignal` is not completable, so there is no channel
            // to report an error to the SDK; a broken id is reported
            // loudly and dropped — keeping it would never route anyway.
            None => tracing::warn!(
                target_invocation_id,
                invocation = %hex(&inv.id),
                "SendSignal target id is not 48 hex characters — dropped"
            ),
        }
        Ok(())
    }

    /// `Task::ClearAllState`: clears all state — lists the existing keys and
    /// deletes each one in THE SAME commit as the journal entry. Reading before
    /// the commit is enough — each SDK command is a separate step, so no
    /// `SetState` can slip in within the same step.
    async fn task_clear_all_state(
        &mut self,
        inv: &Invocation,
        object: Option<PartitionKey>,
        muts: &mut Mutations,
    ) -> Result<()> {
        let owner = owner(object, inv)?;
        for (user_key, _) in self
            .writer
            .state_entries_unbounded(
                owner,
                &inv.target.tenant,
                inv.target.scope(),
                &inv.target.service,
                &inv.target.object_key,
            )
            .await?
        {
            muts.clear_state(
                owner,
                &inv.target.tenant,
                inv.target.scope(),
                &inv.target.service,
                &inv.target.object_key,
                &user_key,
            );
        }
        // Segmented keys: the rows and the holders.
        for user_key in self.segmented_keys(&inv.target).await? {
            self.stage_drop_segmented(muts, &inv.target, &user_key)
                .await?;
        }
        Ok(())
    }

    /// `Task::PromiseGet`.
    async fn task_promise_get(
        &mut self,
        inv: &Invocation,
        object: Option<PartitionKey>,
        b: &mut StepBatch,
        completion_id: u32,
        key: &str,
    ) -> Result<()> {
        let owner = owner(object, inv)?;
        match self.read_promise(owner, &inv.target, key).await? {
            Some(rec) => b.ready.push(Completion {
                id: completion_id,
                result: reply_completion(rec.result),
            }),
            // No result yet: add our name to the wait list,
            // in the same commit as the command's journal entry.
            None => {
                b.muts.add_promise_waiter(
                    owner,
                    &inv.target.tenant,
                    inv.target.scope(),
                    &inv.target.service,
                    &inv.target.object_key,
                    key,
                    &inv.id,
                    completion_id,
                );
            }
        }
        Ok(())
    }

    /// `Task::PromisePeek`.
    async fn task_promise_peek(
        &mut self,
        inv: &Invocation,
        object: Option<PartitionKey>,
        b: &mut StepBatch,
        completion_id: u32,
        key: &str,
    ) -> Result<()> {
        let result = match self
            .read_promise(owner(object, inv)?, &inv.target, key)
            .await?
        {
            Some(rec) => reply_completion(rec.result),
            None => CompletionResult::Void,
        };
        b.ready.push(Completion {
            id: completion_id,
            result,
        });
        Ok(())
    }

    /// `Task::PromiseComplete`.
    async fn task_promise_complete(
        &mut self,
        inv: &Invocation,
        object: Option<PartitionKey>,
        b: &mut StepBatch,
        completion_id: u32,
        key: &str,
        result: &pb::complete_promise_command_message::Completion,
    ) -> Result<()> {
        let owner = owner(object, inv)?;
        match self.read_promise(owner, &inv.target, key).await? {
            None => {
                let reply = promise_reply(result);
                let rec = PromiseRecord {
                    result: reply.clone(),
                    completer: (inv.id, completion_id),
                };
                b.muts.set_promise(
                    owner,
                    &inv.target.tenant,
                    inv.target.scope(),
                    &inv.target.service,
                    &inv.target.object_key,
                    key,
                    &rec.encode(),
                );
                // Waiters receive the result through the outbox, and are
                // removed from the wait list in the same commit —
                // exactly the attachments pattern of `Effect::Finish`.
                for (waiter, cid) in self
                    .writer
                    .promise_waiters_unbounded(
                        owner,
                        &inv.target.tenant,
                        inv.target.scope(),
                        &inv.target.service,
                        &inv.target.object_key,
                        key,
                    )
                    .await?
                {
                    let entry = OutboxEntry::Reply(OutboundReply {
                        caller: waiter,
                        completion_id: cid,
                        result: reply.clone(),
                    });
                    // Owner = the promise's object — the producer of
                    // the reply, not the waiter.
                    self.stage_outbox_for(
                        inv.id,
                        &mut b.muts,
                        owner,
                        (waiter, cid),
                        &entry.encode(),
                    )
                    .await?;
                    b.muts.remove_promise_waiter(
                        owner,
                        &inv.target.tenant,
                        inv.target.scope(),
                        &inv.target.service,
                        &inv.target.object_key,
                        key,
                        &waiter,
                        cid,
                    );
                }
                b.ready.push(Completion {
                    id: completion_id,
                    result: CompletionResult::Void,
                });
            }
            // The completer is OURSELVES on an earlier run —
            // the task replay is idempotent, not a
            // conflict.
            Some(rec) if rec.completer == (inv.id, completion_id) => b.ready.push(Completion {
                id: completion_id,
                result: CompletionResult::Void,
            }),
            Some(_) => b.ready.push(Completion {
                id: completion_id,
                result: CompletionResult::CallFailure(pb::Failure {
                    code: 409,
                    message: format!("promise {key:?} already has a result"),
                    metadata: vec![],
                }),
            }),
        }
        Ok(())
    }

    /// `Task::Attach`.
    async fn task_attach(
        &mut self,
        inv: &Invocation,
        b: &mut StepBatch,
        completion_id: u32,
        target_invocation_id: &str,
    ) -> Result<()> {
        match unhex(target_invocation_id) {
            None => b.ready.push(bad_id(completion_id, target_invocation_id)),
            Some(target) if !self.owns_id(&target) => {
                let entry = OutboxEntry::Attach(OutboundWatch {
                    caller: inv.id,
                    completion_id,
                    target,
                });
                self.stage_outbox_for(
                    inv.id,
                    &mut b.muts,
                    partition_key_of(&inv.id),
                    (inv.id, completion_id),
                    &entry.encode(),
                )
                .await?;
            }
            // A target in another tenant ⇒ treated as nonexistent.
            Some(target) => match self.visible_status(&inv.target.tenant, &target).await? {
                None => b.ready.push(not_found(completion_id, &target)),
                // A segmented output is not delivered under the engine: a
                // `Reply` outbox row lets the shell assemble or COPY it.
                Some(InvocationStatus::Completed(crate::InvocationOutput::Segmented(o))) => {
                    self.stage_segmented_reply(inv, &mut b.muts, completion_id, o)
                        .await?
                }
                Some(s) if s.is_terminal() => b.ready.push(Completion {
                    id: completion_id,
                    result: terminal_completion(s),
                }),
                // Still running: add our name to the wait list — the result
                // comes back later through the existing attachments path.
                Some(_) => {
                    b.muts.add_attachment(&target, &inv.id, completion_id);
                }
            },
        }
        Ok(())
    }

    /// `Task::PeekOutput`.
    async fn task_peek_output(
        &mut self,
        inv: &Invocation,
        b: &mut StepBatch,
        completion_id: u32,
        target_invocation_id: &str,
    ) -> Result<()> {
        match unhex(target_invocation_id) {
            None => b.ready.push(bad_id(completion_id, target_invocation_id)),
            Some(target) if !self.owns_id(&target) => {
                let entry = OutboxEntry::PeekOutput(OutboundWatch {
                    caller: inv.id,
                    completion_id,
                    target,
                });
                self.stage_outbox_for(
                    inv.id,
                    &mut b.muts,
                    partition_key_of(&inv.id),
                    (inv.id, completion_id),
                    &entry.encode(),
                )
                .await?;
            }
            Some(target) => {
                // A target in another tenant ⇒ treated as nonexistent.
                let result = match self.visible_status(&inv.target.tenant, &target).await? {
                    None => {
                        b.ready.push(not_found(completion_id, &target));
                        return Ok(());
                    }
                    // Like `task_attach`: the shell delivers segmented outputs.
                    Some(InvocationStatus::Completed(crate::InvocationOutput::Segmented(o))) => {
                        return self
                            .stage_segmented_reply(inv, &mut b.muts, completion_id, o)
                            .await;
                    }
                    Some(s) if s.is_terminal() => terminal_completion(s),
                    Some(_) => CompletionResult::Void,
                };
                b.ready.push(Completion {
                    id: completion_id,
                    result,
                });
            }
        }
        Ok(())
    }

    /// `Effect::Finish`: the invocation's ending (or suspended) status,
    /// together with everything that must be durable in the same commit.
    async fn apply_finish(
        &mut self,
        inv: &mut Invocation,
        o: Outcome,
        b: &mut StepBatch,
    ) -> Result<()> {
        // **Terminal is terminal.**
        //
        // This is a **safety net**, not a fix for a path that is
        // broken: `turn::cancel` holds the invocation lock
        // throughout writing the terminal status, so through the
        // server's paths today no turn can commit after it.
        // The net lives here because the invariant belongs to
        // *this layer*, while the lock is a convention of the
        // layer above — and `Engine::cancel` is a public API. A
        // future call site that forgets the lock would break in a
        // way with no symptom:
        //
        // 1. `cancel` has returned `{"cancelled": true}` to the
        //    operator, then the invocation shows up `completed`.
        // 2. `cancel` has queued a **failure** for the caller
        //    through the outbox. That row is keyed by `(waiter,
        //    completion)`, so a successful `Reply` **overwrites**
        //    it: the caller gets the result of a cancelled
        //    invocation and carries on as if nothing happened.
        //
        // Cost: one point read every time an invocation ends.
        // There is no cheaper check — the status snapshotted when
        // the turn loaded is exactly what can no longer be trusted
        // here.
        //
        // Reading then writing within the same `step` is atomic:
        // `&mut self` is held from here until `commit`, and every
        // other status-write path must ask for exactly that.
        let stored = self.status_at(&inv.id).await?;
        if let Some((stored, _)) = stored.clone().filter(|(s, _)| s.is_terminal()) {
            // `outcome` must still be set — otherwise the
            // `BIDI_STREAM` turn loop spins forever waiting for an
            // outcome that never comes. But set it to exactly
            // **what is durable**, not what this turn just
            // computed: two answers to one question is the bug
            // that `Output`-carrying-failure already taught once.
            let settled = match stored {
                InvocationStatus::Failed { code, message } => Outcome::Failed {
                    code,
                    message,
                    action: cairn_runtime::FailureAction::Fail,
                },
                // Two invocations both writing `Completed` cannot
                // happen (the state machine closes after `End`),
                // but if it did, what is durable is still the
                // answer — not what was just computed.
                other => Outcome::Completed(pb::OutputCommandMessage {
                    name: String::new(),
                    result: match other {
                        InvocationStatus::Completed(v) => {
                            Some(pb::output_command_message::Result::Value(v.to_value()))
                        }
                        _ => None,
                    },
                }),
            };
            inv.outcome = inv.outcome.take().or(Some(settled));
            return Ok(());
        }
        let mut status = status_of(&o, inv);
        // Suspending while what the SDK waits for **is already**
        // in the journal (written in this very turn — a `ctx.run`
        // result, a lazy `ctx.get`) means the turn will rerun
        // right away (the server's `settle_turn`): the durable
        // status is `running`, not `suspended`. Otherwise a leader
        // change exactly between the two turns would leave it
        // lying still forever — `recover` only reruns `running`,
        // and nothing else would wake it.
        if let Outcome::Suspended(s) = &o {
            let signals = cairn_runtime::SettledSignals {
                signals: inv.settled_signals(),
            };
            if cairn_runtime::suspension_satisfied(s, &inv.settled_completions(), signals) {
                status = InvocationStatus::Running;
            }
        }
        // The scheduled purge goes in THE SAME commit as the
        // ending status: split apart, a death between two commits
        // would leave an invocation that is never purged — the
        // bucket grows with history, with no error to see.
        if status.is_terminal() {
            // Ended: no delivery uses its warm machine anymore.
            self.warm.forget(&inv.id);
            // The handler's retention (manifest) overrides the tenant's,
            // the tenant's overrides the node's.
            if let Some(ttl) = inv
                .retention_ms
                .or_else(|| self.retention_for(&inv.target.tenant))
            {
                b.muts.add_purge(self.purge_due(ttl), &inv.id);
            }
            // Ended ⇒ no turn runs on its deployment anymore:
            // unpin it in the same commit.
            if let Some(d) = &inv.deployment {
                b.muts.unpin_deployment(&inv.id, d);
            }
            // Leaves the queue **in the same commit**: previously
            // `dequeue` was a separate commit after `End`, and
            // answering the caller had to wait one more WAL flush
            // for a bookkeeping row. Folding it in here is also
            // tighter: there is no "done but still queued" window.
            if let Some(slot) = self.vqueue_of(&inv.id).await? {
                b.muts
                    .pop_vqueue(&slot.queue, slot.stage, slot.seq, &inv.id);
            }
            // A pending scheduled retry (this turn's watchdog
            // row) is meaningless for a finished invocation: drop
            // it in the same commit, so that ending the turn does
            // not need another commit — and the caller another
            // durable wait — just to remove it.
            b.muts.drop_retry(&inv.id);
        }
        // The old class is what was just read above (a turn can run on a
        // `paused` one when a completion arrives): exactly what made the old
        // index key.
        let was = match &stored {
            Some((s, at)) => super::meta::Was::of(s, *at),
            None => super::meta::Was::In(cairn_store::query_index::Class::Active, None),
        };
        self.stage_status(&mut b.muts, &inv.id, was, &status)
            .await?;
        if let (Outcome::Suspended(s), InvocationStatus::Suspended) = (&o, &status) {
            b.suspended = Some(s.clone());
        }
        // If this invocation was spawned by `ctx.call`, the result
        // must get back to the caller — and must be durable **in
        // the same commit** as the ending status, otherwise a death
        // exactly between two commits would leave the caller
        // hanging forever.
        if let (Some(reply_to), Some(result)) = (&inv.reply_to, reply_result(&o, inv)) {
            let entry = OutboxEntry::Reply(OutboundReply {
                caller: reply_to.caller,
                completion_id: reply_to.completion_id,
                result,
            });
            // Keyed by **the waiter**, not by this invocation.
            //
            // An invocation may have to return its result to many
            // waiters (a duplicate call attaches to the running
            // invocation). Keyed by target, two waiters that both
            // use completion id 2 would overwrite each other and one
            // would lose its result forever.
            // Owner = this invocation — the producer of the reply,
            // not the waiter.
            self.stage_outbox_for(
                inv.id,
                &mut b.muts,
                partition_key_of(&inv.id),
                (reply_to.caller, reply_to.completion_id),
                &entry.encode(),
            )
            .await?;
        }
        b.finished = reply_result(&o, inv);
        inv.outcome = inv.outcome.take().or(Some(o));
        Ok(())
    }

    /// A `Reply` outbox row for `inv` itself (completion `completion_id`)
    /// carrying the segmented output `o` of an invocation already finished in
    /// this range: the shell assembles or COPYs and then delivers it —
    /// attach/get-output never deliver a segmented output straight from the
    /// engine.
    async fn stage_segmented_reply(
        &mut self,
        inv: &Invocation,
        muts: &mut Mutations,
        completion_id: u32,
        o: crate::SegmentedOutput,
    ) -> Result<()> {
        let entry = OutboxEntry::Reply(OutboundReply {
            caller: inv.id,
            completion_id,
            result: ReplyResult::Segmented(o),
        });
        self.stage_outbox_for(
            inv.id,
            muts,
            partition_key_of(&inv.id),
            (inv.id, completion_id),
            &entry.encode(),
        )
        .await
    }

    /// Attached waiters. Read before the commit so that everything goes into
    /// **one** commit: split apart, a death exactly between two commits would
    /// leave the attached waiters hanging forever on an invocation that
    /// finished long ago.
    pub(super) async fn stage_attachment_replies(
        &mut self,
        inv: &Invocation,
        result: ReplyResult,
        muts: &mut Mutations,
    ) -> Result<()> {
        for (caller, completion_id) in self.writer.attachments_unbounded(&inv.id).await? {
            let entry = OutboxEntry::Reply(OutboundReply {
                caller,
                completion_id,
                result: result.clone(),
            });
            // Owner = this invocation — the producer of the reply, the target
            // of the attachment — not the waiter.
            self.stage_outbox_for(
                inv.id,
                muts,
                partition_key_of(&inv.id),
                (caller, completion_id),
                &entry.encode(),
            )
            .await?;
            muts.remove_attachment(&inv.id, &caller, completion_id);
        }
        Ok(())
    }

    /// Answers one read task, **after** the round's commit.
    pub(super) async fn answer_read(
        &self,
        inv: &Invocation,
        object: Option<PartitionKey>,
        task: Task,
    ) -> Result<Option<Completion>> {
        match task {
            Task::ReadState { completion_id, key } => {
                let value = self
                    .writer
                    .state(
                        owner(object, inv)?,
                        &inv.target.tenant,
                        inv.target.scope(),
                        &inv.target.service,
                        &inv.target.object_key,
                        &key,
                    )
                    .await?;
                // Only a segmented row: return ref + tail — the engine does not
                // assemble; the shell assembles before sending down to an SDK ≤ v5.
                let value = match value {
                    Some(v) => Some(cairn_protocol::value::Logical::Inline(v)),
                    None => self.segmented_value(&inv.target, &key).await?,
                };
                Ok(Some(Completion {
                    id: completion_id,
                    result: CompletionResult::State(value),
                }))
            }
            Task::ReadStateKeys { completion_id } => {
                let keys = self
                    .writer
                    .state_entries_unbounded(
                        owner(object, inv)?,
                        &inv.target.tenant,
                        inv.target.scope(),
                        &inv.target.service,
                        &inv.target.object_key,
                    )
                    .await?
                    .into_iter()
                    .map(|(k, _)| k)
                    .collect::<Vec<_>>();
                let mut keys = keys;
                let seg = self.segmented_keys(&inv.target).await?;
                if !seg.is_empty() {
                    keys.extend(seg);
                    keys.sort();
                    keys.dedup();
                }
                Ok(Some(Completion {
                    id: completion_id,
                    result: CompletionResult::StateKeys(keys),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// `Task::WriteState`: sets or clears one state key of the object.
fn task_write_state(
    inv: &Invocation,
    object: Option<PartitionKey>,
    muts: &mut Mutations,
    key: &Bytes,
    value: Option<&Bytes>,
) -> Result<()> {
    match value {
        Some(v) => {
            muts.set_state(
                owner(object, inv)?,
                &inv.target.tenant,
                inv.target.scope(),
                &inv.target.service,
                &inv.target.object_key,
                key,
                v,
            );
        }
        None => {
            muts.clear_state(
                owner(object, inv)?,
                &inv.target.tenant,
                inv.target.scope(),
                &inv.target.service,
                &inv.target.object_key,
                key,
            );
        }
    }
    Ok(())
}
