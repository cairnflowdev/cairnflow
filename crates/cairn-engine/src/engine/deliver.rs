// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Delivering completions/signals, inbound, dropping outbox/inbound/signal rows.

use super::*;

impl<S: RangeStore> Engine<S> {
    /// Feeds a completion into an invocation, cleaning up what produced it at
    /// the same time.
    ///
    /// The source row (a timer or outbox row) is deleted in the **same commit**
    /// as the notification it produced. Split apart, a death exactly between
    /// the two commits would make it replay forever, and every replay is a
    /// second notification for the same completion id — which the service
    /// protocol forbids.
    pub async fn deliver(
        &mut self,
        inv: &mut Invocation,
        completion: Completion,
        source: Source,
    ) -> Result<Vec<Message>> {
        self.deliver_inner(inv, completion, source, true).await
    }

    /// [`Self::deliver`] that **keeps** `suspended`: writes the notification and
    /// deletes the source row in one commit, without the `suspended → running`
    /// transition. Only when the lifecycle core knows for sure the completion
    /// has not satisfied the wait tree (`CompletionArrived` + `Need::Waiting`).
    pub async fn deliver_staying_suspended(
        &mut self,
        inv: &mut Invocation,
        completion: Completion,
        source: Source,
    ) -> Result<Vec<Message>> {
        self.deliver_inner(inv, completion, source, false).await
    }

    async fn deliver_inner(
        &mut self,
        inv: &mut Invocation,
        completion: Completion,
        source: Source,
        wake: bool,
    ) -> Result<Vec<Message>> {
        // A completion id receives exactly **one** notification. Two sources
        // can produce the same one, and that window is real:
        //
        // The state machine replays tasks for every completion still pending
        // after replay (`replay_task_for` — the recovery path after failover).
        // For `GetPromise`, if the promise got its result *while* the
        // invocation was suspended, that replay produces the notification
        // right away — while the outbox row that `complete_promise_external`
        // queued for the waiter is still there. Delivering that row too is the
        // second one, and the invocation dies with `571` for something it did
        // nothing wrong in.
        //
        // Blocked here rather than at each call site because this is the
        // **only** place every completion-feeding path goes through — the local
        // outbox, timers, signal rows, cross-partition messages. The check
        // follows the same rule as `deliver_completion_local`, which already
        // did exactly this for the cross-partition path alone.
        //
        // The source row must still be deleted: skipping it while keeping the
        // row would make it loop forever.
        if inv.settled_completions().contains(&completion.id) {
            self.clear_copied();
            self.discard_source(&source, &inv.id, completion.id).await?;
            return Ok(Vec::new());
        }
        self.stage_source_removal(&source, inv, completion.id);
        if wake {
            self.stage_wake(inv).await?;
        }
        let msgs = self.step(inv, Event::Completion(completion)).await;
        // The step may end without committing anything (the invocation is
        // closed); do not leave the deletion pending to stick to an unrelated
        // commit later.
        self.clear_staged_removals();
        self.clear_copied();
        msgs
    }

    /// A `suspended` invocation just got something that wakes it ⇒ the durable
    /// status becomes `running`, **in the same commit** that writes the
    /// notification.
    ///
    /// This is what lets [`Self::runnable_invocations_unbounded`] tell
    /// "suspended, nothing has woken it" apart from "suspended, the completion
    /// is already in the journal but the turn has not run yet" — the latter
    /// lives only in RAM (`runner.queue`) until the turn runs, and a leader
    /// change exactly then must be able to find it again.
    ///
    /// Only `suspended`: `paused` waits for a person (`/resume`), and `running`
    /// is already right.
    async fn stage_wake(&mut self, inv: &Invocation) -> Result<()> {
        if let Some(w) = super::meta::Woken::observed(inv.id, self.status(&inv.id).await?.as_ref())
        {
            self.pending_wakes.push(w);
        }
        Ok(())
    }

    /// Feeds a signal into an invocation — the counterpart of [`Self::deliver`]
    /// for [`Event::Signal`]. The source row is deleted in the same commit as
    /// the journal entry it produced; a duplicate (the machine has already seen
    /// this identity) leaves the commit with only the deletion.
    pub async fn deliver_signal(
        &mut self,
        inv: &mut Invocation,
        signal: cairn_protocol::pb::SignalNotificationMessage,
        source: Source,
    ) -> Result<Vec<Message>> {
        self.stage_source_removal(&source, inv, 0);
        self.stage_wake(inv).await?;
        // The audit row of an external signal travels with `step`'s first
        // batch — the batch that writes the signal's entry.
        self.audit_signal_armed = true;
        let msgs = self.step(inv, Event::Signal(signal)).await;
        self.audit_signal_armed = false;
        self.clear_staged_removals();
        msgs
    }

    fn stage_source_removal(&mut self, source: &Source, inv: &Invocation, completion_id: u32) {
        match source {
            Source::Timer { due_ms } => {
                self.pending_timer_removals
                    .push((*due_ms, inv.id, completion_id));
            }
            Source::Outbox { row } => self.pending_outbox_removals.push(*row),
            Source::Inbound { key } => self.pending_inbound_removals.push(*key),
            Source::SignalOutbox { key } => self.pending_signal_removals.push(*key),
            Source::None => {}
        }
    }

    /// Deletes the source row **right away**, with no consequences attached.
    ///
    /// The one-commit version of [`Self::stage_source_removal`], for two cases.
    /// Case one: what this row was going to deliver is already in the journal,
    /// so there is no other mutation to queue with it. Merging the two paths
    /// would force `deliver` to call `step` with an event that has nothing to
    /// do.
    ///
    /// Case two: the target invocation **has ended** — the source row is a
    /// late wake-up, and waking a terminal invocation is running it again. The
    /// call site checks the status and then drops the row through this path.
    pub async fn discard_source(
        &mut self,
        source: &Source,
        id: &InvocationId,
        completion_id: u32,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        match source {
            Source::Timer { due_ms } => {
                muts.remove_timer(*due_ms, id, completion_id);
            }
            Source::Outbox { row } => self.stage_outbox_removal(&mut muts, *row),
            Source::Inbound { key } => {
                muts.pop_inbound(key.owner, key.seq, key.kind, &key.id, key.completion_id);
            }
            Source::SignalOutbox { key } => {
                muts.pop_signal_outbox(key.seq, &key.sender, key.entry_index);
            }
            // No row produced this completion — nothing to delete, and an empty
            // commit is a free round of writes through the object store.
            Source::None => return Ok(()),
        };
        self.writer.commit(muts).await?;
        Ok(())
    }

    fn clear_staged_removals(&mut self) {
        self.pending_timer_removals.clear();
        self.pending_outbox_removals.clear();
        self.pending_inbound_removals.clear();
        self.pending_signal_removals.clear();
        self.pending_wakes.clear();
    }

    /// Accepts a message from another partition **and acks only after it is
    /// durable**.
    ///
    /// This is the handover of responsibility between two partitions, so it is
    /// where INV-5 applies a second time: the sender may forget its outbox row
    /// *only when* the message is durably here. An ack after a `put` that is
    /// not yet durable acks a message that will vanish — and the sender will
    /// never resend, because it thinks it is done.
    ///
    /// The row lives under the pk of the message's **target** — not of the id
    /// in the key, which is the sender in five of the six kinds.
    pub async fn accept_message(&mut self, msg: &InterPartition) -> Result<()> {
        self.accept_messages(std::slice::from_ref(msg)).await
    }

    /// Like [`Self::accept_message`] for a **batch** of messages to this range:
    /// every inbound row in **one** `Mutations`, **one** commit — all or
    /// nothing. A failure at any message before the commit writes no message
    /// of the batch; the commit is one WAL write so there is no "half batch"
    /// state in the store. The caller acks (`200`) only after this commit
    /// itself is durable.
    ///
    /// Deduplication is unchanged (IR-5): the row key is derived from the
    /// message content, so a message received earlier (or repeated within the
    /// batch) overwrites exactly its row, just like the per-message path; the
    /// index row of the old write becomes a stale row.
    pub async fn accept_messages(&mut self, msgs: &[InterPartition]) -> Result<()> {
        let mut muts = Mutations::new();
        for msg in msgs {
            // The "part of the batch staged, not committed" gap: `return` ⇒ the
            // receiver fails while writing the batch (test `receiver_failing_mid_batch_…`).
            cairn_store::failpoint!("engine::accept_messages::stage", err);
            let (kind, id, completion_id) = msg.inbound_key();
            let owner = partition_key_of(msg.destination_id());
            let seq = self.next_message_seq().await?;
            muts.push_inbound(owner, seq, kind, &id, completion_id, &msg.encode());
        }
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Unprocessed cross-partition messages, **from the cursor on**, in order of
    /// receipt. Stale index rows are cleaned up, not returned.
    pub async fn inbound_from(
        &mut self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(InboundKey, Bytes)>> {
        let got = self.writer.inbound_from(from_seq, limit).await?;
        self.forget_stale(got.stale).await?;
        Ok(got
            .rows
            .into_iter()
            .map(|(seq, owner, (kind, id, completion_id), payload)| {
                (
                    InboundKey {
                        seq,
                        owner,
                        kind,
                        id,
                        completion_id,
                    },
                    payload,
                )
            })
            .collect())
    }

    /// Deletes an inbound row without producing any consequence.
    ///
    /// Used when the message is redundant: the completion it carries was
    /// already delivered (the sender resent after a crash), or the target
    /// invocation no longer exists.
    pub async fn drop_inbound(&mut self, key: InboundKey) -> Result<()> {
        let mut muts = Mutations::new();
        muts.pop_inbound(key.owner, key.seq, key.kind, &key.id, key.completion_id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Deletes an outbox row without feeding any completion — used when the
    /// work is done but nobody waits for the result (`ctx.send`).
    pub async fn drop_outbox(&mut self, row: OutboxRef) -> Result<()> {
        let mut muts = Mutations::new();
        self.stage_outbox_removal(&mut muts, row);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Signals still waiting to be sent **from the cursor on**, in order of
    /// production. Stale index rows are cleaned up, not returned.
    pub async fn signals_from(
        &mut self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(SignalRef, Bytes)>> {
        let got = self.writer.signals_from(from_seq, limit).await?;
        self.forget_stale(got.stale).await?;
        Ok(got
            .rows
            .into_iter()
            .map(|(seq, (sender, entry_index), payload)| {
                (
                    SignalRef {
                        seq,
                        sender,
                        entry_index,
                    },
                    payload,
                )
            })
            .collect())
    }

    /// Deletes a signal row: sent (the other partition acked), or the target
    /// has finished for good / does not exist — a signal to the dead is a
    /// no-op.
    pub async fn drop_signal(&mut self, key: SignalRef) -> Result<()> {
        let mut muts = Mutations::new();
        muts.pop_signal_outbox(key.seq, &key.sender, key.entry_index);
        self.writer.commit(muts).await?;
        Ok(())
    }
}
