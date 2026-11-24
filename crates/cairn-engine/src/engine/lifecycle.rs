// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Lifecycle: purge, archive, workflow cleanup, migrate, pause, restart, resume,
//! cancel, `fail_with`.

use super::*;

impl<S: RangeStore> Engine<S> {
    /// Purges an ended invocation: journal, status, idempotency row, and the
    /// scheduled-purge row — **one commit**.
    ///
    /// Idempotent: calling it on an already purged invocation only deletes the
    /// remaining scheduled row (if any). `scheduled_due` is the scheduled row
    /// to delete along with it, `None` for a manual purge through the admin API
    /// — the scheduled row (if any) then fires later, finds nothing left, and
    /// cleans itself up.
    ///
    /// The `idempotency` row is only deleted when it **still points at this
    /// very invocation** — a defence for old data: if the mapping has been
    /// overwritten to point at a new invocation (the key reused after an
    /// earlier purge), deleting it would break someone else's dedupe.
    ///
    /// **A workflow purges more than that**: the workflow's state and promises
    /// go in the same commit too. That is a necessary consequence of the `run`
    /// id being a function of `(scope, service, key)` — after the purge the
    /// same key **can run again**, and a new run that sees the old run's state
    /// is a workflow that starts in the middle.
    pub async fn purge(&mut self, id: &InvocationId, scheduled_due: Option<u64>) -> Result<()> {
        self.warm.forget(id);
        let mut muts = Mutations::new();
        self.take_audit_for(id, &mut muts);
        // A manual purge (`None`) still deletes the scheduled row: the deadline is read from the entity row.
        let due = match scheduled_due {
            Some(due) => Some(due),
            None => self
                .writer
                .get(&cairn_store::keys::purge(id))
                .await?
                .and_then(|v| cairn_store::keys::decode_due(&v).map(|(due, _)| due)),
        };
        if let Some(due) = due {
            muts.remove_purge(due, id);
        }
        // Query-index rows are computed from the metadata + the current
        // position, and deleted in the same commit.
        let meta = self.meta(id).await?;
        if let (Some(meta), Some((status, at))) = (&meta, self.status_at(id).await?) {
            let p = cairn_store::query_index::Placement::new(status.class(), at.unwrap_or(0), meta);
            muts.query_event_with(self.query_indexes, id, meta, Some(p), None);
        }
        muts.clear_invocation_status(id);
        muts.clear_invocation_meta(id);

        let rows = self.writer.journal_unbounded(id).await?;
        let first = rows
            .first()
            .and_then(|(_, raw)| JournalEntry::decode(raw.clone()).ok());
        // Purging an unfinished invocation (manual purge) must also unpin it.
        if let Some(first) = &first {
            if let Some(d) = self.pinned_of(id, first).await? {
                muts.unpin_deployment(id, &d);
            }
        }
        if let Some(first) = &first {
            if let (Some(target), Some(idem)) = (target_of(first), idempotency_of(first)) {
                let holder = self
                    .writer
                    .idempotency_target(
                        &target.tenant,
                        target.scope(),
                        &target.service,
                        &target.handler,
                        &target.object_key,
                        &idem,
                    )
                    .await?;
                if holder.as_ref() == Some(id) {
                    muts.remove_idempotency(
                        &target.tenant,
                        target.scope(),
                        &target.service,
                        &target.handler,
                        &target.object_key,
                        &idem,
                    );
                }
            }
            if let (InvocationKind::WorkflowRun, Some(target)) = (kind_of(first), target_of(first))
            {
                self.stage_workflow_cleanup(&mut muts, &target).await?;
            }
        }
        for (index, _) in &rows {
            muts.purge_journal_entry(id, *index);
        }
        // Durable streams (`$log`, `$trace`): go with the journal, not into
        // the archive.
        for (name, first) in self.writer.stream_chunk_keys_unbounded(id).await? {
            muts.purge_stream_chunk(id, &name, first);
        }
        // Artifact rows: the objects stay where they are, the range's artifact
        // sweep sees that `(id, epoch)` has no row left and holds them in
        // place in the trash (AP-2).
        // Artifact byte counter: rows that counted (decodable, with metadata)
        // stop counting in this same commit.
        for (sha, row) in self.writer.artifacts_of_unbounded(id).await? {
            muts.purge_artifact(id, &sha);
            if let (Some(meta), Some(row)) = (&meta, row) {
                muts.artifact_bytes(cairn_store::artifact_bytes::delta(
                    id,
                    &sha,
                    &meta.tenant,
                    -super::artifact_bytes::signed(row.size),
                ));
            }
        }
        // The holders `Invocation(id)` of segments: the journal goes, the
        // holders go, in the same commit.
        let target = first.as_ref().and_then(target_of);
        self.stage_release_invocation(&mut muts, id, target.as_ref())
            .await?;
        muts.clear_chain(id);
        muts.clear_last_failure(id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// The snapshot to archive before purging: the raw journal, the final
    /// status, the chain head. `None` = no journal (already purged, or never
    /// existed). `purged_at_ms` is set by the call site.
    pub async fn archive_snapshot(
        &self,
        id: &InvocationId,
    ) -> Result<Option<cairn_store::archive::ArchiveRecord>> {
        let rows = self.writer.journal_unbounded(id).await?;
        let Some((_, raw0)) = rows.first() else {
            return Ok(None);
        };
        let first = JournalEntry::decode(raw0.clone())?;
        let target = target_of(&first).ok_or(EngineError::MissingTarget)?;
        let status = self
            .writer
            .get(&cairn_store::keys::invocation_status(id))
            .await?
            .unwrap_or_default();
        let chain = self
            .writer
            .get(&cairn_store::keys::chain(id))
            .await?
            .and_then(|raw| cairn_store::keys::decode_chain(&raw));
        Ok(Some(cairn_store::archive::ArchiveRecord {
            id: *id,
            tenant: target.tenant,
            purged_at_ms: 0,
            status,
            journal: rows,
            chain,
        }))
    }

    /// The engine's archive store, if the node has archiving enabled.
    pub fn archive_store(&self) -> Option<&cairn_store::archive::ArchiveStore> {
        self.archive.as_ref()
    }

    /// A workflow's state + promises, staged into [`Self::purge`]'s commit.
    ///
    /// Promise waiters are deleted too, and **nobody** is woken: they are
    /// waiting for a workflow that was just purged, so there is no result to
    /// deliver. They still have a way out — their invocation waits on that
    /// completion until it is cancelled or hits its turn cap. What is not
    /// allowed is leaving orphan wait rows pointing at a workflow that no
    /// longer exists, because the *next* run of the same key would wrongly
    /// wake them with its own result.
    async fn stage_workflow_cleanup(&self, muts: &mut Mutations, target: &Target) -> Result<()> {
        // A workflow always has a key; no owner means no state to clean up either.
        let Some(owner) = target.object_owner() else {
            return Ok(());
        };
        for (user_key, _) in self
            .writer
            .state_entries_unbounded(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            )
            .await?
        {
            muts.clear_state(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                &user_key,
            );
        }
        // Segmented keys: the rows and the holders.
        for user_key in self.segmented_keys(target).await? {
            self.stage_drop_segmented(muts, target, &user_key).await?;
        }
        for name in self
            .writer
            .promise_names_unbounded(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            )
            .await?
        {
            muts.clear_promise(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                &name,
            );
        }
        for (name, waiter, cid) in self
            .writer
            .promise_waiters_of_object_unbounded(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            )
            .await?
        {
            muts.remove_promise_waiter(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                &name,
                &waiter,
                cid,
            );
        }
        Ok(())
    }

    /// Moves an **unfinished** invocation to deployment `to` — deliberately,
    /// after a trial replay on `to` has matched. The journal does not change
    /// (append-only); the `Pins` row is what changes, and from the next turn on
    /// every turn calls `to`. `false` = no such invocation, or already ended.
    pub async fn migrate(&mut self, id: &InvocationId, to: &str) -> Result<bool> {
        match self.status(id).await? {
            None => return Ok(false),
            Some(s) if s.is_terminal() => return Ok(false),
            Some(_) => {}
        }
        let Some(raw) = self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        else {
            return Ok(false);
        };
        let first = JournalEntry::decode(raw)?;
        let mut muts = Mutations::new();
        if let Some(old) = self.pinned_of(id, &first).await? {
            muts.unpin_deployment(id, &old);
        }
        muts.pin_deployment(id, to);
        self.take_audit_for(id, &mut muts);
        self.writer.commit(muts).await?;
        Ok(true)
    }

    /// Puts an unfinished invocation into `Paused` — giving up `PAUSE`-style
    /// after `maxAttempts`. The journal stays intact; `resume` gives one more
    /// round.
    pub async fn pause_with(
        &mut self,
        id: &InvocationId,
        code: u32,
        message: &str,
    ) -> Result<bool> {
        self.pause_in(id, code, message, None, StageWrite::Keep, RowWrite::Keep)
            .await
    }

    /// [`Self::pause_with`] together with the stage and retry row of a
    /// lifecycle-core Plan, in **one** commit (a preflight `Exhausted` used to
    /// be three commits — `pause_with`, `end_turn`, `cancel_retry`).
    pub async fn pause_in(
        &mut self,
        id: &InvocationId,
        code: u32,
        message: &str,
        slot: Option<&Option<VQueueSlot>>,
        stage: StageWrite,
        row: RowWrite,
    ) -> Result<bool> {
        let was = match self.status_at(id).await? {
            None => return Ok(false),
            Some((s, _)) if s.is_terminal() => return Ok(false),
            Some((s, at)) => super::meta::Was::of(&s, at),
        };
        let mut muts = Mutations::new();
        self.stage_status(
            &mut muts,
            id,
            was,
            &InvocationStatus::Paused {
                code,
                message: message.to_string(),
            },
        )
        .await?;
        self.stage_lifecycle(&mut muts, id, slot, stage, row)
            .await?;
        self.writer.commit(muts).await?;
        #[cfg(feature = "lifecycle-check")]
        self.assert_lifecycle(id).await?;
        Ok(true)
    }

    /// `restart-as-new`: a **new** invocation `new` whose journal is the first
    /// `keep` entries of `old` (entry 0 rebuilt — the input may be replaced —
    /// and carrying `cairnflow-restarted-from`), pinned to `deployment`
    /// (default: the one `old` is pinned to), in queue `queue`.
    ///
    /// If `old` has not ended, it **ends in the same commit**: `Completed` with
    /// output `{"continuedAsNew": "<new>"}`, returning no result to anyone —
    /// the caller (`reply_to` is in the copied entry 0), the attached waiters
    /// and the idempotency key all move to `new`, so they get the new copy's
    /// result. If `old` has already ended it stays as it is; `new` is just a
    /// rerun.
    ///
    /// `new` must have the same partition key as `old` (the call site issues
    /// it): the journal is copied within the same Db.
    pub async fn restart(
        &mut self,
        old: &InvocationId,
        new: &InvocationId,
        keep: u32,
        input: Option<Bytes>,
        deployment: Option<&str>,
        queue: Option<(&QueueName, u64)>,
    ) -> Result<RestartOutcome> {
        self.restart_in(
            old,
            new,
            keep,
            input,
            deployment,
            queue,
            RowWrite::Clear { old_due: None },
        )
        .await
    }

    /// The check of [`Self::restart_in`], writing nothing: `Some` is the
    /// rejection outcome `restart_in` would return (`NotFound`, `BadPrefix`,
    /// `WorkflowRun`). The shell asks before doing costly irreversible work for
    /// the restart — copying artifacts to the new id (AP-10) only when the new
    /// id will really exist.
    pub async fn restart_refusal(
        &self,
        old: &InvocationId,
        keep: u32,
    ) -> Result<Option<RestartOutcome>> {
        if self.status(old).await?.is_none() {
            return Ok(Some(RestartOutcome::NotFound));
        }
        let rows = self.writer.journal_unbounded(old).await?;
        let len = rows.len() as u32;
        if keep == 0 || keep > len {
            return Ok(Some(RestartOutcome::BadPrefix { len }));
        }
        let first = JournalEntry::decode(rows[0].1.clone())?;
        if kind_of(&first).is_workflow_run() {
            return Ok(Some(RestartOutcome::WorkflowRun));
        }
        Ok(None)
    }

    /// [`Self::restart`] with `old`'s retry row following the lifecycle core's
    /// Plan (`row`), in the same commit that ends `old` (the old copy's retry
    /// row is dropped in the same commit — previously it lived until its next
    /// fire). Only applies when `old` has not ended.
    #[allow(clippy::too_many_arguments)]
    pub async fn restart_in(
        &mut self,
        old: &InvocationId,
        new: &InvocationId,
        keep: u32,
        input: Option<Bytes>,
        deployment: Option<&str>,
        queue: Option<(&QueueName, u64)>,
        row: RowWrite,
    ) -> Result<RestartOutcome> {
        use cairn_protocol::{pb, Flags, Frame, MessageType};
        // Writing `new`'s entry 0 and ending `old`: no warm machine of either
        // id is still valid.
        self.warm.forget(old);
        self.warm.forget(new);
        let Some((status, old_at)) = self.status_at(old).await? else {
            return Ok(RestartOutcome::NotFound);
        };
        let rows = self.writer.journal_unbounded(old).await?;
        let len = rows.len() as u32;
        if keep == 0 || keep > len {
            return Ok(RestartOutcome::BadPrefix { len });
        }
        let first = JournalEntry::decode(rows[0].1.clone())?;
        if kind_of(&first).is_workflow_run() {
            return Ok(RestartOutcome::WorkflowRun);
        }
        let target = target_of(&first).ok_or(EngineError::MissingTarget)?;
        let pinned_old = self.pinned_of(old, &first).await?;
        let pin_new = deployment
            .map(str::to_string)
            .or_else(|| pinned_old.clone());

        // Rebuilt entry 0: old headers + provenance, old or new input.
        let frame = Frame::new(first.ty, Flags::NONE, first.payload.clone());
        let Ok(Message::Input(mut msg)) = cairn_protocol::decode(&frame) else {
            return Err(EngineError::MissingTarget);
        };
        // Old headers are copied verbatim, **including** the
        // `cairnflow-call-prefix` mark: the call rule follows the lineage, not
        // the gate at rebuild time. Call sites today always issue a new id
        // (`successor_id`), so the new copy's callees differ from the old
        // copy's ids under either rule; copying the mark keeps the rule from
        // depending on that.
        msg.headers.retain(|h| h.key != HDR_RESTARTED_FROM);
        msg.headers.push(pb::Header {
            key: HDR_RESTARTED_FROM.to_string(),
            value: hex(old),
        });
        // The successor is a new invocation: it lives under this binary's
        // state-write rule (D-W5), even if the old copy was created by a binary
        // from before the rule. The copied prefix can be replayed — the machine
        // only checks **new** commands.
        if !msg.headers.iter().any(|h| h.key == HDR_STATE_LOCK) {
            msg.headers.push(pb::Header {
                key: HDR_STATE_LOCK.to_string(),
                value: "on".to_string(),
            });
        }
        if let Some(v) = input {
            msg.value = Some(cairn_protocol::value::inline(v));
        }
        let wire = Message::Input(msg).encode(Flags::NONE);
        let entry0 =
            JournalEntry::new(MessageType::INPUT, wire.slice(cairn_protocol::HEADER_LEN..));

        let mut muts = Mutations::new();
        let raw0 = entry0.encode();
        muts.append_journal(new, 0, &raw0);
        for (i, raw) in rows.iter().take(keep as usize).skip(1) {
            muts.append_journal(new, *i, raw);
            // The copied prefix carries segment refs: the new copy becomes
            // their holder — purging the old copy does not take them with it.
            if self.values_on() {
                let entry = JournalEntry::decode(raw.clone())?;
                self.stage_entry_holders(&target, new, &entry, &mut muts)?;
            }
        }
        // The new copy is a new invocation: its chain is recomputed from its
        // own entry 0 (entry 0 already differs — it carries `restarted-from`).
        if self.chained.contains(&target.tenant) {
            let (n, h) = cairn_store::archive::chain_of(
                std::iter::once((0u32, raw0.as_ref())).chain(
                    rows.iter()
                        .take(keep as usize)
                        .skip(1)
                        .map(|(i, raw)| (*i, raw.as_ref())),
                ),
            );
            muts.set_chain(new, n, &h);
        }
        let new_meta = self.restart_meta(old, &target, pin_new.as_deref()).await?;
        // Artifact byte counter: `new`'s artifact copies (an earlier commit,
        // the shell's `copy_artifacts`) start counting at the commit that
        // creates its metadata — this commit.
        if self.meta(new).await?.is_none() {
            for (sha, row) in self.writer.artifacts_of_unbounded(new).await? {
                if let Some(row) = row {
                    muts.artifact_bytes(cairn_store::artifact_bytes::delta(
                        new,
                        &sha,
                        &new_meta.tenant,
                        super::artifact_bytes::signed(row.size),
                    ));
                }
            }
        }
        self.stage_status(
            &mut muts,
            new,
            super::meta::Was::New(&new_meta),
            &InvocationStatus::Running,
        )
        .await?;
        if let Some(d) = &pin_new {
            muts.pin_deployment(new, d);
        }
        if let Some((q, seq)) = queue {
            muts.push_vqueue(&q.encode(), Stage::Inbox, seq, new);
        }

        if !status.is_terminal() {
            let pointer = format!("{{\"continuedAsNew\":\"{}\"}}", hex(new));
            self.stage_status(
                &mut muts,
                old,
                super::meta::Was::of(&status, old_at),
                &InvocationStatus::Completed(Bytes::from(pointer).into()),
            )
            .await?;
            self.refresh_tenant_limits();
            if let Some(ttl) = self.retention_for(&target.tenant) {
                muts.add_purge(self.purge_due(ttl), old);
            }
            if let Some(d) = &pinned_old {
                muts.unpin_deployment(old, d);
            }
            if let Some(slot) = self.vqueue_of(old).await? {
                muts.pop_vqueue(&slot.queue, slot.stage, slot.seq, old);
            }
            self.stage_row(&mut muts, old, row);
            for (caller, cid) in self.writer.attachments_unbounded(old).await? {
                muts.remove_attachment(old, &caller, cid);
                muts.add_attachment(new, &caller, cid);
            }
            if let Some(idem) = idempotency_of(&first) {
                muts.set_idempotency(
                    &target.tenant,
                    target.scope(),
                    &target.service,
                    &target.handler,
                    &target.object_key,
                    &idem,
                    new,
                );
            }
        }
        self.writer.commit(muts).await?;
        #[cfg(feature = "lifecycle-check")]
        {
            self.assert_lifecycle(old).await?;
            self.assert_lifecycle(new).await?;
        }
        Ok(RestartOutcome::Restarted { new: *new })
    }

    /// Takes an invocation out of `Paused` so it continues.
    ///
    /// `FailureAction::Pause` declares itself as "waiting for manual
    /// intervention", but before this function **there was no way to
    /// intervene**: once in `Paused` there was only `cancel` or `purge` — i.e.
    /// the promise "kept for inspection, then continued" only kept its first
    /// half.
    ///
    /// Only changes the status back to `Running`; continuing is the call
    /// site's job (it has the `Ctx` to call the SDK, the engine does not).
    ///
    /// **Not every `Paused` can be undone by retrying.**
    /// `JOURNAL_MISMATCH` always leads here (the runtime maps 570 ⇒ `Pause`
    /// regardless of the behaviour the SDK sends), and there a rerun only
    /// reproduces the same error unless the deployment has changed. This
    /// function does not guess on anyone's behalf: it does exactly what it is
    /// asked, and the operator is the one who knows what was fixed outside.
    pub async fn resume(&mut self, id: &InvocationId) -> Result<ResumeOutcome> {
        let Some((status, at)) = self.status_at(id).await? else {
            return Ok(ResumeOutcome::NotFound);
        };
        let InvocationStatus::Paused { .. } = status else {
            return Ok(ResumeOutcome::NotPaused(status));
        };
        let mut muts = Mutations::new();
        self.stage_status(
            &mut muts,
            id,
            super::meta::Was::of(&status, at),
            &InvocationStatus::Running,
        )
        .await?;
        self.writer.commit(muts).await?;
        Ok(ResumeOutcome::Resumed)
    }

    /// Manual purge through the admin API. Only invocations that have finished for good.
    pub async fn purge_now(&mut self, id: &InvocationId) -> Result<PurgeOutcome> {
        match self.status(id).await? {
            None => Ok(PurgeOutcome::NotFound),
            Some(s) if !s.is_terminal() => Ok(PurgeOutcome::StillRunning),
            Some(_) => {
                self.purge(id, None).await?;
                Ok(PurgeOutcome::Purged)
            }
        }
    }

    /// Cancels an invocation that has not finished for good.
    ///
    /// Every consequence lives in **one** commit, exactly like `Effect::Finish`
    /// in [`Engine::step`]: the ending status, the result for the caller
    /// (`reply_to` from entry 0), and the result for every attached waiter.
    /// Split apart, a death between two commits would leave waiters hanging on
    /// an invocation that was cancelled.
    ///
    /// This is an **admin**-level cancel, not the protocol's CANCEL signal
    /// (that one goes through the signal path): an SDK turn in flight is not
    /// cut off, but its result is no longer recorded because the status is
    /// already terminal, and pending timers / retries are cleaned up when due.
    ///
    /// The queue entry is removed **in the same** commit: previously it was
    /// removed in a second commit (`release_queue_slot`), and a death exactly
    /// in between left the entry of an invocation that had finished for good —
    /// in `Inbox`, no path cleans it up. The VO lock (RAM) is the call site's
    /// job.
    pub async fn cancel(&mut self, id: &InvocationId, message: &str) -> Result<CancelOutcome> {
        self.fail_in(id, CANCELLED, message, None, StageWrite::Remove)
            .await
    }

    /// Ends an unfinished invocation with error `code` — the body shared by
    /// [`Self::cancel`] and by `KILL` after `maxAttempts`. All consequences in
    /// one commit: status, waiters, cancellation propagated down the child tree.
    pub async fn fail_with(
        &mut self,
        id: &InvocationId,
        code: u32,
        message: &str,
    ) -> Result<CancelOutcome> {
        self.fail_in(id, code, message, None, StageWrite::Keep)
            .await
    }

    /// [`Self::fail_with`] together with the stage change of a lifecycle-core
    /// Plan (`Remove` — leave the queue) in **the same** commit (an admin
    /// cancel used to leave the queue in a second commit).
    pub async fn fail_in(
        &mut self,
        id: &InvocationId,
        code: u32,
        message: &str,
        slot: Option<&Option<VQueueSlot>>,
        stage: StageWrite,
    ) -> Result<CancelOutcome> {
        let Some((status, at)) = self.status_at(id).await? else {
            return Ok(CancelOutcome::NotFound);
        };
        if status.is_terminal() {
            return Ok(CancelOutcome::AlreadyFinished(status));
        }
        let was = super::meta::Was::of(&status, at);

        let first = match self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        {
            Some(raw) => JournalEntry::decode(raw).ok(),
            None => None,
        };
        let reply_to = first.as_ref().and_then(reply_to_of);
        let pinned = match &first {
            Some(f) => self.pinned_of(id, f).await?,
            None => None,
        };
        let result = ReplyResult::Failure {
            code,
            message: message.to_string(),
        };
        let mut muts = Mutations::new();
        self.stage_status(
            &mut muts,
            id,
            was,
            &InvocationStatus::Failed {
                code,
                message: message.to_string(),
            },
        )
        .await?;
        // Like `Effect::Finish`: a pending scheduled retry is meaningless for a finished invocation.
        muts.drop_retry(id);
        self.stage_lifecycle(&mut muts, id, slot, stage, RowWrite::Keep)
            .await?;
        // A cancel is also an ending — the same retention rule as
        // `Effect::Finish`, and the same deployment-unpinning rule.
        let tenant = first
            .as_ref()
            .and_then(target_of)
            .map(|t| t.tenant)
            .unwrap_or_default();
        self.refresh_tenant_limits();
        if let Some(ttl) = self.retention_for(&tenant) {
            muts.add_purge(self.purge_due(ttl), id);
        }
        if let Some(d) = &pinned {
            muts.unpin_deployment(id, d);
        }
        if let Some(r) = reply_to {
            let entry = OutboxEntry::Reply(OutboundReply {
                caller: r.caller,
                completion_id: r.completion_id,
                result: result.clone(),
            });
            // Owner = the cancelled invocation — the producer of the reply, not
            // the waiter.
            self.stage_outbox(
                &mut muts,
                partition_key_of(id),
                (r.caller, r.completion_id),
                &entry.encode(),
            )
            .await?;
        }
        for (caller, completion_id) in self.writer.attachments_unbounded(id).await? {
            let entry = OutboxEntry::Reply(OutboundReply {
                caller,
                completion_id,
                result: result.clone(),
            });
            // Owner = the cancelled invocation, not the waiter.
            self.stage_outbox(
                &mut muts,
                partition_key_of(id),
                (caller, completion_id),
                &entry.encode(),
            )
            .await?;
            muts.remove_attachment(id, &caller, completion_id);
        }
        // **Cancellation propagates down.** Nobody reads these children's
        // results anymore — the parent is dead — so letting them run on is
        // letting them spend money on a result that will be thrown away. In
        // the workloads cairn targets, that is not wasted resources but a
        // bill.
        //
        // Same commit as the `Failed` status: split apart, a death exactly
        // between two commits would leave an orphan child tree that nothing
        // can undo anymore — the parent is terminal, so there is no run left to
        // replay the cancel command.
        for (id_completion, target) in self.live_children(id).await? {
            let entry = OutboxEntry::Cancel(OutboundCancel {
                caller: *id,
                id_completion,
                target,
            });
            self.stage_outbox(
                &mut muts,
                partition_key_of(id),
                (*id, id_completion),
                &entry.encode(),
            )
            .await?;
        }
        self.writer.commit(muts).await?;
        #[cfg(feature = "lifecycle-check")]
        if stage != StageWrite::Keep {
            // Only when this commit is the whole ending (stage removed in the
            // same commit); a bare `fail_with` leaves the stage to the call
            // site's later commit.
            self.assert_lifecycle(id).await?;
        }
        Ok(CancelOutcome::Cancelled)
    }
}
