// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`Mutations`] — the write batch of one commit, one function per operation per table.

use super::*;

/// The mutation set of one commit.
///
/// Deliberately **without** an arbitrary `put(key, value)`: every write path goes through a
/// named table (INV-3, [`keyspace::Table`]). Writing something that does not belong to
/// those tables means changing [`crate::keyspace`] — and that is exactly the moment to stop
/// and think, because a pending piece of work
/// that is not in the state tables vanishes on failover.
#[derive(Default)]
pub struct Mutations {
    pub(super) batch: Batch,
    count: usize,
    /// Key + value bytes of the batch — an estimate of the bytes written, for per-range
    /// load statistics. A delete counts its key.
    pub(super) bytes: u64,
    /// The first error met while building the batch, returned by [`PartitionWriter::commit`].
    ///
    /// Deferring instead of returning a `Result` from each function is deliberate: these
    /// functions chain (`muts.a().b().c()`), and making the caller `?` at every link would
    /// turn every batch-building site in the engine into a forest of question marks. A
    /// batch only means something when committed, so the commit is the right place for the
    /// error to surface — and there is no path that commits a broken batch.
    pub(super) invalid: Option<StoreError>,
    /// Operations on the three ordered indexes (outbox, signal, inbound), in batch order:
    /// `(slot, write?, key)`. The commit uses it to keep the scan floors ([`SeqFloor`])
    /// right for **every** write path, not only those the flusher knows about.
    pub(super) seq_ops: Vec<(usize, bool, bytes::Bytes)>,
    /// The smallest due time of the due-time index rows **written** in the batch, by slot
    /// of `floors::DUE_INDEXES` — the commit lowers the scan floor (and the durable floor)
    /// to it if needed. Catches every write path, including derived-region rebuilds.
    pub(super) due_low: [Option<u64>; 4],
    /// The largest `seq` of the queue index rows **written** in the batch — the commit
    /// raises the writer's `queue` floor before building the durable floor value
    /// (`floors::QUEUE_HIGH`). Catches every write path, including derived-region rebuilds.
    pub(super) queue_high: Option<u64>,
    /// `(id, status code byte)` of every status write in the batch — the commit reports
    /// them to the writer's hook ([`PartitionWriter::set_status_hook`]) after submitting.
    pub(super) status_writes: Vec<(InvocationId, u8)>,
    /// The batch **writes** (not only deletes) a row the flusher will send out — outbox or
    /// signal outbox ([`keyspace::is_send_key`]). Set in [`Self::put`] itself, the only
    /// write path, so no call site can forget it (LF-1).
    pub(super) sends: bool,
    /// Query counter changes of every [`Self::query_event`] in the batch — the commit
    /// reports them to the writer's counting hook after submitting.
    pub(super) query_events: Vec<crate::query_index::CountDelta>,
    /// Artifact byte counter changes of every [`Self::artifact_bytes`] in the batch — the
    /// commit reports them to the writer's hook after submitting.
    pub(super) artifact_events: Vec<crate::artifact_bytes::ArtifactDelta>,
    /// The batch's billing units by tenant — the commit hands them to the writer's unit
    /// meter after submitting, together with the batch's `seq` ([`crate::units`]). Writes
    /// no row.
    pub(super) units: Vec<(String, u64)>,
}

impl Mutations {
    pub fn new() -> Self {
        Self::default()
    }

    /// The **only** write path into the batch. Every public mutation goes through here.
    ///
    /// One check here instead of one per function: a key over the limit reaching
    /// `WriteBatch` makes SlateDB `assert!` and panic, and a panic here kills the engine
    /// thread — the whole partition stops because of one long object key. Gathering it
    /// here also keeps `count` from counting rows that were never written.
    ///
    /// Errors are **deferred to `commit`**, not returned on the spot: the functions below
    /// chain (`muts.a().b().c()`), and making the caller `?` at each link would turn every
    /// batch-building site in the engine into a forest of question marks. A batch only
    /// means something when committed, so the commit is the right place for the error to
    /// surface — and there is no path that commits a broken batch.
    pub(super) fn put(&mut self, what: &'static str, key: Vec<u8>, value: &[u8]) -> &mut Self {
        if self.accept(what, &key) {
            self.bytes += (key.len() + value.len()) as u64;
            if let Some((slot, due)) = super::floors::due_of_index_key(&key) {
                let low = &mut self.due_low[slot];
                *low = Some(low.map_or(due, |d| d.min(due)));
            }
            self.sends |= keyspace::is_send_key(&key);
            if let Some(seq) = keyspace::ix_vqueue_seq(&key) {
                self.queue_high = Some(self.queue_high.map_or(seq, |q| q.max(seq)));
            }
            let key = bytes::Bytes::from(key);
            let value = bytes::Bytes::copy_from_slice(value);
            if let Some(slot) = SeqFloor::slot_of(&key) {
                self.seq_ops.push((slot, true, key.clone()));
            }
            self.batch.put(key, value);
            self.count += 1;
        }
        self
    }

    pub(super) fn delete(&mut self, what: &'static str, key: Vec<u8>) -> &mut Self {
        if self.accept(what, &key) {
            self.bytes += key.len() as u64;
            let key = bytes::Bytes::from(key);
            if let Some(slot) = SeqFloor::slot_of(&key) {
                self.seq_ops.push((slot, false, key.clone()));
            }
            self.batch.delete(key);
            self.count += 1;
        }
        self
    }

    /// Keeps the **first** error: it is the cause, later errors are usually consequences.
    fn accept(&mut self, what: &'static str, key: &[u8]) -> bool {
        if let Err(e) = StoreError::key(what, key) {
            self.invalid.get_or_insert(e);
        }
        self.invalid.is_none()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn len(&self) -> usize {
        self.count
    }

    /// Writes the status, plus the [`keyspace::Index::Invocations`] row carrying its first
    /// byte (the status code) — one commit, so `recover` and `/invocations` read the
    /// derived region instead of scanning the entity region.
    pub fn set_invocation_status(&mut self, id: &InvocationId, status: &[u8]) -> &mut Self {
        let tag: &[u8] = status.get(..1).unwrap_or(&[]);
        if let Some(&t) = tag.first() {
            self.status_writes.push((*id, t));
        }
        self.put("invocation_status", keyspace::invocation_status(id), status)
            .put("invocation_index", keyspace::ix_invocation(id), tag)
    }

    /// The immutable metadata row of `id` — in the creating commit.
    pub fn put_invocation_meta(
        &mut self,
        id: &InvocationId,
        meta: &crate::query_index::InvocationMeta,
    ) -> &mut Self {
        self.put(
            "invocation_meta",
            crate::query_index::meta_key(id),
            &meta.encode(),
        )
    }

    /// Deletes the metadata row — in the cleanup commit.
    pub fn clear_invocation_meta(&mut self, id: &InvocationId) -> &mut Self {
        self.delete("invocation_meta", crate::query_index::meta_key(id))
    }

    /// The **only write path** of the query indexes: moves invocation `id` from `old` to
    /// `new` (`None` = not yet / already cleaned up), applying every descriptor in
    /// [`crate::query_index::QUERY_INDEXES`]. Same position ⇒ no operation
    /// (`running ↔ suspended` never reaches here with a difference).
    pub fn query_event(
        &mut self,
        id: &InvocationId,
        meta: &crate::query_index::InvocationMeta,
        old: Option<crate::query_index::Placement>,
        new: Option<crate::query_index::Placement>,
    ) -> &mut Self {
        self.query_event_with(crate::query_index::QUERY_INDEXES, id, meta, old, new)
    }

    /// [`Self::query_event`] over a given descriptor table — tests adding experimental
    /// descriptors go through here.
    pub fn query_event_with(
        &mut self,
        indexes: &[crate::query_index::QueryIndex],
        id: &InvocationId,
        meta: &crate::query_index::InvocationMeta,
        old: Option<crate::query_index::Placement>,
        new: Option<crate::query_index::Placement>,
    ) -> &mut Self {
        if old == new {
            return self;
        }
        let ops = crate::query_index::event_ops(indexes, id, meta, old, new);
        for k in ops.deletes {
            self.delete("query_index", k);
        }
        for (k, v) in ops.puts {
            self.put("query_index", k, &v);
        }
        self.query_events
            .push(crate::query_index::CountDelta::of(id, meta, old, new));
        self
    }

    /// Writes a precomputed query index row — only the background build uses it; the live
    /// path goes through [`Self::query_event`].
    pub fn put_query_row(&mut self, key: Vec<u8>, value: &[u8]) -> &mut Self {
        self.put("query_index", key, value)
    }

    /// Deletes one query index row — cleaning rows of a removed descriptor, repairs by
    /// `cairn-ctl`.
    pub fn delete_query_row(&mut self, key: Vec<u8>) -> &mut Self {
        self.delete("query_index", key)
    }

    /// Writes the range's query index state marker (`0x01 ‖ 0xFC`).
    pub fn set_query_marker(&mut self, marker: &[u8]) -> &mut Self {
        self.put("query_marker", crate::query_index::ready_key(), marker)
    }

    /// Pins invocation `id` to `deployment`: the entity row [`Table::Pins`] and the
    /// [`Index::Deployment`] row, in the commit that creates the invocation.
    pub fn pin_deployment(&mut self, id: &InvocationId, deployment: &str) -> &mut Self {
        self.put("pin", keyspace::pin(id), deployment.as_bytes())
            .put("pin_index", keyspace::ix_deployment(deployment, id), &[])
    }

    /// Unpins — in the commit that completes (or cleans up) the invocation. Deleting
    /// something absent is harmless, so it can be called again.
    pub fn unpin_deployment(&mut self, id: &InvocationId, deployment: &str) -> &mut Self {
        self.delete("pin", keyspace::pin(id))
            .delete("pin_index", keyspace::ix_deployment(deployment, id))
    }

    /// Sets the head of `id`'s hash chain — in the same commit as the entries it covers.
    pub fn set_chain(&mut self, id: &InvocationId, len: u32, hash: &[u8; 32]) -> &mut Self {
        self.put(
            "chain",
            keyspace::chain(id),
            &keyspace::encode_chain(len, hash),
        )
    }

    /// Deletes the chain head — in the commit cleaning up the invocation.
    pub fn clear_chain(&mut self, id: &InvocationId) -> &mut Self {
        self.delete("chain", keyspace::chain(id))
    }

    /// An audit row waiting for export + its index — in the same batch as the action it
    /// records.
    pub fn put_audit(&mut self, key: Vec<u8>, record: &[u8]) -> &mut Self {
        let ix = keyspace::ix_audit(&key);
        self.put("audit", key, record);
        self.put("audit_ix", ix, &[])
    }

    /// Drops an exported audit row (the object was written and read back matching).
    pub fn remove_audit(&mut self, key: &[u8]) -> &mut Self {
        self.delete("audit_ix", keyspace::ix_audit(key));
        self.delete("audit", key.to_vec())
    }

    /// The error of the latest failed attempt — overwrites the old row.
    pub fn set_last_failure(
        &mut self,
        id: &InvocationId,
        failure: &keyspace::LastFailure,
    ) -> &mut Self {
        self.put(
            "last_failure",
            keyspace::last_failure(id),
            &failure.encode(),
        )
    }

    /// Deletes the error of the latest attempt — in the commit cleaning up the invocation.
    pub fn clear_last_failure(&mut self, id: &InvocationId) -> &mut Self {
        self.delete("last_failure", keyspace::last_failure(id))
    }

    /// The artifact row `(id, sha)`: the object was fully PUT before this commit (AP-1).
    pub fn put_artifact(
        &mut self,
        id: &InvocationId,
        sha: &[u8; 32],
        row: &keyspace::ArtifactRow,
    ) -> &mut Self {
        self.put("artifact", keyspace::artifact(id, sha), &row.encode())
    }

    /// Deletes the artifact row `(id, sha)` (purge).
    pub fn purge_artifact(&mut self, id: &InvocationId, sha: &[u8; 32]) -> &mut Self {
        self.delete("artifact", keyspace::artifact(id, sha))
    }

    /// One change of the range's artifact byte counter — writes no row; the commit reports
    /// it to the writer's hook after submitting ([`PartitionWriter::set_artifact_bytes`]).
    /// The caller computes it from state read **within the same engine hold** as the
    /// commit (the old row, the metadata) — see [`crate::artifact_bytes`]. `None` = no
    /// change.
    pub fn artifact_bytes(
        &mut self,
        delta: Option<crate::artifact_bytes::ArtifactDelta>,
    ) -> &mut Self {
        if let Some(d) = delta {
            self.artifact_events.push(d);
        }
        self
    }

    /// `n` billing units of `tenant` for the rows in this batch — writes no row; if the
    /// batch is submitted, the writer hands it to the range's unit meter to wait for
    /// durability ([`crate::units`]). Called **at the same place** that builds the rows
    /// producing the units, so no path commits rows and forgets to count them, or counts
    /// without committing.
    pub fn meter_units(&mut self, tenant: &str, n: u64) -> &mut Self {
        if n == 0 {
            return self;
        }
        match self.units.last_mut() {
            Some((t, k)) if t == tenant => *k += n,
            _ => self.units.push((tenant.to_string(), n)),
        }
        self
    }

    /// Units attached to the batch, by tenant (for tests).
    pub fn metered_units(&self) -> &[(String, u64)] {
        &self.units
    }

    /// Writes one chunk of durable stream `name`, keyed by the offset of its first record.
    /// Rewriting the same `(id, name, first)` overwrites — if an earlier commit failed, a
    /// later commit brings back exactly that chunk.
    pub fn put_stream_chunk(
        &mut self,
        id: &InvocationId,
        name: &str,
        first: u64,
        chunk: &keyspace::StreamChunk,
    ) -> &mut Self {
        self.put(
            "stream_chunk",
            keyspace::stream_chunk(id, name, first),
            &chunk.encode(first),
        )
    }

    /// Deletes a chunk — only in the commit cleaning up the invocation (`Engine::purge`).
    pub fn purge_stream_chunk(&mut self, id: &InvocationId, name: &str, first: u64) -> &mut Self {
        self.delete("stream_chunk", keyspace::stream_chunk(id, name, first))
    }

    pub fn clear_invocation_status(&mut self, id: &InvocationId) -> &mut Self {
        self.delete("invocation_status", keyspace::invocation_status(id))
            .delete("invocation_index", keyspace::ix_invocation(id))
    }

    /// Writes a journal entry. Append-only: there is no API to modify an entry already
    /// written.
    pub fn append_journal(
        &mut self,
        id: &InvocationId,
        command_index: u32,
        entry: &[u8],
    ) -> &mut Self {
        self.put("journal", keyspace::journal_entry(id, command_index), entry)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_state(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.put(
            "state",
            keyspace::state(owner_pk, tenant, scope, service, object_key, user_key),
            value,
        )
    }

    pub fn clear_state(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
    ) -> &mut Self {
        self.delete(
            "state",
            keyspace::state(owner_pk, tenant, scope, service, object_key, user_key),
        )
    }

    /// A [`keyspace::Table::SegmentedState`] row. The caller keeps the "one of the two
    /// rows" invariant: it deletes the `State` row of the same key in the same batch.
    #[allow(clippy::too_many_arguments)]
    pub fn put_segmented_state(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
        row: &crate::value::SegmentedRow,
    ) -> &mut Self {
        self.put(
            "segmented_state",
            keyspace::segmented_state(owner_pk, tenant, scope, service, object_key, user_key),
            &row.encode(),
        )
    }

    pub fn clear_segmented_state(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
    ) -> &mut Self {
        self.delete(
            "segmented_state",
            keyspace::segmented_state(owner_pk, tenant, scope, service, object_key, user_key),
        )
    }

    /// Holder `holder` holds segment `(sha, gen)` of size `seg_len` of `owner` — a blind,
    /// idempotent put. The segment must be durable before this batch's commit (AP-1).
    #[allow(clippy::too_many_arguments)]
    pub fn put_value_ref(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        owner: &crate::value::Owner,
        holder: &crate::value::Holder,
        sha: &[u8; 32],
        gen: u64,
        seg_len: u64,
    ) -> &mut Self {
        self.put(
            "value_refs",
            keyspace::value_ref(owner_pk, tenant, owner, holder, sha, gen),
            &crate::value::encode_ref_row(seg_len),
        )
    }

    /// Removes one holder row by a key already read (from a scan of
    /// [`PartitionWriter::value_refs_under_unbounded`]). A key that is not a `ValueRefs`
    /// key ⇒ does nothing.
    pub fn remove_value_ref(&mut self, key: &[u8]) -> &mut Self {
        if keyspace::table_of(key) != Some(keyspace::Table::ValueRefs) {
            return self;
        }
        self.delete("value_refs", key.to_vec())
    }

    /// Timer: the entity row `(id, completion) → due ‖ payload` and the due-time index
    /// row — one commit.
    pub fn add_timer(
        &mut self,
        due_ms: u64,
        id: &InvocationId,
        completion_id: u32,
        payload: &[u8],
    ) -> &mut Self {
        self.put(
            "timers",
            keyspace::timer(id, completion_id),
            &keyspace::encode_due(due_ms, payload),
        )
        .put(
            "timer_index",
            keyspace::ix_timer(due_ms, id, completion_id),
            b"",
        )
    }

    /// Deletes a timer — `due_ms` must be the due time **currently** written in the entity
    /// row (the call site takes it from a read that checked the entity row, see
    /// [`PartitionWriter::due_timers`]).
    pub fn remove_timer(
        &mut self,
        due_ms: u64,
        id: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        self.delete("timers", keyspace::timer(id, completion_id))
            .delete("timer_index", keyspace::ix_timer(due_ms, id, completion_id))
    }

    /// Records `(service, handler, key, idem_key) → invocation`.
    #[allow(clippy::too_many_arguments)]
    pub fn set_idempotency(
        &mut self,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        handler: &str,
        object_key: &str,
        idem_key: &str,
        id: &InvocationId,
    ) -> &mut Self {
        self.put(
            "idempotency",
            keyspace::idempotency(tenant, scope, service, handler, object_key, idem_key),
            id,
        )
    }

    /// Adds one waiter for the result of `target`.
    pub fn add_attachment(
        &mut self,
        target: &InvocationId,
        caller: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        self.put(
            "attachments",
            keyspace::attachment(target, caller, completion_id),
            b"",
        )
    }

    pub fn remove_attachment(
        &mut self,
        target: &InvocationId,
        caller: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        self.delete(
            "attachments",
            keyspace::attachment(target, caller, completion_id),
        )
    }

    /// Writes the result of a durable promise. Overwrites — the call site must check "not
    /// yet present" itself first, within the same commit.
    #[allow(clippy::too_many_arguments)]
    pub fn set_promise(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
        record: &[u8],
    ) -> &mut Self {
        self.put(
            "promise",
            keyspace::promise(owner_pk, tenant, scope, service, object_key, name),
            record,
        )
    }

    /// Deletes a promise's result for good. Used in exactly one place: purging a workflow.
    ///
    /// Promises are **not** deleted when the workflow completes, only at cleanup — a
    /// deliberate difference: a `SHARED` handler running after `run` has finished can still
    /// read the results of that run's promises, just as it can still read the state.
    pub fn clear_promise(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
    ) -> &mut Self {
        self.delete(
            "promise",
            keyspace::promise(owner_pk, tenant, scope, service, object_key, name),
        )
    }

    // An object's identity (owner, scope, service, key) is four separate parameters —
    // grouping them into a type of their own is for the next API touch.
    #[allow(clippy::too_many_arguments)]
    pub fn add_promise_waiter(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
        waiter: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        self.put(
            "promise_waiter",
            keyspace::promise_waiter(
                owner_pk,
                tenant,
                scope,
                service,
                object_key,
                name,
                waiter,
                completion_id,
            ),
            b"",
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn remove_promise_waiter(
        &mut self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
        waiter: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        self.delete(
            "promise_waiter",
            keyspace::promise_waiter(
                owner_pk,
                tenant,
                scope,
                service,
                object_key,
                name,
                waiter,
                completion_id,
            ),
        )
    }

    /// Schedules an invocation to run again. `payload` carries the number of attempts.
    ///
    /// An invocation has **one** schedule: the entity row is keyed by id, rescheduling
    /// overwrites. The index row of the old schedule (if the call site does not delete it)
    /// becomes stale — skipped and cleaned up when it comes due.
    pub fn add_retry(&mut self, due_ms: u64, id: &InvocationId, payload: &[u8]) -> &mut Self {
        self.put(
            "retries",
            keyspace::retry(id),
            &keyspace::encode_due(due_ms, payload),
        )
        .put("retry_index", keyspace::ix_retry(due_ms, id), b"")
    }

    /// Drops the schedule (retry or watchdog) of an invocation **that has completed**,
    /// without needing its due time: only the entity row is deleted. The index row becomes
    /// stale — skipped and cleaned up when it comes due (one merged commit of the
    /// background tick), so the completing commit need not read the due time.
    pub fn drop_retry(&mut self, id: &InvocationId) -> &mut Self {
        self.delete("retries", keyspace::retry(id))
    }

    pub fn remove_retry(&mut self, due_ms: u64, id: &InvocationId) -> &mut Self {
        self.delete("retries", keyspace::retry(id))
            .delete("retry_index", keyspace::ix_retry(due_ms, id))
    }

    /// Queues a signal to send. MUST be in the same commit as the `SendSignal` journal entry
    /// that produced it — the same INV-3 contract as [`Self::push_outbox`].
    pub fn push_signal_outbox(
        &mut self,
        seq: u64,
        sender: &InvocationId,
        entry_index: u32,
        payload: &[u8],
    ) -> &mut Self {
        let key = keyspace::signal_outbox(sender, entry_index);
        let ix = keyspace::ix_seq(keyspace::Index::Signal, seq, &key);
        self.put("signal_outbox", key, &keyspace::encode_seq(seq, payload))
            .put("signal_index", ix, b"")
    }

    pub fn pop_signal_outbox(
        &mut self,
        seq: u64,
        sender: &InvocationId,
        entry_index: u32,
    ) -> &mut Self {
        let key = keyspace::signal_outbox(sender, entry_index);
        let ix = keyspace::ix_seq(keyspace::Index::Signal, seq, &key);
        self.delete("signal_outbox", key).delete("signal_index", ix)
    }

    /// Queues a one-way call that is not due yet. `payload` is the outbox row that will be
    /// moved into `outbox` when due — in one commit, see [`PartitionWriter::due_delayed`]
    /// and its call site.
    pub fn add_delayed(
        &mut self,
        due_ms: u64,
        id: &InvocationId,
        completion_id: u32,
        payload: &[u8],
    ) -> &mut Self {
        self.put(
            "delayed",
            keyspace::delayed(id, completion_id),
            &keyspace::encode_due(due_ms, payload),
        )
        .put(
            "delayed_index",
            keyspace::ix_delayed(due_ms, id, completion_id),
            b"",
        )
    }

    pub fn remove_delayed(
        &mut self,
        due_ms: u64,
        id: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        self.delete("delayed", keyspace::delayed(id, completion_id))
            .delete(
                "delayed_index",
                keyspace::ix_delayed(due_ms, id, completion_id),
            )
    }

    /// Writes schedule row `id` with next fire time `due` — the entity row and the index
    /// row in one commit. `old_due`: the due time of the existing index row (moved ⇒ it is
    /// deleted); `None` = a new schedule.
    ///
    /// A paused schedule writes `due = u64::MAX`: it still has an index row (the range's
    /// schedule list), it never comes due.
    pub fn put_schedule(
        &mut self,
        id: &InvocationId,
        old_due: Option<u64>,
        due: u64,
        row: &[u8],
    ) -> &mut Self {
        if let Some(old) = old_due.filter(|o| *o != due) {
            self.delete("schedule_index", keyspace::ix_schedule_due(old, id));
        }
        self.put(
            "schedules",
            keyspace::schedule(id),
            &keyspace::encode_due(due, row),
        )
        .put("schedule_index", keyspace::ix_schedule_due(due, id), b"")
    }

    /// Deletes schedule `id` (`DELETE /schedules/{name}`) — the entity row and the index
    /// row at due time `due`, in one commit. A logical deletion inside the Db, class L
    /// (`docs/reliability/irreversible.md`).
    pub fn remove_schedule(&mut self, id: &InvocationId, due: u64) -> &mut Self {
        self.delete("schedules", keyspace::schedule(id))
            .delete("schedule_index", keyspace::ix_schedule_due(due, id))
    }

    /// Schedules the cleanup of a completed invocation (retention).
    ///
    /// MUST be in the same commit as the completed status: split apart, a death right
    /// between the two commits leaves an invocation that is never cleaned up — and no
    /// error to notice.
    pub fn add_purge(&mut self, due_ms: u64, id: &InvocationId) -> &mut Self {
        self.put(
            "purges",
            keyspace::purge(id),
            &keyspace::encode_due(due_ms, b""),
        )
        .put("purge_index", keyspace::ix_purge(due_ms, id), b"")
    }

    pub fn remove_purge(&mut self, due_ms: u64, id: &InvocationId) -> &mut Self {
        self.delete("purges", keyspace::purge(id))
            .delete("purge_index", keyspace::ix_purge(due_ms, id))
    }

    /// Deletes a journal entry — **for purge only**.
    ///
    /// The journal is append-only for a live invocation; this deletion path exists for
    /// exactly one thing: cleaning up the whole journal of a completed invocation, after
    /// retention. There is no API to delete single entries midway because there is no
    /// valid use case for it.
    pub fn purge_journal_entry(&mut self, id: &InvocationId, command_index: u32) -> &mut Self {
        self.delete("journal", keyspace::journal_entry(id, command_index))
    }

    /// Resets an idempotency key to "absent" — used when cleaning up the invocation that
    /// holds it.
    ///
    /// The semantic consequence must be said out loud: the dedupe window is exactly the
    /// retention. After the cleanup, a call with the same key is a **new** invocation.
    pub fn remove_idempotency(
        &mut self,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        handler: &str,
        object_key: &str,
        idem_key: &str,
    ) -> &mut Self {
        self.delete(
            "idempotency",
            keyspace::idempotency(tenant, scope, service, handler, object_key, idem_key),
        )
    }

    /// Queues an invocation into a virtual queue: the `VQueueIndex` entity row (its place)
    /// and the queue's index row — **one commit**.
    ///
    /// The entity row is the only lookup path when only the id is at hand, so an entry
    /// without it is an entry nobody can find to clean up — that is, a slot held forever
    /// after the next leader change.
    pub fn push_vqueue(
        &mut self,
        queue: &[u8],
        stage: Stage,
        seq: u64,
        id: &InvocationId,
    ) -> &mut Self {
        self.put("vqueue", keyspace::ix_vqueue(queue, stage, seq, id), b"")
            .put(
                "vqueue_index",
                keyspace::vqueue_index(id),
                &keyspace::encode_vqueue_index(queue, stage, seq),
            )
    }

    /// Moves to another stage: deletes the old row, writes the new one, updates the entity
    /// row — **one commit**.
    ///
    /// `seq` stays the same across every move: it is the invocation's place in the queue,
    /// and suspending then waking up must not push it to the back.
    pub fn move_vqueue(
        &mut self,
        queue: &[u8],
        seq: u64,
        from: Stage,
        to: Stage,
        id: &InvocationId,
    ) -> &mut Self {
        self.delete("vqueue", keyspace::ix_vqueue(queue, from, seq, id));
        self.push_vqueue(queue, to, seq, id)
    }

    /// Removes for good from the queue: the index row + the entity row.
    pub fn pop_vqueue(
        &mut self,
        queue: &[u8],
        stage: Stage,
        seq: u64,
        id: &InvocationId,
    ) -> &mut Self {
        self.delete("vqueue", keyspace::ix_vqueue(queue, stage, seq, id))
            .delete("vqueue_index", keyspace::vqueue_index(id))
    }

    /// Queues work to send out: the entity row (the value carries `seq`) and the index row
    /// in creation order.
    ///
    /// MUST be in the **same** commit as the journal entry that produced it. Splitting it
    /// into two commits opens exactly the window INV-3 exists to close.
    ///
    /// Two rows, **one commit**: the index is the only way the flusher sees this row, so a
    /// main row without its index is a call that is never sent — and no error to notice.
    ///
    /// `seq` **should be the old seq** if the row already exists (D-O2,
    /// `Engine::next_outbox_seq`): a new seq turns the old index row stale — harmless
    /// because the flusher checks the `seq` in the entity row, but one wasted read.
    pub fn push_outbox(
        &mut self,
        owner_pk: PartitionKey,
        seq: u64,
        id: &InvocationId,
        completion_id: u32,
        payload: &[u8],
    ) -> &mut Self {
        let key = keyspace::outbox(owner_pk, id, completion_id);
        let ix = keyspace::ix_seq(keyspace::Index::Outbox, seq, &key);
        self.put("outbox", key, &keyspace::encode_seq(seq, payload))
            .put("outbox_index", ix, b"")
    }

    /// Forgets an outbox row: the main row **and** the index row, one commit.
    pub fn pop_outbox(
        &mut self,
        owner_pk: PartitionKey,
        seq: u64,
        id: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        let key = keyspace::outbox(owner_pk, id, completion_id);
        let ix = keyspace::ix_seq(keyspace::Index::Outbox, seq, &key);
        self.delete("outbox", key).delete("outbox_index", ix)
    }

    /// Receives a message from another partition.
    ///
    /// The key derives from the message content, so writing the same message again
    /// **overwrites**, it does not add a row. See [`keyspace::inbound`]. `owner_pk` is the
    /// pk of the **destination**.
    #[allow(clippy::too_many_arguments)]
    pub fn push_inbound(
        &mut self,
        owner_pk: PartitionKey,
        seq: u64,
        kind: keyspace::InboundKind,
        id: &InvocationId,
        completion_id: u32,
        payload: &[u8],
    ) -> &mut Self {
        let key = keyspace::inbound(owner_pk, kind, id, completion_id);
        let ix = keyspace::ix_seq(keyspace::Index::Inbound, seq, &key);
        self.put("inbound", key, &keyspace::encode_seq(seq, payload))
            .put("inbound_index", ix, b"")
    }

    pub fn pop_inbound(
        &mut self,
        owner_pk: PartitionKey,
        seq: u64,
        kind: keyspace::InboundKind,
        id: &InvocationId,
        completion_id: u32,
    ) -> &mut Self {
        let key = keyspace::inbound(owner_pk, kind, id, completion_id);
        let ix = keyspace::ix_seq(keyspace::Index::Inbound, seq, &key);
        self.delete("inbound", key).delete("inbound_index", ix)
    }

    /// Cleans up a **stale** index row — pointing at an entity row that no longer exists,
    /// or does not match. Only takes derived-region keys: no path can use this function to
    /// delete primary data.
    pub fn forget_index_row(&mut self, key: Vec<u8>) -> &mut Self {
        if keyspace::Region::of_key(&key) == Some(keyspace::Region::Derived) {
            self.delete("stale_index", key);
        } else {
            self.invalid.get_or_insert(StoreError::Corrupt(format!(
                "forget_index_row got a key outside the derived region: {key:02x?}"
            )));
        }
        self
    }

    /// Deletes a row of a tenant being purged: the caller (`Engine::purge_tenant_rows`) has
    /// classified the key with [`keyspace::row_owner`] under a `TenantPurgeGrant`. Keys of
    /// the range itself (ready marker, floors, fences) or keys that do not decode are
    /// refused — no path can use this function to delete something that belongs to no
    /// tenant.
    pub fn purge_tenant_row(&mut self, key: Vec<u8>) -> &mut Self {
        match keyspace::row_owner(&key) {
            keyspace::RowOwner::Range | keyspace::RowOwner::Unknown => {
                self.invalid.get_or_insert(StoreError::Corrupt(format!(
                    "purge_tenant_row got a key that belongs to no tenant: {key:02x?}"
                )));
            }
            _ => {
                self.delete("tenant_purge", key);
            }
        }
        self
    }

    /// Writes a key–value pair directly, **bypassing every invariant** of the functions
    /// above. Only for tests building a state the normal paths cannot produce (stale index
    /// rows, a missing derived region).
    #[cfg(any(test, feature = "testkit"))]
    pub fn raw_put(&mut self, key: Vec<u8>, value: &[u8]) -> &mut Self {
        self.put("raw", key, value)
    }

    /// Deletes a key directly — under the same conditions as [`Self::raw_put`].
    #[cfg(any(test, feature = "testkit"))]
    pub fn raw_delete(&mut self, key: Vec<u8>) -> &mut Self {
        self.delete("raw", key)
    }
}
