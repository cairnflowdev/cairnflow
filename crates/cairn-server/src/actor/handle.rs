// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`EngineHandle`] — the `Clone + Send` sending end of the command channel, the API for
//! the rest of the node.

use super::*;

/// What a range's lifecycle shares with its handle: the command channel, the retiring flag,
/// the latest seal epoch, the waiter registry.
type LoopParts = (
    mpsc::Sender<Command>,
    Arc<AtomicBool>,
    Arc<AtomicU64>,
    Waiters,
);

#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<Command>,
    /// This engine's range is retiring: every command about a key gets `WrongRange`.
    /// One-way — there is no path that clears the flag.
    retiring: Arc<AtomicBool>,
    /// `writer_epoch` of the most recently sealed writer.
    sealed_epoch: Arc<AtomicU64>,
    /// The range's registry of outcome waiters.
    waiters: Waiters,
}

impl EngineHandle {
    /// Build the engine thread. Returns once the engine has finished opening (or failed to
    /// open).
    ///
    /// `tick_interval` enables the background loop for timers and retries. `None` runs no
    /// loop at all — for tests that need to control time.
    pub fn spawn(
        factory: EngineFactory,
        registry: Registry,
        tick_interval: Option<Duration>,
        readiness: Readiness,
    ) -> Result<Self> {
        Self::spawn_with_transport(
            factory,
            registry,
            tick_interval,
            readiness,
            Transport::disconnected(),
            TurnLimits::default(),
            None,
        )
    }

    /// Like [`Self::spawn`], plus transport, turn cap and rule book. The engine runs on a
    /// thread of **its own** (a one-thread pool) — the shape of tests that build a single
    /// engine. The node builds engines on the shared pool with [`Self::spawn_in_pool`].
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_transport(
        factory: EngineFactory,
        registry: Registry,
        tick_interval: Option<Duration>,
        readiness: Readiness,
        transport: Transport,
        limits: TurnLimits,
        rules: Option<RuleStore>,
    ) -> Result<Self> {
        let pool = EnginePool::new(1).map_err(|e| ActorError::Spawn(e.to_string()))?;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();
        let (tx, retiring, sealed_epoch, waiters) = Self::submit_loop(
            &pool,
            factory,
            registry,
            tick_interval,
            readiness,
            transport,
            limits,
            rules,
            ReadyTx::Sync(ready_tx),
        )?;
        // `pool` drops here: its thread exits when this engine ends.
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                tx,
                retiring,
                sealed_epoch,
                waiters,
            }),
            Ok(Err(e)) => Err(ActorError::Spawn(e)),
            Err(_) => Err(ActorError::Stopped),
        }
    }

    /// Build the engine on `pool` and wait — **without blocking the thread** — until it is
    /// ready according to `readiness`.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_in_pool(
        pool: &EnginePool,
        factory: EngineFactory,
        registry: Registry,
        tick_interval: Option<Duration>,
        readiness: Readiness,
        transport: Transport,
        limits: TurnLimits,
        rules: Option<RuleStore>,
    ) -> Result<Self> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (tx, retiring, sealed_epoch, waiters) = Self::submit_loop(
            pool,
            factory,
            registry,
            tick_interval,
            readiness,
            transport,
            limits,
            rules,
            ReadyTx::Async(ready_tx),
        )?;
        match ready_rx.await {
            Ok(Ok(())) => Ok(Self {
                tx,
                retiring,
                sealed_epoch,
                waiters,
            }),
            Ok(Err(e)) => Err(ActorError::Spawn(e)),
            Err(_) => Err(ActorError::Stopped),
        }
    }

    /// Send a range's lifecycle to a thread of `pool`. The `!Send` engine is built **on**
    /// that thread (a `Send` closure carries the factory over) and lives there for good, in
    /// the thread's `LocalSet` — shared with other ranges.
    #[allow(clippy::too_many_arguments)]
    fn submit_loop(
        pool: &EnginePool,
        factory: EngineFactory,
        registry: Registry,
        tick_interval: Option<Duration>,
        readiness: Readiness,
        transport: Transport,
        limits: TurnLimits,
        rules: Option<RuleStore>,
        ready: ReadyTx,
    ) -> Result<LoopParts> {
        let (tx, rx) = mpsc::channel::<Command>(64);
        let retiring = Arc::new(AtomicBool::new(false));
        let loop_retiring = Arc::clone(&retiring);
        let sealed_epoch = Arc::new(AtomicU64::new(0));
        let loop_sealed_epoch = Arc::clone(&sealed_epoch);
        let waiters = Waiters::default();
        let loop_waiters = waiters.clone();
        let budget = pool.turn_budget().cloned();
        let stats = pool.range_stats().clone();
        let handlers = pool.handler_stats().clone();
        let load = pool.load_book().clone();
        let streams = pool.stream_book().clone();
        let query_ranges = pool.query_ranges().clone();
        let egress = Arc::clone(pool.egress());
        let identity = pool.identity().clone();
        let value_urls = Arc::clone(pool.value_urls());
        pool.spawn(Box::new(move || {
            Box::pin(cairn_store::io_source::tagged(
                "engine",
                leadership_loop(
                    factory,
                    registry,
                    transport,
                    rules,
                    LoopOptions {
                        tick_interval,
                        readiness,
                        limits: TurnLimits {
                            max_concurrent_turns: limits.max_concurrent_turns.max(1),
                            ..limits
                        },
                        retiring: loop_retiring,
                        sealed_epoch: loop_sealed_epoch,
                        waiters: loop_waiters,
                        budget,
                        stats,
                        handlers,
                        load,
                        streams,
                        query_ranges,
                        egress,
                        identity,
                        value_urls,
                    },
                    rx,
                    ready,
                ),
            ))
        }))
        .map_err(|_| ActorError::Spawn("engine thread is dead".into()))?;
        Ok((tx, retiring, sealed_epoch, waiters))
    }

    /// The registry of outcome waiters of this range — only effective on the node leading
    /// the range: the leader's commits notify it.
    pub fn waiters(&self) -> &Waiters {
        &self.waiters
    }

    /// Both handles talk to the same range lifecycle.
    pub fn same(&self, other: &EngineHandle) -> bool {
        self.tx.same_channel(&other.tx)
    }

    async fn ask<T>(&self, make: impl FnOnce(oneshot::Sender<Result<T>>) -> Command) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(make(tx))
            .await
            .map_err(|_| ActorError::Stopped)?;
        rx.await.map_err(|_| ActorError::Stopped)?
    }

    pub async fn invoke(
        &self,
        id: InvocationId,
        target: Target,
        limit_key: Option<LimitKey>,
        idempotency_key: Option<String>,
        input: Bytes,
    ) -> Result<InvokeOutcome> {
        self.invoke_logged(
            id,
            target,
            limit_key,
            idempotency_key,
            input,
            None,
            Vec::new(),
        )
        .await
    }

    /// Like [`Self::invoke`], plus the HTTP request's checked `cairnflow-log-level` header —
    /// goes into entry 0.
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_logged(
        &self,
        id: InvocationId,
        target: Target,
        limit_key: Option<LimitKey>,
        idempotency_key: Option<String>,
        input: Bytes,
        log_level: Option<String>,
        tags: Vec<(String, String)>,
    ) -> Result<InvokeOutcome> {
        self.ask(|reply| Command::Invoke {
            id,
            target,
            limit_key,
            idempotency_key,
            input,
            log_level,
            tags,
            reply,
        })
        .await
    }

    /// Create the invocation and answer right away; the first turn runs behind the caller's
    /// back.
    ///
    /// `Waiting` = just created (or queued). Any other outcome means the idempotency key
    /// already points at an existing invocation and we return its result — `/send` must
    /// honour idempotency too.
    pub async fn submit(
        &self,
        id: InvocationId,
        target: Target,
        limit_key: Option<LimitKey>,
        idempotency_key: Option<String>,
        input: Bytes,
    ) -> Result<InvokeOutcome> {
        self.submit_logged(
            id,
            target,
            limit_key,
            idempotency_key,
            input,
            None,
            Vec::new(),
        )
        .await
    }

    /// Like [`Self::submit`], plus the HTTP request's checked `cairnflow-log-level` header —
    /// goes into entry 0.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit_logged(
        &self,
        id: InvocationId,
        target: Target,
        limit_key: Option<LimitKey>,
        idempotency_key: Option<String>,
        input: Bytes,
        log_level: Option<String>,
        tags: Vec<(String, String)>,
    ) -> Result<InvokeOutcome> {
        self.ask(|reply| Command::Submit {
            id,
            target,
            limit_key,
            idempotency_key,
            input,
            log_level,
            tags,
            reply,
        })
        .await
    }

    /// Number of invocations this tick managed to run on.
    pub async fn tick(&self, now_ms: u64) -> Result<usize> {
        self.ask(|reply| Command::Tick { now_ms, reply }).await
    }

    /// Report that the deployment registry just changed.
    pub async fn deployments_changed(&self) -> Result<()> {
        self.ask(|reply| Command::DeploymentsChanged { reply })
            .await
    }

    pub async fn status(&self, id: InvocationId) -> Result<Option<InvocationStatus>> {
        self.ask(|reply| Command::Status { id, reply }).await
    }

    /// The state of `id` **if it has stopped for good** (`completed`/`failed`/`paused`);
    /// `None` = not yet, or missing. For a poll loop that returns nothing to the outside
    /// before completion: a `None` answer does not go through the read fence, so it **must
    /// not** be handed to anyone as a fact about the state.
    pub async fn settled(&self, id: InvocationId) -> Result<Option<InvocationStatus>> {
        self.ask(|reply| Command::Settled { id, reply }).await
    }

    /// Target (when `target`), state and the latest attempt's error of `id` — one turn, one
    /// read fence. See [`InvocationView`].
    pub async fn view(&self, id: InvocationId, target: bool) -> Result<InvocationView> {
        self.ask(|reply| Command::View { id, target, reply }).await
    }

    /// The error of the latest failed attempt of `id`, if any.
    pub async fn last_failure(
        &self,
        id: InvocationId,
    ) -> Result<Option<cairn_store::keys::LastFailure>> {
        self.ask(|reply| Command::LastFailure { id, reply }).await
    }

    /// The target of invocation `id` (entry 0). `None` = no such invocation.
    pub async fn head(&self, id: InvocationId) -> Result<Option<Target>> {
        self.ask(|reply| Command::Head { id, reply }).await
    }

    /// The raw journal: `(entry_index, message_type, payload)`.
    pub async fn journal(&self, id: InvocationId) -> Result<Vec<JournalRow>> {
        self.ask(|reply| Command::Journal { id, reply }).await
    }

    /// One page of the raw journal — see `Engine::journal_page`.
    pub async fn journal_page(
        &self,
        id: InvocationId,
        from: u32,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(Vec<JournalRow>, Option<u32>)> {
        self.ask(|reply| Command::JournalPage {
            id,
            from,
            max_entries,
            max_bytes,
            reply,
        })
        .await
    }

    /// Open `id`'s stream for reading — see [`StreamAccess`]. Leader only.
    pub async fn open_stream(&self, id: InvocationId) -> Result<StreamAccess> {
        self.ask(|reply| Command::OpenStream { id, reply }).await
    }

    /// Chunks of durable stream `name` from `from`, read from the Db — see
    /// [`DurablePage`]. Leader only.
    pub async fn read_stream(
        &self,
        id: InvocationId,
        name: String,
        from: u64,
        max_bytes: usize,
    ) -> Result<DurablePage> {
        self.ask(|reply| Command::ReadStream {
            id,
            name,
            from,
            max_bytes,
            reply,
        })
        .await
    }

    /// The artifact rows of `id` — leader only.
    pub async fn artifacts(&self, id: InvocationId) -> Result<super::ArtifactLookup> {
        self.ask(|reply| Command::Artifacts { id, reply }).await
    }

    /// The first half of the artifact deletion proof of `owner` — only the leader of the
    /// range holding the id.
    pub async fn artifacts_unreferenced(
        &self,
        owner: cairn_store::artifact::Owner,
    ) -> Result<super::UnreferencedArtifacts> {
        self.ask(|reply| Command::ArtifactsUnreferenced { owner, reply })
            .await
    }

    /// The first half of the segment deletion proof of `(tenant, pk, owner)` — only the
    /// leader of the range holding `pk`.
    pub async fn values_referenced(
        &self,
        tenant: String,
        pk: cairn_store::PartitionKey,
        owner: cairn_store::value::Owner,
    ) -> Result<super::ReferencedValues> {
        self.ask(|reply| Command::ValuesReferenced {
            tenant,
            pk,
            owner,
            reply,
        })
        .await
    }

    /// Commit `muts` directly under the engine of the range holding `pk` (`testkit` only).
    #[cfg(feature = "testkit")]
    pub async fn raw_commit(
        &self,
        pk: cairn_store::PartitionKey,
        muts: cairn_store::Mutations,
    ) -> Result<()> {
        self.ask(|reply| Command::RawCommit { pk, muts, reply })
            .await
    }

    /// One batch of row deletes of a purging tenant — returns once the batch is durable.
    /// See [`cairn_engine::Engine::purge_tenant_rows`].
    pub async fn purge_tenant_rows(
        &self,
        grant: cairn_store::proof::TenantPurgeGrant,
        from: Option<cairn_engine::TenantPurgeCursor>,
        budget: usize,
    ) -> Result<cairn_engine::TenantRowsStep> {
        self.ask(|reply| Command::PurgeTenantRows {
            grant,
            from,
            budget,
            check: false,
            reply,
        })
        .await
    }

    /// One **check** batch of a purging tenant's rows: deletes nothing, stops at the first
    /// row. See [`cairn_engine::Engine::find_tenant_rows`].
    pub async fn find_tenant_rows(
        &self,
        grant: cairn_store::proof::TenantPurgeGrant,
        from: Option<cairn_engine::TenantPurgeCursor>,
        budget: usize,
    ) -> Result<cairn_engine::TenantRowsStep> {
        self.ask(|reply| Command::PurgeTenantRows {
            grant,
            from,
            budget,
            check: true,
            reply,
        })
        .await
    }

    /// Cancel an invocation. See [`cairn_engine::CancelOutcome`].
    pub async fn cancel(&self, id: InvocationId) -> Result<cairn_engine::CancelOutcome> {
        self.ask(|reply| Command::Cancel { id, reply }).await
    }

    /// Purge a finished invocation right away. See [`cairn_engine::PurgeOutcome`].
    pub async fn purge(&self, id: InvocationId) -> Result<cairn_engine::PurgeOutcome> {
        self.ask(|reply| Command::Purge { id, reply }).await
    }

    /// Cooperative cancel through the CANCEL signal. `None` = no such invocation.
    pub async fn cancel_signal(&self, id: InvocationId) -> Result<Option<InvocationStatus>> {
        self.ask(|reply| Command::CancelSignal { id, reply }).await
    }

    /// Complete a promise from outside the system, then drain so the waiters are woken.
    pub async fn complete_promise(
        &self,
        tenant: TenantId,
        scope: Option<Scope>,
        service: String,
        object_key: String,
        name: String,
        result: cairn_engine::ReplyResult,
    ) -> Result<cairn_engine::PromiseOutcome> {
        self.ask(|reply| Command::CompletePromise {
            tenant,
            scope,
            service,
            object_key,
            name,
            result,
            reply,
        })
        .await
    }

    /// Unstall, then run on. See [`cairn_engine::ResumeOutcome`].
    pub async fn resume(&self, id: InvocationId) -> Result<cairn_engine::ResumeOutcome> {
        self.ask(|reply| Command::Resume { id, reply }).await
    }

    /// `restart-as-new`: the new copy keeps the first `keep` entries of `id`.
    pub async fn restart(
        &self,
        id: InvocationId,
        keep: u32,
        deployment: Option<String>,
    ) -> Result<cairn_engine::RestartOutcome> {
        self.restart_patched(id, keep, deployment, None).await
    }

    /// Like [`Self::restart`], plus an optional input patch. Until the feature gate opens,
    /// a present `patch` ⇒ [`crate::actor::ActorError::Rejected`]
    /// (`turn::admit::Source::Restart`).
    pub async fn restart_patched(
        &self,
        id: InvocationId,
        keep: u32,
        deployment: Option<String>,
        patch: Option<crate::turn::admit::Patch>,
    ) -> Result<cairn_engine::RestartOutcome> {
        self.ask(|reply| Command::Restart {
            id,
            keep,
            deployment,
            patch,
            reply,
        })
        .await
    }

    /// Move the invocation to deployment `to`. `false` = missing, or already done.
    pub async fn migrate(&self, id: InvocationId, to: String) -> Result<bool> {
        self.ask(|reply| Command::Migrate { id, to, reply }).await
    }

    /// A schedule management command on this range.
    pub async fn schedule(
        &self,
        op: crate::schedules::ScheduleOp,
    ) -> Result<crate::schedules::ScheduleAnswer> {
        self.ask(|reply| Command::Schedule { op, reply }).await
    }

    /// Read a promise. `None` = no result yet.
    pub async fn peek_promise(
        &self,
        tenant: TenantId,
        scope: Option<Scope>,
        service: String,
        object_key: String,
        name: String,
    ) -> Result<Option<cairn_engine::ReplyResult>> {
        self.ask(|reply| Command::PeekPromise {
            tenant,
            scope,
            service,
            object_key,
            name,
            reply,
        })
        .await
    }

    /// Deliver a signal from outside the system. `None` = no such invocation.
    pub async fn external_signal(
        &self,
        id: InvocationId,
        signal: cairn_engine::SignalId,
        value: cairn_engine::CompletionPayload,
    ) -> Result<Option<InvocationStatus>> {
        self.ask(|reply| Command::ExternalSignal {
            id,
            signal,
            value,
            reply,
        })
        .await
    }

    /// List the invocations of this partition.
    ///
    /// `status` filters **inside** the engine, before `limit` cuts — see
    /// [`cairn_engine::Engine::list_invocations`].
    pub async fn list_invocations(
        &self,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<InvocationRow>> {
        let filter = cairn_engine::ListFilter::status(status);
        let rows = self
            .ask(|reply| Command::ListInvocations {
                limit,
                filter,
                detailed: false,
                reply,
            })
            .await?;
        Ok(rows
            .rows
            .into_iter()
            .map(|r| (r.id, r.status, r.target))
            .collect())
    }

    /// Like [`Self::list_invocations`], plus deployment and attempt for each row.
    pub async fn list_invocations_detailed(
        &self,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<InvocationListing>> {
        Ok(self
            .list_invocations_where(limit, cairn_engine::ListFilter::status(status))
            .await?
            .rows)
    }

    /// Like [`Self::list_invocations_detailed`], with the full set of filters.
    pub async fn list_invocations_where(
        &self,
        limit: usize,
        filter: cairn_engine::ListFilter,
    ) -> Result<ListAnswer> {
        self.ask(|reply| Command::ListInvocations {
            limit,
            filter,
            detailed: true,
            reply,
        })
        .await
    }

    /// Number of unfinished invocations of this range by deployment.
    pub async fn pinned_counts(&self) -> Result<std::collections::BTreeMap<String, usize>> {
        self.ask(|reply| Command::PinnedCounts { reply }).await
    }

    /// Voluntarily hand over leadership. A no-op if not holding it.
    pub async fn resign(&self) -> Result<()> {
        self.ask(|reply| Command::Resign { reply }).await
    }

    /// Receive a message from another partition. Returns once the message **is durable**.
    ///
    /// The sender may only delete its outbox row after this function returns `Ok` — that
    /// is INV-5 applied at the boundary between two partitions.
    pub async fn inbound(&self, msg: InterPartition) -> Result<()> {
        self.ask(|reply| Command::Inbound {
            msg: Box::new(msg),
            reply,
        })
        .await
    }

    /// Receive a **batch** of messages from another partition, every message belonging to
    /// this range. Returns once **the whole batch** is durable in one commit; an error ⇒ no
    /// message of the batch is written. An empty batch ⇒ `Ok` right away.
    pub async fn inbound_batch(&self, msgs: Vec<InterPartition>) -> Result<()> {
        if msgs.is_empty() {
            return Ok(());
        }
        self.ask(|reply| Command::InboundBatch { msgs, reply })
            .await
    }

    /// Snapshot of this partition's flow control.
    pub async fn flow(&self) -> Result<crate::flow::FlowSnapshot> {
        self.ask(|reply| Command::Flow { reply }).await
    }

    /// Push the new rule book into the scheduler right away.
    pub async fn set_rules(&self, book: cairn_store::RuleBook) -> Result<()> {
        self.ask(|reply| Command::SetRules {
            book: Box::new(book),
            reply,
        })
        .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.ask(|reply| Command::Shutdown { reply }).await
    }

    /// Mark this engine's range **as retiring** (D23): from now on every command about a key
    /// — call, lookup, cancel, cross-partition message — gets `WrongRange` instead of being
    /// carried out on a Db about to be dropped. Commands already running run to the end.
    ///
    /// Today the node calls it when the range leaves the table, right before shutting the
    /// engine down; the split's quiesce will call it at its "stop taking work" step.
    pub fn mark_retiring(&self) {
        self.retiring.store(true, Ordering::Release);
    }

    /// **Quiesce** (the quiesce step of a split): stop taking work — commands aimed at a key
    /// get `WrongRange` —, wait for work in flight for at most `grace`, then cancel the rest
    /// as on stepping down, seal (flush the WAL, flush the memtable, close), and answer
    /// **after the Db is closed**. The lease is still held: no node reopens the Db while the
    /// caller is cloning it.
    ///
    /// Afterwards the engine stays quiesced: [`Self::retire`] retires the range;
    /// [`Self::resign`] drops the quiesce (split rolled back) — releases the lease and wins
    /// it back as usual; [`Self::shutdown`] stops.
    ///
    /// `Err(SealFailed)` = the Db is **not certainly closed**: it must not be cloned.
    /// `Err(NotLeader)` = this node does not lead the range.
    pub async fn quiesce(&self, grace: Duration) -> Result<()> {
        self.ask(|reply| Command::Quiesce {
            grace,
            holder: None,
            reply,
        })
        .await
    }

    /// Like [`Self::quiesce`], tied to the coordinator's lifetime (SM-15): when `holder`'s
    /// sending end drops — the coordinator finished without `Retire`/`Resign`, panicked, or
    /// its task was cancelled — the engine leaves quiesce as on receiving `Resign`. Without
    /// this tether, a coordinator that vanishes would leave the source range with its Db
    /// closed, holding the lease and returning `WrongRange` forever.
    pub async fn quiesce_held(&self, grace: Duration, holder: QuiesceHold) -> Result<()> {
        self.ask(|reply| Command::Quiesce {
            grace,
            holder: Some(holder),
            reply,
        })
        .await
    }

    /// Flush the range's memtable to L0 **while still serving** — the bulk of the closing
    /// work, done before the split's write-stop window.
    pub async fn flush_memtable(&self) -> Result<()> {
        self.ask(|reply| Command::FlushMemtable {
            fresh: false,
            reply,
        })
        .await
    }

    /// [`Self::flush_memtable`] that always stores a fresh manifest (tenant purge phase B,
    /// G32).
    pub async fn flush_memtable_fresh(&self) -> Result<()> {
        self.ask(|reply| Command::FlushMemtable { fresh: true, reply })
            .await
    }

    /// Wake the range Db's compactor so the compaction just requested through `Admin` runs
    /// right away, without waiting for the poll tick (tenant purge phase B).
    pub async fn wake_compactor(&self) -> Result<()> {
        self.ask(|reply| Command::WakeCompactor { reply }).await
    }

    /// **Retire** (the retire step of a split): quiesce if not done yet, write the
    /// tombstone (`retired`) on the owner record, then leave the leadership loop **for
    /// good** — nobody takes this range back. Not leading the range ⇒ just exit.
    pub async fn retire(&self) -> Result<()> {
        self.ask(|reply| Command::Retire { reply }).await
    }

    /// `writer_epoch` of the writer the engine sealed **most recently** — after
    /// [`Self::quiesce_held`] returns `Ok`, that is the writer just quiesced. The
    /// coordinator closes the source conditional on this epoch: if someone reopened the Db
    /// between the quiesce and the close command, the epoch differs, the close fails and the
    /// operation rolls back. `None` = the engine has not sealed any writer yet.
    pub fn sealed_writer_epoch(&self) -> Option<u64> {
        match self.sealed_epoch.load(Ordering::Acquire) {
            0 => None,
            e => Some(e),
        }
    }

    pub fn is_retiring(&self) -> bool {
        self.retiring.load(Ordering::Acquire)
    }

    /// The engine's leadership loop has exited (command channel closed): every command gets
    /// `Stopped`. A normal exit only happens on shutdown or retirement
    /// ([`Self::is_retiring`]); otherwise it is the job of a dead range (a panic).
    pub fn is_dead(&self) -> bool {
        self.tx.is_closed()
    }
}

/// A human's command with an audit note — [`EngineHandle::audited`]. A `None` note (router
/// built by hand, auth disabled with no identity) ⇒ like calling [`EngineHandle`] directly.
pub struct Audited<'a> {
    handle: &'a EngineHandle,
    note: Option<crate::audit::AuditNote>,
}

impl EngineHandle {
    /// Send the next command with audit note `note`: the `Table::Audit` row goes with the
    /// action's commit (`Command::Audited`).
    pub fn audited(&self, note: Option<crate::audit::AuditNote>) -> Audited<'_> {
        Audited { handle: self, note }
    }
}

impl Audited<'_> {
    async fn ask<T>(self, make: impl FnOnce(oneshot::Sender<Result<T>>) -> Command) -> Result<T> {
        match self.note {
            None => self.handle.ask(make).await,
            Some(note) => {
                self.handle
                    .ask(|reply| Command::Audited {
                        note: Box::new(note),
                        inner: Box::new(make(reply)),
                    })
                    .await
            }
        }
    }

    pub async fn cancel(self, id: InvocationId) -> Result<cairn_engine::CancelOutcome> {
        self.ask(|reply| Command::Cancel { id, reply }).await
    }

    pub async fn purge(self, id: InvocationId) -> Result<cairn_engine::PurgeOutcome> {
        self.ask(|reply| Command::Purge { id, reply }).await
    }

    pub async fn cancel_signal(self, id: InvocationId) -> Result<Option<InvocationStatus>> {
        self.ask(|reply| Command::CancelSignal { id, reply }).await
    }

    pub async fn resume(self, id: InvocationId) -> Result<cairn_engine::ResumeOutcome> {
        self.ask(|reply| Command::Resume { id, reply }).await
    }

    pub async fn restart_patched(
        self,
        id: InvocationId,
        keep: u32,
        deployment: Option<String>,
        patch: Option<crate::turn::admit::Patch>,
    ) -> Result<cairn_engine::RestartOutcome> {
        self.ask(|reply| Command::Restart {
            id,
            keep,
            deployment,
            patch,
            reply,
        })
        .await
    }

    pub async fn migrate(self, id: InvocationId, to: String) -> Result<bool> {
        self.ask(|reply| Command::Migrate { id, to, reply }).await
    }

    pub async fn schedule(
        self,
        op: crate::schedules::ScheduleOp,
    ) -> Result<crate::schedules::ScheduleAnswer> {
        self.ask(|reply| Command::Schedule { op, reply }).await
    }

    pub async fn external_signal(
        self,
        id: InvocationId,
        signal: cairn_engine::SignalId,
        value: cairn_engine::CompletionPayload,
    ) -> Result<Option<InvocationStatus>> {
        self.ask(|reply| Command::ExternalSignal {
            id,
            signal,
            value,
            reply,
        })
        .await
    }
}
