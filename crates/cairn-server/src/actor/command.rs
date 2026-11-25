// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Commands sent into the engine thread: [`Command`] and its reply part.

use super::*;

pub(super) enum Command {
    /// A human's `inner` command (cancel, purge, unstall, restart, signal, schedule) with
    /// an audit note: `inner`'s task carries the note in `Shared`/`Ctx`, and the `turn`
    /// function stages the `Table::Audit` row right after taking the target entity's lock
    /// — in the same commit as the action.
    Audited {
        note: Box<crate::audit::AuditNote>,
        inner: Box<Command>,
    },
    Invoke {
        id: InvocationId,
        target: Target,
        /// Only affects queueing — **not** part of the identity.
        limit_key: Option<LimitKey>,
        /// Idempotency from the HTTP door: a duplicate call attaches to the invocation
        /// holding the key.
        idempotency_key: Option<String>,
        input: Bytes,
        /// The checked `cairnflow-log-level` header.
        log_level: Option<String>,
        /// Checked user tags.
        tags: Vec<(String, String)>,
        reply: oneshot::Sender<Result<InvokeOutcome>>,
    },
    /// Create the invocation, then **answer right away** and run later — the `/send` door.
    ///
    /// Answers right between "durable" and "ran": earlier, the id given to the client could
    /// not be looked up yet; later, a slow handler would hold the request of someone who only
    /// wants to send and move on.
    Submit {
        id: InvocationId,
        target: Target,
        limit_key: Option<LimitKey>,
        idempotency_key: Option<String>,
        input: Bytes,
        /// The checked `cairnflow-log-level` header.
        log_level: Option<String>,
        /// Checked user tags.
        tags: Vec<(String, String)>,
        reply: oneshot::Sender<Result<InvokeOutcome>>,
    },
    /// Fire the timers that are due, then run on the invocations concerned.
    Tick {
        now_ms: u64,
        reply: oneshot::Sender<Result<usize>>,
    },
    /// The deployment registry just changed — run on the invocations waiting for it.
    ///
    /// Carries no data: deployments live at cluster level, and the [`Registry`] has been
    /// updated before this command is sent. The only job here is *waking*.
    DeploymentsChanged {
        reply: oneshot::Sender<Result<()>>,
    },
    Status {
        id: InvocationId,
        reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
    },
    /// The state **only once it has stopped for good** (`completed`/`failed`/`paused`) —
    /// the poll loop `/invoke` blocks on (`ingress/invoke.rs:wait_for_settled`). `None` =
    /// not yet (or missing): that answer is thrown away and seen by nobody, so it does not go
    /// through the read fence; a "done" answer does.
    Settled {
        id: InvocationId,
        reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
    },
    /// Target (when `target`), state and the latest attempt's error in one turn, one read
    /// fence — `GET /invocations/{id}`.
    View {
        id: InvocationId,
        target: bool,
        reply: oneshot::Sender<Result<InvocationView>>,
    },
    /// The error of the latest failed attempt — `GET /invocations/{id}`.
    LastFailure {
        id: InvocationId,
        reply: oneshot::Sender<Result<Option<cairn_store::keys::LastFailure>>>,
    },
    /// The target of an invocation, read from entry 0 — ingress needs it to know which
    /// tenant the invocation belongs to before answering by id.
    Head {
        id: InvocationId,
        reply: oneshot::Sender<Result<Option<Target>>>,
    },
    Journal {
        id: InvocationId,
        reply: oneshot::Sender<Result<Vec<JournalRow>>>,
    },
    /// A page of the journal: from `from`, at most `max_entries` entries and `max_bytes`
    /// bytes; with the next entry index when more remain.
    JournalPage {
        id: InvocationId,
        from: u32,
        max_entries: usize,
        max_bytes: usize,
        reply: oneshot::Sender<Result<(Vec<JournalRow>, Option<u32>)>>,
    },
    /// Open `id`'s stream for reading: the term's hub, the target and the state — only the
    /// leader can answer (AP-13: no follower path).
    OpenStream {
        id: InvocationId,
        reply: oneshot::Sender<Result<StreamAccess>>,
    },
    /// Read chunks of durable stream `name` from the Db — leader only (AP-13): the part
    /// that has left RAM, or a stream the term has never seen.
    ReadStream {
        id: InvocationId,
        name: String,
        from: u64,
        max_bytes: usize,
        reply: oneshot::Sender<Result<DurablePage>>,
    },
    /// The artifact rows of `id` — leader only, like [`Self::ReadStream`].
    Artifacts {
        id: InvocationId,
        reply: oneshot::Sender<Result<ArtifactLookup>>,
    },
    /// The first half of the artifact deletion proof for one lifetime (the artifact
    /// sweep): reads the range's Db under the engine.
    ArtifactsUnreferenced {
        owner: cairn_store::artifact::Owner,
        reply: oneshot::Sender<Result<UnreferencedArtifacts>>,
    },
    /// The first half of the segment deletion proof for one owner (the values sweep):
    /// reads the range's Db under the engine.
    ValuesReferenced {
        tenant: String,
        pk: cairn_store::PartitionKey,
        owner: cairn_store::value::Owner,
        reply: oneshot::Sender<Result<ReferencedValues>>,
    },
    /// Commit a batch directly under the engine of the range holding `pk` — only for tests
    /// building state that product paths do not build yet (holder rows from before the
    /// sealing path existed).
    #[cfg(feature = "testkit")]
    RawCommit {
        pk: cairn_store::PartitionKey,
        muts: cairn_store::Mutations,
        reply: oneshot::Sender<Result<()>>,
    },
    /// One batch of logical deletes of a purging tenant's rows: the grant is built outside
    /// the engine (reading the bucket) and spent here. A command for the whole range, no
    /// lock.
    PurgeTenantRows {
        grant: cairn_store::proof::TenantPurgeGrant,
        from: Option<cairn_engine::TenantPurgeCursor>,
        budget: usize,
        /// `true` = a **check** batch, deletes nothing
        /// ([`cairn_engine::Engine::find_tenant_rows`]).
        check: bool,
        reply: oneshot::Sender<Result<cairn_engine::TenantRowsStep>>,
    },
    /// Cancel an invocation (admin API). Needs write authority: the state changes and the
    /// waiters receive the result.
    Cancel {
        id: InvocationId,
        reply: oneshot::Sender<Result<cairn_engine::CancelOutcome>>,
    },
    /// Purge a finished invocation right away (admin API), without waiting for retention.
    Purge {
        id: InvocationId,
        reply: oneshot::Sender<Result<cairn_engine::PurgeOutcome>>,
    },
    /// Cooperative cancel: deliver the CANCEL signal, the SDK cleans up by itself. Returns
    /// the state afterwards.
    CancelSignal {
        id: InvocationId,
        reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
    },
    /// Complete a promise from **outside the system** — the human-in-the-loop door.
    CompletePromise {
        tenant: TenantId,
        scope: Option<Scope>,
        service: String,
        object_key: String,
        name: String,
        result: cairn_engine::ReplyResult,
        reply: oneshot::Sender<Result<cairn_engine::PromiseOutcome>>,
    },
    /// Take an invocation out of `Paused` and run it on.
    Resume {
        id: InvocationId,
        reply: oneshot::Sender<Result<cairn_engine::ResumeOutcome>>,
    },
    /// `restart-as-new` (5.3).
    Restart {
        id: InvocationId,
        keep: u32,
        deployment: Option<String>,
        /// The input patch (not enabled yet — `turn::admit::Source::Restart`).
        patch: Option<crate::turn::admit::Patch>,
        reply: oneshot::Sender<Result<cairn_engine::RestartOutcome>>,
    },
    /// Move an unfinished invocation to another deployment (after a test replay).
    Migrate {
        id: InvocationId,
        to: String,
        reply: oneshot::Sender<Result<bool>>,
    },
    /// Read a promise without needing any invocation — introspection.
    PeekPromise {
        tenant: TenantId,
        scope: Option<Scope>,
        service: String,
        object_key: String,
        name: String,
        reply: oneshot::Sender<Result<Option<cairn_engine::ReplyResult>>>,
    },
    /// Deliver a signal from **outside the system** to a waiting invocation.
    ExternalSignal {
        id: InvocationId,
        signal: cairn_engine::SignalId,
        value: cairn_engine::CompletionPayload,
        reply: oneshot::Sender<Result<Option<InvocationStatus>>>,
    },
    /// List the partition's invocations — introspection.
    ListInvocations {
        limit: usize,
        /// Filters, applied in the engine before `limit` (state, id prefix, service,
        /// handler).
        filter: cairn_engine::ListFilter,
        /// Also read deployment + attempt for each row (two more GETs per row).
        detailed: bool,
        reply: oneshot::Sender<Result<ListAnswer>>,
    },
    /// Managing server-side schedules — see [`crate::schedules`]. Routed by the schedule's
    /// pk; `List` has no key (asks every range).
    Schedule {
        op: crate::schedules::ScheduleOp,
        reply: oneshot::Sender<Result<crate::schedules::ScheduleAnswer>>,
    },
    /// Number of unfinished invocations by the deployment they are bound to.
    PinnedCounts {
        reply: oneshot::Sender<Result<std::collections::BTreeMap<String, usize>>>,
    },
    /// Voluntarily hand over the partition's leadership — used by leadership balancing.
    ///
    /// Goes through the actor rather than touching the keeper directly, so stepping down
    /// follows the existing order: seal the writer, release the lease (write a tombstone),
    /// then the re-acquisition loop runs by itself — where the fair share keeps it from
    /// competing for this very partition again.
    Resign {
        reply: oneshot::Sender<Result<()>>,
    },
    /// A message from another partition. Write it durably, then ack — **not** processed
    /// here.
    ///
    /// Processing it synchronously while the sender waits would deadlock two engine threads
    /// on a call cycle. See `cairn_store::Table::Inbound`.
    Inbound {
        msg: Box<InterPartition>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// A **batch** of messages from another partition: write the whole batch in one commit,
    /// then ack once — all or nothing. Like [`Self::Inbound`], not processed here. Every
    /// message must belong to this range (`within`); an empty batch never reaches the actor.
    InboundBatch {
        msgs: Vec<InterPartition>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Snapshot of the partition's flow control — `GET /limits|/vqueues|/scheduler`.
    ///
    /// Read purely from the scheduler's RAM, it does **not** touch the store: queue
    /// statistics are not persisted (see `crate::metrics::flow`).
    Flow {
        reply: oneshot::Sender<Result<crate::flow::FlowSnapshot>>,
    },
    /// The rule book just changed — push it straight into the scheduler instead of waiting
    /// for the poll tick.
    ///
    /// The poll is still the guaranteed path (when another node changes the rules we only
    /// learn it through the poll); this command only cuts the latency for the node *where
    /// the command is typed*, so `rules set` followed right away by `GET /limits` does not
    /// give a stale answer.
    SetRules {
        book: Box<cairn_store::RuleBook>,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Stop taking work, wait for work in flight (bounded), seal — answer **after the Db is
    /// closed** — then stay quiesced with the lease still held (the quiesce step of a split,
    /// SM-4).
    Quiesce {
        grace: Duration,
        /// The coordinator's tether: the sending end dropped without `Retire`/`Resign` ⇒ the
        /// coordinator is gone, and the engine leaves quiesce like `Resign` (see
        /// [`quiesced`]).
        holder: Option<QuiesceHold>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Retire: quiesce if not done yet, write the tombstone on the owner record, exit for
    /// good (the retire step of a split). Answers after the tombstone is written.
    Retire {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Flush the memtable to L0 while still serving (the flush step before a split's
    /// quiesce).
    FlushMemtable {
        /// Write the seal barrier first so the flush always stores a fresh manifest
        /// ([`cairn_store::PartitionWriter::flush_memtable_fresh`], tenant purge phase B).
        fresh: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Wake the Db's compactor so the compaction just requested runs right away (tenant
    /// purge phase B).
    WakeCompactor {
        reply: oneshot::Sender<Result<()>>,
    },
}

impl Command {
    /// Return `NotLeader` to the caller.
    ///
    /// Written out branch by branch rather than wrapped generically: each variant has a
    /// different reply type, and the only way to wrap them generically is another layer of
    /// trait objects around `oneshot::Sender` — far more machinery than it saves.
    /// `Shutdown` is deliberately not here: it is not a command that needs leadership.
    pub(super) fn reject_not_leader(self) {
        self.reject_with(|| ActorError::NotLeader)
    }

    /// Return `WrongRange` to the caller — the command concerns a key outside this engine's
    /// range, or the range is retiring (D23).
    pub(super) fn reject_wrong_range(self) {
        self.reject_with(|| ActorError::WrongRange)
    }

    /// The partition key this command concerns, if any — so the actor checks "is it in our
    /// range" by itself (D23) instead of trusting the HTTP layer to have checked.
    ///
    /// A promise without an invocation: the key is derived from the target by exactly the
    /// ingress routing function (`promise_key`). An unkeyed object, with no scope, has no key
    /// at all (D-a) ⇒ `None`: no range check, and the engine refuses the command with
    /// `NoObjectKey` — it does not guess a made-up key to check.
    pub(super) fn target_key(&self) -> Option<PartitionKey> {
        let of_id = |id: &InvocationId| Some(cairn_store::partition_key_of(id));
        let of_object = |tenant: &TenantId, scope: &Option<Scope>, service: &str, key: &str| {
            cairn_store::partition_key_for_target(tenant, scope.as_ref(), service, key, None)
        };
        match self {
            Self::Audited { inner, .. } => inner.target_key(),
            Self::Invoke { id, .. }
            | Self::Submit { id, .. }
            | Self::Status { id, .. }
            | Self::Settled { id, .. }
            | Self::View { id, .. }
            | Self::LastFailure { id, .. }
            | Self::Head { id, .. }
            | Self::Journal { id, .. }
            | Self::JournalPage { id, .. }
            | Self::OpenStream { id, .. }
            | Self::ReadStream { id, .. }
            | Self::Artifacts { id, .. }
            | Self::Cancel { id, .. }
            | Self::Purge { id, .. }
            | Self::CancelSignal { id, .. }
            | Self::Resume { id, .. }
            | Self::Migrate { id, .. }
            | Self::Restart { id, .. }
            | Self::ExternalSignal { id, .. } => of_id(id),
            Self::CompletePromise {
                tenant,
                scope,
                service,
                object_key,
                ..
            }
            | Self::PeekPromise {
                tenant,
                scope,
                service,
                object_key,
                ..
            } => of_object(tenant, scope, service, object_key),
            Self::ArtifactsUnreferenced { owner, .. } => of_id(&owner.id),
            Self::ValuesReferenced { pk, .. } => Some(*pk),
            #[cfg(feature = "testkit")]
            Self::RawCommit { pk, .. } => Some(*pk),
            Self::Schedule { op, .. } => {
                use crate::schedules::ScheduleOp as O;
                match op {
                    O::Put(p) => Some(cairn_store::partition_key_for_schedule(
                        &p.tenant,
                        p.scope.as_ref(),
                        &p.name,
                    )),
                    O::Get(id)
                    | O::Delete(id)
                    | O::Pause(id)
                    | O::Resume(id)
                    | O::Trigger { id, .. } => of_id(id),
                    O::List { .. } | O::Page { .. } | O::Counts { .. } => None,
                }
            }
            Self::Inbound { msg, .. } => of_id(msg.destination_id()),
            // The key of the first message: a keyed command (quiesce/retire return
            // `WrongRange`); the range check for **each** message is [`Self::within`].
            Self::InboundBatch { msgs, .. } => msgs.first().and_then(|m| of_id(m.destination_id())),
            Self::Tick { .. }
            | Self::DeploymentsChanged { .. }
            | Self::ListInvocations { .. }
            | Self::PinnedCounts { .. }
            | Self::Resign { .. }
            | Self::PurgeTenantRows { .. }
            | Self::Flow { .. }
            | Self::SetRules { .. }
            | Self::Shutdown { .. }
            | Self::Quiesce { .. }
            | Self::Retire { .. }
            | Self::FlushMemtable { .. }
            | Self::WakeCompactor { .. } => None,
        }
    }

    /// Every key this command concerns is within `range` (D23). A keyless command ⇒ `true`.
    /// An inbound batch checks **each** message: one message from another range means the
    /// whole batch is refused, no part of it written.
    pub(super) fn within(&self, range: &RangeEntry) -> bool {
        match self {
            Self::Audited { inner, .. } => inner.within(range),
            Self::InboundBatch { msgs, .. } => msgs
                .iter()
                .all(|m| range.contains_key(cairn_store::partition_key_of(m.destination_id()))),
            _ => self.target_key().is_none_or(|k| range.contains_key(k)),
        }
    }

    /// Return the error `err()` to the caller — shared by `NotLeader` and `WrongRange`.
    /// Commands that need no leadership (`Resign`, `SetRules`, `Shutdown`) count as done.
    fn reject_with(self, err: impl Fn() -> ActorError) {
        match self {
            Self::Audited { inner, .. } => inner.reject_with(err),
            Self::Invoke { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Submit { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Tick { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::DeploymentsChanged { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Status { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Settled { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::View { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::LastFailure { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Head { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::OpenStream { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::ReadStream { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Artifacts { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::ArtifactsUnreferenced { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::ValuesReferenced { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            #[cfg(feature = "testkit")]
            Self::RawCommit { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::PurgeTenantRows { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Journal { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::JournalPage { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Cancel { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Purge { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::CancelSignal { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::CompletePromise { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Resume { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Migrate { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Restart { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::PeekPromise { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::ExternalSignal { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::ListInvocations { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::PinnedCounts { reply } => {
                let _ = reply.send(Err(err()));
            }
            Self::Schedule { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            // Handing over something we do not hold is already done.
            Self::Resign { reply } => {
                let _ = reply.send(Ok(()));
            }
            Self::Inbound { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::InboundBatch { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            Self::Flow { reply } => {
                let _ = reply.send(Err(err()));
            }
            // The rule book is cluster configuration; not leading this partition means there
            // is no scheduler to push it into either. Counts as done.
            Self::SetRules { reply, .. } => {
                let _ = reply.send(Ok(()));
            }
            Self::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
            }
            // Not leading means no Db of ours is open to close — the one asking for quiesce
            // needs to know that, not a "done".
            Self::Quiesce { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            // Not leading: there is no tombstone to write. The caller of this path has
            // already handled `Retire` itself (exit for good); here only the answer remains.
            Self::Retire { reply } => {
                let _ = reply.send(Ok(()));
            }
            Self::FlushMemtable { reply, .. } | Self::WakeCompactor { reply } => {
                let _ = reply.send(Err(err()));
            }
        }
    }
}
