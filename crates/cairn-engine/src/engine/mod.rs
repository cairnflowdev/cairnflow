// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

use {
    crate::{
        interpartition::{CompletionPayload, DeliverSignal, InterPartition, SignalId},
        next_due::NextDue,
        outbox::{
            OutboundCall, OutboundCancel, OutboundReply, OutboundWatch, OutboxEntry, ReplyResult,
        },
        promise::PromiseRecord,
        status::{InvocationStatus, Times},
    },
    bytes::Bytes,
    cairn_protocol::{pb, Message},
    cairn_runtime::{
        Completion, CompletionResult, Effect, Event, InvocationMachine, Journal, JournalEntry,
        MachineError, Outcome, StartParams, Task,
    },
    cairn_store::clock::{Clock, SystemClock},
    cairn_store::entropy::HashMap,
    cairn_store::{
        partition_key_of, ClusterConfig, InboundKind, Mutations, PartitionId, PartitionKey,
        PartitionWriter, QueueName, RangeEntry, RangeStore, Scope, Stage, StoreError, TenantId,
    },
    std::{collections::VecDeque, sync::Arc},
};

mod types;
pub use types::*;
mod step;
pub use step::*;
mod effects;
use effects::*;
mod deliver;
mod due;
mod entry;
mod lifecycle;
mod meta;
mod promise;
mod query_build;
pub use meta::target_of_meta;
pub use query_build::{BuildCursor, BuildStep, HintCursor, IndexState, QUERY_BUILD_CHUNK};
mod query;
pub use query::{lifecycle_slot, lifecycle_stage, lifecycle_status};
mod artifact_bytes;
mod audit;
pub use audit::AuditGuard;
mod schedules;
mod seq;
mod tenant_purge;
mod values;
pub use tenant_purge::{TenantPurgeCursor, TenantRowsStep};
mod vqueue;
mod warm;
pub use entry::*;
pub use vqueue::{RowWrite, StageWrite};
pub use warm::{set_warm_check, warm_check_on, WarmUse, WARM_CAP_BYTES};

/// The driver of one partition. Holds a [`PartitionWriter`] so it is `!Send` —
/// the writer must live in one task, exactly as INV-4 wants.
///
/// Generic over the store of the range Db ([`RangeStore`]) with the default
/// `slatedb::Db`: a bare `Engine` is the product version; tests and the core
/// sim open an `Engine<RangeModel>` on the BTreeMap model (`cairn_store::testkit`).
pub struct Engine<S: RangeStore = cairn_store::SlateDb> {
    writer: PartitionWriter<S>,
    /// The cluster's **current** range table (shared, changeable at run time) —
    /// only for knowing where to send messages. "Is this id ours" is asked of `range`.
    cluster: ClusterConfig,
    /// This engine's range — **fixed for the engine's whole life** (D22): a
    /// split creates new engines, it does not modify the old one. Every "ours
    /// or another range's" comparison is against it, so a table change midway
    /// cannot make an engine take on the wrong work.
    range: RangeEntry,
    /// The derived-zone rebuild at open, if any (mark missing or mismatched) —
    /// a measurement for the split coordinator.
    rebuilt: Option<cairn_store::RebuildStats>,
    /// Fired timers, waiting to be deleted in the next commit. See [`Engine::deliver`].
    pending_timer_removals: Vec<(u64, InvocationId, u32)>,
    /// The nearest deadline of the timed tables (timers, retries, delayed) the
    /// engine knows — every timed-row write and every deadline scan lowers it.
    /// See [`NextDue`].
    next_due: NextDue,
    /// Processed outbox rows, waiting to be deleted in the next commit.
    pending_outbox_removals: Vec<OutboxRef>,
    /// Processed cross-partition messages, waiting to be deleted in the next commit.
    pending_inbound_removals: Vec<InboundKey>,
    /// Delivered signal rows (same-partition target), waiting to be deleted in the next commit.
    pending_signal_removals: Vec<SignalRef>,
    /// `suspended` invocations that were just fed a completion/signal, waiting
    /// to move to `running` in the next commit — the commit that writes the
    /// notification. See
    /// [`Engine::deliver`].
    pending_wakes: Vec<meta::Woken>,
    /// The query-index descriptor table the engine maintains —
    /// [`cairn_store::query_index::QUERY_INDEXES`], unless a test adds trial descriptors.
    query_indexes: &'static [cairn_store::query_index::QueryIndex],
    /// The range's in-RAM query counters.
    query_counts: Arc<cairn_store::query_index::QueryCounts>,
    /// Counters are only built once someone asks (`with_counts_on_demand`).
    counts_on_demand: bool,
    /// The range's live artifact byte counter.
    artifact_bytes: Arc<cairn_store::artifact_bytes::ArtifactBytes>,
    /// The range's billing unit counter — the writer moves the units of each
    /// submitted batch into it; counted only once durable.
    unit_meter: Arc<cairn_store::units::UnitMeter>,
    /// The live view of every read handle the engine has issued.
    query_snapshots: Arc<std::sync::atomic::AtomicUsize>,
    /// The next sequence number to issue for a vqueue entry. `None` = not seeded.
    ///
    /// See [`Engine::next_queue_seq`]: this is a number **rebuildable** from a
    /// scan that runs anyway on becoming leader, not pending work — so it lives
    /// in RAM without breaking INV-3.
    next_seq: Option<u64>,
    /// The `seq` issuer for the outbox and its reverse map. See [`OutboxSeqs`].
    outbox: OutboxSeqs,
    /// The highest `seq` that **an invocation's turn** has produced.
    ///
    /// This is what lets a waiter ask exactly its own question — "has the work
    /// *I* spawned run" — instead of "how far has the whole partition drained".
    /// See [`Self::spawned_after`] for why that distinction is worth a map.
    ///
    /// It must be remembered **after the row has been cut**, so it cannot reuse
    /// `outbox.of`: a `ctx.call`'s row disappears as soon as the dispatch is
    /// done, and "dispatched" is not yet "run".
    spawned: HashMap<InvocationId, u64>,
    /// How long to keep ended invocations before purging them (retention).
    ///
    /// `None` = keep forever. The scheduled purge is written in **the same
    /// commit** as the ending status — see [`cairn_store::Mutations::add_purge`].
    retention_ms: Option<u64>,
    /// Per-tenant limits: the range's own snapshot — see
    /// [`crate::TenantLimitsSource`].
    tenant_limits: Arc<crate::TenantLimits>,
    /// The source of the snapshot and the generation copied. Every `step`
    /// compares the generation (one atomic read); only when it changes is the
    /// source's lock read — the turn path touches no shared lock in steady
    /// state.
    tenant_limits_src: Option<(crate::TenantLimitsSource, u64)>,
    /// Tenants with the hash chain enabled — see [`ChainedTenants`].
    chained: crate::chain::ChainedTenants,
    /// The archive store; `None` = the node has archiving off.
    archive: Option<cairn_store::archive::ArchiveStore>,
    /// The artifact store; `None` = the `streams/table` gate is off —
    /// `ArtifactPut` reaches the machine and is rejected as an unknown code.
    artifacts: Option<cairn_store::artifact::ArtifactStore>,
    /// `--archive-artifacts`: archiving copies artifact bytes, not just refs.
    /// Off by default.
    archive_artifacts: bool,
    /// The range's artifact cache: lives with the engine, so it is lost on a
    /// leader change.
    artifact_cache: Option<std::sync::Arc<cairn_store::artifact::ArtifactCache>>,
    /// `T_put` (AP-9): the deadline from an artifact PUT to the commit of its row.
    artifact_put_deadline: std::time::Duration,
    /// The range's clock: purge deadlines are computed from it, and turns read
    /// wall time through [`Self::clock`] instead of calling the system clock
    /// themselves.
    clock: Arc<dyn Clock>,
    /// The `calls/prefix` gate **as it was at range open**: the spawner reads
    /// `cluster-version` on every open (`ensure_known`) and sets it here — no
    /// store read on the turn path, no network call while holding the engine
    /// (D16). Fixed for the engine's whole life; a gate turned on midway is
    /// picked up by the range at its next open.
    call_prefix: bool,
    /// The `calls/placement` gate as it was at range open — the same path and
    /// the same reason as [`Self::call_prefix`].
    call_placement: bool,
    /// The `streams/table` gate as it was at range open — the same path and the
    /// same reason as [`Self::call_prefix`]: off ⇒ no `Streams` row is written
    /// (durable streams live only in RAM).
    streams: bool,
    /// The `schedules/table` gate as it was at range open — the same path as
    /// [`Self::streams`]: off ⇒ the schedule firing loop does not run and no
    /// `Schedules` row is written.
    schedules: bool,
    /// The `audit/table` gate as it was at range open — see `audit.rs`.
    audit: bool,
    /// The `values/segmented` gate as it was at range open — see `values.rs`.
    /// Off ⇒ the `SegmentedState`/`ValueRefs` tables are empty by construction,
    /// no extra reads.
    values: bool,
    /// The segments the shell just PUT for the frame currently going through
    /// [`Self::step_sealed`] — `(sha, gen)`; empty outside that.
    sealed_fresh: Vec<([u8; 32], u64)>,
    /// Bytes in `sealed_fresh`'s segments that the shell merged from old
    /// segments — they do not count units.
    sealed_old_bytes: u64,
    /// The `values/` segment store — the shell PUTs/GETs through it, outside
    /// the engine (D16). `None` = no gate, no sealing.
    value_store: Option<cairn_store::value::ValueStore>,
    /// The audit row key counter within the term (same `writer_epoch`).
    audit_seq: u64,
    /// Pending audit-row slots per entity — see `audit.rs`.
    audit_slots: Arc<audit::AuditSlots>,
    /// Where audit rows are exported: the main store and the cluster prefix.
    audit_store: Option<cairn_store::audit::AuditStore>,
    /// The next `step`'s first batch is a signal-delivery batch (`deliver_signal`).
    audit_signal_armed: bool,
    /// The caller's warm machines between drainer deliveries of completions —
    /// a cache rebuildable from the journal, like
    /// [`Self::outbox`]. See `warm.rs`.
    warm: warm::Warm,
}

impl<S: RangeStore> Engine<S> {
    /// Opens an engine for a one-partition cluster. A shortcut for tests and
    /// single-node deployments.
    pub async fn open(writer: PartitionWriter<S>) -> Result<Self> {
        Self::open_in_cluster(writer, ClusterConfig::single()).await
    }

    /// Opens an engine for `writer`'s range from `cluster`'s current table. A
    /// range missing from the table ⇒ the engine considers no id its own.
    pub async fn open_in_cluster(
        writer: PartitionWriter<S>,
        cluster: ClusterConfig,
    ) -> Result<Self> {
        let p = writer.partition();
        let range = cluster
            .range(p)
            // An empty range: `start > end` so no key falls in.
            .unwrap_or(RangeEntry {
                start: 1,
                end: 0,
                ..RangeEntry::new(0, 0, p)
            });
        Self::open_in_range(writer, cluster, range).await
    }

    /// Opens an engine for exactly range `range` — the engine's range is fixed from here on.
    ///
    /// **The derived zone is guaranteed to match before returning**: the
    /// `DERIVED_READY` mark must be exactly this range's `(range_id, gen)`;
    /// missing or mismatched ⇒ rebuilt from the hot entity zone right here. A
    /// range newly born from a split or merge (gen changed) always goes through
    /// this path — no shortcut serves a Db whose index belongs to another Db.
    pub async fn open_in_range(
        mut writer: PartitionWriter<S>,
        cluster: ClusterConfig,
        range: RangeEntry,
    ) -> Result<Self> {
        let marker = format!("range={} gen={}", range.id.0, range.gen);
        let rebuilt = writer.ensure_derived(marker.as_bytes()).await?;
        if let Some(r) = &rebuilt {
            tracing::info!(
                range = range.id.0,
                gen = range.gen,
                ms = r.elapsed.as_millis() as u64,
                hot_rows = r.regions.hot,
                derived_rows = r.regions.derived,
                bulk_rows = r.regions.bulk,
                byte = r.bytes,
                "rebuilt derived tables"
            );
        }
        // The range's query counters: the writer's hook applies every
        // `query_event` after a batch is submitted; built by the background
        // build loop.
        let query_counts = Arc::new(cairn_store::query_index::QueryCounts::default());
        writer.set_query_counts(query_counts.clone());
        // The artifact byte counter: same pattern, built by its own background
        // loop only when some tenant carries `max-artifact-bytes=`.
        let artifact_bytes = Arc::new(cairn_store::artifact_bytes::ArtifactBytes::default());
        writer.set_artifact_bytes(artifact_bytes.clone());
        // Billing units: attached at the commits that write an entry, create,
        // end, and new artifact rows (`crate::units`).
        let unit_meter = writer.enable_units();
        Ok(Self {
            rebuilt,
            writer,
            cluster,
            range,
            pending_timer_removals: Vec::new(),
            next_due: NextDue::default(),
            pending_outbox_removals: Vec::new(),
            pending_inbound_removals: Vec::new(),
            pending_signal_removals: Vec::new(),
            pending_wakes: Vec::new(),
            query_indexes: cairn_store::query_index::QUERY_INDEXES,
            query_counts,
            counts_on_demand: false,
            artifact_bytes,
            unit_meter,
            query_snapshots: Arc::default(),
            next_seq: None,
            outbox: OutboxSeqs::default(),
            spawned: HashMap::default(),
            retention_ms: None,
            tenant_limits: Arc::default(),
            tenant_limits_src: None,
            chained: Default::default(),
            archive: None,
            artifacts: None,
            archive_artifacts: false,
            artifact_cache: None,
            artifact_put_deadline: cairn_store::artifact::T_PUT,
            clock: Arc::new(SystemClock),
            call_prefix: false,
            call_placement: false,
            streams: false,
            schedules: false,
            audit: false,
            values: false,
            sealed_fresh: Vec::new(),
            sealed_old_bytes: 0,
            value_store: None,
            audit_seq: 0,
            // Becoming leader: rows of the previous term may still be waiting for export.
            audit_slots: Arc::new(audit::AuditSlots::dirty_at_start()),
            audit_store: None,
            audit_signal_armed: false,
            warm: warm::Warm::new(warm::WARM_CAP_BYTES),
        })
    }

    /// The nearest deadline of the timed tables — a shared handle, readable
    /// without the engine lock (the background loop sleeps until it).
    pub fn next_due(&self) -> &NextDue {
        &self.next_due
    }

    /// Sets the retention: ended invocations are purged after this many milliseconds.
    ///
    /// A semantic consequence to know before tuning it: **the idempotency
    /// dedupe window is the retention** — after the purge, a call with the
    /// same idempotency key is a new invocation.
    /// The set of tenants with the hash chain enabled, shared with the node.
    pub fn with_chained_tenants(mut self, chained: crate::chain::ChainedTenants) -> Self {
        self.chained = chained;
        self
    }

    /// The artifact store. The spawner sets it when the `streams/table` gate is
    /// on (the `Artifacts` table comes with that gate).
    pub fn with_artifacts(mut self, a: Option<cairn_store::artifact::ArtifactStore>) -> Self {
        self.artifacts = a;
        self
    }

    /// The range's artifact cache, a share of the node's common `budget`.
    pub fn with_artifact_cache(
        mut self,
        budget: Option<cairn_store::artifact::CacheBudget>,
    ) -> Self {
        self.artifact_cache =
            budget.map(|b| std::sync::Arc::new(cairn_store::artifact::ArtifactCache::new(b)));
        self
    }

    pub fn artifact_cache(&self) -> Option<&std::sync::Arc<cairn_store::artifact::ArtifactCache>> {
        self.artifact_cache.as_ref()
    }

    /// `--archive-artifacts`: archiving before a purge also copies artifact bytes.
    pub fn with_archive_artifacts(mut self, on: bool) -> Self {
        self.archive_artifacts = on;
        self
    }

    pub fn archive_artifacts(&self) -> bool {
        self.archive_artifacts
    }

    /// `T_put` (AP-9) — tests set it short.
    pub fn with_artifact_put_deadline(mut self, d: std::time::Duration) -> Self {
        self.artifact_put_deadline = d;
        self
    }

    pub fn artifact_put_deadline(&self) -> std::time::Duration {
        self.artifact_put_deadline
    }

    /// The artifact store; `None` = off.
    pub fn artifact_store(&self) -> Option<&cairn_store::artifact::ArtifactStore> {
        self.artifacts.as_ref()
    }

    /// The first half of the deletion proof for segments of `(tenant, pk,
    /// owner)` ([`cairn_store::proof::values_referenced`]): reads this range's
    /// Db and the in-RAM table. No network I/O beyond the Db.
    pub async fn values_referenced(
        &self,
        tenant: &str,
        pk: PartitionKey,
        owner: &cairn_store::value::Owner,
    ) -> Result<std::result::Result<cairn_store::proof::ValuesReferenced, String>> {
        Ok(
            cairn_store::proof::values_referenced(&self.writer, &self.cluster, tenant, pk, owner)
                .await?,
        )
    }

    /// The first half of the deletion proof for artifacts of `(tenant, id,
    /// epoch)` — reads this range's Db and the in-RAM table
    /// ([`cairn_store::proof::artifacts_unreferenced`]). No network I/O beyond
    /// the Db: can run under the engine.
    pub async fn artifacts_unreferenced(
        &self,
        owner: &cairn_store::artifact::Owner,
    ) -> Result<std::result::Result<cairn_store::proof::ArtifactsUnreferenced, String>> {
        Ok(cairn_store::proof::artifacts_unreferenced(&self.writer, &self.cluster, owner).await?)
    }

    /// The archive store. `None` = purge without archiving.
    pub fn with_archive(mut self, archive: Option<cairn_store::archive::ArchiveStore>) -> Self {
        self.archive = archive;
        self
    }

    pub fn with_retention(mut self, retention_ms: Option<u64>) -> Self {
        self.retention_ms = retention_ms;
        self
    }

    /// A fixed per-tenant limit snapshot (tests).
    pub fn with_tenant_limits(mut self, limits: Arc<crate::TenantLimits>) -> Self {
        self.tenant_limits = limits;
        self.tenant_limits_src = None;
        self
    }

    /// The node's per-tenant limit source: copies the snapshot right away, and
    /// copies it again in `step` whenever the source changes generation.
    pub fn with_tenant_limits_source(mut self, src: crate::TenantLimitsSource) -> Self {
        let generation = src.generation();
        self.tenant_limits = src.snapshot();
        self.tenant_limits_src = Some((src, generation));
        self
    }

    /// Copies the snapshot again if the source has changed generation.
    pub(super) fn refresh_tenant_limits(&mut self) {
        if let Some((src, seen)) = &mut self.tenant_limits_src {
            let now = src.generation();
            if now != *seen {
                *seen = now;
                self.tenant_limits = src.snapshot();
            }
        }
    }

    /// The limits of `tenant` from the range's snapshot.
    pub fn tenant_limit(&self, tenant: &cairn_store::TenantId) -> crate::TenantLimit {
        self.tenant_limits.of(tenant)
    }

    /// The state of `tenant` — copies the snapshot again first if the source
    /// has changed generation, so a change the node has confirmed (the ACK of
    /// `PUT /admin/tenants/{t}/state`) takes effect at the very next command.
    pub fn tenant_state(&mut self, tenant: &cairn_store::TenantId) -> crate::TenantState {
        self.refresh_tenant_limits();
        self.tenant_limits.of(tenant).state
    }

    /// Whether **new** work of `tenant` is refused (state, `max-live=`) —
    /// copies the snapshot again first if the source has changed generation.
    pub fn tenant_refusal(
        &mut self,
        tenant: &cairn_store::TenantId,
    ) -> Option<crate::TenantRefusal> {
        self.refresh_tenant_limits();
        self.tenant_limits.refusal(tenant)
    }

    /// Whether an `ArtifactPut` of `tenant` is refused (`max-artifact-bytes=`).
    pub fn tenant_artifacts_full(&mut self, tenant: &cairn_store::TenantId) -> bool {
        self.refresh_tenant_limits();
        self.tenant_limits.artifacts_full(tenant).is_some()
    }

    /// The range's per-tenant limit source (shared with the term's scheduler).
    pub fn tenant_limits_source(&self) -> Option<crate::TenantLimitsSource> {
        self.tenant_limits_src.as_ref().map(|(s, _)| s.clone())
    }

    /// The retention applied to an ending of `tenant`, when the handler
    /// declares no `journalRetention`: the tenant's (`retention-ms=`) overrides
    /// `--retention-ms`.
    pub(super) fn retention_for(&self, tenant: &cairn_store::TenantId) -> Option<u64> {
        self.tenant_limits
            .retention(tenant)
            .unwrap_or(self.retention_ms)
    }

    /// The cluster's `calls/prefix` gate as it was at range open
    /// ([`cairn_store::migrate::ClusterVersion::call_prefix_on`]). Off by default.
    pub fn with_call_prefix(mut self, on: bool) -> Self {
        self.call_prefix = on;
        self
    }

    /// The `calls/prefix` gate this engine was opened with.
    pub fn call_prefix(&self) -> bool {
        self.call_prefix
    }

    /// The cluster's `calls/placement` gate as it was at range open
    /// ([`cairn_store::migrate::ClusterVersion::call_placement_on`]). Off by
    /// default.
    pub fn with_call_placement(mut self, on: bool) -> Self {
        self.call_placement = on;
        self
    }

    /// The `calls/placement` gate this engine was opened with.
    pub fn call_placement(&self) -> bool {
        self.call_placement
    }

    /// The cluster's `streams/table` gate as it was at range open
    /// ([`cairn_store::migrate::ClusterVersion::streams_on`]). Off by default.
    pub fn with_streams(mut self, on: bool) -> Self {
        self.streams = on;
        self
    }

    /// The `streams/table` gate this engine was opened with.
    pub fn streams(&self) -> bool {
        self.streams
    }

    /// The cluster's `schedules/table` gate as it was at range open
    /// ([`cairn_store::migrate::ClusterVersion::schedules_on`]). Off by default.
    pub fn with_schedules(mut self, on: bool) -> Self {
        self.schedules = on;
        self
    }

    /// Query counters are only built once someone asks:
    /// [`Self::query_build_step`] returns `Idle` when every descriptor is
    /// `readable` and nobody has asked for the counters yet, instead of
    /// scanning the whole `ByCreated` every term. Off by default (built along
    /// with the background build); the node turns it on.
    pub fn with_counts_on_demand(mut self, on: bool) -> Self {
        self.counts_on_demand = on;
        self
    }

    /// The `schedules/table` gate this engine was opened with.
    pub fn schedules(&self) -> bool {
        self.schedules
    }

    /// The call-rule marks that invocations created by this engine carry in entry 0.
    fn call_rules(&self) -> CallRules {
        CallRules {
            prefix: self.call_prefix,
            placement: self.call_placement,
        }
    }

    /// The engine's clock. Default [`SystemClock`] (the sim controls it through
    /// `entropy` and the virtual runtime); unit tests inject a [`cairn_store::clock::ManualClock`].
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The range's clock — the only time source for turns and for the service
    /// loops running on this engine.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// The partition this engine currently leads.
    pub fn partition(&self) -> PartitionId {
        self.writer.partition()
    }

    /// Opening is done: the recovery path's scan has run, from here on full
    /// table scans stop caching blocks (`PartitionWriter::end_opening`).
    pub fn end_opening(&self) {
        self.writer.end_opening();
    }

    /// The derived-zone rebuild at this engine's open — `None` when the mark already matched.
    pub fn rebuilt(&self) -> Option<&cairn_store::RebuildStats> {
        self.rebuilt.as_ref()
    }

    /// This engine's range — fixed for the engine's whole life.
    pub fn range(&self) -> &RangeEntry {
        &self.range
    }

    /// Whether this partition key belongs to this engine's range.
    pub fn owns_key(&self, key: PartitionKey) -> bool {
        self.range.contains_key(key)
    }

    /// Whether this invocation belongs to this engine's range — compared with
    /// **the engine's own range**, not the current table.
    pub fn owns_id(&self, id: &InvocationId) -> bool {
        self.owns_key(partition_key_of(id))
    }

    pub fn writer(&self) -> &PartitionWriter<S> {
        &self.writer
    }

    /// The direct write path, outside the state machine. It exists so tests can
    /// build broken states; the normal path always goes through [`Engine::step`].
    pub fn writer_mut(&mut self) -> &mut PartitionWriter<S> {
        &mut self.writer
    }

    /// Creates a new invocation.
    ///
    /// Entry 0 (`Input`) is produced **here**, not in the state machine: it is
    /// the request's data, which exists before the SDK runs a single line of
    /// code.
    pub async fn create(
        &mut self,
        id: InvocationId,
        target: &Target,
        opts: CreateOptions<'_>,
        params: StartParams,
        input: Bytes,
    ) -> Result<Invocation> {
        // A new entry 0 for `id`: any warm machine of that id (purged and
        // recreated) belongs to a different journal.
        self.warm.forget(&id);
        let params = self
            .with_eager_state(with_key(params, target, opts.kind), target)
            .await?;
        let entry = input_entry(
            target,
            opts.reply_to,
            opts.idempotency_key,
            opts.kind,
            opts.deployment,
            // The rule for every unkeyed call of this invocation is fixed here,
            // in the same commit as entry 0.
            self.call_rules(),
            opts.log_level,
            opts.schedule.as_ref().map(|s| s.header),
            input,
        );
        let mut meta = meta::meta_of(target, &opts);
        meta.created_ms = self.clock.wall_ms();
        let mut muts = Mutations::new();
        let raw0 = entry.encode();
        let status = match opts.paused {
            Some((code, message)) => InvocationStatus::Paused {
                code,
                message: message.to_string(),
            },
            None => InvocationStatus::Running,
        };
        muts.append_journal(&id, 0, &raw0);
        // Creating over an **existing** id (found by chaos testing: a path that
        // recreates the same id after a `kill -9`): index rows are a function
        // of the current (metadata, status), so the old position is removed in
        // the same commit — otherwise four old rows would stay forever, and the
        // counters would count the id twice. One metadata point read per
        // creation; absent (almost always) ⇒ nothing more is read.
        if let Some(old) = self.meta(&id).await? {
            if let Some((s, at)) = self.status_at(&id).await? {
                let p = cairn_store::query_index::Placement::new(s.class(), at.unwrap_or(0), &old);
                muts.query_event_with(self.query_indexes, &id, &old, Some(p), None);
            }
        }
        // Metadata and status in the same commit as entry 0.
        self.stage_status(&mut muts, &id, meta::Was::New(&meta), &status)
            .await?;
        // The hash chain starts at entry 0, in the same commit.
        let chain = self.chained.contains(&target.tenant).then(|| {
            let h = cairn_store::archive::chain_step(&cairn_store::archive::CHAIN_SEED, 0, &raw0);
            muts.set_chain(&id, 1, &h);
            (1u32, h)
        });
        if let Some(idem) = opts.idempotency_key {
            muts.set_idempotency(
                &target.tenant,
                target.scope(),
                &target.service,
                &target.handler,
                &target.object_key,
                idem,
                &id,
            );
        }
        // The vqueue entry goes into **the same commit** as journal entry 0 and
        // the status. Split apart, it would leave a `Running` invocation that no
        // queue knows about — it would never be granted a permit, and there
        // would be no error to see.
        if let Some((queue, seq)) = opts.queue {
            muts.push_vqueue(&queue.encode(), Stage::Inbox, seq, &id);
        }
        // Pinned to the deployment in **the same commit**: split apart, a death
        // between two commits would leave an invocation nobody knows is running
        // on which deployment — and that deployment could be removed right
        // under its feet.
        if let Some(d) = opts.deployment {
            muts.pin_deployment(&id, d);
        }
        // The schedule row moves `next_at` in the same commit as the run it
        // creates (CG-12): death before the commit ⇒ the next leader fires the
        // same instant again; after ⇒ both are durable.
        if let Some(w) = opts.schedule.as_ref().and_then(|s| s.row.as_ref()) {
            self.stage_schedule(&mut muts, w);
        }
        self.writer.commit(muts).await?;

        Ok(Invocation {
            id,
            target: target.clone(),
            reply_to: opts.reply_to.cloned(),
            kind: opts.kind,
            deployment: opts.deployment.map(str::to_string),
            machine: InvocationMachine::resume(id, params, Journal::from_entries(vec![entry])),
            outcome: None,
            chain,
            retention_ms: None,
            calls: self.call_rules(),
            // `input_entry` always writes the mark.
            state_lock: true,
            journal_bytes: raw0.len() as u64,
            inline_values: false,
        })
    }

    pub async fn seal(self) -> Result<()> {
        Ok(self.writer.seal().await?)
    }
}

impl<S: RangeStore> Engine<S> {
    /// Wall time for scheduled purges, from the injected clock. Only used for
    /// retention — the state machine still does not read the clock: the purge
    /// deadline is not in the journal and is not replayed, so it is not a
    /// source of non-determinism.
    fn purge_due(&self, ttl: u64) -> u64 {
        self.clock.wall_ms().saturating_add(ttl)
    }
}

#[cfg(test)]
mod spawned_tests {
    use {
        super::*,
        cairn_store::{LeadershipGrant, PartitionId},
        slatedb::{object_store::memory::InMemory, Db},
        std::sync::Arc,
    };

    const ID: InvocationId = [7; 24];

    async fn engine() -> Engine {
        let db = Db::builder("partition-0", Arc::new(InMemory::new()))
            .build()
            .await
            .expect("open db");
        let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
            .await
            .expect("open writer");
        Engine::open(w).await.expect("open engine")
    }

    /// Two parties waiting for the work an invocation spawned (e.g. `/resume`
    /// and a signal arriving at the same time) must **both** see its watermark.
    /// If asking deleted it, the later asker would have nothing to wait for and
    /// would answer before the work finished.
    #[tokio::test]
    async fn second_waiter_still_sees_spawned_mark() {
        let mut e = engine().await;
        e.spawned.insert(ID, 7);
        assert_eq!(e.spawned_after(&ID, 3), Some(7));
        assert_eq!(
            e.spawned_after(&ID, 3),
            Some(7),
            "the second asker lost the watermark — the question deleted it"
        );
        assert_eq!(
            e.spawned_after(&ID, 7),
            None,
            "a watermark already drained past is not waited for"
        );
        assert!(
            !e.spawned.contains_key(&ID),
            "an entry drained past must be forgotten"
        );
    }

    /// The purge deadline is computed from the clock **injected** into the
    /// engine, not from the system clock: ending at wall time `T` with
    /// retention `r` ⇒ the purge is scheduled at exactly `T + r`, whatever the
    /// real time is.
    #[tokio::test]
    async fn purge_due_follows_injected_clock() {
        let clock = Arc::new(cairn_store::clock::ManualClock::new(1_000_000));
        let mut e = engine()
            .await
            .with_retention(Some(500))
            .with_clock(clock.clone());
        let target = Target {
            tenant: Default::default(),
            scope: None,
            service: "Svc".into(),
            handler: "run".into(),
            object_key: String::new(),
        };
        let mut inv = e
            .create(
                ID,
                &target,
                CreateOptions::default(),
                StartParams::default(),
                bytes::Bytes::new(),
            )
            .await
            .expect("create");
        e.step(&mut inv, Event::Start).await.expect("start");
        clock.advance(std::time::Duration::from_millis(250));
        e.fail_with(&ID, 500, "broken").await.expect("end");
        assert!(e.due_purges(1_000_749, 8).await.expect("scan").is_empty());
        let due = e.due_purges(1_000_750, 8).await.expect("scan");
        assert_eq!(due, vec![(1_000_750, ID)]);
    }
}
