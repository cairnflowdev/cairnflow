// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The engine's data types: `Target`, `Invocation`, `Source`, the `*Outcome`s, error codes.

use super::*;

pub type InvocationId = cairn_store::InvocationId;

// The state machine (`cairn-runtime`) does not depend on the store, so it has
// its own alias — the two lengths must match, and the compiler keeps it so.
const _: () = assert!(
    std::mem::size_of::<InvocationId>() == std::mem::size_of::<cairn_runtime::InvocationId>()
);

/// One journal row for introspection: `(entry_index, message_type, payload)`.
pub type JournalRow = (u32, u16, Vec<u8>);

/// The error code when an invocation is cancelled through the admin API.
///
/// Continues the 57x range of `cairn_protocol` (570 journal mismatch, 571
/// protocol violation): these are "the runtime ended the invocation" codes,
/// distinguishable from user code's error codes. Waiters (`ctx.call`) receive
/// a failure carrying this code.
pub const CANCELLED: u32 = 572;

/// The error code when an invocation touches the state or a promise of an
/// **unkeyed, unscoped** object (D-a): no partition key owns such a row, so it
/// belongs to no range.
pub const NO_OBJECT_KEY: u32 = 574;

/// The error code when an invocation has used up `retryPolicyMaxAttempts`
/// attempts — recorded in the `paused` (`PAUSE`) or `failed` (`KILL`) status.
pub const MAX_ATTEMPTS_EXHAUSTED: u32 = 575;

/// The code of a **new** invocation put into `paused` right at creation
/// because its handler is quarantined with the `hold` action. Never
/// `failed`: deploy the fixed version, then `resume`.
pub const QUARANTINED: u32 = 576;

/// The code of an invocation put into `Paused` because **cairn's
/// infrastructure** cannot move it forward: the rescue net has handed it over
/// too many times in a row while the turn still failed before leaving a
/// durable scheduled retry, or what it waits for is in a row that does not
/// decode. Never `failed`: fix the cause (store, binary), then `resume`.
pub const INFRA_STUCK: u32 = 577;

/// The routing target of an invocation.
///
/// Durable in **the header of the `Input` entry**, not in RAM. That is INV-3
/// applied to routing itself: when a timer fires after a restart, the node
/// must know which service/handler to call back — if that information lived
/// only in the memory of the process that died, the invocation would sleep
/// forever.
///
/// The `Input` entry is the right place because it is immutable: written once
/// at creation, never changed, and always entry 0.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Target {
    /// The invocation's tenant — the **outermost** part of the identity: the
    /// tenant contains the scope, the scope contains the object. An input to
    /// the partition-key hash and to every user-named key (state, promises,
    /// idempotency, queues). Ingress takes it from the API key, never from the
    /// path; `ctx.call` inherits the caller's tenant — there are no
    /// cross-tenant calls. Commands by id (`attach`, `getOutput`, `signal`) to
    /// an invocation of another tenant are answered as if it did not exist
    /// (`Engine::visible_status`, flow FL-25).
    pub tenant: TenantId,
    /// The invocation's scope — **part of the identity** of everything inside it.
    ///
    /// It lives in `Target` rather than in a separate parameter because it
    /// goes with `service`/`object_key` into *every* state-table key: state,
    /// promises, idempotency. Splitting it out opens exactly the door for a
    /// call site to forget it, and forgetting the scope means reading another
    /// tenant's state.
    pub scope: Option<Scope>,
    pub service: String,
    pub handler: String,
    pub object_key: String,
}

impl Target {
    pub fn scope(&self) -> Option<&Scope> {
        self.scope.as_ref()
    }

    /// The `owner_pk` of this object's state/promises: the partition key
    /// derived from `(scope, service, object_key)`.
    ///
    /// `None` = an **unkeyed, unscoped** object (D-a): no pk owns it, so no
    /// range is the right home for its state or promises. Touching them is a
    /// terminal error ([`NO_OBJECT_KEY`]) — no owner is made up.
    pub fn object_owner(&self) -> Option<PartitionKey> {
        cairn_store::keys::object_owner(&self.tenant, self.scope(), &self.service, &self.object_key)
    }
}

/// The D-a error — see [`Target::object_owner`].
pub(super) fn no_owner(target: &Target) -> EngineError {
    EngineError::NoObjectKey {
        service: target.service.clone(),
    }
}

/// Whether a state-machine task touches the object's state or promises.
pub(super) fn touches_object(fx: &Effect) -> bool {
    matches!(
        fx,
        Effect::Task(
            Task::WriteState { .. }
                | Task::ReadState { .. }
                | Task::ReadStateKeys { .. }
                | Task::ClearAllState
                | Task::PromiseGet { .. }
                | Task::PromisePeek { .. }
                | Task::PromiseComplete { .. }
        )
    )
}

/// The kind of an invocation's target — what `Target` *cannot* say.
///
/// `Target` is the address; this is the **contract**. It decides three things
/// that no other layer can read back after a restart:
///
/// | | `Plain` | `WorkflowRun` | `WorkflowShared` |
/// |---|---|---|---|
/// | id | random / by idempotency key | **a function of `(scope, service, key)`** | like `Plain` |
/// | writes state | yes, except a VO's `SHARED` handlers (mark [`HDR_STATE_LOCK`]) | yes | **no** |
/// | purge | journal + status | plus the workflow's **state and promises** | like `Plain` |
///
/// Durable in **the header of the `Input` entry**, the same place and the same
/// reason as [`Target`]: the deployment registry lives in RAM and it *can
/// change* — a running workflow whose service a new deployment redeclares as
/// `SERVICE` would make every later reload read the wrong contract for an
/// invocation that already exists. Entry 0 is immutable, so the contract is
/// fixed at creation and can no longer change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InvocationKind {
    #[default]
    Plain,
    /// A `WORKFLOW` handler — the main run, exactly once per workflow id.
    WorkflowRun,
    /// A workflow's `SHARED` handler: runs in parallel with `run`, reads state,
    /// completes promises. Does not write state.
    WorkflowShared,
}

impl InvocationKind {
    /// The value written to the entry-0 header. `None` = nothing written (the default).
    pub(super) fn as_header(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::WorkflowRun => Some("workflow-run"),
            Self::WorkflowShared => Some("workflow-shared"),
        }
    }

    pub(super) fn from_header(raw: &str) -> Self {
        match raw {
            "workflow-run" => Self::WorkflowRun,
            "workflow-shared" => Self::WorkflowShared,
            // An unknown value ⇒ `Plain`, not an error: a newer binary may add
            // kinds, and an older binary that reads one must still be able to
            // run that invocation — it just cannot apply rules it does not know.
            _ => Self::Plain,
        }
    }

    pub fn is_workflow_run(self) -> bool {
        matches!(self, Self::WorkflowRun)
    }
}

/// Options for creating an invocation. Gathered in one place because
/// everything here must be durable **in the same commit** as the `Input` entry
/// — splitting it out opens a loss window.
#[derive(Default)]
pub struct CreateOptions<'a> {
    pub reply_to: Option<&'a ReplyTo>,
    /// Records the idempotency mapping `(scope, service, handler, key, idem) → invocation`.
    pub idempotency_key: Option<&'a str>,
    /// The virtual queue and the place in it.
    ///
    /// **Every** invocation goes into a queue — even unscoped ones, even
    /// SHARED handlers. That is D-F4: the vqueue is the *only* path, there is
    /// no "old" branch running alongside. A queueing mechanism with two paths
    /// is a mechanism that will drift apart on the third fix.
    pub queue: Option<(&'a QueueName, u64)>,
    /// The invocation's contract. See [`InvocationKind`].
    pub kind: InvocationKind,
    /// The deployment the invocation is **pinned to** for its whole life: every
    /// turn calls exactly this id's endpoint, even after the service's revision
    /// has moved to another version. `None` = not pinned (hand-built tests) —
    /// turns run against the current revision.
    pub deployment: Option<&'a str>,
    /// `Some((code, message))` = created directly in `Paused` instead of
    /// `Running`, **in the same commit** (`hold` quarantine). The queue entry
    /// stays in `Inbox` — holding no VO lock —, and `recover` does not run it
    /// (it only runs `running`); `resume` is the only way it runs.
    pub paused: Option<(u32, &'a str)>,
    /// The minimum `ctx.log` level the caller requested through the
    /// `cairnflow-log-level` HTTP header: written to the entry-0 header of the
    /// same name, the SDK reads it on every turn. `None` = not written (entry 0
    /// is not longer for invocations that did not ask).
    pub log_level: Option<&'a str>,
    /// A run fired by a **schedule**: the `cairnflow-schedule` header of entry
    /// 0, and — in the schedule's range — the schedule row moves `next_at`
    /// **in the same commit** as the creation (CG-12).
    pub schedule: Option<ScheduleStamp<'a>>,
    /// User tags: go into the metadata and the `ByAttr` index, immutable. The
    /// call site has already checked the caps (`query_index::check_tag`).
    pub tags: &'a [(&'a str, &'a str)],
    /// The sender of a **one-way** call (`ctx.send`): the invocation's parent
    /// when there is no `reply_to`.
    pub sender: Option<InvocationId>,
    /// The call-tree root, carried by the call — the metadata's `root_id` TLV,
    /// the `$root` attribute of `ByAttr`.
    pub root_id: Option<InvocationId>,
}

/// The schedule mark of a creation — see [`CreateOptions::schedule`].
pub struct ScheduleStamp<'a> {
    /// `<name>@<instant>` ([`crate::schedule::header_value`]).
    pub header: &'a str,
    /// The schedule row written in the same commit; `None` in the target range of a cross-range fire.
    pub row: Option<ScheduleWrite<'a>>,
}

/// One schedule-row write: `id`, the old deadline of the index row (`None` =
/// none yet), the new deadline (`u64::MAX` = paused), the row.
pub struct ScheduleWrite<'a> {
    pub id: InvocationId,
    pub old_due: Option<u64>,
    pub due: u64,
    pub row: &'a crate::schedule::ScheduleRow,
}

/// Where to return the result of an invocation called by `ctx.call`.
///
/// Durable in the same place as [`Target`], and for exactly the same reason:
/// when the child invocation completes after a restart, the node must know
/// where to return the result. Kept in RAM, the caller would hang forever.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplyTo {
    pub caller: InvocationId,
    pub completion_id: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Machine(#[from] MachineError),
    #[error("invocation journal is corrupt: {0}")]
    Journal(#[from] cairn_runtime::JournalError),
    #[error("journal is missing entry {0} — not contiguous")]
    JournalGap(u32),
    #[error("invocation has no journal — it was never created")]
    NoJournal,
    #[error("entry 0 is not a valid Input, or the routing header is missing")]
    MissingTarget,
    #[error("outbox is corrupt: {0}")]
    Outbox(#[from] crate::outbox::OutboxError),
    /// D-a: the state/promises of an unkeyed, unscoped object.
    #[error(
        "{service}: state and promises need an object key or a scope — no partition \
         holds the state of an unkeyed service"
    )]
    NoObjectKey { service: String },
}

pub type Result<T> = std::result::Result<T, EngineError>;

/// An invocation running on this node.
pub struct Invocation {
    pub id: InvocationId,
    pub target: Target,
    /// `Some` if this invocation was spawned by `ctx.call`.
    pub reply_to: Option<ReplyTo>,
    /// Read back from entry 0, not from the deployment registry. See [`InvocationKind`].
    pub kind: InvocationKind,
    /// The deployment pinned at creation, read back from entry 0. See [`CreateOptions::deployment`].
    pub deployment: Option<String>,
    pub(super) machine: InvocationMachine,
    pub(super) outcome: Option<Outcome>,
    /// The hash-chain head: `(number of entries covered, hash of the last
    /// entry)`. `None` = the tenant did not have the chain enabled when this
    /// invocation was created.
    pub(super) chain: Option<(u32, [u8; 32])>,
    /// The handler's own retention (the manifest's `journalRetention`),
    /// overriding the node's retention at the ending commit. The call site sets
    /// it before stepping — the engine does not read the deployment registry.
    pub(super) retention_ms: Option<u64>,
    /// The `cairnflow-call-prefix` and `cairnflow-call-placement` marks of
    /// entry 0: this invocation's rule for unkeyed calls.
    pub(super) calls: CallRules,
    /// The [`HDR_STATE_LOCK`] mark of entry 0: the invocation lives under the
    /// rule "a VO's `SHARED` handlers do not write state" (D-W5).
    pub(super) state_lock: bool,
    /// The total encoded bytes of every **durable** journal entry: what
    /// `max-journal-bytes=` compares. Computed at load, added to on every
    /// `Persist`.
    pub(super) journal_bytes: u64,
    /// This turn delivers values **inline** to the SDK: the deployment is fixed
    /// at a wire before v6, the shell assembles every segmented value going
    /// down. The turn layer sets it from the deployment
    /// ([`Self::mark_inline_values`]); the engine does not read the deployment
    /// registry.
    pub(super) inline_values: bool,
}

impl std::fmt::Debug for Invocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Invocation")
            .field("id", &hex(&self.id))
            .field("target", &self.target)
            .field("reply_to", &self.reply_to)
            .field("known_entries", &self.machine.journal().len())
            .field("outcome", &self.outcome)
            .finish()
    }
}

pub(super) fn hex(id: &InvocationId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

impl Invocation {
    /// The total encoded bytes of the durable journal.
    pub fn journal_bytes(&self) -> u64 {
        self.journal_bytes
    }

    /// The handler's retention — `None` = follow the node's retention.
    pub fn set_retention(&mut self, retention_ms: Option<u64>) {
        self.retention_ms = retention_ms;
    }

    /// The wire-v3 parameters of `Start` — see `InvocationMachine::set_wire_v3`.
    pub fn set_wire_v3(&mut self, keepalive_ms: Option<u64>, replay_only: bool) {
        self.machine.set_wire_v3(keepalive_ms, replay_only);
    }

    /// This turn runs a virtual object's `SHARED` handler — the turn layer
    /// decides from the pinned deployment (the engine does not read the
    /// deployment registry). If the invocation carries the [`HDR_STATE_LOCK`]
    /// mark, the machine rejects state-write commands from here on (D-W5);
    /// without the mark (an old binary) the old rule is kept. Call before
    /// `Event::Start`, on **every** machine build of the turn.
    /// This turn's SDK reads values inline (wire before v6): a segmented value
    /// going down that is longer than the tenant's `max-payload=` ⇒ the
    /// invocation ends with `payload_too_large` instead of sending a frame
    /// larger than the limit.
    pub fn mark_inline_values(&mut self) {
        self.inline_values = true;
    }

    pub fn mark_shared_object_handler(&mut self) {
        if self.state_lock {
            self.machine.forbid_state_writes();
        }
    }

    pub fn outcome(&self) -> Option<&Outcome> {
        self.outcome.as_ref()
    }

    /// This turn may not write state (a marked `SHARED` handler) — the shell
    /// does not seal its `SetState` (the machine would reject it; the PUT
    /// would be an orphan).
    pub fn state_writes_forbidden(&self) -> bool {
        self.machine.state_writes_forbidden()
    }

    pub fn known_entries(&self) -> u32 {
        self.machine.journal().len()
    }

    /// The entry index of the `Run` command carrying completion
    /// `completion_id` (the runtime span of a `ctx.run` rerun on a later
    /// attempt). Scans the journal — only called on a rare path (runtime trace
    /// on, no open span).
    pub fn run_entry(&self, completion_id: u32) -> Option<u32> {
        let entries = self.machine.journal().entries();
        let i = entries.iter().position(|e| {
            run_command(e).is_some_and(|m| m.result_completion_id == completion_id)
        })?;
        u32::try_from(i).ok()
    }

    /// The name (`RunCommandMessage.name`) of the `Run` command at entry `entry`.
    pub fn run_name(&self, entry: u32) -> Option<String> {
        run_command(self.machine.journal().get(entry)?).map(|m| m.name)
    }

    /// The completion ids that have a notification in the journal — what the next
    /// turn can replay to the SDK right away.
    pub fn settled_completions(&self) -> Vec<u32> {
        self.machine.settled_completions()
    }

    /// Whether completion `id` already has a notification (without building an array).
    pub fn is_settled(&self, id: u32) -> bool {
        self.machine.is_settled(id)
    }

    /// Signals received, by identity — matched against the suspension's signal leaves.
    pub fn settled_signals(&self) -> &[cairn_protocol::pb::signal_notification_message::SignalId] {
        self.machine.settled_signals()
    }
}

/// The source that produced a completion — decides what must be cleaned up in the same commit.
#[derive(Clone, Copy, Debug)]
pub enum Source {
    None,
    Timer {
        due_ms: u64,
    },
    Outbox {
        row: OutboxRef,
    },
    /// A message received from another partition. Deleted in the same commit as
    /// its consequence, for the same reason as `Outbox`: split apart, a death
    /// exactly between two commits would replay the message, and replaying a
    /// completion is what the service protocol forbids.
    Inbound {
        key: InboundKey,
    },
    /// A signal row of this very partition waiting to be sent (same-partition
    /// target). The key is `(sender, entry index of the SendSignal command)`.
    SignalOutbox {
        key: SignalRef,
    },
}

/// An inbound row: its place in the receipt-order index, its owner (the pk of
/// the message's **target**), and its identity `(kind, invocation, completion)`.
///
/// Everything needed to delete exactly the two rows (entity + index) travels
/// in one type — the same reason as [`OutboxRef`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InboundKey {
    pub seq: u64,
    pub owner: PartitionKey,
    pub kind: InboundKind,
    pub id: InvocationId,
    pub completion_id: u32,
}

/// A signal row waiting to be sent: its place in the production-order index and
/// its identity `(sender, entry index)`. The owner is the sender, derived from the id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignalRef {
    pub seq: u64,
    pub sender: InvocationId,
    pub entry_index: u32,
}

/// An invocation's place in its virtual queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VQueueSlot {
    pub name: QueueName,
    /// The queue name as bytes — exactly the keyspace's `queue_key`.
    pub queue: Vec<u8>,
    pub stage: Stage,
    pub seq: u64,
}

/// One row of the full vqueue table scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VQueueRow {
    pub slot: VQueueSlot,
    pub id: InvocationId,
}

/// The result of a cancel command — see [`Engine::cancel`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// This partition has no such invocation.
    NotFound,
    /// Already stopped; cancelling is a no-op. Carries the status at that time —
    /// the operator needs to know *how it finished*, not just "cannot cancel".
    AlreadyFinished(InvocationStatus),
    /// Just cancelled. The result for waiters is already in the outbox.
    Cancelled,
}

/// The identity of an outbox row: `(invocation in the key, completion id)` — the
/// caller for `Call`/`Attach`/`PeekOutput`/`Cancel`, the **recipient** for `Reply`.
pub type OutboxKey = (InvocationId, u32);

/// An outbox row, with its owner and its place in the ordering index.
///
/// The three go together at **every** place that touches an outbox row, so
/// they travel in one type: deleting the main row while forgetting the index
/// row leaves a pointer to an empty slot. The owner must come along because
/// for a `Reply` row it **cannot** be derived from the key — the index carries
/// it, and this type carries it on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxRef {
    /// The **production** order — what the drainer's cursor follows. Quite
    /// different from the main table's key order, which sorts by owner then id
    /// (hashed numbers).
    pub seq: u64,
    pub owner: PartitionKey,
    pub key: OutboxKey,
}

/// The `seq` issuer for the outbox, and the reverse map of rows not yet cut.
///
/// Both are **rebuildable numbers**, not pending work, so they live in RAM
/// without breaking INV-3 — exactly the argument of [`Engine::next_queue_seq`],
/// and they are rebuilt by the same scan that runs anyway on becoming leader.
#[derive(Default)]
pub(super) struct OutboxSeqs {
    /// `None` = not seeded. Seeded **lazily**, see [`Engine::next_outbox_seq`].
    pub(super) next: Option<u64>,
    /// `(owner, invocation, completion) → seq` for rows **not yet cut**.
    ///
    /// This is what upholds D-O2: the state machine replays tasks for every
    /// pending completion on every replay, so `push_outbox` for an existing row
    /// is an everyday thing. Issuing a new `seq` there would leave **two** index
    /// rows pointing at one main row — and the drainer would send that call
    /// twice.
    ///
    /// It shrinks with every cut, so it is bounded by the number of pending
    /// rows, not by history.
    pub(super) of: HashMap<(PartitionKey, OutboxKey), u64>,
}

/// The result of an unpause command — see [`Engine::resume`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeOutcome {
    NotFound,
    /// Not in `Paused`. Carries the real status — an operator who types
    /// `resume` for an invocation running normally needs to know they made a
    /// mistake, not get an "ok" and believe something was done.
    NotPaused(InvocationStatus),
    Resumed,
}

/// The result of [`Engine::restart`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartOutcome {
    NotFound,
    /// `keep` exceeds the journal, or is 0 (entry 0 is `Input`, it must be kept).
    BadPrefix {
        len: u32,
    },
    /// A workflow's `run` turn: its id is a function of `(scope, service,
    /// key)` — a "new" copy with another id would no longer be that workflow.
    WorkflowRun,
    Restarted {
        new: InvocationId,
    },
}

/// The result of completing a promise from outside — see
/// [`Engine::complete_promise_external`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromiseOutcome {
    /// The result was just written. `woken` = the number of waiting invocations
    /// that got the result queued.
    ///
    /// `woken == 0` is **valid and common**: the outside system answered before
    /// the workflow got to the waiting point. The promise is durable, so a
    /// waiter arriving later reads it right away — there is no race to lose.
    Completed { woken: usize },
    /// Already had a result; this time is a no-op. Carries the old result.
    AlreadyCompleted(ReplyResult),
}

/// The result of a manual purge command — see [`Engine::purge_now`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurgeOutcome {
    /// This partition has no such invocation.
    NotFound,
    /// Not finished for good — purging a running invocation loses pending work.
    /// `Paused` is here too: it still holds its place in an EXCLUSIVE queue.
    StillRunning,
    Purged,
}

/// The filter of [`Engine::list_invocations_where`]: every field filters
/// **inside** the engine, before `limit` — like `status`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListFilter {
    /// The status name ([`crate::InvocationStatus::name`]); filtered on the index, no GET.
    pub status: Option<String>,
    /// A byte prefix of the id — an OTLP `trace_id` is the first 16 bytes. No GET.
    pub id_prefix: Option<Vec<u8>>,
    /// Filter by target (reads entry 0 — one GET per row that passed the two filters above).
    pub service: Option<String>,
    pub handler: Option<String>,
    /// The target's tenant (entry 0). Filtered **before** `limit` like every
    /// other filter: filtering afterwards (at ingress) would cut `limit` on
    /// another tenant's lines and then return too few while claiming it is
    /// complete.
    pub tenant: Option<TenantId>,
    /// Filter by time, ms, half-open `[after, before)`: `createdAt` (entry 0)
    /// and `endedAt` (the status row, only once ended). Lines missing a
    /// timestamp the filter needs are excluded and counted in
    /// [`Listing::untimed`].
    pub created_after: Option<u64>,
    pub created_before: Option<u64>,
    pub ended_after: Option<u64>,
    pub ended_before: Option<u64>,
    /// Sort by `createdAt` descending (lines without a timestamp last): scan
    /// everything (up to the cap) before cutting `limit`. The HTTP layer turns
    /// it on by default when filtering by time.
    pub created_desc: bool,
    /// A scan cap replacing [`LIST_SCAN_MAX`] — for tests; `None` = default.
    pub scan_max: Option<usize>,
}

impl ListFilter {
    /// Status only — the old path of `list_invocations(limit, status)`.
    pub fn status(status: Option<&str>) -> Self {
        Self {
            status: status.map(str::to_owned),
            ..Self::default()
        }
    }

    /// Whether any filter must read entry 0.
    pub fn by_target(&self) -> bool {
        self.service.is_some() || self.handler.is_some() || self.tenant.is_some()
    }

    /// Has a `createdAt` filter.
    pub fn by_created(&self) -> bool {
        self.created_after.is_some() || self.created_before.is_some()
    }

    /// Has an `endedAt` filter.
    pub fn by_ended(&self) -> bool {
        self.ended_after.is_some() || self.ended_before.is_some()
    }

    /// A capped scan (line count and time): filtering by target, by time, or
    /// sorting by time — operations that may read the whole range before
    /// reaching `limit`.
    pub fn capped(&self) -> bool {
        self.by_target() || self.by_created() || self.by_ended() || self.created_desc
    }

    /// The cap on scanned lines.
    pub fn scan_cap(&self) -> usize {
        self.scan_max.unwrap_or(LIST_SCAN_MAX)
    }
}

/// Whether `t` is within `[after, before)` (an absent end = unbounded).
pub(crate) fn within(t: u64, after: Option<u64>, before: Option<u64>) -> bool {
    after.is_none_or(|a| t >= a) && before.is_none_or(|b| t < b)
}

/// The result of [`Engine::list_invocations_where`].
#[derive(Clone, Debug, Default)]
pub struct Listing {
    pub rows: Vec<(
        InvocationId,
        crate::InvocationStatus,
        Option<Target>,
        crate::Times,
    )>,
    /// Lines excluded because they lack a timestamp the time filter needs.
    pub untimed: usize,
    /// The scan stopped at a cap (lines or time) before the end of the range:
    /// there may be matching lines not scanned yet — the HTTP layer reports
    /// `partial`.
    pub capped: bool,
}

/// A list scan **in progress** between chunks
/// ([`Engine::list_invocations_step`]): the caller releases the engine between
/// chunks. Not a consistent snapshot of the whole range: an invocation that
/// changes status between two chunks is seen in the status its chunk read —
/// like every paginated list.
#[derive(Debug, Default)]
pub struct ListScan {
    pub(super) out: Listing,
    pub(super) scanned: usize,
    /// The time **holding the engine** used so far (the [`LIST_SCAN_BUDGET`]
    /// budget counts against it, not the time released).
    pub(super) held: std::time::Duration,
    pub(super) after: Option<InvocationId>,
    pub(super) done: bool,
}

impl ListScan {
    /// Scan finished (or stopped at `limit`/a cap).
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// The result: sorted by `createdAt` descending when the filter asks, cut at `limit`.
    pub fn finish(self, limit: usize, f: &ListFilter) -> Listing {
        let mut out = self.out;
        if f.created_desc {
            // Descending by `createdAt`; lines without a timestamp last, keeping key order.
            out.rows.sort_by(|a, b| {
                b.3.created_ms
                    .unwrap_or(0)
                    .cmp(&a.3.created_ms.unwrap_or(0))
            });
            out.rows.truncate(limit);
        }
        out
    }
}

/// Status-index rows per chunk of a list scan.
pub const LIST_TAG_CHUNK: usize = 1024;

/// Status-index rows per chunk of the recovery scan
/// ([`Engine::runnable_chunk`]): one engine hold of `recover` or of the rescue
/// net's full scan reads at most this many rows.
pub const RECOVER_CHUNK: usize = 1024;

/// Status-index rows per chunk of the VO lock rebuild on becoming leader
/// ([`Engine::restore_chunk`]).
pub const RESTORE_CHUNK: usize = 1024;

/// The scan mode of [`Engine::runnable_chunk`]: large read-ahead like the full
/// table scan on the way to becoming leader (measured ~11× faster on
/// SeaweedFS), **and** block caching — the next chunk reopens the scan at the
/// cursor, so without a cache every chunk would pull again the previous
/// chunk's whole read. During opening it is exactly the old scan mode as well
/// (`scan_open` caches blocks until `end_opening`).
pub const RECOVER_SCAN: cairn_store::ScanHow = cairn_store::ScanHow::Bulk { cache_blocks: true };

/// The maximum rows one listing **reads entry 0 for** when filtering by target:
/// bounds the cost of a rare `?service=` on a crowded range. When the budget
/// runs out, return what was found.
pub const LIST_SCAN_MAX: usize = 10_000;

/// The **time** cap of a capped scan on one range (CG-11): the scan holds the
/// range's engine, so a rare filter on a crowded range must not hold it longer
/// than this — stop, return what was found, `capped`. Below the 5 s fan-out
/// cap of `?scope=cluster`.
pub const LIST_SCAN_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// The `Run` command of a journal entry; an entry of another kind or broken ⇒ `None`.
fn run_command(
    e: &cairn_runtime::journal::JournalEntry,
) -> Option<cairn_protocol::pb::RunCommandMessage> {
    if e.ty != cairn_protocol::MessageType::RUN {
        return None;
    }
    let frame = cairn_protocol::Frame::new(e.ty, cairn_protocol::Flags::NONE, e.payload.clone());
    match cairn_protocol::decode(&frame) {
        Ok(cairn_protocol::Message::Run(m)) => Some(m),
        _ => None,
    }
}
