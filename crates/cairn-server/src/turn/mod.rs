// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The *turn* loop — connects the engine to the SDK.
//!
//! A **turn** = one run of the SDK from the start until it suspends or finishes:
//!
//! ```text
//! load the invocation from the journal  →  Start + journal replay  →  send down to the SDK
//!        →  feed each returned frame into the state machine  →  judge the result
//! ```
//!
//! Every turn builds a **new** state machine from the journal. That is not
//! waste: reloading from the journal is exactly the replay semantics — the same
//! code path as recovery after a crash, so it runs on *every* invocation, not
//! only when something went wrong.
//!
//! **Two transports, one model.** `REQUEST_RESPONSE` ([`drive_rr`]) sends the
//! whole journal and then reads the whole response: there is no path to push a
//! notification midway, so every time the SDK needs to wait, a turn ends and
//! another HTTP round trip follows. `BIDI_STREAM` ([`drive_bidi`]) changes
//! exactly that and **only** that: I/O goes from batch to stream. The notion of
//! a turn, the entry order, the "durable before sent" rule stay byte for byte
//! the same — an invocation that starts in one mode and fails over to a node
//! that only has the other mode still replays.

use std::ops::AsyncFnOnce;

use {
    crate::{
        actor_types::{ActorError, InvokeOutcome, Result, DEFAULT_SDK_TIMEOUT},
        discovery::{Deployment, ProtocolMode},
        drainer::{PoisonQueue, PoisonSight, WakeDuty},
        flow::{Permit, Scheduler},
        invoker,
        lease::{InvGuard, Lease, Locks, Released},
        registry::Catalog,
        transport::Transport,
    },
    bytes::Bytes,
    cairn_engine::{
        CompletionPayload, CreateOptions, DeliverCompletion, Engine, InterPartition, Invocation,
        InvocationId, InvocationKind, InvocationStatus, OutboxEntry, OutboxRef, ReplyResult,
        ReplyTo, Source, StartCall, Target, VQueueSlot, Watch,
    },
    cairn_lifecycle as lc,
    cairn_protocol::{pb, Message, MessageKind, PROTOCOL_VIOLATION},
    cairn_runtime::{Completion, CompletionResult, Event, FailureAction, Outcome, StartParams},
    cairn_store::{Committed, LimitKey, QueueName, Scope, Stage},
    tracing::Instrument,
};

pub mod admit;
mod submit;
pub use submit::*;
mod outbox;
pub use outbox::*;
mod dispatch;
pub use dispatch::*;
mod drive;
pub use drive::*;
mod permit;
pub use permit::*;
mod retry;
pub use retry::*;
mod sdk;
/// Segmented values in the shell: sealing frames, assembling for SDK ≤ v5.
mod values;
pub use sdk::*;
mod timers;
pub use timers::*;
mod inbound;
pub mod schedule;
pub use inbound::*;
mod control;
pub use control::*;
mod live;
pub use live::LiveTurns;
pub mod replay;
pub use replay::ReplayHints;
mod life;
// Runtime spans in `$trace` (`trace: runtime`).
mod rtrace;
pub(crate) use life::{
    attendance, claim, decide as decide_lifecycle, leader_up, pause_infra, read_err, recover_one,
    rescue_view, restore_stages, stage_to, status_of,
};

/// The context of a turn: everything *outside* the engine that processing needs.
///
/// Gathered into one parameter instead of three, and not for looks: adding
/// `transport` made it a fourth, and three context parameters travelling
/// together through ten functions is the surest way for the next addition to
/// miss one place.
pub struct Ctx<'a> {
    /// Hai client HTTP — h1 cho `REQUEST_RESPONSE`, h2c cho `BIDI_STREAM`.
    pub clients: &'a invoker::Clients,
    /// The deployment registry: current revision for the creation path, by id
    /// for the run path. See [`crate::registry`].
    pub deployments: &'a Catalog,
    /// The path for sending messages to another partition. A one-partition
    /// cluster never uses it — there is no boundary to cross.
    pub transport: &'a Transport,
    /// Per-invocation locks. See [`crate::lease`] for lock order and the points
    /// where the engine is released.
    pub locks: &'a Locks,
    /// The partition's outbox drainer, and the watermark it publishes.
    ///
    /// Every place that used to call `drain_outbox` in place now goes through
    /// here: either **poke** and move on, or poke and **compare the
    /// watermark**. No place drains by itself.
    pub drainer: &'a crate::drainer::Drainer,
    /// The path that hands a durable invocation to the scheduler. See
    /// [`crate::drainer::Runner`] for why the drain loop does not run the child
    /// itself.
    pub runner: &'a crate::drainer::Runner,
    /// The term's safety net (FL-16): a turn that ends without acking its
    /// wake-up duty ([`WakeDuty`], nack by default) is marked here; the
    /// reconciliation loop and the per-id backoff chain live here too.
    pub rescue: &'a crate::drainer::Rescue,
    /// Cap on waiting for the SDK within a turn. `None` = no cap.
    pub sdk_timeout: Option<std::time::Duration>,
    /// Upper bound on the `inactivityTimeout` declared in the contract
    /// (`TurnLimits::sdk_timeout_max`).
    pub sdk_timeout_max: std::time::Duration,
    /// The partition's counters, VO locks and scheduling loop. Every turn goes through here.
    pub scheduler: &'a Scheduler,
    /// Cap on waiting for a **slot** in the queue for a turn that waits inside a
    /// task (`resume`, external signal — [`acquire_turn_background`]). `None` =
    /// wait forever.
    ///
    /// Capped because a turn waiting for a slot inside a task holds its
    /// invocation lock: waiting forever would let a tenant out of budget hold
    /// everything else that wants to touch that invocation (cancel, signal).
    /// When the cap passes, the turn is **rescheduled** — the invocation is
    /// durable and still in the queue, so nothing is lost. Background turns
    /// handed out by `Runner` do not use this cap: a parked ticket holds no
    /// invocation lock (FL-30).
    pub queue_wait: Option<std::time::Duration>,
    /// This term's per-handler summary writer, with the slow-log settings and
    /// the node's snapshot.
    pub stats: &'a crate::stats::Shard,
    /// The range's load: every finished SDK turn records its duration and a
    /// weighted `owner_pk` sample.
    pub load: &'a crate::autosplit::RangeLoad,
    /// The range's L0 throttle: only defers **new work** — incoming `StartCall`s
    /// ([`drain_inbound`]) and local child calls ([`dispatch_call`]).
    pub l0: &'a crate::l0::Throttle,
    /// This thread's copy of the node's quarantine list: `hold` at
    /// [`start_invocation`], `cooldown` where slots are requested.
    pub quarantine: &'a crate::quarantine::RangeView,
    /// The range's streams in this term: the turn layer pushes `StreamAppend`
    /// into them, `Side::Seal` seals them.
    pub streams: &'a std::sync::Arc<crate::streams::RangeStreams>,
    /// Cancellation bells of running turns.
    pub live: &'a LiveTurns,
    /// Audit note of a human command — [`Ctx::audit_begin`].
    pub audit: Option<&'a crate::audit::AuditNote>,
    /// Incremental replay hints of the range's term (wire v5).
    pub replay: &'a ReplayHints,
}

/// Waits for the SDK: releases the engine, with a cap.
///
/// The two jobs are combined in one function on purpose. "Release the engine
/// at the SDK's I/O points" is the rule of [`crate::lease`]; "waiting for the
/// SDK must be capped" is the rule of this place. Both belong to the kind of
/// failure that happens by *forgetting to apply it in one place* — and a
/// function cannot forget. The list of its call sites is exactly the list of
/// SDK wait points.
///
/// A third job, since commit stopped waiting for durability itself: **the
/// journal must be durable before the SDK sees it**. The handler runs side
/// effects based on the journal we just wrote; letting it run before the write
/// is durable opens exactly the "did it but does not remember doing it"
/// window. `_durable` is the ticket, and it is mandatory because the compiler
/// will not let this function be called without it.
///
/// `None` in the result = the cap passed.
async fn awaiting_sdk<T>(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    _durable: &Committed,
    io: impl AsyncFnOnce(&Released) -> T,
) -> Option<T> {
    awaiting_sdk_read(engine, ctx, io).await
}

/// Like [`awaiting_sdk`], but for **reading** from the SDK — no ticket required.
///
/// Reading a frame is not an outside effect: the SDK sees nothing new from our
/// side. Requiring a ticket here would make every frame read of a BIDI turn
/// wait for *every* in-flight write of the partition (the ticket is a shared
/// watermark) — paying latency for something that needs no protection. Sending
/// down to the SDK still has to go through [`awaiting_sdk`].
async fn awaiting_sdk_read<T>(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    io: impl AsyncFnOnce(&Released) -> T,
) -> Option<T> {
    // SDK wait time (slow log) is measured **inside** the released part — not
    // counting the time to reacquire the engine, which `Lease::released`
    // measures itself.
    let timed = async move |r: &Released| {
        let started = tokio::time::Instant::now();
        let out = io(r).await;
        crate::stats::note_sdk(started.elapsed());
        out
    };
    match ctx.sdk_timeout {
        None => Some(engine.released(timed).await),
        Some(d) => engine
            .released(async move |r: &Released| tokio::time::timeout(d, timed(r)).await)
            .await
            .ok(),
    }
}

/// A turn that hit the SDK wait cap.
///
/// The outcome is **retryable**, in the same mould as the SDK itself reporting
/// a retryable `Error` — not a new kind of failure. The journal does not change:
/// everything committed is still there, and the next turn replays from exactly
/// there. All that is lost is the work of the unfinished turn, just like a
/// `kill -9` mid-turn.
fn sdk_timed_out(
    ctx: &Ctx<'_>,
    partition: cairn_store::PartitionId,
    id: &InvocationId,
    mode: &str,
) -> Step {
    crate::metrics::sdk::TIMEOUTS.bump(partition.0);
    tracing::warn!(
        invocation = %hex(id),
        mode,
        limit_ms = ctx.sdk_timeout.map(|d| d.as_millis() as u64).unwrap_or(0),
        "SDK made no progress within the allowed limit — turn dropped, retry scheduled"
    );
    Step::Done(InvokeOutcome::Retry {
        delay_ms: None,
        failure: Some((
            INFRA_FAILURE,
            format!(
                "SDK made no progress in {} ms ({mode}) — turn dropped",
                ctx.sdk_timeout.map(|d| d.as_millis() as u64).unwrap_or(0)
            ),
        )),
    })
}

impl Ctx<'_> {
    /// Stages the audit row of a human command for entity `entity` — call it
    /// **after** taking the entity's lock, right before the engine call that
    /// performs the action: the row goes with the first commit that touches
    /// that entity. A command without a note (background path, tests) or with
    /// the `audit/table` gate off ⇒ `None`.
    pub(crate) async fn audit_begin(
        &self,
        engine: &mut Lease,
        entity: InvocationId,
    ) -> Option<cairn_engine::AuditGuard> {
        self.audit_begin_at(engine, entity, false).await
    }

    /// Like [`Self::audit_begin`], for external signals — the row goes with the
    /// signal delivery batch, not with the `Running` commit of the turn carrying it.
    pub(crate) async fn audit_begin_signal(
        &self,
        engine: &mut Lease,
        entity: InvocationId,
    ) -> Option<cairn_engine::AuditGuard> {
        self.audit_begin_at(engine, entity, true).await
    }

    async fn audit_begin_at(
        &self,
        engine: &mut Lease,
        entity: InvocationId,
        signal: bool,
    ) -> Option<cairn_engine::AuditGuard> {
        let n = self.audit?;
        if !engine.audit_on() {
            return None;
        }
        let tenant = match &n.tenant {
            Some(t) => t.clone(),
            None => audit_tenant_of(engine, &entity).await,
        };
        let record = n.record_for(&tenant, "ok", Some(("entity", hex(&entity).into())));
        if signal {
            engine.audit_next_signal(entity, &tenant, n.ts_ms, &record)
        } else {
            engine.audit_next(entity, &tenant, n.ts_ms, &record)
        }
    }
}

/// Tenant of the target entity (invocation or schedule) when ingress does not
/// know it — read under the engine; unreadable ⇒ `default`.
async fn audit_tenant_of(engine: &mut Lease, entity: &InvocationId) -> String {
    if let Ok(Some((target, _))) = engine.head(entity).await {
        return target.tenant.as_str().to_string();
    }
    if let Ok(Some((_, row))) = engine.schedule(entity).await {
        if let Some(t) = row.tenant {
            return t;
        }
    }
    cairn_store::TenantId::DEFAULT.to_string()
}

/// Ends the audit of a human command: a row that went with no commit (the
/// action changed nothing) ⇒ its own commit, with outcome `outcome` — before
/// the reply.
pub(crate) async fn audit_end(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    guard: Option<cairn_engine::AuditGuard>,
    entity: &InvocationId,
    outcome: &str,
) -> Result<()> {
    let (Some(g), Some(n)) = (guard, ctx.audit) else {
        return Ok(());
    };
    let tenant = match &n.tenant {
        Some(t) => t.clone(),
        None => audit_tenant_of(engine, entity).await,
    };
    // A no-op outcome (the command changed nothing): a staged row that went
    // with no commit is dropped, and the command is aggregated outside the
    // range (one object per tick per key, not one PUT per command).
    if matches!(outcome, "noop" | "not_found") && n.aggregates_noops() {
        // A row that already went with the action's commit (which cannot
        // happen with a no-op outcome today) is already written — do not
        // aggregate it as well.
        if engine.audit_discard(g) {
            n.aggregate_noop(&tenant, outcome);
        }
        return Ok(());
    }
    let record = n.record_for(&tenant, outcome, Some(("entity", hex(entity).into())));
    engine.audit_flush(g, Some(&record)).await?;
    Ok(())
}

/// The code of an attempt that failed for infrastructure reasons (SDK made no
/// progress, did not answer) in `lastFailure` — not a code the handler reported.
pub const INFRA_FAILURE: u32 = 500;

/// Cap on the number of outbox processing rounds within one call turn.
///
/// A chain of nested `ctx.call`s is valid; a loop calling itself is not.
/// Without a cap, the latter takes over the engine thread and the whole
/// partition stalls.
const MAX_OUTBOX_ROUNDS: usize = 64;

/// Backoff between retries.
///
/// Without backoff an SDK with a transient error is called again immediately,
/// over and over — and we turn their incident into our attack.
/// The backoff table lives in the lifecycle core (`cairn_lifecycle::backoff_ms`).
pub const RETRY_BASE_MS: u64 = lc::RETRY_BASE_MS;

/// Default cap on waiting for a slot.
///
/// Not an arbitrary number: it is "how long before a blocked turn should
/// release its invocation lock and reschedule". Too short and a normal
/// `EXCLUSIVE` queue (waiting for the previous one to finish) produces a useless
/// storm of reschedules; too long and a tenant out of budget holds its
/// invocation longer than needed.
pub const DEFAULT_QUEUE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Tells the client to come back after this long when a turn is rescheduled for lack of a slot.
///
/// Equal to the first backoff step (`cairn_lifecycle::backoff_ms` at step 0),
/// because that is exactly when the reschedule set by [`park_for_retry`] fires.
/// Saying any other number would be saying something false.
const THROTTLE_RETRY_AFTER_MS: u64 = RETRY_BASE_MS;

/// Does this handler require exclusive execution on the object key?
///
/// Three conditions, and missing one means no lock: the service is a
/// `VIRTUAL_OBJECT`, the handler declares `EXCLUSIVE`, and there is an object
/// key. Without a key there is nothing to be exclusive over.
fn is_exclusive(deployment: Option<&Deployment>, target: &Target) -> bool {
    if target.object_key.is_empty() {
        return false;
    }
    let Some(d) = deployment else {
        return false;
    };
    let Some(svc) = d.service(&target.service) else {
        return false;
    };
    if svc.ty != crate::discovery::ServiceType::VirtualObject {
        return false;
    }
    // Effective type: a VO handler that declares no `ty` is EXCLUSIVE.
    d.handler(&target.service, &target.handler)
        .and_then(|h| h.effective_ty(svc.ty))
        .is_some_and(|t| t == crate::discovery::HandlerType::Exclusive)
}

/// Is this handler a `SHARED` handler of a virtual object — it runs **without
/// holding the object lock**, in parallel with `EXCLUSIVE` handlers of the same
/// key, so it may only read state (D-W5).
///
/// Same source as [`is_exclusive`] — the deployment the invocation is pinned
/// to, immutable — so write rights and locking cannot diverge: for a VO with a
/// key, `is_exclusive` is exactly
/// `!shared_object_handler` (test `shared_object_handler_is_the_unlocked_half_of_a_virtual_object`).
/// Independent of the key: a `SHARED` handler called with an empty key is still
/// `SHARED`. A handler missing from the deployment ⇒ `false` (that turn never
/// reaches the SDK).
pub(super) fn shared_object_handler(deployment: &Deployment, target: &Target) -> bool {
    let Some(svc) = deployment.service(&target.service) else {
        return false;
    };
    if svc.ty != crate::discovery::ServiceType::VirtualObject {
        return false;
    }
    deployment
        .handler(&target.service, &target.handler)
        .and_then(|h| h.effective_ty(svc.ty))
        .is_some_and(|t| t == crate::discovery::HandlerType::Shared)
}

/// The contract of a call, read from the deployment registry.
///
/// Used only on the **creation path**. After that the contract lives in entry 0
/// and every reader reads it back from there — see [`InvocationKind`] for why it
/// is not read again here.
pub fn kind_of(deployments: &Catalog, target: &Target) -> InvocationKind {
    deployment_for(deployments, target).map_or(InvocationKind::Plain, |d| {
        d.kind(&target.service, &target.handler)
    })
}

/// Id of the `run` turn of workflow `target`: a function of `(scope, service,
/// key)`.
///
/// `None` when the workflow key is empty — no workflow carries an empty key,
/// and no partition key owns it either (D-a). The caller refuses that call
/// rather than computing an id under a made-up pk.
pub fn workflow_run_id(target: &Target) -> Option<InvocationId> {
    if target.object_key.is_empty() {
        return None;
    }
    let key = cairn_store::partition_key_for_target(
        &target.tenant,
        target.scope(),
        &target.service,
        &target.object_key,
        None,
    )?;
    Some(cairn_store::workflow_invocation_id(
        key,
        &target.tenant,
        target.scope(),
        &target.service,
        &target.object_key,
    ))
}

/// The virtual queue of a call.
///
/// **Every** invocation goes into a queue, including unscoped ones and SHARED
/// handlers — that is D-F4. What differs between them is the *resource* the
/// queue contends for, not whether they queue at all: a SHARED handler has no
/// lock to hold, an unscoped invocation has no counter to count, and both still
/// go through exactly one code path.
pub fn queue_name(
    deployments: &Catalog,
    target: &Target,
    limit_key: Option<&LimitKey>,
) -> QueueName {
    queue_name_on(deployment_for(deployments, target), target, limit_key)
}

/// Like [`queue_name`], on an already chosen deployment — the queue of a pinned
/// invocation is computed from *its deployment*, not from the current revision.
fn queue_name_on(
    deployment: Option<&Deployment>,
    target: &Target,
    limit_key: Option<&LimitKey>,
) -> QueueName {
    let exclusive = is_exclusive(deployment, target);
    QueueName {
        tenant: target.tenant.clone(),
        scope: target.scope.clone(),
        limit: limit_key.cloned(),
        exclusive,
        service: target.service.clone(),
        // A SHARED handler groups every object key into one queue — deliberately
        // reducing
        // cardinality, xem `QueueName::object_key`.
        object_key: if exclusive {
            target.object_key.clone()
        } else {
            String::new()
        },
    }
}

/// Outcome of processing one row of a drain pass (outbox, inbound) — for counting only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Row {
    /// The row is fully processed (usually deleted).
    Handled,
    /// The row stays where it is — target busy, target dead, not yet eligible,
    /// or broken — with the condition under which the drainer reads it again.
    Left(crate::drainer::Park),
}

/// Invocations with a row skipped in a drain pass because their lock is busy.
type Busy = cairn_store::entropy::HashSet<InvocationId>;

/// Tries to lock `id` for a row of a drain pass, **without waiting**.
///
/// `None` if the id was already busy earlier in this pass or the lock is held
/// (then the id goes into `busy`): every later row of the same id is skipped
/// too, so their relative order stays intact — the next pass scans again from
/// the start and meets them in creation order.
fn try_lock<'c>(ctx: &Ctx<'c>, busy: &mut Busy, id: InvocationId) -> Option<InvGuard<'c>> {
    if busy.contains(&id) {
        return None;
    }
    let guard = ctx.locks.try_invocation(id);
    if guard.is_none() {
        busy.insert(id);
    }
    guard
}

/// **The drainer's budget for one engine hold**: every engine hold of a
/// background holder must stay ≤ 50 ms.
///
/// A drain pass's chunk ends at its row cap (inbound, signal: 64) or when the
/// current hold has exceeded this budget, whichever comes first. A row is one
/// question to the core and one commit (~0.7 ms under a backlog of 3·10⁵), so
/// the row cap is enough on an idle machine; the budget covers the rest (busy
/// engine thread, `db.write` waiting to be polled again). Smaller than the 50 ms
/// cap so that the chunk's last row does not push the hold past the cap.
pub(super) const DRAIN_HOLD_BUDGET: std::time::Duration = std::time::Duration::from_millis(20);

/// The drain pass's current chunk is full: `rows` rows since the previous
/// release, cap `limit`, or the hold has exceeded [`DRAIN_HOLD_BUDGET`].
///
/// At the start of a term: the chunk is also full when a command is waiting for
/// the engine ([`Lease::command_waiting`]) and the chunk has at least one row —
/// the release point is still a point between two rows, and every hold still
/// advances at least one row under a stream of commands.
fn chunk_full(engine: &Lease, rows: usize, limit: usize) -> bool {
    rows >= limit
        || engine.held_for() >= DRAIN_HOLD_BUDGET
        || (rows > 0 && engine.command_waiting())
}

/// **Releases the engine between two chunks of a drain pass**: turns, commands,
/// background ticks waiting for the engine go first, then the drainer takes it
/// back and carries on.
///
/// Only called **between two rows** — never between asking the core and the
/// commit of one row (M4 `drain_attendance_across_release`): each row asks
/// "is anyone working on it" (`Attendance`), reads state and commits within one
/// hold. A row read but not yet judged does not cross the release point — the
/// next chunk re-reads from the cursor (a tenant purge may delete rows while
/// the engine is released). What does cross it: the drainer's ledger (a hint
/// — an absent row is forgotten on the next read), the pass's `busy`/`down`
/// (they only make the drainer **skip** more rows, keeping per-target order),
/// the watermark at the start of the pass (rows arriving later carry a `seq`
/// above it and are left for the next pass), and rows already handed to the
/// `Sender` (like waiting for a send ticket or for a target, which already
/// release the engine). Each queue has one reader — this drainer — so nobody
/// else delivers its rows while the engine is released.
async fn between_chunks(engine: &mut Lease) {
    engine.wait(tokio::task::yield_now()).await;
}

/// Between two queues of a drain pass (inbound → signal → outbox): if the hold
/// has exceeded [`DRAIN_HOLD_BUDGET`], release the engine before the next queue
/// — also when a command is waiting at the start of a term.
pub(crate) async fn release_if_over_budget(engine: &mut Lease) {
    if engine.held_for() >= DRAIN_HOLD_BUDGET || engine.command_waiting() {
        between_chunks(engine).await;
    }
}

fn hex(id: &InvocationId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Reads a scope / limit key that comes from **durable data or from another node**.
///
/// A broken value is **dropped loudly**, without failing the call. The
/// trade-off here is asymmetric: a call that loses its cap costs money and has
/// a metric to show it; a call refused forever loses work, and the user only
/// sees it vanish. The place that *must* refuse is the entry boundary (ingress,
/// SDK), where there is someone to report the error to — not here.
fn parse_ident<T, E: std::fmt::Display>(
    raw: Option<&str>,
    what: &str,
    owner: &InvocationId,
    parse: impl Fn(&str) -> std::result::Result<T, E>,
) -> Option<T> {
    let raw = raw?;
    match parse(raw) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(
                invocation = %hex(owner),
                value = raw,
                error = %e,
                "dropped invalid {what} from durable data"
            );
            None
        }
    }
}

/// The message that opens a turn, split out so tests can read it.
pub fn opening_is_start(msgs: &[Message]) -> bool {
    matches!(msgs.first(), Some(Message::Start(_)))
}

#[cfg(test)]
mod tests {
    use {
        super::{lc, marks_rescue},
        crate::actor_types::ActorError,
        cairn_engine::EngineError,
        cairn_store::StoreError,
    };

    /// FL-16: an I/O error of a turn that `drive` reschedules marks the safety
    /// net; two exceptions do not — lost write rights (the term is ending, the
    /// next term's `recover` takes over) and a turn with a reschedule set
    /// beforehand (the next reschedule was durable before the turn).
    #[test]
    fn rescue_skips_fenced_errors_and_planned_retries() {
        let store = |e: StoreError| ActorError::Engine(EngineError::Store(e));
        let plain = lc::Turn::PLAIN;
        assert!(marks_rescue(&plain, &store(StoreError::Io("503".into()))));
        assert!(!marks_rescue(
            &lc::Turn::planned(lc::Planned {
                attempt: 0,
                next_due: 0,
                journal_len: 0
            }),
            &store(StoreError::Io("503".into()))
        ));
        assert!(!marks_rescue(&plain, &ActorError::NotLeader));
        assert!(!marks_rescue(&plain, &store(StoreError::Fenced)));
        assert!(!marks_rescue(&plain, &store(StoreError::LeaseLost)));
    }

    /// A virtual-object handler that declares no `ty` must hold the VO lock
    /// like `EXCLUSIVE` (Restate's default). It used to be "untyped" and took
    /// **no** lock: an SDK following the first example in `index.ts` (no
    /// `handlerTypes`) ran two state-writing turns of the same object in
    /// parallel and lost updates (found by a billing workload).
    #[test]
    fn vo_handler_without_declared_ty_takes_object_lock() {
        use crate::discovery::{
            accept, EndpointManifest, HandlerManifest, HandlerType, ProtocolMode, ServiceManifest,
            ServiceType,
        };
        let handler = |name: &str, ty| HandlerManifest {
            name: name.into(),
            ty,
            contract: Default::default(),
        };
        let d = accept(
            "http://x",
            EndpointManifest {
                protocol_mode: ProtocolMode::RequestResponse,
                min_protocol_version: 1,
                max_protocol_version: 1,
                services: vec![ServiceManifest {
                    name: "Cart".into(),
                    ty: ServiceType::VirtualObject,
                    handlers: vec![
                        handler("add", None),
                        handler("view", Some(HandlerType::Shared)),
                        handler("checkout", Some(HandlerType::Exclusive)),
                    ],
                    contract: Default::default(),
                }],
            },
        )
        .unwrap();
        let target = |handler: &str, key: &str| cairn_engine::Target {
            tenant: cairn_store::TenantId::default(),
            scope: None,
            service: "Cart".into(),
            handler: handler.into(),
            object_key: key.into(),
        };
        assert!(super::is_exclusive(Some(&d), &target("add", "a")));
        assert!(super::is_exclusive(Some(&d), &target("checkout", "a")));
        assert!(!super::is_exclusive(Some(&d), &target("view", "a")));
        assert!(!super::is_exclusive(Some(&d), &target("add", "")));
    }

    /// State write rights and the object lock read the same source (D-W5): for
    /// every handler of a VO, called with a key, the turn holds **no** lock
    /// exactly when it is a `SHARED` handler — i.e. exactly the turns that may
    /// not write. Plain services and workflows are never touched by this rule
    /// (workflow `SHARED` is enforced in the engine, from
    #[test]
    fn shared_object_handler_is_the_unlocked_half_of_a_virtual_object() {
        use crate::discovery::{
            accept, EndpointManifest, HandlerManifest, HandlerType, ProtocolMode, ServiceManifest,
            ServiceType,
        };
        let handler = |name: &str, ty| HandlerManifest {
            name: name.into(),
            ty,
            contract: Default::default(),
        };
        let svc = |name: &str, ty, handlers| ServiceManifest {
            name: name.into(),
            ty,
            handlers,
            contract: Default::default(),
        };
        let d = accept(
            "http://x",
            EndpointManifest {
                protocol_mode: ProtocolMode::RequestResponse,
                min_protocol_version: 1,
                max_protocol_version: 1,
                services: vec![
                    svc(
                        "Cart",
                        ServiceType::VirtualObject,
                        vec![
                            handler("add", None),
                            handler("view", Some(HandlerType::Shared)),
                            handler("checkout", Some(HandlerType::Exclusive)),
                        ],
                    ),
                    svc("Echo", ServiceType::Service, vec![handler("hi", None)]),
                    svc(
                        "Signup",
                        ServiceType::Workflow,
                        vec![
                            handler("run", Some(HandlerType::Workflow)),
                            handler("click", Some(HandlerType::Shared)),
                        ],
                    ),
                ],
            },
        )
        .unwrap();
        let target = |service: &str, handler: &str, key: &str| cairn_engine::Target {
            tenant: cairn_store::TenantId::default(),
            scope: None,
            service: service.into(),
            handler: handler.into(),
            object_key: key.into(),
        };
        for h in ["add", "view", "checkout"] {
            let t = target("Cart", h, "a");
            assert_eq!(
                super::is_exclusive(Some(&d), &t),
                !super::shared_object_handler(&d, &t),
                "Cart/{h}"
            );
        }
        assert!(super::shared_object_handler(
            &d,
            &target("Cart", "view", "a")
        ));
        assert!(super::shared_object_handler(
            &d,
            &target("Cart", "view", "")
        ));
        assert!(!super::shared_object_handler(
            &d,
            &target("Cart", "add", "")
        ));
        assert!(!super::shared_object_handler(&d, &target("Echo", "hi", "")));
        assert!(!super::shared_object_handler(
            &d,
            &target("Signup", "run", "u")
        ));
        assert!(!super::shared_object_handler(
            &d,
            &target("Signup", "click", "u")
        ));
        assert!(!super::shared_object_handler(
            &d,
            &target("Cart", "missing", "a")
        ));
    }
}
