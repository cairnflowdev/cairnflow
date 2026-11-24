// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `!Send` boundary — the engine lives on its own thread.
//!
//! [`cairn_store::PartitionWriter`] is deliberately **not** `Send` (INV-4). So it cannot go
//! into an axum handler running on a thread pool. That is not an inconvenience to work
//! around: it is exactly what we want — the writer lives in exactly one task, and the
//! compiler guarantees it instead of human discipline.
//!
//! The consequence is the actor style: the engine sits on one thread with a
//! `current_thread` runtime, the rest talks to it through a channel. [`EngineHandle`] is
//! `Clone + Send`, because it is only the sending end of the channel — it grants no write
//! authority.
//!
//! # One thread, many tasks
//!
//! This loop does **not** handle commands in place. It receives a command, then
//! `spawn_local`s a task, and that task goes to win the engine through
//! [`crate::lease::Lease`].
//!
//! It used to handle them in place, and the price was that a 30-second SDK call made
//! everything else in the partition queue for 30 seconds — including
//! `/invocations/{id}`, exactly what an operator needs most during an incident. Now that
//! turn releases the engine while waiting for the SDK, so a status query can slip in
//! between.
//!
//! Three things are unchanged, and they are why this change does not touch correctness:
//!
//! - **One writer, one thread.** `LocalSet` + a `current_thread` runtime: no task runs on
//!   another thread, and [`Engine`] is still `!Send`, so the compiler is still what
//!   enforces it.
//! - **One turn per invocation.** The per-id lock in [`crate::lease`].
//! - **Queue scans are still sequential.** The drain lock, in the same place.
//!
//! What *changed*: turns of two different invocations can interleave, and the background
//! tick no longer stands behind a slow SDK.

use {
    crate::{
        engine_pool::EnginePool,
        flow::Scheduler,
        invoker,
        lease::{engine_lock, EngineLock, Lease, Locks},
        registry::Registry,
        transport::Transport,
        turn,
    },
    bytes::Bytes,
    cairn_engine::{
        Engine, EngineError, InterPartition, InvocationStatus, JournalRow, NextDue, Target,
    },
    cairn_lifecycle as lc,
    cairn_store::{LeaseKeeper, LimitKey, PartitionKey, RangeEntry, RuleStore, Scope, TenantId},
    std::{
        cell::Cell,
        future::Future,
        pin::Pin,
        rc::Rc,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::{
        sync::{mpsc, oneshot, Notify},
        task::JoinSet,
    },
};

pub use crate::actor_types::*;

mod command;
use command::*;
mod handle;
pub use handle::*;
mod leadership;
pub use leadership::*;
mod serve;
pub use serve::*;
mod dispatch;
use dispatch::*;
mod background;
use background::*;
mod waiters;
pub use waiters::{Feeding, WaitGuard, Waiters, Watch};

/// How a partition's lifecycle runs — grouped because these parameters always travel
/// together through three layers of functions, and the next one added would be missed in
/// one place.
struct LoopOptions {
    tick_interval: Option<Duration>,
    readiness: Readiness,
    limits: TurnLimits,
    /// The range is retiring — see [`EngineHandle::mark_retiring`].
    retiring: Arc<AtomicBool>,
    /// `writer_epoch` of the most recently sealed writer — see
    /// [`EngineHandle::sealed_writer_epoch`]. `0` = never sealed.
    sealed_epoch: Arc<AtomicU64>,
    /// The node's turn cap (D-c), if the pool has one.
    budget: Option<crate::engine_pool::TurnBudget>,
    /// The node's range lifecycle measurements.
    stats: crate::range_stats::RangeStats,
    /// The node's per-handler summary.
    handlers: crate::stats::HandlerStats,
    /// The node's per-range load book.
    load: crate::autosplit::LoadBook,
    /// The node's stream RAM cap: one hub per term.
    streams: crate::streams::StreamBook,
    /// The range's registry of outcome waiters — each term hooks it into the new writer.
    waiters: Waiters,
    /// The node's registry of read handles for the query layer.
    query_ranges: crate::query::QueryRanges,
    /// The node's egress policy.
    egress: Arc<crate::egress::EgressPolicy>,
    /// The node's request identity signer.
    identity: crate::identity::Identity,
    /// Issuer of segment read URLs for SDK v6.
    value_urls: Arc<crate::value_urls::ValueUrls>,
}

/// The two brakes of the SDK call path.
///
/// They solve two different problems and **both are needed**: the concurrency cap bounds
/// the *number* of turns in flight, the time cap bounds the *duration* of one turn. With
/// only the first, 32 hung handlers still bring the partition to a halt, just more slowly.
#[derive(Clone, Copy, Debug)]
pub struct TurnLimits {
    /// Cap on the number of client-initiated turns running at once.
    pub max_concurrent_turns: usize,
    /// Cap on waiting for the SDK within a turn. `None` = no cap.
    ///
    /// The meaning differs slightly between the two modes, and the difference is real:
    ///
    /// - `BIDI_STREAM`: restarted on **every** frame ⇒ truly "no progress". A handler that
    ///   sends regularly can run as long as it likes.
    /// - `REQUEST_RESPONSE`: there is no progress signal mid-way ⇒ it is a cap on **the whole
    ///   turn**.
    ///
    /// The only legitimate silence within a turn is the closure of a `ctx.run`: `ctx.sleep`
    /// and `ctx.call` end the turn (suspend) rather than holding the connection. So this
    /// number is really "the longest `ctx.run` closure you allow".
    pub sdk_timeout: Option<Duration>,
    /// Cap on waiting for a **slot** in the queue. `None` = wait forever.
    ///
    /// The third brake, and it solves a different problem from the two above: not "how many
    /// turns in flight" or "how long one turn", but "how long a turn that *cannot start yet*
    /// holds its place". On hitting the cap the turn is rescheduled and the client gets
    /// `429` — the invocation is still durable, still in the queue.
    pub queue_wait: Option<Duration>,
    /// Writes not yet durable while `durable_seq` stands still for longer than this ⇒ step
    /// down. `None` = not watched.
    ///
    /// SlateDB retries object store errors **forever** by default, so an unreachable store
    /// shows up as *waiting for durability forever* rather than as an error. If the incident
    /// is only on this node (its network to the store is cut), stepping down lets another
    /// node — one with a path to the store — take over. If the store is dead for every node
    /// then stepping down only costs one re-acquisition; that is why the cap is long
    /// (default 30s) rather than short.
    pub durability_stall: Option<Duration>,
    /// Soft throttle before the L0 wall — see [`crate::l0`].
    pub l0_throttle: crate::l0::L0ThrottleConfig,
    /// The safety net's `paused` 577 threshold — see [`crate::drainer::InfraStuck`]. No
    /// command-line flag: tests lower it.
    pub infra_stuck: crate::drainer::InfraStuck,
    /// Reschedule a turn of a `suspended` tenant after this long — default
    /// [`crate::flow::SUSPENDED_RETRY`]. No command-line flag: tests lower it.
    pub suspended_retry: Duration,
    /// Incremental replay of `REQUEST_RESPONSE` (wire v5). `false` = `--no-rr-delta-replay`:
    /// every turn replays in full as before v5.
    pub rr_delta_replay: bool,
    /// Cap on the running turns of **one** tenant on the whole node (flag
    /// `--max-turns-per-tenant`). `None` = no cap — a self-hosted single tenant. SaaS
    /// profile: ¼ of the node's slots.
    pub max_turns_per_tenant: Option<usize>,
    /// Upper bound on the SDK wait cap a contract may declare (`inactivityTimeout`; flag
    /// `--inactivity-timeout-max-ms`) — default
    /// [`crate::discovery::INACTIVITY_TIMEOUT_MAX_MS`] (1 hour), SaaS profile 15 minutes.
    pub sdk_timeout_max: Duration,
    /// Status index rows per chunk of the VO lock rebuild on becoming leader
    /// (`actor::restore_flow`) — default [`cairn_engine::RESTORE_CHUNK`]. No command-line
    /// flag: tests lower it to slip commands in between two chunks.
    pub restore_chunk: usize,
    /// `Invocations` index rows per chunk of the `running` reconciliation
    /// (`actor::reconcile_running`: `recover`, the safety net's full scan) — default
    /// [`cairn_engine::RECOVER_CHUNK`]. No command-line flag: tests lower it so `recover` is
    /// replaced at every chunk boundary.
    pub recover_chunk: usize,
}

/// Default cap for [`TurnLimits::durability_stall`].
pub const DEFAULT_DURABILITY_STALL: Duration = Duration::from_secs(30);

impl Default for TurnLimits {
    fn default() -> Self {
        Self {
            max_concurrent_turns: DEFAULT_MAX_CONCURRENT_TURNS,
            sdk_timeout: Some(DEFAULT_SDK_TIMEOUT),
            queue_wait: Some(turn::DEFAULT_QUEUE_WAIT),
            durability_stall: Some(DEFAULT_DURABILITY_STALL),
            l0_throttle: crate::l0::L0ThrottleConfig::default(),
            infra_stuck: crate::drainer::InfraStuck::default(),
            suspended_retry: crate::flow::SUSPENDED_RETRY,
            rr_delta_replay: true,
            max_turns_per_tenant: None,
            sdk_timeout_max: Duration::from_millis(crate::discovery::INACTIVITY_TIMEOUT_MAX_MS),
            restore_chunk: cairn_engine::RESTORE_CHUNK,
            recover_chunk: cairn_engine::RECOVER_CHUNK,
        }
    }
}

/// Cap on the number of client-initiated turns (`Invoke`, `CancelSignal`) running at once
/// in one partition.
///
/// There is a cap because each turn in flight is a connection to the SDK: unchecked, a
/// load spike becomes an attack on the customer's own deployment. Background paths (timer,
/// retry, recovery, the drainer) run no turns of their own: they hand the invocation to
/// the scheduler, and that turn asks for **the same** slots as a client-initiated turn.
/// This cap therefore bounds every SDK connection of the partition, not only those coming
/// from clients.
///
/// This cap limits *connections to the SDK*, not the wait line: the command channel (64
/// places) drains right away because commands are spawned into tasks, so tasks waiting for
/// a slot pile up in a `JoinSet` with no cap of their own. What keeps that number finite is
/// that each ingress client is waiting on its own oneshot — the real backpressure is at the
/// incoming connection end.
pub const DEFAULT_MAX_CONCURRENT_TURNS: usize = 32;

pub type InvocationId = cairn_store::InvocationId;

/// One row of an invocation listing: id, state, and routing target (when entry 0 is still
/// readable).
pub type InvocationRow = (InvocationId, InvocationStatus, Option<Target>);

/// Result of `ListInvocations`: the rows, plus the two flags of a bounded scan — see
/// [`cairn_engine::Listing`].
#[derive(Clone, Debug, Default)]
pub struct ListAnswer {
    pub rows: Vec<InvocationListing>,
    pub untimed: usize,
    pub capped: bool,
}

/// One row of `/invocations` plus the two things an operator asks about next: the
/// deployment the invocation is bound to and the consecutive failed attempts of the pending
/// reschedule (0 = no reschedule).
#[derive(Clone, Debug)]
pub struct InvocationListing {
    pub id: InvocationId,
    pub status: InvocationStatus,
    pub target: Option<Target>,
    pub deployment: Option<String>,
    pub attempt: u32,
    /// `createdAt`/`modifiedAt`.
    pub times: cairn_engine::Times,
}

/// What `GET /invocations/{id}` needs, read in **one** turn and returned through **one**
/// read fence: the target (when the asker needs to know the tenant), the state, and the
/// latest attempt's error while the invocation is still running. Three separate commands
/// would give three answers, each with its own fenced GET; one proof issued after the last
/// read covers all three reads of the same turn.
#[derive(Clone, Debug, Default)]
pub struct InvocationView {
    /// `None` when not asked, or when there is no such invocation.
    pub target: Option<Target>,
    pub status: Option<InvocationStatus>,
    /// Only read when the state is `running`/`suspended`/`paused`; on a read error the
    /// field is dropped, the whole answer does not fail.
    pub last_failure: Option<cairn_store::keys::LastFailure>,
    /// When the pending scheduled retry is due — only while `running` and backing off (a
    /// watch row is not a retry: an attempt is in progress). Dropped on a read error, like
    /// `last_failure`.
    pub next_attempt_ms: Option<u64>,
    /// `createdAt`/`modifiedAt` — with the same two GETs as the target and the state,
    /// through `get_meta`.
    pub times: cairn_engine::Times,
    /// User tags, from the metadata row.
    pub tags: Vec<(String, String)>,
}

/// What a stream reader needs from the leader: the hub of the term leading the range, the
/// target (tenant) and the state of the invocation — one turn, one read fence.
#[derive(Clone, Debug)]
pub struct StreamAccess {
    pub hub: Arc<crate::streams::RangeStreams>,
    pub target: Option<Target>,
    pub status: Option<InvocationStatus>,
}

/// One page of chunks of a durable stream read from the Db on the leader: the chunks with
/// records from `from` onwards, and the invocation state so the reader knows whether it has
/// ended.
#[derive(Clone, Debug)]
pub struct DurablePage {
    /// `(first_offset, chunk)` in order; a corrupt chunk is `None`.
    pub chunks: Vec<(u64, Option<cairn_store::keys::StreamChunk>)>,
    /// Whether the stream has any chunk in the Db (even when `chunks` is empty because
    /// `from` is past all of them).
    pub exists: bool,
    pub status: Option<InvocationStatus>,
}

/// The artifact rows of an invocation read on the leader: `(sha, row)` — a corrupt row is
/// `None`.
pub type ArtifactRows = Vec<([u8; 32], Option<cairn_store::keys::ArtifactRow>)>;

/// [`ArtifactRows`] plus the range's artifact cache, so the read route can fill the cache
/// without going back to the actor.
pub struct ArtifactLookup {
    pub rows: ArtifactRows,
    pub cache: Option<std::sync::Arc<cairn_store::artifact::ArtifactCache>>,
}

/// The first half of an artifact deletion proof, or why it cannot be proved.
pub type UnreferencedArtifacts =
    std::result::Result<cairn_store::proof::ArtifactsUnreferenced, String>;

/// The first half of a segment deletion proof, or why it cannot be proved.
pub type ReferencedValues = std::result::Result<cairn_store::proof::ValuesReferenced, String>;

/// Leadership of a partition, fully opened and ready to run.
pub struct Leadership {
    pub engine: Engine,
    /// `None` for a single-node deployment: there is no lease layer to lose.
    pub keeper: Option<LeaseKeeper>,
}

/// The engine constructor. The closure itself is `Send` (it can cross threads), but the
/// future it returns is not — [`Engine`] is `!Send`, and it is created *inside* the thread.
///
/// `Fn` rather than `FnOnce`: losing leadership and winning it back is normal in a cluster,
/// and each win must open a new [`Engine`]. The old writer has been `seal`ed and there is no
/// way to revive it — that is the point of `seal(self)`.
pub type EngineFactory = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<Leadership, EngineError>>>> + Send,
>;

/// When [`EngineHandle::spawn`] reports "done".
///
/// The two deployment shapes call for two different answers, and picking the wrong one
/// fails in two different ways — so it is an explicit parameter, not an implicit default.
pub enum Readiness {
    /// Wait until leadership is won.
    ///
    /// Right for **a single node**: there is nobody to compete with, so failing to open the
    /// store is a real startup error. Hiding it and returning `NotLeader` forever is a far
    /// worse way to fail — the binary keeps running, the port stays open, every request
    /// fails, and there is no error line where people look.
    WhenLeader,
    /// Report right away, without leadership yet.
    ///
    /// Right for **a cluster**: a node that cannot win the lease must still serve HTTP — it
    /// is a legitimate follower, and it returns `NotLeader` for write commands (principle
    /// N3). Blocking here would turn every follower into a hung process.
    Immediately,
}
