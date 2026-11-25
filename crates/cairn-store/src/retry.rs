// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared retry and time bounds.
//!
//! Two primitives, built on [`crate::pace`] (no second backoff implementation):
//!
//! - [`run`] — retries an operation with **discipline**: error classification
//!   ([`Verdict`]: transient / permanent / ambiguous), jittered exponential backoff
//!   ([`crate::pace::Backoff`], random bytes through [`crate::entropy`]), a cap on the
//!   number of attempts **and/or** on the total time (each attempt is cut to what is
//!   left), a per-attempt bound, cancellable ([`Cancel`]: a stop signal, a lost lease).
//! - [`deadline`] — one bounded wait, no retry. For places where retrying is the caller's
//!   job (a background loop moves on to its next round, a coordinator backs off).
//!
//! **Ambiguity is not retried blindly** (rule 7 of AGENTS). An error the classifier calls
//! [`Verdict::Ambiguous`] — a write may have landed — is returned at once to the caller to
//! resolve by re-reading. A conditional write placed **inside** `op` goes through
//! [`crate::cas::put_resolving`], so the error it returns is already the error after
//! resolution. The per-attempt bound ([`Policy::attempt_bound`]) drops the future midway:
//! use it only for reads, or read-back-before-write operations (repeatable).
//!
//! **Metrics by operation name.** Each call site declares a static [`Op`] in [`ops`] (a
//! closed table, like `failpoint::NAMES`), and the counters are atomics — no locks, no
//! allocation. `/metrics` prints every `Op` of [`ops::ALL`], even when 0.
//!
//! **The clock** is the tokio runtime's timer (as in `pace`): under `start_paused`
//! (simulation, unit tests) every bound counts in virtual time.

use {
    crate::{error::StoreError, pace::Backoff},
    std::{
        future::Future,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    },
    tokio::time::Instant,
};

/// What kind an error is — decides whether to retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Transient (503, network, one attempt timing out): may be retried.
    Transient,
    /// Retrying cannot change the outcome (`Corrupt`, `Fenced`, tombstone…): return at once.
    Permanent,
    /// May have written: **no** blind retry — return at once, the caller resolves by
    /// re-reading (rule 7).
    Ambiguous,
}

/// An error [`run`] can retry: it classifies itself, and can build a "timed out" error when
/// the primitive itself cuts an attempt.
pub trait Retryable: Sized {
    fn verdict(&self) -> Verdict;
    /// The error for an attempt cut after `after` (the per-attempt bound, or what is left of
    /// the total bound). Should be transient: the next attempt may make it.
    fn timed_out(op: &'static str, after: Duration) -> Self;
}

impl Retryable for StoreError {
    fn verdict(&self) -> Verdict {
        store_verdict(self)
    }
    fn timed_out(op: &'static str, after: Duration) -> Self {
        StoreError::Io(format!("{op}: took longer than {after:?}"))
    }
}

/// Classifies store errors (formerly `node::is_transient_io`, LS-12). Each layer wraps
/// store errors its own way: `Io` (admin objects, `cluster-version`), `LeaseIo`
/// (`cluster.conf`, owner records), SlateDB's `Unavailable` (manifest). Accepting only `Io`
/// meant reloading the table was never retried (RD1). Everything else is permanent:
/// retrying cannot change `Corrupt`, `Fenced`, a tombstone.
pub fn store_verdict(e: &StoreError) -> Verdict {
    match e {
        StoreError::Io(_) | StoreError::LeaseIo(_) => Verdict::Transient,
        StoreError::Slate(e) if matches!(e.kind(), slatedb::ErrorKind::Unavailable) => {
            Verdict::Transient
        }
        _ => Verdict::Permanent,
    }
}

/// Classifies raw object_store errors (the probe gate, opening a session — places still
/// holding the original error not yet wrapped into [`StoreError`]). `Generic` is a 5xx after
/// the client's retry loop, a network error, a timeout: transient. Everything else is a
/// meaningful answer from the store (`NotImplemented` of `LocalFileSystem` for CAS,
/// `Precondition`, `AlreadyExists`, `PermissionDenied`, …): retrying changes nothing.
pub fn object_store_verdict(e: &slatedb::object_store::Error) -> Verdict {
    match e {
        slatedb::object_store::Error::Generic { .. } => Verdict::Transient,
        _ => Verdict::Permanent,
    }
}

impl Retryable for slatedb::object_store::Error {
    fn verdict(&self) -> Verdict {
        object_store_verdict(self)
    }
    fn timed_out(op: &'static str, after: Duration) -> Self {
        slatedb::object_store::Error::Generic {
            store: "retry",
            source: format!("{op}: took longer than {after:?}").into(),
        }
    }
}

/// The retry policy of one call site. It must have at least one bound (attempt count or
/// total time) — [`Policy::new`] does not allow building an unbounded policy.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// The first rest; doubles up to [`Self::max_pause`], jitter `[d/2, d]`.
    pub first: Duration,
    pub max_pause: Duration,
    /// Cap on the number of attempts, **including** the first. `u32::MAX` = time bound only.
    pub attempts: u32,
    /// Total bound counted from the first attempt, rests included. The last attempt is cut
    /// to what is left.
    pub total: Option<Duration>,
    /// Per-attempt bound. Only for reads or repeatable operations (see the module header).
    pub attempt_bound: Option<Duration>,
}

impl Policy {
    /// A policy with an attempt cap and no time bound.
    pub const fn new(first: Duration, max_pause: Duration, attempts: u32) -> Self {
        assert!(attempts >= 1, "at least one attempt");
        Self {
            first,
            max_pause,
            attempts,
            total: None,
            attempt_bound: None,
        }
    }

    /// A policy with only a total time bound.
    pub const fn for_duration(first: Duration, max_pause: Duration, total: Duration) -> Self {
        Self {
            first,
            max_pause,
            attempts: u32::MAX,
            total: Some(total),
            attempt_bound: None,
        }
    }

    pub const fn with_total(mut self, total: Duration) -> Self {
        self.total = Some(total);
        self
    }

    pub const fn with_attempt_bound(mut self, bound: Duration) -> Self {
        self.attempt_bound = Some(bound);
        self
    }
}

/// The cancellation signal of a retry loop: checked before each retry, and it interrupts
/// a rest. It does **not** cut an attempt in flight (that is the job of
/// [`Policy::attempt_bound`]): cancelling in the middle of a write would add one more
/// ambiguous outcome.
pub trait Cancel {
    fn is_cancelled(&self) -> bool;
    /// Returns once cancelled. Default: never.
    fn cancelled(&self) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

/// Cannot be cancelled.
pub struct Never;

impl Cancel for Never {
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Lease lost ⇒ stop retrying: the next attempt no longer has any right to do anything. A
/// suspension does not cancel: on revival the work being retried is still ours.
impl Cancel for &crate::session::LeaseKeeper {
    fn is_cancelled(&self) -> bool {
        self.is_lost()
    }
    fn cancelled(&self) -> impl Future<Output = ()> + Send {
        self.lost()
    }
}

impl<C: Cancel + Sync> Cancel for Option<C> {
    fn is_cancelled(&self) -> bool {
        self.as_ref().is_some_and(C::is_cancelled)
    }
    async fn cancelled(&self) {
        match self {
            Some(c) => c.cancelled().await,
            None => std::future::pending().await,
        }
    }
}

/// A `watch<bool>` stop signal (background loops of the session, the node): `true` = stop.
impl Cancel for tokio::sync::watch::Receiver<bool> {
    fn is_cancelled(&self) -> bool {
        *self.borrow()
    }
    fn cancelled(&self) -> impl Future<Output = ()> + Send {
        let mut rx = self.clone();
        async move {
            // The sender dropped = nobody can stop us any more: wait forever.
            if rx.wait_for(|stop| *stop).await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Tries `op` according to `policy`, classifying errors with [`Retryable::verdict`].
pub async fn run<T, E, F, Fut>(
    name: &'static Op,
    policy: &Policy,
    cancel: impl Cancel,
    op: F,
) -> Result<T, E>
where
    E: Retryable + std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    run_classified(name, policy, cancel, E::verdict, op).await
}

/// Like [`run`], with the call site's own classifier.
pub async fn run_classified<T, E, F, Fut>(
    name: &'static Op,
    policy: &Policy,
    cancel: impl Cancel,
    classify: impl Fn(&E) -> Verdict,
    mut op: F,
) -> Result<T, E>
where
    E: Retryable + std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let t0 = Instant::now();
    let end = policy.total.map(|t| t0 + t);
    let mut backoff = Backoff::new(policy.first, policy.max_pause);
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        // Bound of this attempt: the per-attempt bound, cut to what is left of the total.
        let left = end.map(|e| e.saturating_duration_since(Instant::now()));
        let bound = match (policy.attempt_bound, left) {
            (Some(a), Some(l)) => Some(a.min(l)),
            (a, l) => a.or(l),
        };
        let res = match bound {
            None => op().await,
            Some(b) => match tokio::time::timeout(b, op()).await {
                Ok(r) => r,
                Err(_) => Err(E::timed_out(name.name, b)),
            },
        };
        let err = match res {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        match classify(&err) {
            Verdict::Transient => {}
            Verdict::Permanent => {
                name.permanent.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
            Verdict::Ambiguous => {
                name.ambiguous.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(op = name.name, attempt = attempt, error = %err, "ambiguous outcome — returned to the caller to resolve by re-reading");
                return Err(err);
            }
        }
        if cancel.is_cancelled() {
            return Err(err);
        }
        // Count the attempts before drawing jitter: when out of attempts, no byte of
        // `entropy` is drawn (the same simulation trace as the old helper).
        let now = Instant::now();
        let pause = (attempt < policy.attempts).then(|| backoff.next_pause());
        let out_of_time = match (pause, end) {
            (Some(p), Some(e)) => now + p >= e,
            _ => false,
        };
        let Some(pause) = pause.filter(|_| !out_of_time) else {
            name.exhausted.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                op = name.name,
                attempt = attempt,
                elapsed_ms = now.duration_since(t0).as_millis() as u64,
                error = %err,
                "retry bound exhausted"
            );
            return Err(err);
        };
        name.retries.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(op = name.name, attempt = attempt, pause_ms = pause.as_millis() as u64, error = %err, "retrying");
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(err),
            () = crate::pace::retry_pause(pause) => {}
        }
    }
}

/// Waits for `fut` at most `bound`. Past the bound ⇒ `Err(Elapsed)`, counted into
/// `exhausted` of `name`. The future is dropped midway: for a write the outcome is
/// **ambiguous** — the caller resolves it by rule 7 (re-read, or a later conditional write
/// will miss on its own record).
pub async fn deadline<T>(
    name: &'static Op,
    bound: Duration,
    fut: impl Future<Output = T>,
) -> Result<T, Elapsed> {
    match tokio::time::timeout(bound, fut).await {
        Ok(v) => Ok(v),
        Err(_) => {
            name.exhausted.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                op = name.name,
                limit_ms = bound.as_millis() as u64,
                "wait bound exceeded"
            );
            Err(Elapsed {
                op: name.name,
                bound,
            })
        }
    }
}

/// One [`deadline`] that ran past its bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed {
    pub op: &'static str,
    pub bound: Duration,
}

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} took longer than {:?}", self.op, self.bound)
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for StoreError {
    fn from(e: Elapsed) -> Self {
        StoreError::Io(e.to_string())
    }
}

/// Counters of one retried / bounded operation. Declared statically in [`ops`].
pub struct Op {
    pub name: &'static str,
    retries: AtomicU64,
    exhausted: AtomicU64,
    permanent: AtomicU64,
    ambiguous: AtomicU64,
}

/// Snapshot of the counters of one [`Op`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpCounts {
    /// **Re**tries (the first attempt not counted).
    pub retries: u64,
    /// Bounds exhausted: attempt count, total time, or a [`deadline`].
    pub exhausted: u64,
    /// Stops because of a permanent error.
    pub permanent: u64,
    /// Stops because of an ambiguous outcome.
    pub ambiguous: u64,
}

impl Op {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            retries: AtomicU64::new(0),
            exhausted: AtomicU64::new(0),
            permanent: AtomicU64::new(0),
            ambiguous: AtomicU64::new(0),
        }
    }

    /// Counts a retry where the call site does its own backoff (loops with their own
    /// policy, like the leadership loop — `pace::Backoff` directly).
    pub fn note_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an exhausted bound of a loop with its own policy.
    pub fn note_exhausted(&self) {
        self.exhausted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn counts(&self) -> OpCounts {
        OpCounts {
            retries: self.retries.load(Ordering::Relaxed),
            exhausted: self.exhausted.load(Ordering::Relaxed),
            permanent: self.permanent.load(Ordering::Relaxed),
            ambiguous: self.ambiguous.load(Ordering::Relaxed),
        }
    }
}

/// The closed table of every operation using the primitive — the name is the metric's
/// `op` label. Adding a call site means adding an `Op` here and to [`ALL`].
pub mod ops {
    use super::Op;

    /// One read of an engine open (`node::open_io`, LS-12).
    pub static OPEN_IO: Op = Op::new("node.open_io");
    /// One I/O of a shared WAL log seal (`nodewal::retry_io`, NW-3).
    pub static SEAL_IO: Op = Op::new("nodewal.seal_io");
    /// Writing "released" in the background (`session::release_loop`).
    pub static SESSION_RELEASE: Op = Op::new("session.release");
    /// The keeper's in-place release (`LeaseKeeper::release`/`retire`).
    pub static KEEPER_RELEASE: Op = Op::new("keeper.release");
    /// The clean owner-record CAS after the seal (`LeaseKeeper::writer_sealed`).
    pub static KEEPER_SEALED: Op = Op::new("keeper.writer_sealed");
    /// Writing the terminal state of a session.
    pub static SESSION_TERMINAL: Op = Op::new("session.terminal");
    /// Flushing the memtable on request, holding the engine.
    pub static FLUSH_MEMTABLE: Op = Op::new("actor.flush_memtable");
    /// Clone/union while the source is quiesced.
    pub static SPLIT_CLONE: Op = Op::new("split.clone");
    /// Claiming the child ranges in the session.
    pub static SPLIT_CLAIM: Op = Op::new("split.claim");
    /// Rolling back an intent in the background.
    pub static SPLIT_SETTLE: Op = Op::new("split.settle");
    /// A failed engine open ⇒ the leadership loop backs off and takes the range again
    /// (only retries are counted).
    pub static OPEN_ENGINE: Op = Op::new("actor.open_engine");
    /// One SlateDB `Admin` operation (reading the manifest, checkpoints, the WAL mark).
    pub static SLATE_ADMIN: Op = Op::new("slatedb.admin");
    /// The outer loop of a shared WAL log seal (only exhausted bounds are counted).
    pub static SEAL_ROUNDS: Op = Op::new("nodewal.seal_rounds");
    // The boot path: one `Op` per step, sharing one boot bound
    // (`node::BOOT_IO_DEADLINE`).
    /// The conditional PUT and CAS probe gate (`config/gate.rs`).
    pub static BOOT_GATE: Op = Op::new("boot.gate");
    /// The bucket's `format` mark.
    pub static BOOT_FORMAT: Op = Op::new("boot.format");
    /// Opening a session incarnation: LIST `sessions/<node>/`, `Create` of the record.
    pub static BOOT_SESSION: Op = Op::new("boot.session");
    /// The cluster-tier migration (`cluster-version`).
    pub static BOOT_MIGRATE: Op = Op::new("boot.migrate");
    /// Opening the range table (`cluster.conf`).
    pub static BOOT_TABLE: Op = Op::new("boot.table");
    /// The cluster secret (`auth/cluster-secret`).
    pub static BOOT_SECRET: Op = Op::new("boot.secret");
    /// The key store (`auth/keys`).
    pub static BOOT_AUTH: Op = Op::new("boot.auth");
    /// Tenant flags (`tenants`).
    pub static BOOT_TENANTS: Op = Op::new("boot.tenants");
    /// Per-range flush windows (`range-windows`).
    pub static BOOT_WINDOWS: Op = Op::new("boot.windows");
    /// One read of the state scanner / `verify`: an entry failing past its bound is
    /// skipped and counted, without failing the whole round.
    pub static SCAN_READ: Op = Op::new("scan.read");
    /// Draining a Db's native WAL into SSTs in the `wal/shared` migration step: write the
    /// fence, drain the WAL, flush the memtable, close.
    pub static WAL_DRAIN: Op = Op::new("migrate.wal_drain");
    /// SlateDB `build` (writer, reader) and closing the Db after a failed range
    /// acquisition — bound [`crate::config::DEFAULT_OPEN_DB_BOUND`].
    pub static OPEN_DB: Op = Op::new("store.open_db");
    /// Flushing the memtable up to a wal id (`nodewal::flush_through`): range acquisition
    /// and the forced flush loop by frame age.
    pub static FLUSH_THROUGH: Op = Op::new("nodewal.flush_through");
    /// Claiming an id for a rotated stream (`Create` of floor 0 of the new log + LIST).
    pub static ROTATE_CLAIM: Op = Op::new("nodewal.rotate_claim");
    /// Sealing a rotated stream's log itself, in the background.
    pub static ROTATE_SEAL: Op = Op::new("nodewal.rotate_seal");
    /// Retrying the whole id claim for a rotated slot that has no stream yet, in the
    /// background: a failed claim does not fail-stop.
    pub static ROTATE_RETRY: Op = Op::new("nodewal.rotate_retry");
    /// Cleaning incarnation markers older than the one just claimed
    /// (`session/incarnation.rs`).
    pub static SESSION_MARKER: Op = Op::new("session.marker");
    /// PUT and HEAD of the probe object of the GC clock-skew gate
    /// (`cairn-server/src/gc.rs:measure_clock_skew`).
    pub static GC_CLOCK_PROBE: Op = Op::new("gc.clock_probe");
    /// Conditional GET of the node's own session record before a batch of replies on the
    /// leader (`session/fence.rs`).
    pub static SESSION_READ_FENCE: Op = Op::new("session.read_fence");
    /// One I/O of the coordinator role (`session/coordinator.rs`): reading/CASing
    /// `control/coordinator`, the fence. Bound TTL/3.
    pub static CONTROL_COORDINATOR: Op = Op::new("control.coordinator");
    /// A failed bump of the `control/registry-gen` generation, retried in the background
    /// until it wins or the generation has moved past (cairn-server `registry_gen.rs`).
    pub static REGISTRY_GEN: Op = Op::new("registry.gen");
    /// Reading the `Server` header of the S3 endpoint at startup to recognise a store
    /// verified for S3-2 (cairn-server `store_check.rs`).
    pub static BOOT_STORE_ID: Op = Op::new("boot.store_id");
    /// Waiting for the SDK to close its half after the runtime lets go of a
    /// `BIDI_STREAM` stream (cairn-server `invoker::BidiSession`, bound
    /// `BIDI_CLOSE_GRACE`). Past the bound the runtime sends `RST_STREAM` — each one counts
    /// against the rapid-reset limit of the SDK side's h2 server.
    pub static SDK_BIDI_CLOSE: Op = Op::new("sdk.bidi_close");
    /// One artifact sweep round (cairn-server `artifact_sweep.rs`): LIST by page, ask the
    /// engine, hold in place / really delete. Bound on the whole round; past it the next
    /// tick starts over.
    pub static ARTIFACT_SWEEP: Op = Op::new("artifact.sweep");
    /// One values sweep round (cairn-server `value_sweep.rs`): like [`ARTIFACT_SWEEP`].
    pub static VALUE_SWEEP: Op = Op::new("value.sweep");
    /// Warming the disk cache when acquiring a range (`disk_cache::DiskCache::warm`): once,
    /// bound `WARM_DEADLINE`; past it, give up.
    pub static DISK_CACHE_WARM: Op = Op::new("disk_cache.warm");
    /// Waiting for the head of the leader's response when ingress forwards a request
    /// (cairn-server `ingress/forward.rs`). Past the bound ⇒ an ambiguous `504` — the request
    /// may have run.
    pub static INGRESS_FORWARD: Op = Op::new("ingress.forward");
    /// The replay attempt calling the SDK at ingress: one bounded wait, so a hung SDK does
    /// not hold the request forever.
    pub static SDK_REPLAY: Op = Op::new("sdk.replay");
    /// The node's invocation list (cairn-server `ingress/invocations.rs::
    /// local_invocations`, `ingress/observe.rs::cluster_invocations`): bound 4 s per range,
    /// 5 s for the whole local part of the cluster-wide list. Past the bound ⇒ the range (or
    /// node) is missing from the answer, `partial: true` / `x-cairn-partial: true`.
    pub static INGRESS_LIST_LOCAL: Op = Op::new("ingress.list_local");
    /// The rule book GET of the node-level poll loop, and an engine that has just become
    /// leader waiting for the first read round (`RuleStore::poll_once`/`first_round`). Past
    /// the bound ⇒ keep the current copy, the next round retries early.
    pub static RULES_POLL: Op = Op::new("rules.poll");
    /// One tick of the driver that makes children self-standing after a split (cairn-server
    /// `lineage.rs`): read the ancestors' tombstones, ask for a full compaction, ask whether
    /// it has finished. Past the bound ⇒ skip the tick, the next tick goes on.
    pub static LINEAGE_COMPACT: Op = Op::new("lineage.compact");
    /// One poll of `GET /admin/tenants/{t}/purge` (cairn-server `ingress/admin.rs`): read
    /// the book, LIST the trash, classify entries with a bound, LIST the prefixes. Past the
    /// bound ⇒ `503`.
    pub static TENANT_PURGE_STATUS: Op = Op::new("tenant_purge.status");
    /// One check lot of the tenant purge's final check (cairn-server `tenant_purge.rs`,
    /// `check_keyed`): it deletes nothing, so a failed lot is tried again within the round.
    pub static TENANT_PURGE_CHECK: Op = Op::new("tenant_purge.check_lot");
    /// The outcome PUT of an audited action on an object (cairn-server `audit.rs`,
    /// `Intent::done`).
    pub static AUDIT_OUTCOME: Op = Op::new("audit.outcome");
    /// One `entry` GET while classifying trash entries (`trash.rs`, `Trash::classify`).
    pub static TRASH_ENTRY_READ: Op = Op::new("trash.entry_read");
    /// Asking the peers that have not confirmed applying a registry change again, within
    /// the registry write request (cairn-server `registry_sync.rs`, `confirm_peers_at`).
    pub static REGISTRY_CONFIRM: Op = Op::new("registry.confirm");
    /// One read of a snapshot of every owner record (`owner.rs`, `referenced_logs`: the
    /// LIST of `owners/`, one GET per record) — a dead-log sweep round takes two.
    pub static OWNER_SNAPSHOT_READ: Op = Op::new("owner.snapshot_read");
    /// A session record read by the dead-log sweep (`nodewal/sweep.rs`, `forget_unlisted`).
    pub static SWEEP_SESSION_READ: Op = Op::new("nodewal.sweep_session_read");
    /// A read of the op ledger by the auto-split decider, to restore the split cooldown
    /// and the node's split interval after a restart (cairn-server `autosplit.rs`).
    pub static AUTOSPLIT_LEDGER_READ: Op = Op::new("autosplit.ledger_read");

    pub static ALL: [&Op; 52] = [
        &OPEN_IO,
        &DISK_CACHE_WARM,
        &ARTIFACT_SWEEP,
        &VALUE_SWEEP,
        &SEAL_IO,
        &SESSION_RELEASE,
        &KEEPER_RELEASE,
        &KEEPER_SEALED,
        &SESSION_TERMINAL,
        &FLUSH_MEMTABLE,
        &SPLIT_CLONE,
        &SPLIT_CLAIM,
        &SPLIT_SETTLE,
        &OPEN_ENGINE,
        &SLATE_ADMIN,
        &SEAL_ROUNDS,
        &BOOT_GATE,
        &BOOT_FORMAT,
        &BOOT_SESSION,
        &BOOT_MIGRATE,
        &BOOT_TABLE,
        &BOOT_SECRET,
        &BOOT_AUTH,
        &BOOT_TENANTS,
        &BOOT_WINDOWS,
        &SCAN_READ,
        &WAL_DRAIN,
        &OPEN_DB,
        &FLUSH_THROUGH,
        &ROTATE_CLAIM,
        &ROTATE_SEAL,
        &ROTATE_RETRY,
        &SESSION_MARKER,
        &GC_CLOCK_PROBE,
        &SESSION_READ_FENCE,
        &CONTROL_COORDINATOR,
        &REGISTRY_GEN,
        &BOOT_STORE_ID,
        &SDK_BIDI_CLOSE,
        &INGRESS_FORWARD,
        &SDK_REPLAY,
        &INGRESS_LIST_LOCAL,
        &RULES_POLL,
        &LINEAGE_COMPACT,
        &TENANT_PURGE_STATUS,
        &TENANT_PURGE_CHECK,
        &AUDIT_OUTCOME,
        &TRASH_ENTRY_READ,
        &REGISTRY_CONFIRM,
        &OWNER_SNAPSHOT_READ,
        &SWEEP_SESSION_READ,
        &AUTOSPLIT_LEDGER_READ,
    ];
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    };

    static T_ATTEMPTS: Op = Op::new("test.attempts");
    static T_TOTAL: Op = Op::new("test.total");
    static T_PERMANENT: Op = Op::new("test.permanent");
    static T_AMBIGUOUS: Op = Op::new("test.ambiguous");
    static T_CANCEL: Op = Op::new("test.cancel");
    static T_BOUND: Op = Op::new("test.bound");
    static T_DEADLINE: Op = Op::new("test.deadline");
    static T_OK: Op = Op::new("test.ok");

    fn io() -> StoreError {
        StoreError::Io("503".into())
    }

    /// Attempt cap exhausted: exactly `attempts` calls, `attempts − 1` retries, one
    /// exhausted bound; the error returned is the last attempt's. The rest times stay
    /// within the `Backoff` envelope (jitter on the upper half).
    #[tokio::test(start_paused = true)]
    async fn attempt_cap_exhausted() {
        let calls = AtomicU32::new(0);
        let p = Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8);
        let t0 = Instant::now();
        let r: Result<(), _> = run(&T_ATTEMPTS, &p, Never, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err(io()) }
        })
        .await;
        assert!(matches!(r, Err(StoreError::Io(_))));
        assert_eq!(calls.load(Ordering::Relaxed), 8);
        let c = T_ATTEMPTS.counts();
        assert_eq!((c.retries, c.exhausted), (7, 1));
        // 25+50+100+200+400+800+1000 ms, each within [d/2, d].
        let e = t0.elapsed();
        assert!(
            e >= Duration::from_millis(1287) && e <= Duration::from_millis(2575),
            "{e:?}"
        );
    }

    /// Total bound: no rest past the deadline, the attempt is cut to what is left.
    #[tokio::test(start_paused = true)]
    async fn total_time_cap_exhausted() {
        let p = Policy::for_duration(
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(5),
        );
        let t0 = Instant::now();
        let r: Result<(), StoreError> = run(&T_TOTAL, &p, Never, || async {
            // One hung attempt: the total bound cuts it.
            std::future::pending::<()>().await;
            Ok(())
        })
        .await;
        assert!(matches!(r, Err(StoreError::Io(_))), "{r:?}");
        assert!(t0.elapsed() <= Duration::from_secs(5));
        assert_eq!(T_TOTAL.counts().exhausted, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_error_returns_at_once() {
        let calls = AtomicU32::new(0);
        let p = Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8);
        let r: Result<(), _> = run(&T_PERMANENT, &p, Never, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err(StoreError::Corrupt("x".into())) }
        })
        .await;
        assert!(matches!(r, Err(StoreError::Corrupt(_))));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(T_PERMANENT.counts().permanent, 1);
        assert_eq!(T_PERMANENT.counts().retries, 0);
    }

    /// Ambiguity is not retried blindly: returned at once to the caller.
    #[tokio::test(start_paused = true)]
    async fn ambiguous_is_not_retried() {
        let calls = AtomicU32::new(0);
        let p = Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8);
        let r: Result<(), _> = run_classified(
            &T_AMBIGUOUS,
            &p,
            Never,
            |_: &StoreError| Verdict::Ambiguous,
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Err(io()) }
            },
        )
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(T_AMBIGUOUS.counts().ambiguous, 1);
    }

    /// Cancellation interrupts the rest (without waiting out `max_pause`) and returns the
    /// last error.
    #[tokio::test(start_paused = true)]
    async fn cancel_interrupts_pause() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let calls = Arc::new(AtomicU32::new(0));
        let p = Policy::new(Duration::from_secs(60), Duration::from_secs(60), 100);
        let c2 = Arc::clone(&calls);
        let task = tokio::spawn(async move {
            let t0 = Instant::now();
            let r: Result<(), _> = run(&T_CANCEL, &p, rx, || {
                c2.fetch_add(1, Ordering::Relaxed);
                async { Err(io()) }
            })
            .await;
            (r, t0.elapsed())
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(true).unwrap();
        let (r, took) = task.await.unwrap();
        assert!(matches!(r, Err(StoreError::Io(_))));
        assert!(took < Duration::from_secs(30), "{took:?}");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// Already cancelled: the first transient error returns at once, no rest.
    #[tokio::test(start_paused = true)]
    async fn already_cancelled_does_not_retry() {
        let (_tx, rx) = tokio::sync::watch::channel(true);
        let calls = AtomicU32::new(0);
        let p = Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8);
        let r: Result<(), _> = run(&T_CANCEL, &p, rx, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err(io()) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// Per-attempt bound: the hung attempt is cut, the next one succeeds.
    #[tokio::test(start_paused = true)]
    async fn per_attempt_cap() {
        let calls = AtomicU32::new(0);
        let p = Policy::new(Duration::from_millis(25), Duration::from_secs(1), 3)
            .with_attempt_bound(Duration::from_secs(2));
        let t0 = Instant::now();
        let r: Result<u32, StoreError> = run(&T_BOUND, &p, Never, || {
            let n = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if n == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(n)
            }
        })
        .await;
        assert_eq!(r.unwrap(), 1);
        assert!(t0.elapsed() < Duration::from_secs(3));
        assert_eq!(T_BOUND.counts().retries, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_counts_exhaustion() {
        let r = deadline(
            &T_DEADLINE,
            Duration::from_secs(2),
            std::future::pending::<()>(),
        )
        .await;
        assert_eq!(
            r,
            Err(Elapsed {
                op: "test.deadline",
                bound: Duration::from_secs(2)
            })
        );
        assert_eq!(T_DEADLINE.counts().exhausted, 1);
        assert_eq!(
            deadline(&T_DEADLINE, Duration::from_secs(2), async { 7 }).await,
            Ok(7)
        );
        assert_eq!(T_DEADLINE.counts().exhausted, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn immediate_success_counts_nothing() {
        let p = Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8);
        let r: Result<u8, StoreError> = run(&T_OK, &p, Never, || async { Ok(1) }).await;
        assert_eq!(r.unwrap(), 1);
        assert_eq!(T_OK.counts(), OpCounts::default());
    }

    #[test]
    fn classifies_store_errors() {
        assert_eq!(store_verdict(&io()), Verdict::Transient);
        assert_eq!(
            store_verdict(&StoreError::LeaseIo("x".into())),
            Verdict::Transient
        );
        assert_eq!(store_verdict(&StoreError::Fenced), Verdict::Permanent);
        assert_eq!(
            store_verdict(&StoreError::RangeRetired { range: 1 }),
            Verdict::Permanent
        );
    }

    #[test]
    fn classifies_object_store_errors() {
        use slatedb::object_store::Error as E;
        let generic = E::Generic {
            store: "s3",
            source: "503".into(),
        };
        assert_eq!(object_store_verdict(&generic), Verdict::Transient);
        let not_impl = E::NotImplemented {
            operation: "put_opts".into(),
            implementer: "LocalFileSystem".into(),
        };
        assert_eq!(object_store_verdict(&not_impl), Verdict::Permanent);
        let pre = E::Precondition {
            path: "x".into(),
            source: "412".into(),
        };
        assert_eq!(object_store_verdict(&pre), Verdict::Permanent);
    }

    #[test]
    fn op_names_unique() {
        let mut names: Vec<_> = ops::ALL.iter().map(|o| o.name).collect();
        names.sort_unstable();
        let n = names.len();
        names.dedup();
        assert_eq!(names.len(), n, "duplicate op name");
    }
}
