// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The per-node shared WAL.**
//!
//! Each node incarnation writes the WAL of every range it holds into `S` log streams
//! `nodewal/<node>/<inc>/<stripe>/<seq>`; each Db still sees a WAL of its own through the
//! `slatedb::wal` traits (SlateDB RFC 0030 + fork patch F1). This module contains:
//!
//! - [`NodeWal`] — the WAL of one incarnation: the streams ([`stripe`]), centralised
//!   sealing + replay when acquiring a range ([`NodeWal::take_over`]), combined GC by
//!   durable floor, forced flush by frame age, and the fail-stop signal.
//! - [`acquire_shared`] — the sequence for acquiring a range that uses the shared WAL:
//!   seal → open the Db (replay) → CAS our segment into the owner record → flush the
//!   replayed rows to L0 → drop the old segments → hand the Db to the engine.
//!
//! Every rule here exists because of a counterexample found by model checking the design
//! (C1–C7 in `docs/design/decisions.md`).

pub(crate) mod format;
pub mod inspect;
pub(crate) mod log;
pub mod stats;
pub(crate) mod stripe;
mod tailer;
mod wals;

pub use {
    tailer::{NodeTailers, SharedWalReader},
    wals::NodeWals,
};

use crate::entropy::{HashMap, HashSet};
use {
    crate::{
        config::ObjectStores,
        error::{Result, StoreError},
        owner::{LogId, SessionId, WalSegment},
        session::{LeaseKeeper, OwnerWal},
    },
    async_trait::async_trait,
    format::{DataObject, Decoded, Frame},
    log::{Created, LogStore},
    slatedb::{
        config::{FlushOptions, FlushType},
        object_store::{path::Path, ObjectStore},
        wal::{
            WalError, WalFileRange, WalGc, WalIterator, WalRows, WriterInit, WriterInitResult,
            WriterManifest,
        },
        Db,
    },
    std::{
        collections::{BTreeMap, VecDeque},
        ops::Bound,
        sync::{Arc, Mutex, OnceLock, Weak},
        time::Duration,
    },
    stripe::{RangeWal, Rotation, Stripe},
    tokio::sync::{mpsc, watch},
};

mod init;
use init::*;
mod admin;
pub use admin::*;
mod gc;
pub use gc::*;
mod sweep;
pub(crate) use sweep::*;
mod flush;
pub use flush::*;
mod acquire;
pub use acquire::*;

/// Name of the shared WAL in the Db's manifest (fork patch F1, `ManifestV2.wal_name`).
/// Opening a Db carrying this name with the native WAL — or the other way round — is
/// refused by SlateDB.
pub const SHARED_WAL_NAME: &str = "cairnflow-shared/1";

/// Shared WAL tuning knobs. The defaults are provisional: W = 1 PUT in flight per stream,
/// S = 2 streams, T = 60 s, a 2 s PUT bound, `stall_max` = 3 × the PUT bound. Cost
/// measurements suggested W = 2 / S = 1 — changing the defaults is a product decision; all
/// four knobs W, S, window and object cap can be tuned here.
#[derive(Clone, Debug)]
pub struct NodeWalConfig {
    /// The stream's batching window (D-h).
    pub window: Duration,
    /// Number of streams per node `S` (D-i).
    pub stripes: u16,
    /// Number of PUTs in flight at once per stream `W`. Acks still go **in seq order**: if
    /// object n+1 is durable before n, it waits for n. 0 counts as 1.
    pub inflight: usize,
    /// Object size cap: reaching it cuts at once.
    pub object_cap: usize,
    /// Linger L of a cut: the first row after an idle spell waits an extra L before the
    /// cut, so commits arriving close together (a batch of SDK frames, the `Running` commit
    /// right after the commit receiving a message) share one object instead of the later
    /// commit waiting for the earlier commit's whole PUT (W = 1). The cut deadline is
    /// `max(first row + L, previous deadline + window)`: L never makes cuts denser than the
    /// window. Default [`DEFAULT_LINGER`] = 1 ms; `0` = the old behaviour: the first row
    /// after an idle spell cuts at once. `flush()` and the size cap still cut at once. L only
    /// chooses **when** to cut — object content, seq order, acks after the PUT and in order
    /// (NW-1), and re-reading on ambiguity (NW-2) do not change; M3 treats `Cut` as a free
    /// action. Clamped to [`MAX_LINGER`].
    pub linger: Duration,
    /// Forced flush age T (D-j): a frame durable for longer than T but not yet in L0 ⇒
    /// flush the memtable.
    pub frame_age_flush: Duration,
    /// Bound of each PUT.
    pub put_timeout: Duration,
    /// Budget for trying one PUT before fail-stop, as a number of PUT bounds:
    /// `put_attempts × put_timeout` is `stall_max`. Errors that come back fast (503) are
    /// retried more often within that budget, with backoff + jitter (`stripe::put`); a hung
    /// PUT gets exactly `put_attempts` tries.
    pub put_attempts: u32,
    /// Tick of the node's combined GC.
    pub gc_interval: Duration,
    /// The node's PUT latency meter: every attempt to PUT a log object is measured; a
    /// failed attempt is a "timeout" for the slowness score.
    pub meter: Option<crate::health::SlowMeter>,
    /// Safety net for a failed rotation: "another node can take over the ranges we cannot
    /// serve". Present and returning `true` ⇒ a failed id claim keeps the slot in the
    /// rotated state and retries in the background; absent or `false` (we are the only live
    /// node, every other node is slow or draining — the whole bucket is slow) ⇒ fail-stop
    /// as before: a new incarnation builds its full set of initial streams without claiming
    /// ids.
    pub rotation_net: Option<RotationNet>,
}

/// See [`NodeWalConfig::rotation_net`]. The node sets it from the session's peer health
/// (`cairn-server/src/slow.rs:peers_can_serve`).
#[derive(Clone)]
pub struct RotationNet(Arc<dyn Fn() -> bool + Send + Sync>);

impl RotationNet {
    pub fn new(others_can_serve: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        RotationNet(Arc::new(others_can_serve))
    }

    fn others_can_serve(&self) -> bool {
        (self.0)()
    }
}

impl std::fmt::Debug for RotationNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RotationNet")
    }
}

/// Default linger of a cut ([`NodeWalConfig::linger`], `--wal-linger-us`): the gap between
/// two appends of one batch of SDK frames measured 0.2–0.5 ms, so 1 ms leaves a margin of
/// two.
pub const DEFAULT_LINGER: Duration = Duration::from_millis(1);

/// Cap of [`NodeWalConfig::linger`]: a larger `--wal-linger-us` is refused at startup, and
/// `Stripe::run` clamps here — the cut deadline `first row + L` never overflows `Instant`.
/// 100 ms is already ~4 × the PUT p50 of S3 Standard: a longer linger only adds latency.
pub const MAX_LINGER: Duration = Duration::from_millis(100);

impl Default for NodeWalConfig {
    fn default() -> Self {
        Self {
            window: crate::DEFAULT_FLUSH_INTERVAL,
            stripes: 2,
            inflight: 1,
            object_cap: 1 << 20,
            linger: DEFAULT_LINGER,
            frame_age_flush: Duration::from_secs(60),
            put_timeout: Duration::from_secs(2),
            put_attempts: 3,
            gc_interval: Duration::from_secs(30),
            meter: None,
            rotation_net: None,
        }
    }
}

impl NodeWalConfig {
    /// Maximum time a stream may stand with a queue before the node fail-stops.
    pub fn stall_max(&self) -> Duration {
        self.put_timeout * self.put_attempts.max(1)
    }
}

/// The fail-stop signal of an incarnation: fires once, keeps the first reason and cause
/// (`cairnflow_failstop_total{cause}`). The cause is set **before** the reason shows up on
/// `watch`, so whoever sees the reason can always read the cause.
///
/// Third cell: the incarnation has yielded at least one range dirty
/// ([`NodeWal::note_dirty_yield`]). A fail-stop because the log was sealed or because of C1
/// then carries the label [`crate::failstop::Cause::DirtyYield`] — it only changes the
/// counter label, not the path taken.
#[derive(Clone)]
pub(crate) struct FailStop(
    Arc<watch::Sender<Option<String>>>,
    Arc<std::sync::OnceLock<crate::failstop::Cause>>,
    Arc<std::sync::atomic::AtomicBool>,
);

impl FailStop {
    fn new() -> Self {
        FailStop(
            Arc::new(watch::Sender::new(None)),
            Arc::new(std::sync::OnceLock::new()),
            Arc::default(),
        )
    }

    pub(crate) fn trigger(&self, cause: crate::failstop::Cause, reason: String) {
        let cause =
            if cause.follows_dirty_yield() && self.2.load(std::sync::atomic::Ordering::Acquire) {
                crate::failstop::Cause::DirtyYield
            } else {
                cause
            };
        let _ = self.1.set(cause);
        self.0.send_if_modified(|v| {
            if v.is_none() {
                *v = Some(reason);
                true
            } else {
                false
            }
        });
    }

    fn cause(&self) -> Option<crate::failstop::Cause> {
        self.1.get().copied()
    }
}

/// A sealed log of another incarnation, with its canonical part.
struct SealedLog {
    store: LogStore,
    /// The effective seal position.
    seal_at: u64,
    /// Index of the canonical part (from the floor at seal time to before the seal): which
    /// object carries which (range, wal id) frames — the head from the index the writer
    /// wrote, the tail from the objects read while probing for holes. GC only needs this.
    index: BTreeMap<u64, Vec<(u32, u64)>>,
    /// Data objects read: the tail read while probing for holes, plus objects a range
    /// acquisition needed ([`SealedLog::frames_of`]).
    objects: Mutex<BTreeMap<u64, DataObject>>,
}

impl SealedLog {
    /// Frames of `range` with wal id above `after` in the canonical part, by seq. Only GETs
    /// objects that the index says carry frames of the range and that have not been read:
    /// whoever acquires a range does not read the frames of other ranges in the log.
    ///
    /// An object in the index that is now absent: the writer's GC (the log was still live
    /// at that point; a dead log has no floor GC) deleted it after proving that every frame
    /// in it is outside every reference — this range's included, whose
    /// `replay_after_wal_id` only grows — so skip it, **if** the floor has passed it. Absent
    /// while the floor has not passed it, or a seal at that spot, means the log contradicts
    /// the index: an error, no guessing.
    async fn frames_of(&self, log: &LogId, range: u32, after: Option<u64>) -> Result<Vec<Frame>> {
        use futures::{StreamExt, TryStreamExt};
        let needed = |r: u32, id: u64| r == range && after.is_none_or(|a| id > a);
        let want: Vec<u64> = self
            .index
            .iter()
            .filter(|(_, fs)| fs.iter().any(|(r, id)| needed(*r, *id)))
            .map(|(seq, _)| *seq)
            .collect();
        let missing: Vec<u64> = {
            let objects = self.objects.lock().expect("mutex");
            want.iter()
                .filter(|s| !objects.contains_key(s))
                .copied()
                .collect()
        };
        let store = &self.store;
        let fetched: Vec<(u64, Option<Decoded>)> = futures::stream::iter(missing)
            .map(|seq| async move {
                Ok::<_, StoreError>((seq, retry_io(log, || store.get(seq)).await?))
            })
            .buffered(SEAL_READ_AHEAD as usize)
            .try_collect()
            .await?;
        let mut floor = None;
        for (seq, got) in fetched {
            match got {
                Some(Decoded::Data(d)) => {
                    self.objects.lock().expect("mutex").insert(seq, d);
                }
                None => {
                    if floor.is_none() {
                        floor = Some(retry_io(log, || store.floor()).await?);
                    }
                    if floor.is_none_or(|f| seq >= f) {
                        return Err(StoreError::Corrupt(format!(
                            "log {log}: object {seq} is in the index but absent above the GC floor"
                        )));
                    }
                }
                Some(Decoded::Seal { .. }) => {
                    return Err(StoreError::Corrupt(format!(
                        "log {log}: the index says {seq} is data but it is a seal"
                    )))
                }
            }
        }
        let objects = self.objects.lock().expect("mutex");
        Ok(want
            .iter()
            .filter_map(|s| objects.get(s))
            .flat_map(|o| {
                o.frames
                    .iter()
                    .filter(|f| needed(f.range, f.wal_file_id))
                    .cloned()
            })
            .collect())
    }
}

/// Result of the centralised seal + replay for **one** range: every frame of the range in
/// the logs of `wal_segment`, by `wal_file_id`.
pub struct Takeover {
    range: u32,
    frames: Vec<Frame>,
    floor_id: Option<u64>,
    /// A segment of this very incarnation was skipped because its log has no frame of the
    /// range yet — attaching the range to a stream must check that again (see `register`).
    skipped_own: bool,
}

impl Takeover {
    /// Number of the range's frames read from the old logs.
    pub fn frames(&self) -> usize {
        self.frames.len()
    }
}

/// Retry policy for one I/O when sealing: 8 tries, backoff from 25 ms doubling up to 1 s,
/// jitter as in `stripe::put` — ~2.5 s in total for one operation.
const SEAL_IO_POLICY: crate::retry::Policy =
    crate::retry::Policy::new(Duration::from_millis(25), Duration::from_secs(1), 8);

/// One I/O of a seal, retried on I/O errors (503, network) with jittered backoff. Every
/// operation here is a read, or a `Create` whose retry meeting `AlreadyExists` makes the
/// seal loop re-read from the start — retrying does not change the result, only the time of
/// the read, and the seal loop already tolerates that (C2, C3′). Before, a single error
/// failed the **whole** seal and the range acquisition started over: a long log under a
/// throttled read budget almost never got read to the end (`s3_slowdown`). Other errors
/// (`Corrupt`…) return at once.
///
/// Uses the shared `crate::retry` primitive. The classification is unchanged: only `Io` is
/// transient — `LogStore` wraps every object store error as `Io`.
async fn retry_io<T, F, Fut>(log: &LogId, op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    use {crate::retry, tracing::Instrument as _};
    retry::run_classified(
        &retry::ops::SEAL_IO,
        &SEAL_IO_POLICY,
        retry::Never,
        |e| match e {
            StoreError::Io(_) => retry::Verdict::Transient,
            _ => retry::Verdict::Permanent,
        },
        op,
    )
    .instrument(tracing::debug_span!("seal_io", log = %log))
    .await
}

/// Cap on the number of rounds of one seal. Each repeated round is a real race (a zombie
/// writes, GC deletes) — 16 rounds in a row means the store is inconsistent. Linear backoff
/// of 20 ms × round from the 4th round: ~2.3 s of sleep in total.
const SEAL_ROUNDS_MAX: u32 = 16;

/// Maximum number of parallel GETs when sealing a log (read-ahead), and of parallel DELETEs
/// when GC deletes objects below the new floor.
const SEAL_READ_AHEAD: u64 = 16;
const GC_DELETES_IN_FLIGHT: usize = 16;

struct Inner {
    stores: ObjectStores,
    wal_store: Arc<dyn ObjectStore>,
    wal_prefix: Path,
    session: SessionId,
    cfg: NodeWalConfig,
    /// Declared capability of the WAL tier — for the new stream of a rotation.
    atomic_put: bool,
    /// The incarnation's streams, by slot `range % S`. A slot changes stream when its
    /// stream rotates; a short lock, not on the per-turn path (`RangeWal` holds its
    /// `Arc<Stripe>` directly).
    stripes: Mutex<Vec<Arc<Stripe>>>,
    /// Rotated stripe ids of this incarnation: their logs no longer ack, so this very
    /// incarnation can seal, replay (`take_over`) and purge (`sweep.rs`) them.
    retired: Mutex<HashSet<u16>>,
    /// Next stripe id to try to claim when rotating — every initial id is below it.
    next_stripe: Mutex<u32>,
    rotate_tx: mpsc::UnboundedSender<Rotation>,
    /// Logs a range acquisition has just dropped from the owner record — for
    /// `sweep::hint_loop`.
    hint_tx: mpsc::UnboundedSender<Vec<LogId>>,
    failstop: FailStop,
    /// Sealed logs of other incarnations (or our rotated streams), so range takers do not
    /// seal and read them again. Only a cache: no round reads progress from here. Dropped
    /// once the log is no longer listed (`sweep::forget_unlisted`).
    sealed: tokio::sync::Mutex<HashMap<LogId, Arc<tokio::sync::OnceCell<Arc<SealedLog>>>>>,
    /// Dbs open on this node — for forced flush by frame age, and for GC to read
    /// `replay_after_wal_id` without GETting the manifest.
    dbs: Mutex<HashMap<u32, Db>>,
    /// The floor of each of our logs that the GC loop cleans.
    floors: Mutex<HashMap<LogId, FloorState>>,
    /// The last seq in the written index of each of our logs — shared by the GC loop and
    /// the index loop (`gc::index_loop`).
    indexed: Mutex<HashMap<LogId, u64>>,
    /// Largest `replay_after_wal_id` seen on the **stored** manifest of each range Db open
    /// on the node — GC only trusts the in-memory mark up to here (`Inner::refs`, NW-7).
    durable_replay_after: Mutex<HashMap<u32, u64>>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        for t in self.tasks.lock().expect("mutex").drain(..) {
            t.abort();
        }
    }
}

/// The shared WAL of **one** node incarnation.
#[derive(Clone)]
pub struct NodeWal(Arc<Inner>);

impl NodeWal {
    /// Builds the WAL of incarnation `session` and runs its background tasks (streams, GC,
    /// forced flush) on the current runtime.
    pub fn start(stores: &ObjectStores, session: SessionId, cfg: NodeWalConfig) -> NodeWal {
        let wal_store = Arc::clone(stores.wal().store());
        let wal_prefix = stores.main().prefix().clone();
        // Declared capability of the WAL tier: re-read ambiguous PUTs or not.
        let atomic_put = stores.wal().atomic_put();
        let failstop = FailStop::new();
        let (rotate_tx, rotate_rx) = mpsc::unbounded_channel();
        let (hint_tx, hint_rx) = mpsc::unbounded_channel();
        let stripes: Vec<_> = (0..cfg.stripes.max(1))
            .map(|stripe| {
                let log = LogId {
                    session: session.clone(),
                    stripe,
                };
                let store = LogStore::new(Arc::clone(&wal_store), &wal_prefix, &log);
                Stripe::new(
                    log,
                    store,
                    cfg.clone(),
                    atomic_put,
                    failstop.clone(),
                    Some(rotate_tx.clone()),
                )
            })
            .collect();
        let next_stripe = u32::from(cfg.stripes.max(1));
        let inner = Arc::new(Inner {
            stores: stores.clone(),
            wal_store,
            wal_prefix,
            session,
            cfg,
            atomic_put,
            stripes: Mutex::new(stripes),
            retired: Mutex::new(HashSet::default()),
            next_stripe: Mutex::new(next_stripe),
            rotate_tx,
            hint_tx,
            failstop,
            sealed: tokio::sync::Mutex::new(HashMap::default()),
            dbs: Mutex::new(HashMap::default()),
            floors: Mutex::new(HashMap::default()),
            indexed: Mutex::new(HashMap::default()),
            durable_replay_after: Mutex::new(HashMap::default()),
            tasks: Mutex::new(Vec::new()),
        });
        LIVE.lock().expect("mutex").push(Arc::downgrade(&inner));
        let mut tasks = Vec::new();
        for s in &inner.stripes() {
            tasks.push(
                tokio::spawn(crate::io_source::tagged(
                    "nodewal.stripe",
                    Arc::clone(s).run(),
                ))
                .abort_handle(),
            );
        }
        tasks.push(
            tokio::spawn(crate::io_source::tagged(
                "nodewal.gc",
                gc_loop(Arc::downgrade(&inner)),
            ))
            .abort_handle(),
        );
        tasks.push(
            tokio::spawn(crate::io_source::tagged(
                "nodewal.sweep",
                sweep::sweep_loop(Arc::downgrade(&inner)),
            ))
            .abort_handle(),
        );
        tasks.push(
            tokio::spawn(crate::io_source::tagged(
                "nodewal.sweep",
                sweep::hint_loop(Arc::downgrade(&inner), hint_rx),
            ))
            .abort_handle(),
        );
        tasks.push(
            tokio::spawn(crate::io_source::tagged(
                "nodewal.index",
                index_loop(Arc::downgrade(&inner)),
            ))
            .abort_handle(),
        );
        tasks.push(
            tokio::spawn(crate::io_source::tagged(
                "nodewal.age_flush",
                age_flush_loop(Arc::downgrade(&inner)),
            ))
            .abort_handle(),
        );
        tasks.push(
            tokio::spawn(crate::io_source::tagged(
                "nodewal.rotate",
                rotate_loop(Arc::downgrade(&inner), rotate_rx),
            ))
            .abort_handle(),
        );
        *inner.tasks.lock().expect("mutex") = tasks;
        NodeWal(inner)
    }

    pub fn session(&self) -> &SessionId {
        &self.0.session
    }

    pub fn config(&self) -> &NodeWalConfig {
        &self.0.cfg
    }

    /// GC floors of this incarnation's logs, and which ranges are pinning them.
    pub fn floors(&self) -> FloorReport {
        self.0.floor_report()
    }

    fn stripe(&self, range: u32) -> Arc<Stripe> {
        let stripes = self.0.stripes.lock().expect("mutex");
        Arc::clone(&stripes[range as usize % stripes.len()])
    }

    /// The log range `range` writes to in this incarnation (assigned by range id, D-i).
    pub fn log_of(&self, range: u32) -> LogId {
        self.stripe(range).log.clone()
    }

    /// The slot of range `range` has a running stream: false when the slot's stream has
    /// rotated and claiming a new id has not finished yet — range acquisition refuses to
    /// open a Db into that slot ([`acquire_shared_with_hint`]).
    pub fn slot_ready(&self, range: u32) -> bool {
        !self.stripe(range).is_retired()
    }

    /// Fires when the incarnation must fail-stop: the log was sealed, a PUT with an unknown
    /// outcome, a stream standing longer than `stall_max`, or a range lost behind our back.
    /// The node calls [`crate::Session::fail_stop`] for this incarnation.
    pub fn fail_stopped(&self) -> watch::Receiver<Option<String>> {
        self.0.failstop.0.subscribe()
    }

    /// Cause of the first fail-stop — `None` while not fail-stopped.
    pub fn fail_stop_cause(&self) -> Option<crate::failstop::Cause> {
        self.0.failstop.cause()
    }

    /// Triggers fail-stop from outside (e.g. an owner record CAS that missed).
    pub fn fail_stop(&self, cause: crate::failstop::Cause, reason: impl Into<String>) {
        self.0.failstop.trigger(cause, reason.into());
    }

    /// Range `range` has just stepped down while the owner record may still point at a
    /// live log of this incarnation (a **dirty** yield): the seal failed or timed out, or
    /// the clean owner-record CAS after the seal missed. Only counts and marks
    /// (`cairnflow_dirty_yields_total`, label `dirty_yield` for a later fail-stop); changes
    /// nothing on the path taken.
    pub fn note_dirty_yield(&self, range: u32) {
        self.0
            .failstop
            .2
            .store(true, std::sync::atomic::Ordering::Release);
        crate::failstop::record_dirty_yield();
        tracing::debug!(session = %self.0.session, range, "dirty yield: the owner record may still point at a live log");
    }

    /// **A dirty yield does not seal a live log**: range `range` is about to yield while
    /// the owner record will still list `logs`. Each of those logs that is still a **live**
    /// stream of this incarnation rotates at once ([`Stripe::rotate_for_dirty_yield`]: stops
    /// acking, closes every range, asks for a new stream) — **before** the caller writes the
    /// owner record. The range taker seals a log that has stopped acking, and if this very
    /// node takes the range back, `take_over` treats that log like anyone else's (NW-5): no
    /// fail-stop.
    ///
    /// Logs of other incarnations, of rotated or dead streams: skipped. Returns the number
    /// of streams just rotated.
    pub fn rotate_for_dirty_yield(&self, range: u32, logs: &[LogId]) -> usize {
        let mut rotated = 0;
        for log in logs.iter().filter(|l| l.session == self.0.session) {
            let Some(stripe) = self.0.stripes().into_iter().find(|s| s.log == *log) else {
                continue;
            };
            if stripe.rotate_for_dirty_yield(format!(
                "range {range}: dirty yield, the owner record keeps a segment pointing at log {log}"
            )) {
                rotated += 1;
            }
        }
        rotated
    }

    /// Stops every background task. Rows not yet PUT are dropped and those waiting for
    /// durability get an error — only for when the incarnation has ended.
    pub fn shutdown(&self) {
        tracing::debug!(session = %self.0.session, "stopping the incarnation's shared WAL");
        for s in &self.0.stripes() {
            if !s.is_dead() {
                // `Other`: a deliberate stop that does not reach `Session::fail_stop` —
                // `NodeWals` aborts the watch task before calling `shutdown` (NW-2).
                s.die(
                    stripe::unavailable(format!("incarnation {} has ended", self.0.session)),
                    crate::failstop::Cause::Other,
                    format!("incarnation {} has ended", self.0.session),
                );
            }
        }
        for t in self.0.tasks.lock().expect("mutex").drain(..) {
            t.abort();
        }
        // Slots of this incarnation without a stream no longer matter: the next
        // incarnation builds its full set of initial streams without claiming ids.
        if self.0.streamless_slots() > 0 {
            if let Some(m) = &self.0.cfg.meter {
                m.set_wal_draining(false);
            }
        }
    }

    /// The flush window of range `range` alone (D-h) — `None` = the node's window.
    pub fn set_range_window(&self, range: u32, window: Option<Duration>) {
        self.stripe(range).set_window(range, window);
    }

    /// Highest wal id allocated to the range on its stream — the `wal_floor_id` of a clean
    /// release.
    pub fn last_wal_id(&self, range: u32) -> Option<u64> {
        self.stripe(range).last_wal_id(range)
    }

    /// Seals every log in the range's `wal_segment` and takes the range's frames from them.
    /// Each log is sealed and read **only once** for all the ranges acquired from it.
    ///
    /// A segment pointing at a log of **this very incarnation** means we lost the range and
    /// took it back without fail-stopping — counterexample C1: replaying our own log may
    /// read before an old PUT of ours arrives. Fail-stop.
    ///
    /// `after`: frames with wal id ≤ this mark are not needed (already in L0 according to a
    /// manifest that was read) — objects carrying only those are not GET.
    pub async fn take_over(
        &self,
        range: u32,
        wal: &OwnerWal,
        after: Option<u64>,
    ) -> Result<Takeover> {
        let mut frames = Vec::new();
        let mut seen = HashSet::default();
        let mut skipped_own = false;
        for seg in &wal.segments {
            if !seen.insert(seg.log.clone()) {
                continue;
            }
            if seg.log.session == self.0.session && !self.0.is_retired(&seg.log) {
                // A segment of this very incarnation whose log has never had a frame of
                // the range (an earlier acquisition failed after CASing the segment, before
                // handing over the Db): nothing to replay — skip. Frames present ⇒ C1: do
                // not read our own live log.
                //
                // The log of a **rotated** stream, though, is not a live log: that stream
                // stopped acking before rotating, so no late PUT of it is acked any more —
                // seal it and replay it like anyone else's log (the branch below), no
                // fail-stop.
                let stripe = self.0.stripes().into_iter().find(|s| s.log == seg.log);
                if stripe.is_some_and(|s| !s.had_frames(range)) {
                    skipped_own = true;
                    continue;
                }
                self.fail_stop(
                    crate::failstop::Cause::C1,
                    format!(
                        "range {range}: wal_segment points at log {} of this very incarnation",
                        seg.log
                    ),
                );
                return Err(StoreError::LeaseLost);
            }
            let sealed = self.sealed(&seg.log).await?;
            frames.extend(sealed.frames_of(&seg.log, range, after).await?);
        }
        frames.sort_by_key(|f| f.wal_file_id);
        if let Some(w) = frames
            .windows(2)
            .find(|w| w[0].wal_file_id == w[1].wal_file_id)
        {
            return Err(StoreError::Corrupt(format!(
                "range {range}: two frames with the same wal id {} in the logs of wal_segment",
                w[0].wal_file_id
            )));
        }
        Ok(Takeover {
            range,
            frames,
            floor_id: wal.floor_id,
            skipped_own,
        })
    }

    /// Seals `log` once for the whole node. A failed seal removes its empty cell from the
    /// map: the next call (range acquisition, next purge round) seals again from the start
    /// instead of waiting forever on a cell that will never be filled.
    async fn sealed(&self, log: &LogId) -> Result<Arc<SealedLog>> {
        let cell = {
            let mut map = self.0.sealed.lock().await;
            Arc::clone(map.entry(log.clone()).or_default())
        };
        let res = cell.get_or_try_init(|| self.seal(log)).await.cloned();
        if res.is_err() {
            let mut map = self.0.sealed.lock().await;
            if map
                .get(log)
                .is_some_and(|c| Arc::ptr_eq(c, &cell) && !c.initialized())
            {
                map.remove(log);
            }
        }
        res
    }

    /// Puts the seal at the first hole from the durable GC floor, then re-reads the floor.
    async fn seal(&self, log: &LogId) -> Result<Arc<SealedLog>> {
        let store = LogStore::new(Arc::clone(&self.0.wal_store), &self.0.wal_prefix, log);
        let sealer = self.0.session.to_string();
        let mut attempt: u32 = 0;
        loop {
            // Each repeated round is a real race (a zombie writes, GC deletes) — rare.
            // Repeating forever means an inconsistent store (Create misses but the GET
            // sees nothing): back off gradually, no hot spinning, and give up at
            // [`SEAL_ROUNDS_MAX`] — the open fails (the leadership loop backs off and
            // takes the range again), the next dead-log sweep redoes it; repeating forever
            // would hold the lease without opening.
            if attempt >= SEAL_ROUNDS_MAX {
                crate::retry::ops::SEAL_ROUNDS.note_exhausted();
                return Err(StoreError::Io(format!(
                    "log {log}: sealing repeated {attempt} rounds and the store is still inconsistent — giving up, retrying later"
                )));
            }
            if attempt >= 3 {
                if attempt == 3 {
                    tracing::warn!(log = %log, "sealing a shared WAL log repeated many times — backing off");
                }
                crate::pace::retry_pause(Duration::from_millis(20 * u64::from(attempt.min(50))))
                    .await;
            }
            attempt += 1;
            let floor = retry_io(log, || store.floor()).await?;
            // The index the writer wrote: objects `[floor, upto]` are acked data — the
            // writer only acks in order, and an acked object is never sealed over — so the
            // hole (and the seal, if any) lies above `upto`: probe from `upto + 1`, do not
            // GET the head. Entries below the floor were deleted by GC (every frame outside
            // every reference): skip them. Index absent, damaged or GET failed ⇒ probe from
            // the floor as before.
            let mut index: BTreeMap<u64, Vec<(u32, u64)>> = match retry_io(log, || store.index())
                .await
            {
                Ok(Some(ix)) => ix
                    .entries
                    .into_iter()
                    .filter(|(s, _)| *s >= floor)
                    .collect(),
                Ok(None) => BTreeMap::new(),
                Err(e) => {
                    tracing::debug!(log = %log, error = %e, "cannot read the shared WAL log index — probing from the floor");
                    BTreeMap::new()
                }
            };
            let indexed = index.len();
            let mut objects = BTreeMap::new();
            let mut at = index
                .keys()
                .next_back()
                .map_or(floor, |&u| (u + 1).max(floor));
            // Read ahead in windows of 1, 2, 4… up to `SEAL_READ_AHEAD` parallel GETs,
            // process **in order** and stop at the first object that is not data — the
            // result is the same as reading one at a time (each GET is an independent read;
            // a hole or seal in front means the GETs after it are dropped). Extra GETs past
            // the hole are fewer than the objects read. Read sequentially, a 512-object log
            // under S3 latency took ~11 s.
            let mut window = 1;
            let existing = 'read: loop {
                let gets = (at..at + window).map(|seq| {
                    let store = &store;
                    async move { retry_io(log, || store.get(seq)).await }
                });
                for got in futures::future::join_all(gets).await {
                    match got? {
                        Some(Decoded::Data(d)) => {
                            objects.insert(at, d);
                            at += 1;
                        }
                        Some(Decoded::Seal { .. }) => break 'read true,
                        None => break 'read false,
                    }
                }
                window = (window * 2).min(SEAL_READ_AHEAD);
            };
            if !existing {
                // A Create that failed with an unknown outcome and is then retried meets
                // `AlreadyExists` — re-read from the start and find our own seal.
                let seal = format::encode_seal(&sealer, floor);
                match retry_io(log, || store.create(at, seal.clone())).await? {
                    Created::Yes => {}
                    // Data just arrived at that spot (the zombie won), or someone just
                    // sealed: re-read from the start.
                    Created::AlreadyExists => continue,
                }
            }
            // The floor has passed the seal position ⇒ the name `at` was just deleted by
            // GC ⇒ a stale seal, does not count (counterexample C2).
            if retry_io(log, || store.floor()).await? > at {
                continue;
            }
            for (seq, o) in &objects {
                index.insert(
                    *seq,
                    o.frames.iter().map(|f| (f.range, f.wal_file_id)).collect(),
                );
            }
            // Those who come later (another node acquiring another range of this log, the
            // sweep) read the index all the way to the seal: one GET instead of the whole
            // tail. If the write fails, so be it — they probe the tail as we did.
            if !objects.is_empty() {
                if let Some(bytes) = format::encode_index(&index) {
                    if let Err(e) = store.put_index(bytes).await {
                        tracing::debug!(log = %log, error = %e, "cannot write the index of the sealed shared WAL log");
                    }
                }
            }
            tracing::info!(log = %log, seal = at, objects = index.len(), indexed, read = objects.len(), "sealed shared WAL log");
            return Ok(Arc::new(SealedLog {
                store,
                seal_at: at,
                index,
                objects: Mutex::new(objects),
            }));
        }
    }

    /// The range's `WriterInit` for `Db::builder().with_wal_writer(..)`.
    fn writer_init(&self, takeover: Takeover, first: Arc<OnceLock<u64>>) -> SharedWriterInit {
        SharedWriterInit {
            stripe: self.stripe(takeover.range),
            range: takeover.range,
            frames: takeover.frames,
            floor_id: takeover.floor_id,
            skipped_own: takeover.skipped_own,
            first,
        }
    }

    fn wal_gc(&self) -> Arc<dyn WalGc> {
        Arc::new(NoopWalGc)
    }

    fn register_db(&self, range: u32, db: &Db) {
        self.0.dbs.lock().expect("mutex").insert(range, db.clone());
    }

    /// The range's Db has closed (or is about to close) on this node.
    pub fn forget_db(&self, range: u32) {
        self.0.dbs.lock().expect("mutex").remove(&range);
    }

    /// A range acquisition has just dropped `logs` from the owner record: they may have just
    /// stopped being listed — the hint round purges them early (`sweep::hint_loop`), without
    /// waiting for the full sweep. Only a hint: the hint round re-reads everything from the
    /// bucket.
    pub(crate) fn hint_unlisted(&self, logs: Vec<LogId>) {
        if !logs.is_empty() {
            let _ = self.0.hint_tx.send(logs);
        }
    }
}

impl Inner {
    /// Snapshot of the current streams.
    pub(super) fn stripes(&self) -> Vec<Arc<Stripe>> {
        self.stripes.lock().expect("mutex").clone()
    }

    /// Drops from the seal cache every fully sealed log that `keep` does not keep. Cells
    /// still being sealed (not initialised) stay for whoever is waiting on them.
    pub(super) async fn evict_sealed(&self, keep: impl Fn(&LogId) -> bool) {
        self.sealed
            .lock()
            .await
            .retain(|log, cell| !cell.initialized() || keep(log));
    }

    /// Number of slots whose current stream has rotated and has no new stream yet.
    fn streamless_slots(&self) -> usize {
        self.stripes().iter().filter(|s| s.is_retired()).count()
    }

    /// Sets the meter's draining flag by the number of slots without a stream: any such
    /// slot left means the node drains.
    fn refresh_wal_draining(&self) {
        if let Some(m) = &self.cfg.meter {
            m.set_wal_draining(self.streamless_slots() > 0);
        }
    }

    /// `log` is the log of a **rotated** stream of this very incarnation.
    fn is_retired(&self, log: &LogId) -> bool {
        log.session == self.session
            && (self.retired.lock().expect("mutex").contains(&log.stripe)
                || self
                    .stripes()
                    .iter()
                    .any(|s| s.log == *log && s.is_retired()))
    }
}

/// Maximum number of stripe ids tried for one rotation (`AlreadyExists`, or a directory
/// that already has objects): past it ⇒ fail-stop as before streams could rotate.
const ROTATE_IDS_MAX: u32 = 16;

/// Retry policy for one I/O of an id claim (`Create` of floor 0, LIST): I/O errors are
/// retried a few times, each bounded by **2 × `stall_max`** — not `put_timeout`: the slot
/// rotates precisely because the node's PUTs are slower than `stall_max`, so a `Create`
/// just as slow must still be able to finish. The old bound (`put_timeout`) cut off every
/// claim under a node slow across the board (a chaos run saw 18 `wal_stall`).
fn rotate_claim_policy(cfg: &NodeWalConfig) -> crate::retry::Policy {
    crate::retry::Policy::new(Duration::from_millis(25), Duration::from_millis(500), 4)
        .with_attempt_bound((cfg.stall_max() * 2).max(cfg.put_timeout))
}

/// Retry policy for the **whole** id claim of a rotated slot: on failure the slot keeps the
/// rotated state (no stream, node draining — `SlowMeter::set_wal_draining`) and retries in
/// the background with backoff, as long as [`NodeWalConfig::rotation_net`] still says
/// another node can take the ranges. After 10 minutes ⇒ fail-stop as before (last net).
fn rotate_retry_policy() -> crate::retry::Policy {
    crate::retry::Policy::for_duration(
        Duration::from_millis(500),
        Duration::from_secs(10),
        Duration::from_secs(600),
    )
}

/// Policy for sealing a rotated stream's log in the background: try until done (or for 10
/// minutes), each try bounded — a PUT hung on that very log path makes the seal hang too.
/// Running out of the bound is fine: any range acquisition that needs this log seals it
/// before replaying (`take_over`).
fn rotate_seal_policy(cfg: &NodeWalConfig) -> crate::retry::Policy {
    crate::retry::Policy::for_duration(
        Duration::from_millis(100),
        Duration::from_secs(5),
        Duration::from_secs(600),
    )
    .with_attempt_bound((cfg.stall_max() * 2).max(Duration::from_secs(1)))
}

/// **Stream rotation** (inspired by the "poison one shard, reopen + replay" pattern of
/// Lance's MemWAL). A stream that has run out of `stall_max` (or met an "unknown" outcome)
/// has already closed all its ranges itself and stopped acking ([`Stripe::retire`]); this
/// loop:
///
/// 1. claims a new stripe id in the same incarnation: `Create` of floor 0 of the new log
///    (`AlreadyExists` ⇒ another id), then LIST that log's directory — any object other
///    than floor 0 (an earlier life reusing `inc`) ⇒ skip that id;
/// 2. builds the new stream for that slot (taking over the ranges' own windows), records
///    the old id in `retired`, runs the stream;
/// 3. seals the old log itself in the background ([`rotate_seal_policy`]) — into `sealed`.
///    The old log has no floor GC: it stays whole until no owner record lists it any more,
///    and then this very incarnation purges it (`sweep.rs`, proof
///    `DeleteProof::rotated_log`; before, it had to wait for the incarnation to die).
///
/// The old stream's ranges get `Fenced`, step down, yield dirty and are taken back
/// (usually by this very node) on the new stream: the acquisition seals the old log
/// **before** replaying (the same `sealed` cell as step 3), so late PUTs of the old stream
/// are fenced — and the old stream had stopped acking before that. The new stream is built
/// without waiting for step 3 to finish: a PUT hung on the old log path makes the seal hang
/// too, and waiting for it would block the whole slot. The node and session live on; the
/// other streams are not touched.
///
/// **A failed id claim does not fail-stop** (a chaos run with a node slow on every WAL PUT
/// made the new log's floor-0 `Create` slow too). The old stream has stopped acking and
/// its ranges have yielded dirty — NW-5 allows sealing + replaying a rotated stream's log.
/// So the slot **keeps the rotated state** (the dead old stream stays in the slot: a range
/// `register` mapped to the slot returns `Fenced`, [`NodeWal::slot_ready`] is false) and
/// the claim is retried in the background ([`rotate_retry_policy`]). From the moment the
/// stream stalls until every slot has a stream again, the node **drains**
/// (`SlowMeter::set_wal_draining`, set in [`Stripe::stalled`] before closing the ranges):
/// the balancer refuses to give it ranges, the session record announces `draining=1`, and
/// the ranges just yielded go to other nodes. Each rotation is its own task: a slow claim
/// for one slot does not block another.
async fn rotate_loop(inner: Weak<Inner>, mut rx: mpsc::UnboundedReceiver<Rotation>) {
    while let Some(rotation) = rx.recv().await {
        let Some(strong) = inner.upgrade() else {
            return;
        };
        stats::note_rotation(rotation.cause);
        let task = tokio::spawn(crate::io_source::tagged(
            "nodewal.rotate",
            rotate_until_claimed(inner.clone(), rotation),
        ));
        strong
            .tasks
            .lock()
            .expect("mutex")
            .push(task.abort_handle());
    }
}

/// One rotation of [`rotate_loop`], retried until an id is claimed.
async fn rotate_until_claimed(inner: Weak<Inner>, rotation: Rotation) {
    let Rotation { old, cause, reason } = rotation;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    let failures = AtomicU32::new(0);
    let failures = &failures;
    // The net has said "nobody can take the ranges": stop retrying, fail-stop.
    let give_up = AtomicBool::new(false);
    let give_up = &give_up;
    let res = crate::retry::run_classified(
        &crate::retry::ops::ROTATE_RETRY,
        &rotate_retry_policy(),
        crate::retry::Never,
        |e: &StoreError| {
            if give_up.load(Ordering::Acquire) {
                crate::retry::Verdict::Permanent
            } else {
                crate::retry::store_verdict(e)
            }
        },
        || {
            let inner = inner.clone();
            let old = Arc::clone(&old);
            async move {
                let Some(inner) = inner.upgrade() else {
                    return Err(StoreError::LeaseLost);
                };
                let res = rotate(&inner, &old).await;
                if let Err(e) = &res {
                    let n = failures.fetch_add(1, Ordering::Relaxed) + 1;
                    let others = inner
                        .cfg
                        .rotation_net
                        .as_ref()
                        .is_some_and(RotationNet::others_can_serve);
                    if others {
                        tracing::warn!(log = %old.log, attempt = n, error = %e, "no new stream claimed yet for the rotated slot — node draining, retrying in the background");
                    } else {
                        give_up.store(true, Ordering::Release);
                    }
                }
                res
            }
        },
    )
    .await;
    let Some(inner) = inner.upgrade() else { return };
    match res {
        Ok(log) => {
            tracing::warn!(old = %old.log, new = %log, cause = cause.label(), failures = failures.load(Ordering::Relaxed), reason = %reason, "rotated the shared WAL stream");
        }
        Err(e) => {
            tracing::error!(log = %old.log, error = %e, reason = %reason, failures = failures.load(Ordering::Relaxed), "cannot rotate the shared WAL stream and no other node can take the ranges — fail-stop the incarnation");
            inner.failstop.trigger(
                crate::failstop::Cause::WalStall,
                format!("{reason}; stream rotation failed: {e}"),
            );
        }
    }
    inner.refresh_wal_draining();
}

async fn rotate(inner: &Arc<Inner>, old: &Arc<Stripe>) -> Result<LogId> {
    let log = claim_stripe(inner).await?;
    let store = LogStore::new(Arc::clone(&inner.wal_store), &inner.wal_prefix, &log);
    let stripe = Stripe::new(
        log.clone(),
        store,
        inner.cfg.clone(),
        inner.atomic_put,
        inner.failstop.clone(),
        Some(inner.rotate_tx.clone()),
    );
    stripe.set_windows(old.windows());
    inner.retired.lock().expect("mutex").insert(old.log.stripe);
    {
        let mut stripes = inner.stripes.lock().expect("mutex");
        let Some(slot) = stripes.iter_mut().find(|s| Arc::ptr_eq(s, old)) else {
            return Err(StoreError::Io(format!(
                "stream {} is no longer in the incarnation",
                old.log
            )));
        };
        *slot = Arc::clone(&stripe);
    }
    let mut tasks = vec![tokio::spawn(crate::io_source::tagged(
        "nodewal.stripe",
        Arc::clone(&stripe).run(),
    ))
    .abort_handle()];
    let weak = Arc::downgrade(inner);
    let old_log = old.log.clone();
    let policy = rotate_seal_policy(&inner.cfg);
    tasks.push(
        tokio::spawn(crate::io_source::tagged("nodewal.rotate_seal", async move {
            let sealed = crate::retry::run_classified(
                &crate::retry::ops::ROTATE_SEAL,
                &policy,
                crate::retry::Never,
                |_: &StoreError| crate::retry::Verdict::Transient,
                || {
                    let weak = weak.clone();
                    let log = old_log.clone();
                    async move {
                        let Some(inner) = weak.upgrade() else {
                            return Ok(());
                        };
                        NodeWal(inner).sealed(&log).await.map(|_| ())
                    }
                },
            )
            .await;
            if let Err(e) = sealed {
                tracing::warn!(log = %old_log, error = %e, "could not seal the rotated stream's log yet — a range acquisition will seal it");
            }
        }))
        .abort_handle(),
    );
    inner.tasks.lock().expect("mutex").extend(tasks);
    Ok(log)
}

/// Claims a new stripe id for the incarnation (step 1 of [`rotate_loop`]).
async fn claim_stripe(inner: &Inner) -> Result<LogId> {
    use futures::TryStreamExt;
    let policy = rotate_claim_policy(&inner.cfg);
    for _ in 0..ROTATE_IDS_MAX {
        let id = {
            let mut next = inner.next_stripe.lock().expect("mutex");
            let id = *next;
            *next += 1;
            id
        };
        let stripe = u16::try_from(id).map_err(|_| {
            StoreError::Io(format!("incarnation {} out of stripe ids", inner.session))
        })?;
        let log = LogId {
            session: inner.session.clone(),
            stripe,
        };
        let store = LogStore::new(Arc::clone(&inner.wal_store), &inner.wal_prefix, &log);
        let created = crate::retry::run_classified(
            &crate::retry::ops::ROTATE_CLAIM,
            &policy,
            crate::retry::Never,
            crate::retry::store_verdict,
            || store.claim(),
        )
        .await?;
        if created == Created::AlreadyExists {
            // Possibly our own ambiguous `Create`: skip the id, no re-read — a spare id does
            // no harm.
            continue;
        }
        let dir = Path::from(store.dir());
        let others = crate::retry::run_classified(
            &crate::retry::ops::ROTATE_CLAIM,
            &policy,
            crate::retry::Never,
            crate::retry::store_verdict,
            || async {
                inner
                    .wal_store
                    .list(Some(&dir))
                    .try_filter(|m| std::future::ready(!m.location.as_ref().contains("/floor/")))
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| StoreError::Io(e.to_string()))
            },
        )
        .await?;
        if others.is_empty() {
            return Ok(log);
        }
        tracing::warn!(log = %log, objects = others.len(), "the new stripe id already has objects from an earlier life — skipping the id");
    }
    Err(StoreError::Io(format!(
        "could not claim a new stripe id after {ROTATE_IDS_MAX} tries"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(xs: &[u32]) -> HashSet<u32> {
        xs.iter().copied().collect()
    }

    /// Ranges pinning the floor first, then the oldest unflushed frame; skips ranges being
    /// flushed and ranges flushed on the previous tick; no more than `room`.
    #[test]
    fn pick_forced_flushes_orders_and_skips() {
        let s = |xs: &[(u32, u64)]| {
            xs.iter()
                .map(|(r, ms)| (*r, Duration::from_millis(*ms)))
                .collect::<Vec<_>>()
        };
        let stale = s(&[(1, 100), (2, 900), (3, 500), (4, 700), (5, 300), (6, 800)]);
        assert_eq!(
            pick_forced_flushes(stale.clone(), &set(&[]), &set(&[]), &set(&[]), 4),
            vec![2, 6, 4, 3]
        );
        // Range 5 pins the floor: goes first even though it is not the oldest.
        assert_eq!(
            pick_forced_flushes(stale.clone(), &set(&[]), &set(&[]), &set(&[5]), 2),
            vec![5, 2]
        );
        // 2 is being flushed, 6 was just flushed: skip both.
        assert_eq!(
            pick_forced_flushes(stale.clone(), &set(&[2]), &set(&[6]), &set(&[]), 3),
            vec![4, 3, 5]
        );
        assert!(pick_forced_flushes(stale, &set(&[]), &set(&[]), &set(&[]), 0).is_empty());
    }

    /// After a dirty yield, a sealed log or C1 carries the `dirty_yield` label; other causes
    /// keep their own label; without a dirty yield nothing changes.
    #[test]
    fn fail_stop_after_dirty_yield_is_labelled_dirty_yield() {
        use crate::failstop::Cause;
        let label = |dirty: bool, cause: Cause| {
            let f = FailStop::new();
            f.2.store(dirty, std::sync::atomic::Ordering::Release);
            f.trigger(cause, "x".into());
            f.cause().expect("triggered")
        };
        for c in Cause::ALL {
            assert_eq!(label(false, c), c, "no dirty yield: {c}");
            let want = if c.follows_dirty_yield() {
                Cause::DirtyYield
            } else {
                c
            };
            assert_eq!(label(true, c), want, "after a dirty yield: {c}");
        }
    }

    /// A dirty yield asks for a stream rotation while the rotation loop has stopped (the
    /// incarnation is shutting down, the channel has no receiver any more). The stream has
    /// stopped acking, and its slot will never get a new stream. Must see: the incarnation
    /// fail-stops, like a [`stripe::Stripe`] stalling in the same situation — not draining
    /// silently without anyone knowing.
    #[test]
    fn dirty_yield_rotation_without_a_rotation_loop_fails_stop() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let log = LogId {
            session: SessionId {
                node: "a".into(),
                inc: 1,
            },
            stripe: 0,
        };
        let failstop = FailStop::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let s = stripe::Stripe::new(
            log.clone(),
            LogStore::new(store, &Path::from("cairn"), &log),
            NodeWalConfig::default(),
            true,
            failstop.clone(),
            Some(tx),
        );
        assert!(s.rotate_for_dirty_yield("dirty yield".into()));
        assert!(
            failstop.cause().is_some(),
            "the rotation loop has stopped but the dirty-yielding stream did not fail-stop"
        );
    }
}
