// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logs on the
// normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The mixer of one **stream** (stripe) and the `WalWriter` handle of each range on it.
//!
//! - Each Db holds a [`RangeWal`]; `append` pushes a batch into the stream's buffer.
//! - One task per stream **cuts** the buffer into an object when the window deadline
//!   arrives, when the object size cap is reached, or when a Db calls `flush`. Each range
//!   with rows in the object gets exactly one frame with that range's next `wal_file_id`.
//! - Objects are written with `PutMode::Create` at the next `seq`; at most W PUTs are in
//!   flight at once (`inflight`), and results are processed in cut order ⇒ acks go **in
//!   order**.
//! - PUT done ⇒ every range in the object gets `WalFlushed` ⇒ SlateDB's `await_durable`
//!   opens (D11 keeps its meaning).
//! - `AlreadyExists` on our own log: on an `atomic_put` store the stream re-reads the
//!   object at `seq` — exactly our bytes ⇒ ack, a seal ⇒ the log has been sealed ⇒ every
//!   range on the stream closes with `Fenced` and the node fail-stops; on a non-atomic
//!   store it counts as sealed at once. A PUT with an unknown outcome after the budget runs
//!   out ⇒ the stream **rotates**: it stops acking, closes all its ranges, and `NodeWal`
//!   builds a new stream in the same incarnation with a new stripe id — an object not
//!   known to be durable is never acked, and only this stream's ranges have to be taken
//!   back.

use crate::entropy::HashMap;
// The runtime's clock: outside simulation it is `std::time::Instant`, inside deterministic
// simulation it is the virtual clock.
use tokio::time::Instant;
use {
    super::{
        format::{self, Frame},
        log::{Created, LogStore},
        stats::{note_resolved, PutResolved, RotationCause},
        FailStop, NodeWalConfig,
    },
    crate::{failstop::Cause, owner::LogId},
    async_trait::async_trait,
    futures::stream::{FuturesOrdered, StreamExt},
    slatedb::{
        wal::{
            WalError, WalEvent, WalFlushResult, WalObserver, WalStatus, WalStatusListener,
            WalWriter,
        },
        RowEntry, ValueDeletable,
    },
    std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
    },
    tokio::sync::{mpsc, oneshot, Notify},
};

/// A request to rotate a stream (sent by [`Stripe::run`], received by `NodeWal`'s rotation
/// loop).
pub(crate) struct Rotation {
    pub old: Arc<Stripe>,
    pub cause: RotationCause,
    pub reason: String,
}

pub(crate) type RotateTx = mpsc::UnboundedSender<Rotation>;

/// A `put` that did not finish.
enum PutFailure {
    /// The log has been sealed (a seal, or `AlreadyExists` on a non-atomic store):
    /// fail-stop the whole incarnation, as before.
    Fenced(WalError, Cause, String),
    /// The budget ran out before it finished (`unknown`: the last re-read could not tell
    /// whose object it is): rotate the stream.
    Stalled {
        unknown: bool,
        last: String,
        reason: String,
    },
}

/// The WAL's "no longer usable" error, keeping the reason intact for whoever reads the log.
pub(crate) fn unavailable(reason: impl Into<String>) -> WalError {
    WalError::Unavailable(Arc::new(std::io::Error::other(reason.into())))
}

/// The first backoff between two PUT attempts; doubles each time, up to
/// [`PUT_BACKOFF_MAX`].
const PUT_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_millis(25);
const PUT_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(1);
/// Minimum bound of a re-read after `AlreadyExists`, even when the budget is nearly spent:
/// a GET needs a real span of time to have a chance to finish.
const PUT_ATTEMPT_MIN: std::time::Duration = std::time::Duration::from_millis(10);

/// Rows appended but not yet cut into any object.
#[derive(Default)]
struct Pending {
    rows: Vec<RowEntry>,
    batches: u64,
    bytes: usize,
}

type Waiter = (u64, oneshot::Sender<Result<(), WalError>>);

struct RangeState {
    /// Registration generation: each time a Db opens the range on the stream is a new
    /// generation; the handle of an old Db (a different generation) counts as closed.
    gen: u64,
    next_wal_id: u64,
    epoch: u64,
    status: WalStatus,
    listeners: Vec<WalStatusListener>,
    /// Number of batches appended / durable.
    appended: u64,
    durable: u64,
    waiters: Vec<Waiter>,
    /// Durable frames that may not be in L0 yet, with the time each became durable — for
    /// forced flush by frame age.
    unflushed: VecDeque<(u64, Instant)>,
    closed: bool,
}

struct State {
    next_seq: u64,
    /// Highest acked seq (durable together with every seq below it); −1 = none yet.
    acked_tip: i64,
    pending: BTreeMap<u32, Pending>,
    pending_bytes: usize,
    oldest_pending: Option<Instant>,
    /// The **scheduled** deadline of the latest cut (not when it actually ran) — the window
    /// counts from here, see `run`.
    last_due: Option<Instant>,
    flush_requested: bool,
    ranges: HashMap<u32, RangeState>,
    /// The stream is dead (sealed, a PUT with an unknown outcome): every operation returns
    /// this error.
    dead: Option<WalError>,
    /// Index of acked objects: seq → (range, wal id) of each frame — for GC.
    index: BTreeMap<u64, Vec<(u32, u64)>>,
    /// Ranges that have ever had frames in this log (for the whole life of the
    /// incarnation, never cleared) — for rule C1.
    had_frames: crate::entropy::HashSet<u32>,
    next_gen: u64,
    /// Per-range flush windows (D-h) — absent = `cfg.window`. Lives outside `ranges` so it
    /// survives every re-registration of the range.
    windows: HashMap<u32, std::time::Duration>,
    /// Ranges being held from "freezing by frame age" ([`Stripe::hold_age_freeze`]): the
    /// number of holders.
    age_freeze_holds: HashMap<u32, u32>,
}

/// While alive, the range's `should_flush_memtable` returns `false`: a write does not
/// freeze the memtable because a frame is over age. For the fence of `flush_through`: its
/// row must **stay in the memtable** until the explicit flush after it is durable, so that
/// flush's mark covers the fence itself. If writing the fence froze at once (an old frame of
/// the range is already over age), the mark would stop before the fence — the fence falls
/// into the gap, the next forced-flush tick chases it and writes a new fence, forever.
pub(crate) struct AgeFreezeHold {
    stripe: Arc<Stripe>,
    range: u32,
}

impl Drop for AgeFreezeHold {
    fn drop(&mut self) {
        let mut st = self.stripe.lock();
        if let Some(n) = st.age_freeze_holds.get_mut(&self.range) {
            *n -= 1;
            if *n == 0 {
                st.age_freeze_holds.remove(&self.range);
            }
        }
    }
}

/// One log stream of this incarnation.
pub(crate) struct Stripe {
    pub log: LogId,
    pub store: LogStore,
    cfg: NodeWalConfig,
    /// Declared capability of the WAL tier (`StoreLocation::atomic_put`): resolve
    /// `AlreadyExists` by re-reading or not.
    atomic_put: bool,
    /// Channel for asking to rotate the stream when a PUT stalls; `None` = fail-stop as
    /// before.
    rotate: Option<RotateTx>,
    /// The stream has rotated: it dies **without** fail-stop, and its log can be sealed and
    /// replayed by this very incarnation (`NodeWal::take_over`).
    retired: AtomicBool,
    state: Mutex<State>,
    wake: Notify,
    failstop: FailStop,
}

/// Outcome of one attempt of [`Stripe::put`].
enum Attempt {
    Done,
    Fenced(String),
    Retry { why: String, unknown: bool },
}

/// Outcome of [`Stripe::resolve`].
enum Resolution {
    Ours,
    /// Not ours; a string describing what the object is.
    Theirs(String),
    Absent,
    /// Unreadable; the error.
    Unknown(String),
}

/// An object that has been cut and is waiting for its PUT.
struct Cut {
    seq: u64,
    bytes: bytes::Bytes,
    /// (range, wal id, batch count, largest seq, generation).
    frames: Vec<(u32, u64, u64, Option<u64>, u64)>,
}

fn row_bytes(r: &RowEntry) -> usize {
    let v = match &r.value {
        ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
        ValueDeletable::Tombstone => 0,
    };
    r.key.len() + v + 40
}

impl Stripe {
    pub fn new(
        log: LogId,
        store: LogStore,
        cfg: NodeWalConfig,
        atomic_put: bool,
        failstop: FailStop,
        rotate: Option<RotateTx>,
    ) -> Arc<Self> {
        Arc::new(Stripe {
            log,
            store,
            cfg,
            atomic_put,
            rotate,
            retired: AtomicBool::new(false),
            state: Mutex::new(State {
                next_seq: 0,
                acked_tip: -1,
                pending: BTreeMap::new(),
                pending_bytes: 0,
                oldest_pending: None,
                last_due: None,
                flush_requested: false,
                ranges: HashMap::default(),
                dead: None,
                index: BTreeMap::new(),
                had_frames: Default::default(),
                next_gen: 0,
                windows: HashMap::default(),
                age_freeze_holds: HashMap::default(),
            }),
            wake: Notify::new(),
            failstop,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("WAL stream mutex")
    }

    /// Attaches the range to the stream for a newly opened Db; returns the first wal id
    /// actually used and the generation of this attachment.
    ///
    /// An old Db of the range on the stream that has **not** closed (abandoned after a lost
    /// lease, a seal past its bound, or a range acquisition that failed midway) is detached:
    /// its uncut rows are dropped (nobody has been acked), its durability waiters get
    /// `Closed`, and its handle (old generation) can no longer write. Before, this attachment
    /// was refused, and the incarnation could never take the range back (found in
    /// simulation, seed 48). Old frames already in the log are handled by rule C1 in
    /// `NodeWal::take_over` — before we get here. The first id is not lower than the next id
    /// of the previous generation: its late in-flight frames do not share an id with ours.
    ///
    /// `skipped_own`: the range acquisition skipped our own segment because its log had no
    /// frame of the range. Checked again **under the lock**, right before detaching the old
    /// Db: if a frame of the old Db was cut (and possibly acked) between the check in
    /// `take_over` and here, we would not replay it — refuse (C1); the next acquisition
    /// fail-stops.
    pub fn register(
        &self,
        range: u32,
        first_wal_id: u64,
        epoch: u64,
        skipped_own: bool,
    ) -> Result<(u64, u64), WalError> {
        let mut st = self.lock();
        if let Some(e) = &st.dead {
            return Err(e.clone());
        }
        if skipped_own && st.had_frames.contains(&range) {
            return Err(unavailable(format!(
                "range {range}: log {} has frames of the range after our own segment was skipped",
                self.log
            )));
        }
        let mut first_wal_id = first_wal_id;
        let mut orphaned = None;
        if let Some(old) = st.ranges.remove(&range) {
            first_wal_id = first_wal_id.max(old.next_wal_id);
            if !old.closed {
                orphaned = Some(old);
            }
            if let Some(p) = st.pending.remove(&range) {
                st.pending_bytes = st.pending_bytes.saturating_sub(p.bytes);
            }
        }
        let gen = st.next_gen;
        st.next_gen += 1;
        st.ranges.insert(
            range,
            RangeState {
                gen,
                next_wal_id: first_wal_id,
                epoch,
                status: WalStatus {
                    closed_reason: None,
                    estimated_bytes: 0,
                    last_flushed_wal_id: first_wal_id.saturating_sub(1),
                    last_flushed_seq: None,
                    buffered_wal_entries_count: 0,
                },
                listeners: Vec::new(),
                appended: 0,
                durable: 0,
                waiters: Vec::new(),
                unflushed: VecDeque::new(),
                closed: false,
            },
        );
        drop(st);
        if let Some(mut old) = orphaned {
            tracing::warn!(log = %self.log, range, "detaching the range's old unclosed Db from the stream");
            old.status.closed_reason = Some(WalError::Closed);
            for l in &old.listeners {
                l(WalEvent::WalClosed(old.status.clone()));
            }
            for (_, tx) in old.waiters.drain(..) {
                let _ = tx.send(Err(WalError::Closed));
            }
        }
        Ok((first_wal_id, gen))
    }

    /// Ranges with durable frames older than `age` that are not yet in L0, with the age and
    /// wal id of their oldest unflushed frame.
    pub fn stale_ranges(&self, age: std::time::Duration) -> Vec<(u32, std::time::Duration, u64)> {
        let st = self.lock();
        st.ranges
            .iter()
            .filter(|(_, rs)| !rs.closed)
            .filter_map(|(r, rs)| {
                let (id, at) = rs.unflushed.front()?;
                let oldest = at.elapsed();
                (oldest >= age).then_some((*r, oldest, *id))
            })
            .collect()
    }

    /// Drops from the range's "unflushed" queue every frame already below the manifest's
    /// `replay_after_wal_id`. SlateDB only asks `should_flush_memtable` (where this queue is
    /// trimmed) on a write, so an idle range that was just force-flushed needs this trim —
    /// otherwise it looks over age forever and gets force-flushed again on every tick.
    pub fn forget_flushed(&self, range: u32, replay_after_wal_id: u64) {
        let mut st = self.lock();
        if let Some(rs) = st.ranges.get_mut(&range) {
            while rs
                .unflushed
                .front()
                .is_some_and(|(id, _)| *id <= replay_after_wal_id)
            {
                rs.unflushed.pop_front();
            }
        }
    }

    /// Index of acked objects from `from` upwards.
    pub fn index_from(&self, from: u64) -> BTreeMap<u64, Vec<(u32, u64)>> {
        self.lock()
            .index
            .range(from..)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// GC has deleted every object below `floor`.
    pub fn forget_below(&self, floor: u64) {
        let mut st = self.lock();
        st.index = st.index.split_off(&floor);
    }

    /// Sets (or removes) the flush window of range `range` alone — effective from the next
    /// cut deadline computation.
    pub fn set_window(&self, range: u32, window: Option<std::time::Duration>) {
        {
            let mut st = self.lock();
            match window {
                Some(w) => st.windows.insert(range, w),
                None => st.windows.remove(&range),
            };
        }
        self.wake.notify_one();
    }

    pub fn is_dead(&self) -> bool {
        self.lock().dead.is_some()
    }

    /// The stream has rotated ([`Self::retire`]): it acks nothing any more.
    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    /// Per-range flush windows — the new stream of a rotation takes them over.
    pub fn windows(&self) -> HashMap<u32, std::time::Duration> {
        self.lock().windows.clone()
    }

    /// Sets every per-range window at once (the new stream of a rotation).
    pub fn set_windows(&self, windows: HashMap<u32, std::time::Duration>) {
        self.lock().windows = windows;
        self.wake.notify_one();
    }

    /// Highest acked seq (−1 = none yet) — the index loop counts new objects by it.
    pub fn acked_tip(&self) -> i64 {
        self.lock().acked_tip
    }

    /// The stream's cut-and-PUT loop. Exits when the stream dies.
    ///
    /// At most `W = cfg.inflight` PUTs in flight at once; results are processed **in cut
    /// order** (`FuturesOrdered`), so object n+1 is only acked after n. If n's PUT misses ⇒
    /// the stream dies before looking at n+1 — object n+1 may already be on the store, but
    /// above the hole n, outside the canonical part, and it is never acked.
    pub async fn run(self: Arc<Self>) {
        let depth = self.cfg.inflight.max(1);
        let mut inflight = FuturesOrdered::new();
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let due = {
                let st = self.lock();
                if st.dead.is_some() {
                    return;
                }
                if inflight.len() >= depth {
                    None
                } else {
                    // The window is **the spacing between two cut deadlines**, not the
                    // delay since the first row, and the deadline counts from the previous
                    // deadline rather than from when the previous cut actually ran:
                    //
                    // - a row arriving after an idle spell longer than the window is cut at
                    //   once;
                    // - under load, the deadlines sit on an evenly spaced grid one window
                    //   apart, like the native WAL's flush tick — timer latency (≈ 1–3 ms
                    //   on a busy runtime) does not accumulate.
                    //
                    // Before, deadline = first row + window: every commit waited a whole
                    // window plus the timer latency, and the grid drifted by that latency
                    // (bench `load.ts` on `memory://`: 14–26 % slower than the native WAL).
                    //
                    // The stream's window is **the smallest among the ranges with frames
                    // waiting** (D-h): a low-tier range sharing a stream with a high-tier
                    // range enjoys its speed — nobody is slower than its own tier; a stream
                    // with only low-tier ranges cuts sparsely, i.e. fewer PUTs — exactly what
                    // that tier sells.
                    let window = st
                        .pending
                        .keys()
                        .map(|r| st.windows.get(r).copied().unwrap_or(self.cfg.window))
                        .min()
                        .unwrap_or(self.cfg.window);
                    // Linger: the first row waits an extra L — `0` gives exactly the old
                    // deadline. Clamped to `MAX_LINGER`: `t + L` does not overflow
                    // `Instant` (a huge L once panicked the stream's task).
                    let linger = self.cfg.linger.min(super::MAX_LINGER);
                    st.oldest_pending.map(|t| {
                        if st.flush_requested || st.pending_bytes >= self.cfg.object_cap {
                            Instant::now()
                        } else {
                            let first = t + linger;
                            st.last_due.map_or(first, |d| first.max(d + window))
                        }
                    })
                }
            };
            if let Some(d) = due.filter(|d| *d <= Instant::now()) {
                if let Some(cut) = self.cut(d) {
                    let me = Arc::clone(&self);
                    inflight.push_back(async move {
                        crate::probe::mark("wal.put", cut.seq);
                        let res = me.put(cut.seq, cut.bytes.clone()).await;
                        crate::probe::mark("wal.put_done", cut.seq);
                        (cut, res)
                    });
                }
                continue;
            }
            let sleep = due.map(tokio::time::sleep_until);
            // `biased`: acks first — durability waiters are released as early as possible;
            // deadlines and messages are only wake-ups, the top of the loop reconsiders
            // everything. In-flight PUTs are bounded, so the first branch does not starve
            // the other two.
            tokio::select! {
                biased;
                Some((cut, res)) = inflight.next(), if !inflight.is_empty() => match res {
                    // The stream was rotated **from outside** while the PUT was in flight
                    // (a dirty yield): every range is closed, the taker may already have
                    // sealed the log — no ack, no fail-stop on meeting that seal, no second
                    // rotation request.
                    _ if self.is_retired() => return,
                    Ok(()) => self.ack(cut),
                    Err(PutFailure::Fenced(error, cause, reason)) => {
                        tracing::error!(log = %self.log, cause = %cause, reason = %reason, "shared WAL stream died — fail-stop the incarnation");
                        self.die(error, cause, reason);
                        return;
                    }
                    Err(PutFailure::Stalled { unknown, last, reason }) => {
                        self.stalled(unknown, last, reason);
                        return;
                    }
                },
                _ = async { sleep.expect("has a deadline").await }, if sleep.is_some() => {}
                _ = notified => {}
            }
        }
    }

    /// Cuts the buffer into an object; `due` is the scheduled deadline of this cut.
    fn cut(&self, due: Instant) -> Option<Cut> {
        crate::failpoint!("nodewal::stripe::cut");
        let mut st = self.lock();
        if st.pending.is_empty() {
            st.oldest_pending = None;
            st.flush_requested = false;
            return None;
        }
        if let Some(t) = st.oldest_pending {
            // How long the first row of this object waited on the window grid (µs).
            crate::probe::mark("wal.cut_wait_us", t.elapsed().as_micros() as u64);
        }
        crate::probe::mark("wal.cut_ranges", st.pending.len() as u64);
        let pending = std::mem::take(&mut st.pending);
        st.pending_bytes = 0;
        st.oldest_pending = None;
        st.flush_requested = false;
        st.last_due = Some(due);
        let mut frames = Vec::with_capacity(pending.len());
        let mut meta = Vec::with_capacity(pending.len());
        for (range, p) in pending {
            let rs = st
                .ranges
                .get_mut(&range)
                .expect("pending rows of an unattached range");
            let id = rs.next_wal_id;
            rs.next_wal_id += 1;
            rs.status.estimated_bytes = rs.status.estimated_bytes.saturating_sub(p.bytes);
            rs.status.buffered_wal_entries_count = 0;
            let max_seq = p.rows.iter().map(|r| r.seq).max();
            meta.push((range, id, p.batches, max_seq, rs.gen));
            frames.push(Frame {
                range,
                epoch: rs.epoch,
                wal_file_id: id,
                rows: p.rows,
            });
        }
        for f in &frames {
            st.had_frames.insert(f.range);
        }
        let seq = st.next_seq;
        st.next_seq += 1;
        let bytes = format::encode_data(st.acked_tip, &frames);
        Some(Cut {
            seq,
            bytes,
            frames: meta,
        })
    }

    /// `Create` at `seq` within the budget `stall_max` = `put_timeout × put_attempts`. When
    /// the budget runs out, returns [`PutFailure::Stalled`] — the stream rotates
    /// ([`Self::stalled`]).
    ///
    /// **A slow PUT is not cancelled** (like upstream SlateDB): an attempt past
    /// `put_timeout` keeps running, and a **parallel** `Create` (same bytes) is started; the
    /// first to finish wins, and the outcome of every attempt is resolved by re-reading as
    /// below. Before, `tokio::time::timeout` cancelled the attempt's future — but the
    /// request may already have gone out, and a uniformly slow store (every PUT 1.5 ×
    /// `put_timeout`) never had any attempt finish. Errors that come back fast (503
    /// SlowDown under throttling) are retried after a jittered backoff: the budget is not
    /// burnt in ~0.1 ms.
    ///
    /// **`AlreadyExists`**: object_store retries every 5xx by itself, `Create` included,
    /// and a slow attempt may still arrive — so `AlreadyExists` on **any** attempt may be
    /// our own write. On an `atomic_put` store the stream re-reads the object at `seq`
    /// ([`Self::resolve`]) — the four outcomes of [`PutResolved`]. The comparison is
    /// meaningful because only this stream writes **data** objects at the path
    /// `<node>/<inc>/<stripe>/<seq>` (`inc` is never reused), every attempt sends the same
    /// bytes, and a sealer only writes seals. A non-atomic store is not re-read (our read
    /// could see the half with data of a torn object while the sealer sees it empty): it
    /// counts as sealed, as before.
    async fn put(&self, seq: u64, bytes: bytes::Bytes) -> Result<(), PutFailure> {
        use futures::stream::FuturesUnordered;
        let deadline = Instant::now() + self.cfg.stall_max();
        // Jitter: streams (and nodes) hitting the same burst of 503s do not retry in
        // lockstep.
        let mut backoff = crate::pace::Backoff::new(PUT_BACKOFF_MIN, PUT_BACKOFF_MAX);
        let mut attempts = FuturesUnordered::new();
        // An attempt already counted as "over the bound" for the slowness meter: if it
        // arrives late it is not measured again.
        let mut metered: Vec<bool> = Vec::new();
        let mut launched: u32 = 0;
        let mut last = String::from("no attempt finished yet");
        // The latest re-read could not tell whose object it is: if the budget runs out at
        // that point the outcome is "unknown", not "stalled".
        let mut unknown = false;
        // When to start the next attempt: now, after `put_timeout` if the one in flight
        // has not finished, or after a backoff if every attempt failed fast.
        let mut next = Instant::now();
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            if now >= next {
                launched += 1;
                metered.push(false);
                attempts.push(self.attempt(seq, bytes.clone(), launched, deadline));
                next = now + self.cfg.put_timeout;
            }
            tokio::select! {
                biased;
                Some((n, started, outcome)) = attempts.next(), if !attempts.is_empty() => {
                    // Slowness signal — atomics only, no lock.
                    if let Some(m) = &self.cfg.meter {
                        if !metered[n as usize - 1] {
                            m.wal_attempt(started.elapsed(), matches!(outcome, Attempt::Done));
                        }
                    }
                    match outcome {
                        Attempt::Done => return Ok(()),
                        Attempt::Fenced(what) => return Err(self.fenced(seq, n, &what)),
                        Attempt::Retry { why, unknown: u } => {
                            last = why;
                            unknown = u;
                            if attempts.is_empty() {
                                let left = deadline.saturating_duration_since(Instant::now());
                                let pause = backoff.next_pause().min(left);
                                tracing::warn!(log = %self.log, seq, attempt = n, error = %last, pause_ms = pause.as_millis() as u64, "shared WAL log PUT missed, retrying");
                                next = Instant::now() + pause;
                            }
                        }
                    }
                }
                () = tokio::time::sleep_until(next.min(deadline)) => {
                    // The attempt in flight is past `put_timeout`: do not cancel it, start
                    // one more in parallel at the top of the loop. For the meter, that is
                    // one "over the bound" as before.
                    if !attempts.is_empty() && Instant::now() >= next {
                        if let Some(m) = &self.cfg.meter {
                            for done in metered.iter_mut().filter(|d| !**d) {
                                *done = true;
                                m.wal_attempt(self.cfg.put_timeout, false);
                            }
                        }
                        last = format!("PUT took longer than {:?}", self.cfg.put_timeout);
                        unknown = false;
                        tracing::warn!(log = %self.log, seq, attempt = launched, "shared WAL log PUT slower than put_timeout — adding a parallel attempt");
                    }
                }
            }
        }
        // Attempts still in flight are abandoned here: the request may still arrive, but the
        // stream acks nothing any more and its log is sealed before anyone replays it.
        if unknown {
            note_resolved(PutResolved::Unknown);
        }
        Err(PutFailure::Stalled {
            unknown,
            reason: format!(
                "log {} #{seq}: PUT {} after {launched} attempts in {:?} ({last})",
                self.log,
                if unknown {
                    "with unknown outcome"
                } else {
                    "not finished"
                },
                self.cfg.stall_max()
            ),
            last,
        })
    }

    /// One attempt of [`Self::put`]: `Create`, then a re-read on `AlreadyExists`. Returns
    /// (attempt number, start time, outcome).
    async fn attempt(
        &self,
        seq: u64,
        bytes: bytes::Bytes,
        n: u32,
        deadline: Instant,
    ) -> (u32, Instant, Attempt) {
        let started = Instant::now();
        // `return(msg)` fails **one attempt** (the retry loop keeps going); failing every
        // attempt runs out the budget like a real PUT.
        if let Some(arg) = crate::failpoint::hit("nodewal::stripe::put") {
            let why = format!(
                "failpoint nodewal::stripe::put: {}",
                arg.unwrap_or_default()
            );
            return (
                n,
                started,
                Attempt::Retry {
                    why,
                    unknown: false,
                },
            );
        }
        let outcome = match self.store.create(seq, bytes.clone()).await {
            Ok(Created::Yes) => Attempt::Done,
            Ok(Created::AlreadyExists) if self.atomic_put => {
                let left = deadline
                    .saturating_duration_since(Instant::now())
                    .max(PUT_ATTEMPT_MIN);
                match self
                    .resolve(seq, &bytes, self.cfg.put_timeout.min(left))
                    .await
                {
                    Resolution::Ours => {
                        note_resolved(PutResolved::Ours);
                        tracing::warn!(log = %self.log, seq, attempt = n, "ambiguous shared WAL log PUT: the object at seq is our own — ack");
                        Attempt::Done
                    }
                    Resolution::Theirs(what) => {
                        note_resolved(PutResolved::Theirs);
                        Attempt::Fenced(what)
                    }
                    Resolution::Absent => {
                        note_resolved(PutResolved::Absent);
                        Attempt::Retry {
                            why: "AlreadyExists but the re-read found no object".into(),
                            unknown: false,
                        }
                    }
                    Resolution::Unknown(e) => Attempt::Retry {
                        why: format!("AlreadyExists, re-read failed: {e}"),
                        unknown: true,
                    },
                }
            }
            // A store that can tear objects (`atomic_put = false`): re-reading to tell "our
            // earlier send" from "sealed" is not safe — treat it as fenced.
            Ok(Created::AlreadyExists) => Attempt::Fenced("AlreadyExists, no re-read".into()),
            Err(e) => Attempt::Retry {
                why: e.to_string(),
                unknown: false,
            },
        };
        (n, started, outcome)
    }

    /// The "log sealed at `seq`" error: `wal_fenced` when met on the first attempt,
    /// `wal_ambiguous` when an earlier attempt had an unknown outcome. On an `atomic_put`
    /// store both are now only ever a real seal.
    fn fenced(&self, seq: u64, attempt: u32, what: &str) -> PutFailure {
        if attempt == 1 {
            PutFailure::Fenced(
                WalError::Fenced,
                Cause::WalFenced,
                format!("log {} sealed at #{seq} ({what})", self.log),
            )
        } else {
            PutFailure::Fenced(
                WalError::Fenced,
                Cause::WalAmbiguous,
                format!(
                    "log {} #{seq}: PUT with unknown outcome, then sealed ({what})",
                    self.log
                ),
            )
        }
    }

    /// Re-reads the object at `seq` after `AlreadyExists`, bounded by `limit`. Only called
    /// on an `atomic_put` store: the object there is one whole write.
    ///
    /// Compares **all bytes** with `ours` (the object is ≤ `object_cap`, and this read is
    /// rare): stronger than reading the header, it also catches a log reusing a name (a
    /// reused incarnation — NW-2) instead of acking wrongly onto an earlier life's data.
    async fn resolve(
        &self,
        seq: u64,
        ours: &bytes::Bytes,
        limit: std::time::Duration,
    ) -> Resolution {
        match tokio::time::timeout(limit, self.store.get_raw(seq)).await {
            Ok(Ok(Some(got))) if got == *ours => Resolution::Ours,
            Ok(Ok(Some(got))) => Resolution::Theirs(match format::decode(&got) {
                Ok(format::Decoded::Seal { sealer, .. }) => format!("seal by {sealer}"),
                Ok(format::Decoded::Data(_)) => {
                    tracing::error!(log = %self.log, seq, "unknown data object at the stream's own seq — no ack");
                    "data object that is not our bytes".into()
                }
                Err(e) => {
                    tracing::error!(log = %self.log, seq, error = %e, "undecodable object at the stream's own seq — no ack");
                    format!("damaged object: {e}")
                }
            }),
            Ok(Ok(None)) => Resolution::Absent,
            Ok(Err(e)) => Resolution::Unknown(e.to_string()),
            Err(_) => Resolution::Unknown(format!("GET took longer than {limit:?}")),
        }
    }

    fn ack(&self, cut: Cut) {
        crate::failpoint!("nodewal::stripe::ack");
        crate::probe::mark("wal.ack", cut.seq);
        let now = Instant::now();
        let mut events = Vec::new();
        let mut resolved = Vec::new();
        {
            let mut st = self.lock();
            if self.is_retired() {
                // Rotated from outside between the PUT finishing and here: every range is
                // closed, nothing to report.
                return;
            }
            st.acked_tip = cut.seq as i64;
            st.index.insert(
                cut.seq,
                cut.frames
                    .iter()
                    .map(|(r, id, _, _, _)| (*r, *id))
                    .collect(),
            );
            for (range, id, batches, max_seq, gen) in &cut.frames {
                let Some(rs) = st.ranges.get_mut(range).filter(|rs| rs.gen == *gen) else {
                    continue;
                };
                rs.durable += batches;
                rs.status.last_flushed_wal_id = *id;
                if let Some(s) = max_seq {
                    rs.status.last_flushed_seq =
                        Some(rs.status.last_flushed_seq.unwrap_or(0).max(*s));
                }
                rs.unflushed.push_back((*id, now));
                events.push((rs.listeners.clone(), rs.status.clone()));
                let durable = rs.durable;
                let (done, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut rs.waiters)
                    .into_iter()
                    .partition(|(t, _)| *t <= durable);
                rs.waiters = keep;
                resolved.extend(done.into_iter().map(|(_, tx)| tx));
            }
        }
        // Call the listeners **before** releasing the `flush` waiters: SlateDB bumps the
        // Db's `next_wal_id` in the listener, and the next memtable flush must see it.
        for (listeners, status) in events {
            for l in listeners {
                l(WalEvent::WalFlushed(status.clone()));
            }
        }
        for tx in resolved {
            let _ = tx.send(Ok(()));
        }
    }

    /// The `stall_max` budget ran out (or the outcome is "unknown"): ask `NodeWal` to rotate
    /// the stream — this stream stops acking and closes all its ranges **before** the
    /// request goes out, so no object of this log is acked after this point. No rotation
    /// channel (or the rotation loop has stopped) ⇒ fail-stop as before streams could
    /// rotate.
    fn stalled(self: &Arc<Self>, unknown: bool, last: String, reason: String) {
        let cause = if unknown {
            RotationCause::Unknown
        } else {
            RotationCause::Stall
        };
        if let Some(tx) = &self.rotate {
            tracing::warn!(log = %self.log, cause = cause.label(), reason = %reason, "shared WAL stream stalled — rotating the stream");
            if !self.retire(&reason) {
                // Already rotated from outside (a dirty yield): the request has gone out.
                return;
            }
            if tx
                .send(Rotation {
                    old: Arc::clone(self),
                    cause,
                    reason: reason.clone(),
                })
                .is_ok()
            {
                return;
            }
            // The rotation loop has stopped (the incarnation is shutting down): the stream
            // is closed, only the reason is left to report.
            self.failstop.trigger(Cause::WalStall, reason);
            return;
        }
        tracing::error!(log = %self.log, cause = %Cause::WalStall, reason = %reason, "shared WAL stream died — fail-stop the incarnation");
        self.die(
            unavailable(format!("log {}: {last}", self.log)),
            Cause::WalStall,
            reason,
        );
    }

    /// The stream rotates: like [`Self::die`] but **without** triggering fail-stop. The Dbs
    /// get `Fenced` (this log is about to be sealed by ourselves) — the range's leadership
    /// loop steps down and takes the range back on the new stream. Returns `true` for
    /// exactly **one** call — the one that flips the flag; that call sends the rotation
    /// request: a stalled stream ([`Self::stalled`]) and a dirty yield
    /// ([`Self::rotate_for_dirty_yield`]) may race, and with two requests for one stream the
    /// later one would no longer find its slot.
    ///
    /// The node **drains first**, before closing the ranges: this stream's ranges step down
    /// as soon as they get `Fenced`, and the balancer must not take them back into a slot
    /// that has no stream. The rotation loop clears the flag once every slot has a stream
    /// again (`Inner::refresh_wal_draining`).
    pub(crate) fn retire(&self, reason: &str) -> bool {
        if self.retired.swap(true, Ordering::AcqRel) {
            return false;
        }
        if let Some(m) = &self.cfg.meter {
            m.set_wal_draining(true);
        }
        self.close_all(WalError::Fenced);
        tracing::debug!(log = %self.log, reason = %reason, "shared WAL stream rotated");
        true
    }

    /// **Rotate because of a dirty yield**: a range of the stream is about to step down
    /// while its Db has not sealed cleanly, and the owner record will keep a segment
    /// pointing at this log. The stream stops acking and closes all its ranges **before**
    /// returning — that is, before the owner record is written —, then asks `NodeWal` to
    /// rotate like a stalled stream. The range taker seals a log that has stopped acking:
    /// harmless, the node lives (before this, the log was live ⇒ the next PUT met the seal
    /// ⇒ the whole node fail-stopped). The stream's other ranges get `Fenced`, step down and
    /// are taken back on the new stream, as when the stream stalls.
    ///
    /// `false`: the stream has already rotated or died — nothing to do. No rotation channel:
    /// the stream still stops acking — the owner record points at a closed log —, there is
    /// just no new stream. The rotation loop has stopped (the incarnation is shutting down):
    /// stop acking, then fail-stop, like a stalled stream ([`Self::stalled`]).
    pub(crate) fn rotate_for_dirty_yield(self: &Arc<Self>, reason: String) -> bool {
        if self.is_dead() {
            return false;
        }
        if !self.retire(&reason) {
            return false;
        }
        tracing::warn!(log = %self.log, cause = RotationCause::DirtyYield.label(), reason = %reason, "dirty yield — rotating the shared WAL stream before writing the owner record");
        if let Some(tx) = &self.rotate {
            if tx
                .send(Rotation {
                    old: Arc::clone(self),
                    cause: RotationCause::DirtyYield,
                    reason: reason.clone(),
                })
                .is_err()
            {
                // The rotation loop has stopped (the incarnation is shutting down): the
                // stream's slot will never get a new stream. Report it like
                // [`Self::stalled`] in the same situation, do not drain silently.
                self.failstop.trigger(Cause::DirtyYield, reason);
            }
        }
        true
    }

    /// The stream stops accepting frames: durability waiters get an error, `failstop` fires
    /// with `reason`. The caller logs — a real error at `error` level, a deliberate stop
    /// (the incarnation has ended) not: a "fail-stop" line on every graceful shutdown once
    /// sent investigations down the wrong path.
    pub(crate) fn die(&self, error: WalError, cause: Cause, reason: String) {
        self.close_all(error);
        self.failstop.trigger(cause, reason);
    }

    /// Marks the stream dead, drops uncut rows, closes every range with `error`.
    fn close_all(&self, error: WalError) {
        let mut events = Vec::new();
        let mut failed = Vec::new();
        {
            let mut st = self.lock();
            st.dead = Some(error.clone());
            st.pending.clear();
            st.pending_bytes = 0;
            st.oldest_pending = None;
            for rs in st.ranges.values_mut() {
                if rs.status.closed_reason.is_none() {
                    rs.status.closed_reason = Some(error.clone());
                }
                events.push((rs.listeners.clone(), rs.status.clone()));
                failed.extend(rs.waiters.drain(..).map(|(_, tx)| tx));
            }
        }
        for (listeners, status) in events {
            for l in listeners {
                l(WalEvent::WalClosed(status.clone()));
            }
        }
        for tx in failed {
            let _ = tx.send(Err(error.clone()));
        }
    }

    fn append(&self, key: Key, rows: &[RowEntry]) -> Result<(), WalError> {
        let bytes: usize = rows.iter().map(row_bytes).sum();
        {
            let mut st = self.lock();
            if let Some(e) = &st.dead {
                return Err(e.clone());
            }
            let range = key.0;
            let rs = st
                .ranges
                .get_mut(&range)
                .filter(|rs| rs.gen == key.1)
                .ok_or(WalError::Closed)?;
            if rs.closed {
                return Err(WalError::Closed);
            }
            rs.appended += 1;
            crate::probe::mark("wal.append", u64::from(range));
            rs.status.estimated_bytes += bytes;
            rs.status.buffered_wal_entries_count += rows.len();
            let p = st.pending.entry(range).or_default();
            p.rows.extend_from_slice(rows);
            p.batches += 1;
            p.bytes += bytes;
            st.pending_bytes += bytes;
            if st.oldest_pending.is_none() {
                st.oldest_pending = Some(Instant::now());
            }
        }
        self.wake.notify_one();
        Ok(())
    }

    /// The returned `wal_id`: the id of the frame that will hold the range's pending rows
    /// (allocated at cut time, `cut`), or with no pending rows the latest allocated frame
    /// id. It is an **upper bound** for rows appended before the call, does not decrease
    /// across calls, and `last_flushed_wal_id` reaches it once the frame is durable. The rule
    /// "rows appended **after** the call go into a higher id" of `WalWriter::flush` (SlateDB
    /// #2084) does **not** hold: rows arriving before the cut go into the same frame. That
    /// rule only serves the mark of `Db::begin_checkpoint`/`create_checkpoint` with the WAL
    /// enabled — cairn never calls `Db` checkpoints (G16; see the library assumptions in
    /// `docs/reliability/irreversible.md` §0.3).
    fn flush(&self, key: Key) -> Result<WalFlushResult, WalError> {
        let (wal_id, rx) = {
            let mut st = self.lock();
            if let Some(e) = &st.dead {
                return Err(e.clone());
            }
            let range = key.0;
            let pending = st.pending.contains_key(&range);
            let rs = st
                .ranges
                .get_mut(&range)
                .filter(|rs| rs.gen == key.1)
                .ok_or(WalError::Closed)?;
            let wal_id = if pending {
                rs.next_wal_id
            } else {
                rs.next_wal_id.saturating_sub(1)
            };
            let target = rs.appended;
            if rs.durable >= target {
                return Ok(WalFlushResult {
                    wal_id,
                    completion: Box::pin(async { Ok(()) }),
                });
            }
            let (tx, rx) = oneshot::channel();
            rs.waiters.push((target, tx));
            crate::probe::mark("wal.flush_req", u64::from(range));
            st.flush_requested = true;
            (wal_id, rx)
        };
        self.wake.notify_one();
        Ok(WalFlushResult {
            wal_id,
            completion: Box::pin(async move { rx.await.unwrap_or(Err(WalError::Closed)) }),
        })
    }

    fn status(&self, key: Key) -> Result<WalStatus, WalStatus> {
        let st = self.lock();
        let Some(rs) = st.ranges.get(&key.0).filter(|rs| rs.gen == key.1) else {
            return Err(WalStatus {
                closed_reason: Some(WalError::Closed),
                estimated_bytes: 0,
                last_flushed_wal_id: 0,
                last_flushed_seq: None,
                buffered_wal_entries_count: 0,
            });
        };
        if rs.status.closed_reason.is_some() {
            Err(rs.status.clone())
        } else {
            Ok(rs.status.clone())
        }
    }

    fn subscribe(&self, key: Key, listener: WalStatusListener) -> Result<(), WalError> {
        let mut st = self.lock();
        let range = key.0;
        let rs = st
            .ranges
            .get_mut(&range)
            .filter(|rs| rs.gen == key.1)
            .ok_or(WalError::Closed)?;
        rs.listeners.push(listener);
        Ok(())
    }

    fn should_flush_memtable(&self, key: Key, replay_after_wal_id: u64) -> bool {
        let mut st = self.lock();
        let held = st.age_freeze_holds.contains_key(&key.0);
        let Some(rs) = st.ranges.get_mut(&key.0).filter(|rs| rs.gen == key.1) else {
            return false;
        };
        while rs
            .unflushed
            .front()
            .is_some_and(|(id, _)| *id <= replay_after_wal_id)
        {
            rs.unflushed.pop_front();
        }
        !held
            && rs
                .unflushed
                .front()
                .is_some_and(|(_, t)| t.elapsed() >= self.cfg.frame_age_flush)
    }

    /// Holds range `range` from freezing the memtable by frame age until the guard goes out
    /// of scope — see [`AgeFreezeHold`].
    pub(crate) fn hold_age_freeze(self: &Arc<Self>, range: u32) -> AgeFreezeHold {
        *self.lock().age_freeze_holds.entry(range).or_default() += 1;
        AgeFreezeHold {
            stripe: Arc::clone(self),
            range,
        }
    }

    /// This log has had frames of range `range` (cut into an object).
    pub fn had_frames(&self, range: u32) -> bool {
        self.lock().had_frames.contains(&range)
    }

    /// Highest wal id allocated to the range — for `wal_floor_id` on a clean release.
    pub fn last_wal_id(&self, range: u32) -> Option<u64> {
        self.lock()
            .ranges
            .get(&range)
            .map(|rs| rs.next_wal_id.saturating_sub(1))
    }

    fn close(&self, key: Key) {
        let event = {
            let mut st = self.lock();
            let Some(rs) = st.ranges.get_mut(&key.0).filter(|rs| rs.gen == key.1) else {
                return;
            };
            if rs.closed {
                return;
            }
            rs.closed = true;
            if rs.status.closed_reason.is_none() {
                rs.status.closed_reason = Some(WalError::Closed);
            }
            (rs.listeners.clone(), rs.status.clone())
        };
        for l in event.0 {
            l(WalEvent::WalClosed(event.1.clone()));
        }
    }
}

/// Key of one attachment of a range to a stream: (range, generation).
pub(crate) type Key = (u32, u64);

/// `WalWriter` of **one** range on one stream.
pub(crate) struct RangeWal {
    pub stripe: Arc<Stripe>,
    /// (range, registration generation).
    pub key: Key,
}

#[async_trait]
impl WalWriter for RangeWal {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
        self.stripe.append(self.key, write_batch)
    }

    async fn flush(&mut self) -> Result<WalFlushResult, WalError> {
        self.stripe.flush(self.key)
    }

    fn should_flush_memtable(&self, replay_after_wal_id: u64) -> bool {
        self.stripe
            .should_flush_memtable(self.key, replay_after_wal_id)
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(RangeObserver {
            stripe: Arc::clone(&self.stripe),
            key: self.key,
        })
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.stripe.status(self.key)
    }

    /// Pushes out the range's remaining rows, then detaches the range from the stream —
    /// **without** closing the stream: the other ranges keep writing.
    async fn close(&mut self) -> Result<(), WalError> {
        if let Ok(f) = self.stripe.flush(self.key) {
            let _ = f.completion.await;
        }
        self.stripe.close(self.key);
        Ok(())
    }
}

struct RangeObserver {
    stripe: Arc<Stripe>,
    key: Key,
}

impl WalObserver for RangeObserver {
    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.stripe.status(self.key)
    }

    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError> {
        self.stripe.subscribe(self.key, listener)
    }
}
