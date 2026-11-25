// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared WAL follower: **one tailer per remote log**, splitting frames out to every local
//! `DbReader` through [`SharedWalReader`].
//!
//! - A tailer reads the log from the floor, GETs the next object by its predictable name
//!   (no LIST), stops at the first spot that is not data, waits one tick and tries again.
//!   A follower's GET/HEAD therefore scale with **node pairs** (and streams), not ranges.
//! - On an `atomic_put` store the tailer **exposes every data object contiguous from the
//!   floor** as soon as it can read it, like SlateDB's reader: an object seen is a whole
//!   write, and the sealer (read-after-write consistency, S3-1) sees it too — a seal
//!   `Create` there misses, so a seal only lands in the first hole above, and every object
//!   below the seal is replayed by the taker. The tailer reads sequentially and stops at the
//!   first spot that is not data, so an object above a hole (W > 1) is never read. Model:
//!   M3 `expose_contiguous` (`Action::TailRead`), separate model `--expose-contiguous`.
//! - A store that can tear objects (`atomic_put = false`): the tailer keeps `wm` = the
//!   largest `acked_upto` seen and only **exposes** objects with `seq ≤ wm` (counterexample
//!   C5): an object the writer has not acked may be torn — the follower reads the half with
//!   data, the sealer sees it empty and puts a seal over it — and nothing already exposed
//!   may disappear. The tail of a quiet log is then not exposed (I4): the follower defers
//!   to the leader (`SharedWalReader::unexposed_since`).
//! - GC has passed the spot a reader needs ⇒ the iterator returns `WalTruncated` ⇒ the
//!   reader (fork patch F1, `Stream` mode) loads the new manifest at once. A reader only
//!   accepts frames **contiguous** by wal id: a next frame with an id greater than the one
//!   the reader needs ⇒ `WalTruncated` at that id, even without knowing what GC deleted — a
//!   tailer slow across a GC tick jumps the floor over objects it never read (they do not
//!   go into `dropped`), or the owner record drops the segment of an old log the reader has
//!   not finished. Applying a frame after a gap would serve a state that is not a prefix.
//! - A tailer stops when it meets a seal, when the log disappears (a dead log purged), or
//!   when no reader follows the log any more. A stopped tailer nobody holds is pruned from
//!   `NodeTailers`.
//! - A tailer that has caught up **backs off** its GETs, doubling on each miss up to
//!   [`BACKOFF_POLLS`] poll ticks; reading an object returns it to the poll tick, and a
//!   follower read ([`NodeTailers::nudge`]) pulls every tailer back to the poll tick at
//!   once. The GC floor is LISTed by time, not by number of misses.

use crate::entropy::HashMap;
// The runtime's clock: outside simulation it is `std::time::Instant`, inside deterministic
// simulation it is the virtual clock.
use tokio::time::Instant;
use {
    super::{
        format::{DataObject, Decoded, Frame},
        log::LogStore,
        SHARED_WAL_NAME,
    },
    crate::{
        config::ObjectStores,
        error::Result,
        ids::PartitionId,
        owner::{self, LogId, WalKind},
    },
    async_trait::async_trait,
    slatedb::{
        object_store::{path::Path, ObjectStore},
        wal::{WalError, WalFileRange, WalIterator, WalReader, WalRows},
    },
    std::{
        collections::{BTreeMap, VecDeque},
        ops::Bound,
        sync::{Arc, Mutex, Weak},
        time::Duration,
    },
    tokio::sync::watch,
};

/// Re-read the GC floor (one LIST) at most once every this many poll ticks — the first
/// time on the tailer's first miss (it may start below the floor).
const FLOOR_EVERY_POLLS: u32 = 10;

/// A tailer that has caught up backs off up to this many poll ticks between two GETs: an
/// idle log costs one GET per `BACKOFF_POLLS × poll` instead of per `poll`. In exchange,
/// the first object after an idle stretch reaches the follower at most that late — unless
/// someone reads from the follower ([`NodeTailers::nudge`]).
const BACKOFF_POLLS: u32 = 8;

struct TailState {
    next: u64,
    wm: i64,
    objects: BTreeMap<u64, DataObject>,
    /// Index of `objects` by range: wal id → `seq` of the object carrying that frame (each
    /// range has at most one frame per object, ids grow with `seq`). A reader finds a
    /// range's next frame in O(log n) instead of rebuilding, sorting and filtering every
    /// frame of every range on each `next`.
    by_range: HashMap<u32, BTreeMap<u64, u64>>,
    /// When the tailer read each **unexposed** object (`seq > wm`): the tail the follower
    /// knows it is missing ([`SharedWalReader::unexposed_since`]). An object leaves here once
    /// exposed.
    read_at: BTreeMap<u64, Instant>,
    /// Largest wal id of each range found in objects GC deleted before we got to emit them —
    /// a reader needing an id ≤ this mark must load the manifest.
    dropped: HashMap<u32, u64>,
    sealed: bool,
}

struct LogTailer {
    log: LogId,
    store: LogStore,
    /// `atomic_put` store: expose every object contiguous from the floor, without waiting
    /// for `acked_upto`.
    contiguous: bool,
    state: Mutex<TailState>,
    /// The `run` loop has ended (seal, log gone, nobody following).
    done: std::sync::atomic::AtomicBool,
}

impl LogTailer {
    fn lock(&self) -> std::sync::MutexGuard<'_, TailState> {
        self.state.lock().expect("mutex tailer")
    }

    /// Exposed frames (seq ≤ wm) of `range` with wal id ≥ `from`, by wal id, at most
    /// `limit`.
    fn exposed_from(&self, range: u32, from: u64, limit: usize) -> Vec<Frame> {
        let st = self.lock();
        if st.wm < 0 {
            return Vec::new();
        }
        let wm = st.wm as u64;
        let Some(ids) = st.by_range.get(&range) else {
            return Vec::new();
        };
        ids.range(from..)
            .take_while(|(_, seq)| **seq <= wm)
            .take(limit)
            .filter_map(|(id, seq)| {
                st.objects
                    .get(seq)?
                    .frames
                    .iter()
                    .find(|f| f.range == range && f.wal_file_id == *id)
                    .cloned()
            })
            .collect()
    }

    /// Largest exposed wal id of `range`.
    fn last_exposed(&self, range: u32) -> Option<u64> {
        let st = self.lock();
        if st.wm < 0 {
            return None;
        }
        let wm = st.wm as u64;
        st.by_range
            .get(&range)?
            .iter()
            .rev()
            .find(|(_, seq)| **seq <= wm)
            .map(|(id, _)| *id)
    }

    fn dropped(&self, range: u32) -> Option<u64> {
        self.lock().dropped.get(&range).copied()
    }

    /// When the tailer read the earliest **unexposed** object with a frame of `range` —
    /// `None` when the unexposed tail has nothing of the range.
    fn unexposed_since(&self, range: u32) -> Option<Instant> {
        let st = self.lock();
        st.read_at
            .iter()
            .filter(|(seq, _)| {
                st.objects
                    .get(seq)
                    .is_some_and(|o| o.frames.iter().any(|f| f.range == range))
            })
            .map(|(_, at)| *at)
            .next()
    }

    fn is_done(&self) -> bool {
        self.done.load(std::sync::atomic::Ordering::Acquire)
    }

    async fn run(
        self: Arc<Self>,
        poll: Duration,
        bump: watch::Sender<u64>,
        nudge: watch::Receiver<u64>,
        owner: Weak<TailersInner>,
    ) {
        self.follow(poll, &bump, nudge, &owner).await;
        self.done.store(true, std::sync::atomic::Ordering::Release);
    }

    /// No reader holds this tailer any more (only the `NodeTailers` map and the `run` loop
    /// itself): remove it from the map — under the map lock, so nobody has just obtained
    /// it — and return `true` so the loop stops.
    fn retire_if_unfollowed(self: &Arc<Self>, owner: &Weak<TailersInner>) -> bool {
        let Some(owner) = owner.upgrade() else {
            return true;
        };
        let mut logs = owner.logs.lock().expect("mutex");
        if Arc::strong_count(self) > 2 {
            return false;
        }
        if logs.get(&self.log).is_some_and(|t| Arc::ptr_eq(t, self)) {
            logs.remove(&self.log);
        }
        true
    }

    async fn follow(
        self: &Arc<Self>,
        poll: Duration,
        bump: &watch::Sender<u64>,
        mut nudge: watch::Receiver<u64>,
        owner: &Weak<TailersInner>,
    ) {
        let backoff_max = poll * BACKOFF_POLLS;
        let floor_every = poll * FLOOR_EVERY_POLLS;
        let mut wait = poll;
        let mut floor_read: Option<Instant> = None;
        nudge.borrow_and_update();
        loop {
            let next = self.lock().next;
            match self.store.get(next).await {
                Ok(Some(Decoded::Data(d))) => {
                    let mut st = self.lock();
                    st.wm = st.wm.max(d.acked_upto);
                    if self.contiguous {
                        // Every `seq < next` has been read or lies below the floor: this
                        // object adjoins the exposed part.
                        st.wm = st.wm.max(next as i64);
                    }
                    for f in &d.frames {
                        st.by_range
                            .entry(f.range)
                            .or_default()
                            .insert(f.wal_file_id, next);
                    }
                    st.objects.insert(next, d);
                    st.read_at.insert(next, Instant::now());
                    if st.wm >= 0 {
                        let exposed = st.wm as u64;
                        st.read_at.retain(|seq, _| *seq > exposed);
                    }
                    st.next += 1;
                    drop(st);
                    wait = poll;
                    bump.send_modify(|v| *v += 1);
                    continue;
                }
                Ok(Some(Decoded::Seal { .. })) => {
                    self.lock().sealed = true;
                    bump.send_modify(|v| *v += 1);
                    // The log is sealed: nothing new is coming. Its unexposed part reaches
                    // the follower through the L0 the new owner flushes before serving.
                    return;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(error = %e, "shared WAL log tailer read error");
                }
            }
            if self.retire_if_unfollowed(owner) {
                tracing::debug!(log = %self.log, "stopping the shared WAL log tailer, no reader follows it any more");
                return;
            }
            if floor_read.is_none_or(|t| t.elapsed() >= floor_every) {
                floor_read = Some(Instant::now());
                if let Ok(floor) = self.store.floor().await {
                    // No floor, and the last object we read is gone too: the log has been
                    // purged (`LogStore::forget` moves data and floors away, the seal stays
                    // at a spot we have not reached) — ordinary GC always writes the floor
                    // before deleting. A lagging tailer will never see the seal: treat it
                    // as sealed so readers refresh their log list, then stop.
                    let next = self.lock().next;
                    if floor == 0 && next > 0 && matches!(self.store.get(next - 1).await, Ok(None))
                    {
                        self.lock().sealed = true;
                        bump.send_modify(|v| *v += 1);
                        tracing::debug!(log = %self.log, "stopping tailer: the shared WAL log has been purged");
                        return;
                    }
                    let mut st = self.lock();
                    if floor > st.next {
                        st.next = floor;
                    }
                    let below: Vec<u64> = st.objects.range(..floor).map(|(k, _)| *k).collect();
                    for seq in below {
                        st.read_at.remove(&seq);
                        if let Some(o) = st.objects.remove(&seq) {
                            for f in o.frames {
                                let d = st.dropped.entry(f.range).or_insert(0);
                                *d = (*d).max(f.wal_file_id);
                                if let Some(ids) = st.by_range.get_mut(&f.range) {
                                    ids.remove(&f.wal_file_id);
                                    if ids.is_empty() {
                                        st.by_range.remove(&f.range);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Miss: wait and try again, doubling the backoff up to the cap. A follower
            // reader nudging ⇒ try at once and return to the poll tick.
            tokio::select! {
                _ = tokio::time::sleep(wait) => wait = (wait * 2).min(backoff_max),
                _ = nudge.changed() => wait = poll,
            }
        }
    }
}

struct TailersInner {
    wal_store: Arc<dyn ObjectStore>,
    /// Declared capability of the WAL tier: expose contiguously, or keep the C5 gate.
    atomic_put: bool,
    wal_prefix: Path,
    owner_store: Arc<dyn ObjectStore>,
    owner_prefix: Path,
    poll: Duration,
    logs: Mutex<HashMap<LogId, Arc<LogTailer>>>,
    /// Bumped whenever a tailer advances — every iterator waits on this.
    bump: watch::Sender<u64>,
    /// Bumped when someone reads from the follower: every tailer stops backing off.
    nudge: watch::Sender<u64>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Drop for TailersInner {
    fn drop(&mut self) {
        for t in self.tasks.lock().expect("mutex").drain(..) {
            t.abort();
        }
    }
}

/// The tailers of **one** follower node, shared by all its readers.
#[derive(Clone)]
pub struct NodeTailers(Arc<TailersInner>);

impl NodeTailers {
    pub fn new(stores: &ObjectStores, poll: Duration) -> Self {
        NodeTailers(Arc::new(TailersInner {
            wal_store: Arc::clone(stores.wal().store()),
            atomic_put: stores.wal().atomic_put(),
            wal_prefix: stores.main().prefix().clone(),
            owner_store: Arc::clone(stores.main().store()),
            owner_prefix: stores.main().prefix().clone(),
            poll,
            logs: Mutex::new(HashMap::default()),
            bump: watch::Sender::new(0),
            nudge: watch::Sender::new(0),
            tasks: Mutex::new(Vec::new()),
        }))
    }

    /// Someone reads from the follower: every backing-off tailer retries at once and
    /// returns to the poll tick — the next read sees new writes within one poll tick, not
    /// within the backoff cap.
    pub fn nudge(&self) {
        self.0.nudge.send_modify(|v| *v += 1);
    }

    fn tailer(&self, log: &LogId) -> Arc<LogTailer> {
        let mut logs = self.0.logs.lock().expect("mutex");
        // A stopped tailer only the map still holds: prune it.
        logs.retain(|_, t| !(t.is_done() && Arc::strong_count(t) == 1));
        if let Some(t) = logs.get(log) {
            return Arc::clone(t);
        }
        let t = Arc::new(LogTailer {
            log: log.clone(),
            contiguous: self.0.atomic_put,
            done: std::sync::atomic::AtomicBool::new(false),
            store: LogStore::new(Arc::clone(&self.0.wal_store), &self.0.wal_prefix, log),
            state: Mutex::new(TailState {
                next: 0,
                wm: -1,
                objects: BTreeMap::new(),
                by_range: HashMap::default(),
                read_at: BTreeMap::new(),
                dropped: HashMap::default(),
                sealed: false,
            }),
        });
        let task = tokio::spawn(crate::io_source::tagged(
            "nodewal.tailer",
            Arc::clone(&t).run(
                self.0.poll,
                self.0.bump.clone(),
                self.0.nudge.subscribe(),
                Arc::downgrade(&self.0),
            ),
        ));
        let mut tasks = self.0.tasks.lock().expect("mutex");
        tasks.retain(|h| !h.is_finished());
        tasks.push(task.abort_handle());
        logs.insert(log.clone(), Arc::clone(&t));
        t
    }

    /// Number of logs being followed — one GET chain each, however many ranges. Stopped
    /// tailers (seal, log gone, nobody following) do not count.
    pub fn followed_logs(&self) -> usize {
        self.0
            .logs
            .lock()
            .expect("mutex")
            .values()
            .filter(|t| !t.is_done())
            .count()
    }

    /// `WalReader` for range `range`, following the logs of the current `wal_segment`.
    /// `None` when the range does not use the shared WAL.
    pub async fn reader(&self, range: u32) -> Result<Option<SharedWalReader>> {
        let Some(rec) = owner::read_owner(
            &self.0.owner_store,
            &self.0.owner_prefix,
            PartitionId(range),
        )
        .await?
        else {
            return Ok(None);
        };
        if rec.wal_kind != WalKind::Shared {
            return Ok(None);
        }
        let reader = SharedWalReader {
            tailers: self.clone(),
            range,
            logs: Arc::new(Mutex::new(Vec::new())),
            refreshed: Arc::new(Mutex::new(Instant::now())),
        };
        reader.set_logs(rec.wal_segments.iter().map(|s| s.log.clone()).collect());
        Ok(Some(reader))
    }
}

/// `WalReader` of a range that uses the shared WAL, reading through the node's tailers.
#[derive(Clone)]
pub struct SharedWalReader {
    tailers: NodeTailers,
    range: u32,
    logs: Arc<Mutex<Vec<Arc<LogTailer>>>>,
    refreshed: Arc<Mutex<Instant>>,
}

impl SharedWalReader {
    fn set_logs(&self, logs: Vec<LogId>) {
        let tailers: Vec<_> = logs.iter().map(|l| self.tailers.tailer(l)).collect();
        *self.logs.lock().expect("mutex") = tailers;
    }

    fn logs(&self) -> Vec<Arc<LogTailer>> {
        self.logs.lock().expect("mutex").clone()
    }

    /// When the range's owner changes the logs change: re-read `wal_segment` when every
    /// followed log has been sealed, or every `10 × poll` to be safe.
    async fn maybe_refresh(&self) {
        let all_sealed = self.logs().iter().all(|t| t.lock().sealed);
        let due = self.refreshed.lock().expect("mutex").elapsed() >= self.tailers.0.poll * 10;
        if !(all_sealed || due) {
            return;
        }
        *self.refreshed.lock().expect("mutex") = Instant::now();
        let inner = &self.tailers.0;
        if let Ok(Some(rec)) = owner::read_owner(
            &inner.owner_store,
            &inner.owner_prefix,
            PartitionId(self.range),
        )
        .await
        {
            self.set_logs(rec.wal_segments.iter().map(|s| s.log.clone()).collect());
        }
    }

    /// Exposed frames of the range with wal id ≥ `from`, merged over every log, by wal id
    /// — at most `limit` per log (the first frame ≥ `from` is always present).
    fn frames_from(&self, from: u64, limit: usize) -> Vec<Frame> {
        let mut frames: Vec<Frame> = self
            .logs()
            .iter()
            .flat_map(|t| t.exposed_from(self.range, from, limit))
            .collect();
        frames.sort_by_key(|f| f.wal_file_id);
        frames.dedup_by_key(|f| f.wal_file_id);
        frames
    }

    fn dropped(&self) -> Option<u64> {
        self.logs()
            .iter()
            .filter_map(|t| t.dropped(self.range))
            .max()
    }

    /// The **contiguous** frames of the range from `want` (ids `want`, `want + 1`, …)
    /// before the bound `end`, and the first id the reader needs that is missing —
    /// `Some(k)` when GC deleted frames ≥ k before the tailer emitted them (`dropped`), when
    /// the next frame has id > k (a gap), or when a frame before `end` has disappeared. On
    /// `Some(k)` the reader loads the manifest (`WalTruncated(k)`): the frames in the gap —
    /// if there ever were any — are already in the L0 of the manifest GC relied on.
    ///
    /// A real gap (an id allocated when the range was attached but carried by no frame,
    /// `register` picking a higher first id) also stops the reader until the stored
    /// manifest covers it: acquiring a range flushes L0 before serving (`flush_through`), so
    /// it only costs one manifest load — or one manifest poll tick if the first load was
    /// too early. Slow, not wrong: the reader's state is still a prefix.
    fn contiguous(
        &self,
        mut want: u64,
        end: Bound<u64>,
        limit: usize,
    ) -> (Vec<Frame>, Option<u64>) {
        let dropped = self.dropped();
        let before_end = |id: u64| match end {
            Bound::Included(e) => id <= e,
            Bound::Excluded(e) => id < e,
            Bound::Unbounded => true,
        };
        let mut run = Vec::new();
        for f in self.frames_from(want, limit) {
            if !before_end(f.wal_file_id) || run.len() >= limit {
                return (run, None);
            }
            if dropped.is_some_and(|d| d >= want) || f.wal_file_id > want {
                return (run, Some(want));
            }
            want = f.wal_file_id + 1;
            run.push(f);
        }
        // Frames ran out: a missing id before a bounded end (a frame seen when the bound
        // was computed is gone now) or one deleted by GC ⇒ a gap.
        let bounded_gap = !matches!(end, Bound::Unbounded) && before_end(want);
        if bounded_gap || dropped.is_some_and(|d| d >= want) {
            return (run, Some(want));
        }
        (run, None)
    }

    /// Since when the range has had a frame in an object the tailer **has read but not
    /// exposed** (no `acked_upto` header covers it yet — C5); `None` = none. Always `None`
    /// on an `atomic_put` store: an object that can be read is exposed at once, and the
    /// defer-to-leader rule is only a safety net for non-atomic stores.
    ///
    /// Under load, the tail is exposed after one cut window + one PUT + one poll tick. A
    /// tail staying longer means the owner's log has gone **quiet** after its last object:
    /// no object will come to cover it, and the reader only sees that part once the owner
    /// forces a flush by frame age (T, 60 s by default) and the reader reloads the manifest.
    /// The follower reader uses it to know its copy **may be stale** (I4).
    pub fn unexposed_since(&self) -> Option<Instant> {
        self.logs()
            .iter()
            .filter_map(|t| t.unexposed_since(self.range))
            .min()
    }
}

/// Bounded iterator: the contiguous frames, then, if it stopped at a gap, `WalTruncated`
/// (the reader treats the WAL as ending there — SlateDB keeps the rows replayed before the
/// error).
struct VecIter {
    rows: VecDeque<WalRows>,
    truncated: Option<u64>,
}

#[async_trait]
impl WalIterator for VecIter {
    async fn next(&mut self) -> std::result::Result<Option<WalRows>, WalError> {
        if let Some(rows) = self.rows.pop_front() {
            return Ok(Some(rows));
        }
        match self.truncated {
            Some(k) => Err(WalError::WalTruncated(k)),
            None => Ok(None),
        }
    }
}

/// Iterator without an end: returns the range's next frame once it is exposed.
struct StreamIter {
    reader: SharedWalReader,
    next: u64,
}

#[async_trait]
impl WalIterator for StreamIter {
    async fn next(&mut self) -> std::result::Result<Option<WalRows>, WalError> {
        let mut changes = self.reader.tailers.0.bump.subscribe();
        loop {
            changes.borrow_and_update();
            let (run, gap) = self.reader.contiguous(self.next, Bound::Unbounded, 1);
            if let Some(f) = run.into_iter().next() {
                self.next = f.wal_file_id + 1;
                return Ok(Some(WalRows {
                    rows: f.rows,
                    last_consumed_wal_file_id: f.wal_file_id,
                }));
            }
            if let Some(k) = gap {
                return Err(WalError::WalTruncated(k));
            }
            self.reader.maybe_refresh().await;
            let poll = self.reader.tailers.0.poll;
            let _ = tokio::time::timeout(poll * 10, changes.changed()).await;
        }
    }
}

#[async_trait]
impl WalReader for SharedWalReader {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> std::result::Result<Box<dyn WalIterator>, WalError> {
        let WalFileRange(start, end) = wal_file_id_range;
        let start = match start {
            Bound::Included(s) => s,
            Bound::Excluded(s) => s + 1,
            Bound::Unbounded => 0,
        };
        if matches!(end, Bound::Unbounded) {
            return Ok(Box::new(StreamIter {
                reader: self.clone(),
                next: start,
            }));
        }
        let (run, truncated) = self.contiguous(start, end, usize::MAX);
        let rows = run
            .into_iter()
            .map(|f| WalRows {
                rows: f.rows,
                last_consumed_wal_file_id: f.wal_file_id,
            })
            .collect();
        Ok(Box::new(VecIter { rows, truncated }))
    }

    async fn last_wal_file_id(
        &self,
        replay_after_wal_id: u64,
    ) -> std::result::Result<u64, WalError> {
        Ok(self
            .logs()
            .iter()
            .filter_map(|t| t.last_exposed(self.range))
            .max()
            .unwrap_or(replay_after_wal_id)
            .max(replay_after_wal_id))
    }

    fn wal_name(&self) -> Option<&str> {
        Some(SHARED_WAL_NAME)
    }
}

#[cfg(test)]
mod tests {
    //! Follower reader on a real tailer, in-memory store, hand-built objects.

    use {
        super::*,
        crate::{nodewal::format, owner::SessionId},
        bytes::Bytes,
        slatedb::{object_store::memory::InMemory, RowEntry, ValueDeletable},
    };

    const POLL: Duration = Duration::from_millis(5);

    fn frame(range: u32, wal_file_id: u64) -> Frame {
        Frame {
            range,
            epoch: 1,
            wal_file_id,
            rows: vec![RowEntry {
                key: Bytes::from(format!("k{range}-{wal_file_id}")),
                value: ValueDeletable::Value(Bytes::from_static(b"v")),
                seq: wal_file_id,
                create_ts: None,
                expire_ts: None,
            }],
        }
    }

    fn tailers(store: Arc<dyn ObjectStore>) -> NodeTailers {
        NodeTailers(Arc::new(TailersInner {
            wal_store: Arc::clone(&store),
            atomic_put: true,
            wal_prefix: Path::from("cairn"),
            owner_store: store,
            owner_prefix: Path::from("cairn"),
            poll: POLL,
            logs: Mutex::new(HashMap::default()),
            bump: watch::Sender::new(0),
            nudge: watch::Sender::new(0),
            tasks: Mutex::new(Vec::new()),
        }))
    }

    fn reader(tailers: &NodeTailers, range: u32, log: &LogId) -> SharedWalReader {
        let r = SharedWalReader {
            tailers: tailers.clone(),
            range,
            logs: Arc::new(Mutex::new(Vec::new())),
            refreshed: Arc::new(Mutex::new(Instant::now())),
        };
        r.set_logs(vec![log.clone()]);
        r
    }

    async fn next_of(
        it: &mut Box<dyn WalIterator>,
    ) -> std::result::Result<Option<WalRows>, WalError> {
        tokio::time::timeout(Duration::from_secs(5), it.next())
            .await
            .expect("the follower iterator returned nothing within 5 s")
    }

    /// **A tailer jumping over the GC floor past unread objects: the reader must not apply
    /// frames after the gap.**
    ///
    /// 1. The log has objects 0, 1 carrying frames 1, 2 of range 7. Range 7's reader reads
    ///    both (`next = 3`); the tailer reads on and misses at object 2.
    /// 2. While the tailer is "slow", the owner writes objects 2…9 (range 7: ids 3…10;
    ///    object 6 also carries id 1 of range 8), flushes L0, writes the manifest and GC
    ///    deletes 2…5, floor 6 — here: objects 2…5 never exist on the store, floor 6 does.
    /// 3. The tailer reads the floor, jumps `next` 2 → 6 (objects 2…5 were never read so
    ///    they do not go into `dropped`), reads 6…9.
    ///
    /// Must see: range 7's reader at `next = 3` gets `WalTruncated(3)` (loads the manifest)
    /// — it does not get frame 7 and push the mark over the gap 3…6 (a state that is not a
    /// prefix). The bounded iterator (the manifest load path) also stops at the gap. No
    /// false positive: range 8 (the first frame after the gap is exactly the frame it needs)
    /// and a reader of range 7 starting at 7 get frames normally.
    #[tokio::test]
    async fn reader_does_not_apply_frames_past_objects_the_tailer_skipped() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = LogId {
            session: SessionId {
                node: "a".into(),
                inc: 1,
            },
            stripe: 0,
        };
        let ls = LogStore::new(Arc::clone(&store), &Path::from("cairn"), &log);
        for (seq, id) in [(0u64, 1u64), (1, 2)] {
            ls.create(seq, format::encode_data(seq as i64, &[frame(7, id)]))
                .await
                .unwrap();
        }
        let tailers = tailers(Arc::clone(&store));
        let r7 = reader(&tailers, 7, &log);
        let mut it = r7.iterator((1..).into()).await.unwrap();
        for want in [1, 2] {
            let rows = next_of(&mut it).await.unwrap().expect("frame");
            assert_eq!(rows.last_consumed_wal_file_id, want);
        }

        ls.write_floor(6).await.unwrap();
        for seq in 6u64..10 {
            let mut frames = vec![frame(7, seq + 1)];
            if seq == 6 {
                frames.push(frame(8, 1));
            }
            ls.create(seq, format::encode_data(seq as i64, &frames))
                .await
                .unwrap();
        }

        match next_of(&mut it).await {
            Err(WalError::WalTruncated(3)) => {}
            other => panic!(
                "range 7 reader at 3 must get WalTruncated(3), got {:?}",
                other.map(|r| r.map(|r| r.last_consumed_wal_file_id))
            ),
        }

        // The bounded path (`replay_wal_into` after loading a manifest older than the GC).
        let mut bounded = r7
            .iterator(WalFileRange(Bound::Included(3), Bound::Included(10)))
            .await
            .unwrap();
        assert!(
            matches!(next_of(&mut bounded).await, Err(WalError::WalTruncated(3))),
            "the bounded iterator must stop at the gap"
        );

        // No false positive.
        let r8 = reader(&tailers, 8, &log);
        let mut it8 = r8.iterator((1..).into()).await.unwrap();
        assert_eq!(
            next_of(&mut it8)
                .await
                .unwrap()
                .expect("frame")
                .last_consumed_wal_file_id,
            1
        );
        let mut it7 = r7.iterator((7..).into()).await.unwrap();
        for want in 7..=10 {
            assert_eq!(
                next_of(&mut it7)
                    .await
                    .unwrap()
                    .expect("frame")
                    .last_consumed_wal_file_id,
                want
            );
        }
    }
}
