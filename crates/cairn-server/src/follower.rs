// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Follower reads.
//!
//! A node that is not the leader can still answer `/invocations/{id}` and
//! `/journal`, through a `DbReader` pointed at the same bucket. There is no
//! replication protocol here: the WAL on the object store already is the redo
//! channel, and SlateDB replays it itself.
//!
//! ## Three principles of this read path
//!
//! 1. **Serve only positive results.** The reader is always slightly stale
//!    (floor = the poll period), so its "not found" is no proof of absence —
//!    an invocation just created at the leader has not arrived yet. Not found
//!    ⇒ return `None` and let ingress redirect to the leader, **never** answer
//!    404 from stale data.
//! 2. **Refuse to serve when wedged.** [`StallDetector`] watches for the stall
//!    signature (`durable < manifest` lasting); once it fires every read
//!    returns `None` until the reader catches up again. Better a 503 than a
//!    wrong answer.
//! 3. **Readers open lazily, on demand — and close themselves when
//!    abandoned.** Each reader costs ~10 requests per poll period against the
//!    object store; multiplied by the number of partitions that is a real
//!    number. Only partitions someone asks about pay that price, and a
//!    partition nobody has asked about for [`EVICT_AFTER`] has its reader
//!    closed — without eviction the reader set only grows, and a long-lived
//!    node pays polling for every partition *ever* asked about once. The next
//!    query reopens it from scratch: one tick slower, not wrong. When the node
//!    **becomes leader** of a partition its reader is closed at once too:
//!    reads go through the engine, and the owner record now points at the
//!    node's own log — the tailer would GET its own log every poll tick until
//!    evicted for idleness.

use {
    cairn_engine::{InvocationStatus, JournalRow},
    cairn_store::{
        Freshness, NodeTailers, ObjectStores, PartitionId, PartitionReader, ReaderHealth,
        StallDetector,
    },
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex, RwLock},
        time::Duration,
    },
    tokio::sync::watch,
};

/// State of an open reader, for `/cluster` to inspect.
#[derive(Clone, Copy, Debug)]
pub struct ReaderStatus {
    pub freshness: Freshness,
    pub healthy: bool,
}

/// Why a reader was opened: the **first** query for a range without a reader.
/// The gauge `cairnflow_follower_readers_open{reason}` counts open readers by
/// that reason. Stream reads, `$log`/`$trace` and queries have no reason here
/// — they only read at the leader and never open a reader (AP-13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// `GET /invocations/{id}` (status).
    Status = 0,
    /// `GET /invocations/{id}/journal`, or the first entry to learn the tenant.
    Journal = 1,
}

impl Reason {
    fn gauge(self) -> &'static std::sync::atomic::AtomicU64 {
        &crate::metrics::agent::FOLLOWER_READERS_OPEN[self as usize]
    }
}

/// A reader nobody has asked about for this long is closed.
const EVICT_AFTER: Duration = Duration::from_secs(5 * 60);

/// A read waits at most this long for a reader open in flight, then returns
/// `None` (redirect to the leader); the open keeps running in the background.
const OPEN_WAIT: Duration = Duration::from_secs(2);

/// When a range's unexposed tail (shared-WAL objects the tailer has read but
/// no header covers yet) has sat for more than this many poll ticks — at least
/// [`STALE_TAIL_MIN`] — the owner's log is taken to be quiet: the
/// **non-final** state the reader sees may be stale until the frame-age forced
/// flush (I4), so defer to the leader. Under load the tail is exposed after
/// one cut window + one PUT + one poll tick.
const STALE_TAIL_POLLS: u32 = 4;
const STALE_TAIL_MIN: Duration = Duration::from_secs(1);

/// A background reader open gives up after this long: `DbReader` retries
/// store errors without bound, and an open that hangs forever would mean the
/// range is never opened again.
const OPEN_BOUND: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct Slot {
    reader: PartitionReader,
    /// Updated by the sampling task, read by every request. A plain `RwLock`
    /// (not tokio's) because both sides only touch it for a very short moment.
    status: Arc<RwLock<ReaderStatus>>,
    /// Last time someone asked — the sampling task uses it to decide to close the reader.
    last_used: Arc<RwLock<tokio::time::Instant>>,
}

/// A cell of the reader map: an opened reader, or an open in flight in the background.
enum Entry {
    Open(Slot),
    /// Set to `true` when the open finishes (successfully or not).
    Opening(watch::Receiver<bool>),
}

/// The read path for partitions this node does **not** lead.
#[derive(Clone)]
pub struct FollowerReads {
    stores: ObjectStores,
    poll: Duration,
    /// The node's shared-WAL log tailer: a range on the shared WAL reads its
    /// WAL by **log**, one tailer per log for every range on it; a range on its
    /// own WAL reads as before.
    tailers: NodeTailers,
    /// A lock shared by the whole node — `/metrics`, `/cluster` and every
    /// follower read go through it — so it is **never held across an
    /// `await`**: the reader open runs in the background, with the cell marked
    /// `Opening` meanwhile.
    slots: Arc<Mutex<BTreeMap<u32, Entry>>>,
    /// This node leads the partition (per the routing table) — its reader is
    /// closed at the next sampling tick. `None` = unknown, do not close.
    leads: Option<Arc<dyn Fn(PartitionId) -> bool + Send + Sync>>,
}

impl FollowerReads {
    pub fn new(stores: ObjectStores, poll: Duration) -> Self {
        Self {
            tailers: NodeTailers::new(&stores, poll),
            stores,
            poll,
            slots: Arc::new(Mutex::new(BTreeMap::new())),
            leads: None,
        }
    }

    /// Closes the readers of partitions that `leads` says this node leads.
    pub fn with_leader_check(
        mut self,
        leads: impl Fn(PartitionId) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.leads = Some(Arc::new(leads));
        self
    }

    /// Invocation status, read from the reader. `None` = cannot be served from
    /// here (reader not open yet, reader wedged, id not seen yet, or a
    /// **non-final** status while the range has a stale unexposed tail —
    /// [`Self::tail_stale`]) — the caller redirects to the leader.
    ///
    /// Outcomes (`completed`, `failed`) cannot go stale any more: serve them.
    pub async fn status(&self, p: PartitionId, id: &[u8; 24]) -> Option<InvocationStatus> {
        let slot = self.slot(p, Reason::Status).await?;
        let raw = slot.reader.invocation_status(id).await.ok()??;
        let status = InvocationStatus::decode(raw)?;
        let done = matches!(
            status,
            InvocationStatus::Completed(_) | InvocationStatus::Failed { .. }
        );
        (done || !self.tail_stale(&slot)).then_some(status)
    }

    /// Like [`Self::status`], with timestamps: the status row's `create_ts`
    /// and the metadata row's `created_ms` — the same marks the leader reads
    /// (G20). The metadata costs one more GET, only when the status can be
    /// served.
    pub async fn status_times(
        &self,
        p: PartitionId,
        id: &[u8; 24],
    ) -> Option<(InvocationStatus, cairn_engine::Times)> {
        let slot = self.slot(p, Reason::Status).await?;
        let (raw, modified) = slot
            .reader
            .get_meta(&cairn_store::keys::invocation_status(id))
            .await
            .ok()??;
        let status = InvocationStatus::decode(raw)?;
        let done = matches!(
            status,
            InvocationStatus::Completed(_) | InvocationStatus::Failed { .. }
        );
        if !done && self.tail_stale(&slot) {
            return None;
        }
        // `createdAt` from the metadata row, as at the leader; a missing row
        // (impossible with format 10) ⇒ the `create_ts` of entry 0.
        let created = match slot
            .reader
            .get_meta(&cairn_store::query_index::meta_key(id))
            .await
            .ok()
            .flatten()
            .and_then(|(raw, _)| cairn_store::query_index::InvocationMeta::decode(&raw))
        {
            Some(m) => Some(m.created_ms),
            None => cairn_engine::Times::stamp(
                slot.reader
                    .get_meta(&cairn_store::keys::journal_entry(id, 0))
                    .await
                    .ok()
                    .flatten()
                    .map(|(_, ts)| ts),
            ),
        };
        Some((
            status,
            cairn_engine::Times {
                created_ms: created,
                modified_ms: cairn_engine::Times::stamp(Some(modified)),
            },
        ))
    }

    /// The range's unexposed tail has sat past the limit (I4): the owner's log
    /// went quiet after its last object, so that part only reaches the reader
    /// at the frame-age forced flush (60 s by default) plus one manifest load
    /// tick — the follower's copy may be that stale.
    fn tail_stale(&self, slot: &Slot) -> bool {
        let limit = (self.poll * STALE_TAIL_POLLS).max(STALE_TAIL_MIN);
        slot.reader
            .unexposed_tail_age()
            .is_some_and(|age| age >= limit)
    }

    /// The journal of an invocation. `None` when it cannot be served **or**
    /// when the journal is empty — empty on a slightly stale reader cannot be
    /// told apart from "not arrived yet", so the leader answers instead.
    ///
    /// As with [`Self::status`], a range with a stale unexposed tail ⇒ `None`:
    /// the follower's journal may then be missing its tail (I4).
    /// [`Self::journal_head`] is for callers that only need the first entry
    /// (unchanged after creation).
    pub async fn journal(&self, p: PartitionId, id: &[u8; 24]) -> Option<Vec<JournalRow>> {
        let slot = self.slot(p, Reason::Journal).await?;
        if self.tail_stale(&slot) {
            return None;
        }
        Self::rows(&slot, id).await
    }

    /// One journal page — like [`Self::journal`]: `None` when it cannot be
    /// served, the unexposed tail is stale, or the page is empty (defer to the
    /// leader).
    pub async fn journal_page(
        &self,
        p: PartitionId,
        id: &[u8; 24],
        from: u32,
        max_entries: usize,
        max_bytes: usize,
    ) -> Option<(Vec<JournalRow>, Option<u32>)> {
        let slot = self.slot(p, Reason::Journal).await?;
        if self.tail_stale(&slot) {
            return None;
        }
        let (rows, next) = slot
            .reader
            .journal_page(id, from, max_entries, max_bytes)
            .await
            .ok()?;
        if rows.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(rows.len());
        for (index, raw) in rows {
            let entry = cairn_runtime::JournalEntry::decode(raw).ok()?;
            out.push((index, entry.ty.raw(), entry.payload.to_vec()));
        }
        Some((out, next))
    }

    /// The journal for reading the **first entry** (tenant, target — unchanged
    /// after the creating commit): an unexposed tail does not make it stale,
    /// so do not defer to the leader because of the tail.
    pub async fn journal_head(&self, p: PartitionId, id: &[u8; 24]) -> Option<Vec<JournalRow>> {
        let slot = self.slot(p, Reason::Journal).await?;
        Self::rows(&slot, id).await
    }

    async fn rows(slot: &Slot, id: &[u8; 24]) -> Option<Vec<JournalRow>> {
        let rows = slot.reader.journal(id).await.ok()?;
        if rows.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(rows.len());
        for (index, raw) in rows {
            let entry = cairn_runtime::JournalEntry::decode(raw).ok()?;
            out.push((index, entry.ty.raw(), entry.payload.to_vec()));
        }
        Some(out)
    }

    /// State of the open readers — for `/cluster`.
    pub async fn statuses(&self) -> BTreeMap<u32, ReaderStatus> {
        let slots = self.slots.lock().expect("slots lock");
        slots
            .iter()
            .filter_map(|(p, e)| match e {
                Entry::Open(s) => Some((*p, *s.status.read().expect("status lock"))),
                Entry::Opening(_) => None,
            })
            .collect()
    }

    /// The slot of a partition, opened if missing. `None` when it cannot be
    /// served yet — the DB was never created by any leader, the store is
    /// misbehaving, or the open did not finish within [`OPEN_WAIT`] — and
    /// **failures are not cached**: the next query tries again.
    async fn slot(&self, p: PartitionId, reason: Reason) -> Option<Slot> {
        // Someone is reading: a backing-off tailer reads again at once.
        self.tailers.nudge();
        let mut opened = {
            let mut slots = self.slots.lock().expect("slots lock");
            match slots.get(&p.0) {
                Some(Entry::Open(s)) => return Self::serve(s),
                Some(Entry::Opening(rx)) => rx.clone(),
                None => {
                    let (tx, rx) = watch::channel(false);
                    slots.insert(p.0, Entry::Opening(rx.clone()));
                    self.open_in_background(p, reason, tx);
                    rx
                }
            }
        };
        let _ = tokio::time::timeout(OPEN_WAIT, opened.wait_for(|done| *done)).await;
        let slots = self.slots.lock().expect("slots lock");
        match slots.get(&p.0) {
            Some(Entry::Open(s)) => Self::serve(s),
            _ => None,
        }
    }

    /// Records the query; serves when the reader is healthy.
    fn serve(s: &Slot) -> Option<Slot> {
        *s.last_used.write().expect("last_used lock") = tokio::time::Instant::now();
        s.status
            .read()
            .expect("status lock")
            .healthy
            .then(|| s.clone())
    }

    /// Opens `p`'s reader in the background, bounded by [`OPEN_BOUND`]; when
    /// done (successfully or not) signals `done`. Failure ⇒ remove the
    /// `Opening` cell, the next query opens again.
    fn open_in_background(&self, p: PartitionId, reason: Reason, done: watch::Sender<bool>) {
        let this = self.clone();
        tokio::spawn(cairn_store::io_source::tagged(
            "follower.open",
            async move {
                let opened = tokio::time::timeout(
                    OPEN_BOUND,
                    this.stores
                        .open_reader_with(p.0, this.poll, Some(&this.tailers)),
                )
                .await;
                match opened {
                    Ok(Ok(reader)) => this.install(p, reason, reader),
                    Ok(Err(e)) => {
                        tracing::debug!(partition = p.0, error = %e, "follower reader not opened yet");
                        this.slots.lock().expect("slots lock").remove(&p.0);
                    }
                    Err(_) => {
                        tracing::warn!(
                            partition = p.0,
                            limit_s = OPEN_BOUND.as_secs(),
                            "opening follower reader over its limit — giving up, the next query reopens"
                        );
                        this.slots.lock().expect("slots lock").remove(&p.0);
                    }
                }
                let _ = done.send(true);
            },
        ));
    }

    /// Puts the reader just opened into the map and runs its sampling task.
    fn install(&self, p: PartitionId, reason: Reason, reader: PartitionReader) {
        let status = Arc::new(RwLock::new(ReaderStatus {
            freshness: reader.freshness(),
            healthy: true,
        }));
        let last_used = Arc::new(RwLock::new(tokio::time::Instant::now()));
        let slot = Slot {
            reader: reader.clone(),
            status: Arc::clone(&status),
            last_used: Arc::clone(&last_used),
        };
        self.slots
            .lock()
            .expect("slots lock")
            .insert(p.0, Entry::Open(slot));
        crate::metrics::agent::bump(reason.gauge());

        // The sampling task, which also does eviction. Its interval is exactly
        // the reader's poll period: faster only takes duplicate samples,
        // slower stretches the GRACE samples too far.
        let poll = self.poll;
        let all_slots = Arc::clone(&self.slots);
        let leads = self.leads.clone();
        tokio::spawn(cairn_store::io_source::tagged(
            "follower.sample",
            async move {
                let mut detector = StallDetector::default();
                loop {
                    cairn_store::pace::poll_gap(poll).await;

                    // Nobody asks any more ⇒ close: remove it from the map and
                    // end the task — the reader (and its poll loop) dies with
                    // the last drop. Requests holding a clone of the slot still
                    // finish normally.
                    let idle = last_used.read().expect("last_used lock").elapsed();
                    let leading = leads.as_ref().is_some_and(|l| l(p));
                    if idle > EVICT_AFTER || leading {
                        let mut slots = all_slots.lock().expect("slots lock");
                        if matches!(slots.get(&p.0), Some(Entry::Open(s)) if Arc::ptr_eq(&s.status, &status))
                        {
                            slots.remove(&p.0);
                        }
                        drop(slots);
                        crate::metrics::agent::sub(reason.gauge(), 1);
                        tracing::info!(
                            partition = p.0,
                            idle_s = idle.as_secs(),
                            leading,
                            "closing follower reader (nobody asks, or the node now leads the range)"
                        );
                        return;
                    }

                    let f = reader.freshness();
                    let health = detector.observe(f);
                    let healthy = matches!(health, ReaderHealth::Healthy);
                    if !healthy {
                        tracing::warn!(
                            partition = p.0,
                            durable = f.durable_seq,
                            manifest = f.manifest_seq,
                            "follower reader wedged — no longer serving reads from this partition"
                        );
                    }
                    *status.write().expect("status lock") = ReaderStatus {
                        freshness: f,
                        healthy,
                    };
                }
            },
        ));
    }
}
