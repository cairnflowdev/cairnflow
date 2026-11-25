// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The single writer of a partition — INV-4, enforced by the type system.
//!
//! The type system blocks **programming errors**: cloning the writer, handing it to a
//! background task, writing after leadership was given up. SlateDB's `writer_epoch` blocks
//! **timing errors**: an old leader GC-pauses, then wakes up and keeps writing. Both are
//! needed; neither layer can replace the other.
//!
//! An engine whose store is never shared across nodes can get away with discipline. We do
//! not have that privilege: the store is one single copy on object storage, one mistake
//! corrupts the only copy of the state, with no replica as a buffer.

// The runtime's clock: `std::time::Instant` outside simulation, a virtual clock under
// deterministic simulation.
use tokio::time::Instant;
use {
    crate::{
        error::{Result, StoreError},
        keyspace::{self, Index, InvocationId, QueueName, Stage, Table},
        partition_table::PartitionKey,
        scope::{Scope, TenantId},
    },
    slatedb::Db,
    std::{
        marker::PhantomData,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, OnceLock,
        },
        time::Duration,
    },
};

mod validity;
pub use validity::*;
mod mutations;
pub use mutations::*;
mod scan;
pub use scan::*;
mod floors;
mod read;
pub use floors::Floors;
mod store;
#[cfg(any(test, feature = "testkit"))]
pub use store::ModelStatus;
pub use store::{Batch, Flush, RangeIter, RangeStore, Row, ScanHow, SlateIter, StoreStatus};

/// The production store of a range Db — the default of [`PartitionWriter`] and
/// `cairn_engine::Engine`.
pub type SlateDb = Db;

pub use crate::ids::PartitionId;

/// Proof that **everything its holder may have seen is durable**.
///
/// Only [`PartitionWriter::durability`] (the `Ready` arm) and [`Pending::durable`] can
/// create this value. There is no path to a `Committed` while a write is not yet durable —
/// that is how INV-5 is enforced at the type level, instead of by a "remember to await
/// before acking" comment.
///
/// **Why it is a parameter of functions that do not read it.** `Shared::finish` and the
/// SDK wait gate take a `&Committed` (and `Transport::send` takes a narrower ticket derived
/// from it, [`SendTicket`]) without using the value inside. That is the point: they are the
/// three kinds of **outward effect** cairn has, and INV-5 says no effect may precede
/// durability. Making them demand the proof lets the **compiler** keep the list of call
/// sites instead of the author's memory.
///
/// **A ticket is a mark, not an item.** The earlier version kept the latest WAL handle in a
/// slot and *took it away* when waiting; a task arriving later found the slot empty and got
/// a ticket at once — while the data it had just read (SlateDB reads at the `Memory` level
/// by default) was not yet durable (`cairn-server/tests/durability.rs`). Now a ticket is
/// only issued once the store's `durable_seq` has passed the seq of the latest write **at
/// the time of asking** — that is, everything the asker may have read. Nobody takes away
/// anybody else's share.
#[derive(Debug, Clone, Copy)]
pub struct Committed(());

/// A **send** ticket (LF-1/LF-2): every **outgoing** row (outbox, signal outbox) its holder
/// may have seen is durable — narrower than [`Committed`], it says nothing about other
/// writes.
///
/// Only [`PartitionWriter::send_durability`] (the `Ready` arm), [`Pending::durable`]
/// (through `From<&Committed>`) and `From<&Committed>` itself create it. Only
/// `Transport::send`/`send_batch` take it: a cross-range message only carries the bytes of
/// the outgoing row (plus the routing table), so a ticket covering that row is enough
/// (IR-1). Replies to clients and sends down to the SDK still demand [`Committed`] — the
/// compiler keeps that boundary:
///
/// ```compile_fail
/// # use cairn_store::{Committed, SendTicket};
/// fn reply_to_client(_: &Committed) {}
/// fn narrow_ticket(t: &SendTicket) { reply_to_client(t) }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SendTicket(());

/// A full ticket is stronger than a send ticket.
impl From<&Committed> for SendTicket {
    fn from(_: &Committed) -> Self {
        SendTicket(())
    }
}

/// Result of [`PartitionWriter::send_durability`].
pub enum SendDurability {
    /// Every write with an outgoing row is durable.
    Ready(SendTicket),
    /// Wait for the latest write with an outgoing row to be durable — see [`Pending`].
    Wait(Pending),
    /// A write with an outgoing row was submitted but its handle was lost (an error or a
    /// cancellation after submission, LF-3): only a barrier submitted **after** it, once
    /// durable, can cover it — [`PartitionWriter::send_barrier`].
    Barrier,
}

/// Result of [`PartitionWriter::durability`].
pub enum Durability {
    /// Everything written is durable — a ticket issued on the spot, no waiting.
    Ready(Committed),
    /// Some write is not yet durable. Wait with [`Pending::durable`] — **after** releasing
    /// every lock others need to keep writing.
    Wait(Pending),
}

/// A mark to wait for, **detached from the writer**.
///
/// Detaching is the whole point: waiting for durability must happen once the engine is
/// released, and the engine cannot be released while someone is borrowing
/// `&PartitionWriter`. This value carries everything it needs, so it lives on its own —
/// the call site takes it out, releases the engine, then waits. Several waiters on the
/// same mark wake together at one flush: that is the group-commit part
/// (`tests/group_commit.rs`).
pub struct Pending {
    handle: slatedb::WriteHandle,
    status: StoreStatus,
    validity: Arc<Validity>,
}

impl Pending {
    /// Waits for durability. This is the only path that produces a [`Committed`] for a
    /// write still in flight.
    pub async fn durable(self) -> Result<Committed> {
        crate::failpoint!("writer::commit::before_durable", err);
        crate::probe::mark("durable.begin", 0);
        // Layer 2 fires here if we are a zombie: an experiment found that a fenced writer
        // still gets successful `put`s, and only the durability wait reveals it.
        self.handle
            .await_durable()
            .await
            .map_err(StoreError::from_slate)?;
        crate::probe::mark("durable.end", 0);
        // Durable but the waiter does not know yet: a `return` here is "written, then
        // failed" — the row is in the WAL but nobody may be acked.
        crate::failpoint!("writer::commit::after_durable", err);
        // A suspended grant: no ack meanwhile — wait for revival (ack later, under the new
        // deadline) or loss (`alive` below refuses).
        if self.validity.is_paused() {
            self.validity.settled().await;
        }
        // Check again **after** waiting: SlateDB returns `Ok` for a durable seq even when
        // the store closed (fenced) while we waited. Refusing the ticket then is overly
        // cautious but cheap — the client gets NotLeader and retries.
        alive(&self.status, &self.validity)?;
        Ok(Committed(()))
    }
}

/// The LF-3 guard in [`PartitionWriter::commit`]: dropped without `disarm` (an error after
/// `db.write`, or a cancelled future) ⇒ sets `send_doubt`.
struct SendDoubt(Option<std::rc::Rc<std::cell::Cell<bool>>>);

impl SendDoubt {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for SendDoubt {
    fn drop(&mut self) {
        if let Some(flag) = &self.0 {
            flag.set(true);
        }
    }
}

/// Is the store still alive and do we still hold the write right — the condition for
/// **every** ticket.
///
/// Applies to tickets issued on the spot too: a leader that lost its lease must no longer
/// answer read-only queries, because the data it reads may already have been overwritten
/// by the new leader.
fn alive(status: &StoreStatus, validity: &Validity) -> Result<()> {
    match status.close_reason() {
        Some(slatedb::CloseReason::Fenced) => return Err(StoreError::Fenced),
        Some(_) => return Err(StoreError::Closed),
        None => {}
    }
    if !validity.is_valid() {
        return Err(StoreError::LeaseLost);
    }
    Ok(())
}

/// The partition's single writer. **NOT** `Clone`, **NOT** `Send`, **NOT** `Sync`.
///
/// The following mistakes all **fail to compile**:
///
/// Cloning the writer to hand it to a background task:
/// ```compile_fail
/// # use cairn_store::{PartitionWriter};
/// fn clone_writer(w: &PartitionWriter) -> PartitionWriter { w.clone() }
/// ```
///
/// The writer is not `Send` — asserted directly on the type:
/// ```compile_fail
/// # use cairn_store::PartitionWriter;
/// fn require_send<T: Send>() {}
/// require_send::<PartitionWriter>();
/// ```
///
/// Sending the writer through `tokio::spawn`. Note the writer must **live across an
/// await** for `!Send` to bite: a future holding no value across an await point is still
/// `Send`. That is not a hole — the writer is only dangerous when *used* in another task,
/// and using it means awaiting:
/// ```compile_fail
/// # use cairn_store::{PartitionWriter, Mutations};
/// fn send(mut w: PartitionWriter) {
///     tokio::spawn(async move {
///         let _ = w.commit(Mutations::new()).await;
///     });
/// }
/// ```
///
/// Writing after leadership was given up (`seal` consumed the writer):
/// ```compile_fail
/// # use cairn_store::{PartitionWriter, Mutations};
/// # async fn f(mut w: PartitionWriter) {
/// w.seal().await.unwrap();
/// w.commit(Mutations::new()).await.unwrap();
/// # }
/// ```
///
/// `!Send` is a real constraint: the writer must live in one task. That holds anyway for
/// a single-threaded state machine, so nothing is lost.
pub struct PartitionWriter<S: RangeStore = Db> {
    db: S,
    partition: PartitionId,
    validity: Arc<Validity>,
    /// The latest write pushed into the WAL — **never taken away**.
    ///
    /// One slot rather than a queue, and that is right because the WAL is sequential: when
    /// a later seq is durable every earlier seq is durable. So waiting for the latest is
    /// waiting for all. Whoever needs a ticket *clones* this handle ([`Self::durability`]);
    /// the slot stays as it is for later askers.
    latest: Option<slatedb::WriteHandle>,
    /// The latest write that **writes an outgoing row** (outbox, signal outbox) — the mark
    /// of send tickets (LF-1). Overwritten like `latest`.
    latest_send: Option<slatedb::WriteHandle>,
    /// LF-3: a commit with an outgoing row submitted its batch but could not store the
    /// handle (an error after `db.write`, or the future cancelled there). Set ⇒ send
    /// tickets demand a barrier. `Rc` so the guard in `commit` does not borrow the writer.
    send_doubt: std::rc::Rc<std::cell::Cell<bool>>,
    /// The store's state — `durable_seq` and the close reason. Read with `borrow()` rather
    /// than `Db::status()`, because the latter clones the whole manifest.
    status: StoreStatus,
    /// The seq of the latest write, published for [`WriterHealth`] — readable without
    /// borrowing the writer (that is, without the engine lock).
    latest_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Bytes pushed through this writer (cumulative) — see [`WriterHealth::bytes_written`].
    written: Arc<std::sync::atomic::AtomicU64>,
    /// Time `db.write` spent stalled — see [`WriterHealth::write_stall`].
    stall: Arc<WriteStall>,
    /// The fresh copy of `external_dbs` read from the bucket — see
    /// [`WriterHealth::note_external_paths`].
    fresh_external: FreshExternal,
    /// SlateDB's statistics for the Db — see [`WriterHealth::db_stats`].
    db_stats: Option<Arc<crate::dbstats::DbStats>>,
    /// Keys read by scans, by region — see [`ScanCounts`].
    scanned: std::cell::Cell<ScanCounts>,
    /// Start points of every ordered-index scan, waiting for tests to take them — see
    /// [`Self::take_seq_scans`].
    #[cfg(any(test, feature = "testkit"))]
    seq_scans: std::cell::RefCell<Vec<(keyspace::Index, u64)>>,
    /// Start due times of every due-time index scan, waiting for tests to take them — see
    /// [`Self::take_due_scans`].
    #[cfg(any(test, feature = "testkit"))]
    due_scans: std::cell::RefCell<Vec<(keyspace::Index, u64)>>,
    /// The scan floors of the three ordered indexes, by slot of [`SeqFloor::INDEXES`].
    /// Never borrowed across an `await`.
    seq_floors: std::cell::RefCell<[SeqFloor; 3]>,
    /// The scan floors of the four due-time indexes, by slot of `floors::DUE_INDEXES`
    /// (timer, retry, purge, delayed): no index row (live or stale) has a due time below
    /// the floor. A commit writing a row below the floor lowers it (`Mutations::due_low`);
    /// a scan starts at the floor and raises it to the due time of the first row it meets.
    /// Without floors every background tick walks again through the tombstones of every
    /// removed row until compaction removes them — and since every turn writes and then
    /// removes a watchdog row, that is every turn. Loaded from the durable floors at open,
    /// written durably with commits (`floors.rs`).
    due_floors: [std::cell::Cell<u64>; 4],
    /// The cursor of a timer scan that hit its cap: the last index key read; `None` = the
    /// next scan starts at the floor. The term's RAM, never borrowed across an `await` —
    /// see [`Self::due_timers`].
    timer_resume: std::cell::RefCell<Option<Vec<u8>>>,
    /// The largest known `seq` of the three ordered indexes — a lower bound of issued
    /// `seq`s, written durably with the floors (`floors::SEQ_HIGH`) and read back
    /// regardless of the stamp.
    seq_high: std::cell::Cell<u64>,
    /// The largest queue `seq` the writer **knows for sure** — `None` = not known yet
    /// (`floors::QUEUE_HIGH`).
    queue_high: std::cell::Cell<Option<u64>>,
    /// The largest `seq` written into the queue index by any commit of this writer.
    queue_seen: std::cell::Cell<u64>,
    /// `writer_epoch` of the Db under this writer — the stamp of the durable floors (`floors.rs`).
    epoch: u64,
    /// The floors written in a **successful** commit of this writer; `None` = not written
    /// yet (the next commit stamps again), or the previous commit failed.
    floors_written: std::cell::Cell<Option<Floors>>,
    /// Within the open (from [`Self::open`] to [`Self::end_opening`]): whole-table scans of
    /// the derived region cache blocks — see `scan_open`.
    opening: std::cell::Cell<bool>,
    /// A hook called after each submitted commit, for each invocation status write of the
    /// batch: `(id, status code byte)` — see [`Self::set_status_hook`].
    status_hook: Option<StatusHook>,
    /// The range's query counters — see [`Self::set_query_counts`].
    query_counts: Option<std::sync::Arc<crate::query_index::QueryCounts>>,
    /// The range's artifact byte counter — see [`Self::set_artifact_bytes`].
    artifact_bytes: Option<std::sync::Arc<crate::artifact_bytes::ArtifactBytes>>,
    /// The range's billing unit meter — see [`Self::enable_units`].
    units: Option<std::sync::Arc<crate::units::UnitMeter>>,
    /// Durable streams riding along every batch — see [`Rider`].
    rider: Option<Box<dyn Rider>>,
    _not_send_sync: PhantomData<*const ()>,
}

/// A hook reporting invocation status writes — see
/// [`PartitionWriter::set_status_hook`]: `(id, status code byte)`.
pub type StatusHook = Box<dyn Fn(&InvocationId, u8)>;

/// One durable stream chunk riding along a commit.
#[derive(Clone, Debug)]
pub struct RiderChunk {
    pub id: InvocationId,
    pub name: String,
    /// Offset of the first record — the row's key.
    pub first: u64,
    pub chunk: keyspace::StreamChunk,
}

/// Whoever **rides along** commits: the range's durable streams.
///
/// Every batch of the writer — the commit of any invocation on the range, and the barrier
/// of [`PartitionWriter::seal`] — also carries the chunks not yet carried, at most **one row
/// per stream**. There is no separate time-based commit (AP-16).
///
/// Called on the engine thread while holding the engine: must be cheap when there is
/// nothing (one atomic), no I/O, no node-wide lock.
pub trait Rider {
    /// Chunks not yet carried. **Does not change state**: if the batch cannot be submitted,
    /// the next call takes exactly those chunks again (same key — overwritten, not doubled).
    fn pending(&self) -> Vec<RiderChunk>;
    /// The batch carrying `chunks` was submitted at `seq` — durable once `durable_seq ≥ seq`.
    fn carried(&self, seq: u64, chunks: &[RiderChunk]);
}

/// The health of the store under one writer, readable **from outside the engine lock**.
///
/// Why from outside: a `db.write` can stall (SlateDB backpressure) while still holding the
/// engine, and that is exactly when the store is in trouble. A watcher that had to take
/// the engine to look would stall with it.
#[derive(Clone)]
pub struct WriterHealth {
    status: StoreStatus,
    latest_seq: Arc<std::sync::atomic::AtomicU64>,
    written: Arc<std::sync::atomic::AtomicU64>,
    stall: Arc<WriteStall>,
    fresh_external: FreshExternal,
    db_stats: Option<Arc<crate::dbstats::DbStats>>,
}

/// Ancestor paths read from the manifest **on the bucket**: a lock of one writer, not a
/// global lock; only observers (the lineage loop, `/metrics`) touch it, never on the
/// turn's path.
type FreshExternal = Arc<std::sync::Mutex<Option<Vec<String>>>>;

/// A `db.write` longer than this counts as **stalled**.
///
/// `db.write` normally only inserts into the memtable — a few µs. Waiting longer than this
/// is almost certainly SlateDB backpressure (`max_unflushed_bytes`: the memtable cannot be
/// flushed to L0 because L0 hit its cap) — exactly the wall the L0 throttle wants to avoid.
pub const WRITE_STALL_MIN: std::time::Duration = std::time::Duration::from_millis(10);

/// Stall counter of `db.write`, shared between the writer and [`WriterHealth`].
#[derive(Default)]
struct WriteStall {
    count: std::sync::atomic::AtomicU64,
    micros: std::sync::atomic::AtomicU64,
}

/// Number and total duration of `db.write` stalls ≥ [`WRITE_STALL_MIN`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStallTotals {
    pub count: u64,
    pub micros: u64,
}

impl WriterHealth {
    /// Waits until the store closes (fence, broken for good, or clean close), returns the
    /// reason. `None` = nobody holds the store any more (it was dropped).
    pub async fn closed(&mut self) -> Option<slatedb::CloseReason> {
        self.status.closed().await
    }

    /// The durable seq — writes with a seq not above this number are durable.
    pub fn durable_seq(&self) -> u64 {
        self.status.durable_seq()
    }

    /// `(latest written seq, durable seq)` if some write is not yet durable.
    pub fn lag(&self) -> Option<(u64, u64)> {
        let latest = self.latest_seq.load(Ordering::Acquire);
        let durable = self.status.durable_seq();
        (latest > durable).then_some((latest, durable))
    }

    /// Bytes (key + value) committed through this writer, cumulative since open. An
    /// estimate — not counting what SlateDB adds itself. The per-range load statistics read
    /// the difference between two queries, outside the engine lock.
    pub fn bytes_written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Number of L0 views in the writer's **in-RAM** manifest.
    ///
    /// Read from the Db's state watch — updated after every memtable flush and every
    /// compaction writing a manifest — so it is cheap: one watch borrow, no GET. It counts
    /// views rather than SSTs, the same unit as `l0_max_ssts` (see
    /// [`crate::ObjectStores::range_l0_views`], the version reading the manifest on the
    /// store).
    pub fn l0_views(&self) -> usize {
        self.status.l0_views()
    }

    /// Number of ancestor Dbs whose SSTs the range's Db still reads — the number of elements
    /// of [`Self::external_paths`]. As cheap as [`Self::l0_views`].
    pub fn external_depth(&self) -> Option<usize> {
        self.external_paths().map(|p| p.len())
    }

    /// The paths of the ancestor Dbs whose SSTs the range's Db still reads — see
    /// [`StoreStatus::external_paths`], **intersected** with the latest copy read from the
    /// bucket ([`Self::note_external_paths`]). As cheap as [`Self::l0_views`].
    ///
    /// The in-RAM manifest only changes when the writer polls the manifest (1 h) or writes
    /// one itself, so after a detach written by someone else (GC, the lineage loop) it lags
    /// by up to an hour. Intersecting is right because the `external_dbs` of **one** Db only
    /// shrink over time (a clone sets them once at Db creation, only detach removes; range
    /// ids are never reused): the intersection of two snapshots is the newer snapshot.
    pub fn external_paths(&self) -> Option<Vec<String>> {
        let mut paths = self.status.external_paths()?;
        if let Some(fresh) = self.fresh_external.lock().ok().and_then(|g| g.clone()) {
            paths.retain(|p| fresh.binary_search(p).is_ok());
        }
        Some(paths)
    }

    /// Records the copy of `external_dbs` (distinct paths, sorted) just read from the
    /// manifest on the bucket ([`crate::ObjectStores::range_external_paths`]) —
    /// [`Self::external_paths`] minus the ancestors already detached that the in-RAM
    /// manifest has not seen yet.
    pub fn note_external_paths(&self, mut paths: Vec<String>) {
        paths.sort_unstable();
        paths.dedup();
        if let Ok(mut g) = self.fresh_external.lock() {
            *g = Some(paths);
        }
    }

    /// Number and duration of `db.write` stalls, cumulative since the writer opened.
    pub fn write_stall(&self) -> WriteStallTotals {
        WriteStallTotals {
            count: self.stall.count.load(Ordering::Relaxed),
            micros: self.stall.micros.load(Ordering::Relaxed),
        }
    }

    /// SlateDB's statistics for this Db (`backpressure_count`, `l0_stall_count`,
    /// `l0_sst_count`) — `None` when the code opening the writer attached no recorder.
    pub fn db_stats(&self) -> Option<crate::dbstats::DbStatsSnapshot> {
        self.db_stats.as_ref().map(|s| s.snapshot())
    }
}

impl<S: RangeStore> PartitionWriter<S> {
    /// The **only** way to get a writer: the grant is consumed.
    ///
    /// `Db::open` bumps `writer_epoch` and CASes it into the manifest, fencing every old
    /// writer of the same partition — including a frozen process that has no idea it has
    /// lost the right (verified experimentally).
    pub async fn open(grant: LeadershipGrant, db: S) -> Result<Self> {
        let status = db.status();
        let epoch = db.writer_epoch();
        Ok(Self {
            db,
            partition: grant.partition,
            // The writer's **own** validity, a child of the lease validity: losing the
            // lease ends the writer; but `seal` lowers the writer's validity without
            // lowering the lease's — a quiescing range still holds the lease after the Db
            // closes, and the keeper must not take that for a lost lease.
            validity: Arc::new(Validity::child(Arc::clone(&grant.validity))),
            latest: None,
            latest_send: None,
            send_doubt: Default::default(),
            status,
            latest_seq: Arc::default(),
            written: Arc::default(),
            stall: Arc::default(),
            fresh_external: Arc::default(),
            db_stats: None,
            scanned: Default::default(),
            #[cfg(any(test, feature = "testkit"))]
            seq_scans: Default::default(),
            #[cfg(any(test, feature = "testkit"))]
            due_scans: Default::default(),
            seq_floors: Default::default(),
            due_floors: Default::default(),
            timer_resume: Default::default(),
            seq_high: Default::default(),
            queue_high: Default::default(),
            queue_seen: Default::default(),
            epoch,
            floors_written: std::cell::Cell::new(None),
            opening: std::cell::Cell::new(true),
            status_hook: None,
            query_counts: None,
            artifact_bytes: None,
            units: None,
            rider: None,
            _not_send_sync: PhantomData,
        })
    }

    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// `writer_epoch` of the Db under this writer (at open). The split/merge coordinator
    /// closes the source conditionally on this epoch.
    pub fn writer_epoch(&self) -> u64 {
        self.epoch
    }

    /// Attaches the SlateDB statistics recorder of this Db — taken from
    /// [`crate::ObjectStores::take_db_stats`] right after opening the Db.
    pub fn with_db_stats(mut self, stats: Option<Arc<crate::dbstats::DbStats>>) -> Self {
        self.db_stats = stats;
        self
    }

    /// One engine hold of this range lasting `held`, by holder `holder`
    /// ([`crate::dbstats::HOLDERS`]) — the actor records it on release. Without a
    /// recorder, nothing. One atomic addition, no lock.
    pub fn note_engine_hold(&self, holder: usize, held: std::time::Duration) {
        if let Some(s) = &self.db_stats {
            s.note_hold(holder, held.as_micros() as u64);
        }
    }

    /// A store health handle, usable without borrowing the writer.
    /// Scan counters by region, since the writer opened.
    pub fn scan_counts(&self) -> ScanCounts {
        self.scanned.get()
    }

    /// `(index, start seq)` of every ordered-index scan (outbox, signal, inbound) since the
    /// previous take. With [`Self::tombstones_from`], lets tests say "this flush turn walked
    /// through so many deleted rows" without measuring time.
    /// The current retry scan floor — see `due_floors`.
    #[cfg(any(test, feature = "testkit"))]
    pub fn retry_floor(&self) -> u64 {
        self.due_floor(Index::Retry)
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn take_seq_scans(&self) -> Vec<(keyspace::Index, u64)> {
        self.seq_scans.take()
    }

    /// `(index, start due time)` of every due-time index scan (timer, retry, purge,
    /// delayed) since the previous take — lets tests say "the first scan after reopening
    /// starts from the durable floor".
    #[cfg(any(test, feature = "testkit"))]
    pub fn take_due_scans(&self) -> Vec<(keyspace::Index, u64)> {
        self.due_scans.take()
    }

    pub fn health(&self) -> WriterHealth {
        WriterHealth {
            status: self.status.clone(),
            latest_seq: Arc::clone(&self.latest_seq),
            written: Arc::clone(&self.written),
            stall: Arc::clone(&self.stall),
            fresh_external: Arc::clone(&self.fresh_external),
            db_stats: self.db_stats.clone(),
        }
    }

    /// Pushes a batch into the WAL. Does **not** wait for it to be durable — see
    /// [`Self::durability`].
    ///
    /// `&mut self` ⇒ two commits can never run concurrently on the same writer.
    ///
    /// **Why the wait is split out.** Measured (`tests/group_commit.rs`): 50 writes issued
    /// then waited on together take 13ms, the same 50 writes waited on one by one take
    /// 583ms — **44.6×**. SlateDB gathers every in-flight write into one `current_wal` and
    /// flushes once; grouping is something it already does. The obstacle sits above: the
    /// engine lives in a `Mutex`, and waiting for durability *while* holding the mutex means
    /// `current_wal` never has more than one entry to group. Returning right after
    /// `db.write` is the only way for that buffer to hold more than one thing.
    ///
    /// **The cost, said plainly:** this function is no longer the INV-5 fence. The fence
    /// moved to [`Committed`], and every outward effect must demand it — see the comment on
    /// [`Committed`] about why the compiler keeps that list instead of the author's memory.
    ///
    /// **Cancellation midway.** If the future is cancelled (or `db.write` returns an error)
    /// **after** the batch was submitted, SlateDB still applies that batch, but `latest`
    /// does not get its handle: a ticket issued afterwards only covers it through a later
    /// write (a prefix), and [`Self::seal`] covers it with a barrier. Every place that can
    /// cancel a task in the middle of `commit` belongs to **the end of a term** — when every
    /// later ticket has become meaningless:
    ///
    /// - `TermTasks::abort` (flusher, rate loop, rescue net) at the start of
    ///   `quiesce_in_flight`;
    /// - `JoinSet::shutdown` of the term's tasks (turns, commands) when stepping down,
    ///   losing the lease, or at the end of quiesce (`actor/serve.rs`);
    /// - a `select!` dropping the term-start `restore_flow` when the lease is lost, and the
    ///   `tokio::time::timeout` around `seal` — the barrier write itself
    ///   (`actor/leadership.rs`);
    /// - the node's runtime shutting down.
    ///
    /// No place cancels a `commit` mid-term: the `select!`s of the command loop, the
    /// background loop and the rescue net only choose between **waits** (the engine already
    /// released), and an HTTP request abandoned by its client does not cancel the command's
    /// task (the task lives in the range's `JoinSet`, only the reply `oneshot` is dropped).
    /// The failpoint `writer::commit::after_submit` builds this gap mid-term
    /// (`tests/failpoints.rs::cancelled_outbox_commit_is_not_sent_before_it_is_durable`).
    pub async fn commit(&mut self, mutations: Mutations) -> Result<()> {
        // A broken batch means there is nothing to commit. Refused even before the lease
        // check: a wrong key does not become right because we still hold leadership.
        if let Some(e) = mutations.invalid {
            return Err(e);
        }
        // Refuse early: if the lease layer already knows we lost the right, do not spend a
        // round trip. This is only an optimisation, NOT the safety mechanism — safety is the
        // layer below.
        //
        // A **suspended** grant waits instead of returning an error: the turn in flight goes
        // through no error branch (the engine's in-RAM state — staged sources, scan floors —
        // stays as if nothing had happened), and on revival it writes on as usual. Waiting
        // while holding the engine is deliberate: every other turn of the range must stop
        // too, and the suspension cap bounds the wait.
        if !self.validity.is_valid() && !self.validity.settled().await {
            return Err(StoreError::LeaseLost);
        }
        crate::failpoint!("writer::commit::before_write", err);

        // Index rows written **below** the scan floor go into `below` — before writing, so
        // that even a write that reports an error yet reaches the store does not slip
        // through (extra entries are harmless, see [`SeqFloor`]). Deletes are only dropped
        // from `below` after the write is done. Due-time rows written below the floor lower
        // it — before writing, for the same reason; and before building the durable floor
        // value just below, so the batch carrying the row also carries the lowered floor
        // (FL-18).
        for (cell, low) in self.due_floors.iter().zip(mutations.due_low) {
            if let Some(low) = low {
                if low < cell.get() {
                    cell.set(low);
                }
            }
        }
        let seq_ops = mutations.seq_ops;
        // Before building the floor value just below: a batch writing queue entry `seq`
        // also carries `queue` ≥ `seq` (`floors::QUEUE_HIGH`).
        if let Some(q) = mutations.queue_high {
            self.note_queue_seq(q);
        }
        let status_writes = mutations.status_writes;
        let query_events = mutations.query_events;
        let artifact_events = mutations.artifact_events;
        let units = mutations.units;
        let sends = mutations.sends;
        // Debug builds: every outbox/signal index row **written** in the batch must come
        // with the `sends` bit (two independent paths to the same truth).
        debug_assert!(
            sends
                || !seq_ops.iter().any(|(slot, put, _)| *put
                    && matches!(SeqFloor::INDEXES[*slot], Index::Outbox | Index::Signal)),
            "batch writes an outbox/signal index row without marking `sends` — LF-1"
        );
        if !seq_ops.is_empty() {
            let mut floors = self.seq_floors.borrow_mut();
            for (slot, put, key) in &seq_ops {
                let f = &mut floors[*slot];
                match keyspace::parse_ix_seq(key, SeqFloor::INDEXES[*slot]) {
                    Some((seq, _)) if *put => {
                        // Before building the floor value just below: a batch carrying
                        // row `seq` also carries `high` ≥ `seq`.
                        self.note_seq_high(seq);
                        if seq < f.floor {
                            f.below.insert((seq, key.clone()));
                        }
                    }
                    _ => {}
                }
            }
        }

        // Durable floors: if they differ from the written value they go **in this same
        // batch** — no separate commit, no separate PUT. The fence is the Db's `writer_epoch`.
        let mut batch = mutations.batch;
        let floors = self.floors_now();
        let stamped = (self.floors_written.get() != Some(floors)).then(|| {
            batch.put(keyspace::floors().into(), floors.encode(self.epoch).into());
            floors
        });

        // Durable streams riding along: ≤ 1 row per stream.
        let rides = self.rider.as_ref().map(|r| r.pending()).unwrap_or_default();
        let mut bytes = mutations.bytes;
        for c in &rides {
            let key = keyspace::stream_chunk(&c.id, &c.name, c.first);
            let value = c.chunk.encode(c.first);
            bytes += (key.len() + value.len()) as u64;
            batch.put(key.into(), value.into());
        }
        let started = tokio::time::Instant::now();
        // LF-3: from here until the handle is stored, an error or a cancellation leaves a
        // batch that was (possibly) applied with nobody waiting — the guard sets `send_doubt`.
        let doubt = sends.then(|| SendDoubt(Some(std::rc::Rc::clone(&self.send_doubt))));
        let handle = match self.db.write(batch).await {
            Ok(h) => h,
            Err(e) => {
                // Unknown whether the batch reached the store: write the floors again next time.
                self.floors_written.set(None);
                return Err(e);
            }
        };
        // The batch has been submitted — SlateDB will apply it — but its handle is not
        // stored yet: the gap of a future cancelled here (see "Cancellation midway" in the
        // function doc). The point only considers batches with an outbox row.
        crate::failpoint!(
            "writer::commit::after_submit",
            seq_ops
                .iter()
                .any(|(slot, put, _)| *put && SeqFloor::INDEXES[*slot] == Index::Outbox),
            err
        );
        if let Some(d) = doubt {
            d.disarm();
        }
        if stamped.is_some() {
            self.floors_written.set(stamped);
        }
        if !seq_ops.is_empty() {
            // In batch order: a write then a delete of the same key leaves the key dead.
            let mut floors = self.seq_floors.borrow_mut();
            for (slot, put, key) in seq_ops {
                let f = &mut floors[slot];
                if let Some((seq, _)) = keyspace::parse_ix_seq(&key, SeqFloor::INDEXES[slot]) {
                    if seq < f.floor {
                        if put {
                            f.below.insert((seq, key));
                        } else {
                            f.below.remove(&(seq, key));
                        }
                    }
                }
            }
        }
        // A stall (backpressure) is recorded — two atomics, no lock.
        let took = started.elapsed();
        if took >= WRITE_STALL_MIN {
            self.stall.count.fetch_add(1, Ordering::Relaxed);
            self.stall
                .micros
                .fetch_add(took.as_micros() as u64, Ordering::Relaxed);
        }

        // Overwrite rather than queue: the WAL is sequential, so the newest handle covers
        // every handle before it.
        self.latest_seq.store(handle.seqnum(), Ordering::Release);
        self.written.fetch_add(bytes, Ordering::Relaxed);
        if sends {
            self.latest_send = Some(handle.clone());
        }
        if !rides.is_empty() {
            if let Some(r) = &self.rider {
                r.carried(handle.seqnum(), &rides);
            }
        }
        // Billing units: the batch was submitted at this seq — the meter only counts them
        // once that seq is durable. The range's lock, not a shared lock.
        if let (Some(meter), false) = (&self.units, units.is_empty()) {
            meter.committed(handle.seqnum(), crate::units::hour_now(), units);
        }
        self.latest = Some(handle);
        if let Some(hook) = &self.status_hook {
            for (id, tag) in &status_writes {
                hook(id, *tag);
            }
        }
        if let (Some(counts), false) = (&self.query_counts, query_events.is_empty()) {
            counts.apply(&query_events);
        }
        if let (Some(bytes), false) = (&self.artifact_bytes, artifact_events.is_empty()) {
            bytes.apply(&artifact_events);
        }
        Ok(())
    }

    /// The validity handle of this writer — the query layer checks it before every batch
    /// (`RangeReadHandle::alive`): with the lease lost or the writer sealed, reading
    /// through the handle stops.
    pub fn validity_handle(&self) -> ValidityHandle {
        ValidityHandle(Arc::clone(&self.validity))
    }

    /// Attaches the range's query counters: every `query_event` of a batch is applied to
    /// them right after the batch is submitted, on the engine thread.
    pub fn set_query_counts(&mut self, counts: std::sync::Arc<crate::query_index::QueryCounts>) {
        self.query_counts = Some(counts);
    }

    /// Attaches the range's artifact byte counter: every [`Mutations::artifact_bytes`] of a
    /// batch is applied to it right after the batch is submitted, on the engine thread.
    pub fn set_artifact_bytes(
        &mut self,
        bytes: std::sync::Arc<crate::artifact_bytes::ArtifactBytes>,
    ) {
        self.artifact_bytes = Some(bytes);
    }

    /// Enables the range's billing unit meter: every [`Mutations::meter_units`] of a
    /// submitted batch goes into it with the batch's `seq`, and only counts once that seq
    /// is durable ([`crate::units`]). Calling it again returns the existing meter.
    pub fn enable_units(&mut self) -> std::sync::Arc<crate::units::UnitMeter> {
        self.units
            .get_or_insert_with(|| crate::units::UnitMeter::new(self.status.clone()))
            .clone()
    }

    /// Sets the hook reporting **every** invocation status write — called right after the
    /// commit containing it is submitted (not yet durable), on the engine thread.
    ///
    /// One place for every outcome path: statuses can only be written through
    /// [`Mutations::set_invocation_status`], and every batch goes through `commit`. The
    /// outcome notifier of `/invoke` relies on that. The hook must be cheap — no global
    /// lock, no I/O: it runs while holding the engine.
    pub fn set_status_hook(&mut self, hook: StatusHook) {
        self.status_hook = Some(hook);
    }

    /// Attaches the commit rider ([`Rider`]) — this term's durable streams.
    pub fn set_rider(&mut self, rider: Box<dyn Rider>) {
        self.rider = Some(rider);
    }

    /// A ticket, or the mark to wait for to get one.
    ///
    /// `Ready` when every write pushed through this writer is durable — that is, everything
    /// a read on the leader may have seen. `Wait` carries a copy of the latest handle:
    /// waiting on it waits for exactly the mark **at the time of asking**, whoever else is
    /// waiting too. Nothing is taken out of the writer, so no later asker is wrongly given
    /// a ticket.
    ///
    /// `Err` if the store has closed (fenced) or the lease is lost — even when nothing is
    /// in flight.
    pub fn durability(&self) -> Result<Durability> {
        alive(&self.status, &self.validity)?;
        match &self.latest {
            Some(h) if self.status.durable_seq() < h.seqnum() => Ok(Durability::Wait(Pending {
                handle: h.clone(),
                status: self.status.clone(),
                validity: Arc::clone(&self.validity),
            })),
            _ => Ok(Durability::Ready(Committed(()))),
        }
    }

    /// A **send** ticket, or the mark to wait for to get one (LF-1).
    ///
    /// Like [`Self::durability`], but compared with the latest write **with an outgoing
    /// row** (`latest_send`), not with the latest write. Lemma: every outgoing row a read on
    /// the leader sees lies in a batch that went through `commit` with the `sends` bit; that
    /// batch either stored its handle — so `latest_send` is it or a later write — or lost
    /// it, and then `send_doubt` is set ⇒ [`SendDurability::Barrier`]. The WAL is
    /// sequential (prefixes): `latest_send` durable ⇒ every outgoing row read before the
    /// time of asking is durable.
    pub fn send_durability(&self) -> Result<SendDurability> {
        alive(&self.status, &self.validity)?;
        if self.send_doubt.get() {
            return Ok(SendDurability::Barrier);
        }
        match &self.latest_send {
            Some(h) if self.status.durable_seq() < h.seqnum() => {
                Ok(SendDurability::Wait(Pending {
                    handle: h.clone(),
                    status: self.status.clone(),
                    validity: Arc::clone(&self.validity),
                }))
            }
            _ => Ok(SendDurability::Ready(SendTicket(()))),
        }
    }

    /// The LF-3 barrier: a write submitted **after** the write with an outgoing row that
    /// lost its handle (deleting the `seal_barrier` key, like [`Self::seal`] — outside every
    /// range). It becomes the mark of send tickets (and of full tickets): durable ⇒ the
    /// write that lost its handle is durable (prefix). On an error or a cancellation
    /// `send_doubt` stays set — the next ask tries again.
    pub async fn send_barrier(&mut self) -> Result<()> {
        crate::failpoint!("writer::barrier");
        let mut barrier = Batch::default();
        barrier.delete(keyspace::seal_barrier().into());
        let handle = self.db.write(barrier).await?;
        self.latest_seq.store(handle.seqnum(), Ordering::Release);
        self.latest_send = Some(handle.clone());
        self.latest = Some(handle);
        self.send_doubt.set(false);
        Ok(())
    }

    /// A clone of the store under the writer — **read only**: the query layer's read
    /// handle keeps it, reads at the `Remote` level through the snapshot, never writes.
    pub fn read_store(&self) -> S
    where
        S: Clone,
    {
        self.db.clone()
    }

    /// The store under the writer — for contract tests flushing the WAL themselves (`tests/range_store.rs`).
    #[cfg(any(test, feature = "testkit"))]
    pub fn store(&self) -> &S {
        &self.db
    }

    /// The seq of the latest write with an outgoing row, if any — for the flusher's cheap
    /// check after getting a ticket (the `turn::outbox::undurable_send` tripwire).
    pub fn latest_send_seq(&self) -> Option<u64> {
        self.latest_send.as_ref().map(|h| h.seqnum())
    }

    /// The store's `durable_seq`.
    pub fn durable_seq(&self) -> u64 {
        self.status.durable_seq()
    }

    /// A read that sees only **durable** data ([`RangeStore::get_durable`]).
    pub async fn get_durable(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        self.db.get_durable(key).await
    }

    /// Every write pushed through this writer is durable — no ticket, no lease check: only
    /// for counting (`cairnflow_drain_send_waits_total`).
    pub fn caught_up(&self) -> bool {
        self.latest
            .as_ref()
            .is_none_or(|h| self.status.durable_seq() >= h.seqnum())
    }

    /// The writer's grant is **suspended**: no longer valid but not lost. `Some` = a future
    /// waiting until the suspension ends — `Ok` on revival, [`StoreError::LeaseLost`] on
    /// loss. The future does not borrow the writer, so the call site can release the
    /// engine while waiting (`Lease::sync`).
    pub fn paused(&self) -> Option<impl std::future::Future<Output = Result<()>> + 'static> {
        self.validity.is_paused().then(|| {
            let validity = Arc::clone(&self.validity);
            async move {
                if validity.settled().await {
                    Ok(())
                } else {
                    Err(StoreError::LeaseLost)
                }
            }
        })
    }

    /// **The leader-side read fence** for an answer this writer just read: the future
    /// confirms — with a conditional GET on the incarnation's session record, batched for
    /// the whole node — that nobody took the range **after** the read. `None` = the grant
    /// lives under no session (single node). The request is only issued when the future is
    /// first polled, so the call site creates it **after** the read; the future does not
    /// borrow the writer, so the call site can release the engine while waiting
    /// (`Lease::sync_reply`).
    pub fn read_fence(&self) -> Option<impl std::future::Future<Output = Result<()>> + 'static> {
        let fence = Arc::clone(self.validity.read_fence()?);
        Some(async move {
            crate::probe::mark("fence.begin", 0);
            let ok = fence().await;
            crate::probe::mark("fence.end", u64::from(ok));
            if ok {
                Ok(())
            } else {
                Err(StoreError::LeaseLost)
            }
        })
    }

    /// [`Self::durability`] and then wait right away — for call sites holding **no** lock
    /// others need (tests, `seal`, tools). The server goes through `Lease::sync` to release
    /// the engine while waiting.
    pub async fn wait_durable(&self) -> Result<Committed> {
        if let Some(settle) = self.paused() {
            settle.await?;
        }
        match self.durability()? {
            Durability::Ready(c) => Ok(c),
            Durability::Wait(p) => p.durable().await,
        }
    }

    /// Flushes the memtable into an L0 SST and writes the manifest — **while still
    /// serving**.
    ///
    /// Step 2 of the split protocol: does the bulk of the closing work outside the
    /// write-stop window, so that `seal` in step 3 only flushes the small part written
    /// afterwards.
    pub async fn flush_memtable(&self) -> Result<()> {
        self.db.flush(Flush::MemTable).await
    }

    /// [`Self::flush_memtable`] that always stores a fresh manifest: it first writes the
    /// barrier key of [`Self::seal`] (a delete of a key outside every range) and waits for it
    /// to be durable. An empty memtable stores no manifest, and then `recent_snapshot_min_seq`
    /// keeps the value of an older flush, which can name a snapshot closed long ago. Tenant
    /// purge phase B reads that value right after this flush to decide if a full compaction
    /// may go (G32); without a fresh manifest an idle range would wait for ever.
    pub async fn flush_memtable_fresh(&self) -> Result<()> {
        let mut barrier = Batch::default();
        barrier.delete(keyspace::seal_barrier().into());
        self.db
            .write(barrier)
            .await?
            .await_durable()
            .await
            .map_err(StoreError::from_slate)?;
        self.db.flush(Flush::MemTable).await
    }

    /// Wakes the Db's compactor so a compaction just requested through `Admin` runs at once
    /// (tenant purge phase B). See [`RangeStore::wake_compactor`].
    pub fn wake_compactor(&self) {
        self.db.wake_compactor()
    }

    /// Gives up leadership and **really closes the Db**. Consumes the writer ⇒ no writing
    /// after giving up.
    ///
    /// The procedure: force what is pending to be durable → flush the WAL → flush the
    /// memtable into L0 (the manifest covers everything written) → close. Returns once the
    /// Db is closed: after that a clone with a projection takes it as a source, and the
    /// child sees every acked row. `Db::close` flushes the memtable itself, but
    /// **swallows** that flush's error (only logs it); here we want to *know* — a write lost
    /// exactly while handing over is the kind of bug that only shows up at a later read, on
    /// another node.
    ///
    /// **A barrier before flushing.** A write submitted to SlateDB whose future was
    /// cancelled (quiesce and stepping down cancel tasks midway) is still applied by SlateDB
    /// afterwards — possibly **after** the memtable flush, leaving rows in the WAL that the
    /// manifest does not cover: F3 refuses to clone such a Db. A durable write goes through
    /// the same SlateDB write queue, so once it is durable every write submitted before it
    /// is also in the memtable and the WAL. It deletes a derived-region key nobody uses —
    /// outside every range, so no child sees it.
    ///
    /// If the flush fails it still closes (so the Db is not left open), and returns the
    /// flush's error.
    pub async fn seal(self) -> Result<()> {
        self.validity.revoke();
        let flushed = async {
            crate::failpoint!("writer::seal::before_flush", err);
            if let Some(handle) = &self.latest {
                handle
                    .await_durable()
                    .await
                    .map_err(StoreError::from_slate)?;
            }
            let mut barrier = Batch::default();
            barrier.delete(keyspace::seal_barrier().into());
            // Quiesce (split, giving up the lease): the barrier carries every durable stream
            // record not yet carried — no extra PUT (AP-15).
            let rides = self.rider.as_ref().map(|r| r.pending()).unwrap_or_default();
            for c in &rides {
                barrier.put(
                    keyspace::stream_chunk(&c.id, &c.name, c.first).into(),
                    c.chunk.encode(c.first).into(),
                );
            }
            let handle = self.db.write(barrier).await?;
            if let Some(r) = self.rider.as_ref().filter(|_| !rides.is_empty()) {
                r.carried(handle.seqnum(), &rides);
            }
            handle
                .await_durable()
                .await
                .map_err(StoreError::from_slate)?;
            for what in [Flush::Wal, Flush::MemTable] {
                self.db.flush(what).await?;
            }
            Ok::<_, StoreError>(())
        }
        .await;
        let closed = self.db.close().await;
        flushed.and(closed)
    }
}

/// The part only the SlateDB version has: reading tombstones (the model keeps no history).
impl PartitionWriter<Db> {
    /// The number of keys of index `ix`, from `from_seq` on, whose newest version is a
    /// **tombstone** — the deleted rows a scan starting at `from_seq` walks through
    /// (SlateDB's filter drops them, but they still have to be read) until compaction
    /// removes them.
    ///
    /// Reads newest-first (`scan_prefix_by_recency`): the first time a key is met is its
    /// newest version.
    #[cfg(any(test, feature = "testkit"))]
    pub async fn tombstones_from(&self, ix: keyspace::Index, from_seq: u64) -> Result<u64> {
        let mut it = self
            .db
            .scan_prefix_by_recency(keyspace::index_prefix(ix))
            .await
            .map_err(StoreError::from_slate)?;
        let mut seen = std::collections::BTreeSet::new();
        let mut n = 0;
        while let Some(e) = it.next_entry().await.map_err(StoreError::from_slate)? {
            if !seen.insert(e.key.clone()) {
                continue;
            }
            let after = keyspace::parse_ix_seq(&e.key, ix).is_some_and(|(seq, _)| seq >= from_seq);
            if after && matches!(e.value, slatedb::ValueDeletable::Tombstone) {
                n += 1;
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tick origin is initialised lazily, so an `Instant` **before** the origin — when
    /// a lease takeover was issued — must round-trip intact, not clamped to 0 (that is,
    /// "expired since forever").
    #[test]
    fn ticks_roundtrip_around_epoch() {
        let epoch = tick_epoch();
        for at in [
            epoch - Duration::from_millis(250),
            epoch,
            epoch + Duration::from_secs(3600),
        ] {
            assert_eq!(from_ticks(to_ticks(at)), at);
        }
        assert!(to_ticks(epoch - Duration::from_millis(1)) < to_ticks(epoch));
    }

    #[test]
    fn validity_expires_by_clock_and_late_extension_cannot_revive_it() {
        let h = ValidityHandle(Arc::new(Validity::new(to_ticks(
            Instant::now() + Duration::from_millis(30),
        ))));
        assert!(h.is_valid());
        assert!(h.extend_until(Instant::now() + Duration::from_millis(60)));
        std::thread::sleep(Duration::from_millis(80));
        assert!(!h.is_valid(), "past the deadline yet still valid");
        assert!(!h.extend_until(Instant::now() + Duration::from_secs(10)));
        assert!(!h.is_valid(), "a late renewal revived the grant");
    }

    /// A session root has a **suspension cap**. Past the deadline ⇒ no longer valid (the
    /// children neither) but not lost: `lost()` does not fire, a renewal winning the CAS
    /// moves the deadline and the grant runs again. Past the cap too ⇒ lost, moving the
    /// deadline no longer revives it.
    #[tokio::test]
    async fn paused_root_revives_on_extension_and_is_lost_past_the_cap() {
        let ms = Duration::from_millis;
        let root = ValidityHandle::root_with_pause(Instant::now() + ms(30), ms(60), None);
        let c = child_of(&root);
        tokio::time::sleep(ms(40)).await;
        assert!(!c.is_valid(), "expired yet child valid (LS-2)");
        assert!(!c.is_lost(), "suspended yet considered lost");
        assert!(
            futures::FutureExt::now_or_never(c.lost()).is_none(),
            "lost() fired during the suspension"
        );
        // A winning CAS whose new deadline is still in the past: not lost, still suspended.
        assert!(root.extend_until(Instant::now() - ms(1)));
        assert!(!c.is_valid() && !c.is_lost());
        // A win carrying a deadline in the future: revived.
        assert!(root.extend_until(Instant::now() + ms(30)));
        assert!(
            c.is_valid(),
            "a renewal winning the CAS during the suspension did not revive the child"
        );
        // This time let it pass the cap too: lost, and no longer revivable.
        tokio::time::timeout(Duration::from_secs(2), c.lost())
            .await
            .expect("lost() did not fire at the suspension cap");
        assert!(c.is_lost() && root.is_lost());
        assert!(!root.extend_until(Instant::now() + Duration::from_secs(10)));
        assert!(!c.is_valid(), "a renewal after the cap revived the grant");
    }

    /// Commits and tickets **wait** during a suspension instead of returning an error —
    /// waking on revival (going on), or on loss (`LeaseLost`).
    #[tokio::test]
    async fn settled_waits_out_the_pause() {
        let ms = Duration::from_millis;
        let root = ValidityHandle::root_with_pause(Instant::now() + ms(10), ms(500), None);
        let c = child_of(&root);
        tokio::time::sleep(ms(20)).await;
        assert!(c.0.is_paused());
        let r2 = root.clone();
        tokio::spawn(async move {
            tokio::time::sleep(ms(30)).await;
            assert!(r2.extend_until(Instant::now() + Duration::from_secs(10)));
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(2), c.0.settled())
                .await
                .expect("settled() did not wake on revival"),
            "revived yet settled() reports lost"
        );
        let root = ValidityHandle::root_with_pause(Instant::now() + ms(10), ms(30), None);
        let c = child_of(&root);
        tokio::time::sleep(ms(20)).await;
        assert!(
            !tokio::time::timeout(Duration::from_secs(2), c.0.settled())
                .await
                .expect("settled() did not wake at the cap"),
            "past the cap yet settled() reports revived"
        );
    }

    fn child_of(parent: &ValidityHandle) -> ValidityHandle {
        LeadershipGrant::from_session(PartitionId(0), 1, parent).validity_handle()
    }

    #[test]
    fn child_invalid_when_parent_revoked() {
        let parent = ValidityHandle::root(Instant::now() + Duration::from_secs(10));
        let a = child_of(&parent);
        let b = child_of(&parent);
        assert!(a.is_valid() && b.is_valid());
        // Revoking one child on its own does not touch the parent or the other child.
        a.revoke();
        assert!(!a.is_valid() && b.is_valid() && parent.is_valid());
        parent.revoke();
        assert!(
            !b.is_valid(),
            "the session is lost yet the partition grant is still valid"
        );
    }

    #[test]
    fn child_invalid_when_parent_expires() {
        let parent = ValidityHandle::root(Instant::now() + Duration::from_millis(30));
        let c = child_of(&parent);
        assert!(c.is_valid());
        assert!(parent.extend_until(Instant::now() + Duration::from_millis(60)));
        std::thread::sleep(Duration::from_millis(40));
        assert!(c.is_valid(), "session renewed, child grant still valid");
        std::thread::sleep(Duration::from_millis(40));
        assert!(!c.is_valid(), "session expired yet child grant valid");
    }

    #[tokio::test]
    async fn lost_wakes_on_parent() {
        // Revoke the parent.
        let parent = ValidityHandle::root(Instant::now() + Duration::from_secs(10));
        let c = child_of(&parent);
        let p2 = parent.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p2.revoke();
        });
        tokio::time::timeout(Duration::from_secs(2), c.lost())
            .await
            .expect("the child's lost() did not wake when the parent was revoked");

        // The parent passes its deadline, nobody revokes anything.
        let parent = ValidityHandle::root(Instant::now() + Duration::from_millis(30));
        let c = child_of(&parent);
        tokio::time::timeout(Duration::from_secs(2), c.lost())
            .await
            .expect("the child's lost() did not wake when the parent expired");
    }

    #[test]
    fn pinned_child_ignores_parent() {
        let parent = ValidityHandle::root(Instant::now() + Duration::from_secs(10));
        let c = child_of(&parent);
        c.pin_for_fault_injection();
        parent.revoke();
        assert!(
            c.is_valid(),
            "a pinned grant must ignore its parent — that is the fault-injection scenario"
        );
        c.revoke();
        assert!(!c.is_valid(), "pinning does not block revoke");
    }

    /// Whole-table scans must read SSTs in large chunks, not one 4 KiB block at a time.
    ///
    /// SlateDB's default `ScanOptions` reads **one block per GET, one task** — measured ~11×
    /// slower than a 4 MiB × 4 task read-ahead on SeaweedFS. The whole-table scans
    /// (`recover` on becoming leader, `restore_flow`, `/invocations`) are exactly that kind
    /// of scan, and they lie on the failover path.
    #[tokio::test]
    async fn full_table_scan_reads_ahead() {
        use crate::testkit::{FaultyStore, Op, PathClass};
        use slatedb::{
            config::{FlushOptions, FlushType},
            object_store::memory::InMemory,
        };

        // `Invocations` index rows are small (~30 bytes), so more rows are needed for the
        // index to reach ~1 MiB.
        const ROWS: usize = 32_000;
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let open = || async {
            let db = Db::builder(
                "p",
                store.clone() as Arc<dyn slatedb::object_store::ObjectStore>,
            )
            .build()
            .await
            .unwrap();
            PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
                .await
                .unwrap()
        };

        // ~1 MiB of status index rows, flushed into an SST, then closed: the next open reads
        // cold.
        let mut w = open().await;
        let status = vec![7u8; 1];
        for chunk in 0..ROWS / 500 {
            let mut m = Mutations::new();
            for i in 0..500 {
                let mut id = [0u8; keyspace::ID_LEN];
                id[..8].copy_from_slice(&((chunk * 500 + i) as u64).to_be_bytes());
                m.set_invocation_status(&id, &status);
            }
            w.commit(m).await.unwrap();
        }
        w.wait_durable().await.unwrap();
        w.db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        w.seal().await.unwrap();

        let w = open().await;
        let before = store.counts();
        let rows = w.invocation_tags_unbounded().await.unwrap();
        let gets = store
            .counts()
            .since(&before)
            .get(Op::Get, PathClass::Compacted);
        assert_eq!(rows.len(), ROWS);
        // Default: ~1 MiB / 4 KiB ≈ 260 GETs. 4 MiB read-ahead: a few GETs for the
        // index/filter + one or two data reads.
        assert!(
            gets <= 16,
            "a whole-table scan cost {gets} GETs — reading one block at a time?"
        );
    }
}
