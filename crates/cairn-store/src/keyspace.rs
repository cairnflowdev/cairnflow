// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The key space of every state table (listed in [`Table`] and [`Index`]).
//!
//! **Why this module has existed since the first commit (INV-3).** Redo replication only
//! carries KV mutations — a follower does not know which service to call. That is only
//! valid when *all pending work* has been materialized as rows in a state table. An action
//! that lives only in the leader's RAM disappears on failover, and **no simple test catches
//! it**. So the only way to write is through the functions here: there is no "write an
//! arbitrary key" API.
//!
//! # Three regions (D21)
//!
//! ```text
//! 0x00 ‖ owner_pk(u64 BE) ‖ table ‖ <key>   hot entities — status, outbox, inbound, timer, …
//! 0x01 ‖ index ‖ <key>                      derived — outside every range, rebuildable
//! 0x02 ‖ owner_pk(u64 BE) ‖ table ‖ <key>   bulk entities — Journal, State
//! ```
//!
//! The range of a partition `[a, b)` is two byte ranges `[0x00‖a, 0x00‖b)` +
//! `[0x02‖a, 0x02‖b)` ([`owned_ranges`]) — the multi-range projection of the SlateDB fork
//! (F4) cuts exactly that much. Region `0x01` lies outside every range, so after a split or
//! merge the children see it empty (the union filters it out), and it is rebuilt from the
//! hot region. Which table lives in which region is decided by **one** function:
//! [`region_of`].
//!
//! `owner_pk` is the partition key of **the local entity that owns the row** — not of
//! whatever id happens to stand in the key. An outbox `Reply` row belongs to the producer of
//! the reply, not to the recipient; an inbound message belongs to the destination, not to
//! the sender. Get this wrong and after a split neither side sees the row, while the sender
//! has already deleted its outbox — an acked message is lost.
//!
//! Every key goes through **exactly one** of the two builders — [`entity`] and [`index`] —
//! and every `parse_*` strips the key header through exactly one of the two splitters
//! ([`entity_body`], [`index_body`]), then reads the rest from offset 0.
//!
//! Every variable-length component has a length prefix, so two different keys never encode
//! to the same bytes.

use {
    crate::{
        cluster::partition_key_of,
        partition_table::{partition_key_for_target, PartitionKey},
        scope::{LimitKey, Scope, TenantId},
    },
    bytes::{BufMut, BytesMut},
};

pub use crate::ids::{id_from_hex, id_from_slice, id_hex, InvocationId, ID_LEN};

/// **Entity** table codes. Values **must not change** once there is real data.
///
/// Retired codes keep their slot and **must not be reused** — a code reused for another
/// meaning is the surest way for an old piece of data to be read as something completely
/// different: 0x05 (`inbox`, replaced by the virtual queue), 0x0A (`deployments`, now in
/// the bucket — `DeploymentStore`), 0x11 (`vqueues`, now [`Index::VQueue`]), 0x13
/// (`outboxseq`, now [`Index::Outbox`]), 0x14.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Table {
    InvocationStatus = 0x01,
    Journal = 0x02,
    State = 0x03,
    /// A timer of an invocation: `id ‖ completion → due_ms ‖ payload`.
    ///
    /// **An entity row, not an index**: the timer's due time lives only here. The due-time
    /// ordering the timer loop needs is [`Index::Timer`], rebuildable from this table.
    Timers = 0x04,
    Outbox = 0x06,
    /// A scheduled re-run of an invocation: `id → due_ms ‖ attempt ‖ journal_len`.
    ///
    /// **A separate table, not reusing `timers`.** The temptation is to stuff it into
    /// `timers` with `completion_id = 0` as a marker. But completion ids are allocated by the
    /// *SDK*, and the runtime has no right to dictate that an SDK must not use 0 — an SDK
    /// that chooses differently turns a retry into a notification for a completion that
    /// does not exist. One table code byte is far cheaper than that class of bug.
    ///
    /// An invocation has **one** scheduled retry: keyed by id, rescheduling overwrites.
    Retries = 0x07,
    Idempotency = 0x08,
    Attachments = 0x09,
    /// Messages received from another partition, **not yet processed**.
    ///
    /// Why this must be a state table and not a direct function call: partition A and
    /// partition B are two different writers, possibly on two different nodes, so A cannot
    /// write into B's state. A sends a message, B **writes it down and only then acks**, and
    /// processes it at its own pace.
    ///
    /// Two properties fall out of that, both mandatory:
    ///
    /// 1. **INV-3.** An acked message is pending work of B; it must be in B's state, not in
    ///    anyone's RAM.
    /// 2. **No deadlock.** If B processed *synchronously* while A was waiting, a call cycle
    ///    A→B→A would lock both engine threads solid. Acking early and processing later
    ///    removes that possibility entirely.
    Inbound = 0x0B,
    /// Durable workflow promise: `(service, object_key, name) → result`.
    Promises = 0x0C,
    /// Waiters on a promise that has not completed.
    PromiseWaiters = 0x0D,
    /// A scheduled cleanup of a finished invocation (retention): `id → due_ms`.
    ///
    /// The same reason to exist as `Retries`: a timed job of the *runtime* must not borrow
    /// the `timers` table with a completion id as a marker — completion ids are allocated by
    /// the SDK. And it must be a state table (INV-3): "will clean up X at T" is pending work;
    /// kept in RAM, nobody cleans up after a failover, and the bucket grows with *history*
    /// instead of with load — exactly the kind of failure that has no error to notice.
    Purges = 0x0E,
    /// One-way calls NOT yet due (`OneWayCall.invoke_time`):
    /// `caller ‖ completion → due_ms ‖ outbox row`.
    ///
    /// Not in `outbox`: far-future rows at the head of the outbox mean the due rows behind
    /// them are never seen. When the time comes (per [`Index::Delayed`]), the row is MOVED to
    /// the outbox in one commit and then takes the normal send path.
    ///
    /// Not in `timers` for the reason given at `Retries`: the value here is an outbox row,
    /// not a completion for the SDK.
    Delayed = 0x0F,
    /// Signals waiting to be sent (`SendSignal`, 0x0410).
    ///
    /// Not in `outbox` because of the key: the outbox is keyed by `(invocation, completion
    /// id)`, while `SendSignal` **has no** completion id — it is not completable. The
    /// deterministic identity of a signal send is `(sender, entry index of the command in
    /// the sender's journal)`. Stuffing the entry index into the outbox's completion id
    /// column invites collisions: those two number spaces are unrelated, and one row
    /// overwriting another is a signal vanishing without a trace.
    SignalOutbox = 0x10,
    /// `vqidx | invocation_id[24] → (stage[1], seq[8], queue_key)`.
    ///
    /// An invocation's place in the virtual queue — an entity row; the ordered queue is
    /// [`Index::VQueue`], rebuilt from this table. The reverse path, and a **hot** one:
    /// every event of an invocation (suspended, finished, failed, cancelled) needs to find
    /// its vqueue entry with only the id in hand.
    VQueueIndex = 0x12,
    /// The deployment an invocation that **has not finished** is pinned to:
    /// `id → deployment id`.
    ///
    /// A hot copy of the `cairnflow-deployment` header in entry 0 — entry 0 lives in the bulk
    /// region, while the derived region is only rebuilt from the hot region. Written in the
    /// commit that creates the invocation, deleted in the commit that finishes it; the source
    /// of [`Index::Deployment`], which answers "who is still running on this deployment".
    Pins = 0x15,
    /// `id → len ‖ hash[32]` — the head of the journal hash chain: the hash of the last
    /// entry written, and the number of entries the chain covers. Only present for
    /// invocations of tenants with hash chains enabled; updated **in the same commit** as
    /// every journal write, deleted in the cleanup commit. Journal entries keep their
    /// layout — the chain sits next to them, not inside them.
    Chain = 0x16,
    /// `id → code ‖ attempt ‖ at_ms ‖ message` — the error of the **most recent** failed
    /// attempt: diagnostic data for the operator (`paused` 575 says why), **not** lifecycle
    /// state — the core does not read it. Written in the commit that ends a turn scheduling
    /// a retry, deleted in the cleanup commit. **Lenient read** (D27, no `DATA_FORMAT`
    /// bump): absent = no failed attempt recorded yet; a broken value = absent; an old
    /// binary that does not know this table ignores it (rebuilding the derived region does
    /// not read it).
    LastFailure = 0x17,
    /// Chunk rows of **durable streams**:
    /// `id ‖ len ‖ name ‖ first_offset(u64 BE) → chunk` ([`StreamChunk`]).
    ///
    /// Bulk region: many bytes, and rebuilding the derived region does not need to read
    /// them. The owner is the pk in the id — they follow the range on a split, and are
    /// deleted in `Engine::purge` together with the journal. Keyed by the chunk's **first
    /// offset**, so writing the same chunk again (an earlier commit failed, carried over to a
    /// later commit) overwrites rather than duplicates. A binary that does not know this code
    /// leaks rows on purge — the gate [`crate::migrate::STREAMS_TABLE`] blocks it.
    Streams = 0x18,
    /// Artifacts of an invocation:
    /// `id ‖ sha256[32] → v1 ‖ epoch u64 BE ‖ varint size ‖ varint len ‖
    /// content-type` ([`ArtifactRow`]).
    ///
    /// The row is **the truth** that object `artifacts/<tenant>/<id>/<epoch>/<sha>` belongs
    /// to a live invocation: the artifact read route looks it up, and the artifact sweep loop
    /// only holds in place (trash) objects that no longer have a row matching `(id, epoch)`.
    /// Every row of an id carries **the same** epoch — the epoch is allocated lazily at the
    /// first artifact, from `entropy`, and never reused, so an id recreated after a purge has
    /// a different epoch. Hot region (small), deleted in `Engine::purge` together with the
    /// journal. The same gate as [`Table::Streams`] ([`crate::migrate::STREAMS_TABLE`]): a
    /// binary that does not know this code leaks rows.
    Artifacts = 0x19,
    /// **Server-side schedules**: `schedule id → due ‖ schedule row`.
    ///
    /// A schedule id is `schedule pk ‖ H16(tenant, scope, name)` (`cairn_store::schedule_id`)
    /// — the owner is that pk, so a schedule follows the range on split/merge like every hot
    /// row. The value starts with [`encode_due`] (the next fire time, or `u64::MAX` when
    /// paused) so that [`Index::ScheduleDue`] can be rebuilt without understanding the rest —
    /// the rest is the `cairn_engine::schedule` row, **read strictly** with a version byte.
    /// Gate [`crate::migrate::SCHEDULES_TABLE`]: a binary that does not know this code fires
    /// nothing when it leads a range with schedules.
    Schedules = 0x1A,
    /// **Immutable metadata** of an invocation:
    /// `id → v1 ‖ created_ms ‖ kind ‖ flags ‖ tenant, scope, service, handler, key
    /// ‖ TLV*` ([`crate::query_index::InvocationMeta`]). Written in the creating commit,
    /// deleted in the cleanup commit. The source of every query index
    /// ([`crate::query_index::QUERY_INDEXES`]) and of `createdAt`. A separate row, not
    /// attached to the status row: the status row is rewritten on every transition,
    /// wake-ups included.
    InvocationMeta = 0x1B,
    /// **Audit rows waiting for export**:
    /// `target entity[24] ‖ writer_epoch(u64 BE) ‖ count(u64 BE) → JSON record`.
    ///
    /// The owner is the pk of the target entity (invocation, schedule), so the row follows
    /// the data on split/merge. Written **in the same commit** as the person's action
    /// (cancel, purge, unstick, restart, signal, schedule), deleted after the object
    /// `audit/tenant/<tenant>/<date>/r-<hex key>.json` has been written and read back
    /// matching. The object name is a function of the key ⇒ exported exactly once across
    /// every leader change. Purge deletes table by table and does not touch this table: the
    /// rows survive a purge. Gate [`crate::migrate::AUDIT_TABLE`]: a binary that does not
    /// know this code does not export rows (they leak until a new binary leads the range).
    Audit = 0x1C,
    /// **Segmented state values**: the key is **exactly** that of [`Table::State`]
    /// (`object_rest ‖ user_key`, the same `owner_pk`), the value is a
    /// [`crate::value::SegmentedRow`] (read **strictly**).
    ///
    /// A separate table because a `State` row is the customer's raw bytes — there is no
    /// free byte to use as a distinguishing marker. **Invariant:** each `(object, user_key)`
    /// has at most one of the two rows; every write to one side deletes the other in the
    /// same commit. Bulk region like `State`. When this table was introduced, no product path
    /// wrote it yet; a state read that meets one of its rows errors
    /// ([`crate::StoreError::Segmented`]) rather than treating it as absent.
    SegmentedState = 0x1D,
    /// **Segment holders**: one row per (holder, segment) —
    /// `var(tenant) ‖ owner[32] ‖ holder ‖ sha[32] ‖ gen u64 BE → v1 ‖ varint
    /// seg_len` ([`value_ref`], [`crate::value::Holder`]).
    ///
    /// The row is **the truth** that object `values/<t>/<pk>/<owner>/<sha>-<gen>` still has a
    /// holder: the values sweep loop only holds in place (trash) objects for which the owner
    /// has no row carrying that `(sha, gen)`. The owner is the pk of the object (or of the id,
    /// for an unkeyed invocation) — every holder of one owner is in the same range. Written
    /// **in the same commit** as the journal entry or [`Table::SegmentedState`] row pointing
    /// at the segment; the segment is PUT before the commit (AP-1). Hot region (small).
    ValueRefs = 0x1E,
}

impl Table {
    pub const ALL: [Table; 25] = [
        Table::InvocationStatus,
        Table::Journal,
        Table::State,
        Table::Timers,
        Table::Outbox,
        Table::Retries,
        Table::Idempotency,
        Table::Attachments,
        Table::Inbound,
        Table::Promises,
        Table::PromiseWaiters,
        Table::Purges,
        Table::Delayed,
        Table::SignalOutbox,
        Table::VQueueIndex,
        Table::Pins,
        Table::Chain,
        Table::LastFailure,
        Table::Streams,
        Table::Artifacts,
        Table::Schedules,
        Table::InvocationMeta,
        Table::Audit,
        Table::SegmentedState,
        Table::ValueRefs,
    ];

    pub fn from_u8(b: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|t| *t as u8 == b)
    }
}

/// A region of the keyspace — the first byte of every key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Region {
    /// Small entities — enough to rebuild the derived region.
    Hot = 0x00,
    /// Indexes. Outside every range; rebuilt after a split/merge.
    Derived = 0x01,
    /// Entities with many bytes that rebuilding does not need to read.
    Bulk = 0x02,
}

impl Region {
    pub const ALL: [Region; 3] = [Region::Hot, Region::Derived, Region::Bulk];

    pub fn of_key(key: &[u8]) -> Option<Region> {
        Self::ALL
            .into_iter()
            .find(|r| Some(&(*r as u8)) == key.first())
    }

    /// `[region, region+1)` — every key of this region.
    pub fn bounds(self) -> (Vec<u8>, Vec<u8>) {
        (vec![self as u8], vec![self as u8 + 1])
    }
}

/// Which region an entity table lives in. **The only place** that decides — folding the
/// bulk region into the hot region (if multi-range projection had to go) means changing
/// exactly this function.
///
/// The bulk region holds the many-byte things nobody needs to rebuild the derived region:
/// the journal (~78% of the bytes per invocation in measured data, and it stays for the
/// whole retention), state, and durable stream chunks. Separating them makes rebuilding
/// proportional to the number of small rows, not to the data volume.
pub const fn region_of(table: Table) -> Region {
    match table {
        Table::Journal | Table::State | Table::SegmentedState | Table::Streams => Region::Bulk,
        _ => Region::Hot,
    }
}

/// Index codes of the derived region. Every index is **rebuildable** from the hot region
/// and is written in the same commit as its entity row. Code 0x0A used to be the legacy
/// `deployments` table — retired, not reused.
///
/// **Index rows are hints.** Users (timer fire, retry, cleanup, drain) always check that the
/// entity row still exists and matches; an index row that disagrees is skipped and cleaned
/// up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Index {
    /// `due ‖ id ‖ completion` — from [`Table::Timers`].
    Timer = 0x01,
    /// `due ‖ id` — from [`Table::Retries`].
    Retry = 0x02,
    /// `due ‖ id` — from [`Table::Purges`].
    Purge = 0x03,
    /// `due ‖ caller ‖ completion` — from [`Table::Delayed`].
    Delayed = 0x04,
    /// `seq ‖ entity key` — the production order of [`Table::Outbox`].
    ///
    /// The outbox is keyed by `(owner, invocation, completion)` — that is the **identity**,
    /// and it is the deduplication mechanism: the state machine replays tasks for every
    /// pending completion on *every* replay, so rewriting must be overwriting. The index is
    /// keyed by `seq` — **production order**, something a cursor can follow. The entity key
    /// follows `seq` intact, so the index points straight back to the main row — owner
    /// included, which cannot be derived from the id for `Reply` rows.
    Outbox = 0x05,
    /// `seq ‖ entity key` — the production order of [`Table::SignalOutbox`].
    Signal = 0x06,
    /// `seq ‖ entity key` — the receive order of [`Table::Inbound`].
    Inbound = 0x07,
    /// `queue ‖ stage ‖ seq ‖ id` — the virtual queue, from [`Table::VQueueIndex`].
    ///
    /// `queue_key` comes **before** `stage` because every hot scan is "what is left in this
    /// queue"; `id` comes last so that merging two Dbs (`seq` is a per-Db counter) does not
    /// make two rows overwrite each other.
    VQueue = 0x08,
    /// `id → status code` — from [`Table::InvocationStatus`]: `recover` and `/invocations`
    /// read here, not scanning the entity region.
    Invocations = 0x09,
    /// `deployment ‖ id` — from [`Table::Pins`]: unfinished invocations by the deployment
    /// they are pinned to. Code 0x0B, not 0x0A (retired).
    Deployment = 0x0B,
    /// `due ‖ schedule id` — from [`Table::Schedules`]. A paused schedule has a row at
    /// `due = u64::MAX`: the index is both the fire queue and the range's list of schedules
    /// (`GET /schedules`) without scanning the hot region.
    ScheduleDue = 0x0C,
    /// `audit row key` — [`Table::Audit`] rows still waiting for export. The export loop
    /// scans here instead of the whole hot region; rebuilt from the hot region after a
    /// split/merge like every index. Code `0x80`: `0x0D..=0x7F` is the range of query
    /// indexes (`query_index::QUERY_CODES`).
    Audit = 0x80,
    /// **Durable progress marks**: a single key (`0x01 ‖ 0xFD`) carrying the scan marks of
    /// the four due-ordered indexes and the three order indexes, stamped with the
    /// `writer_epoch` of the writer that wrote it. Lenient read: absent, broken, or a stamp
    /// not from the immediately preceding writer ⇒ every mark 0 (scan from the start like an
    /// old binary). Not in [`Self::REBUILT`]: rebuilding sets the marks to 0. See
    /// `writer/floors.rs`.
    Floors = 0xFD,
    /// **A reserved key, never to be read**: the barrier of `PartitionWriter::seal`. Seal
    /// writes a durable *delete* at this key before flushing, so every late write of a task
    /// cancelled at quiesce lands in the WAL before it — the flush covers everything, and
    /// the clone (F3) does not see "the WAL still has data". Only a tombstone: no value, no
    /// one scans it, not in [`Self::REBUILT`] (rebuilding deletes the whole derived region and
    /// it goes along — harmless). No table or index may take code `0xFE`.
    SealBarrier = 0xFE,
    /// The mark "the derived region matches this Db": `(range_id, gen)`.
    Ready = 0xFF,
}

impl Index {
    /// Every index ([`Self::REBUILT`] and the three range keys).
    pub const ALL: [Index; 15] = [
        Index::Timer,
        Index::Retry,
        Index::Purge,
        Index::Delayed,
        Index::Outbox,
        Index::Signal,
        Index::Inbound,
        Index::VQueue,
        Index::Invocations,
        Index::Deployment,
        Index::ScheduleDue,
        Index::Audit,
        Index::Floors,
        Index::SealBarrier,
        Index::Ready,
    ];

    pub fn from_u8(b: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|ix| *ix as u8 == b)
    }

    /// The indexes rebuildable from the hot region.
    pub const REBUILT: [Index; 12] = [
        Index::Timer,
        Index::Retry,
        Index::Purge,
        Index::Delayed,
        Index::Outbox,
        Index::Signal,
        Index::Inbound,
        Index::VQueue,
        Index::Invocations,
        Index::Deployment,
        Index::ScheduleDue,
        Index::Audit,
    ];
}

/// Where an invocation is in its queue.
///
/// Four stages, and the design decision is **two different resources with two different
/// lifecycles**: the concurrency counter is released on suspension, the virtual object's
/// EXCLUSIVE lock is not. Merging them into one reopens exactly the race the queue exists
/// to close.
///
/// | Stage | Meaning | Holds counter? | Holds VO lock? |
/// |---|---|---|---|
/// | `Inbox` | waiting for resources | no | no |
/// | `Running` | executing | **yes** | yes |
/// | `Suspended` | suspended waiting for a timer/completion | no | **yes** |
/// | `Paused` | paused by the operator | no | yes |
///
/// There is no `Input` stage (one that would separate "received" from "enqueued"): in cairn
/// `Engine::create` already writes journal entry 0 + the status in one commit, and the
/// vqueue entry going into that same commit *is* the queue. Another stage would only add
/// another commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Stage {
    Inbox = 0x01,
    Running = 0x02,
    Suspended = 0x03,
    Paused = 0x04,
}

impl Stage {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Inbox),
            0x02 => Some(Self::Running),
            0x03 => Some(Self::Suspended),
            0x04 => Some(Self::Paused),
            _ => None,
        }
    }

    /// Whether this stage holds the concurrency counter.
    pub fn holds_counter(self) -> bool {
        matches!(self, Self::Running)
    }

    /// Whether this stage holds the virtual object's EXCLUSIVE lock.
    ///
    /// `Inbox` is the only stage that does not — that is its very definition.
    pub fn holds_lock(self) -> bool {
        !matches!(self, Self::Inbox)
    }
}

// ─────────────────────────────────────────────────────── the two builders

/// `region ‖ owner_pk ‖ table`. Private: apart from the build/split functions below, no
/// function may know this number.
const ENTITY_HEADER_LEN: usize = 1 + 8 + 1;

/// `region ‖ index`.
const INDEX_HEADER_LEN: usize = 1 + 1;

/// **The only builder** of every entity row key:
/// `region_of(table) ‖ owner_pk(u64 BE) ‖ table ‖ rest`.
///
/// `owner_pk` is the partition key of the local entity that owns the row — the row follows
/// the range of this pk when the partition is split.
pub fn entity(owner_pk: PartitionKey, table: Table, rest: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(ENTITY_HEADER_LEN + rest.len());
    b.push(region_of(table) as u8);
    b.extend_from_slice(&owner_pk.0.to_be_bytes());
    b.push(table as u8);
    b.extend_from_slice(rest);
    b
}

/// **The only builder** of every derived region key: `0x01 ‖ index ‖ rest`.
pub fn index(ix: Index, rest: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(INDEX_HEADER_LEN + rest.len());
    b.push(Region::Derived as u8);
    b.push(ix as u8);
    b.extend_from_slice(rest);
    b
}

/// The key builder of **query indexes**: `0x01 ‖ code ‖ rest`, with `code` in
/// [`crate::query_index::QUERY_CODES`] — a code allocated by the descriptor table
/// [`crate::query_index::QUERY_INDEXES`], not an [`Index`]. A code outside the range is a
/// programming error (a panic in tests, never caused by data).
pub fn query_index(code: u8, rest: &[u8]) -> Vec<u8> {
    assert!(
        crate::query_index::QUERY_CODES.contains(&code)
            || code == *crate::query_index::QUERY_CODES.end() + 1,
        "query index code {code:#x} out of range"
    );
    let mut b = Vec::with_capacity(INDEX_HEADER_LEN + rest.len());
    b.push(Region::Derived as u8);
    b.push(code);
    b.extend_from_slice(rest);
    b
}

/// The part after the key header of a [`query_index`] key with code `code`.
pub fn query_index_body(key: &[u8], code: u8) -> Option<&[u8]> {
    let ok = key.len() >= INDEX_HEADER_LEN && key[0] == Region::Derived as u8 && key[1] == code;
    ok.then(|| &key[INDEX_HEADER_LEN..])
}

/// The key of the range's query index status mark (`0x01 ‖ 0xFC`).
pub fn query_index_marker() -> Vec<u8> {
    vec![Region::Derived as u8, crate::query_index::QUERY_READY]
}

/// The part after the key header of an [`entity`] key belonging to `table`. `None` = a key
/// of another table, or not an entity key.
pub fn entity_body(key: &[u8], table: Table) -> Option<&[u8]> {
    let ok = key.len() >= ENTITY_HEADER_LEN
        && key[0] == region_of(table) as u8
        && key[ENTITY_HEADER_LEN - 1] == table as u8;
    ok.then(|| &key[ENTITY_HEADER_LEN..])
}

/// Whether the key belongs to a table whose rows the drainer **sends** to another range —
/// the outbox or the signal outbox (LF-1): their entity rows or their order index rows.
pub fn is_send_key(key: &[u8]) -> bool {
    entity_body(key, Table::Outbox).is_some()
        || entity_body(key, Table::SignalOutbox).is_some()
        || index_body(key, Index::Outbox).is_some()
        || index_body(key, Index::Signal).is_some()
}

/// The part after the key header of an [`index`] key belonging to `ix`.
pub fn index_body(key: &[u8], ix: Index) -> Option<&[u8]> {
    let ok = key.len() >= INDEX_HEADER_LEN && key[0] == Region::Derived as u8 && key[1] == ix as u8;
    ok.then(|| &key[INDEX_HEADER_LEN..])
}

/// The table of an entity key. `None` for derived region keys.
pub fn table_of(key: &[u8]) -> Option<Table> {
    let region = Region::of_key(key)?;
    let table = Table::from_u8(*key.get(ENTITY_HEADER_LEN - 1)?)?;
    (region != Region::Derived && region_of(table) == region).then_some(table)
}

/// The owner of an entity key. `None` for derived region keys.
pub fn owner_of(key: &[u8]) -> Option<PartitionKey> {
    table_of(key)?;
    let pk = key.get(1..9)?;
    Some(PartitionKey(u64::from_be_bytes(pk.try_into().ok()?)))
}

/// The byte ranges held by a partition owning the pk range `[start, end]` (both ends
/// **inclusive**, exactly the range table's `RangeEntry`): one range per entity region,
/// `[lo, hi)`.
///
/// This is what is handed to SlateDB's projection on split/merge. The derived region is
/// absent — deliberately: it lies outside every range.
pub fn owned_ranges(start: PartitionKey, end: PartitionKey) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut regions: Vec<Region> = Table::ALL.into_iter().map(region_of).collect();
    regions.sort();
    regions.dedup();
    regions
        .into_iter()
        .map(|r| {
            let at = |pk: u64| {
                let mut v = vec![r as u8];
                v.extend_from_slice(&pk.to_be_bytes());
                v
            };
            let hi = match end.0.checked_add(1) {
                Some(next) => at(next),
                None => vec![r as u8 + 1],
            };
            (at(start.0), hi)
        })
        .collect()
}

/// Prefix to scan a whole index.
pub fn index_prefix(ix: Index) -> Vec<u8> {
    index(ix, &[])
}

/// `[0x01 ‖ ix, 0x01 ‖ ix+1)` — every row of an index.
pub fn index_bounds(ix: Index) -> (Vec<u8>, Vec<u8>) {
    let lo = index_prefix(ix);
    let mut hi = lo.clone();
    match hi.last_mut() {
        Some(b) if *b < u8::MAX => *b += 1,
        // `Ready` (0xFF): up to the end of the derived region.
        _ => hi = Region::Bulk.bounds().0,
    }
    (lo, hi)
}

/// The owner of rows that belong to the invocation at the start of the key itself (status,
/// journal, attachments, vqueue index, signal outbox, timer, retry, scheduled purge, delayed
/// call): the pk is right in the first 8 bytes of the id.
fn own(id: &InvocationId) -> PartitionKey {
    partition_key_of(id)
}

/// A value carrying `due_ms` at the start — the entity rows of the timed tables.
pub fn encode_due(due_ms: u64, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + payload.len());
    v.extend_from_slice(&due_ms.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

/// Inverse of [`encode_due`].
pub fn decode_due(raw: &[u8]) -> Option<(u64, &[u8])> {
    let mut c = Cursor(raw);
    Some((c.u64()?, c.rest()))
}

/// A value carrying `seq` at the start — entity rows with an order index (outbox, signal,
/// inbound). `seq` is in the entity row so that rebuilding keeps the production order, and
/// to tell a still-valid index row apart from an old row of an overwrite.
pub fn encode_seq(seq: u64, payload: &[u8]) -> Vec<u8> {
    encode_due(seq, payload)
}

/// Inverse of [`encode_seq`].
pub fn decode_seq(raw: &[u8]) -> Option<(u64, &[u8])> {
    decode_due(raw)
}

/// Reads the rest of a key sequentially — instead of any hard-coded offset.
///
/// Every read returns `None` when bytes are missing: broken data does happen, and a panic
/// on a scan path kills the engine thread.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(n)?;
        self.0 = tail;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn id(&mut self) -> Option<InvocationId> {
        self.take(ID_LEN)?.try_into().ok()
    }

    /// A segment with a `u16` length prefix — the inverse of [`put_var`].
    fn var(&mut self) -> Option<&'a [u8]> {
        let n = u16::from_be_bytes(self.take(2)?.try_into().ok()?) as usize;
        self.take(n)
    }

    fn rest(self) -> &'a [u8] {
        self.0
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The maximum length of a key, including the table code and every prefix.
///
/// This number is **not our choice** — SlateDB does `assert!(key.len() <= u16::MAX)` right
/// inside `WriteBatch`. Exceeding the cap is a *panic*, and a panic on the write path kills
/// the engine thread too, i.e. the whole partition stops because of one long object key.
///
/// It also covers a second hazard: the length prefix of [`put_var`] is a `u16`, so a segment
/// longer than 65535 would wrap around and make two different segment sets encode to the
/// same key. But the bytes of that segment are *inside* the key, so a segment over the cap
/// ⇒ a key over the cap. One check is enough for both.
///
/// Object keys and idempotency keys are chosen externally, so this is not a theoretical
/// risk. [`crate::Mutations`] blocks it on the write path, `PartitionWriter` and
/// `PartitionReader` block it on the read path.
pub const MAX_KEY: usize = u16::MAX as usize;

/// `Err(len)` if the key exceeds [`MAX_KEY`].
pub fn check_key(key: &[u8]) -> std::result::Result<(), usize> {
    if key.len() <= MAX_KEY {
        Ok(())
    } else {
        Err(key.len())
    }
}

/// Writes a variable-length segment with a length prefix.
///
/// Not order-preserving globally — and it does not need to be: every scan of ours is a
/// *prefix* scan (every state of an object, every journal entry of an invocation), so all
/// that is needed is that keys of the same group share a byte prefix, and that the encoding
/// is unambiguous. A length prefix gives both.
///
/// Deliberately **no** length assert here. This function only builds bytes; it does not
/// know whether the key will be written or only used as a scan prefix, and it has no way to
/// return an error. The check lives in [`check_key`], which runs on the *fully built* key —
/// the only place that knows enough to decide, and the place that has a `Result` to report
/// it.
fn put_var(b: &mut BytesMut, s: &[u8]) {
    b.put_u16(s.len() as u16);
    b.put_slice(s);
}

/// Scope prefix of the four identity tables: `len(scope) ‖ scope`.
///
/// **`len = 0` for an unscoped invocation**, so the old layout (before flow control) is
/// exactly a special case of this layout — but the bytes differ, and that is why changing
/// the key of four tables is a breaking change. It was done *now* because cairn had no
/// production data yet; doing it early is cheap, retrofitting is expensive.
///
/// Scope is capped at [`crate::MAX_COMPONENT`] bytes so this part is never what pushes a
/// key over [`MAX_KEY`] — but [`check_key`] still runs, since it does not need to know that
/// to be correct.
fn put_scope(b: &mut BytesMut, scope: Option<&Scope>) {
    put_var(b, scope.map(|s| s.as_str()).unwrap_or("").as_bytes());
}

/// Tenant prefix: `len(tenant) ‖ tenant`, **before the scope** in every key with
/// user-chosen names. Tenant `default` is written empty (`len = 0`) — it is always present,
/// so the layout is never ambiguous between a key with a tenant and a key without one.
///
/// The owner (the partition key at the start of the key) already differs between two
/// tenants ([`crate::partition_key_for_target`]); the tenant still goes into the key body
/// because two partition keys *can* collide (a 64-bit hash), and a collision must not turn
/// into a cross-tenant read.
fn put_tenant(b: &mut BytesMut, tenant: &TenantId) {
    put_var(
        b,
        if tenant.is_default() {
            ""
        } else {
            tenant.as_str()
        }
        .as_bytes(),
    );
}

// ─────────────────────────────────────────────────────── invocation_status

pub fn invocation_status(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::InvocationStatus, id)
}

/// The id from a status key.
pub fn parse_invocation_status(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::InvocationStatus)?).id()
}

/// A row of [`Index::Invocations`]: `id → status code`.
///
/// The status code is **the first byte of the status value** — the storage layer does not
/// know the status encoding, it only copies that byte; the meaning belongs to the engine.
pub fn ix_invocation(id: &InvocationId) -> Vec<u8> {
    index(Index::Invocations, id)
}

pub fn parse_ix_invocation(key: &[u8]) -> Option<InvocationId> {
    Cursor(index_body(key, Index::Invocations)?).id()
}

// ─────────────────────────────────────────────────────── pins (2.4)

/// A row of [`Table::Pins`]: which deployment invocation `id` is pinned to.
pub fn pin(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Pins, id)
}

pub fn parse_pin(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::Pins)?).id()
}

/// The journal hash chain head row of invocation `id` — see [`Table::Chain`].
pub fn chain(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Chain, id)
}

pub fn parse_chain(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::Chain)?).id()
}

/// The value of a chain row: `len u32 ‖ hash[32]`.
pub fn encode_chain(len: u32, hash: &[u8; 32]) -> Vec<u8> {
    let mut b = Vec::with_capacity(36);
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(hash);
    b
}

pub fn decode_chain(raw: &[u8]) -> Option<(u32, [u8; 32])> {
    if raw.len() != 36 {
        return None;
    }
    let len = u32::from_be_bytes(raw[..4].try_into().ok()?);
    Some((len, raw[4..].try_into().ok()?))
}

// ─────────────────────────────────────────────────────── failed attempts

/// The row holding the error of `id`'s most recent failed attempt — see
/// [`Table::LastFailure`].
pub fn last_failure(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::LastFailure, id)
}

pub fn parse_last_failure(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::LastFailure)?).id()
}

/// The byte cap of `message` in [`LastFailure`]: the row goes through the WAL on every
/// failed attempt, so however long the SDK's message is, only this much is kept (cut at a
/// character boundary).
pub const LAST_FAILURE_MESSAGE_MAX: usize = 1024;

/// The error of one failed attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastFailure {
    /// The code the SDK reported (HTTP-like), or `500` for an infrastructure error of the
    /// attempt.
    pub code: u32,
    pub message: String,
    /// `ctx.attempt` of that attempt (0 = the first).
    pub attempt: u32,
    /// Wall-clock time when the attempt ended.
    pub at_ms: u64,
}

impl LastFailure {
    /// An error with `message` cut down to [`LAST_FAILURE_MESSAGE_MAX`] bytes.
    pub fn new(code: u32, message: &str, attempt: u32, at_ms: u64) -> Self {
        let mut end = message.len().min(LAST_FAILURE_MESSAGE_MAX);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            code,
            message: message[..end].to_string(),
            attempt,
            at_ms,
        }
    }

    /// `code u32 ‖ attempt u32 ‖ at_ms u64 ‖ message` (big-endian, message is UTF-8 to the
    /// end of the value).
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(16 + self.message.len());
        b.extend_from_slice(&self.code.to_be_bytes());
        b.extend_from_slice(&self.attempt.to_be_bytes());
        b.extend_from_slice(&self.at_ms.to_be_bytes());
        b.extend_from_slice(self.message.as_bytes());
        b
    }

    /// Lenient read: shorter than 16 bytes ⇒ `None` (as if absent); a message that is not
    /// UTF-8 has its broken characters replaced.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < 16 {
            return None;
        }
        Some(Self {
            code: u32::from_be_bytes(raw[..4].try_into().ok()?),
            attempt: u32::from_be_bytes(raw[4..8].try_into().ok()?),
            at_ms: u64::from_be_bytes(raw[8..16].try_into().ok()?),
            message: String::from_utf8_lossy(&raw[16..]).into_owned(),
        })
    }
}

/// A row of [`Index::Deployment`]: `len(deployment)‖deployment ‖ id`.
pub fn ix_deployment(deployment: &str, id: &InvocationId) -> Vec<u8> {
    let mut b = BytesMut::with_capacity(8 + deployment.len() + ID_LEN);
    put_var(&mut b, deployment.as_bytes());
    b.put_slice(id);
    index(Index::Deployment, &b)
}

/// Prefix to scan every invocation pinned to `deployment`.
pub fn ix_deployment_prefix(deployment: &str) -> Vec<u8> {
    let mut b = BytesMut::with_capacity(8 + deployment.len());
    put_var(&mut b, deployment.as_bytes());
    index(Index::Deployment, &b)
}

/// `(deployment, id)` from an [`Index::Deployment`] row.
pub fn parse_ix_deployment(key: &[u8]) -> Option<(String, InvocationId)> {
    let mut c = Cursor(index_body(key, Index::Deployment)?);
    let d = std::str::from_utf8(c.var()?).ok()?.to_string();
    Some((d, c.id()?))
}

// ─────────────────────────────────────────────────────── journal

/// `journal | invocation_id | command_index`.
///
/// `command_index` is a big-endian u32 so a prefix scan per invocation returns entries
/// **in write order** — exactly the order the service protocol's REPLAY phase needs.
pub fn journal_entry(id: &InvocationId, command_index: u32) -> Vec<u8> {
    let mut r = BytesMut::with_capacity(ID_LEN + 4);
    r.put_slice(id);
    r.put_u32(command_index);
    entity(own(id), Table::Journal, &r)
}

/// Cuts a journal page: gathers entries up to `max_entries` or — after the first entry —
/// until the total bytes reach `max_bytes`; the first entry **not** taken is the next index.
/// Shared by the leader ([`crate::PartitionWriter`]) and followers
/// ([`crate::PartitionReader`]).
pub struct JournalCut {
    rows: Vec<(u32, bytes::Bytes)>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
    next: Option<u32>,
}

impl JournalCut {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            max_entries: max_entries.max(1),
            max_bytes,
            next: None,
        }
    }

    /// Takes entry `index`; `true` = the page is full, stop scanning (this entry is the next
    /// index, not part of the page).
    pub fn push(&mut self, index: u32, value: bytes::Bytes) -> bool {
        let full = self.rows.len() >= self.max_entries
            || (!self.rows.is_empty() && self.bytes >= self.max_bytes);
        if full {
            self.next = Some(index);
            return true;
        }
        self.bytes += value.len();
        self.rows.push((index, value));
        false
    }

    pub fn finish(self) -> (Vec<(u32, bytes::Bytes)>, Option<u32>) {
        (self.rows, self.next)
    }
}

/// Prefix to scan every journal entry of an invocation.
pub fn journal_prefix(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Journal, id)
}

// ─────────────────────────────────────────────────────── durable streams

/// The byte cap of a stream name in the key (one length byte).
pub const STREAM_NAME_MAX: usize = u8::MAX as usize;

/// A chunk row `(id, name, first_offset)` — see [`Table::Streams`].
///
/// A name longer than [`STREAM_NAME_MAX`] is cut there — callers check the name first (the
/// stream cap is 128 bytes); the cut only ensures an ambiguous key is never built.
pub fn stream_chunk(id: &InvocationId, name: &str, first_offset: u64) -> Vec<u8> {
    let mut r = stream_rest(id, name);
    r.put_u64(first_offset);
    entity(own(id), Table::Streams, &r)
}

/// Prefix of every chunk of **one** stream — `first_offset` is big-endian so a prefix scan
/// returns chunks in offset order.
pub fn stream_prefix(id: &InvocationId, name: &str) -> Vec<u8> {
    entity(own(id), Table::Streams, &stream_rest(id, name))
}

/// Prefix of every chunk of every durable stream of an invocation (purge).
pub fn streams_prefix(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Streams, id)
}

fn stream_rest(id: &InvocationId, name: &str) -> BytesMut {
    let name = &name.as_bytes()[..name.len().min(STREAM_NAME_MAX)];
    let mut r = BytesMut::with_capacity(ID_LEN + 1 + name.len() + 8);
    r.put_slice(id);
    r.put_u8(name.len() as u8);
    r.put_slice(name);
    r
}

/// `(id, name, first_offset)` from a [`stream_chunk`] key. A name that is not UTF-8
/// ⇒ `None`.
pub fn parse_stream_chunk(key: &[u8]) -> Option<(InvocationId, String, u64)> {
    let mut c = Cursor(entity_body(key, Table::Streams)?);
    let id = c.id()?;
    let n = c.u8()? as usize;
    let name = std::str::from_utf8(c.take(n)?).ok()?.to_string();
    let first = c.u64()?;
    c.0.is_empty().then_some((id, name, first))
}

/// Version of the value layout of [`StreamChunk`]. Lenient read: another version ⇒ treated
/// as a broken chunk (the reader sees a `gap`), no panic, no error for the whole stream.
const STREAM_CHUNK_V1: u8 = 1;

/// One record in a chunk. Identity `(step, attempt, k)` as in `StreamHub` (AP-11); the
/// offset is allocated by the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkRecord {
    pub offset: u64,
    pub step: u32,
    pub attempt: u32,
    pub k: u32,
    pub body: ChunkBody,
}

/// Record content — the same three kinds as the server's `streams::Body`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChunkBody {
    Data(bytes::Bytes),
    Gap { dropped: u64 },
    Superseded { step: u32, attempt: u32 },
}

/// The value of a chunk row: consecutive records from `first_offset`, plus two counters
/// **cumulative up to the end of this chunk** — payload bytes received and records dropped
/// because of the cap — so that a new leader term can rebuild the invocation's cap (across
/// every attempt) from the last chunk without scanning the whole stream.
///
/// ```text
/// v1 ‖ varint total ‖ varint dropped ‖ { kind ‖ varint Δoffset ‖ varint step
///    ‖ varint attempt ‖ varint k ‖ body }*
/// body: Data ⇒ varint len ‖ bytes; Gap ⇒ varint dropped; Superseded ⇒ varint
///       step ‖ varint attempt
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamChunk {
    pub total_bytes: u64,
    pub dropped: u64,
    pub records: Vec<ChunkRecord>,
}

impl StreamChunk {
    /// Encodes with `first` as the offset of the first record (matching the key). Records
    /// must be strictly increasing by offset — the caller (the hub) allocates offsets that
    /// way.
    pub fn encode(&self, first: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            16 + self
                .records
                .iter()
                .map(|r| match &r.body {
                    ChunkBody::Data(b) => b.len() + 12,
                    _ => 12,
                })
                .sum::<usize>(),
        );
        out.push(STREAM_CHUNK_V1);
        put_varint(&mut out, self.total_bytes);
        put_varint(&mut out, self.dropped);
        let mut at = first;
        for r in &self.records {
            out.push(match &r.body {
                ChunkBody::Data(_) => 0,
                ChunkBody::Gap { .. } => 1,
                ChunkBody::Superseded { .. } => 2,
            });
            put_varint(&mut out, r.offset.saturating_sub(at));
            put_varint(&mut out, u64::from(r.step));
            put_varint(&mut out, u64::from(r.attempt));
            put_varint(&mut out, u64::from(r.k));
            match &r.body {
                ChunkBody::Data(b) => {
                    put_varint(&mut out, b.len() as u64);
                    out.extend_from_slice(b);
                }
                ChunkBody::Gap { dropped } => put_varint(&mut out, *dropped),
                ChunkBody::Superseded { step, attempt } => {
                    put_varint(&mut out, u64::from(*step));
                    put_varint(&mut out, u64::from(*attempt));
                }
            }
            at = r.offset;
        }
        out
    }

    /// Inverse of [`Self::encode`]. Broken or an unknown version ⇒ `None` (lenient read).
    pub fn decode(first: u64, raw: &[u8]) -> Option<StreamChunk> {
        let mut c = Varints(raw);
        if c.byte()? != STREAM_CHUNK_V1 {
            return None;
        }
        let mut out = StreamChunk {
            total_bytes: c.varint()?,
            dropped: c.varint()?,
            records: Vec::new(),
        };
        let mut at = first;
        while !c.0.is_empty() {
            let kind = c.byte()?;
            let offset = at.checked_add(c.varint()?)?;
            let step = u32::try_from(c.varint()?).ok()?;
            let attempt = u32::try_from(c.varint()?).ok()?;
            let k = u32::try_from(c.varint()?).ok()?;
            let body = match kind {
                0 => {
                    let n = usize::try_from(c.varint()?).ok()?;
                    ChunkBody::Data(bytes::Bytes::copy_from_slice(c.take(n)?))
                }
                1 => ChunkBody::Gap {
                    dropped: c.varint()?,
                },
                2 => ChunkBody::Superseded {
                    step: u32::try_from(c.varint()?).ok()?,
                    attempt: u32::try_from(c.varint()?).ok()?,
                },
                _ => return None,
            };
            out.records.push(ChunkRecord {
                offset,
                step,
                attempt,
                k,
                body,
            });
            at = offset;
        }
        Some(out)
    }

    /// The next offset after this chunk (`first` if empty).
    pub fn next_offset(&self, first: u64) -> u64 {
        self.records.last().map_or(first, |r| r.offset + 1)
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Reads a chunk value sequentially; every read returns `None` when bytes are missing.
struct Varints<'a>(&'a [u8]);

impl<'a> Varints<'a> {
    fn byte(&mut self) -> Option<u8> {
        let (b, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(*b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(n)?;
        self.0 = tail;
        Some(head)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= u64::from(b & 0x7f).checked_shl(shift)?;
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    }
}

// ─────────────────────────────────────────────────────── artifact

/// An artifact row `(id, sha)` — see [`Table::Artifacts`].
pub fn artifact(id: &InvocationId, sha: &[u8; 32]) -> Vec<u8> {
    let mut r = Vec::with_capacity(ID_LEN + 32);
    r.extend_from_slice(id);
    r.extend_from_slice(sha);
    entity(own(id), Table::Artifacts, &r)
}

/// Prefix of every artifact row of an invocation.
pub fn artifacts_prefix(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Artifacts, id)
}

/// `(id, sha)` from an [`artifact`] key.
pub fn parse_artifact(key: &[u8]) -> Option<(InvocationId, [u8; 32])> {
    let mut c = Cursor(entity_body(key, Table::Artifacts)?);
    let id = c.id()?;
    let sha: [u8; 32] = c.take(32)?.try_into().ok()?;
    c.0.is_empty().then_some((id, sha))
}

const ARTIFACT_ROW_V1: u8 = 1;

/// The value of an artifact row. Lenient read: broken or an unknown version ⇒ `None` — the
/// reader treats the row as **present** (keeping the object), not as absent; see
/// [`crate::proof::DeleteProof::purged_invocation`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRow {
    pub epoch: u64,
    pub size: u64,
    pub content_type: String,
}

impl ArtifactRow {
    pub fn encode(&self) -> Vec<u8> {
        let ct = self.content_type.as_bytes();
        let mut out = Vec::with_capacity(1 + 8 + 10 + 10 + ct.len());
        out.push(ARTIFACT_ROW_V1);
        out.extend_from_slice(&self.epoch.to_be_bytes());
        put_varint(&mut out, self.size);
        put_varint(&mut out, ct.len() as u64);
        out.extend_from_slice(ct);
        out
    }

    pub fn decode(raw: &[u8]) -> Option<ArtifactRow> {
        let mut c = Varints(raw);
        if c.byte()? != ARTIFACT_ROW_V1 {
            return None;
        }
        let epoch = u64::from_be_bytes(c.take(8)?.try_into().ok()?);
        let size = c.varint()?;
        let n = usize::try_from(c.varint()?).ok()?;
        let content_type = std::str::from_utf8(c.take(n)?).ok()?.to_string();
        c.0.is_empty().then_some(ArtifactRow {
            epoch,
            size,
            content_type,
        })
    }
}

// ─────────────────────────────────────────────────────── state

/// Virtual object state: `state | scope | service | object_key | user_key`.
///
/// Scope comes first because it is **part of the object's identity**, not an extra label:
/// `Cart/a` in scope `bob` is a different instance from unscoped `Cart/a` — separate state,
/// separate queue, separate promises.
///
/// `owner_pk` = `partition_key_for_target(tenant, scope, service, object_key, None)` — see
/// [`object_owner`].
pub fn state(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    user_key: &[u8],
) -> Vec<u8> {
    let mut r = object_rest(tenant, scope, service, object_key);
    r.put_slice(user_key);
    entity(owner_pk, Table::State, &r)
}

/// Prefix to scan every state key of a virtual object.
pub fn state_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> Vec<u8> {
    entity(
        owner_pk,
        Table::State,
        &object_rest(tenant, scope, service, object_key),
    )
}

/// `user_key` from a state key.
pub fn parse_state_user_key(key: &[u8]) -> Option<&[u8]> {
    let mut c = Cursor(entity_body(key, Table::State)?);
    skip_object(&mut c)?;
    Some(c.rest())
}

/// The key of a [`Table::SegmentedState`] row — the same body as [`state`].
pub fn segmented_state(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    user_key: &[u8],
) -> Vec<u8> {
    let mut r = object_rest(tenant, scope, service, object_key);
    r.put_slice(user_key);
    entity(owner_pk, Table::SegmentedState, &r)
}

/// Prefix to scan every [`Table::SegmentedState`] row of an object.
pub fn segmented_state_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> Vec<u8> {
    entity(
        owner_pk,
        Table::SegmentedState,
        &object_rest(tenant, scope, service, object_key),
    )
}

/// `user_key` from a [`Table::SegmentedState`] key.
pub fn parse_segmented_state_user_key(key: &[u8]) -> Option<&[u8]> {
    let mut c = Cursor(entity_body(key, Table::SegmentedState)?);
    skip_object(&mut c)?;
    Some(c.rest())
}

/// `(tenant, object_rest, user_key)` from a [`Table::SegmentedState`] key — `object_rest` is
/// the input of [`crate::value::Owner::of_object_rest`].
pub fn parse_segmented_state(key: &[u8]) -> Option<(TenantId, &[u8], &[u8])> {
    let body = entity_body(key, Table::SegmentedState)?;
    let tenant = lead_tenant(body)?;
    let mut c = Cursor(body);
    skip_object(&mut c)?;
    let user_key = c.rest();
    let rest = &body[..body.len() - user_key.len()];
    Some((tenant, rest, user_key))
}

/// The identity body of an object (the common part of the State key) — the input of
/// [`crate::value::Owner::of_object_rest`].
pub fn object_rest_bytes(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> Vec<u8> {
    object_rest(tenant, scope, service, object_key).to_vec()
}

fn value_refs_body(tenant: &TenantId, owner: &crate::value::Owner) -> BytesMut {
    let mut b = BytesMut::with_capacity(96);
    put_tenant(&mut b, tenant);
    b.put_slice(&owner.0);
    b
}

fn put_holder(b: &mut BytesMut, h: &crate::value::Holder) {
    use crate::value::Holder;
    match h {
        Holder::State(k) => {
            b.put_u8(Holder::STATE);
            put_var(b, k);
        }
        Holder::Invocation(id) => {
            b.put_u8(Holder::INVOCATION);
            b.put_slice(id);
        }
        Holder::Promise(name) => {
            b.put_u8(Holder::PROMISE);
            put_var(b, name.as_bytes());
        }
    }
}

/// The key of a [`Table::ValueRefs`] row: holder `holder` holds segment `(sha, gen)` of
/// `owner`.
pub fn value_ref(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    owner: &crate::value::Owner,
    holder: &crate::value::Holder,
    sha: &[u8; 32],
    gen: u64,
) -> Vec<u8> {
    let mut b = value_refs_body(tenant, owner);
    put_holder(&mut b, holder);
    b.put_slice(sha);
    b.put_u64(gen);
    entity(owner_pk, Table::ValueRefs, &b)
}

/// Prefix to scan every holder row of an owner.
pub fn value_refs_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    owner: &crate::value::Owner,
) -> Vec<u8> {
    entity(owner_pk, Table::ValueRefs, &value_refs_body(tenant, owner))
}

/// Prefix to scan every segment held by one holder.
pub fn value_refs_holder_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    owner: &crate::value::Owner,
    holder: &crate::value::Holder,
) -> Vec<u8> {
    let mut b = value_refs_body(tenant, owner);
    put_holder(&mut b, holder);
    entity(owner_pk, Table::ValueRefs, &b)
}

/// A decoded [`Table::ValueRefs`] row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueRefKey {
    pub tenant: TenantId,
    pub owner: crate::value::Owner,
    pub holder: crate::value::Holder,
    pub sha: [u8; 32],
    pub gen: u64,
}

/// Inverse of [`value_ref`]; `None` = a broken key (the caller keeps it).
pub fn parse_value_ref(key: &[u8]) -> Option<ValueRefKey> {
    use crate::value::{Holder, Owner};
    let body = entity_body(key, Table::ValueRefs)?;
    let tenant = lead_tenant(body)?;
    let mut c = Cursor(body);
    c.var()?;
    let owner = Owner(c.take(32)?.try_into().ok()?);
    let holder = match c.u8()? {
        Holder::STATE => Holder::State(bytes::Bytes::copy_from_slice(c.var()?)),
        Holder::INVOCATION => Holder::Invocation(c.id()?),
        Holder::PROMISE => Holder::Promise(String::from_utf8(c.var()?.to_vec()).ok()?),
        _ => return None,
    };
    let sha: [u8; 32] = c.take(32)?.try_into().ok()?;
    let gen = c.u64()?;
    c.is_empty().then_some(ValueRefKey {
        tenant,
        owner,
        holder,
        sha,
        gen,
    })
}

/// `len(tenant)‖tenant ‖ len(scope)‖scope ‖ len(svc)‖svc ‖ len(key)‖key` — the identity of
/// an object, the common part of the State, Promises and PromiseWaiters keys.
fn object_rest(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> BytesMut {
    let mut b = BytesMut::with_capacity(64);
    put_tenant(&mut b, tenant);
    put_scope(&mut b, scope);
    put_var(&mut b, service.as_bytes());
    put_var(&mut b, object_key.as_bytes());
    b
}

/// Inverse of [`object_rest`], skipping the values.
fn skip_object(c: &mut Cursor<'_>) -> Option<()> {
    c.var()?;
    c.var()?;
    c.var()?;
    c.var()?;
    Some(())
}

/// The `owner_pk` of an object's state/promises, when it can be derived from the object's
/// identity itself: it has a scope or an object key.
///
/// `None` = an object **with no key and no scope**: no pk can be derived, so no range is
/// the right place for that state. The caller must refuse (D-a), not invent an owner.
pub fn object_owner(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> Option<PartitionKey> {
    partition_key_for_target(tenant, scope, service, object_key, None)
}

// ─────────────────────────────────────────────────────── timers

/// The entity row of a timer: `timers | id | completion_id → due ‖ payload`.
pub fn timer(id: &InvocationId, completion_id: u32) -> Vec<u8> {
    entity(own(id), Table::Timers, &id_cid(id, completion_id))
}

/// `(id, completion_id)` from a timer key.
pub fn parse_timer(key: &[u8]) -> Option<(InvocationId, u32)> {
    parse_id_cid(entity_body(key, Table::Timers)?)
}

/// The index row of a timer: `due_ms | id | completion_id`.
///
/// `due_ms` big-endian comes right after the index code, so scanning from the start returns
/// timers **in due order**: the timer loop only needs to scan up to the current mark.
pub fn ix_timer(due_ms: u64, id: &InvocationId, completion_id: u32) -> Vec<u8> {
    index(Index::Timer, &due_id_cid(due_ms, id, completion_id))
}

/// Upper bound (exclusive) to scan every timer due at `now_ms`.
pub fn ix_timer_due_upper_bound(now_ms: u64) -> Vec<u8> {
    due_upper_bound(Index::Timer, now_ms)
}

/// `(due_ms, id, completion_id)` from a timer index key.
pub fn parse_ix_timer(key: &[u8]) -> Option<(u64, InvocationId, u32)> {
    parse_due_id_cid(key, Index::Timer)
}

/// `due_ms ‖ id ‖ completion` — the common remainder of the timer and delayed indexes.
fn due_id_cid(due_ms: u64, id: &InvocationId, completion_id: u32) -> BytesMut {
    let mut r = BytesMut::with_capacity(8 + ID_LEN + 4);
    r.put_u64(due_ms);
    r.put_slice(id);
    r.put_u32(completion_id);
    r
}

/// `due_ms ‖ id` — the common remainder of the retry and purge indexes.
fn due_id(due_ms: u64, id: &InvocationId) -> BytesMut {
    let mut r = BytesMut::with_capacity(8 + ID_LEN);
    r.put_u64(due_ms);
    r.put_slice(id);
    r
}

/// Upper bound (exclusive) to scan every row of a due-ordered index that is due at
/// `now_ms`.
fn due_upper_bound(ix: Index, now_ms: u64) -> Vec<u8> {
    index(ix, &now_ms.saturating_add(1).to_be_bytes())
}

fn parse_due_id_cid(key: &[u8], ix: Index) -> Option<(u64, InvocationId, u32)> {
    let mut c = Cursor(index_body(key, ix)?);
    Some((c.u64()?, c.id()?, c.u32()?))
}

fn parse_due_id(key: &[u8], ix: Index) -> Option<(u64, InvocationId)> {
    let mut c = Cursor(index_body(key, ix)?);
    Some((c.u64()?, c.id()?))
}

// ─────────────────────────────────────────────────────── vqueue

/// The identity of a virtual queue — **a composite key, not a hash**.
///
/// The queue identity could be hashed (e.g. SHA-256 truncated to 16 bytes). Cairn does not
/// take that road, and the first reason is a hard one: **a queue id collision is a
/// correctness bug, not a performance bug.** Two different virtual objects landing in the
/// same EXCLUSIVE queue means they lock each other — one hangs, the other stalls, and there
/// is no log line to trace it. Hashing and then truncating always has a non-zero collision
/// probability — and the strings here are chosen by outsiders. The tree's hash (SHA-256
/// truncated to 8 bytes for the partition key, `partition_table.rs`) was chosen to
/// **spread evenly**, not to be an identity; using it here would be the wrong place, and
/// hashing only to make up an identity we *already have* is not worth it.
///
/// The second reason: cairn's entire keyspace is length-prefixed composite keys, and every
/// scan is a prefix scan. Queues are no exception.
///
/// In exchange the key is longer — hard-capped at 111 bytes thanks to the 36-byte caps on
/// the scope and the limit key. Two non-trivial extra benefits: `/vqueues` can read out the
/// queue's **name** without a reverse lookup table, and the scan order groups queues of the
/// same scope next to each other — exactly what the recovery scan needs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueName {
    /// The queue's tenant: `Cart/x` of tenant A and of tenant B are two objects — two queues,
    /// two EXCLUSIVE locks, not blocking each other.
    pub tenant: TenantId,
    pub scope: Option<Scope>,
    pub limit: Option<LimitKey>,
    /// Whether this handler requires exclusive execution on the object key.
    ///
    /// It goes into the key because two queues of the same service — one EXCLUSIVE, one
    /// SHARED — are two different queues with two different rules, and a byte here is much
    /// cheaper than deriving it from `object_key.is_empty()`.
    pub exclusive: bool,
    pub service: String,
    /// Only meaningful when `exclusive`.
    ///
    /// This is a deliberate choice: **SHARED handlers do not put `object_key` into the
    /// key**. Cardinality is reduced on purpose — trading less fairness between object keys
    /// for not creating one queue per key. EXCLUSIVE handlers do, because there the queue
    /// *is* the lock.
    pub object_key: String,
}

impl QueueName {
    /// `len(tenant)‖tenant ‖ len(scope)‖scope ‖ len(l1)‖l1 ‖ len(l2)‖l2 ‖
    /// exclusive ‖ len(svc)‖svc [‖ len(key)‖key]`
    ///
    /// `partition_id` does **not** go into the key: it is already the identity of the whole
    /// store (one partition, one database). Adding it would be redundant.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(64);
        put_tenant(&mut b, &self.tenant);
        put_scope(&mut b, self.scope.as_ref());
        put_var(
            &mut b,
            self.limit.as_ref().map(|l| l.l1()).unwrap_or("").as_bytes(),
        );
        put_var(
            &mut b,
            self.limit
                .as_ref()
                .and_then(|l| l.l2())
                .unwrap_or("")
                .as_bytes(),
        );
        b.put_u8(self.exclusive as u8);
        put_var(&mut b, self.service.as_bytes());
        if self.exclusive {
            put_var(&mut b, self.object_key.as_bytes());
        }
        b.to_vec()
    }

    /// Reads the queue name back from the key. `None` = wrong layout.
    ///
    /// This is what lets `/vqueues` work without a reverse lookup table, and what recovery on
    /// becoming leader relies on: scan the table, read the names, rebuild the counters.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        let mut c = Cursor(raw);
        let take = |c: &mut Cursor<'_>| -> Option<String> {
            Some(std::str::from_utf8(c.var()?).ok()?.to_string())
        };
        let tenant = take(&mut c)?;
        let scope = take(&mut c)?;
        let l1 = take(&mut c)?;
        let l2 = take(&mut c)?;
        let exclusive = c.u8()? != 0;
        let service = take(&mut c)?;
        let object_key = if exclusive {
            take(&mut c)?
        } else {
            String::new()
        };
        if !c.is_empty() {
            return None;
        }
        let limit = match (l1.is_empty(), l2.is_empty()) {
            (true, _) => None,
            (false, true) => Some(LimitKey::parse(&l1).ok()?),
            (false, false) => Some(LimitKey::parse(&format!("{l1}/{l2}")).ok()?),
        };
        Some(Self {
            tenant: if tenant.is_empty() {
                TenantId::default()
            } else {
                TenantId::new(&tenant).ok()?
            },
            scope: if scope.is_empty() {
                None
            } else {
                Scope::new(&scope).ok()
            },
            limit,
            exclusive,
            service,
            object_key,
        })
    }

    /// The lock resource this queue contends for: `(tenant, scope, service, object_key)`.
    ///
    /// **The limit key is not here, and that is the crux.** Two calls to the same virtual
    /// object with different limit keys sit in two different queues — yet they must still
    /// lock each other, because they are *the same object*. So the lock is a separate
    /// resource, not the queue itself.
    pub fn lock_name(&self) -> Option<(TenantId, Option<Scope>, String, String)> {
        self.exclusive.then(|| {
            (
                self.tenant.clone(),
                self.scope.clone(),
                self.service.clone(),
                self.object_key.clone(),
            )
        })
    }
}

impl std::fmt::Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.tenant.is_default() {
            write!(f, "{}:", self.tenant)?;
        }
        if let Some(s) = &self.scope {
            write!(f, "{s}/")?;
        }
        if let Some(l) = &self.limit {
            write!(f, "[{l}]/")?;
        }
        f.write_str(&self.service)?;
        if self.exclusive {
            write!(f, "/{}!", self.object_key)?;
        }
        Ok(())
    }
}

/// The index row of a virtual queue: `queue_key | stage[1] | seq[8] | id[24]`.
///
/// `id` comes last: `seq` is a per-Db counter, so merging two Dbs can yield two rows with
/// the same `(queue, stage, seq)` — with `id` they do not overwrite each other.
pub fn ix_vqueue(queue: &[u8], stage: Stage, seq: u64, id: &InvocationId) -> Vec<u8> {
    let mut r = BytesMut::with_capacity(queue.len() + 9 + ID_LEN);
    r.put_slice(queue);
    r.put_u8(stage as u8);
    r.put_u64(seq);
    r.put_slice(id);
    index(Index::VQueue, &r)
}

/// Prefix to scan every entry of a queue, all stages.
pub fn ix_vqueue_prefix(queue: &[u8]) -> Vec<u8> {
    index(Index::VQueue, queue)
}

/// Prefix to scan a queue at **one stage**: "the head of the queue at stage X" is a single
/// prefix seek, because `stage` comes before `seq`.
pub fn ix_vqueue_stage_prefix(queue: &[u8], stage: Stage) -> Vec<u8> {
    let mut r = BytesMut::with_capacity(queue.len() + 1);
    r.put_slice(queue);
    r.put_u8(stage as u8);
    index(Index::VQueue, &r)
}

/// The length of the fixed tail of an [`ix_vqueue`] key: `stage ‖ seq ‖ id`.
const VQUEUE_TAIL: usize = 1 + 8 + ID_LEN;

/// `(queue_key, stage, seq, id)` from a queue index key.
///
/// `queue_key` has a variable length, but the tail is fixed — so the parser does not need
/// to know anything about the internal layout of `queue_key`.
pub fn parse_ix_vqueue(key: &[u8]) -> Option<(Vec<u8>, Stage, u64, InvocationId)> {
    let body = index_body(key, Index::VQueue)?;
    let (queue, tail) = body.split_at_checked(body.len().checked_sub(VQUEUE_TAIL)?)?;
    let mut c = Cursor(tail);
    let stage = Stage::from_u8(c.u8()?)?;
    let seq = c.u64()?;
    let id = c.id()?;
    Some((queue.to_vec(), stage, seq, id))
}

/// Only the `seq` of a queue index key ([`ix_vqueue`]) — no allocation: commit reads it for
/// every row written (the `queue` mark of `floors.rs`).
pub fn ix_vqueue_seq(key: &[u8]) -> Option<u64> {
    let body = index_body(key, Index::VQueue)?;
    let at = body.len().checked_sub(VQUEUE_TAIL)? + 1;
    Some(u64::from_be_bytes(body.get(at..at + 8)?.try_into().ok()?))
}

/// Entity row: `vqidx | invocation_id`.
pub fn vqueue_index(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::VQueueIndex, id)
}

pub fn parse_vqueue_index(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::VQueueIndex)?).id()
}

/// The value of a `VQueueIndex` row: `stage[1] | seq[8] | queue_key`.
pub fn encode_vqueue_index(queue: &[u8], stage: Stage, seq: u64) -> Vec<u8> {
    let mut b = BytesMut::with_capacity(9 + queue.len());
    b.put_u8(stage as u8);
    b.put_u64(seq);
    b.put_slice(queue);
    b.to_vec()
}

pub fn decode_vqueue_index(raw: &[u8]) -> Option<(Vec<u8>, Stage, u64)> {
    let mut c = Cursor(raw);
    let stage = Stage::from_u8(c.u8()?)?;
    let seq = c.u64()?;
    Some((c.rest().to_vec(), stage, seq))
}

// ─────────────────────────────────────────────────────── outbox

/// Work that must be sent out (calling another service, returning a result to the caller).
///
/// **This is the table that enforces INV-3 most visibly.** An outgoing call that exists
/// only in the leader's RAM is lost on failover. Written to the outbox in the *same* commit
/// as the journal part that produced it, it survives, and the new leader picks it up.
///
/// **The key is `(invocation, completion id)`, not a sequence number.** The state machine
/// replays tasks for every pending completion on *every* replay — that is by design, and it
/// is why the driver must keep the "idempotent task" contract. An increasing counter breaks
/// that contract: each replay creates another row, and each row is one call to the target
/// service. A deterministic key turns rewriting into overwriting. The production order lives
/// in [`Index::Outbox`]; the value carries `seq` ([`encode_seq`]).
///
/// `owner_pk`: `Call`/`Attach`/`PeekOutput`/`Cancel` rows belong to the calling invocation
/// — the `id` in the key itself. `Reply` rows do **not**: the `id` there is the recipient
/// (usually in another partition), while the owner is the **producer** of the reply — the
/// invocation that finished, the target of the attachment, the object of the promise.
pub fn outbox(owner_pk: PartitionKey, id: &InvocationId, completion_id: u32) -> Vec<u8> {
    entity(owner_pk, Table::Outbox, &id_cid(id, completion_id))
}

/// `(invocation, completion_id)` from an outbox key.
pub fn parse_outbox(key: &[u8]) -> Option<(InvocationId, u32)> {
    parse_id_cid(entity_body(key, Table::Outbox)?)
}

/// `id ‖ completion` — the remainder of the Outbox, SignalOutbox, Timers and Delayed keys.
fn id_cid(id: &InvocationId, completion_id: u32) -> BytesMut {
    let mut r = BytesMut::with_capacity(ID_LEN + 4);
    r.put_slice(id);
    r.put_u32(completion_id);
    r
}

fn parse_id_cid(body: &[u8]) -> Option<(InvocationId, u32)> {
    let mut c = Cursor(body);
    Some((c.id()?, c.u32()?))
}

// ─────────────────────────────────────────────── production-order indexes

/// A row of an order index ([`Index::Outbox`], [`Index::Signal`], [`Index::Inbound`]):
/// `seq ‖ entity key`.
///
/// `seq` is big-endian so byte order matches numeric order; the entity key is copied
/// verbatim after it, so the index row points straight back to the main row — owner
/// included. Scanning from a `seq` is "read on from the cursor", not "take a snapshot and
/// filter".
pub fn ix_seq(ix: Index, seq: u64, entity_key: &[u8]) -> Vec<u8> {
    let mut r = Vec::with_capacity(8 + entity_key.len());
    r.extend_from_slice(&seq.to_be_bytes());
    r.extend_from_slice(entity_key);
    index(ix, &r)
}

/// Lower bound to scan an order index from `seq` onwards.
pub fn ix_seq_from(ix: Index, seq: u64) -> Vec<u8> {
    index(ix, &seq.to_be_bytes())
}

/// `(seq, entity key)` from an order index row.
pub fn parse_ix_seq(key: &[u8], ix: Index) -> Option<(u64, &[u8])> {
    let mut c = Cursor(index_body(key, ix)?);
    let seq = c.u64()?;
    let entity_key = c.rest();
    (!entity_key.is_empty()).then_some((seq, entity_key))
}

// ─────────────────────────────────────────────────────── retries

/// The entity row of a scheduled retry: `retries | id → due ‖ payload`.
pub fn retry(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Retries, id)
}

pub fn parse_retry(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::Retries)?).id()
}

/// Index row: `due_ms | id`.
pub fn ix_retry(due_ms: u64, id: &InvocationId) -> Vec<u8> {
    index(Index::Retry, &due_id(due_ms, id))
}

/// Upper bound (exclusive) to scan every retry due at `now_ms`.
pub fn ix_retry_due_upper_bound(now_ms: u64) -> Vec<u8> {
    due_upper_bound(Index::Retry, now_ms)
}

/// `(due_ms, invocation_id)` from a retry index key.
pub fn parse_ix_retry(key: &[u8]) -> Option<(u64, InvocationId)> {
    parse_due_id(key, Index::Retry)
}

// ─────────────────────────────────────────────────────── purges

/// The entity row of a scheduled purge: `purges | id → due`.
pub fn purge(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Purges, id)
}

pub fn parse_purge(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::Purges)?).id()
}

/// Index row: `due_ms | id`.
pub fn ix_purge(due_ms: u64, id: &InvocationId) -> Vec<u8> {
    index(Index::Purge, &due_id(due_ms, id))
}

/// Upper bound (exclusive) to scan every scheduled purge due at `now_ms`.
pub fn ix_purge_due_upper_bound(now_ms: u64) -> Vec<u8> {
    due_upper_bound(Index::Purge, now_ms)
}

/// `(due_ms, invocation_id)` from a purge index key.
pub fn parse_ix_purge(key: &[u8]) -> Option<(u64, InvocationId)> {
    parse_due_id(key, Index::Purge)
}

// ─────────────────────────────────────────────────────── audit

/// Audit row key: `entity_pk ‖ Audit ‖ entity[24] ‖ writer_epoch ‖ count`.
pub fn audit(target: &InvocationId, writer_epoch: u64, seq: u64) -> Vec<u8> {
    let mut rest = Vec::with_capacity(ID_LEN + 16);
    rest.extend_from_slice(target);
    rest.extend_from_slice(&writer_epoch.to_be_bytes());
    rest.extend_from_slice(&seq.to_be_bytes());
    entity(own(target), Table::Audit, &rest)
}

/// The target entity of an audit row key.
pub fn parse_audit(key: &[u8]) -> Option<InvocationId> {
    let body = entity_body(key, Table::Audit)?;
    (body.len() == ID_LEN + 16).then(|| body[..ID_LEN].try_into().ok())?
}

/// Audit index row: the entity row key verbatim.
pub fn ix_audit(row_key: &[u8]) -> Vec<u8> {
    index(Index::Audit, row_key)
}

/// The entity row key from an audit index row.
pub fn parse_ix_audit(key: &[u8]) -> Option<&[u8]> {
    index_body(key, Index::Audit).filter(|b| parse_audit(b).is_some())
}

// ─────────────────────────────────────────────────────── schedules

/// The entity row of a schedule: `schedules | schedule id → due ‖ schedule row`.
pub fn schedule(id: &InvocationId) -> Vec<u8> {
    entity(own(id), Table::Schedules, id)
}

pub fn parse_schedule(key: &[u8]) -> Option<InvocationId> {
    Cursor(entity_body(key, Table::Schedules)?).id()
}

/// Index row: `due_ms | schedule id`.
pub fn ix_schedule_due(due_ms: u64, id: &InvocationId) -> Vec<u8> {
    index(Index::ScheduleDue, &due_id(due_ms, id))
}

/// Upper bound (exclusive) to scan every schedule due at `now_ms`.
pub fn ix_schedule_due_upper_bound(now_ms: u64) -> Vec<u8> {
    due_upper_bound(Index::ScheduleDue, now_ms)
}

/// `(due_ms, schedule id)` from a schedule index key.
pub fn parse_ix_schedule_due(key: &[u8]) -> Option<(u64, InvocationId)> {
    parse_due_id(key, Index::ScheduleDue)
}

// ─────────────────────────────────────────────────────── signal outbox

/// A signal waiting to be sent: `signal_outbox | sender | entry_index`.
///
/// The same remainder layout as [`outbox`] — only the table code and the meaning of the u32
/// column differ. The owner is the sender: its row, produced in its turn.
pub fn signal_outbox(sender: &InvocationId, entry_index: u32) -> Vec<u8> {
    entity(
        own(sender),
        Table::SignalOutbox,
        &id_cid(sender, entry_index),
    )
}

/// `(sender, entry_index)` from a signal outbox key.
pub fn parse_signal_outbox(key: &[u8]) -> Option<(InvocationId, u32)> {
    parse_id_cid(entity_body(key, Table::SignalOutbox)?)
}

// ─────────────────────────────────────────────────────── delayed

/// The entity row of a one-way call that is not yet due:
/// `delayed | caller | completion → due ‖ outbox row`.
///
/// `(invocation, completion)` is the key so that the key is **deterministic per call**:
/// replaying the task on replay overwrites, it does not create more — the same contract as
/// [`outbox`].
pub fn delayed(id: &InvocationId, completion_id: u32) -> Vec<u8> {
    entity(own(id), Table::Delayed, &id_cid(id, completion_id))
}

pub fn parse_delayed(key: &[u8]) -> Option<(InvocationId, u32)> {
    parse_id_cid(entity_body(key, Table::Delayed)?)
}

/// Index row: `due_ms | caller | completion`.
pub fn ix_delayed(due_ms: u64, id: &InvocationId, completion_id: u32) -> Vec<u8> {
    index(Index::Delayed, &due_id_cid(due_ms, id, completion_id))
}

/// Upper bound (exclusive) to scan every call due at `now_ms`.
pub fn ix_delayed_due_upper_bound(now_ms: u64) -> Vec<u8> {
    due_upper_bound(Index::Delayed, now_ms)
}

/// `(due_ms, invocation, completion)` from a delayed index key.
pub fn parse_ix_delayed(key: &[u8]) -> Option<(u64, InvocationId, u32)> {
    parse_due_id_cid(key, Index::Delayed)
}

// ─────────────────────────────────────────────────────── idempotency

/// `(service, handler, object_key, idempotency_key) → invocation`.
///
/// This is how the idempotency guarantee is kept: "at most one invocation for that key
/// set". The key is **the exact bytes**, not a hash of them — with a hash, two different
/// sets could collide, and the consequence is a call silently receiving the result of
/// another call. With an externally chosen `idempotency_key`, that is not a theoretical
/// risk.
///
/// Scope comes first because deduplication also **lives inside the scope**: key `k` of
/// tenant `bob` and key `k` of tenant `alice` are two different keys. Otherwise a tenant
/// that guesses another tenant's key could attach to their invocation.
///
/// The owner is derived from the key set itself — `partition_key_for_target(tenant, scope,
/// service, object_key, Some(idem))`, exactly where every call with the same key is routed
/// — so the caller does not have to (and cannot) pass a wrong one.
pub fn idempotency(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    handler: &str,
    object_key: &str,
    idem_key: &str,
) -> Vec<u8> {
    let owner = partition_key_for_target(tenant, scope, service, object_key, Some(idem_key))
        .expect("with an idempotency key, tiers 0–2 always yield a pk");
    let mut r = BytesMut::with_capacity(64);
    put_tenant(&mut r, tenant);
    put_scope(&mut r, scope);
    put_var(&mut r, service.as_bytes());
    put_var(&mut r, handler.as_bytes());
    put_var(&mut r, object_key.as_bytes());
    put_var(&mut r, idem_key.as_bytes());
    entity(owner, Table::Idempotency, &r)
}

// ─────────────────────────────────────────────────────── attachments

/// An additional waiter of an invocation: `attach | target | caller | completion`.
///
/// Idempotency: a duplicate call "attaches to the running invocation and receives the same
/// result". An invocation can therefore have several waiters, so the place to deliver the
/// result no longer fits inside the `Input` entry — that can only hold one.
///
/// The owner is `target`: the waiting list belongs to the invocation being waited on.
pub fn attachment(target: &InvocationId, caller: &InvocationId, completion_id: u32) -> Vec<u8> {
    let mut r = BytesMut::with_capacity(2 * ID_LEN + 4);
    r.put_slice(target);
    r.put_slice(caller);
    r.put_u32(completion_id);
    entity(own(target), Table::Attachments, &r)
}

/// Prefix to scan every waiter of an invocation.
pub fn attachments_prefix(target: &InvocationId) -> Vec<u8> {
    entity(own(target), Table::Attachments, target)
}

/// `(caller, completion_id)` from an attachment key.
pub fn parse_attachment(key: &[u8]) -> Option<(InvocationId, u32)> {
    let mut c = Cursor(entity_body(key, Table::Attachments)?);
    c.id()?;
    Some((c.id()?, c.u32()?))
}

// ─────────────────────────────────────────────────────── inbound

/// Kind of cross-partition message. Values **must not change** once there is data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum InboundKind {
    /// "Create and run this invocation." The key is the callee's id.
    StartCall = 1,
    /// "Deliver this completion to that invocation." The key is `(invocation, completion)`.
    Completion = 2,
    /// "Put my name on the list waiting for that invocation's result."
    /// The key is `(waiter, completion)` — the identity of the wait request.
    Attach = 3,
    /// "Show me the current result of that invocation, without waiting."
    /// The key has the same form as `Attach`.
    PeekOutput = 4,
    /// "Deliver this signal to one of your invocations." The key is `(SENDER, entry index of
    /// the SendSignal command)` — not the destination: the destination is in the value. The
    /// key only needs to be the deterministic identity of the send so that resending becomes
    /// an overwrite; routing was done by the time the message reached this node.
    Signal = 5,
    /// "Cancel this invocation — its parent has been cancelled." The key is `(PARENT, the
    /// completion the parent's `Call` uses to receive the child's id)`, the same pattern as
    /// `Signal`: that is the deterministic identity of *the cancel*, so resending is an
    /// overwrite. The destination is in the value — routing was done by the time the message
    /// reached this node.
    Cancel = 6,
}

impl InboundKind {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::StartCall),
            2 => Some(Self::Completion),
            3 => Some(Self::Attach),
            4 => Some(Self::PeekOutput),
            5 => Some(Self::Signal),
            6 => Some(Self::Cancel),
            _ => None,
        }
    }
}

/// The key of a cross-partition message.
///
/// **The key is derived from the message content, not from a sequence number.** That is
/// the whole deduplication mechanism: the sender keeps the outbox row until it receives the
/// ack, so a crash between the ack and the delete resends — and the resend overwrites
/// exactly the old row instead of creating a second one.
///
/// Deduplication here is **mandatory**, not an optimization: delivering the same completion
/// twice is a *protocol violation* under the service protocol, and the invocation is ended
/// with an error. A naive "at-least-once" mechanism would kill exactly the invocations it is
/// trying to save.
///
/// `owner_pk` is the pk of the message's **destination** (`destination()`), not of the `id`
/// in the key — in five of the six kinds, `id` is the sender.
/// The value carries `seq` ([`encode_seq`]); receive order is in [`Index::Inbound`].
pub fn inbound(
    owner_pk: PartitionKey,
    kind: InboundKind,
    id: &InvocationId,
    completion_id: u32,
) -> Vec<u8> {
    let mut r = BytesMut::with_capacity(1 + ID_LEN + 4);
    r.put_u8(kind as u8);
    r.put_slice(id);
    r.put_u32(completion_id);
    entity(owner_pk, Table::Inbound, &r)
}

/// Decodes an inbound key into `(kind, invocation, completion)`.
pub fn parse_inbound(key: &[u8]) -> Option<(InboundKind, InvocationId, u32)> {
    let mut c = Cursor(entity_body(key, Table::Inbound)?);
    let kind = InboundKind::from_u8(c.u8()?)?;
    let id = c.id()?;
    let completion = c.u32()?;
    c.is_empty().then_some((kind, id, completion))
}

// ─────────────────────────────────────────────────────── promises

/// Durable promise: `promise | scope | service | object_key | name`.
///
/// Lives in the same partition as its object — every handler of the same object key routes
/// to the same partition, so get/complete never crosses a partition boundary. `owner_pk` as
/// for [`state`].
pub fn promise(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    name: &str,
) -> Vec<u8> {
    let mut r = object_rest(tenant, scope, service, object_key);
    r.put_slice(name.as_bytes());
    entity(owner_pk, Table::Promises, &r)
}

/// Prefix to scan **every** promise of an object.
///
/// It exists because purging a workflow must also clean up its promises. The promise name
/// sits raw at the end of the key — see [`promise`] — and [`parse_promise_name`] reads it
/// back.
pub fn promises_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> Vec<u8> {
    entity(
        owner_pk,
        Table::Promises,
        &object_rest(tenant, scope, service, object_key),
    )
}

/// The promise name (raw bytes) from a promise key.
pub fn parse_promise_name(key: &[u8]) -> Option<&[u8]> {
    let mut c = Cursor(entity_body(key, Table::Promises)?);
    skip_object(&mut c)?;
    Some(c.rest())
}

/// Prefix to scan every waiter of **every** promise of an object.
pub fn promise_waiters_object_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
) -> Vec<u8> {
    entity(
        owner_pk,
        Table::PromiseWaiters,
        &object_rest(tenant, scope, service, object_key),
    )
}

/// A waiter of a promise: `pwait | scope | service | object_key | name(var) |
/// waiter | completion`.
///
/// The promise name needs a length prefix HERE (unlike [`promise`]) because other components
/// follow it — without the prefix, `("a", waiter)` and `("aw", aiter)` are ambiguous.
#[allow(clippy::too_many_arguments)]
pub fn promise_waiter(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    name: &str,
    waiter: &InvocationId,
    completion_id: u32,
) -> Vec<u8> {
    let mut r = object_rest(tenant, scope, service, object_key);
    put_var(&mut r, name.as_bytes());
    r.put_slice(waiter);
    r.put_u32(completion_id);
    entity(owner_pk, Table::PromiseWaiters, &r)
}

/// Prefix to scan every waiter of one promise.
pub fn promise_waiters_prefix(
    owner_pk: PartitionKey,
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    name: &str,
) -> Vec<u8> {
    let mut r = object_rest(tenant, scope, service, object_key);
    put_var(&mut r, name.as_bytes());
    entity(owner_pk, Table::PromiseWaiters, &r)
}

/// `(promise name, waiter, completion_id)` from a promise waiter key.
///
/// Reads each component in turn rather than cutting "the last 28 bytes": the name carries a
/// length prefix (see [`promise_waiter`]), so there is no boundary to guess.
pub fn parse_promise_waiter(key: &[u8]) -> Option<(&[u8], InvocationId, u32)> {
    let mut c = Cursor(entity_body(key, Table::PromiseWaiters)?);
    skip_object(&mut c)?;
    let name = c.var()?;
    let waiter = c.id()?;
    let cid = c.u32()?;
    c.is_empty().then_some((name, waiter, cid))
}

// ─────────────────────────────────────────────────────── derived marks

/// The key of the mark "the derived region matches this Db".
pub fn derived_ready() -> Vec<u8> {
    index(Index::Ready, &[])
}

/// The key of the durable progress marks (`0x01 ‖ 0xFD`) — see [`Index::Floors`].
pub fn floors() -> Vec<u8> {
    index(Index::Floors, &[])
}

/// The key of the seal barrier (`0x01 ‖ 0xFE`) — see [`Index::SealBarrier`]. Only ever
/// *deleted*, never read.
pub fn seal_barrier() -> Vec<u8> {
    index(Index::SealBarrier, &[])
}

// ─────────────────────────────────────────────────────── key parsing

/// `command_index` from a journal key. `None` if the key does not have the right layout.
pub fn parse_journal_index(key: &[u8]) -> Option<u32> {
    let mut c = Cursor(entity_body(key, Table::Journal)?);
    c.id()?;
    c.u32()
}

// ─────────────────────────────────────────────── owner of a row

/// Who a row belongs to — for logically deleting the rows of **one** tenant in a range Db
/// (tenant purge). Rows of different tenants are interleaved by the `owner_pk` hash, so the
/// deleter scans the whole range and asks this function about each key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowOwner<'a> {
    /// The tenant is in the key itself: State, Promises, PromiseWaiters, Idempotency, query
    /// index rows.
    Tenant(TenantId),
    /// A row keyed by an id: the id's tenant is looked up in the Db (`InvocationMeta`, then
    /// journal entry 0). The id may belong to another range (the recipient of a `Reply`, the
    /// sender of an inbound message) — then `owner`, if present, is the local entity owning
    /// the row: look up the tenant of that `owner_pk` **group**.
    Id {
        id: InvocationId,
        owner: Option<PartitionKey>,
    },
    /// An index row pointing at an entity key (outbox, signal, inbound, audit): the owner of
    /// that row.
    Entity(&'a [u8]),
    /// A row of the range itself (ready mark, progress marks, seal barrier, query index
    /// mark): belongs to no tenant.
    Range,
    /// A key that cannot be decoded: the deleter **keeps** it and reports it.
    Unknown,
}

/// The tenant at the start of the key body (`len ‖ tenant`, empty = `default`) —
/// [`put_tenant`] and the query index's `put_lp` have the same form.
fn lead_tenant(body: &[u8]) -> Option<TenantId> {
    let raw = Cursor(body).var()?;
    if raw.is_empty() {
        return Some(TenantId::default());
    }
    TenantId::new(std::str::from_utf8(raw).ok()?).ok()
}

/// The owner of row `key`. An exhaustive `match` over [`Table`] and [`Index`]: a new table
/// or index that has not been classified does not compile.
pub fn row_owner(key: &[u8]) -> RowOwner<'_> {
    if let Some(table) = table_of(key) {
        let Some(body) = entity_body(key, table) else {
            return RowOwner::Unknown;
        };
        let owner = owner_of(key);
        let by_id = |id: Option<InvocationId>| {
            id.map_or(RowOwner::Unknown, |id| RowOwner::Id { id, owner })
        };
        return match table {
            // `SegmentedState` is keyed like `State`; `ValueRefs` starts with the tenant —
            // no owner to decode.
            Table::State
            | Table::SegmentedState
            | Table::ValueRefs
            | Table::Promises
            | Table::PromiseWaiters
            | Table::Idempotency => lead_tenant(body).map_or(RowOwner::Unknown, RowOwner::Tenant),
            Table::Inbound => by_id(parse_inbound(key).map(|(_, id, _)| id)),
            Table::InvocationStatus
            | Table::Journal
            | Table::Timers
            | Table::Outbox
            | Table::Retries
            | Table::Attachments
            | Table::Purges
            | Table::Delayed
            | Table::SignalOutbox
            | Table::VQueueIndex
            | Table::Pins
            | Table::Chain
            | Table::LastFailure
            | Table::Streams
            | Table::Artifacts
            | Table::Schedules
            | Table::InvocationMeta
            | Table::Audit => by_id(Cursor(body).id()),
        };
    }
    if Region::of_key(key) != Some(Region::Derived) || key.len() < INDEX_HEADER_LEN {
        return RowOwner::Unknown;
    }
    let code = key[1];
    if crate::query_index::QUERY_CODES.contains(&code) {
        // Every query index descriptor starts with `Part::Tenant`
        // (`query_index::tests::every_index_leads_with_the_tenant`).
        return lead_tenant(&key[INDEX_HEADER_LEN..]).map_or(RowOwner::Unknown, RowOwner::Tenant);
    }
    if code == crate::query_index::QUERY_READY {
        return RowOwner::Range;
    }
    let Some(ix) = Index::from_u8(code) else {
        return RowOwner::Unknown;
    };
    let id = |id: Option<InvocationId>| {
        id.map_or(RowOwner::Unknown, |id| RowOwner::Id { id, owner: None })
    };
    match ix {
        Index::Timer => id(parse_ix_timer(key).map(|(_, id, _)| id)),
        Index::Retry => id(parse_ix_retry(key).map(|(_, id)| id)),
        Index::Purge => id(parse_ix_purge(key).map(|(_, id)| id)),
        Index::Delayed => id(parse_ix_delayed(key).map(|(_, id, _)| id)),
        Index::ScheduleDue => id(parse_ix_schedule_due(key).map(|(_, id)| id)),
        Index::VQueue => id(parse_ix_vqueue(key).map(|(.., id)| id)),
        Index::Invocations => id(parse_ix_invocation(key)),
        Index::Deployment => id(parse_ix_deployment(key).map(|(_, id)| id)),
        Index::Outbox | Index::Signal | Index::Inbound => {
            parse_ix_seq(key, ix).map_or(RowOwner::Unknown, |(_, entity)| RowOwner::Entity(entity))
        }
        Index::Audit => parse_ix_audit(key).map_or(RowOwner::Unknown, RowOwner::Entity),
        Index::Floors | Index::SealBarrier | Index::Ready => RowOwner::Range,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: InvocationId = [1u8; ID_LEN];
    const B: InvocationId = [2u8; ID_LEN];

    /// The owner of a row: a table carrying the tenant in its key ⇒ `Tenant`; a table keyed
    /// by id ⇒ `Id` with the row's `owner_pk`; an index by id ⇒ `Id` without owner; order
    /// indexes and audit ⇒ `Entity`; range keys ⇒ `Range`; garbage ⇒ `Unknown`.
    #[test]
    fn row_owner_classifies_every_kind_of_row() {
        let acme = TenantId::new("acme").unwrap();
        let pk = PartitionKey(7);
        let named = |k: Vec<u8>| row_owner(&k).clone_owned();
        for t in [acme.clone(), TenantId::default()] {
            for k in [
                state(pk, &t, None, "Svc", "k", b"u"),
                promise(pk, &t, None, "Svc", "k", "p"),
                promise_waiter(pk, &t, None, "Svc", "k", "p", &A, 3),
                idempotency(&t, None, "Svc", "h", "k", "idem"),
            ] {
                assert_eq!(named(k), Owned::Tenant(t.clone()));
            }
        }
        let own_a = Some(partition_key_of(&A));
        for k in [
            invocation_status(&A),
            journal_entry(&A, 0),
            timer(&A, 1),
            retry(&A),
            purge(&A),
            delayed(&A, 1),
            signal_outbox(&A, 1),
            vqueue_index(&A),
            pin(&A),
            chain(&A),
            last_failure(&A),
            stream_chunk(&A, "$log", 0),
            artifact(&A, &[9; 32]),
            schedule(&A),
            crate::query_index::meta_key(&A),
            audit(&A, 1, 2),
            attachment(&A, &B, 1),
        ] {
            assert_eq!(named(k), Owned::Id(A, own_a), "table keyed by id");
        }
        assert_eq!(
            named(outbox(pk, &B, 1)),
            Owned::Id(B, Some(pk)),
            "Reply: id is the recipient, owner is the producer"
        );
        assert_eq!(
            named(inbound(pk, InboundKind::Completion, &B, 1)),
            Owned::Id(B, Some(pk))
        );
        for k in [
            ix_timer(5, &A, 1),
            ix_retry(5, &A),
            ix_purge(5, &A),
            ix_delayed(5, &A, 1),
            ix_schedule_due(5, &A),
            ix_vqueue(b"q", Stage::Inbox, 1, &A),
            ix_invocation(&A),
            ix_deployment("d", &A),
        ] {
            assert_eq!(named(k), Owned::Id(A, None), "index by id");
        }
        let ob = outbox(pk, &B, 1);
        assert_eq!(
            row_owner(&ix_seq(Index::Outbox, 9, &ob)),
            RowOwner::Entity(&ob)
        );
        let au = audit(&A, 1, 2);
        assert_eq!(row_owner(&ix_audit(&au)), RowOwner::Entity(&au));
        for k in [
            derived_ready(),
            floors(),
            seal_barrier(),
            query_index_marker(),
        ] {
            assert_eq!(row_owner(&k), RowOwner::Range, "{k:02x?}");
        }
        let meta = crate::query_index::InvocationMeta {
            tenant: "acme".into(),
            service: "Svc".into(),
            ..Default::default()
        };
        let p = crate::query_index::Placement::new(crate::query_index::Class::Active, 1, &meta);
        for ix in crate::query_index::QUERY_INDEXES {
            for (k, _) in crate::query_index::rows(ix, &A, &meta, p) {
                assert_eq!(named(k), Owned::Tenant(acme.clone()), "{}", ix.name);
            }
        }
        for k in [
            vec![],
            vec![0x00],
            vec![0x01, 0x7E],
            vec![0x01, 0xEE],
            vec![0x09, 1, 2],
        ] {
            assert_eq!(row_owner(&k), RowOwner::Unknown, "{k:02x?}");
        }
    }

    /// An owned version of [`RowOwner`] for comparisons in tests.
    #[derive(Debug, PartialEq)]
    enum Owned {
        Tenant(TenantId),
        Id(InvocationId, Option<PartitionKey>),
        Other,
    }

    trait CloneOwned {
        fn clone_owned(&self) -> Owned;
    }

    impl CloneOwned for RowOwner<'_> {
        fn clone_owned(&self) -> Owned {
            match self {
                RowOwner::Tenant(t) => Owned::Tenant(t.clone()),
                RowOwner::Id { id, owner } => Owned::Id(*id, *owner),
                _ => Owned::Other,
            }
        }
    }
    const O: PartitionKey = PartitionKey(7);

    fn sc(s: &str) -> Scope {
        Scope::new(s).unwrap()
    }

    /// Tenant `default` — every key of the older tests.
    fn dt() -> TenantId {
        TenantId::default()
    }

    fn q(scope: Option<&str>, limit: Option<&str>, exclusive: bool, key: &str) -> QueueName {
        QueueName {
            tenant: dt(),
            scope: scope.map(sc),
            limit: limit.map(|l| LimitKey::parse(l).unwrap()),
            exclusive,
            service: "Cart".into(),
            object_key: key.into(),
        }
    }

    fn in_ranges(key: &[u8], ranges: &[(Vec<u8>, Vec<u8>)]) -> bool {
        ranges
            .iter()
            .any(|(lo, hi)| key >= lo.as_slice() && key < hi.as_slice())
    }

    /// Every entity key, one per table, under owner `O` (when the owner is explicit) or the
    /// pk of `A` (when the owner is derived from the id).
    fn entity_keys() -> Vec<Vec<u8>> {
        vec![
            invocation_status(&A),
            journal_entry(&A, 0),
            state(O, &dt(), None, "s", "k", b"u"),
            timer(&A, 1),
            outbox(O, &B, 1),
            retry(&A),
            idempotency(&dt(), None, "s", "h", "k", "i"),
            attachment(&A, &B, 1),
            inbound(O, InboundKind::Attach, &B, 1),
            promise(O, &dt(), None, "s", "k", "n"),
            promise_waiter(O, &dt(), None, "s", "k", "n", &A, 1),
            purge(&A),
            delayed(&A, 1),
            signal_outbox(&A, 1),
            vqueue_index(&A),
            pin(&A),
            chain(&A),
            last_failure(&A),
            stream_chunk(&A, "$log", 0),
            artifact(&A, &[7; 32]),
            schedule(&A),
            audit(&A, 1, 2),
            crate::query_index::meta_key(&A),
            segmented_state(O, &dt(), None, "s", "k", b"u"),
            value_ref(
                O,
                &dt(),
                &crate::value::Owner([3; 32]),
                &crate::value::Holder::Invocation(A),
                &[4; 32],
                5,
            ),
        ]
    }

    fn index_keys() -> Vec<Vec<u8>> {
        let ek = outbox(O, &B, 1);
        vec![
            ix_invocation(&A),
            ix_timer(9, &A, 1),
            ix_retry(9, &A),
            ix_purge(9, &A),
            ix_delayed(9, &A, 1),
            ix_seq(Index::Outbox, 9, &ek),
            ix_seq(Index::Signal, 9, &signal_outbox(&A, 1)),
            ix_seq(Index::Inbound, 9, &inbound(O, InboundKind::Attach, &B, 1)),
            ix_vqueue(&q(None, None, false, "").encode(), Stage::Inbox, 9, &A),
            ix_deployment("dp_1", &A),
            ix_schedule_due(9, &A),
            ix_audit(&audit(&A, 1, 2)),
            derived_ready(),
        ]
    }

    #[test]
    fn tables_do_not_overlap() {
        // One code per table; no table encroaches on another.
        let keys = entity_keys();
        let mut heads: Vec<u8> = keys.iter().map(|k| table_of(k).unwrap() as u8).collect();
        heads.sort();
        heads.dedup();
        assert_eq!(heads.len(), keys.len(), "two tables share a code");
        assert_eq!(
            keys.len(),
            Table::ALL.len(),
            "table missing from the test list"
        );
    }

    #[test]
    fn last_failure_round_trips_and_reads_lax() {
        let f = LastFailure::new(500, "boom", 2, 1_700_000_000_000);
        assert_eq!(LastFailure::decode(&f.encode()), Some(f));
        // Shorter than the fixed header ⇒ as if absent, no panic.
        assert_eq!(LastFailure::decode(&[0; 15]), None);
        // A long message is cut at a character boundary, within the cap.
        let long = "€".repeat(LAST_FAILURE_MESSAGE_MAX);
        let cut = LastFailure::new(1, &long, 0, 0);
        assert!(cut.message.len() <= LAST_FAILURE_MESSAGE_MAX);
        assert!(long.starts_with(&cut.message));
        assert_eq!(parse_last_failure(&last_failure(&A)), Some(A));
    }

    /// The byte layout of the current `DATA_FORMAT`, copied by hand from the layout design.
    #[test]
    fn layout_is_region_owner_table_then_rest() {
        let pk_a = partition_key_of(&A).0.to_be_bytes();
        let s =
            |x: &str| -> Vec<u8> { [&(x.len() as u16).to_be_bytes()[..], x.as_bytes()].concat() };
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (
                invocation_status(&A),
                [&[0x00][..], &pk_a, &[0x01], &A].concat(),
            ),
            (
                journal_entry(&A, 3),
                [&[0x02][..], &pk_a, &[0x02], &A, &3u32.to_be_bytes()].concat(),
            ),
            (
                state(O, &dt(), None, "S", "k", b"u"),
                [
                    &[0x02][..],
                    &O.0.to_be_bytes(),
                    &[0x03],
                    // tenant `default` (empty), then scope (empty).
                    &s(""),
                    &s(""),
                    &s("S"),
                    &s("k"),
                    b"u",
                ]
                .concat(),
            ),
            (
                outbox(O, &B, 2),
                [
                    &[0x00][..],
                    &O.0.to_be_bytes(),
                    &[0x06],
                    &B,
                    &2u32.to_be_bytes(),
                ]
                .concat(),
            ),
            (
                inbound(O, InboundKind::Signal, &A, 2),
                [
                    &[0x00][..],
                    &O.0.to_be_bytes(),
                    &[0x0B, 5],
                    &A,
                    &2u32.to_be_bytes(),
                ]
                .concat(),
            ),
            (
                ix_timer(9, &A, 2),
                [
                    &[0x01, 0x01][..],
                    &9u64.to_be_bytes(),
                    &A,
                    &2u32.to_be_bytes(),
                ]
                .concat(),
            ),
            (
                state(O, &TenantId::new("acme").unwrap(), None, "S", "k", b"u"),
                [
                    &[0x02][..],
                    &O.0.to_be_bytes(),
                    &[0x03],
                    &s("acme"),
                    &s(""),
                    &s("S"),
                    &s("k"),
                    b"u",
                ]
                .concat(),
            ),
            (ix_invocation(&A), [&[0x01, 0x09][..], &A].concat()),
            (floors(), vec![0x01, 0xFD]),
            (seal_barrier(), vec![0x01, 0xFE]),
            (derived_ready(), vec![0x01, 0xFF]),
        ];
        for (got, want) in cases {
            assert_eq!(got, want);
        }
    }

    /// `ValueRefs` keys round-trip with every holder kind; a holder prefix covers exactly that
    /// holder's rows (the length of `user_key` is in the key: `a` does not cover `ab`); a
    /// truncated key or an unknown holder kind ⇒ `None`.
    #[test]
    fn value_ref_round_trips_and_holder_prefix_is_exact() {
        use crate::value::{Holder, Owner};
        let acme = TenantId::new("acme").unwrap();
        let owner = Owner([3; 32]);
        for holder in [
            Holder::State(bytes::Bytes::from_static(b"messages")),
            Holder::Invocation(A),
            Holder::Promise("p".into()),
        ] {
            let k = value_ref(O, &acme, &owner, &holder, &[4; 32], 0x0102);
            assert_eq!(
                parse_value_ref(&k),
                Some(ValueRefKey {
                    tenant: acme.clone(),
                    owner,
                    holder: holder.clone(),
                    sha: [4; 32],
                    gen: 0x0102,
                })
            );
            assert!(k.starts_with(&value_refs_prefix(O, &acme, &owner)));
            assert!(k.starts_with(&value_refs_holder_prefix(O, &acme, &owner, &holder)));
            assert!(matches!(row_owner(&k), RowOwner::Tenant(t) if t == acme));
            assert_eq!(parse_value_ref(&k[..k.len() - 1]), None);
        }
        let a = value_ref(O, &acme, &owner, &Holder::State("ab".into()), &[4; 32], 1);
        assert!(!a.starts_with(&value_refs_holder_prefix(
            O,
            &acme,
            &owner,
            &Holder::State("a".into())
        )));
        let mut bad = value_ref(O, &acme, &owner, &Holder::Invocation(A), &[4; 32], 1);
        let at = ENTITY_HEADER_LEN + 2 + 4 + 32;
        bad[at] = 0x09;
        assert_eq!(parse_value_ref(&bad), None);
    }

    /// `SegmentedState` is keyed exactly like `State` (a different table code), reads back
    /// `user_key`.
    #[test]
    fn segmented_state_keys_mirror_state() {
        let st = state(O, &dt(), None, "Chat", "c1", b"messages");
        let sg = segmented_state(O, &dt(), None, "Chat", "c1", b"messages");
        assert_eq!(st[ENTITY_HEADER_LEN..], sg[ENTITY_HEADER_LEN..]);
        assert_eq!(parse_segmented_state_user_key(&sg), Some(&b"messages"[..]));
        assert!(sg.starts_with(&segmented_state_prefix(O, &dt(), None, "Chat", "c1")));
        assert_eq!(parse_state_user_key(&sg), None);
        let (t, rest, user) = parse_segmented_state(&sg).unwrap();
        assert_eq!((t, user), (dt(), &b"messages"[..]));
        assert_eq!(rest, &object_rest_bytes(&dt(), None, "Chat", "c1")[..]);
    }

    /// The bulk region holds exactly Journal and State; every other entity table is in the
    /// hot region.
    #[test]
    fn region_of_puts_journal_and_state_in_bulk() {
        for t in Table::ALL {
            let want = if matches!(
                t,
                Table::Journal | Table::State | Table::SegmentedState | Table::Streams
            ) {
                Region::Bulk
            } else {
                Region::Hot
            };
            assert_eq!(region_of(t), want, "{t:?}");
        }
    }

    /// The owner goes into the bytes: two different owners ⇒ two different keys, and the
    /// owner can be read back from the key.
    #[test]
    fn owner_is_encoded_and_readable() {
        let other = PartitionKey(u64::MAX);
        assert_ne!(outbox(O, &A, 1), outbox(other, &A, 1));
        assert_eq!(owner_of(&outbox(other, &A, 1)), Some(other));
        assert_eq!(owner_of(&invocation_status(&A)), Some(partition_key_of(&A)));
        for k in index_keys() {
            assert_eq!(owner_of(&k), None, "a derived key has no owner: {k:?}");
            assert_eq!(table_of(&k), None);
        }
    }

    /// The range of a pk holds every entity key of it, no key of the neighbouring pk, and no
    /// derived key.
    #[test]
    fn owned_ranges_cover_exactly_the_owner() {
        let ranges = owned_ranges(O, O);
        assert_eq!(ranges.len(), 2, "one range per entity region");
        for k in [
            state(O, &dt(), None, "s", "k", b"u"),
            outbox(O, &A, 1),
            promise(O, &dt(), None, "s", "k", "n"),
        ] {
            assert!(in_ranges(&k, &ranges), "{k:?}");
        }
        for pk in [PartitionKey(6), PartitionKey(8)] {
            assert!(!in_ranges(&outbox(pk, &A, 1), &ranges));
            assert!(!in_ranges(&state(pk, &dt(), None, "s", "k", b"u"), &ranges));
        }
        for k in index_keys() {
            assert!(!in_ranges(&k, &ranges), "derived key in the range: {k:?}");
        }
        // A range up to the end of the key space.
        let top = owned_ranges(PartitionKey(u64::MAX - 1), PartitionKey(u64::MAX));
        assert!(in_ranges(&outbox(PartitionKey(u64::MAX), &A, 1), &top));
        assert!(in_ranges(
            &state(PartitionKey(u64::MAX), &dt(), None, "s", "k", b"u"),
            &top
        ));
    }

    #[test]
    fn journal_sorts_by_command_index() {
        let mut keys: Vec<Vec<u8>> = (0..300u32).rev().map(|i| journal_entry(&A, i)).collect();
        keys.sort();
        let expected: Vec<Vec<u8>> = (0..300u32).map(|i| journal_entry(&A, i)).collect();
        assert_eq!(keys, expected, "journal scan must keep write order");
    }

    #[test]
    fn journals_of_two_invocations_do_not_mix() {
        let pa = journal_prefix(&A);
        assert!(journal_entry(&A, 7).starts_with(&pa));
        assert!(!journal_entry(&B, 7).starts_with(&pa));
    }

    #[test]
    fn timers_sort_by_due_time() {
        let mut keys: Vec<Vec<u8>> = [500u64, 1, 999_999, 42]
            .iter()
            .map(|d| ix_timer(*d, &A, 0))
            .collect();
        keys.sort();
        let due: Vec<u64> = keys.iter().map(|k| parse_ix_timer(k).unwrap().0).collect();
        assert_eq!(due, vec![1, 42, 500, 999_999]);
    }

    #[test]
    fn due_timers_fall_below_upper_bound() {
        let now = 1_000u64;
        let ub = ix_timer_due_upper_bound(now);
        assert!(ix_timer(now, &A, 0) < ub, "a due timer must be picked up");
        assert!(ix_timer(now - 1, &A, 0) < ub);
        assert!(
            ix_timer(now + 1, &A, 0) > ub,
            "a timer not yet due must not be picked up"
        );
        assert!(ix_timer(0, &A, 0) > index_prefix(Index::Timer));
    }

    #[test]
    fn state_key_is_unambiguous() {
        // Without length prefixes, ("ab","c") and ("a","bc") would encode identically.
        assert_ne!(
            state(O, &dt(), None, "ab", "c", b""),
            state(O, &dt(), None, "a", "bc", b"")
        );
        let p = state_prefix(O, &dt(), None, "svc", "obj");
        assert!(state(O, &dt(), None, "svc", "obj", b"x").starts_with(&p));
        assert!(!state(O, &dt(), None, "svc", "obj2", b"x").starts_with(&p));
    }

    /// Same object key, different scope ⇒ independent state.
    #[test]
    fn scope_separates_state_of_same_object_key() {
        let bob = sc("bob");
        let alice = sc("alice");
        assert_ne!(
            state(O, &dt(), Some(&bob), "Cart", "a", b"items"),
            state(O, &dt(), Some(&alice), "Cart", "a", b"items")
        );
        assert_ne!(
            state(O, &dt(), Some(&bob), "Cart", "a", b"items"),
            state(O, &dt(), None, "Cart", "a", b"items")
        );
        let p = state_prefix(O, &dt(), Some(&bob), "Cart", "a");
        assert!(state(O, &dt(), Some(&bob), "Cart", "a", b"x").starts_with(&p));
        assert!(!state(O, &dt(), Some(&alice), "Cart", "a", b"x").starts_with(&p));
        assert!(!state(O, &dt(), None, "Cart", "a", b"x").starts_with(&p));
        assert_ne!(
            idempotency(&dt(), Some(&bob), "s", "h", "k", "i"),
            idempotency(&dt(), Some(&alice), "s", "h", "k", "i")
        );
        assert_ne!(
            promise(O, &dt(), Some(&bob), "s", "k", "n"),
            promise(O, &dt(), Some(&alice), "s", "k", "n")
        );
        assert_ne!(
            promise_waiter(O, &dt(), Some(&bob), "s", "k", "n", &A, 0),
            promise_waiter(O, &dt(), Some(&alice), "s", "k", "n", &A, 0)
        );
    }

    /// Idempotency lives under the pk that every call with the same key is routed to.
    #[test]
    fn idempotency_owner_is_the_routing_key() {
        let k = idempotency(&dt(), None, "Mailer", "send", "", "i1");
        assert_eq!(
            owner_of(&k),
            partition_key_for_target(&dt(), None, "Mailer", "", Some("i1"))
        );
        let k = idempotency(&dt(), None, "Cart", "add", "u1", "i1");
        assert_eq!(owner_of(&k), object_owner(&dt(), None, "Cart", "u1"));
        // A different tenant ⇒ a different owner: calls are routed by a hash that includes
        // the tenant.
        let acme = TenantId::new("acme").unwrap();
        let k = idempotency(&acme, None, "Cart", "add", "u1", "i1");
        assert_eq!(owner_of(&k), object_owner(&acme, None, "Cart", "u1"));
        assert_ne!(owner_of(&k), object_owner(&dt(), None, "Cart", "u1"));
    }

    /// Same owner, same scope, same object key, different tenant ⇒ different keys in every
    /// table with user-chosen names — even when the two partition keys collide (forced here
    /// by using owner `O` for both).
    #[test]
    fn tenant_separates_keys_of_same_object() {
        let acme = TenantId::new("acme").unwrap();
        let globex = TenantId::new("globex").unwrap();
        let bob = sc("bob");
        for scope in [None, Some(&bob)] {
            assert_ne!(
                state(O, &acme, scope, "Cart", "a", b"items"),
                state(O, &globex, scope, "Cart", "a", b"items")
            );
            assert_ne!(
                state(O, &acme, scope, "Cart", "a", b"items"),
                state(O, &dt(), scope, "Cart", "a", b"items")
            );
            let p = state_prefix(O, &acme, scope, "Cart", "a");
            assert!(state(O, &acme, scope, "Cart", "a", b"x").starts_with(&p));
            assert!(!state(O, &globex, scope, "Cart", "a", b"x").starts_with(&p));
            assert_ne!(
                promise(O, &acme, scope, "s", "k", "n"),
                promise(O, &globex, scope, "s", "k", "n")
            );
            assert_ne!(
                promise_waiter(O, &acme, scope, "s", "k", "n", &A, 0),
                promise_waiter(O, &globex, scope, "s", "k", "n", &A, 0)
            );
        }
        assert_ne!(
            idempotency(&acme, None, "s", "h", "k", "i"),
            idempotency(&globex, None, "s", "h", "k", "i")
        );
        // The queues (and EXCLUSIVE locks) of the same `Cart/x` in two tenants.
        let mut a = q(None, None, true, "x");
        a.tenant = acme.clone();
        let mut g = a.clone();
        g.tenant = globex;
        assert_ne!(a.encode(), g.encode());
        assert_ne!(a.lock_name(), g.lock_name());
        assert_eq!(QueueName::decode(&a.encode()), Some(a));
    }

    #[test]
    fn no_scope_is_zero_length_prefix() {
        // Tenant `default` then no scope: two zero length prefixes.
        let s = state(O, &dt(), None, "s", "k", b"u");
        assert_eq!(entity_body(&s, Table::State).unwrap()[..4], [0, 0, 0, 0]);
        let p = promise(O, &dt(), None, "s", "k", "n");
        assert_eq!(entity_body(&p, Table::Promises).unwrap()[..4], [0, 0, 0, 0]);
    }

    // ───────────────────────────────────────────────────────── vqueue

    #[test]
    fn queue_name_roundtrip() {
        for name in [
            q(None, None, false, ""),
            q(Some("acme"), None, false, ""),
            q(Some("acme"), Some("growth"), false, ""),
            q(Some("acme"), Some("growth/alice"), true, "user-42"),
            q(None, None, true, ""),
        ] {
            let raw = name.encode();
            assert_eq!(QueueName::decode(&raw), Some(name.clone()), "{name}");
        }
    }

    #[test]
    fn queue_name_is_unambiguous() {
        let a = QueueName {
            tenant: dt(),
            scope: Some(sc("ab")),
            limit: None,
            exclusive: false,
            service: "c".into(),
            object_key: String::new(),
        };
        let b = QueueName {
            tenant: dt(),
            scope: Some(sc("a")),
            limit: None,
            exclusive: false,
            service: "bc".into(),
            object_key: String::new(),
        };
        assert_ne!(a.encode(), b.encode());
        assert_ne!(
            q(Some("acme"), Some("a"), false, "").encode(),
            q(Some("acme"), Some("b"), false, "").encode()
        );
        assert_eq!(
            q(Some("acme"), Some("a"), true, "u1").lock_name(),
            q(Some("acme"), Some("b"), true, "u1").lock_name()
        );
        assert_eq!(q(Some("acme"), None, false, "").lock_name(), None);
    }

    #[test]
    fn shared_groups_every_object_key_into_one_queue() {
        let a = QueueName {
            object_key: "u1".into(),
            ..q(Some("acme"), None, false, "")
        };
        let b = QueueName {
            object_key: "u2".into(),
            ..q(Some("acme"), None, false, "")
        };
        assert_eq!(a.encode(), b.encode());
        let x = q(Some("acme"), None, true, "u1");
        let y = q(Some("acme"), None, true, "u2");
        assert_ne!(x.encode(), y.encode());
    }

    #[test]
    fn vqueue_scans_by_stage_then_seq() {
        let queue = q(Some("acme"), None, true, "u1").encode();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for stage in [Stage::Suspended, Stage::Inbox, Stage::Running] {
            for seq in [9u64, 1, 100] {
                keys.push(ix_vqueue(&queue, stage, seq, &A));
            }
        }
        keys.sort();
        let order: Vec<(Stage, u64)> = keys
            .iter()
            .map(|k| {
                let (_, s, q, _) = parse_ix_vqueue(k).unwrap();
                (s, q)
            })
            .collect();
        assert_eq!(
            order,
            vec![
                (Stage::Inbox, 1),
                (Stage::Inbox, 9),
                (Stage::Inbox, 100),
                (Stage::Running, 1),
                (Stage::Running, 9),
                (Stage::Running, 100),
                (Stage::Suspended, 1),
                (Stage::Suspended, 9),
                (Stage::Suspended, 100),
            ]
        );
        let p = ix_vqueue_stage_prefix(&queue, Stage::Inbox);
        assert!(ix_vqueue(&queue, Stage::Inbox, 0, &A).starts_with(&p));
        assert!(!ix_vqueue(&queue, Stage::Running, 0, &A).starts_with(&p));
    }

    /// Two Dbs merged together yield two rows with the same `(queue, stage, seq)` — the id
    /// in the key keeps them from overwriting each other.
    #[test]
    fn vqueue_rows_of_two_dbs_do_not_collide() {
        let queue = q(None, None, false, "").encode();
        assert_ne!(
            ix_vqueue(&queue, Stage::Inbox, 1, &A),
            ix_vqueue(&queue, Stage::Inbox, 1, &B)
        );
    }

    #[test]
    fn parse_vqueue_recovers_name() {
        for name in [
            q(None, None, false, ""),
            q(Some("acme"), Some("growth/alice"), true, "user-42"),
        ] {
            let queue = name.encode();
            let key = ix_vqueue(&queue, Stage::Paused, 7, &B);
            let (back, stage, seq, id) = parse_ix_vqueue(&key).unwrap();
            assert_eq!(back, queue);
            assert_eq!((stage, seq, id), (Stage::Paused, 7, B));
            assert_eq!(QueueName::decode(&back), Some(name));
            assert_eq!(ix_vqueue_seq(&key), Some(7));
        }
        assert_eq!(parse_ix_vqueue(&index_prefix(Index::VQueue)), None);
        assert_eq!(parse_ix_vqueue(&invocation_status(&A)), None);
        assert_eq!(ix_vqueue_seq(&index_prefix(Index::VQueue)), None);
        assert_eq!(ix_vqueue_seq(&invocation_status(&A)), None);
        assert_eq!(ix_vqueue_seq(&ix_seq(Index::Outbox, 7, b"x")), None);
    }

    #[test]
    fn reverse_index_roundtrip() {
        let queue = q(Some("acme"), None, true, "u1").encode();
        let raw = encode_vqueue_index(&queue, Stage::Running, 42);
        assert_eq!(decode_vqueue_index(&raw), Some((queue, Stage::Running, 42)));
    }

    #[test]
    fn stage_distinguishes_counter_from_key() {
        assert!(Stage::Running.holds_counter());
        for s in [Stage::Inbox, Stage::Suspended, Stage::Paused] {
            assert!(!s.holds_counter(), "{s:?} must not hold the counter");
        }
        assert!(!Stage::Inbox.holds_lock());
        for s in [Stage::Running, Stage::Suspended, Stage::Paused] {
            assert!(s.holds_lock(), "{s:?} must hold the VO lock");
        }
    }

    #[test]
    fn outbox_within_invocation_sorts_by_completion_id() {
        let mut keys: Vec<Vec<u8>> = [9u32, 1, 100, 10]
            .iter()
            .map(|c| outbox(O, &A, *c))
            .collect();
        keys.sort();
        let cids: Vec<u32> = keys.iter().map(|k| parse_outbox(k).unwrap().1).collect();
        assert_eq!(cids, vec![1, 9, 10, 100]);
    }

    #[test]
    fn retries_sort_by_due_time() {
        let mut keys: Vec<Vec<u8>> = [500u64, 1, 999_999, 42]
            .iter()
            .map(|d| ix_retry(*d, &A))
            .collect();
        keys.sort();
        let due: Vec<u64> = keys.iter().map(|k| parse_ix_retry(k).unwrap().0).collect();
        assert_eq!(due, vec![1, 42, 500, 999_999]);
        let ub = ix_retry_due_upper_bound(500);
        assert!(ix_retry(500, &A) < ub, "a due retry must be picked up");
        assert!(ix_retry(501, &A) > ub, "not yet due must not be picked up");
    }

    #[test]
    fn idempotency_keyed_by_exact_bytes_unambiguously() {
        assert_ne!(
            idempotency(&dt(), None, "ab", "c", "k", "i"),
            idempotency(&dt(), None, "a", "bc", "k", "i")
        );
        assert_ne!(
            idempotency(&dt(), None, "s", "h", "k", "i1"),
            idempotency(&dt(), None, "s", "h", "k", "i2")
        );
        assert_eq!(
            idempotency(&dt(), None, "s", "h", "k", "i"),
            idempotency(&dt(), None, "s", "h", "k", "i")
        );
    }

    #[test]
    fn attachments_scannable_by_target() {
        let p = attachments_prefix(&A);
        assert!(attachment(&A, &B, 7).starts_with(&p));
        assert!(!attachment(&B, &A, 7).starts_with(&p));
        assert_eq!(parse_attachment(&attachment(&A, &B, 7)), Some((B, 7)));
    }

    #[test]
    fn outbox_rewrite_same_key_overwrites() {
        assert_eq!(outbox(O, &A, 3), outbox(O, &A, 3));
        assert_ne!(outbox(O, &A, 3), outbox(O, &B, 3));
        assert_ne!(outbox(O, &A, 3), outbox(O, &A, 4));
    }

    /// Order indexes: byte order is the numeric order of `seq`, and each row points back to
    /// exactly the entity key — owner included.
    #[test]
    fn ix_seq_orders_by_seq_and_points_back() {
        let ek = outbox(PartitionKey(99), &A, 7);
        let mut keys: Vec<Vec<u8>> = [300u64, 2, 1, u64::MAX, 256, 0]
            .iter()
            .map(|s| ix_seq(Index::Outbox, *s, &ek))
            .collect();
        keys.sort();
        let seqs: Vec<u64> = keys
            .iter()
            .map(|k| parse_ix_seq(k, Index::Outbox).unwrap().0)
            .collect();
        assert_eq!(seqs, vec![0, 1, 2, 256, 300, u64::MAX]);
        let row = ix_seq(Index::Outbox, 5, &ek);
        let (_, back) = parse_ix_seq(&row, Index::Outbox).unwrap();
        assert_eq!(back, ek.as_slice());
        assert_eq!(owner_of(back), Some(PartitionKey(99)));
        assert!(ix_seq(Index::Outbox, 5, &ek) >= ix_seq_from(Index::Outbox, 5));
        assert!(ix_seq(Index::Outbox, 4, &ek) < ix_seq_from(Index::Outbox, 5));
        assert_eq!(
            parse_ix_seq(&ix_seq(Index::Outbox, 5, &ek), Index::Signal),
            None
        );
    }

    #[test]
    fn value_with_due_or_seq_round_trips() {
        assert_eq!(decode_due(&encode_due(9, b"x")), Some((9, &b"x"[..])));
        assert_eq!(
            decode_seq(&encode_seq(u64::MAX, b"")),
            Some((u64::MAX, &b""[..]))
        );
        assert_eq!(decode_seq(b"short"), None);
    }

    /// Every `parse_*` reads back exactly what its builder wrote.
    #[test]
    fn every_parser_round_trips_its_constructor() {
        let bob = sc("bob");
        assert_eq!(parse_invocation_status(&invocation_status(&A)), Some(A));
        assert_eq!(parse_ix_invocation(&ix_invocation(&A)), Some(A));
        assert_eq!(parse_journal_index(&journal_entry(&A, 300)), Some(300));
        assert_eq!(
            parse_state_user_key(&state(O, &dt(), Some(&bob), "S", "k", b"user/key")),
            Some(&b"user/key"[..])
        );
        assert_eq!(parse_timer(&timer(&A, 2)), Some((A, 2)));
        assert_eq!(parse_ix_timer(&ix_timer(9, &A, 2)), Some((9, A, 2)));
        assert_eq!(parse_outbox(&outbox(O, &B, 2)), Some((B, 2)));
        assert_eq!(parse_retry(&retry(&A)), Some(A));
        assert_eq!(parse_ix_retry(&ix_retry(9, &A)), Some((9, A)));
        assert_eq!(parse_purge(&purge(&A)), Some(A));
        assert_eq!(parse_ix_purge(&ix_purge(9, &A)), Some((9, A)));
        assert_eq!(parse_schedule(&schedule(&A)), Some(A));
        assert_eq!(parse_ix_schedule_due(&ix_schedule_due(9, &A)), Some((9, A)));
        assert_eq!(parse_schedule(&purge(&A)), None);
        assert!(ix_schedule_due(9, &A) < ix_schedule_due_upper_bound(9));
        assert!(ix_schedule_due(10, &A) >= ix_schedule_due_upper_bound(9));
        assert_eq!(parse_delayed(&delayed(&A, 2)), Some((A, 2)));
        assert_eq!(parse_ix_delayed(&ix_delayed(9, &A, 2)), Some((9, A, 2)));
        assert_eq!(parse_signal_outbox(&signal_outbox(&A, 2)), Some((A, 2)));
        assert_eq!(parse_attachment(&attachment(&A, &B, 2)), Some((B, 2)));
        assert_eq!(parse_vqueue_index(&vqueue_index(&A)), Some(A));
        for kind in (1..=6).filter_map(InboundKind::from_u8) {
            assert_eq!(parse_inbound(&inbound(O, kind, &B, 2)), Some((kind, B, 2)));
        }
        assert_eq!(
            parse_promise_name(&promise(O, &dt(), Some(&bob), "S", "k", "name/x/y")),
            Some("name/x/y".as_bytes())
        );
        assert_eq!(
            parse_promise_waiter(&promise_waiter(O, &dt(), Some(&bob), "S", "k", "n", &B, 2)),
            Some((&b"n"[..], B, 2))
        );
        let tricky = String::from_utf8(vec![b'x'; ID_LEN + 4]).unwrap();
        assert_eq!(
            parse_promise_waiter(&promise_waiter(O, &dt(), None, "S", "k", &tricky, &A, 1)),
            Some((tricky.as_bytes(), A, 1))
        );
    }

    /// Each parser only accepts keys of **its own table**.
    #[test]
    fn parsers_reject_keys_of_other_tables() {
        assert_eq!(parse_outbox(&signal_outbox(&A, 1)), None);
        assert_eq!(parse_signal_outbox(&outbox(O, &A, 1)), None);
        assert_eq!(parse_timer(&delayed(&A, 1)), None);
        assert_eq!(parse_delayed(&timer(&A, 1)), None);
        assert_eq!(parse_ix_timer(&ix_delayed(9, &A, 1)), None);
        assert_eq!(parse_ix_retry(&ix_purge(9, &A)), None);
        assert_eq!(parse_retry(&purge(&A)), None);
        assert_eq!(parse_journal_index(&invocation_status(&A)), None);
        assert_eq!(parse_invocation_status(&vqueue_index(&A)), None);
        assert_eq!(parse_invocation_status(&ix_invocation(&A)), None);
    }

    /// Keys truncated at every length: no panic, and parsers with a fixed tail return
    /// `None`.
    #[test]
    fn truncated_keys_never_panic() {
        let mut keys = entity_keys();
        keys.extend(index_keys());
        for key in keys {
            for n in 0..key.len() {
                let k = &key[..n];
                let _ = (table_of(k), owner_of(k), Region::of_key(k));
                assert_eq!(parse_invocation_status(k), None);
                assert_eq!(parse_journal_index(k), None);
                let _ = parse_state_user_key(k);
                assert_eq!(parse_timer(k), None);
                assert_eq!(parse_ix_timer(k), None);
                assert_eq!(parse_outbox(k), None);
                assert_eq!(parse_ix_retry(k), None);
                assert_eq!(parse_attachment(k), None);
                assert_eq!(parse_inbound(k), None);
                let _ = parse_promise_name(k);
                assert_eq!(parse_promise_waiter(k), None);
                assert_eq!(parse_ix_purge(k), None);
                assert_eq!(parse_ix_delayed(k), None);
                assert_eq!(parse_signal_outbox(k), None);
                let _ = parse_ix_vqueue(k);
                let _ = parse_ix_seq(k, Index::Outbox);
            }
        }
    }

    /// Chunk keys: all three parts read back, ordered by offset, and a stream's prefix does
    /// not swallow a stream whose name it is a prefix of (`$log` vs `$logx`).
    #[test]
    fn stream_chunk_keys_round_trip_and_sort_by_offset() {
        let k = stream_chunk(&A, "ui", 300);
        assert_eq!(parse_stream_chunk(&k), Some((A, "ui".into(), 300)));
        assert_eq!(owner_of(&k), Some(partition_key_of(&A)));
        assert!(stream_chunk(&A, "ui", 2) < stream_chunk(&A, "ui", 256));
        assert!(k.starts_with(&stream_prefix(&A, "ui")));
        assert!(k.starts_with(&streams_prefix(&A)));
        assert!(!stream_chunk(&A, "$logx", 0).starts_with(&stream_prefix(&A, "$log")));
        assert!(!stream_chunk(&B, "ui", 0).starts_with(&streams_prefix(&A)));
        assert_eq!(parse_stream_chunk(&journal_entry(&A, 0)), None);
    }

    /// Artifact rows: the key reads back, under the owner's prefix; the value round-trips
    /// intact, truncated or an unknown version ⇒ `None`.
    #[test]
    fn artifact_rows_round_trip_and_read_lax() {
        let k = artifact(&A, &[9; 32]);
        assert_eq!(parse_artifact(&k), Some((A, [9; 32])));
        assert_eq!(owner_of(&k), Some(partition_key_of(&A)));
        assert!(k.starts_with(&artifacts_prefix(&A)));
        assert!(!artifact(&B, &[9; 32]).starts_with(&artifacts_prefix(&A)));
        assert_eq!(parse_artifact(&stream_chunk(&A, "x", 0)), None);
        let row = ArtifactRow {
            epoch: u64::MAX - 3,
            size: 16 << 20,
            content_type: "image/png".into(),
        };
        let raw = row.encode();
        assert_eq!(ArtifactRow::decode(&raw), Some(row));
        assert_eq!(ArtifactRow::decode(&raw[..raw.len() - 1]), None);
        let mut v2 = raw.clone();
        v2[0] = 2;
        assert_eq!(ArtifactRow::decode(&v2), None);
    }

    /// Chunk values: all three record kinds round-trip intact; truncated, unknown version,
    /// unknown kind ⇒ `None`, no panic.
    #[test]
    fn stream_chunk_values_round_trip_and_read_lax() {
        let chunk = StreamChunk {
            total_bytes: 1 << 40,
            dropped: 3,
            records: vec![
                ChunkRecord {
                    offset: 7,
                    step: 2,
                    attempt: 0,
                    k: 0,
                    body: ChunkBody::Data(bytes::Bytes::from_static(b"{\"tok\":1}")),
                },
                ChunkRecord {
                    offset: 8,
                    step: 2,
                    attempt: 1,
                    k: 0,
                    body: ChunkBody::Superseded {
                        step: 2,
                        attempt: 0,
                    },
                },
                ChunkRecord {
                    offset: 9,
                    step: u32::MAX,
                    attempt: 1,
                    k: 300,
                    body: ChunkBody::Gap { dropped: 12 },
                },
            ],
        };
        let raw = chunk.encode(7);
        assert_eq!(StreamChunk::decode(7, &raw), Some(chunk.clone()));
        assert_eq!(chunk.next_offset(7), 10);
        assert_eq!(StreamChunk::default().next_offset(5), 5);
        for cut in 0..raw.len() {
            // Cut midway: either None, or a valid prefix of the records.
            if let Some(got) = StreamChunk::decode(7, &raw[..cut]) {
                assert!(got.records.len() < chunk.records.len(), "cut {cut}");
            }
        }
        let mut bad = raw.clone();
        bad[0] = 2;
        assert_eq!(StreamChunk::decode(7, &bad), None);
        assert_eq!(StreamChunk::decode(7, &[1, 0, 0, 9, 0, 0, 0, 0]), None);
    }

    #[test]
    fn every_table_is_decodable() {
        for t in Table::ALL {
            assert_eq!(Table::from_u8(t as u8), Some(t));
        }
        for retired in [0x00, 0x05, 0x0A, 0x11, 0x13, 0x14] {
            assert_eq!(Table::from_u8(retired), None, "{retired:#x}");
        }
    }
}
