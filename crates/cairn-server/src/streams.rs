// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Invocation streams on the range leader.
//!
//! One [`RangeStreams`] per leader **term** of a range: the turn layer
//! (`turn/sdk.rs`) pushes `StreamAppend` records in, SSE readers
//! (`ingress/streams.rs`) read them out. It lives in RAM and is lost when the term ends —
//! exactly the semantics of an **ephemeral stream**: the attempt producing tokens is
//! cancelled at the same moment and runs again with a new `attempt`, while the full output
//! is already in the journal. Readers get `moved` (+ `reset` for ephemeral streams) and go
//! looking for the new leader.
//!
//! **Locking.** One `Mutex` per range, never held across an `.await`: the engine thread
//! holds it for one record push, an SSE reader holds it for copying one chunk. No
//! node-wide lock. The node's RAM limit is **one atomic counter** ([`NodeRam`]) that every
//! range subtracts from.
//!
//! **Identity and deduplication (AP-11).** Each record carries `(step, attempt, k)`
//! assigned by the SDK. `step` is the record's step — the position of the running closure's
//! `Run` command, or the cursor outside closures — so parallel closures are steps
//! interleaved on one stream. Within **each** step, `(attempt, k)` strictly increases (a new
//! attempt of the same step has a larger `attempt`); a record not greater than the last
//! record **of that step** is a duplicate — dropped, not counted as lost. For a new attempt
//! of a step the runtime inserts a [`Body::Superseded`] for that step's previous attempt.
//! Offsets are assigned by the runtime, contiguous.
//!
//! **Limits.** [`MAX_STREAMS`] streams per invocation; an ephemeral stream keeps at most
//! [`EPHEMERAL_BYTES`] (a ring: old records are overwritten, a reader lagging too far sees a
//! `gap`); the node's RAM is limited ([`NodeRam`]), and each tenant only holds its own share
//! ([`TenantShare`], `max-stream-ram=`). Over a limit the record is dropped, counted in
//! `cairnflow_stream_dropped_total`, and a `gap` is inserted (merged when adjacent). An
//! invocation **never** fails because of a stream.
//!
//! **Durable streams.** Records of durable streams (`$log`, `$trace`,
//! `durable: true`) also **ride along** on every batch of the range writer ([`StreamRider`],
//! `cairn_store::Rider`): each batch carries at most one chunk per stream — the records from
//! `carried` to the end. No separate time-based commit (AP-16); only when the range's
//! not-yet-carried part exceeds [`FLUSH_BYTES`] does the turn layer commit a batch with
//! only `Streams` rows, nobody waiting ([`RangeStreams::wants_flush`]). A batch submitted at
//! `seq` ⇒ its records are durable once `durable_seq ≥ seq` — only then do readers see
//! `durable: true` (AP-12). A new term seeds the offset, the last identities and the
//! accumulated counters from the last chunk in the Db ([`RangeStreams::seed`]); the part not
//! yet carried when a term ends goes with `seal`'s fence (AP-15).

use {
    bytes::Bytes,
    cairn_protocol::pb,
    cairn_store::{InvocationId, TenantId},
    std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard,
        },
        time::Duration,
    },
    tokio::sync::watch,
};

/// Maximum number of streams per invocation.
pub const MAX_STREAMS: usize = 16;
/// Bytes an ephemeral stream keeps in RAM; beyond it the oldest record is overwritten.
pub const EPHEMERAL_BYTES: usize = 1 << 20;
/// A sealed stream stays readable (from RAM) this long, then `410`.
pub const STREAM_LINGER: Duration = Duration::from_secs(60);
/// Default RAM limit of every `StreamHub` on a node.
pub const DEFAULT_NODE_RAM: u64 = 256 << 20;
/// Maximum length of a stream name (bytes).
pub const MAX_NAME: usize = 128;
/// Estimated RAM cost of one record besides its payload.
const RECORD_OVERHEAD: usize = 48;
/// Maximum `$log` payload bytes per invocation, over every attempt.
pub const LOG_MAX_BYTES: u64 = 1 << 20;
/// Maximum `$trace` payload bytes per invocation, over every attempt.
pub const TRACE_MAX_BYTES: u64 = 1 << 20;
/// Maximum payload bytes of **every** user durable stream
/// (`durable: true`) added together, per invocation.
pub const DURABLE_MAX_BYTES: u64 = 8 << 20;
/// The **not-yet-carried** part of durable streams on a range above this ⇒ flush when full:
/// a batch with only `Streams` rows — ≤ 1 PUT per 256 KiB of logs.
pub const FLUSH_BYTES: u64 = 256 << 10;

/// The node's stream configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    /// RAM limit of every `StreamHub` on the node.
    pub node_ram: u64,
    /// Limit on concurrent SSE connections of the node; beyond it ⇒ `429` + `Retry-After`.
    pub sse_max: usize,
    /// Limit on long-lived connections (SSE and waiting attaches) of **one tenant** on the node
    /// (`--max-open-streams-per-tenant`); beyond it ⇒ `429
    /// too_many_open_streams`. `0` = no limit.
    pub tenant_max: usize,
    /// `:keepalive` interval of an idle SSE connection.
    pub keepalive: Duration,
    /// Maximum lifetime of an SSE connection; the client reconnects with `Last-Event-ID`.
    pub max_life: Duration,
    /// An invocation suspended waiting on something external (`Waiting`) whose stream receives
    /// no record for this long: the ephemeral stream's records are released, only the book is
    /// kept (next offset, deduplication). An ephemeral stream only promises to keep **the
    /// running step**; while suspended no step is running. The grace period lets a reader that
    /// is keeping up finish reading the step that just finished.
    pub idle_release: Duration,
    /// Interval for re-checking the authorization of an open SSE connection: a revoked
    /// key, a logged-out session, a tenant being deleted ⇒ the stream is closed
    /// within ≤ one tick.
    pub recheck: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            node_ram: DEFAULT_NODE_RAM,
            sse_max: 1024,
            tenant_max: 256,
            keepalive: Duration::from_secs(15),
            max_life: Duration::from_secs(600),
            idle_release: STREAM_LINGER,
            recheck: Duration::from_secs(30),
        }
    }
}

/// System streams: names starting with `$`, always durable.
pub const LOG: &str = "$log";
pub const TRACE: &str = "$trace";

/// Identity of one record in a stream (AP-11).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ident {
    /// Journal length the SDK knew at append time.
    pub step: u32,
    /// The turn's `retry_count_since_last_stored_entry`.
    pub attempt: u32,
    /// Order within the step.
    pub k: u32,
}

/// The content of one record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    /// The writer's payload (the SDK sends JSON).
    Data(Bytes),
    /// `dropped` records dropped because of a limit right before this point.
    Gap { dropped: u64 },
    /// Attempt `attempt` of step `step` was replaced by a new attempt: its records (before
    /// this one) belong to an abandoned branch.
    Superseded { step: u32, attempt: u32 },
}

/// One received record, with the offset assigned by the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub ident: Ident,
    pub body: Body,
}

impl Record {
    /// A record read back from a chunk in the Db.
    pub fn from_chunk(r: cairn_store::keys::ChunkRecord) -> Self {
        use cairn_store::keys::ChunkBody;
        Self {
            offset: r.offset,
            ident: Ident {
                step: r.step,
                attempt: r.attempt,
                k: r.k,
            },
            body: match r.body {
                ChunkBody::Data(b) => Body::Data(b),
                ChunkBody::Gap { dropped } => Body::Gap { dropped },
                ChunkBody::Superseded { step, attempt } => Body::Superseded { step, attempt },
            },
        }
    }

    fn to_chunk(&self) -> cairn_store::keys::ChunkRecord {
        use cairn_store::keys::ChunkBody;
        cairn_store::keys::ChunkRecord {
            offset: self.offset,
            step: self.ident.step,
            attempt: self.ident.attempt,
            k: self.ident.k,
            body: match &self.body {
                Body::Data(b) => ChunkBody::Data(b.clone()),
                Body::Gap { dropped } => ChunkBody::Gap { dropped: *dropped },
                Body::Superseded { step, attempt } => ChunkBody::Superseded {
                    step: *step,
                    attempt: *attempt,
                },
            },
        }
    }

    fn cost(&self) -> usize {
        RECORD_OVERHEAD
            + match &self.body {
                Body::Data(b) => b.len(),
                _ => 0,
            }
    }
}

/// The RAM limit of every `StreamHub` on one node: one atomic counter, no lock
/// — plus **each tenant's share** ([`TenantShare`]): one tenant cannot fill the whole
/// node's stream RAM.
#[derive(Debug)]
pub struct NodeRam {
    used: AtomicU64,
    cap: u64,
    /// The share of each tenant seen on the node. Locked **only** the first time an
    /// invocation is bound to a tenant within a term ([`RangeStreams::bind_tenant`]),
    /// not on every record — counting and limit checks are atomic on the share already taken.
    /// One entry per tenant ever seen: the number of tenants of a cell is bounded.
    shares: Mutex<BTreeMap<TenantId, Arc<TenantShare>>>,
    /// The share of streams not bound to a tenant: no limit of its own (node limit only).
    open: Arc<TenantShare>,
}

/// One tenant's share of stream RAM on the node: bytes held
/// and the limit (`max-stream-ram=`; absent = [`NodeRam::default_share`]).
#[derive(Debug)]
pub struct TenantShare {
    used: AtomicU64,
    cap: AtomicU64,
}

impl TenantShare {
    fn new(cap: u64) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicU64::new(0),
            cap: AtomicU64::new(cap),
        })
    }

    /// Bytes held.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
}

impl NodeRam {
    pub fn new(cap: u64) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicU64::new(0),
            cap,
            shares: Mutex::new(BTreeMap::new()),
            open: TenantShare::new(u64::MAX),
        })
    }

    /// Default limit of a tenant that does not declare `max-stream-ram=`: 1/8 of the node's
    /// RAM; the `default` tenant (self-hosting with one tenant) keeps the whole node limit.
    pub fn default_share(&self, tenant: &TenantId) -> u64 {
        if tenant.is_default() {
            self.cap
        } else {
            self.cap / 8
        }
    }

    /// The share of `tenant`, limit `max_ram` (absent = default). Locks the share book —
    /// see [`Self::shares`].
    fn share_of(&self, tenant: &TenantId, max_ram: Option<u64>) -> Arc<TenantShare> {
        let cap = max_ram.unwrap_or_else(|| self.default_share(tenant));
        let mut shares = self.shares.lock().unwrap_or_else(|e| e.into_inner());
        let share = shares
            .entry(tenant.clone())
            .or_insert_with(|| TenantShare::new(cap));
        share.cap.store(cap, Ordering::Release);
        Arc::clone(share)
    }

    /// Take `n` bytes of `share` if there is room in both the tenant's share and the node.
    fn take(&self, share: &TenantShare, n: u64) -> bool {
        let cap = share.cap.load(Ordering::Acquire);
        if share
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                (u + n <= cap).then_some(u + n)
            })
            .is_err()
        {
            return false;
        }
        let ok = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                (u + n <= self.cap).then_some(u + n)
            })
            .is_ok();
        if ok {
            crate::metrics::agent::add(&crate::metrics::agent::STREAM_RAM_BYTES, n);
        } else {
            sub(&share.used, n);
        }
        ok
    }

    /// Take `n` bytes regardless of limits (`gap` records — bounded by merging adjacent ones).
    fn force(&self, share: &TenantShare, n: u64) {
        share.used.fetch_add(n, Ordering::AcqRel);
        self.used.fetch_add(n, Ordering::AcqRel);
        crate::metrics::agent::add(&crate::metrics::agent::STREAM_RAM_BYTES, n);
    }

    fn give(&self, share: &TenantShare, n: u64) {
        sub(&share.used, n);
        sub(&self.used, n);
        crate::metrics::agent::sub(&crate::metrics::agent::STREAM_RAM_BYTES, n);
    }

    /// Bytes held.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
}

/// Saturating subtraction on an atomic counter.
fn sub(c: &AtomicU64, n: u64) {
    let _ = c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
        Some(u.saturating_sub(n))
    });
}

/// The node's book: a shared RAM limit, a new hub for each term of a range.
#[derive(Clone, Debug)]
pub struct StreamBook {
    ram: Arc<NodeRam>,
    idle_release: Duration,
}

impl Default for StreamBook {
    fn default() -> Self {
        Self::new(DEFAULT_NODE_RAM)
    }
}

impl StreamBook {
    pub fn new(cap: u64) -> Self {
        Self {
            ram: NodeRam::new(cap),
            idle_release: STREAM_LINGER,
        }
    }

    /// Like [`Self::new`], with [`StreamConfig::idle_release`].
    pub fn with_idle_release(mut self, idle: Duration) -> Self {
        self.idle_release = idle;
        self
    }

    /// [`StreamConfig::idle_release`] of the hubs built from this book.
    pub fn idle_release(&self) -> Duration {
        self.idle_release
    }

    /// The hub of one term. `durable` = the `streams/table` gate as at range open
    /// (`Engine::streams`): when off, every stream — including `$log`/`$trace` and
    /// `{durable: true}` streams — only lives in RAM, and no `Streams` row is ever written.
    pub fn term(&self, durable: bool) -> Arc<RangeStreams> {
        let mut hub = RangeStreams::new(Arc::clone(&self.ram), durable);
        hub.idle_release = self.idle_release;
        Arc::new(hub)
    }

    pub fn ram(&self) -> &NodeRam {
        &self.ram
    }
}

struct Stream {
    durable: bool,
    /// RAM share of the owning tenant — every byte of the stream is subtracted here
    /// and from the node limit.
    share: Arc<TenantShare>,
    records: VecDeque<Record>,
    /// RAM bytes `records` holds (already subtracted from [`NodeRam`]).
    bytes: usize,
    /// The next offset to assign.
    next: u64,
    /// The last record received (any step) — the step of runtime records
    /// ([`RangeStreams::append_runtime`]).
    last: Option<Ident>,
    /// The last `(attempt, k)` of each step — deduplication (AP-11).
    steps: BTreeMap<u32, (u32, u32)>,
    /// Steps that went through `resume` ([`RangeStreams::reset`]) → the old `attempt`: the
    /// first arriving record of that step opens a new run (`superseded`, then accepted), even
    /// though its identity is not greater — the lifecycle core resets `attempt` to 0.
    reset: BTreeMap<u32, u32>,
    /// The writer has closed the stream.
    closed: bool,
    /// When it was sealed (invocation finished, `paused`, `restart`).
    sealed: Option<tokio::time::Instant>,
    /// Durable streams: records with offsets below this are in a submitted batch.
    carried: u64,
    /// Durable streams: records with offsets below this are durable (AP-12).
    durable_upto: u64,
    /// Payload bytes received, accumulated over every term (seeded from the last chunk).
    total_bytes: u64,
    /// Records dropped because of limits, accumulated over every term.
    dropped_total: u64,
}

impl Stream {
    fn new(durable: bool, share: Arc<TenantShare>) -> Self {
        Self {
            durable,
            share,
            records: VecDeque::new(),
            bytes: 0,
            next: 0,
            last: None,
            steps: BTreeMap::new(),
            reset: BTreeMap::new(),
            closed: false,
            sealed: None,
            carried: 0,
            durable_upto: 0,
            total_bytes: 0,
            dropped_total: 0,
        }
    }

    /// A durable stream continues from what was durable in the previous term: the next
    /// offset, the last identities and the accumulated counters are read from the last chunk.
    fn resumed(first: u64, last: &cairn_store::keys::StreamChunk, share: Arc<TenantShare>) -> Self {
        let next = last.next_offset(first);
        let mut s = Self::new(true, share);
        s.next = next;
        s.carried = next;
        s.durable_upto = next;
        s.total_bytes = last.total_bytes;
        s.dropped_total = last.dropped;
        s.last = last.records.last().map(|r| Ident {
            step: r.step,
            attempt: r.attempt,
            k: r.k,
        });
        // Every step of the last chunk, not just the last record: parallel closures
        // interleave right up to the end of the chunk. A step older than the last chunk that
        // sends again in this term is a new attempt (a new `attempt`) — not a duplicate.
        // In order: `superseded` opens a new run of the step, so the identities before it no
        // longer block — even when the new run has a smaller `attempt`
        // (after `resume`).
        for r in &last.records {
            if let cairn_store::keys::ChunkBody::Superseded { step, .. } = r.body {
                s.steps.remove(&step);
            }
            s.note(Ident {
                step: r.step,
                attempt: r.attempt,
                k: r.k,
            });
        }
        s
    }

    /// `resume`: every step seen moves into [`Self::reset`].
    fn reset_steps(&mut self) {
        for (step, (attempt, _)) in std::mem::take(&mut self.steps) {
            self.reset.insert(step, attempt);
        }
    }

    /// Record a received identity for its step.
    fn note(&mut self, ident: Ident) {
        let at = self.steps.entry(ident.step).or_default();
        *at = (*at).max((ident.attempt, ident.k));
    }

    /// RAM cost of the records not yet carried.
    fn uncarried_cost(&self, upto: u64) -> u64 {
        let first = self.first();
        self.records
            .iter()
            .skip(self.carried.saturating_sub(first) as usize)
            .take_while(|r| r.offset < upto)
            .map(|r| r.cost() as u64)
            .sum()
    }

    /// The chunk from `carried` to the end — `None` when nothing is left uncarried.
    fn pending_chunk(&self) -> Option<(u64, cairn_store::keys::StreamChunk)> {
        if !self.durable || self.next <= self.carried {
            return None;
        }
        let first = self.first();
        let records: Vec<_> = self
            .records
            .iter()
            .skip(self.carried.saturating_sub(first) as usize)
            .map(Record::to_chunk)
            .collect();
        let at = records.first()?.offset;
        Some((
            at,
            cairn_store::keys::StreamChunk {
                total_bytes: self.total_bytes,
                dropped: self.dropped_total,
                records,
            },
        ))
    }

    fn first(&self) -> u64 {
        self.records.front().map_or(self.next, |r| r.offset)
    }

    /// Add a record; `paid` = its RAM already taken ([`NodeRam::take`]).
    fn push(&mut self, ram: &NodeRam, ident: Ident, body: Body, paid: bool) {
        let rec = Record {
            offset: self.next,
            ident,
            body,
        };
        let cost = rec.cost();
        if !paid {
            ram.force(&self.share, cost as u64);
        }
        if let Body::Data(b) = &rec.body {
            self.total_bytes += b.len() as u64;
        }
        self.bytes += cost;
        self.next += 1;
        self.records.push_back(rec);
    }

    /// Drop a record because of a limit: merge into the last `gap` if there is one, otherwise add one.
    fn drop_one(&mut self, ram: &NodeRam, ident: Ident) {
        crate::metrics::agent::bump(&crate::metrics::agent::STREAM_DROPPED);
        self.dropped_total += 1;
        // Merge into the last `gap` — unless it has already been carried: a submitted chunk
        // no longer changes, so a new `gap`.
        if let Some(Record {
            offset,
            body: Body::Gap { dropped },
            ..
        }) = self.records.back_mut()
        {
            if !self.durable || *offset >= self.carried {
                *dropped += 1;
                return;
            }
        }
        self.push(ram, ident, Body::Gap { dropped: 1 }, false);
    }

    /// Overwrite the oldest records until back under the limit. A durable stream only
    /// overwrites records **already durable** — that part can be read back from the Db.
    fn evict(&mut self, ram: &NodeRam) {
        while self.bytes > EPHEMERAL_BYTES {
            if self.durable
                && self
                    .records
                    .front()
                    .is_none_or(|r| r.offset >= self.durable_upto)
            {
                break;
            }
            let Some(r) = self.records.pop_front() else {
                break;
            };
            let c = r.cost();
            self.bytes -= c;
            ram.give(&self.share, c as u64);
        }
    }

    fn release(&mut self, ram: &NodeRam) {
        ram.give(&self.share, self.bytes as u64);
        self.bytes = 0;
        self.records.clear();
    }

    /// Release records nobody needs to keep in RAM any more ([`RangeStreams::tick`]): every
    /// record of an ephemeral stream, the durable part of a durable stream. The book (`next`,
    /// `steps`, `closed`, counters) is kept as is. `true` if anything was released.
    fn release_settled(&mut self, ram: &NodeRam) -> bool {
        let before = self.records.len();
        while let Some(r) = self.records.front() {
            if self.durable && r.offset >= self.durable_upto {
                break;
            }
            let c = r.cost();
            self.records.pop_front();
            self.bytes -= c;
            ram.give(&self.share, c as u64);
        }
        self.records.len() != before
    }
}

/// Outcome of one frame push.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Appended {
    pub accepted: u32,
    /// Duplicates (identity not greater than the last record) — dropped, not lost.
    pub duplicates: u32,
    /// Dropped because of a limit (the 17th stream, the node's RAM, a closed stream, a bad name).
    pub dropped: u32,
    /// Tokens of the `$trace` spans just **closed** (an `end` record received):
    /// `(model, input, output)` — the turn layer adds them to metering.
    pub tokens: Vec<(Option<String>, u64, u64)>,
}

/// Open `$trace` spans remember their model, at most this many per range (the model is
/// declared at `start`, the tokens at `end`).
const OPEN_MODELS_MAX: usize = 4096;

/// One chunk read out for a reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    pub records: Vec<Record>,
    /// The offset the reader asks for next.
    pub next: u64,
    /// Number of records the reader missed because they were overwritten (an ephemeral
    /// stream lagging too far): `> 0` ⇒ the reader sees a `gap` before `records`.
    pub missed: u64,
    /// Sealed or closed **and** nothing left after `next`.
    pub ended: bool,
    pub durable: bool,
    /// Durable streams: records with offsets below this are durable — only they are reported
    /// `durable: true` (AP-12). Ephemeral streams: 0.
    pub durable_upto: u64,
}

/// The state of one stream for `GET /invocations/{id}` (`StreamInfo`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub name: String,
    pub durable: bool,
    /// The next offset (the number of records assigned).
    pub next: u64,
    pub sealed: bool,
}

/// A submitted batch carrying stream chunks: `(seq, [(id, name, next offset)])`.
type Flight = (u64, Vec<(InvocationId, String, u64)>);

#[derive(Default)]
struct Inner {
    invs: BTreeMap<InvocationId, BTreeMap<String, Stream>>,
    /// The term has ended: the hub only reports `moved` to readers.
    ended: bool,
    /// Submitted batches not yet known durable, by increasing `seq`.
    flights: VecDeque<Flight>,
    /// Models of open `$trace` spans, bounded by [`OPEN_MODELS_MAX`].
    models: BTreeMap<(InvocationId, String), String>,
    models_order: VecDeque<(InvocationId, String)>,
    /// Invocations suspended waiting on something external since this time
    /// ([`RangeStreams::waiting`]); cleared on a new record or sealing.
    waiting: BTreeMap<InvocationId, tokio::time::Instant>,
    /// Invocations just `resume`d in this term ([`RangeStreams::reset`]): durable streams
    /// seeded from the Db afterwards also open a new run. Cleared at the next sealing.
    resets: BTreeSet<InvocationId>,
    /// The RAM share of the tenant owning each invocation ([`RangeStreams::bind_tenant`]).
    /// Absent ⇒ the node's open share.
    owners: BTreeMap<InvocationId, Arc<TenantShare>>,
}

/// The durable `seq` of the range writer — `WriterHealth::durable_seq`.
type DurableSeq = Box<dyn Fn() -> u64 + Send + Sync>;

/// The streams of every invocation on one range, within one leader term.
pub struct RangeStreams {
    inner: Mutex<Inner>,
    /// Changes after every change — SSE readers wait on it, then read again.
    tick: watch::Sender<u64>,
    ram: Arc<NodeRam>,
    /// RAM cost of every **not-yet-carried** durable stream record on the range. The fast path
    /// of [`StreamRider::pending`] (no lock when 0) and the flush-when-full mark.
    uncarried: AtomicU64,
    /// Attached at the start of the term ([`Self::attach`]); absent ⇒ no record is durable.
    durable_seq: std::sync::OnceLock<DurableSeq>,
    /// Wall-clock mark (ms) of the next `log_flush_ms` flush; `u64::MAX`
    /// = none scheduled.
    flush_due_ms: AtomicU64,
    /// The `streams/table` gate is on ([`StreamBook::term`]): durable streams are allowed.
    durable_ok: bool,
    /// [`StreamConfig::idle_release`].
    idle_release: Duration,
}

impl std::fmt::Debug for RangeStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeStreams").finish_non_exhaustive()
    }
}

/// A valid name for a writer: not empty, ≤ [`MAX_NAME`]; a leading `$` only for
/// the two system streams.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && (!name.starts_with('$') || name == LOG || name == TRACE)
}

/// System streams (`$log`, `$trace`) — always durable.
pub fn is_system(name: &str) -> bool {
    name == LOG || name == TRACE
}

impl RangeStreams {
    pub fn new(ram: Arc<NodeRam>, durable_ok: bool) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            tick: watch::channel(0).0,
            ram,
            uncarried: AtomicU64::new(0),
            durable_seq: std::sync::OnceLock::new(),
            flush_due_ms: AtomicU64::new(u64::MAX),
            durable_ok,
            idle_release: STREAM_LINGER,
        }
    }

    /// Schedule a flush by `log_flush_ms` = `every_ms`: at the **next multiple**
    /// of `every_ms` on the node's clock, so that ranges on the same tick fall on the same
    /// mark. An earlier schedule wins.
    pub fn arm_flush(&self, every_ms: u64, now_ms: u64) {
        if every_ms == 0 {
            return;
        }
        let due = (now_ms / every_ms + 1).saturating_mul(every_ms);
        self.flush_due_ms.fetch_min(due, Ordering::AcqRel);
    }

    /// The `log_flush_ms` flush is due and durable records remain uncarried: clear the
    /// schedule, return `true` — the caller commits an empty batch (the rider fills it).
    pub fn take_flush_due(&self, now_ms: u64) -> bool {
        if self.flush_due_ms.load(Ordering::Acquire) > now_ms {
            return false;
        }
        self.flush_due_ms.store(u64::MAX, Ordering::Release);
        self.uncarried.load(Ordering::Acquire) > 0
    }

    /// Attach the durable `seq` source of this term's writer (once).
    pub fn attach(&self, durable_seq: impl Fn() -> u64 + Send + Sync + 'static) {
        let _ = self.durable_seq.set(Box::new(durable_seq));
    }

    /// A batch became durable ⇒ raise `durable_upto`, then overwrite the durable part above the RAM limit.
    fn refresh_locked(&self, inner: &mut Inner) {
        let Some(seq) = self.durable_seq.get().map(|f| f()) else {
            return;
        };
        while inner.flights.front().is_some_and(|(s, _)| *s <= seq) {
            let Some((_, marks)) = inner.flights.pop_front() else {
                break;
            };
            for (id, name, next) in marks {
                if let Some(s) = inner.invs.get_mut(&id).and_then(|m| m.get_mut(&name)) {
                    s.durable_upto = s.durable_upto.max(next);
                    s.evict(&self.ram);
                }
            }
        }
    }

    /// Durable stream `name` of `id` is not in this term's hub yet: the turn layer must read
    /// the last chunk in the Db ([`Self::seed`]) before pushing records — assigning offsets
    /// from 0 would overwrite the previous term's chunks.
    pub fn needs_seed(&self, id: &InvocationId, name: &str, durable: bool) -> bool {
        if !self.durable_ok || !(durable || is_system(name)) || !valid_name(name) {
            return false;
        }
        let inner = self.lock();
        !inner.ended
            && !inner
                .invs
                .get(id)
                .is_some_and(|m| m.contains_key(name) || m.len() >= MAX_STREAMS)
    }

    /// Seed a durable stream from its last chunk in the Db (`PartitionWriter::stream_tail_unbounded`):
    /// `None` ⇒ a new stream. A corrupt last chunk ⇒ the stream is closed for this term
    /// (new records are dropped and counted): without knowing the next offset nothing can be
    /// written over it.
    pub fn seed(
        &self,
        id: &InvocationId,
        name: &str,
        tail: Option<(u64, Option<cairn_store::keys::StreamChunk>)>,
    ) {
        let mut inner = self.lock();
        if inner.ended {
            return;
        }
        let share = self.share_locked(&inner, id);
        let streams = inner.invs.entry(*id).or_default();
        if streams.contains_key(name) || streams.len() >= MAX_STREAMS {
            return;
        }
        let s = match tail {
            None => Stream::new(true, share),
            Some((first, Some(last))) => Stream::resumed(first, &last, share),
            Some((_, None)) => {
                let mut s = Stream::new(true, share);
                s.closed = true;
                s
            }
        };
        let mut s = s;
        // Seeded after a `resume` within this term: the last chunk belongs to the previous run.
        if inner.resets.contains(id) {
            s.reset_steps();
        }
        inner
            .invs
            .entry(*id)
            .or_default()
            .insert(name.to_string(), s);
    }

    /// The not-yet-carried part of durable streams on the range has exceeded [`FLUSH_BYTES`].
    pub fn wants_flush(&self) -> bool {
        self.uncarried.load(Ordering::Acquire) >= FLUSH_BYTES
    }

    /// The uncarried chunks, one per stream — see `cairn_store::Rider::pending`.
    fn pending_chunks(&self) -> Vec<cairn_store::RiderChunk> {
        if self.uncarried.load(Ordering::Acquire) == 0 {
            return Vec::new();
        }
        let inner = self.lock();
        let mut out = Vec::new();
        for (id, streams) in &inner.invs {
            for (name, s) in streams {
                if let Some((first, chunk)) = s.pending_chunk() {
                    out.push(cairn_store::RiderChunk {
                        id: *id,
                        name: name.clone(),
                        first,
                        chunk,
                    });
                }
            }
        }
        out
    }

    /// A batch submitted at `seq` carried `chunks` — see `cairn_store::Rider::carried`.
    fn carried(&self, seq: u64, chunks: &[cairn_store::RiderChunk]) {
        if !chunks.is_empty() {
            // ≤ 1 row per stream per commit — counted so benches can see it.
            let mut streams: Vec<(&cairn_store::InvocationId, &str)> =
                chunks.iter().map(|c| (&c.id, c.name.as_str())).collect();
            streams.sort_unstable();
            streams.dedup();
            use crate::metrics::agent::{
                STREAM_ROWS_CARRIED, STREAM_ROWS_EXTRA, STREAM_ROW_COMMITS,
            };
            STREAM_ROWS_CARRIED.fetch_add(chunks.len() as u64, Ordering::Relaxed);
            STREAM_ROW_COMMITS.fetch_add(1, Ordering::Relaxed);
            STREAM_ROWS_EXTRA.fetch_add((chunks.len() - streams.len()) as u64, Ordering::Relaxed);
        }
        let mut inner = self.lock();
        let mut marks = Vec::with_capacity(chunks.len());
        let mut freed = 0u64;
        for c in chunks {
            let next = c.chunk.next_offset(c.first);
            if let Some(s) = inner.invs.get_mut(&c.id).and_then(|m| m.get_mut(&c.name)) {
                if next > s.carried {
                    freed += s.uncarried_cost(next);
                    s.carried = next;
                }
            }
            marks.push((c.id, c.name.clone(), next));
        }
        let _ = self
            .uncarried
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                Some(u.saturating_sub(freed))
            });
        inner.flights.push_back((seq, marks));
        self.refresh_locked(&mut inner);
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // No `await` while holding it, so it is only poisoned when a push panics midway —
        // what was pushed is still consistent (each record is
        // one `push_back`).
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn changed(&self) {
        self.tick.send_modify(|t| *t = t.wrapping_add(1));
    }

    /// Readers wait for changes on this channel.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tick.subscribe()
    }

    /// Like [`Self::append`], with the tenant's limit on **user** streams per invocation
    /// (`max-streams=`): a new stream over the limit is dropped
    /// like stream number [`MAX_STREAMS`] + 1 — an invocation never fails because of a stream.
    /// System streams (`$log`, `$trace`) do not count.
    pub fn append_capped(
        &self,
        id: &InvocationId,
        msg: pb::StreamAppendMessage,
        cap: Option<u32>,
    ) -> Appended {
        if self.over_user_cap(id, &msg.name, cap) {
            let n = msg.records.len() as u32;
            crate::metrics::agent::add(&crate::metrics::agent::STREAM_DROPPED, n as u64);
            return Appended {
                dropped: n,
                ..Appended::default()
            };
        }
        self.append(id, msg)
    }

    /// A **new** user stream `name` would exceed the tenant's limit `cap`.
    pub fn over_user_cap(&self, id: &InvocationId, name: &str, cap: Option<u32>) -> bool {
        let Some(cap) = cap else {
            return false;
        };
        if is_system(name) {
            return false;
        }
        let inner = self.lock();
        inner.invs.get(id).is_some_and(|m| {
            !m.contains_key(name) && m.keys().filter(|n| !is_system(n)).count() >= cap as usize
        })
    }

    /// Bind `id` to the stream RAM share of `tenant` on the node,
    /// limit `max_ram` (`max-stream-ram=`; absent = default). Call before any push
    /// or seed of `id`; an unbound `id` uses the node's shared limit.
    pub fn bind_tenant(&self, id: &InvocationId, tenant: &TenantId, max_ram: Option<u64>) {
        let mut inner = self.lock();
        match inner.owners.get(id) {
            // Already bound: only update the limit (the flag may change between two turns).
            Some(share) => share.cap.store(
                max_ram.unwrap_or_else(|| self.ram.default_share(tenant)),
                Ordering::Release,
            ),
            None => {
                let share = self.ram.share_of(tenant, max_ram);
                inner.owners.insert(*id, share);
            }
        }
    }

    /// The RAM share of `id` (bound), otherwise the node's open share.
    fn share_locked(&self, inner: &Inner, id: &InvocationId) -> Arc<TenantShare> {
        Arc::clone(inner.owners.get(id).unwrap_or(&self.ram.open))
    }

    /// Push one `StreamAppend` frame of `id`.
    pub fn append(&self, id: &InvocationId, msg: pb::StreamAppendMessage) -> Appended {
        let mut out = Appended::default();
        let n = msg.records.len() as u32;
        let mut inner = self.lock();
        // A new record ⇒ a step is running: no longer "suspended and idle".
        inner.waiting.remove(id);
        if inner.ended || !valid_name(&msg.name) {
            out.dropped = n;
            crate::metrics::agent::add(&crate::metrics::agent::STREAM_DROPPED, n as u64);
            return out;
        }
        // The model book of open spans: borrowed separately from `inner` while
        // the stream is borrowed, given back at the end of the function.
        let mut models = Models {
            map: std::mem::take(&mut inner.models),
            order: std::mem::take(&mut inner.models_order),
        };
        let share = self.share_locked(&inner, id);
        let streams = inner.invs.entry(*id).or_default();
        if !streams.contains_key(&msg.name) && streams.len() >= MAX_STREAMS {
            out.dropped = n;
            crate::metrics::agent::add(&crate::metrics::agent::STREAM_DROPPED, n as u64);
            models.put_back(&mut inner);
            return out;
        }
        let durable = (msg.durable || is_system(&msg.name)) && self.durable_ok;
        // Byte limits of durable streams: `$log`/`$trace` each have their own limit; user
        // durable streams share one limit. The counters accumulate over every term
        // (seeded from the last chunk), so retries do not create extra quota.
        let system = match msg.name.as_str() {
            LOG => Some(0),
            TRACE => Some(1),
            _ => None,
        };
        let (cap, others) = match system {
            Some(0) => (LOG_MAX_BYTES, 0),
            Some(_) => (TRACE_MAX_BYTES, 0),
            None => (
                DURABLE_MAX_BYTES,
                streams
                    .iter()
                    .filter(|(n, st)| st.durable && !is_system(n) && **n != msg.name)
                    .map(|(_, st)| st.total_bytes)
                    .sum(),
            ),
        };
        let s = streams
            .entry(msg.name)
            .or_insert_with(|| Stream::new(durable, share));
        // A new attempt after `paused`/`resume` reopens a sealed stream; a stream
        // the writer closed does not reopen.
        s.sealed = None;
        if s.closed {
            out.dropped = n;
            crate::metrics::agent::add(&crate::metrics::agent::STREAM_DROPPED, n as u64);
            models.put_back(&mut inner);
            return out;
        }
        let kind = s.durable as usize;
        // RAM bytes added by this push (before overwriting) — for a durable stream,
        // exactly the new uncarried part.
        let before = s.bytes;
        for r in msg.records {
            let ident = Ident {
                step: r.step,
                attempt: r.attempt,
                k: r.k,
            };
            let prev = s.steps.get(&ident.step).copied();
            if prev.is_none() {
                if let Some(attempt) = s.reset.remove(&ident.step) {
                    s.push(
                        &self.ram,
                        ident,
                        Body::Superseded {
                            step: ident.step,
                            attempt,
                        },
                        false,
                    );
                }
            }
            if prev.is_some_and(|p| (ident.attempt, ident.k) <= p) {
                out.duplicates += 1;
                continue;
            }
            if let Some((attempt, _)) = prev {
                if ident.attempt > attempt {
                    s.push(
                        &self.ram,
                        ident,
                        Body::Superseded {
                            step: ident.step,
                            attempt,
                        },
                        false,
                    );
                }
            }
            s.steps.insert(ident.step, (ident.attempt, ident.k));
            s.last = Some(ident);
            if system == Some(1) {
                // Tokens are measured **before** any limit: the `$trace` limit and
                // the RAM limit bound telemetry, not the bill. Duplicates were already
                // dropped above, so nothing is counted twice.
                tokens_of(&mut models, id, &r.payload, &mut out.tokens);
            }
            if s.durable && others + s.total_bytes + r.payload.len() as u64 > cap {
                s.drop_one(&self.ram, ident);
                out.dropped += 1;
                if let Some(i) = system {
                    crate::metrics::agent::bump(&crate::metrics::agent::LOG_DROPPED[i]);
                }
                continue;
            }
            let cost = (RECORD_OVERHEAD + r.payload.len()) as u64;
            if (!s.durable && cost as usize > EPHEMERAL_BYTES) || !self.ram.take(&s.share, cost) {
                s.drop_one(&self.ram, ident);
                out.dropped += 1;
                continue;
            }
            if system == Some(0) {
                // OTLP export: right on receipt, without waiting for durability.
                crate::otel::offer_log(id, &r.payload);
            }
            if system == Some(1) {
                // OTLP export: right on receipt, without waiting for durability.
                crate::otel::offer_trace(id, &r.payload);
            }
            crate::metrics::agent::bump(&crate::metrics::agent::STREAM_RECORDS[kind]);
            crate::metrics::agent::add(
                &crate::metrics::agent::STREAM_BYTES[kind],
                r.payload.len() as u64,
            );
            s.push(&self.ram, ident, Body::Data(r.payload), true);
            out.accepted += 1;
        }
        if s.durable {
            let added = s.bytes.saturating_sub(before) as u64;
            self.uncarried.fetch_add(added, Ordering::AcqRel);
        }
        s.evict(&self.ram);
        if msg.close {
            s.closed = true;
        }
        models.put_back(&mut inner);
        self.refresh_locked(&mut inner);
        drop(inner);
        self.changed();
        out
    }

    /// A **runtime** record on system stream `name` (`trace: runtime`): it takes no part in
    /// identity-based deduplication — `(step, attempt, k)` is the SDK's space — so it does
    /// not change the last record; it is still subject to the byte and RAM limits like every
    /// record. A durable stream must already be seeded ([`Self::seed`]).
    pub fn append_runtime(&self, id: &InvocationId, name: &str, attempt: u32, payload: Bytes) {
        let mut inner = self.lock();
        if inner.ended || !is_system(name) {
            return;
        }
        let share = self.share_locked(&inner, id);
        let streams = inner.invs.entry(*id).or_default();
        if !streams.contains_key(name) && streams.len() >= MAX_STREAMS {
            return;
        }
        let s = streams
            .entry(name.to_string())
            .or_insert_with(|| Stream::new(self.durable_ok, share));
        s.sealed = None;
        let ident = Ident {
            step: s.last.map_or(0, |l| l.step),
            attempt,
            k: u32::MAX,
        };
        let cap = if name == LOG {
            LOG_MAX_BYTES
        } else {
            TRACE_MAX_BYTES
        };
        let before = s.bytes;
        let cost = (RECORD_OVERHEAD + payload.len()) as u64;
        if s.closed || s.total_bytes + payload.len() as u64 > cap || !self.ram.take(&s.share, cost)
        {
            s.drop_one(&self.ram, ident);
        } else {
            if name == TRACE {
                crate::otel::offer_trace(id, &payload);
            }
            s.push(&self.ram, ident, Body::Data(payload), true);
        }
        let added = s.bytes.saturating_sub(before) as u64;
        self.uncarried.fetch_add(added, Ordering::AcqRel);
        s.evict(&self.ram);
        self.refresh_locked(&mut inner);
        drop(inner);
        self.changed();
    }

    /// Seal every stream of `id` (`Side::Seal`: finished, `paused`,
    /// `restart`). Streams stay readable for [`STREAM_LINGER`], then are cleaned up.
    pub fn seal(&self, id: &InvocationId) {
        let now = tokio::time::Instant::now();
        let mut inner = self.lock();
        inner.resets.remove(id);
        inner.waiting.remove(id);
        let mut touched = false;
        if let Some(streams) = inner.invs.get_mut(id) {
            for s in streams.values_mut() {
                if s.sealed.is_none() {
                    s.sealed = Some(now);
                    touched = true;
                }
            }
        }
        Self::sweep_locked(&mut inner, &self.ram, now);
        drop(inner);
        if touched {
            self.changed();
        }
    }

    /// `resume` of `id` (`turn/life.rs::resume`): the lifecycle core resets `attempt` to 0,
    /// so the suspended step runs again with an identity **not greater** than the identity
    /// already received. Every step seen on every stream of `id` opens a new run at the first
    /// arriving record (`superseded`, then accepted), including durable streams seeded again
    /// from the Db within this term ([`Self::seed`]). The remaining gap: if the term changes
    /// between `resume` and the first record of the rerun, the new hub does not know about this
    /// `resume`, and the suspended step's records are blocked as duplicates again.
    pub fn reset(&self, id: &InvocationId) {
        let mut inner = self.lock();
        if inner.ended {
            return;
        }
        inner.resets.insert(*id);
        if let Some(streams) = inner.invs.get_mut(id) {
            for s in streams.values_mut() {
                s.reset_steps();
            }
        }
    }

    /// A turn of `id` ended suspended waiting on something external (`InvokeOutcome::Waiting`):
    /// after [`StreamConfig::idle_release`] without a new record, [`Self::tick`]
    /// releases its records in RAM, keeping the book.
    pub fn waiting(&self, id: &InvocationId) {
        let now = tokio::time::Instant::now();
        let mut inner = self.lock();
        if !inner.ended && inner.invs.contains_key(id) {
            inner.waiting.insert(*id, now);
        }
    }

    /// The range's timer tick (`turn/timers.rs`): clean up streams sealed longer than
    /// [`STREAM_LINGER`] — previously only at the next sealing, so a quiet range would never
    /// return `410` — and release the records of invocations suspended and idle longer than
    /// [`StreamConfig::idle_release`].
    ///
    /// **An ephemeral stream only promises to keep the running step**; while suspended no
    /// step is running, and what is done is read back from durable storage. Releasing = giving
    /// back bytes, keeping the book: the next offset, deduplication identities, the closed flag
    /// — the next run continues at the right offset, readers of the released part get a `gap`.
    /// Durable streams only release the durable part (read back from the Db).
    pub fn tick(&self) {
        let now = tokio::time::Instant::now();
        let mut inner = self.lock();
        let idle = self.idle_release;
        let due: Vec<InvocationId> = inner
            .waiting
            .iter()
            .filter(|(_, at)| now.saturating_duration_since(**at) >= idle)
            .map(|(id, _)| *id)
            .collect();
        let mut touched = false;
        for id in due {
            inner.waiting.remove(&id);
            let Some(streams) = inner.invs.get_mut(&id) else {
                continue;
            };
            for s in streams.values_mut() {
                touched |= s.release_settled(&self.ram);
            }
        }
        Self::sweep_locked(&mut inner, &self.ram, now);
        drop(inner);
        if touched {
            self.changed();
        }
    }

    /// Clean up streams sealed longer than [`STREAM_LINGER`].
    pub fn sweep(&self) {
        let now = tokio::time::Instant::now();
        let mut inner = self.lock();
        Self::sweep_locked(&mut inner, &self.ram, now);
    }

    fn sweep_locked(inner: &mut Inner, ram: &NodeRam, now: tokio::time::Instant) {
        inner.invs.retain(|_, streams| {
            streams.retain(|_, s| {
                // A durable stream with a not-yet-durable part is kept: nobody else holds it.
                let keep = (s.durable && s.next > s.durable_upto)
                    || s.sealed
                        .is_none_or(|at| now.saturating_duration_since(at) < STREAM_LINGER);
                if !keep {
                    s.release(ram);
                }
                keep
            });
            !streams.is_empty()
        });
        let invs = &inner.invs;
        inner.owners.retain(|id, _| invs.contains_key(id));
    }

    /// The term ends (stepping down, quiesce, split): give the RAM back, tell readers.
    ///
    /// A durable stream with a **not-yet-carried** part keeps it for `seal`'s fence
    /// (AP-15) — `seal` runs after this command; its RAM is given back when the hub drops.
    pub fn end_term(&self) {
        let mut inner = self.lock();
        inner.ended = true;
        let ram = &self.ram;
        inner.invs.retain(|_, streams| {
            streams.retain(|_, s| {
                let keep = s.durable && s.next > s.carried;
                if !keep {
                    s.release(ram);
                }
                keep
            });
            !streams.is_empty()
        });
        drop(inner);
        self.changed();
    }

    /// The term has ended.
    pub fn ended(&self) -> bool {
        self.lock().ended
    }

    /// At most `max` records of stream `name` from `from`. `None` = the stream is not in the
    /// hub (no record yet, or already cleaned up after sealing).
    pub fn read(&self, id: &InvocationId, name: &str, from: u64, max: usize) -> Option<Slice> {
        let mut inner = self.lock();
        self.refresh_locked(&mut inner);
        let s = inner.invs.get(id)?.get(name)?;
        let first = s.first();
        let start = from.max(first);
        let skip = (start - first) as usize;
        let records: Vec<Record> = s.records.iter().skip(skip).take(max).cloned().collect();
        let next = records.last().map_or(start, |r| r.offset + 1);
        let ended = (s.sealed.is_some() || s.closed) && next >= s.next;
        Some(Slice {
            records,
            next,
            missed: first.saturating_sub(from),
            ended,
            durable: s.durable,
            durable_upto: if s.durable { s.durable_upto } else { 0 },
        })
    }

    /// The streams of `id`, for `GET /invocations/{id}`.
    pub fn info(&self, id: &InvocationId) -> Vec<Info> {
        let inner = self.lock();
        inner
            .invs
            .get(id)
            .map(|streams| {
                streams
                    .iter()
                    .map(|(name, s)| Info {
                        name: name.clone(),
                        durable: s.durable,
                        next: s.next,
                        sealed: s.sealed.is_some() || s.closed,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The model book of open `$trace` spans, borrowed out of `Inner` for one push.
struct Models {
    map: BTreeMap<(InvocationId, String), String>,
    order: VecDeque<(InvocationId, String)>,
}

impl Models {
    fn put_back(self, inner: &mut Inner) {
        inner.models = self.map;
        inner.models_order = self.order;
    }
}

/// Read one `$trace` record (the SDK's JSON): a `start` with `attrs.model` ⇒ remember the
/// span's model; an `end` with `attrs.input_tokens`/`output_tokens` ⇒ one token line, the
/// model taken from the `end` or from the remembered `start`. Corrupt ⇒ skipped.
fn tokens_of(
    models: &mut Models,
    id: &InvocationId,
    payload: &[u8],
    out: &mut Vec<(Option<String>, u64, u64)>,
) {
    let Ok(d) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return;
    };
    let Some(span) = d["span_id"].as_str() else {
        return;
    };
    let key = (*id, span.to_string());
    match d["kind"].as_str() {
        Some("start") => {
            if let Some(m) = d["attrs"]["model"].as_str() {
                if models.map.len() >= OPEN_MODELS_MAX {
                    if let Some(old) = models.order.pop_front() {
                        models.map.remove(&old);
                    }
                }
                if models.map.insert(key.clone(), m.to_string()).is_none() {
                    models.order.push_back(key);
                }
            }
        }
        Some("end") => {
            let opened = models.map.remove(&key);
            if opened.is_some() {
                models.order.retain(|k| *k != key);
            }
            let input = d["attrs"]["input_tokens"].as_u64().unwrap_or(0);
            let output = d["attrs"]["output_tokens"].as_u64().unwrap_or(0);
            if input > 0 || output > 0 {
                let model = d["attrs"]["model"].as_str().map(str::to_string).or(opened);
                out.push((model, input, output));
            }
        }
        _ => {}
    }
}

/// A term's durable streams riding along on the range writer's batches
/// (`PartitionWriter::set_rider`).
pub struct StreamRider(pub Arc<RangeStreams>);

impl cairn_store::Rider for StreamRider {
    fn pending(&self) -> Vec<cairn_store::RiderChunk> {
        self.0.pending_chunks()
    }

    fn carried(&self, seq: u64, chunks: &[cairn_store::RiderChunk]) {
        self.0.carried(seq, chunks);
    }
}

impl Drop for RangeStreams {
    fn drop(&mut self) {
        let inner = self.inner.get_mut().unwrap_or_else(|e| e.into_inner());
        for streams in inner.invs.values_mut() {
            for s in streams.values_mut() {
                s.release(&self.ram);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: InvocationId = [1; 24];

    fn rec(step: u32, attempt: u32, k: u32, payload: &str) -> pb::StreamRecord {
        pb::StreamRecord {
            step,
            attempt,
            k,
            payload: Bytes::copy_from_slice(payload.as_bytes()),
        }
    }

    fn append(name: &str, records: Vec<pb::StreamRecord>) -> pb::StreamAppendMessage {
        pb::StreamAppendMessage {
            name: name.into(),
            durable: false,
            records,
            close: false,
        }
    }

    fn bodies(s: &Slice) -> Vec<Body> {
        s.records.iter().map(|r| r.body.clone()).collect()
    }

    fn data(p: &str) -> Body {
        Body::Data(Bytes::copy_from_slice(p.as_bytes()))
    }

    #[test]
    fn offsets_are_dense_and_duplicates_are_dropped_not_lost() {
        let hub = StreamBook::default().term(true);
        let a = hub.append(&A, append("ui", vec![rec(1, 0, 0, "a"), rec(1, 0, 1, "b")]));
        assert_eq!(a.accepted, 2);
        // Resent (the SDK reran the handler, replay has not recognised it yet): a duplicate identity.
        let b = hub.append(&A, append("ui", vec![rec(1, 0, 1, "b"), rec(2, 0, 0, "c")]));
        assert_eq!((b.accepted, b.duplicates, b.dropped), (1, 1, 0));
        let s = hub.read(&A, "ui", 0, 10).unwrap();
        assert_eq!(bodies(&s), vec![data("a"), data("b"), data("c")]);
        assert_eq!(
            s.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(s.next, 3);
        assert!(!s.ended);
    }

    #[test]
    fn new_attempt_of_the_same_step_inserts_superseded() {
        let hub = StreamBook::default().term(true);
        hub.append(&A, append("ui", vec![rec(3, 0, 0, "x"), rec(3, 0, 1, "y")]));
        hub.append(&A, append("ui", vec![rec(3, 1, 0, "x2")]));
        // The journal grew: attempt back to 0 at the next step — not a new attempt.
        hub.append(&A, append("ui", vec![rec(4, 0, 0, "z")]));
        let s = hub.read(&A, "ui", 0, 10).unwrap();
        assert_eq!(
            bodies(&s),
            vec![
                data("x"),
                data("y"),
                Body::Superseded {
                    step: 3,
                    attempt: 0
                },
                data("x2"),
                data("z"),
            ]
        );
    }

    /// Two parallel `ctx.run` closures (parallel tools): each closure's records carry
    /// the `step` of its own `Run` command, so the two steps interleave on one stream.
    /// Order is only strict **within** each step: duplicates within a step are still dropped,
    /// a new attempt of a step (not the last step) inserts `superseded` for that very step.
    /// `resume` (the core resets `attempt` to 0): the suspended step runs again with an
    /// identity that is not greater. Within the same term, seeded again after `resume`, and in
    /// the next term (the last chunk already has `superseded`) — all three accept the new run
    /// and still drop its own duplicates.
    #[test]
    fn resume_opens_a_new_run_of_the_step() {
        use cairn_store::keys::{ChunkBody, ChunkRecord, StreamChunk};
        let hub = StreamBook::default().term(true);
        hub.append(&A, append("ui", vec![rec(1, 2, 3, "old")]));
        hub.reset(&A);
        let a = hub.append(
            &A,
            append("ui", vec![rec(1, 0, 0, "new"), rec(1, 0, 0, "new")]),
        );
        assert_eq!(a.duplicates, 1, "{a:?}");
        let s = hub.read(&A, "ui", 0, 20).unwrap();
        assert_eq!(
            bodies(&s),
            vec![
                data("old"),
                Body::Superseded {
                    step: 1,
                    attempt: 2
                },
                data("new"),
            ]
        );

        let chunk = |records| StreamChunk {
            total_bytes: 0,
            dropped: 0,
            records,
        };
        let old = ChunkRecord {
            offset: 0,
            step: 1,
            attempt: 2,
            k: 3,
            body: ChunkBody::Data(Bytes::from_static(b"old")),
        };
        // Seeded from the Db after `resume` within the same term: the last chunk belongs to the
        // previous run.
        let (hub, _) = attached();
        hub.reset(&A);
        hub.seed(&A, LOG, Some((0, Some(chunk(vec![old.clone()])))));
        let b = hub.append(&A, durable(LOG, vec![rec(1, 0, 0, "new")]));
        assert_eq!((b.accepted, b.duplicates), (1, 0), "{b:?}");

        // The next term: `superseded` in the last chunk opens a new run.
        let (hub, _) = attached();
        let marker = ChunkRecord {
            offset: 1,
            step: 1,
            attempt: 0,
            k: 0,
            body: ChunkBody::Superseded {
                step: 1,
                attempt: 2,
            },
        };
        let new = ChunkRecord {
            offset: 2,
            step: 1,
            attempt: 0,
            k: 0,
            body: ChunkBody::Data(Bytes::from_static(b"new")),
        };
        hub.seed(&A, LOG, Some((0, Some(chunk(vec![old, marker, new])))));
        let c = hub.append(
            &A,
            durable(LOG, vec![rec(1, 0, 0, "new"), rec(1, 0, 1, "next")]),
        );
        assert_eq!((c.accepted, c.duplicates), (1, 1), "{c:?}");
    }

    #[test]
    fn interleaved_steps_are_ordered_per_step() {
        let hub = StreamBook::default().term(true);
        let a = hub.append(
            &A,
            append(
                "ui",
                vec![
                    rec(5, 0, 0, "a0"),
                    rec(6, 0, 0, "b0"),
                    rec(5, 0, 1, "a1"),
                    rec(6, 0, 1, "b1"),
                ],
            ),
        );
        assert_eq!((a.accepted, a.duplicates), (4, 0), "{a:?}");
        let b = hub.append(
            &A,
            append(
                "ui",
                vec![rec(5, 0, 1, "a1"), rec(6, 0, 0, "b0"), rec(7, 0, 0, "c")],
            ),
        );
        assert_eq!((b.accepted, b.duplicates), (1, 2), "{b:?}");
        hub.append(&A, append("ui", vec![rec(5, 1, 0, "a0'")]));
        let s = hub.read(&A, "ui", 0, 20).unwrap();
        assert_eq!(
            bodies(&s),
            vec![
                data("a0"),
                data("b0"),
                data("a1"),
                data("b1"),
                data("c"),
                Body::Superseded {
                    step: 5,
                    attempt: 0
                },
                data("a0'"),
            ]
        );
    }

    #[test]
    fn ephemeral_ring_overwrites_and_slow_reader_sees_missed() {
        let hub = StreamBook::default().term(true);
        let big = "x".repeat(300 << 10);
        for k in 0..5 {
            hub.append(&A, append("ui", vec![rec(1, 0, k, &big)]));
        }
        let s = hub.read(&A, "ui", 0, 10).unwrap();
        assert!(s.missed >= 1, "old records overwritten: {s:?}");
        assert_eq!(s.records.first().unwrap().offset, s.missed);
        assert_eq!(s.next, 5);
        // A reader that catches up misses nothing.
        assert_eq!(hub.read(&A, "ui", 4, 10).unwrap().missed, 0);
    }

    #[test]
    fn node_ram_cap_drops_and_coalesces_into_one_gap() {
        let book = StreamBook::new(4096);
        let hub = book.term(true);
        let p = "y".repeat(1500);
        let out = hub.append(
            &A,
            append("ui", (0..5).map(|k| rec(1, 0, k, &p)).collect::<Vec<_>>()),
        );
        assert_eq!(out.accepted, 2, "{out:?}");
        assert_eq!(out.dropped, 3);
        let s = hub.read(&A, "ui", 0, 10).unwrap();
        assert_eq!(s.records.len(), 3);
        assert_eq!(s.records[2].body, Body::Gap { dropped: 3 });
        drop(hub);
        assert_eq!(book.ram().used(), 0, "hub drop gives all RAM back");
    }

    /// **One tenant cannot fill the node's stream RAM**: the RAM limit used to be one shared
    /// counter, so when tenant `alpha` pushed 1 MiB full, the records — `durable: true` ones
    /// included — of tenant `bravo` were dropped.
    ///
    /// Order: a 1 MiB book; `alpha` (invocation `A`) pushes 60 × 16 KiB onto the ephemeral
    /// stream `ui`, then 2000 one-byte records onto `ui2` — close to the limit; then `bravo`
    /// (invocation `B`) pushes one durable 1 KiB record.
    ///
    /// Must see: `bravo`'s record is accepted; `alpha` is dropped once it reaches its share
    /// (by default 1/8 of the node's RAM); the `default` tenant (self-hosting with one
    /// tenant) still uses the whole node limit.
    #[test]
    fn one_tenant_cannot_starve_another() {
        let book = StreamBook::new(1 << 20);
        let hub = book.term(true);
        let b: InvocationId = [7; 24];
        hub.bind_tenant(&A, &TenantId::new("alpha").unwrap(), None);
        hub.bind_tenant(&b, &TenantId::new("bravo").unwrap(), None);
        // Large blocks on `ui`, then small blocks on `ui2`: fill close to the node's limit,
        // each stream below the ephemeral stream limit (not overwritten, leaving no gap).
        let p = "a".repeat(16 << 10);
        let big = (0..60).map(|k| rec(1, 0, k, &p)).collect::<Vec<_>>();
        let small = (0..2000).map(|k| rec(1, 0, k, "a")).collect::<Vec<_>>();
        hub.append(&A, append("ui", big));
        let out = hub.append(&A, append("ui2", small));
        assert!(out.dropped > 0, "alpha reaches its share: {out:?}");
        let mut m = append("notes", vec![rec(1, 0, 0, &"b".repeat(1024))]);
        m.durable = true;
        let out = hub.append(&b, m);
        assert_eq!(
            out.accepted, 1,
            "bravo's durable record was dropped: {out:?}"
        );
        assert!(
            book.ram().used() <= ((1 << 20) / 8) + (4 << 10),
            "alpha holds {} bytes",
            book.ram().used()
        );
        // `default`: the whole node limit.
        let c: InvocationId = [8; 24];
        let hub2 = StreamBook::new(1 << 20).term(true);
        hub2.bind_tenant(&c, &TenantId::default(), None);
        let out = hub2.append(
            &c,
            append("ui", (0..32).map(|k| rec(1, 0, k, &p)).collect::<Vec<_>>()),
        );
        assert_eq!(out.dropped, 0, "default uses the whole limit: {out:?}");
    }

    /// The tenant's `max-streams=`: a new user stream over the
    /// limit is dropped like the 17th stream (an invocation never fails because of a stream);
    /// existing streams still accept; system streams (`$log`) do not count.
    #[test]
    fn tenant_stream_cap_drops_new_user_streams() {
        let hub = StreamBook::default().term(true);
        let cap = Some(2);
        for name in ["s0", "s1"] {
            let out = hub.append_capped(&A, append(name, vec![rec(1, 0, 0, "a")]), cap);
            assert_eq!(out.accepted, 1, "{name}");
        }
        let out = hub.append_capped(&A, append("s2", vec![rec(1, 0, 0, "a")]), cap);
        assert_eq!(out.dropped, 1);
        assert!(hub.read(&A, "s2", 0, 1).is_none());
        let out = hub.append_capped(&A, append("s0", vec![rec(2, 0, 0, "b")]), cap);
        assert_eq!(out.accepted, 1, "existing stream");
        let out = hub.append_capped(&A, append(LOG, vec![rec(1, 0, 0, "a")]), cap);
        assert_eq!(out.accepted, 1, "$log does not count against the limit");
    }

    #[test]
    fn seventeenth_stream_is_dropped_and_bad_names_too() {
        let hub = StreamBook::default().term(true);
        for i in 0..MAX_STREAMS {
            assert_eq!(
                hub.append(&A, append(&format!("s{i}"), vec![rec(1, 0, 0, "a")]))
                    .accepted,
                1
            );
        }
        let out = hub.append(&A, append("s16", vec![rec(1, 0, 0, "a")]));
        assert_eq!(out.dropped, 1);
        assert!(hub.read(&A, "s16", 0, 1).is_none());
        let id = [2; 24];
        assert_eq!(
            hub.append(&id, append("$x", vec![rec(1, 0, 0, "a")]))
                .dropped,
            1
        );
        assert_eq!(
            hub.append(&id, append("", vec![rec(1, 0, 0, "a")])).dropped,
            1
        );
        assert_eq!(
            hub.append(&id, append(LOG, vec![rec(1, 0, 0, "a")]))
                .accepted,
            1
        );
        assert!(
            hub.read(&id, LOG, 0, 1).unwrap().durable,
            "$log is always durable"
        );
    }

    /// An invocation suspended and idle longer than `idle_release` keeps only its book — the
    /// ephemeral stream gives back all its bytes, the durable stream keeps its not-yet-durable
    /// part; duplicates are still dropped, new records continue at the right offset; a new record
    /// means nothing is released.
    #[tokio::test(start_paused = true)]
    async fn waiting_invocation_keeps_only_its_ledger() {
        let hub = StreamBook::default().term(true);
        hub.append(&A, append("ui", vec![rec(1, 0, 0, "a"), rec(1, 0, 1, "b")]));
        hub.append(&A, durable("notes", vec![rec(1, 0, 0, "not durable")]));
        hub.waiting(&A);
        tokio::time::advance(STREAM_LINGER - Duration::from_secs(1)).await;
        hub.tick();
        assert_eq!(
            hub.read(&A, "ui", 0, 10).unwrap().records.len(),
            2,
            "grace period not over yet"
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        hub.tick();
        let s = hub.read(&A, "ui", 0, 10).unwrap();
        assert_eq!((s.missed, s.records.len()), (2, 0));
        assert_eq!(
            hub.read(&A, "notes", 0, 10).unwrap().records.len(),
            1,
            "the not-yet-durable part of a durable stream must not be released"
        );
        let o = hub.append(&A, append("ui", vec![rec(1, 0, 1, "b"), rec(2, 0, 0, "c")]));
        assert_eq!((o.accepted, o.duplicates), (1, 1), "{o:?}");
        let s = hub.read(&A, "ui", 2, 10).unwrap();
        assert_eq!(s.records.first().map(|r| r.offset), Some(2));

        // Suspended, then a new record (a running step): nothing released.
        hub.waiting(&A);
        hub.append(&A, append("ui", vec![rec(3, 0, 0, "d")]));
        tokio::time::advance(STREAM_LINGER * 2).await;
        hub.tick();
        assert_eq!(hub.read(&A, "ui", 2, 10).unwrap().records.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn seal_ends_readers_and_linger_expires_the_stream() {
        let hub = StreamBook::default().term(true);
        hub.append(&A, append("ui", vec![rec(1, 0, 0, "a")]));
        let mut rx = hub.subscribe();
        hub.seal(&A);
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        let s = hub.read(&A, "ui", 0, 10).unwrap();
        assert_eq!(s.records.len(), 1);
        assert!(
            s.ended,
            "reading a sealed stream to the end means it has ended"
        );
        assert!(hub.info(&A)[0].sealed);
        tokio::time::advance(STREAM_LINGER + Duration::from_secs(1)).await;
        hub.sweep();
        assert!(hub.read(&A, "ui", 0, 10).is_none());
        // A new attempt after `resume` reopens a stream not yet cleaned up.
        hub.append(&A, append("ui", vec![rec(2, 0, 0, "b")]));
        hub.seal(&A);
        hub.append(&A, append("ui", vec![rec(3, 0, 0, "c")]));
        assert!(!hub.read(&A, "ui", 0, 10).unwrap().ended);
    }

    fn durable(name: &str, records: Vec<pb::StreamRecord>) -> pb::StreamAppendMessage {
        let mut m = append(name, records);
        m.durable = true;
        m
    }

    /// A hub with a `durable_seq` source controlled by the test.
    fn attached() -> (Arc<RangeStreams>, Arc<AtomicU64>) {
        let hub = StreamBook::default().term(true);
        let seq = Arc::new(AtomicU64::new(0));
        let s = Arc::clone(&seq);
        hub.attach(move || s.load(Ordering::Acquire));
        (hub, seq)
    }

    /// The uncarried part goes in **one** chunk per stream, and once carried it stops;
    /// durable only when `durable_seq` passes the batch's `seq` (AP-12).
    #[test]
    fn durable_records_ride_once_and_turn_durable_by_seq() {
        use cairn_store::Rider;
        let (hub, seq) = attached();
        let rider = StreamRider(Arc::clone(&hub));
        assert!(rider.pending().is_empty(), "empty: no lock, no chunk");
        hub.append(
            &A,
            durable("notes", vec![rec(1, 0, 0, "a"), rec(1, 0, 1, "b")]),
        );
        hub.append(&A, append("ui", vec![rec(1, 0, 0, "temp")]));
        let p = rider.pending();
        assert_eq!(p.len(), 1, "ephemeral streams do not ride along");
        assert_eq!((p[0].name.as_str(), p[0].first), ("notes", 0));
        assert_eq!(p[0].chunk.records.len(), 2);
        assert_eq!(p[0].chunk.total_bytes, 2);
        // A failed batch (no `carried` call): the next time takes exactly that chunk again.
        assert_eq!(rider.pending()[0].chunk, p[0].chunk);
        rider.carried(7, &p);
        assert!(rider.pending().is_empty());
        assert_eq!(hub.read(&A, "notes", 0, 10).unwrap().durable_upto, 0);
        seq.store(7, Ordering::Release);
        assert_eq!(hub.read(&A, "notes", 0, 10).unwrap().durable_upto, 2);
        // A later record: a new chunk starts at offset 2, total bytes accumulate.
        hub.append(&A, durable("notes", vec![rec(2, 0, 0, "cd")]));
        let p = rider.pending();
        assert_eq!((p[0].first, p[0].chunk.total_bytes), (2, 4));
        assert!(!hub.wants_flush());
    }

    /// A new term continues from the last chunk: offset, last identities (duplicates are
    /// dropped), counters; `end_term` keeps the uncarried part for `seal`'s fence.
    #[test]
    fn seed_resumes_from_the_last_chunk_and_end_term_keeps_uncarried() {
        use cairn_store::{
            keys::{ChunkBody, ChunkRecord, StreamChunk},
            Rider,
        };
        let (hub, _) = attached();
        assert!(hub.needs_seed(&A, LOG, false), "$log is always durable");
        assert!(
            !hub.needs_seed(&A, "ui", false),
            "ephemeral streams are not seeded"
        );
        let last = StreamChunk {
            total_bytes: 10,
            dropped: 1,
            records: vec![ChunkRecord {
                offset: 4,
                step: 3,
                attempt: 0,
                k: 1,
                body: ChunkBody::Data(Bytes::from_static(b"x")),
            }],
        };
        hub.seed(&A, LOG, Some((4, Some(last))));
        assert!(!hub.needs_seed(&A, LOG, true));
        let out = hub.append(
            &A,
            durable(LOG, vec![rec(3, 0, 1, "dup"), rec(3, 0, 2, "y")]),
        );
        assert_eq!((out.accepted, out.duplicates), (1, 1));
        let rider = StreamRider(Arc::clone(&hub));
        let p = rider.pending();
        assert_eq!(
            (p[0].first, p[0].chunk.total_bytes, p[0].chunk.dropped),
            (5, 11, 1)
        );
        // A corrupt last chunk: the stream is closed for this term.
        let b = [9; 24];
        hub.seed(&b, LOG, Some((0, None)));
        assert_eq!(
            hub.append(&b, durable(LOG, vec![rec(1, 0, 0, "z")]))
                .dropped,
            1
        );

        hub.end_term();
        assert!(hub.ended());
        let p = rider.pending();
        assert_eq!(p.len(), 1, "uncarried part left for seal: {p:?}");
        rider.carried(1, &p);
        assert!(rider.pending().is_empty());
    }

    fn data_count(s: &Slice) -> usize {
        s.records
            .iter()
            .filter(|r| matches!(r.body, Body::Data(_)))
            .count()
    }

    /// `$log` stops at 1 MiB of payload per invocation — the excess is dropped,
    /// counted in `cairnflow_log_dropped_total{stream="$log"}`, with one final `gap`.
    #[test]
    fn system_streams_stop_at_one_mib_and_end_with_one_gap() {
        let hub = StreamBook::default().term(true);
        let big = "x".repeat(100 << 10);
        let before = crate::metrics::agent::LOG_DROPPED[0].load(Ordering::Relaxed);
        for k in 0..12 {
            hub.append(&A, durable(LOG, vec![rec(1, 0, k, &big)]));
        }
        let s = hub.read(&A, LOG, 0, 100).unwrap();
        assert_eq!(data_count(&s), 10, "10 × 100 KiB fit in 1 MiB");
        assert_eq!(s.records.last().unwrap().body, Body::Gap { dropped: 2 });
        let after = crate::metrics::agent::LOG_DROPPED[0].load(Ordering::Relaxed);
        assert!(after >= before + 2, "{before} → {after}");
        // `$trace` has its own limit.
        hub.append(&A, durable(TRACE, vec![rec(1, 0, 0, &big)]));
        assert_eq!(data_count(&hub.read(&A, TRACE, 0, 10).unwrap()), 1);
    }

    /// The limit belongs to **the invocation, across every attempt and every term**: the
    /// previous term's bytes are read from the last chunk (`total_bytes`).
    #[test]
    fn cap_counts_bytes_of_earlier_terms() {
        use cairn_store::keys::StreamChunk;
        let hub = StreamBook::default().term(true);
        let last = StreamChunk {
            total_bytes: LOG_MAX_BYTES - 4,
            dropped: 0,
            records: Vec::new(),
        };
        hub.seed(&A, LOG, Some((50, Some(last))));
        let out = hub.append(&A, durable(LOG, vec![rec(9, 3, 0, "12345678")]));
        assert_eq!(out.dropped, 1, "{out:?}");
    }

    /// User durable streams share an 8 MiB limit per invocation; ephemeral streams
    /// do not count.
    #[test]
    fn durable_user_streams_share_eight_mib() {
        let hub = StreamBook::new(1 << 30).term(true);
        let big = "x".repeat(1 << 20);
        for k in 0..6 {
            hub.append(&A, durable("a", vec![rec(1, 0, k, &big)]));
        }
        for k in 0..4 {
            hub.append(&A, durable("b", vec![rec(1, 0, k, &big)]));
        }
        let a = data_count(&hub.read(&A, "a", 0, 100).unwrap());
        let b = data_count(&hub.read(&A, "b", 0, 100).unwrap());
        assert_eq!(a + b, 8, "a {a} b {b}");
        hub.append(&A, append("ui", vec![rec(1, 0, 0, &big[..1000])]));
        assert_eq!(data_count(&hub.read(&A, "ui", 0, 10).unwrap()), 1);
    }

    #[test]
    fn closed_stream_takes_nothing_more_and_end_term_releases_ram() {
        let book = StreamBook::default();
        let hub = book.term(true);
        let mut m = append("ui", vec![rec(1, 0, 0, "a")]);
        m.close = true;
        hub.append(&A, m);
        assert!(hub.read(&A, "ui", 0, 10).unwrap().ended);
        assert_eq!(
            hub.append(&A, append("ui", vec![rec(2, 0, 0, "b")]))
                .dropped,
            1
        );
        assert!(book.ram().used() > 0);
        hub.end_term();
        assert!(hub.ended());
        assert_eq!(book.ram().used(), 0);
        assert_eq!(
            hub.append(&A, append("ui", vec![rec(3, 0, 0, "c")]))
                .dropped,
            1
        );
    }
}
