// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Test tooling — only with the `testkit` feature.
//!
//! [`FaultyStore`] wraps a real `ObjectStore` and injects faults by rule: blocking
//! requests behind a gate ([`Gate`]), slowing them, failing them, or hanging forever. It
//! also counts requests by operation and by path class, so tests can measure the rate of
//! store calls without any other mock.
//!
//! Plugged into [`crate::ObjectStores`] through [`crate::StoreLocation::from_store`]:
//!
//! ```ignore
//! let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
//! let gate = Gate::closed();
//! faulty.add_rule(Rule::wal_put(Action::Gate(gate.clone())));
//! let loc = StoreLocation::from_store(faulty.clone(), "cairn", "memory://cairn");
//! ```
//!
//! **Rules are checked in the order added, and every matching rule runs.** A gate, then a
//! delay, then an error compose; the first rule that fails or hangs stops there. Counters
//! go up when a request *arrives*, before any rule — they count attempts, including those
//! blocked or corrupted.
//!
//! **S3 simulation:** [`Action::Latency`] draws a delay from a seeded distribution
//! ([`LatencyDist`]), [`Action::SlowDown`] throttles per prefix like S3 ([`SlowDown`]), and
//! [`profile`] builds the whole rule set from a profile file for a real process.

use {
    bytes::Bytes,
    futures::{stream::BoxStream, StreamExt},
    slatedb::object_store::{
        path::Path, CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, RenameOptions, Result,
    },
    std::{
        fmt,
        ops::{BitOr, Range},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::sync::watch,
};

#[cfg(feature = "failpoints")]
mod failpoints;
mod latency;
#[cfg(feature = "testkit")]
pub mod profile;
pub mod range_model;
mod slowdown;
#[cfg(feature = "failpoints")]
pub use failpoints::{FailGuard, Paused, StepController};
pub use {
    latency::{LatencyDist, SimRng},
    range_model::{RangeBucket, RangeModel},
    slowdown::{ClientRetry, SlowDown, SlowDownConfig, SlowDownStats},
};

/// One kind of request to the object store.
///
/// `Head` is separate from `Get` even though object_store 0.14 folds both into
/// `get_opts`. `Delete` is each path going through `delete_stream`. `PutMultipart` is only
/// intercepted when the upload is created; the parts after that go straight through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Put,
    PutMultipart,
    Get,
    Head,
    List,
    Delete,
    Copy,
    Rename,
}

impl Op {
    pub const ALL: [Op; 8] = [
        Op::Put,
        Op::PutMultipart,
        Op::Get,
        Op::Head,
        Op::List,
        Op::Delete,
        Op::Copy,
        Op::Rename,
    ];

    fn index(self) -> usize {
        self as usize
    }
}

/// The set of operations a [`Rule`] applies to. Combined with `|`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OpSet(u16);

impl OpSet {
    pub const NONE: OpSet = OpSet(0);
    pub const PUT: OpSet = OpSet::of(Op::Put);
    pub const PUT_MULTIPART: OpSet = OpSet::of(Op::PutMultipart);
    pub const GET: OpSet = OpSet::of(Op::Get);
    pub const HEAD: OpSet = OpSet::of(Op::Head);
    pub const LIST: OpSet = OpSet::of(Op::List);
    pub const DELETE: OpSet = OpSet::of(Op::Delete);
    pub const COPY: OpSet = OpSet::of(Op::Copy);
    pub const RENAME: OpSet = OpSet::of(Op::Rename);
    /// Every write of a new object: `put` and `put_multipart`.
    pub const WRITES: OpSet = OpSet(OpSet::PUT.0 | OpSet::PUT_MULTIPART.0);
    pub const ALL: OpSet = OpSet((1 << Op::ALL.len()) - 1);

    pub const fn of(op: Op) -> OpSet {
        OpSet(1 << op as u16)
    }

    pub fn contains(self, op: Op) -> bool {
        self.0 & OpSet::of(op).0 != 0
    }
}

impl BitOr for OpSet {
    type Output = OpSet;

    fn bitor(self, rhs: OpSet) -> OpSet {
        OpSet(self.0 | rhs.0)
    }
}

impl From<Op> for OpSet {
    fn from(op: Op) -> OpSet {
        OpSet::of(op)
    }
}

/// A condition on the path — compared on the full string, including the store's prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathMatch {
    Any,
    Contains(String),
    Prefix(String),
    /// Every path that [`PathClass::of`] classifies as [`PathClass::Lease`].
    LeaseLayer,
    /// Every path that [`PathClass::of`] classifies into this class.
    Class(PathClass),
}

impl PathMatch {
    fn matches(&self, path: &Path) -> bool {
        match self {
            PathMatch::Any => true,
            PathMatch::Contains(s) => path.as_ref().contains(s.as_str()),
            PathMatch::Prefix(s) => path.as_ref().starts_with(s.as_str()),
            PathMatch::LeaseLayer => PathClass::of(path) == PathClass::Lease,
            PathMatch::Class(c) => PathClass::of(path) == *c,
        }
    }
}

/// A gate: requests matching the rule wait while the gate is closed, go on once it opens.
///
/// Cheap to clone, every clone shares one gate. The gate holds no receiver, so
/// [`Gate::blocked`] is exactly the number of requests waiting — tests use it to know for
/// sure that a request has *reached* the gate before checking something.
#[derive(Clone, Debug)]
pub struct Gate(Arc<watch::Sender<bool>>);

impl Gate {
    pub fn opened() -> Gate {
        Gate(Arc::new(watch::Sender::new(true)))
    }

    pub fn closed() -> Gate {
        Gate(Arc::new(watch::Sender::new(false)))
    }

    pub fn open(&self) {
        self.0.send_replace(true);
    }

    pub fn close(&self) {
        self.0.send_replace(false);
    }

    pub fn is_open(&self) -> bool {
        *self.0.borrow()
    }

    /// Number of requests waiting at this gate.
    pub fn blocked(&self) -> usize {
        self.0.receiver_count()
    }

    async fn pass(&self) {
        let mut rx = self.0.subscribe();
        // The sender lives as long as `self` does, so `wait_for` cannot fail.
        let _ = rx.wait_for(|open| *open).await;
    }
}

/// A **torn and stale** object that an **unconditional** PUT heals — see [`Action::Tear`].
/// Copies share state: tests ask [`Tear::healed`] to know the overwrite has arrived.
#[derive(Clone, Debug, Default)]
pub struct Tear(Arc<TearState>);

#[derive(Debug, Default)]
struct TearState {
    healed: std::sync::atomic::AtomicBool,
    /// The body GET returns while torn: the **first** read after the rule took effect,
    /// frozen (readers see no later write).
    frozen: Mutex<Option<Bytes>>,
}

impl Tear {
    pub fn new() -> Tear {
        Tear::default()
    }

    /// An unconditional PUT has reached the object (from then on the rule has no effect).
    pub fn healed(&self) -> bool {
        self.0.healed.load(Ordering::SeqCst)
    }
}

/// When a [`Tripwire`] fires, relative to matching request number `skip + 1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TripAt {
    /// The process dies **before** the request leaves: nothing reaches the store.
    Before,
    /// The request reaches the store (a PUT lands, a DELETE deletes), then the process dies
    /// before reading the answer.
    After,
    /// The request reaches the store but the caller gets an **error** — a "torn" PUT seen
    /// from the client: it does not know whether it wrote. The process lives on; the wire
    /// fires only once.
    Torn,
}

/// A tripwire for deterministic simulation: lets `skip` matching requests through, then
/// fires on the next request according to [`TripAt`]. `Before`/`After` **freeze the whole
/// store** (every later request hangs, like `kill -9` seen from the bucket).
#[derive(Clone, Debug)]
pub struct Tripwire(Arc<TripState>);

#[derive(Debug)]
struct TripState {
    at: TripAt,
    left: Mutex<u64>,
    fired: std::sync::atomic::AtomicBool,
}

impl Tripwire {
    pub fn new(skip: u64, at: TripAt) -> Tripwire {
        Tripwire(Arc::new(TripState {
            at,
            left: Mutex::new(skip),
            fired: Default::default(),
        }))
    }

    /// Whether the wire has fired.
    pub fn fired(&self) -> bool {
        self.0.fired.load(Ordering::SeqCst)
    }

    /// `Some(at)` if this request makes the wire fire.
    fn hit(&self) -> Option<TripAt> {
        if self.fired() {
            return None;
        }
        let mut left = self.0.left.lock().unwrap();
        if *left > 0 {
            *left -= 1;
            return None;
        }
        self.0.fired.store(true, Ordering::SeqCst);
        Some(self.0.at)
    }
}

/// What to do with a request matching the rule.
#[derive(Clone, Debug)]
pub enum Action {
    /// Wait until the gate opens.
    Gate(Gate),
    /// Sleep for a while, then go on.
    Delay(Duration),
    /// Return `object_store::Error::Generic`. `times: None` is forever; `Some(n)` is the
    /// first n matching requests, after which the rule has no effect.
    Fail { times: Option<u32> },
    /// Let the first `skip` matching requests through, then fail **exactly one** next
    /// request like [`Action::Fail`]; after that the rule has no effect — corrupts exactly
    /// the `skip + 1`-th call of an operation (e.g. the second `Create` of the probe
    /// gate).
    FailNth { skip: u32 },
    /// Never returns.
    Hang,
    /// Return `object_store::Error::Precondition` — like a lost conditional PUT. Applies to
    /// every matching operation, unconditional PUTs included.
    Precondition,
    /// GETs return an etag **flipping** between two fake values, a different one each time
    /// — the shape of an object torn across disks (MinIO EC, seen under chaos): GET never
    /// yields an etag that `If-Match` accepts.
    FlipEtag,
    /// A caching layer returning a stale `304`: every GET **with** `If-None-Match` returns
    /// `NotModified`, whatever the object has become — the shape of a caching proxy in
    /// front of the bucket, or of a weakly consistent store. Unconditional GETs go through.
    StaleNotModified,
    /// A **torn and stale** object (MinIO EC after a half-done PUT, or a stale read cache):
    /// GET/HEAD return the read frozen at the first time, the etag flips like
    /// [`Action::FlipEtag`]; conditional PUTs lose (`Precondition`). An **unconditional**
    /// PUT (`Overwrite`) reaches the store and heals the object — from then on the rule has
    /// no effect ([`Tear::healed`]).
    Tear(Tear),
    /// Performs the operation **and then** sleeps before answering — the write is already
    /// on the store but the caller does not know yet. The shape of "a PUT that timed out
    /// but actually wrote". Applies to `put` and `get` (the GET has already read before
    /// answering: the caller holds the old version while the store has changed).
    DelayResponse(Duration),
    /// Let the first `pass` matching requests through, then block every later request at
    /// the gate — blocks exactly the `pass + 1`-th write of an object (e.g. the commit CAS
    /// of `cluster.conf` after the CAS writing the intent).
    GateAfter { pass: u32, gate: Gate },
    /// The deterministic simulation's tripwire — see [`Tripwire`]. `After`/`Torn` apply to
    /// `put` and `delete`; for other operations they act like `Before`.
    Trip(Tripwire),
    /// Sleep for a duration drawn from `dist`, then go on. One [`SimRng`] per rule, seeded
    /// from `seed`; samples are drawn in the order of matching requests, so same seed + same
    /// order ⇒ same sequence of delays.
    Latency { dist: LatencyDist, seed: u64 },
    /// S3-style throttling: once the prefix partition's budget is spent, `503 SlowDown` —
    /// see [`SlowDown`].
    SlowDown(SlowDown),
    /// **An ambiguous outcome**: the request reaches the store **and is applied**, then the
    /// caller gets an answer that does not say so — see [`Ambiguity`]. The first `skip`
    /// matching requests go through normally; after that `times: Some(n)` is the next n
    /// requests, `None` is forever. Applies to `put` and `delete`; for other operations
    /// (nothing to apply) it acts like `Fail`.
    Ambiguous {
        reply: Ambiguity,
        skip: u32,
        times: Option<u32>,
    },
    /// **A PUT landing late**: the request flies for `Duration` before it is applied to the
    /// store — and is applied **even if the caller has stopped waiting** (the future was
    /// cancelled by `put_timeout`), like an HTTP request already sent: cancelling on the
    /// client side cannot pull it back off the network. A caller still waiting gets the
    /// real answer of the application. Only applies to `put`; for other operations it acts
    /// like [`Action::Delay`].
    LandLate(Duration),
    /// The inner action applies only to the **first** time each (operation, path) pair
    /// matches the rule; later ones go through — "each op fails exactly once" (like Neon's
    /// `UnreliableWrapper`). The inner action runs like a bare action: its `times`/`skip`
    /// are not counted.
    EachOnce(Box<Action>),
    /// The inner action applies to each matching request with **probability** `p`, drawn
    /// from a [`SimRng`] seeded with `seed` (in request order, under the lock — same seed +
    /// same order ⇒ same sequence of faults). Like [`Action::EachOnce`], the inner action
    /// runs like a bare action. Store profiles use it for fault rates (the `faults` section
    /// of [`profile`]).
    Chance {
        p: f64,
        seed: u64,
        inner: Box<Action>,
    },
}

/// The answer of an [`Action::Ambiguous`] request — after it has been applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ambiguity {
    /// `object_store::Error::Generic`: "a 5xx or a timeout that actually wrote", nobody
    /// retries.
    Error,
    /// The shape of object_store 0.14 **retrying a 5xx on its own** (conditional PUTs
    /// included): the first attempt wrote, the retry sends the identical request and the
    /// caller gets that attempt's answer — `Precondition` for a PUT `Update` (the etag was
    /// changed by the first attempt itself), `AlreadyExists` for `Create`, `Ok` for
    /// `Overwrite` and `delete`.
    Retried,
}

/// One rule: which operations, which paths, what to do.
#[derive(Clone, Debug)]
pub struct Rule {
    pub ops: OpSet,
    pub path: PathMatch,
    pub action: Action,
}

impl Rule {
    pub fn new(ops: impl Into<OpSet>, path: PathMatch, action: Action) -> Rule {
        Rule {
            ops: ops.into(),
            path,
            action,
        }
    }

    /// `put` of a WAL object — SlateDB keeps its WAL under `<root>/wal/`.
    pub fn wal_put(action: Action) -> Rule {
        Rule::new(OpSet::PUT, PathMatch::Contains("/wal/".into()), action)
    }

    /// `put` of an object of the shared WAL log — `nodewal/<node>/<inc>/<stripe>/`.
    /// `node` filters by node (`None` = every node).
    pub fn nodewal_put(node: Option<&str>, action: Action) -> Rule {
        let path = match node {
            Some(n) => format!("nodewal/{n}/"),
            None => "nodewal/".into(),
        };
        Rule::new(OpSet::PUT, PathMatch::Contains(path), action)
    }

    /// `put` of a lease-layer record — `leases/`, `owners/`, `sessions/`, `nodes/`
    /// (matched by segment, like [`PathClass::Lease`]).
    pub fn lease_put(action: Action) -> Rule {
        Rule::new(OpSet::PUT, PathMatch::LeaseLayer, action)
    }

    /// **One** ambiguous `put` ([`Action::Ambiguous`]) on a path containing `path`: the
    /// first matching write is applied, then returns `reply`; after that the rule has no
    /// effect.
    pub fn ambiguous_put(path: &str, reply: Ambiguity) -> Rule {
        Rule::new(
            OpSet::PUT,
            PathMatch::Contains(path.into()),
            Action::Ambiguous {
                reply,
                skip: 0,
                times: Some(1),
            },
        )
    }
}

/// Identifies a rule that was added, to remove it with [`FaultyStore::remove_rule`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RuleId(u64);

/// A class of path, per the layout of SlateDB and of the lease layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathClass {
    Wal,
    /// Logs of the per-node shared WAL: `nodewal/<node>/<inc>/<stripe>/…`.
    NodeWal,
    Manifest,
    Compacted,
    Lease,
    Other,
}

impl PathClass {
    pub const ALL: [PathClass; 6] = [
        PathClass::Wal,
        PathClass::NodeWal,
        PathClass::Manifest,
        PathClass::Compacted,
        PathClass::Lease,
        PathClass::Other,
    ];

    /// Classifies by path segment: the first `wal`, `manifest`, `compacted` or lease-layer
    /// segment (`leases`, `owners`, `sessions`, `nodes`) met decides.
    pub fn of(path: &Path) -> PathClass {
        for part in path.parts() {
            match part.as_ref() {
                "wal" => return PathClass::Wal,
                "nodewal" => return PathClass::NodeWal,
                "manifest" => return PathClass::Manifest,
                "compacted" => return PathClass::Compacted,
                "leases" | "owners" | "sessions" | "incarnations" | "nodes" => {
                    return PathClass::Lease
                }
                _ => {}
            }
        }
        PathClass::Other
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// The kind of object **within** a path class — for the `object` label of the per-caller
/// counters: the last meaningful segment of the path (`.compactions`, `rules`, `seal`…),
/// not the specific name, so the set of values is bounded.
pub fn object_kind(path: &Path) -> &'static str {
    let parts: Vec<_> = path.parts().collect();
    let last = parts
        .last()
        .map(|p| p.as_ref().to_string())
        .unwrap_or_default();
    let has = |s: &str| parts.iter().any(|p| p.as_ref() == s);
    if last.ends_with(".compactions") {
        return "compactions";
    }
    // `gc/manifest.boundary`, `gc/compactions.boundary` of the fork (F7).
    if last.ends_with(".boundary") {
        return "boundary";
    }
    if last.ends_with(".manifest") || has("manifest") {
        return "manifest";
    }
    if last.ends_with(".sst") {
        return "sst";
    }
    if has("nodewal") {
        return match last.as_str() {
            "index" => "nodewal-index",
            _ if has("floor") => "nodewal-floor",
            _ => "nodewal-object",
        };
    }
    for (seg, kind) in [
        ("sessions", "sessions"),
        ("owners", "owners"),
        ("leases", "leases"),
        ("nodes", "nodes"),
        ("deployments", "deployments"),
        ("catalog", "catalog"),
        ("auth", "auth"),
        ("capture-requests", "capture-requests"),
        ("capture-artifacts", "capture-artifacts"),
        ("captures", "captures"),
        ("quarantine", "quarantine"),
        ("metering", "metering"),
        ("archive", "archive"),
        ("values", "values"),
        ("artifacts", "artifacts"),
        ("gc-barriers", "gc-barriers"),
        ("ops", "ops"),
        ("checkpoints", "checkpoints"),
        ("wal", "wal"),
    ] {
        if has(seg) {
            return kind;
        }
    }
    match last.as_str() {
        "cluster.conf" => "cluster.conf",
        "cluster-version" => "cluster-version",
        "rules" => "rules",
        "tenants" => "tenants",
        "range-windows" => "range-windows",
        "shedding" => "shedding",
        "format" => "format",
        _ => "other",
    }
}

const N_OPS: usize = Op::ALL.len();
const N_CLASSES: usize = PathClass::ALL.len();

/// The kind of a `put`: overwrite, create (`If-None-Match: *`) or CAS (`If-Match`).
///
/// S3 charges the three kinds the same, but they behave differently under load (a lost
/// conditional PUT is a wasted request), so the cost model counts them separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PutKind {
    Overwrite,
    Create,
    Update,
}

impl PutKind {
    pub const ALL: [PutKind; 3] = [PutKind::Overwrite, PutKind::Create, PutKind::Update];

    fn of(mode: &PutMode) -> PutKind {
        match mode {
            PutMode::Overwrite => PutKind::Overwrite,
            PutMode::Create => PutKind::Create,
            PutMode::Update(_) => PutKind::Update,
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// One S3 LIST page returns at most this many keys — each page is one billed request.
pub const LIST_PAGE: u64 = 1000;

/// Snapshot of the counters: number of requests by (operation, path class).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreCounts {
    cells: [[u64; N_CLASSES]; N_OPS],
    /// GET body bytes returned, by path class.
    get_bytes: [u64; N_CLASSES],
    /// `put`s by kind, by path class.
    put_kinds: [[u64; N_CLASSES]; 3],
    /// `put` body bytes, by path class.
    put_bytes: [u64; N_CLASSES],
    /// LIST pages (one page per [`LIST_PAGE`] elements, at least one), by path class of
    /// the prefix.
    list_pages: [u64; N_CLASSES],
}

impl StoreCounts {
    pub fn get(&self, op: Op, class: PathClass) -> u64 {
        self.cells[op.index()][class.index()]
    }

    pub fn op(&self, op: Op) -> u64 {
        self.cells[op.index()].iter().sum()
    }

    pub fn class(&self, class: PathClass) -> u64 {
        self.cells.iter().map(|row| row[class.index()]).sum()
    }

    pub fn total(&self) -> u64 {
        self.cells.iter().flatten().sum()
    }

    /// GET body bytes (including range GETs) of one path class.
    pub fn get_bytes(&self, class: PathClass) -> u64 {
        self.get_bytes[class.index()]
    }

    /// Number of `put`s of kind `kind` for one path class.
    pub fn put_kind(&self, kind: PutKind, class: PathClass) -> u64 {
        self.put_kinds[kind.index()][class.index()]
    }

    /// `put` body bytes of one path class.
    pub fn put_bytes(&self, class: PathClass) -> u64 {
        self.put_bytes[class.index()]
    }

    /// Number of LIST pages (billed LIST requests) of one path class — larger than
    /// `get(Op::List, class)` when one listing is longer than a page.
    pub fn list_pages(&self, class: PathClass) -> u64 {
        self.list_pages[class.index()]
    }

    /// The increase since `earlier` — for computing rates over a measurement window.
    pub fn since(&self, earlier: &StoreCounts) -> StoreCounts {
        let mut out = StoreCounts::default();
        for o in 0..N_OPS {
            for c in 0..N_CLASSES {
                out.cells[o][c] = self.cells[o][c].saturating_sub(earlier.cells[o][c]);
            }
        }
        for c in 0..N_CLASSES {
            out.get_bytes[c] = self.get_bytes[c].saturating_sub(earlier.get_bytes[c]);
            out.put_bytes[c] = self.put_bytes[c].saturating_sub(earlier.put_bytes[c]);
            out.list_pages[c] = self.list_pages[c].saturating_sub(earlier.list_pages[c]);
            for k in 0..3 {
                out.put_kinds[k][c] = self.put_kinds[k][c].saturating_sub(earlier.put_kinds[k][c]);
            }
        }
        out
    }

    /// Adds two snapshots — merging the counts of several stores (e.g. main store + WAL store).
    pub fn plus(&self, other: &StoreCounts) -> StoreCounts {
        let mut out = self.clone();
        for o in 0..N_OPS {
            for c in 0..N_CLASSES {
                out.cells[o][c] += other.cells[o][c];
            }
        }
        for c in 0..N_CLASSES {
            out.get_bytes[c] += other.get_bytes[c];
            out.put_bytes[c] += other.put_bytes[c];
            out.list_pages[c] += other.list_pages[c];
            for k in 0..3 {
                out.put_kinds[k][c] += other.put_kinds[k][c];
            }
        }
        out
    }
}

struct Entry {
    id: RuleId,
    rule: Rule,
    /// How many more times `Fail { times: Some(_) }` may fire, or how many more requests
    /// `GateAfter` lets through.
    fails_left: Option<u32>,
    /// Randomness source of `Latency`.
    rng: Option<SimRng>,
    /// Number of requests `Ambiguous` still lets through before firing.
    skip_left: u32,
    /// The (operation, path) pairs `EachOnce` has already corrupted.
    seen: std::collections::BTreeSet<(usize, String)>,
}

/// A per-caller counter cell: (label, operation, path class, object kind).
type SourceKey = (&'static str, usize, usize, &'static str);

#[derive(Default)]
struct State {
    rules: Mutex<(u64, Vec<Entry>)>,
    counts: [[AtomicU64; N_CLASSES]; N_OPS],
    get_bytes: [AtomicU64; N_CLASSES],
    put_kinds: [[AtomicU64; N_CLASSES]; 3],
    put_bytes: [AtomicU64; N_CLASSES],
    list_pages: [AtomicU64; N_CLASSES],
    /// Requests by (caller label, operation, path class) — the label of
    /// [`crate::io_source::tagged`] in effect on the task sending the request.
    by_source: Mutex<std::collections::BTreeMap<SourceKey, u64>>,
    /// Requests carrying a range label ([`crate::io_source::in_range`]) by (caller label,
    /// operation, range) — the background cost per range.
    by_range: Mutex<std::collections::BTreeMap<(&'static str, usize, u32), u64>>,
    /// The cadence of [`Action::FlipEtag`].
    flips: AtomicU64,
    /// Faults injected: `Fail`, `Ambiguous` returning an error, retry-style `Ambiguous` —
    /// see [`FaultyStore::injected`].
    injected: [AtomicU64; 3],
    /// [`Action::LandLate`] PUTs injected — see [`FaultyStore::landed_late`].
    late: AtomicU64,
    /// A `Before`/`After` [`Tripwire`] has fired, or [`FaultyStore::freeze`]: every
    /// request hangs.
    frozen: std::sync::atomic::AtomicBool,
    /// The byte ranges of every GET returned, while recording — see
    /// [`FaultyStore::record_reads`].
    reads: Mutex<Option<Vec<ByteRead>>>,
    /// Every write that reached the store, in order of completion, while recording — see
    /// [`FaultyStore::record_writes`].
    writes: Mutex<Option<Vec<(Op, Path)>>>,
    /// The parent directories of every object written or copied, while keeping empty
    /// directories — see [`FaultyStore::keep_empty_dirs`].
    dirs: Mutex<Option<std::collections::BTreeSet<String>>>,
}

/// One GET returned: the path and the byte range — see [`FaultyStore::record_reads`].
pub type ByteRead = (Path, Range<u64>);

impl State {
    fn note_dirs(&self, path: &Path) {
        if let Some(dirs) = self.dirs.lock().unwrap().as_mut() {
            let parts: Vec<String> = path.parts().map(|p| p.as_ref().to_string()).collect();
            for i in 1..parts.len() {
                dirs.insert(parts[..i].join("/"));
            }
        }
    }

    /// Direct subdirectories of `prefix` the store keeps even though they are empty.
    fn kept_dirs(&self, prefix: &str) -> Vec<Path> {
        let dirs = self.dirs.lock().unwrap();
        let Some(dirs) = dirs.as_ref() else {
            return Vec::new();
        };
        dirs.iter()
            .filter(|d| {
                let rest = if prefix.is_empty() {
                    Some(d.as_str())
                } else {
                    d.strip_prefix(prefix).and_then(|r| r.strip_prefix('/'))
                };
                rest.is_some_and(|r| !r.is_empty() && !r.contains('/'))
            })
            .map(|d| Path::from(d.as_str()))
            .collect()
    }

    fn read(&self, path: &Path, range: Range<u64>) {
        if let Some(log) = self.reads.lock().unwrap().as_mut() {
            log.push((path.clone(), range));
        }
    }

    fn wrote(&self, op: Op, path: &Path) {
        if let Some(log) = self.writes.lock().unwrap().as_mut() {
            log.push((op, path.clone()));
        }
    }
}

/// The effect of the rules on the **outcome** of a request that went through.
#[derive(Default)]
struct Effects {
    flip_etag: bool,
    stale_not_modified: bool,
    /// An [`Action::Tear`] not yet healed.
    tear: Option<Tear>,
    delay_response: Option<Duration>,
    /// After the request reached the store: die (`After`) or return an error (`Torn`).
    trip: Option<TripAt>,
    /// After the request was applied: an ambiguous answer ([`Action::Ambiguous`]).
    ambiguous: Option<Ambiguity>,
    /// The PUT is applied this much later, in a task detached from the caller
    /// ([`Action::LandLate`]).
    land_late: Option<Duration>,
}

impl Effects {
    /// Applies the "after reaching the store" part of a [`Tripwire`].
    async fn after(&self, state: &State, op: Op, path: &Path) -> Result<()> {
        match self.trip {
            Some(TripAt::After) => {
                state.frozen.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                unreachable!()
            }
            Some(TripAt::Torn) => Err(Error::Generic {
                store: "FaultyStore",
                source: format!("injected torn response: {op:?} {path}").into(),
            }),
            _ => Ok(()),
        }
    }
}

/// The error returned to the caller when [`Ambiguity::Error`] fires.
fn ambiguous_error(op: Op, path: &Path) -> Error {
    Error::Generic {
        store: "FaultyStore",
        source: format!("injected ambiguous response (applied): {op:?} {path}").into(),
    }
}

impl State {
    /// Counts, then runs every matching rule in order. Actions are chosen under the lock and
    /// carried out outside it — a gate or a delay must not hold the lock.
    async fn intercept(&self, op: Op, path: &Path) -> Result<Effects> {
        if self.frozen.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let class = PathClass::of(path).index();
        self.counts[op.index()][class].fetch_add(1, Ordering::Relaxed);
        *self
            .by_source
            .lock()
            .unwrap()
            .entry((
                crate::io_source::current(),
                op.index(),
                class,
                object_kind(path),
            ))
            .or_default() += 1;
        if let Some(range) = crate::io_source::current_range() {
            *self
                .by_range
                .lock()
                .unwrap()
                .entry((crate::io_source::current(), op.index(), range))
                .or_default() += 1;
        }
        let actions: Vec<Action> = {
            let mut rules = self.rules.lock().unwrap();
            rules
                .1
                .iter_mut()
                .filter(|e| e.rule.ops.contains(op) && e.rule.path.matches(path))
                .filter_map(|e| match (&e.rule.action, &mut e.fails_left) {
                    (Action::Fail { times: Some(_) }, Some(0)) => None,
                    (Action::Fail { times: Some(_) }, Some(left)) => {
                        *left -= 1;
                        Some(e.rule.action.clone())
                    }
                    (Action::GateAfter { .. }, Some(left)) if *left > 0 => {
                        *left -= 1;
                        None
                    }
                    (Action::FailNth { .. }, _) if e.skip_left > 0 => {
                        e.skip_left -= 1;
                        None
                    }
                    (Action::FailNth { .. }, Some(0)) => None,
                    (Action::FailNth { .. }, left) => {
                        *left = Some(0);
                        Some(Action::Fail { times: None })
                    }
                    (Action::Ambiguous { .. }, _) if e.skip_left > 0 => {
                        e.skip_left -= 1;
                        None
                    }
                    (Action::Ambiguous { .. }, Some(0)) => None,
                    (Action::Ambiguous { .. }, left) => {
                        if let Some(left) = left {
                            *left -= 1;
                        }
                        Some(e.rule.action.clone())
                    }
                    (Action::EachOnce(inner), _) => e
                        .seen
                        .insert((op.index(), path.to_string()))
                        .then(|| (**inner).clone()),
                    (Action::Chance { p, inner, .. }, _) => {
                        let rng = e.rng.as_mut().expect("a Chance rule has an rng");
                        // The top 53 bits ⇒ a uniform real in [0, 1).
                        let draw = (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
                        (draw < *p).then(|| (**inner).clone())
                    }
                    // Draw the sample **under** the lock: sample order = request order.
                    (Action::Latency { dist, .. }, _) => {
                        let rng = e.rng.as_mut().expect("a Latency rule has an rng");
                        Some(Action::Delay(dist.sample(rng)))
                    }
                    (action, _) => Some(action.clone()),
                })
                .collect()
        };
        let mut effects = Effects::default();
        for action in actions {
            match action {
                Action::Gate(gate) | Action::GateAfter { gate, .. } => gate.pass().await,
                Action::Delay(d) => tokio::time::sleep(d).await,
                Action::Latency { .. } => unreachable!("turned into Delay under the lock"),
                Action::SlowDown(s) => s.admit(op, path).await?,
                Action::Fail { .. } | Action::FailNth { .. } => {
                    self.injected[0].fetch_add(1, Ordering::Relaxed);
                    return Err(Error::Generic {
                        store: "FaultyStore",
                        source: format!("injected failure: {op:?} {path}").into(),
                    });
                }
                Action::Hang => std::future::pending::<()>().await,
                Action::Precondition => {
                    return Err(Error::Precondition {
                        path: path.to_string(),
                        source: format!("injected precondition: {op:?}").into(),
                    })
                }
                Action::Ambiguous { reply, .. } if matches!(op, Op::Put | Op::Delete) => {
                    let i = match reply {
                        Ambiguity::Error => 1,
                        Ambiguity::Retried => 2,
                    };
                    self.injected[i].fetch_add(1, Ordering::Relaxed);
                    effects.ambiguous = Some(reply)
                }
                Action::Ambiguous { .. } => {
                    return Err(Error::Generic {
                        store: "FaultyStore",
                        source: format!(
                            "injected failure (ambiguous, nothing to apply): {op:?} {path}"
                        )
                        .into(),
                    })
                }
                // Already opened at the choice step; with two nested layers the inner one does nothing.
                Action::EachOnce(_) | Action::Chance { .. } => {}
                Action::FlipEtag => effects.flip_etag = true,
                Action::StaleNotModified => effects.stale_not_modified = true,
                Action::Tear(t) if !t.healed() => effects.tear = Some(t),
                Action::Tear(_) => {}
                Action::DelayResponse(d) => effects.delay_response = Some(d),
                Action::LandLate(d) if op == Op::Put => {
                    self.late.fetch_add(1, Ordering::Relaxed);
                    effects.land_late = Some(d);
                }
                Action::LandLate(d) => tokio::time::sleep(d).await,
                Action::Trip(wire) => match wire.hit() {
                    None => {}
                    Some(TripAt::Before) => {
                        self.frozen.store(true, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    }
                    Some(at) if matches!(op, Op::Put | Op::Delete) => effects.trip = Some(at),
                    Some(_) => {
                        self.frozen.store(true, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    }
                },
            }
        }
        Ok(effects)
    }

    /// A fake etag for a GET hit by [`Action::FlipEtag`]: two values, alternating.
    fn flipped(&self, meta: &mut ObjectMeta) {
        let n = self.flips.fetch_add(1, Ordering::Relaxed);
        let base = meta.e_tag.clone().unwrap_or_default();
        meta.e_tag = Some(format!("{base}-torn{}", n % 2));
    }
}

/// An object store injecting faults by rule, delegating everything else to the inner store.
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<State>,
}

impl FaultyStore {
    /// A GET of an [`Action::Tear`] object not yet healed: the frozen copy, a flipping etag.
    async fn torn_get(&self, t: &Tear, location: &Path) -> Result<GetResult> {
        use slatedb::object_store::GetResultPayload;
        let mut got = self.inner.get_opts(location, GetOptions::default()).await?;
        let meta = got.meta.clone();
        let fresh = got.bytes().await?;
        let body = t.0.frozen.lock().unwrap().get_or_insert(fresh).clone();
        got = GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once({
                let body = body.clone();
                async move { Ok(body) }
            }))),
            meta: ObjectMeta {
                size: body.len() as u64,
                ..meta
            },
            range: 0..body.len() as u64,
            attributes: Default::default(),
            extensions: Default::default(),
        };
        self.state.flipped(&mut got.meta);
        Ok(got)
    }

    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<FaultyStore> {
        // Requests of SlateDB's internal tasks carry that task's label — see
        // [`crate::io_source`].
        crate::io_source::install_slatedb_hook();
        Arc::new(FaultyStore {
            inner,
            state: Arc::default(),
        })
    }

    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }

    pub fn add_rule(&self, rule: Rule) -> RuleId {
        let mut rules = self.state.rules.lock().unwrap();
        rules.0 += 1;
        let id = RuleId(rules.0);
        let fails_left = match &rule.action {
            Action::Fail { times } | Action::Ambiguous { times, .. } => *times,
            Action::GateAfter { pass, .. } => Some(*pass),
            Action::FailNth { .. } => Some(1),
            _ => None,
        };
        let skip_left = match &rule.action {
            Action::Ambiguous { skip, .. } | Action::FailNth { skip } => *skip,
            _ => 0,
        };
        let rng = match &rule.action {
            Action::Latency { seed, .. } | Action::Chance { seed, .. } => Some(SimRng::new(*seed)),
            _ => None,
        };
        rules.1.push(Entry {
            id,
            rule,
            fails_left,
            rng,
            skip_left,
            seen: Default::default(),
        });
        id
    }

    /// Removes a rule. Requests waiting at that rule's gate keep waiting — opening the gate
    /// releases them. Returns `false` if the rule is gone.
    pub fn remove_rule(&self, id: RuleId) -> bool {
        let mut rules = self.state.rules.lock().unwrap();
        let before = rules.1.len();
        rules.1.retain(|e| e.id != id);
        rules.1.len() != before
    }

    pub fn clear_rules(&self) {
        self.state.rules.lock().unwrap().1.clear();
    }

    /// Faults injected so far: `(fail, ambiguous, retried)` — `Fail` (including through
    /// [`Action::Chance`]/[`Action::EachOnce`]), `Ambiguous` returning an error after
    /// applying, retry-style `Ambiguous`. An ambiguous one only counts when the request is
    /// a `put`/`delete`; a write that itself failed has nothing ambiguous but is still
    /// counted (counted when the action is chosen).
    pub fn injected(&self) -> (u64, u64, u64) {
        let i = &self.state.injected;
        (
            i[0].load(Ordering::Relaxed),
            i[1].load(Ordering::Relaxed),
            i[2].load(Ordering::Relaxed),
        )
    }

    /// Number of [`Action::LandLate`] PUTs injected (counted when the action is chosen).
    pub fn landed_late(&self) -> u64 {
        self.state.late.load(Ordering::Relaxed)
    }

    /// Freeze: every request from now on hangs forever — `kill -9` seen from the bucket.
    pub fn freeze(&self) {
        self.state.frozen.store(true, Ordering::SeqCst);
    }

    /// The store is frozen (by [`Self::freeze`] or a [`Tripwire`]).
    pub fn is_frozen(&self) -> bool {
        self.state.frozen.load(Ordering::SeqCst)
    }

    /// Keep empty directories like SeaweedFS (its filer has real directories): from now on,
    /// the parent directory of every object written or copied still shows up in
    /// `list_with_delimiter` after its last object is deleted. A recursive `list` does not
    /// change — it only lists objects. Found in a soak: logs already removed for good were
    /// seen again forever by the dead-log sweep.
    pub fn keep_empty_dirs(&self) {
        *self.state.dirs.lock().unwrap() = Some(Default::default());
    }

    /// Starts recording the byte ranges of every GET (dropping what was recorded before).
    /// Lets tests say "no block was read twice" — GET counts alone cannot tell two
    /// different blocks from one block read again.
    pub fn record_reads(&self) {
        *self.state.reads.lock().unwrap() = Some(Vec::new());
    }

    /// The byte ranges GET since [`Self::record_reads`], in the order returned; stops
    /// recording.
    pub fn take_reads(&self) -> Vec<ByteRead> {
        self.state.reads.lock().unwrap().take().unwrap_or_default()
    }

    /// Starts recording the order of every write **that reached the store** — `put`
    /// (conditional ones included), `put_multipart` (when opened), `copy`, `delete` — in the
    /// order the store answered (dropping what was recorded before). Lets tests pin a
    /// library's PUT order (G4 in `irreversible.md` §0.3: a clone writes the child manifest
    /// before the checkpoint on the ancestor). Writes blocked or corrupted by a rule before
    /// reaching the store are not included.
    pub fn record_writes(&self) {
        *self.state.writes.lock().unwrap() = Some(Vec::new());
    }

    /// The writes since [`Self::record_writes`], in order; stops recording.
    pub fn take_writes(&self) -> Vec<(Op, Path)> {
        self.state.writes.lock().unwrap().take().unwrap_or_default()
    }

    /// Number of requests by caller label ([`crate::io_source`]): `(label, operation,
    /// path class, count)`, non-zero cells only, sorted by label.
    pub fn counts_by_source(&self) -> Vec<(&'static str, Op, PathClass, u64)> {
        let mut out: std::collections::BTreeMap<(&'static str, usize, usize), u64> =
            Default::default();
        for (&(src, o, c, _), &n) in self.state.by_source.lock().unwrap().iter() {
            *out.entry((src, o, c)).or_default() += n;
        }
        out.into_iter()
            .map(|((src, o, c), n)| (src, Op::ALL[o], PathClass::ALL[c], n))
            .collect()
    }

    /// Requests carrying a range label ([`crate::io_source::in_range`]): `(label,
    /// operation, range, count)`, non-zero cells only, sorted by label.
    pub fn counts_by_range(&self) -> Vec<(&'static str, Op, u32, u64)> {
        self.state
            .by_range
            .lock()
            .unwrap()
            .iter()
            .map(|(&(src, o, r), &n)| (src, Op::ALL[o], r, n))
            .collect()
    }

    /// Like [`Self::counts_by_source`], further split by [`object_kind`].
    pub fn counts_by_source_object(&self) -> Vec<(&'static str, Op, PathClass, &'static str, u64)> {
        self.state
            .by_source
            .lock()
            .unwrap()
            .iter()
            .map(|(&(src, o, c, k), &n)| (src, Op::ALL[o], PathClass::ALL[c], k, n))
            .collect()
    }

    pub fn counts(&self) -> StoreCounts {
        let mut out = StoreCounts::default();
        for o in 0..N_OPS {
            for c in 0..N_CLASSES {
                out.cells[o][c] = self.state.counts[o][c].load(Ordering::Relaxed);
            }
        }
        for c in 0..N_CLASSES {
            out.get_bytes[c] = self.state.get_bytes[c].load(Ordering::Relaxed);
            out.put_bytes[c] = self.state.put_bytes[c].load(Ordering::Relaxed);
            out.list_pages[c] = self.state.list_pages[c].load(Ordering::Relaxed);
            for k in 0..3 {
                out.put_kinds[k][c] = self.state.put_kinds[k][c].load(Ordering::Relaxed);
            }
        }
        out
    }
}

impl fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultyStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let class = PathClass::of(location).index();
        self.state.put_kinds[PutKind::of(&opts.mode).index()][class]
            .fetch_add(1, Ordering::Relaxed);
        self.state.put_bytes[class].fetch_add(payload.content_length() as u64, Ordering::Relaxed);
        let effects = self.state.intercept(Op::Put, location).await?;
        if let Some(t) = &effects.tear {
            if !matches!(opts.mode, PutMode::Overwrite) {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "injected torn object: conditional PUT lost".into(),
                });
            }
            t.0.healed.store(true, Ordering::SeqCst);
        }
        let res = match effects.land_late {
            None => {
                self.inner
                    .put_opts(location, payload.clone(), opts.clone())
                    .await
            }
            // A detached task: if the caller is cancelled the application still lands on time.
            Some(d) => {
                let (inner, state, path) = (
                    Arc::clone(&self.inner),
                    Arc::clone(&self.state),
                    location.clone(),
                );
                let (p, o) = (payload.clone(), opts.clone());
                let (tx, rx) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    tokio::time::sleep(d).await;
                    let res = inner.put_opts(&path, p, o).await;
                    if res.is_ok() {
                        state.note_dirs(&path);
                    }
                    let _ = tx.send(res);
                });
                rx.await.unwrap_or_else(|_| {
                    Err(Error::Generic {
                        store: "FaultyStore",
                        source: format!("late PUT abandoned: {location}").into(),
                    })
                })
            }
        };
        if let Some(d) = effects.delay_response {
            tokio::time::sleep(d).await;
        }
        if res.is_ok() {
            self.state.note_dirs(location);
            self.state.wrote(Op::Put, location);
        }
        effects.after(&self.state, Op::Put, location).await?;
        // A write that never reached the store has nothing ambiguous: return its real error.
        match (effects.ambiguous, res) {
            (Some(Ambiguity::Error), Ok(_)) => Err(ambiguous_error(Op::Put, location)),
            (Some(Ambiguity::Retried), Ok(_)) => self.inner.put_opts(location, payload, opts).await,
            (_, res) => res,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.state.intercept(Op::PutMultipart, location).await?;
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        self.state.wrote(Op::PutMultipart, location);
        Ok(upload)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let op = if options.head { Op::Head } else { Op::Get };
        let effects = self.state.intercept(op, location).await?;
        if let Some(t) = &effects.tear {
            return self.torn_get(t, location).await;
        }
        if effects.stale_not_modified && options.if_none_match.is_some() {
            return Err(Error::NotModified {
                path: location.to_string(),
                source: "injected stale 304".into(),
            });
        }
        let mut got = self.inner.get_opts(location, options).await?;
        if !got.range.is_empty() {
            self.state.read(location, got.range.clone());
        }
        self.state.get_bytes[PathClass::of(location).index()].fetch_add(
            got.range.end.saturating_sub(got.range.start),
            Ordering::Relaxed,
        );
        if effects.flip_etag {
            self.state.flipped(&mut got.meta);
        }
        if let Some(d) = effects.delay_response {
            tokio::time::sleep(d).await;
        }
        Ok(got)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.state.intercept(Op::Get, location).await?;
        let got = self.inner.get_ranges(location, ranges).await?;
        for r in ranges {
            self.state.read(location, r.clone());
        }
        self.state.get_bytes[PathClass::of(location).index()]
            .fetch_add(got.iter().map(|b| b.len() as u64).sum(), Ordering::Relaxed);
        Ok(got)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Delete one by one so the rules can check each path. Loses the inner store's
        // batching — fine for tests.
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let state = Arc::clone(&state);
                async move {
                    let location = location?;
                    let effects = state.intercept(Op::Delete, &location).await?;
                    inner.delete(&location).await?;
                    state.wrote(Op::Delete, &location);
                    effects.after(&state, Op::Delete, &location).await?;
                    match effects.ambiguous {
                        Some(Ambiguity::Error) => {
                            return Err(ambiguous_error(Op::Delete, &location))
                        }
                        // S3 DELETE is idempotent: the retry returns 204 though the object is gone.
                        Some(Ambiguity::Retried) => match inner.delete(&location).await {
                            Ok(()) | Err(Error::NotFound { .. }) => {}
                            Err(e) => return Err(e),
                        },
                        None => {}
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_impl(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_impl(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let at = prefix.cloned().unwrap_or_default();
        self.state.intercept(Op::List, &at).await?;
        let mut got = self.inner.list_with_delimiter(prefix).await?;
        for d in self.state.kept_dirs(at.as_ref()) {
            if !got.common_prefixes.contains(&d) {
                got.common_prefixes.push(d);
            }
        }
        let n = (got.objects.len() + got.common_prefixes.len()) as u64;
        self.state.list_pages[PathClass::of(&at).index()]
            .fetch_add(n.saturating_sub(1) / LIST_PAGE + 1, Ordering::Relaxed);
        Ok(got)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.state.intercept(Op::Copy, to).await?;
        self.inner.copy_opts(from, to, options).await?;
        self.state.note_dirs(to);
        self.state.wrote(Op::Copy, to);
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.state.intercept(Op::Rename, to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

impl FaultyStore {
    /// `list` returns a synchronous stream, so the rules run as the stream's first element:
    /// an injected error becomes the only `Err` element, a gate holds the stream empty.
    fn list_impl(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        let prefix = prefix.cloned();
        let offset = offset.cloned();
        futures::stream::once(async move {
            let at = prefix.clone().unwrap_or_default();
            match state.intercept(Op::List, &at).await {
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                Ok(_) => {
                    // The first page counts at call time; every `LIST_PAGE` elements after
                    // that is one more page — exactly the number of requests S3 sees.
                    let pages = &state.list_pages[PathClass::of(&at).index()];
                    pages.fetch_add(1, Ordering::Relaxed);
                    let inner_stream = match &offset {
                        Some(offset) => inner.list_with_offset(prefix.as_ref(), offset),
                        None => inner.list(prefix.as_ref()),
                    };
                    let state = Arc::clone(&state);
                    let class = PathClass::of(&at).index();
                    let mut seen = 0u64;
                    inner_stream
                        .inspect(move |item| {
                            if item.is_ok() {
                                if seen > 0 && seen.is_multiple_of(LIST_PAGE) {
                                    state.list_pages[class].fetch_add(1, Ordering::Relaxed);
                                }
                                seen += 1;
                            }
                        })
                        .boxed()
                }
            }
        })
        .flatten()
        .boxed()
    }
}

/// A minimal object store with **etag = hash of the content**, like S3.
///
/// Enough for the lease layer: conditional PUT, GET, delete, copy, LIST by prefix. The rest
/// is `unimplemented!()` so a misuse shows up at once instead of returning meaningless
/// data. It exists because `InMemory` derives etags from the **write**, so it cannot
/// reproduce the class of bug "rewriting exactly the old content ⇒ the etag does not
/// change".
#[derive(Debug, Default)]
pub struct ContentEtagStore {
    files: Arc<Mutex<std::collections::BTreeMap<Path, Bytes>>>,
}

impl fmt::Display for ContentEtagStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentEtagStore")
    }
}

/// Content hash. Need not be strong — it must be **a function of the content**.
fn content_etag(body: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in body {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    format!("{h:016x}")
}

fn content_meta(location: &Path, body: &Bytes) -> ObjectMeta {
    ObjectMeta {
        location: location.clone(),
        last_modified: Default::default(),
        size: body.len() as u64,
        e_tag: Some(content_etag(body)),
        version: None,
    }
}

#[async_trait::async_trait]
impl ObjectStore for ContentEtagStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        use slatedb::object_store::PutMode;
        let body: Bytes = payload.iter().flatten().copied().collect();
        let mut files = self.files.lock().expect("mutex");
        let current = files.get(location);
        match opts.mode {
            PutMode::Create if current.is_some() => {
                return Err(Error::AlreadyExists {
                    path: location.to_string(),
                    source: "already exists".into(),
                })
            }
            PutMode::Update(v) if current.map(|c| content_etag(c)) != v.e_tag => {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "etag mismatch".into(),
                });
            }
            _ => {}
        }
        let tag = content_etag(&body);
        files.insert(location.clone(), body);
        Ok(PutResult {
            e_tag: Some(tag),
            version: None,
            extensions: Default::default(),
        })
    }

    async fn get_opts(&self, location: &Path, _o: GetOptions) -> Result<GetResult> {
        use slatedb::object_store::GetResultPayload;
        let files = self.files.lock().expect("mutex");
        let body = files.get(location).ok_or_else(|| Error::NotFound {
            path: location.to_string(),
            source: "not found".into(),
        })?;
        let meta = content_meta(location, body);
        let size = body.len() as u64;
        let body = body.clone();
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(body) },
            ))),
            meta,
            range: 0..size,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let files = Arc::clone(&self.files);
        locations
            .map(move |p| {
                let p = p?;
                files.lock().expect("mutex").remove(&p);
                Ok(p)
            })
            .boxed()
    }

    async fn put_multipart_opts(
        &self,
        _l: &Path,
        _o: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!("the lease layer does not use multipart")
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let files = self.files.lock().expect("mutex");
        let metas: Vec<Result<ObjectMeta>> = files
            .iter()
            .filter(|(p, _)| prefix.is_none_or(|pre| p.prefix_matches(pre)))
            .map(|(p, b)| Ok(content_meta(p, b)))
            .collect();
        futures::stream::iter(metas).boxed()
    }

    async fn list_with_delimiter(&self, _p: Option<&Path>) -> Result<ListResult> {
        unimplemented!()
    }

    /// Copy with overwrite (the trash of `delete_owner`).
    async fn copy_opts(&self, from: &Path, to: &Path, _o: CopyOptions) -> Result<()> {
        let mut files = self.files.lock().expect("mutex");
        let body = files.get(from).cloned().ok_or_else(|| Error::NotFound {
            path: from.to_string(),
            source: "not found".into(),
        })?;
        files.insert(to.clone(), body);
        Ok(())
    }

    async fn rename_opts(&self, _f: &Path, _t: &Path, _o: RenameOptions) -> Result<()> {
        unimplemented!()
    }
}

// ───────────────────────────────────────────── CAS for stores without CAS

/// Moved to [`crate::local`]: a node on `file://` uses it in production, under the
/// exclusive lock of the directory.
pub use crate::local::LocalCas;

/// The retry index rows `(due, id)` still on `db` — for tests matching the derived index
/// against the entity rows (stale index rows of moved retry schedules).
pub async fn retry_index_rows(
    db: &slatedb::Db,
) -> Result<Vec<(u64, crate::InvocationId)>, slatedb::Error> {
    let from = crate::keyspace::ix_retry(0, &[0; 24]);
    let to = crate::keyspace::ix_retry_due_upper_bound(u64::MAX);
    let mut it = db.scan(from..to).await?;
    let mut out = Vec::new();
    while let Some(kv) = it.next().await? {
        if let Some(r) = crate::keyspace::parse_ix_retry(&kv.key) {
            out.push(r);
        }
    }
    Ok(out)
}

/// A root session grant valid until `valid_until`, with no suspension (expired means
/// lost) — for tests of code that follows a session's validity
/// ([`crate::Session::live_changes`]).
pub fn validity_until(valid_until: tokio::time::Instant) -> crate::ValidityHandle {
    crate::ValidityHandle::root_with_pause(valid_until, Duration::ZERO, None)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        slatedb::{object_store::memory::InMemory, Db, Settings},
    };

    async fn open_db(store: &Arc<FaultyStore>, settings: Settings) -> Db {
        let store: Arc<dyn ObjectStore> = store.clone();
        Db::builder("db", store)
            .with_settings(settings)
            .build()
            .await
            .unwrap()
    }

    fn fast_flush() -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn gate_holds_wal_flush_until_opened() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let db = open_db(&store, fast_flush()).await;

        let gate = Gate::closed();
        store.add_rule(Rule::wal_put(Action::Gate(gate.clone())));
        let before = db.status().durable_seq;
        let handle = db.put(b"k", b"v").await.unwrap();

        // Make sure the flush has reached the gate, then assert it cannot get through.
        tokio::time::timeout(Duration::from_secs(5), async {
            while gate.blocked() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("WAL flush never reached the gate");
        let held = tokio::time::timeout(Duration::from_millis(200), handle.await_durable()).await;
        assert!(held.is_err(), "write became durable through a closed gate");
        assert_eq!(db.status().durable_seq, before);

        gate.open();
        tokio::time::timeout(Duration::from_secs(5), handle.await_durable())
            .await
            .expect("write not durable after the gate opened")
            .unwrap();
        assert!(db.status().durable_seq > before);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn fail_rule_surfaces_error() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let db = open_db(
            &store,
            Settings {
                object_store_max_retries: Some(0),
                ..fast_flush()
            },
        )
        .await;

        store.add_rule(Rule::wal_put(Action::Fail { times: None }));
        let handle = db.put(b"k", b"v").await.unwrap();
        let res = tokio::time::timeout(Duration::from_secs(5), handle.await_durable())
            .await
            .expect("await_durable hung instead of failing");
        assert!(
            res.is_err(),
            "WAL put failed but the write was acked durable"
        );
    }

    #[tokio::test]
    async fn fail_times_then_recovers_and_remove_rule() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("x/leases/partition-0");
        let id = store.add_rule(Rule::lease_put(Action::Fail { times: Some(2) }));
        assert!(store.put(&p, "a".into()).await.is_err());
        assert!(store.put(&p, "a".into()).await.is_err());
        store.put(&p, "a".into()).await.unwrap();

        let id2 = store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Prefix("x/".into()),
            Action::Fail { times: None },
        ));
        assert!(store.get(&p).await.is_err());
        // HEAD is its own operation: a GET rule does not touch it.
        store.head(&p).await.unwrap();
        assert!(store.remove_rule(id2));
        assert!(!store.remove_rule(id2));
        store.get(&p).await.unwrap();
        assert!(store.remove_rule(id));
    }

    /// `FailNth`: exactly matching request number `skip + 1` fails, those before and after pass.
    #[tokio::test]
    async fn fail_nth_fails_exactly_one_request() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("x/leases/partition-0");
        store.add_rule(Rule::lease_put(Action::FailNth { skip: 2 }));
        store.put(&p, "a".into()).await.unwrap();
        store.put(&p, "a".into()).await.unwrap();
        assert!(store.put(&p, "a".into()).await.is_err());
        for _ in 0..3 {
            store.put(&p, "a".into()).await.unwrap();
        }
        assert_eq!(store.injected().0, 1);
    }

    /// `LandLate`: the caller stops waiting (timeout) yet the write still lands on time —
    /// a later `Create` sees the `AlreadyExists` of that very write. A caller waiting long
    /// enough gets the real answer.
    #[tokio::test(start_paused = true)]
    async fn land_late_applies_after_the_caller_gave_up() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("x/nodewal/n/1/0/00000000000000000001");
        let id = store.add_rule(Rule::nodewal_put(
            None,
            Action::LandLate(Duration::from_secs(3)),
        ));
        let create = || PutOptions::from(PutMode::Create);
        let gave_up = tokio::time::timeout(
            Duration::from_secs(2),
            store.put_opts(&p, "a".into(), create()),
        )
        .await;
        assert!(
            gave_up.is_err(),
            "the caller must time out before the write lands"
        );
        assert!(
            store.inner().head(&p).await.is_err(),
            "written before it was due"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        let got = store.inner().get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"a", "an abandoned write must still land");
        assert!(store.remove_rule(id));
        let err = store.put_opts(&p, "a".into(), create()).await.unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }), "{err:?}");

        let q = Path::from("x/nodewal/n/1/0/00000000000000000002");
        store.add_rule(Rule::nodewal_put(
            None,
            Action::LandLate(Duration::from_millis(50)),
        ));
        store.put_opts(&q, "b".into(), create()).await.unwrap();
        assert_eq!(store.landed_late(), 2);
    }

    fn ambiguous(reply: Ambiguity, skip: u32, times: Option<u32>) -> Action {
        Action::Ambiguous { reply, skip, times }
    }

    /// `Ambiguous` applies the write **and then** answers: `Error` ⇒ an error while the
    /// object is there; `Retried` ⇒ the answer to the identical resend — `Create` sees
    /// `AlreadyExists`, `Update` sees `Precondition`, while the object is the first one's.
    #[tokio::test]
    async fn ambiguous_applies_the_write_then_misreports_it() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("x/owners/range-1");
        let read = |store: Arc<FaultyStore>| async move {
            store
                .inner()
                .get(&Path::from("x/owners/range-1"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        };

        let id = store.add_rule(Rule::lease_put(ambiguous(Ambiguity::Error, 0, Some(1))));
        let err = store.put(&p, "a".into()).await.unwrap_err();
        assert!(matches!(err, Error::Generic { .. }), "{err:?}");
        assert_eq!(
            &read(store.clone()).await[..],
            b"a",
            "the write must have reached the store"
        );
        // `times: Some(1)` is used up: the next one goes straight through.
        let v = store.put(&p, "b".into()).await.unwrap();
        assert!(store.remove_rule(id));

        store.add_rule(Rule::lease_put(ambiguous(Ambiguity::Retried, 0, Some(2))));
        let upd = PutOptions::from(PutMode::Update(v.into()));
        let err = store.put_opts(&p, "c".into(), upd).await.unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }), "{err:?}");
        assert_eq!(&read(store.clone()).await[..], b"c");
        let q = Path::from("x/owners/range-2");
        let err = store
            .put_opts(&q, "d".into(), PutMode::Create.into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }), "{err:?}");
        assert_eq!(
            &store.inner().get(&q).await.unwrap().bytes().await.unwrap()[..],
            b"d"
        );
    }

    /// `skip` lets the first n matching requests through (e.g. the intent CAS) before
    /// firing (the rollback CAS); an ambiguous `delete` really deleted.
    #[tokio::test]
    async fn ambiguous_skips_then_fires_on_put_and_delete() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("cluster.conf");
        store.add_rule(Rule::new(
            OpSet::PUT | OpSet::DELETE,
            PathMatch::Contains("cluster.conf".into()),
            ambiguous(Ambiguity::Error, 1, None),
        ));
        store.put(&p, "v1".into()).await.unwrap();
        assert!(store.put(&p, "v2".into()).await.is_err());
        assert_eq!(
            &store.inner().get(&p).await.unwrap().bytes().await.unwrap()[..],
            b"v2"
        );
        assert!(store.delete(&p).await.is_err());
        assert!(matches!(
            store.inner().get(&p).await,
            Err(Error::NotFound { .. })
        ));
        // GET has nothing to apply: like `Fail`.
        assert!(store.get(&p).await.is_err());
    }

    /// `EachOnce`: each (operation, path) pair fails exactly the first time.
    #[tokio::test]
    async fn each_once_fails_every_op_path_pair_exactly_once() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        store.add_rule(Rule::new(
            OpSet::PUT | OpSet::GET,
            PathMatch::Any,
            Action::EachOnce(Box::new(Action::Fail { times: None })),
        ));
        let (a, b) = (Path::from("a"), Path::from("b"));
        assert!(store.put(&a, "1".into()).await.is_err());
        store.put(&a, "1".into()).await.unwrap();
        assert!(store.put(&b, "1".into()).await.is_err());
        store.put(&b, "1".into()).await.unwrap();
        assert!(
            store.get(&a).await.is_err(),
            "GET is another operation: its first time fails"
        );
        store.get(&a).await.unwrap();
        store.put(&a, "2".into()).await.unwrap();

        // Wraps an ambiguous outcome: the first time writes then reports an error, later ones are normal.
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Any,
            Action::EachOnce(Box::new(ambiguous(Ambiguity::Error, 0, None))),
        ));
        assert!(store.put(&a, "x".into()).await.is_err());
        assert_eq!(
            &store.inner().get(&a).await.unwrap().bytes().await.unwrap()[..],
            b"x"
        );
        store.put(&a, "y".into()).await.unwrap();
    }

    #[tokio::test]
    async fn counters_track_ops_and_path_classes() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let t0 = store.counts();
        let db = open_db(&store, fast_flush()).await;
        db.put(b"k", b"v")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.close().await.unwrap();

        let c = store.counts().since(&t0);
        assert!(c.get(Op::Put, PathClass::Wal) >= 1, "{c:?}");
        assert!(c.get(Op::Put, PathClass::Manifest) >= 1, "{c:?}");
        assert_eq!(c.class(PathClass::Lease), 0);

        let lease = Path::from("p/leases/partition-3");
        let mark = store.counts();
        store.put(&lease, "x".into()).await.unwrap();
        store.head(&lease).await.unwrap();
        store.get(&lease).await.unwrap();
        store.list(Some(&Path::from("p"))).collect::<Vec<_>>().await;
        store.delete(&lease).await.unwrap();
        let d = store.counts().since(&mark);
        for op in [Op::Put, Op::Head, Op::Get, Op::Delete] {
            assert_eq!(d.get(op, PathClass::Lease), 1, "{op:?}: {d:?}");
        }
        assert_eq!(d.get(Op::List, PathClass::Other), 1);
        assert_eq!(d.total(), 5);
        assert_eq!(d.class(PathClass::Lease), 4);
        assert_eq!(d.op(Op::Put), 1);
        assert!(store.inner().head(&lease).await.is_err());
    }

    #[tokio::test]
    async fn put_kinds_bytes_and_list_pages_are_counted() {
        use slatedb::object_store::UpdateVersion;
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("x/leases/a");
        store.put(&p, "abc".into()).await.unwrap();
        let created = store
            .put_opts(
                &Path::from("x/leases/b"),
                "de".into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &Path::from("x/leases/b"),
                "f".into(),
                PutMode::Update(UpdateVersion {
                    e_tag: created.e_tag,
                    version: None,
                })
                .into(),
            )
            .await
            .unwrap();
        let c = store.counts();
        for kind in PutKind::ALL {
            assert_eq!(c.put_kind(kind, PathClass::Lease), 1, "{kind:?}");
        }
        assert_eq!(c.op(Op::Put), 3);
        assert_eq!(c.put_bytes(PathClass::Lease), 6);

        // 2 500 objects under one prefix: one LIST is three pages.
        for i in 0..2500 {
            store
                .inner()
                .put(&Path::from(format!("y/{i:05}")), "z".into())
                .await
                .unwrap();
        }
        let mark = store.counts();
        let n = store
            .list(Some(&Path::from("y")))
            .collect::<Vec<_>>()
            .await
            .len();
        assert_eq!(n, 2500);
        let d = store.counts().since(&mark);
        assert_eq!(d.get(Op::List, PathClass::Other), 1);
        assert_eq!(d.list_pages(PathClass::Other), 3);
        let both = d.plus(&d);
        assert_eq!(both.list_pages(PathClass::Other), 6);
    }

    /// A 503 from the throttling rule reaches SlateDB as `Error::Generic` — the variant
    /// `RetryingObjectStore` retries. Heavy throttling, **without** the client's retry
    /// loop: the write is still durable, only slow, and real 503s reached the caller.
    #[tokio::test]
    async fn slowdown_503_is_retried_by_slatedb_until_durable() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let db = open_db(&store, fast_flush()).await;
        let slow = SlowDown::new(SlowDownConfig {
            write_rate: 2.0,
            read_rate: 1000.0,
            initial_depth: 0,
            max_depth: 0,
            split_after: Duration::from_secs(3600),
            client_retry: None,
            seed: 3,
        });
        store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::SlowDown(slow.clone()),
        ));
        let mut handles = Vec::new();
        for i in 0..8u8 {
            handles.push(db.put([i], b"v").await.unwrap());
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        for h in handles {
            tokio::time::timeout(Duration::from_secs(30), h.await_durable())
                .await
                .expect("write not durable under 503s")
                .unwrap();
        }
        let st = slow.stats();
        assert!(st.surfaced > 0, "the throttling rule did not fire: {st:?}");
        assert_eq!(st.retries, 0);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn latency_action_samples_per_rule_in_request_order() {
        let mk = || {
            let store = FaultyStore::wrap(Arc::new(InMemory::new()));
            store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Any,
                Action::Latency {
                    dist: LatencyDist::LogNormal {
                        p50: Duration::from_millis(10),
                        p99: Duration::from_millis(90),
                    },
                    seed: 11,
                },
            ));
            store
        };
        let run = |store: Arc<FaultyStore>| async move {
            let mut out = Vec::new();
            for i in 0..10 {
                let t = tokio::time::Instant::now();
                store
                    .put(&Path::from(format!("k{i}")), "v".into())
                    .await
                    .unwrap();
                out.push(t.elapsed());
            }
            // A GET does not match the rule: no delay.
            let t = tokio::time::Instant::now();
            store.get(&Path::from("k0")).await.unwrap();
            assert_eq!(t.elapsed(), Duration::ZERO);
            out
        };
        let a = run(mk()).await;
        assert_eq!(a, run(mk()).await);
        let mut rng = SimRng::new(11);
        let dist = LatencyDist::LogNormal {
            p50: Duration::from_millis(10),
            p99: Duration::from_millis(90),
        };
        // Exactly SimRng(seed)'s sample sequence, rounded to the virtual clock's resolution.
        for got in &a {
            let want = dist.sample(&mut rng);
            assert!(
                got.abs_diff(want) <= Duration::from_millis(1),
                "{got:?} vs {want:?}"
            );
        }
    }

    #[tokio::test]
    async fn hang_and_delay_actions() {
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let p = Path::from("a/b");
        let id = store.add_rule(Rule::new(OpSet::PUT, PathMatch::Any, Action::Hang));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), store.put(&p, "x".into()))
                .await
                .is_err()
        );
        store.remove_rule(id);
        store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::Delay(Duration::from_millis(50)),
        ));
        let t = tokio::time::Instant::now();
        store.put(&p, "x".into()).await.unwrap();
        assert!(t.elapsed() >= Duration::from_millis(50));
    }
}
