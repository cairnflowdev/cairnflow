// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Deterministic simulation of the whole cluster (`docs/testing.md`): swarm,
//! library slots, a liveness phase, shrinking.
//!
//! Three cluster nodes on one `InMemory` bucket, sharing **one** thread: a
//! tokio `current_thread` runtime with a paused clock (`start_paused`), the
//! engines of every range on that runtime's `LocalSet`
//! (`NodeConfig::inline_engines`), SlateDB's compaction/GC on that same
//! runtime. Every source of nondeterminism goes through **one** seeded source
//! ([`SimRng`]):
//!
//! - **wall clock, pid, random bytes** (invocation ids, session names, SlateDB's
//!   seeds and clock): [`SimEntropy`] installed into `cairn_store::entropy`;
//! - **network:** `netfilter::LocalNet` — node-to-node, node-to-SDK and
//!   sim-to-node requests go straight into the receiver's `Router`, no socket;
//!   the fate of each node-to-node message is drawn from the seed ([`SimNet`]:
//!   pass, drop, delay, hold then release);
//! - **store:** one [`SimStore`] per node over a `FaultyStore` over the shared
//!   bucket — each request draws: straight through, delayed (virtual clock), or
//!   failed (`Generic`);
//! - **load:** `Caller{i}` calls `Callee` in another range, the receiving node
//!   drawn from the seed; `Flaky{j}` fails in a retryable way `k_j` times and
//!   then recovers, under the deployment's retry policy (`maxAttempts`,
//!   exponential backoff) drawn from the seed — `k_j ≥ maxAttempts` must end
//!   `paused` 575.
//!
//! **Swarm** ([`Plan`], [`Off`]): each fault feature is on with probability 1/2
//! drawn from the seed — store faults, network faults, killing a node,
//! ambiguous conditional writes to objects cairn manages itself (rule 7),
//! **death in a library slot** (a SlateDB SST is on the bucket, the manifest not
//! yet written — G1, NW-7), reviving a dead node — the WAL kind and the shared
//! WAL's knobs (T, GC interval, W, S) — and **flow control** ([`Flow`]): the
//! tenant's `rate` limit, the `concurrency` limit of the scope `Flaky` runs in,
//! the node's slot wait limit (`queue_wait`), so that throttled turns and
//! exhausted slot waits really happen.
//!
//! **One run** ([`run`]): a fault phase (load under faults), a healing phase
//! (stop injecting faults, possibly revive nodes), a **liveness phase** — run
//! until no unfinished work is left ([`pending`]: invocations not done, ranges
//! nobody leads, intents still there), each unfinished item with a reason; this
//! is the **drain check (a)** —, then the **real attempts check (b)**
//! ([`real_attempts`]): a `Flaky` `paused` 575 is only valid when the SDK has
//! seen all `maxAttempts` values of `ctx.attempt` — no attempt is spent without
//! the SDK being called. Then shut the cluster down and run a full `cairn-ctl
//! verify`: a finding no loop heals is a safety failure, a finding a background
//! loop can heal that is still left after convergence is a liveness failure
//! (except named known holes, [`KNOWN_RESIDUE`]). A red seed is **shrunk along
//! the swarm dimensions** ([`shrink`]) and printed as a `#[test]`.
//!
//! **The trace** is the sequence of events the sim sees: every store request
//! (node, operation, path, fate), every node-to-node message, every step of the
//! load and its result — each event stamped with virtual milliseconds. Same seed
//! ⇒ same trace hash (needs `tokio_unstable`, [`same_seed_same_trace`]).
//!
//! `CAIRN_SIM_SEED=<n>` first seed (default 1), `CAIRN_SIM_SEEDS=<k>` number of
//! seeds; `CAIRN_SIM_TRACE_DIR=<dir>` writes each run's trace to a file for
//! `diff`; `CAIRN_SIM_RT_SEED=<n>` (only in a `tokio_unstable` build) seeds the
//! runtime's RNG with `n` instead of the sim's seed — keeping the plan, changing
//! the `select!` schedule, to hunt a red schedule reproducibly.

#![allow(unexpected_cfgs)]

// The nondeterminism lints (`clippy.toml`) are **on** for this file: the sim
// must not call the real clock, randomness, maps or network. `common` is the
// harness shared by every test (real-clock deadlines, a direct HTTP client);
// `fake_sdk` reads the wall clock through `entropy`, so even the `Sleep` step
// runs on virtual time.
#[allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "harness shared by every test; the parts the sim uses call no nondeterminism source"
)]
mod common;

use {
    anyhow::{anyhow, ensure, Result},
    bytes::Bytes,
    cairn_server::{
        netfilter::{Held, LocalNet, NetFilter, Verdict},
        node::{self, NodeConfig, NodeMode, RunningNode},
    },
    cairn_store::{testkit::FaultyStore, ObjectStores, StoreLocation, TenantId},
    common::fake_sdk::{FakeSdk, Step},
    futures::{stream::BoxStream, StreamExt},
    slatedb::object_store::{
        memory::InMemory, path::Path, CopyOptions, Error as OsError, GetOptions, GetResult,
        ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, RenameOptions, Result as OsResult,
    },
    std::{
        collections::BTreeMap,
        fmt,
        hash::{Hash, Hasher},
        ops::Range,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

const NODES: [&str; 3] = ["alpha", "beta", "gamma"];
const PARTITIONS: u16 = 4;
const TTL: Duration = Duration::from_secs(2);
/// Number of `Caller` invocations in the load.
const CALLS: usize = 6;
/// Number of `Flaky` invocations in the load.
const FLAKY: usize = 4;
/// The scope of `Flaky` — the `concurrency` limit of [`Flow`] hangs on it.
const FLAKY_SCOPE: &str = "sim";

// ───────────────────────────────────────────────────────── random source

/// splitmix64 — one source for the whole run. Shared through `Arc<Mutex<_>>`
/// because the store and the network filter must be `Send + Sync`; every draw
/// happens on the sim's single thread, so draw order = event order.
#[derive(Clone)]
struct SimRng(Arc<Mutex<u64>>);

impl SimRng {
    fn new(seed: u64) -> Self {
        Self(Arc::new(Mutex::new(seed)))
    }

    fn next(&self) -> u64 {
        let mut s = self.0.lock().unwrap();
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    /// True with probability `permille`/1000.
    fn chance(&self, permille: u64) -> bool {
        self.below(1000) < permille
    }
}

/// The process's nondeterminism source (`cairn_store::entropy`) for the sim
/// thread: wall clock = a fixed epoch + the virtual clock; a fixed pid; random
/// bytes from the seed.
struct SimEntropy {
    rng: SimRng,
    t0: tokio::time::Instant,
}

/// 2026-01-01T00:00:00Z — the wall-clock epoch of every run.
const SIM_EPOCH: Duration = Duration::from_secs(1_767_225_600);

impl cairn_store::entropy::Source for SimEntropy {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + SIM_EPOCH + self.t0.elapsed()
    }

    fn process_id(&self) -> u32 {
        4242
    }

    fn fill(&self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.rng.next().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

// ───────────────────────────────────────────────────────── trace

/// The event sequence of one run. Each line is stamped with virtual ms since setup.
#[derive(Clone)]
struct Trace {
    lines: Arc<Mutex<Vec<String>>>,
    t0: tokio::time::Instant,
    /// Virtual ms of the latest event — the [`Watchdog`] thread reads it.
    last_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl Trace {
    fn new() -> Self {
        Self {
            lines: Arc::default(),
            t0: tokio::time::Instant::now(),
            last_ms: Arc::default(),
        }
    }

    fn push(&self, what: impl fmt::Display) {
        let ms = self.t0.elapsed().as_millis();
        self.last_ms.store(ms as u64, Ordering::SeqCst);
        self.lines.lock().unwrap().push(format!("{ms:>7} {what}"));
    }

    fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    fn hash(&self) -> u64 {
        // FNV-1a: deterministic across processes (`DefaultHasher` makes no such promise).
        let mut h = Fnv(0xcbf2_9ce4_8422_2325);
        for l in self.lines.lock().unwrap().iter() {
            l.hash(&mut h);
        }
        h.finish()
    }
}

struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }
}

// ───────────────────────────────────────────────────────── seeded store

/// Probability (per mille) of each fate of a store request.
#[derive(Clone, Copy, Debug)]
struct StoreFaults {
    fail: u64,
    delay: u64,
    max_delay_ms: u64,
}

/// Wraps a node's store: traces every request and injects faults/latency drawn
/// from the seed. It sits **above** the node's `FaultyStore` (without modifying
/// `FaultyStore`), so `FaultyStore`'s rules (freezing a node) still work.
struct SimStore {
    node: &'static str,
    inner: Arc<dyn ObjectStore>,
    rng: SimRng,
    trace: Trace,
    faults: StoreFaults,
    /// Healing phase: stop injecting faults and latency.
    healed: Arc<AtomicBool>,
}

impl fmt::Debug for SimStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimStore({})", self.node)
    }
}

impl fmt::Display for SimStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimStore({})", self.node)
    }
}

impl SimStore {
    /// The fate of one request: drawn from the seed, written to the trace, sleeps if delayed.
    async fn intercept(&self, op: &str, path: &Path) -> OsResult<()> {
        let fate = if self.healed.load(Ordering::SeqCst) {
            "ok".to_string()
        } else if self.faults.fail > 0 && self.rng.chance(self.faults.fail) {
            "fail".to_string()
        } else if self.faults.delay > 0 && self.rng.chance(self.faults.delay) {
            format!("delay{}", 1 + self.rng.below(self.faults.max_delay_ms))
        } else {
            "ok".to_string()
        };
        self.trace
            .push(format_args!("store {} {op} {path} {fate}", self.node));
        if fate == "fail" {
            return Err(OsError::Generic {
                store: "sim",
                source: "fault injected by the seed".into(),
            });
        }
        if let Some(ms) = fate.strip_prefix("delay") {
            tokio::time::sleep(Duration::from_millis(ms.parse().unwrap())).await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for SimStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.intercept("put", location).await?;
        let r = self.inner.put_opts(location, payload, opts).await;
        if let Err(e) = &r {
            self.trace.push(format_args!(
                "store {} put-err {location} {}",
                self.node,
                kind(e)
            ));
        }
        r
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.intercept("put-mp", location).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let op = if options.head { "head" } else { "get" };
        self.intercept(op, location).await?;
        let r = self.inner.get_opts(location, options).await;
        if let Err(e) = &r {
            self.trace.push(format_args!(
                "store {} {op}-err {location} {}",
                self.node,
                kind(e)
            ));
        }
        r
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        self.intercept("get-ranges", location).await?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        let me = SimStore {
            node: self.node,
            inner: Arc::clone(&self.inner),
            rng: self.rng.clone(),
            trace: self.trace.clone(),
            faults: self.faults,
            healed: self.healed.clone(),
        };
        let me = Arc::new(me);
        locations
            .then(move |location| {
                let me = Arc::clone(&me);
                async move {
                    let location = location?;
                    me.intercept("delete", &location).await?;
                    me.inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.list_impl(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.list_impl(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.intercept("list-delim", prefix.unwrap_or(&Path::default()))
            .await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.intercept("copy", to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OsResult<()> {
        self.intercept("rename", to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

impl SimStore {
    fn list_impl(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let me = Arc::new(SimStore {
            node: self.node,
            inner: Arc::clone(&self.inner),
            rng: self.rng.clone(),
            trace: self.trace.clone(),
            faults: self.faults,
            healed: self.healed.clone(),
        });
        let prefix = prefix.cloned();
        let offset = offset.cloned();
        futures::stream::once(async move {
            let at = prefix.clone().unwrap_or_default();
            match me.intercept("list", &at).await {
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                Ok(()) => match &offset {
                    Some(o) => me.inner.list_with_offset(prefix.as_ref(), o),
                    None => me.inner.list(prefix.as_ref()),
                },
            }
        })
        .flatten()
        .boxed()
    }
}

fn kind(e: &OsError) -> &'static str {
    match e {
        OsError::NotFound { .. } => "not-found",
        OsError::AlreadyExists { .. } => "exists",
        OsError::Precondition { .. } => "precondition",
        OsError::NotModified { .. } => "not-modified",
        _ => "other",
    }
}

// ───────────────────────────────────────────────────────── seeded network

/// Probability (per mille) of each fate of a node-to-node message.
#[derive(Clone, Copy, Debug)]
struct NetFaults {
    drop: u64,
    delay: u64,
    hold: u64,
    max_delay_ms: u64,
}

/// The sim's `NetFilter`: the fate of each message is drawn from the seed; a
/// held message is released after a virtual interval also drawn from the seed.
struct SimNet {
    rng: SimRng,
    trace: Trace,
    faults: NetFaults,
    /// Healing phase: every message goes straight through.
    healed: Arc<AtomicBool>,
    /// `host:port` of the internal port → node name (ports are chosen by the
    /// OS and differ between runs — the trace only carries names).
    names: Mutex<BTreeMap<String, &'static str>>,
    /// Node pairs currently cut off from each other (both directions) — the
    /// control-plane swarm ([`CtrlFaults`]); `(n, "*")` = `n` cut off from every peer.
    cuts: Mutex<Vec<(String, String)>>,
}

impl SimNet {
    fn name_of(&self, to: &str) -> String {
        self.names
            .lock()
            .unwrap()
            .get(to)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "?".into())
    }
}

impl NetFilter for SimNet {
    fn verdict(&self, from: &str, to: &str, path: &str) -> Verdict {
        let f = self.faults;
        let to_name = self.name_of(to);
        let cut = self.cuts.lock().unwrap().iter().any(|(a, b)| {
            let pair = |x: &str, y: &str| x == a && (y == b || b == "*");
            pair(from, &to_name) || pair(&to_name, from)
        });
        let v = if self.healed.load(Ordering::SeqCst) {
            Verdict::Pass
        } else if cut || self.rng.chance(f.drop) {
            Verdict::Drop
        } else if self.rng.chance(f.delay) {
            Verdict::Delay(Duration::from_millis(1 + self.rng.below(f.max_delay_ms)))
        } else if self.rng.chance(f.hold) {
            Verdict::Hold
        } else {
            Verdict::Pass
        };
        self.trace
            .push(format_args!("net {from}->{to_name} {path} {v:?}"));
        v
    }

    fn hold(&self, held: Held) {
        let after = Duration::from_millis(1 + self.rng.below(5_000));
        let trace = self.trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let path = held.path().to_string();
            let got = held.release().await;
            trace.push(format_args!("net release {path} {got:?}"));
        });
    }
}

// ───────────────────────────────────────────────────────── swarm

/// Features of a run that can be switched off entirely (swarm): each feature
/// is on with probability 1/2 drawn from the seed, and `Off` forces it off —
/// for shrinking along the swarm dimensions. Every value is still drawn in the
/// same order even when its feature is off, so switching one feature off does
/// not shuffle the rest of the plan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Off {
    /// Store faults and latency drawn from the seed.
    store: bool,
    /// Drop, delay, hold node-to-node messages.
    net: bool,
    /// Kill a node in the middle of the load.
    kill: bool,
    /// Ambiguous conditional writes to objects cairn manages itself (rule 7).
    ambiguous: bool,
    /// Death in a SlateDB slot: the SST is on the bucket, the manifest not yet written.
    slot: bool,
    /// Revive the dead node in the healing phase.
    revive: bool,
    /// Rules (`rate`, `concurrency`) and the slot wait limit ([`Flow`]).
    flow: bool,
    /// The quiet-renewal gate on from the start: the first node to come up
    /// alone enters `mode=solo`, later nodes challenge it; a revived node
    /// challenges the old incarnation's `solo` record.
    quiet: bool,
    /// Control plane: kill exactly the node holding `control/coordinator`.
    kill_coord: bool,
    /// Cut one node ↔ coordinator (network, both directions) for a while.
    cut_coord: bool,
    /// Cut one node ↔ bucket (every store request of it fails) for a while.
    cut_bucket: bool,
    /// Hang one node (a paused VM): its store blocks at a gate and it is cut off
    /// from every peer for a while, then it carries on.
    pause: bool,
    /// Hidden sabotage, only for the acceptance test of check (b): hide attempt
    /// 0 from the fake SDK's trace — exactly the shape of an attempt spent
    /// without the handler being called. Not a swarm dimension.
    sabotage_attempts: bool,
    /// Hidden sabotage, only for the acceptance test of the liveness phase: the
    /// healing phase freezes the store of **every** node instead of healing.
    /// Not a swarm dimension (not in [`Off::FEATURES`]).
    sabotage_heal: bool,
}

/// Switch off one feature of [`Off`].
type Knock = fn(&mut Off);

impl Off {
    const FEATURES: [(&'static str, Knock); 12] = [
        ("store", |o| o.store = true),
        ("net", |o| o.net = true),
        ("kill", |o| o.kill = true),
        ("ambiguous", |o| o.ambiguous = true),
        ("slot", |o| o.slot = true),
        ("revive", |o| o.revive = true),
        ("flow", |o| o.flow = true),
        ("quiet", |o| o.quiet = true),
        ("kill_coord", |o| o.kill_coord = true),
        ("cut_coord", |o| o.cut_coord = true),
        ("cut_bucket", |o| o.cut_bucket = true),
        ("pause", |o| o.pause = true),
    ];
}

/// Every knob of one run, drawn from the seed.
#[derive(Clone, Copy, Debug)]
struct Plan {
    store: StoreFaults,
    net: NetFaults,
    /// Kill node `.0` `.1` virtual ms after the load starts: its store hangs
    /// forever (`FaultyStore::freeze`), both ports leave the network — a
    /// `kill -9` as seen from the bucket and from the peers.
    kill: Option<(usize, u64)>,
    /// Rate (per mille) of ambiguous writes/deletes on objects cairn manages
    /// itself, and the rule's seed (`Action::Chance` + `Ambiguity::Retried`).
    ambiguous: Option<(u64, u64)>,
    /// Node `.0` dies right after its `.1 + 1`-th SST reaches the bucket
    /// (`Tripwire` `After` on `compacted/`): SlateDB's "uploaded, manifest not
    /// yet written" slot (G1, NW-7).
    slot: Option<(usize, u64)>,
    /// The healing phase revives the dead node (new incarnation, same name).
    revive: bool,
    /// How long the fault phase lasts (virtual ms) after the load is sent.
    chaos_ms: u64,
    /// Shared WAL knobs: forced-flush age T, GC interval, W, S — small values
    /// make the L0 flush path, the floor GC and the dead-log sweep run often in
    /// one run (NW-4, NW-7, NW-8), and give the `slot` tripwire SSTs to fire on.
    age_flush_ms: u64,
    gc_ms: u64,
    inflight: usize,
    stripes: u16,
    /// Linger L of the cut (µs, `NodeWalConfig::linger`).
    linger_us: u64,
    /// Rules and the slot wait limit (swarm `flow`).
    flow: Option<Flow>,
    /// Retry policy of every `Flaky` (always present — it is load, not a fault).
    retry: RetryPlan,
    /// `Flaky{j}` fails its first `fails[j]` times (`Step::FailBelow`), then recovers.
    fails: [u32; FLAKY],
    /// Each `Flaky` turn stands still this long (virtual ms, `Step::Stall`)
    /// before failing/completing — holding a `concurrency` slot long enough for
    /// other turns to wait past `queue_wait` and really get throttled.
    stall_ms: u64,
    /// The `control/quiet-lease` gate is on before the nodes come up (swarm
    /// `quiet`); off ⇒ nodes come up with `--quiet-lease off` (`direct`).
    quiet: bool,
    /// Control-plane faults.
    ctrl: CtrlFaults,
}

/// Control-plane faults, one of each kind, at `.after` virtual ms after the
/// load starts. Most meaningful when the gate is on (`quiet`: nodes delegate
/// renewal to the coordinator — LS-14/LS-15), but drawn independently.
#[derive(Clone, Copy, Debug, Default)]
struct CtrlFaults {
    /// Kill the coordinator at that moment (`kill -9` like `kill`).
    kill_coord: Option<u64>,
    /// Cut node `.0` (or the next node if it is the coordinator) ↔ the
    /// coordinator from `.1` for `.2` ms.
    cut_coord: Option<(usize, u64, u64)>,
    /// Every store request of node `.0` fails from `.1` for `.2` ms.
    cut_bucket: Option<(usize, u64, u64)>,
    /// Node `.0` hangs (store blocked at a gate, cut off from every peer) from `.1` for `.2` ms.
    pause: Option<(usize, u64, u64)>,
}

/// Flow control of one run.
#[derive(Clone, Copy, Debug)]
struct Flow {
    /// The `rate` limit (CU/s) and `burst` of tenant `default` — each node holds
    /// `rate / number of leading nodes`; every turn (Caller, Callee, Flaky) is subject to it.
    rate: u32,
    burst: u32,
    /// The `concurrency` limit of scope [`FLAKY_SCOPE`].
    concurrency: u32,
    /// `TurnLimits::queue_wait` of every node: a turn waiting for a slot longer
    /// than this is dropped and rescheduled (`Throttled`).
    queue_wait_ms: u64,
}

/// Retry policy of handler `Flaky{j}/run` (manifest).
#[derive(Clone, Copy, Debug)]
struct RetryPlan {
    max_attempts: u32,
    initial_ms: u64,
    max_ms: u64,
}

impl Plan {
    fn draw(rng: &SimRng, off: Off) -> Plan {
        let on = |forced_off: bool| {
            let pick = rng.chance(500);
            pick && !forced_off
        };
        // The "shared WAL or root WAL" draw from before the root WAL was removed:
        // keep the draw so every old seed still yields the same remaining
        // knobs.
        let _ = rng.chance(500);
        let store_on = on(off.store);
        let store = StoreFaults {
            fail: rng.below(15),
            delay: 50 + rng.below(200),
            max_delay_ms: 5 + rng.below(80),
        };
        let net_on = on(off.net);
        let net = NetFaults {
            drop: rng.below(30),
            delay: rng.below(100),
            hold: rng.below(20),
            max_delay_ms: 1 + rng.below(300),
        };
        let kill_on = on(off.kill);
        let kill = (rng.below(NODES.len() as u64) as usize, rng.below(3_000));
        let ambiguous_on = on(off.ambiguous);
        let ambiguous = (1 + rng.below(50), rng.next());
        let slot_on = on(off.slot);
        let slot = (rng.below(NODES.len() as u64) as usize, rng.below(4));
        let revive_on = on(off.revive);
        let chaos_ms = 1_000 + rng.below(9_000);
        let age_flush_ms = 500 + rng.below(5_000);
        let gc_ms = 500 + rng.below(10_000);
        let inflight = 1 + rng.below(2) as usize;
        let stripes = 1 + rng.below(2) as u16;
        let flow_on = on(off.flow);
        // A `Flaky` turn costs `⌈stall_ms / 100⌉` CU plus the durable steps; the
        // tenant splits `rate` over 3 nodes. The drawn range keeps the drain
        // within the [`CONVERGE`] limit yet tight enough for turns to be
        // throttled (seed 20 with `rate` 3 and a 779 ms stall needs ~120 virtual
        // s to drain — correct, just slow).
        let flow = Flow {
            rate: 12 + rng.below(36) as u32,
            burst: 1 + rng.below(10) as u32,
            concurrency: 1 + rng.below(3) as u32,
            queue_wait_ms: 50 + rng.below(950),
        };
        let max_attempts = 2 + rng.below(4) as u32;
        let retry = RetryPlan {
            max_attempts,
            initial_ms: 20 + rng.below(280),
            max_ms: 500 + rng.below(1_500),
        };
        // Half recover before the limit, half run out of attempts (`paused` 575).
        let fails = std::array::from_fn(|_| rng.below(u64::from(max_attempts) * 2) as u32);
        let stall_ms = rng.below(500);
        // Drawn **last**: a new feature does not shuffle the knobs of old seeds.
        let quiet = on(off.quiet);
        // Control plane — after `quiet`, same reason.
        let window = |rng: &SimRng| {
            (
                rng.below(NODES.len() as u64) as usize,
                rng.below(4_000),
                500 + rng.below(6_000),
            )
        };
        let kill_coord = (on(off.kill_coord), rng.below(4_000));
        let cut_coord = (on(off.cut_coord), window(rng));
        let cut_bucket = (on(off.cut_bucket), window(rng));
        let pause = (on(off.pause), window(rng));
        let ctrl = CtrlFaults {
            kill_coord: kill_coord.0.then_some(kill_coord.1),
            cut_coord: cut_coord.0.then_some(cut_coord.1),
            cut_bucket: cut_bucket.0.then_some(cut_bucket.1),
            pause: pause.0.then_some(pause.1),
        };
        // Linger L of the WAL cut — after `ctrl`, same reason. Leans towards the
        // product default; 0 keeps the old behaviour in the swarm; 4 ms is close
        // to the simulation's 5 ms window.
        let linger_us = [0, 1_000, 1_000, 4_000][rng.below(4) as usize];
        Plan {
            store: if store_on {
                store
            } else {
                StoreFaults {
                    fail: 0,
                    delay: 0,
                    max_delay_ms: 1,
                }
            },
            net: if net_on {
                net
            } else {
                NetFaults {
                    drop: 0,
                    delay: 0,
                    hold: 0,
                    max_delay_ms: 1,
                }
            },
            kill: kill_on.then_some(kill),
            ambiguous: ambiguous_on.then_some(ambiguous),
            slot: slot_on.then_some(slot),
            revive: revive_on,
            chaos_ms,
            age_flush_ms,
            gc_ms,
            inflight,
            stripes,
            linger_us,
            flow: flow_on.then_some(flow),
            retry,
            fails,
            stall_ms,
            quiet,
            ctrl,
        }
    }

    /// Which features are on — the dimensions shrinking tries to switch off.
    fn enabled(&self, feature: &str) -> bool {
        match feature {
            "store" => self.store.fail > 0 || self.store.delay > 0,
            "net" => self.net.drop + self.net.delay + self.net.hold > 0,
            "kill" => self.kill.is_some(),
            "ambiguous" => self.ambiguous.is_some(),
            "slot" => self.slot.is_some(),
            "revive" => self.revive,
            "flow" => self.flow.is_some(),
            "quiet" => self.quiet,
            "kill_coord" => self.ctrl.kill_coord.is_some(),
            "cut_coord" => self.ctrl.cut_coord.is_some(),
            "cut_bucket" => self.ctrl.cut_bucket.is_some(),
            "pause" => self.ctrl.pause.is_some(),
            _ => false,
        }
    }
}

// ───────────────────────────────────────────────────────── cluster

struct SimNode {
    node: RunningNode,
    faulty: Arc<FaultyStore>,
}

async fn start_node(
    bucket: &Arc<InMemory>,
    name: &'static str,
    sim: &Shared,
    plan: &Plan,
) -> Result<SimNode> {
    let faulty = FaultyStore::wrap(bucket.clone());
    let healed = sim.healed.load(Ordering::SeqCst);
    if let Some((permille, seed)) = plan.ambiguous.filter(|_| !healed) {
        use cairn_store::testkit::{Action, Ambiguity, OpSet, PathClass, PathMatch, Rule};
        // Only objects cairn manages itself: on SlateDB's WAL, manifest and SSTs
        // and on the shared WAL log, `AlreadyExists` is a fence (D12, NW-2) —
        // see `common::install_env_faults`.
        for class in [PathClass::Lease, PathClass::Other] {
            faulty.add_rule(Rule::new(
                OpSet::PUT | OpSet::DELETE,
                PathMatch::Class(class),
                Action::Chance {
                    p: permille as f64 / 1000.0,
                    seed,
                    inner: Box::new(Action::Ambiguous {
                        reply: Ambiguity::Retried,
                        skip: 0,
                        times: None,
                    }),
                },
            ));
        }
    }
    if let Some((victim, skip)) = plan.slot {
        if NODES[victim] == name && !healed {
            use cairn_store::testkit::{
                Action, OpSet, PathClass, PathMatch, Rule, TripAt, Tripwire,
            };
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Class(PathClass::Compacted),
                Action::Trip(Tripwire::new(skip, TripAt::After)),
            ));
        }
    }
    let store = Arc::new(SimStore {
        node: name,
        inner: faulty.clone(),
        rng: sim.rng.clone(),
        trace: sim.trace.clone(),
        faults: plan.store,
        healed: sim.healed.clone(),
    });
    let stores = ObjectStores::single(StoreLocation::from_store(store, "", "memory://"));
    let mut cfg = NodeConfig::new(
        stores,
        NodeMode {
            partitions: PARTITIONS,
            lease_ttl: TTL,
            node_id: Some(name.to_string()),
        },
    );
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.node_wal.window = Duration::from_millis(5);
    cfg.node_wal.frame_age_flush = Duration::from_millis(plan.age_flush_ms);
    cfg.node_wal.gc_interval = Duration::from_millis(plan.gc_ms);
    cfg.node_wal.inflight = plan.inflight;
    cfg.node_wal.stripes = plan.stripes;
    cfg.node_wal.linger = Duration::from_micros(plan.linger_us);
    cfg.inline_engines = true;
    // The `quiet` swarm dimension picks between the two protocols: off ⇒ an
    // explicit `direct` — a node setting up a new bucket turns the gate on
    // with delegation by default, so without turning it off both halves of
    // this dimension would be the same. On ⇒ the gate is written before the
    // nodes come up (like `enable-quiet-lease`), and the node setting up sees
    // it already there and leaves it.
    if !plan.quiet {
        cfg.quiet_lease = cairn_store::quiet_lease::Bootstrap::Off;
    }
    if let Some(f) = plan.flow {
        cfg.limits.queue_wait = Some(Duration::from_millis(f.queue_wait_ms));
    }
    let filter: Arc<dyn NetFilter> = sim.net.clone();
    cfg.net_filter = Some(filter);
    // Egress policy on: the sim network hands out addresses on `127.0.0.1`, so
    // allow exactly that — other internal destinations are blocked by the URL
    // layer (`egress_denied_before_local_net`).
    cfg.egress = cairn_server::egress::EgressPolicy {
        deny_private: true,
        allow: vec!["127.0.0.1/32".parse()?],
        ..Default::default()
    };
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("cannot build node {name}: {e}"))?;
    sim.net
        .names
        .lock()
        .unwrap()
        .insert(node.internal_addr.to_string(), name);
    sim.trace.push(format_args!("node {name} up"));
    Ok(SimNode { node, faulty })
}

/// The shared part of one run.
struct Shared {
    rng: SimRng,
    trace: Trace,
    net: Arc<SimNet>,
    healed: Arc<AtomicBool>,
}

/// The cluster of one run: nodes by position in [`NODES`] (a revived node
/// takes the old node's place), death flags, and old nodes waiting to be dropped.
struct Cluster {
    bucket: Arc<InMemory>,
    nodes: Vec<SimNode>,
    dead: Vec<bool>,
    graveyard: Vec<SimNode>,
    /// The victim of the `slot` tripwire ([`Plan::slot`]): until the healing
    /// phase it may die at any moment (its `n`-th SST reaches the bucket), so it
    /// must not count as a node that will survive.
    doomed: Option<usize>,
    /// The node [`Self::kill_planned`] has spared (not killed again later).
    spared: Vec<bool>,
}

impl Cluster {
    /// Kill as planned (`kill` of [`Plan`], `kill_coord` of [`CtrlFaults`]) —
    /// **unless** that kill would leave the cluster with no node except the
    /// victim of a `slot` tripwire that has not fired yet. Three permanent deaths
    /// (kill, coordinator kill, slot tripwire) on three different nodes take down
    /// the whole cluster before the healing phase: no node takes the load,
    /// `send` runs out of attempts ("cannot send"), and without `revive` it does
    /// not converge either — the run goes red because of the plan, not the
    /// product. Whether it happens depends on who holds the coordinator role when
    /// `kill_coord` fires (a schedule race), so it used to make seed 4 flaky.
    /// Returns `false` when skipped.
    fn kill_planned(&mut self, i: usize, why: &str, trace: &Trace) -> bool {
        if self.spared[i] {
            return false;
        }
        let survives = |k: usize| k != i && !self.dead[k] && self.doomed != Some(k);
        if !self.dead[i] && !(0..self.nodes.len()).any(survives) {
            // Once per node: a planned kill is a one-time fault.
            self.spared[i] = true;
            trace.push(format_args!(
                "node {} not killed ({why}): last live node besides the slot tripwire's victim",
                NODES[i]
            ));
            return false;
        }
        self.kill(i, why, trace);
        true
    }

    /// `kill -9` node `i`: its store hangs, both ports leave the network.
    fn kill(&mut self, i: usize, why: &str, trace: &Trace) {
        if self.dead[i] {
            return;
        }
        let n = &self.nodes[i];
        n.faulty.freeze();
        LocalNet::unserve(&n.node.addr.to_string());
        LocalNet::unserve(&n.node.internal_addr.to_string());
        self.dead[i] = true;
        trace.push(format_args!("node {} killed ({why})", NODES[i]));
    }

    /// A node whose store froze itself (the `slot` tripwire fired) dies with it.
    fn reap(&mut self, trace: &Trace) {
        for i in 0..self.nodes.len() {
            if !self.dead[i] && self.nodes[i].faulty.is_frozen() {
                self.kill(i, "SST gap → manifest", trace);
            }
        }
    }

    fn alive(&self) -> impl Iterator<Item = (usize, &SimNode)> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| !self.dead[*i])
    }
}

/// Executor of a run's [`CtrlFaults`]: each fault starts exactly once at its
/// mark (checked on the fault phase's beat); a window ends by itself after its
/// length (its own task, virtual time — not waiting for the next beat, since
/// one `send` can hold the fault phase for a very long time), or in the
/// healing phase.
#[derive(Default)]
struct Ctrl {
    killed: bool,
    cut: bool,
    bucket: bool,
    paused: bool,
    /// Undo work of every open window — the healing phase runs them all.
    undo: Vec<Arc<dyn Fn() + Send + Sync>>,
}

/// Index (in [`NODES`]) of the node holding `control/coordinator`, per the bucket.
async fn coordinator_now(cluster: &Cluster) -> Option<usize> {
    let raw = cluster
        .bucket
        .get(&Path::from("control/coordinator"))
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    if text.lines().any(|l| l == "released") {
        return None;
    }
    let session = text.lines().find_map(|l| l.strip_prefix("session="))?;
    let node = session.rsplit_once('/')?.0;
    NODES.iter().position(|n| *n == node)
}

impl Ctrl {
    /// Open a window: `undo` runs after `len` virtual ms (its own task) or in the
    /// healing phase — whichever comes first; running it twice is harmless.
    fn window(
        &mut self,
        sim: &Shared,
        len: u64,
        what: String,
        undo: impl Fn() + Send + Sync + 'static,
    ) {
        let undo: Arc<dyn Fn() + Send + Sync> = Arc::new(undo);
        self.undo.push(undo.clone());
        let trace = sim.trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(len)).await;
            undo();
            trace.push(format_args!("ctrl {what} healed"));
        });
    }

    /// Start the faults due at `now` virtual ms since the load started.
    async fn tick(&mut self, plan: &CtrlFaults, cluster: &mut Cluster, sim: &Shared, now: u64) {
        let trace = &sim.trace;
        if plan.kill_coord.is_some_and(|at| !self.killed && now >= at) {
            self.killed = true;
            match coordinator_now(cluster).await {
                Some(c) => {
                    cluster.kill_planned(c, "coordinator", trace);
                }
                None => trace.push("ctrl kill_coord: no coordinator"),
            }
        }
        if let Some((n, at, len)) = plan.cut_coord.filter(|(_, at, _)| !self.cut && now >= *at) {
            let _ = at;
            self.cut = true;
            if let Some(c) = coordinator_now(cluster).await {
                let n = if n == c { (n + 1) % NODES.len() } else { n };
                let pair = (NODES[n].to_string(), NODES[c].to_string());
                trace.push(format_args!("ctrl cut {} <-> {}", pair.0, pair.1));
                sim.net.cuts.lock().unwrap().push(pair.clone());
                let net = sim.net.clone();
                self.window(
                    sim,
                    len,
                    format!("cut {} <-> {}", pair.0, pair.1),
                    move || net.cuts.lock().unwrap().retain(|p| *p != pair),
                );
            }
        }
        if let Some((n, at, len)) = plan
            .cut_bucket
            .filter(|(_, at, _)| !self.bucket && now >= *at)
        {
            use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
            let _ = at;
            self.bucket = true;
            if !cluster.dead[n] {
                let faulty = cluster.nodes[n].faulty.clone();
                let id = faulty.add_rule(Rule::new(
                    OpSet::ALL,
                    PathMatch::Any,
                    Action::Fail { times: None },
                ));
                trace.push(format_args!("ctrl bucket cut {}", NODES[n]));
                self.window(sim, len, format!("bucket cut {}", NODES[n]), move || {
                    faulty.remove_rule(id);
                });
            }
        }
        if let Some((n, at, len)) = plan.pause.filter(|(_, at, _)| !self.paused && now >= *at) {
            use cairn_store::testkit::{Action, Gate, OpSet, PathMatch, Rule};
            let _ = at;
            self.paused = true;
            if !cluster.dead[n] {
                let gate = Gate::closed();
                let faulty = cluster.nodes[n].faulty.clone();
                let id = faulty.add_rule(Rule::new(
                    OpSet::ALL,
                    PathMatch::Any,
                    Action::Gate(gate.clone()),
                ));
                sim.net
                    .cuts
                    .lock()
                    .unwrap()
                    .push((NODES[n].to_string(), "*".into()));
                trace.push(format_args!("ctrl pause {}", NODES[n]));
                let net = sim.net.clone();
                let name = NODES[n];
                self.window(sim, len, format!("pause {name}"), move || {
                    faulty.remove_rule(id);
                    gate.open();
                    net.cuts
                        .lock()
                        .unwrap()
                        .retain(|(a, b)| !(a == name && b == "*"));
                });
            }
        }
    }

    /// Healing phase: every window ends.
    fn heal(&mut self, sim: &Shared) {
        for undo in self.undo.drain(..) {
            undo();
        }
        sim.net.cuts.lock().unwrap().clear();
    }
}

/// The first key (`prefix-i`) of `service` that falls into range `range`.
fn key_on(node: &RunningNode, service: &str, range: u32, prefix: &str) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| {
            node.cluster
                .partition_of(&TenantId::default(), service, k)
                .0
                == range
        })
        .expect("a key falls into the range")
}

/// (Virtual) limit of one request from the sim client: a request arriving just
/// as a node dies has its handler hang with that node's store, as a real client
/// sees a hung node.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// `POST url` (body `body`) over the in-process network. Returns the code and JSON.
async fn post(url: String, body: serde_json::Value) -> Result<(u16, serde_json::Value)> {
    let req = reqwest::Client::new()
        .post(url)
        .json(&body)
        .timeout(CLIENT_TIMEOUT)
        .build()?;
    let resp = LocalNet::call(req).await?;
    let code = resp.status().as_u16();
    Ok((code, resp.json().await.unwrap_or(serde_json::Value::Null)))
}

/// `GET url` over the in-process network. Returns the code and JSON.
async fn get(url: String) -> Result<(u16, serde_json::Value)> {
    let req = reqwest::Client::new()
        .get(url)
        .timeout(CLIENT_TIMEOUT)
        .build()?;
    let resp = LocalNet::call(req).await?;
    let code = resp.status().as_u16();
    Ok((code, resp.json().await.unwrap_or(serde_json::Value::Null)))
}

/// Send (`POST path`, a `/send` path) to node `first`, then to the other nodes
/// in turn until one acks. Every attempt goes into the trace. `429 throttled`
/// is an ack too: the invocation is durable, has an id, and is rescheduled.
async fn send(cluster: &mut Cluster, first: usize, path: &str, trace: &Trace) -> Result<String> {
    for attempt in 0..200usize {
        cluster.reap(trace);
        let i = (first + attempt) % cluster.nodes.len();
        let url = cluster.nodes[i].node.url();
        let got = post(format!("{url}{path}"), serde_json::json!("x")).await;
        match got {
            Ok((code, body))
                if code == 200 || code == 202 || (code == 429 && body["id"].is_string()) =>
            {
                trace.push(format_args!("load send {path} via {} -> {code}", NODES[i]));
                return body["id"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("ack without an id: {body}"));
            }
            Ok((code, _)) => {
                trace.push(format_args!("load send {path} via {} -> {code}", NODES[i]))
            }
            Err(e) => trace.push(format_args!("load send {path} via {} -> err {e}", NODES[i])),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("cannot send {path}"))
}

/// Write the rules of [`Flow`] through the first live node that answers, until
/// it works: the `rate` limit of tenant `default` and the `concurrency` limit of
/// scope [`FLAKY_SCOPE`].
async fn post_rules(cluster: &mut Cluster, flow: Flow, trace: &Trace) -> Result<()> {
    let rules = [
        serde_json::json!({ "tenant": "default", "rate": flow.rate, "burst": flow.burst }),
        serde_json::json!({ "pattern": FLAKY_SCOPE, "concurrency": flow.concurrency }),
    ];
    for rule in rules {
        let mut done = false;
        for attempt in 0..200usize {
            cluster.reap(trace);
            let Some(i) = (0..cluster.nodes.len())
                .map(|k| (attempt + k) % cluster.nodes.len())
                .find(|i| !cluster.dead[*i])
            else {
                break;
            };
            let got = post(
                format!("{}/rules", cluster.nodes[i].node.url()),
                rule.clone(),
            )
            .await;
            let code = got.as_ref().map_or(0, |(c, _)| *c);
            trace.push(format_args!("load rules {rule} via {} -> {code}", NODES[i]));
            if (200..300).contains(&code) {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(done, "cannot write rule {rule}");
    }
    Ok(())
}

/// The healing phase until all unfinished work must be done (virtual).
const CONVERGE: Duration = Duration::from_secs(90);

/// The cluster's unfinished work, each item with a reason (liveness mode): load
/// not done, and ranges no live node leads. Returns `Err` as soon as it sees a
/// safety violation (a wrong result).
async fn pending(
    cluster: &Cluster,
    ids: &[String],
    flaky: &[String],
    trace: &Trace,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for id in ids.iter().chain(flaky) {
        let is_flaky = flaky.contains(id);
        let mut seen = Vec::new();
        let mut done = false;
        for (i, n) in cluster.alive() {
            match get(format!("{}/invocations/{id}", n.node.url())).await {
                Ok((200, st)) if st["status"] == "completed" => {
                    // The load's invariant: Caller returns exactly Callee's result from
                    // another range; a recovered Flaky returns "ok".
                    let want = if is_flaky { "ok" } else { "pong" };
                    ensure!(st["output"] == want, "{id}: wrong result {st}");
                    done = true;
                    break;
                }
                // Flaky out of `maxAttempts`: a valid outcome for (a); (b) checks
                // whether it is real after convergence.
                Ok((200, st)) if is_flaky && st["status"] == "paused" => {
                    ensure!(st["code"] == 575, "{id}: paused but not 575: {st}");
                    done = true;
                    break;
                }
                Ok((200, st)) if st["status"] == "failed" || st["status"] == "paused" => {
                    return Err(anyhow!("{id}: outcome outside the scenario {st}"));
                }
                Ok((200, st)) => seen.push(format!("{}={}", NODES[i], st["status"])),
                Ok((code, _)) => seen.push(format!("{}={code}", NODES[i])),
                Err(_) => seen.push(format!("{}=error", NODES[i])),
            }
        }
        if !done {
            out.push(format!("invocation {id} not done ({})", seen.join(", ")));
        }
    }
    let Some((_, first)) = cluster.alive().next() else {
        out.push("no live node left".into());
        return Ok(out);
    };
    for r in first.node.cluster.table().ranges() {
        if r.intent.is_some() {
            out.push(format!("range {} still has intent {:?}", r.id.0, r.intent));
        }
        let mut led = false;
        for (_, n) in cluster.alive() {
            if let Some(h) = n.node.partitions.get(r.id) {
                if h.flow().await.is_ok() {
                    led = true;
                    break;
                }
            }
        }
        if !led {
            out.push(format!("range {} has no live leader yet", r.id.0));
        }
    }
    if !out.is_empty() {
        trace.push(format_args!("pending {}", out.len()));
    }
    Ok(out)
}

/// One whole run: build the cluster, run the load under faults (fault phase),
/// heal, run until no unfinished work is left (liveness phase), shut the
/// cluster down, then `verify` the bucket.
async fn run(rng: SimRng, trace: Trace, off: Off) -> Result<()> {
    let plan = Plan::draw(&rng, off);
    trace.push(format_args!("plan {plan:?}"));
    let net = Arc::new(SimNet {
        rng: rng.clone(),
        trace: trace.clone(),
        faults: plan.net,
        healed: Arc::default(),
        names: Mutex::default(),
        cuts: Mutex::default(),
    });
    let sim = Shared {
        rng: rng.clone(),
        trace: trace.clone(),
        healed: net.healed.clone(),
        net,
    };
    let mut cluster = Cluster {
        bucket: Arc::new(InMemory::new()),
        nodes: Vec::new(),
        dead: vec![false; NODES.len()],
        graveyard: Vec::new(),
        doomed: plan.slot.map(|(victim, _)| victim),
        spared: vec![false; NODES.len()],
    };
    if plan.quiet {
        // Like `cairn-ctl cluster enable-quiet-lease --apply`: every binary of
        // the sim knows the gate step.
        let store: Arc<dyn ObjectStore> = cluster.bucket.clone();
        let on = cairn_store::quiet_lease::QuietLease {
            on: true,
            knows: cairn_store::migrate::QUIET_LEASE,
            by: "sim".into(),
            at_ms: 0,
            delegation: true,
            ..Default::default()
        };
        cairn_store::quiet_lease::write(&store, &Path::default(), &on, None).await?;
    }
    for name in NODES {
        cluster
            .nodes
            .push(start_with_retries(&cluster.bucket, name, &sim, &plan).await?);
    }
    // Wait (virtually) for the cluster to balance leadership — otherwise the first
    // node up holds every range and there are no cross-node messages. Virtual time is cheap: it only costs events.
    tokio::time::sleep(cairn_server::balance::settle_for(TTL) + TTL * 8).await;
    cluster.reap(&trace);
    // Fake SDK: `Caller{i}` (range `i % P`) calls `Callee` in another range drawn
    // from the seed. One service per pair so each Caller knows its Callee key.
    let mut sdk = FakeSdk::builder().object("Callee", "run", vec![Step::output("pong")]);
    let mut callers = Vec::new();
    for i in 0..CALLS {
        let src = (i as u32) % u32::from(PARTITIONS);
        let dst = (src + 1 + rng.below(u64::from(PARTITIONS) - 1) as u32) % u32::from(PARTITIONS);
        let service = format!("Caller{i}");
        let callee = key_on(&cluster.nodes[0].node, "Callee", dst, &format!("callee{i}"));
        let caller = key_on(&cluster.nodes[0].node, &service, src, "caller");
        sdk = sdk.object(
            &service,
            "run",
            vec![
                Step::call("Callee", "run", Some(&callee)),
                Step::EchoCallResult,
            ],
        );
        callers.push((service, caller));
    }
    // `Flaky{j}`: an unkeyed service, called within the scope of the `concurrency` limit.
    for (j, fails) in plan.fails.iter().enumerate() {
        let service = format!("Flaky{j}");
        sdk = sdk
            .service(
                &service,
                "run",
                vec![
                    Step::Stall(plan.stall_ms),
                    Step::FailBelow(*fails),
                    Step::output("ok"),
                ],
            )
            .contract(
                &service,
                "run",
                cairn_server::discovery::Contract {
                    retry_policy_initial_interval: Some(plan.retry.initial_ms),
                    retry_policy_max_interval: Some(plan.retry.max_ms),
                    retry_policy_exponentiation_factor: Some(2.into()),
                    retry_policy_max_attempts: Some(plan.retry.max_attempts),
                    retry_policy_on_max_attempts: Some(
                        cairn_server::discovery::OnMaxAttempts::Pause,
                    ),
                    ..Default::default()
                },
            );
    }
    let sdk = sdk.spawn().await?;
    for i in 0..cluster.nodes.len() {
        register(&mut cluster, i, &sdk.url, &trace).await;
    }
    egress_denied_before_local_net(&mut cluster, &sdk.url, &trace).await?;
    if let Some(flow) = plan.flow {
        post_rules(&mut cluster, flow, &trace).await?;
        // The rule book reaches the node on its poll beat (2 s) — let it apply before sending the load.
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    // Fault phase: the kill is scheduled after every node has the deployment
    // (registering on a dead node never completes — the spike's seed 147).
    trace.push("load start");
    let t0 = tokio::time::Instant::now();
    let mut ctrl = Ctrl::default();
    let mut ids = Vec::new();
    for (service, key) in &callers {
        let first = rng.below(NODES.len() as u64) as usize;
        if let Some((victim, after)) = plan.kill {
            if t0.elapsed() >= Duration::from_millis(after) {
                cluster.kill_planned(victim, "plan", &trace);
            }
        }
        ctrl.tick(
            &plan.ctrl,
            &mut cluster,
            &sim,
            t0.elapsed().as_millis() as u64,
        )
        .await;
        ids.push(
            send(
                &mut cluster,
                first,
                &format!("/invoke/{service}/run/send?key={key}"),
                &trace,
            )
            .await?,
        );
    }
    let mut flaky = Vec::new();
    for j in 0..FLAKY {
        let first = rng.below(NODES.len() as u64) as usize;
        if let Some((victim, after)) = plan.kill {
            if t0.elapsed() >= Duration::from_millis(after) {
                cluster.kill_planned(victim, "plan", &trace);
            }
        }
        ctrl.tick(
            &plan.ctrl,
            &mut cluster,
            &sim,
            t0.elapsed().as_millis() as u64,
        )
        .await;
        let path = format!("/scope/{FLAKY_SCOPE}/invoke/Flaky{j}/run/send");
        flaky.push(send(&mut cluster, first, &path, &trace).await?);
    }
    while t0.elapsed() < Duration::from_millis(plan.chaos_ms) {
        if let Some((victim, after)) = plan.kill {
            if t0.elapsed() >= Duration::from_millis(after) {
                cluster.kill_planned(victim, "plan", &trace);
            }
        }
        ctrl.tick(
            &plan.ctrl,
            &mut cluster,
            &sim,
            t0.elapsed().as_millis() as u64,
        )
        .await;
        cluster.reap(&trace);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Healing phase: stop injecting store/network faults, remove the ambiguity
    // rule and the slot tripwire, revive the dead node if the plan says so.
    cluster.reap(&trace);
    ctrl.heal(&sim);
    sim.healed.store(true, Ordering::SeqCst);
    for (_, n) in cluster.alive() {
        n.faulty.clear_rules();
        if off.sabotage_heal {
            n.faulty.freeze();
        }
    }
    trace.push("heal");
    if plan.revive {
        for (i, name) in NODES.into_iter().enumerate() {
            if cluster.dead[i] {
                let fresh = start_with_retries(&cluster.bucket, name, &sim, &plan).await?;
                let old = std::mem::replace(&mut cluster.nodes[i], fresh);
                cluster.graveyard.push(old);
                cluster.dead[i] = false;
                trace.push(format_args!("node {name} revived"));
            }
        }
    }
    // Liveness phase: run until no unfinished work is left. Each unfinished item has a reason.
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let left = pending(&cluster, &ids, &flaky, &trace).await?;
        if left.is_empty() {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "did not converge within {CONVERGE:?} (virtual) after healing:\n  {}",
            left.join("\n  ")
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    trace.push("converged");
    // (b) real attempts — after the drain (a), before shutting the cluster down.
    let phantom = real_attempts(
        &cluster,
        &sdk,
        &flaky,
        plan.retry.max_attempts,
        off.sabotage_attempts,
        &trace,
    )
    .await;
    trace.push(format_args!("real-attempts phantom {}", phantom.len()));
    // A known suspicion, present in the product until the lifecycle core
    // landed: a host `Drive` turn (recover after a leader change, timer, rescue
    // net) throttled before `Running` still spent an attempt. The swarm never hit
    // it (300 seeds); a seed that goes red here should be compared with
    // `tests/lifecycle.rs::parked_first_turn_does_not_spend_attempt` before calling it a new bug.
    ensure!(
        phantom.is_empty(),
        "real attempts (b): attempts spent without the SDK being called:\n  {}",
        phantom.join("\n  ")
    );
    for (i, n) in cluster.nodes.into_iter().enumerate() {
        if cluster.dead[i] {
            // Dead node: drop it like `kill -9`, no clean shutdown (its store hangs).
            drop(n);
            continue;
        }
        n.node.shutdown().await;
        trace.push(format_args!("node {} down", NODES[i]));
    }
    drop(cluster.graveyard);
    // A full `cairn-ctl verify` on the bucket. A finding no loop heals is a
    // (safety) failure; a finding a background loop can heal that is still there
    // after the cluster converged and shut down cleanly is work nobody did
    // (liveness). Named known holes are exempt in `KNOWN_RESIDUE`.
    let rep = cairn_server::ctl::Ctl::new(ObjectStores::single(StoreLocation::from_store(
        cluster.bucket,
        "",
        "memory://",
    )))
    .verify(cairn_server::ctl::VerifyOptions::default())
    .await
    .map_err(|e| anyhow!("verify failed: {e}"))?;
    let show = |fs: &[&cairn_server::ctl::Finding]| {
        fs.iter()
            .map(|f| format!("{} range {:?}: {}", f.check, f.range, f.detail))
            .collect::<Vec<_>>()
            .join("; ")
    };
    let broken: Vec<_> = rep.findings.iter().filter(|f| f.heals.is_none()).collect();
    ensure!(broken.is_empty(), "verify: {}", show(&broken));
    let residue: Vec<_> = rep
        .findings
        .iter()
        .filter(|f| f.heals.is_some() && !KNOWN_RESIDUE.contains(&f.check))
        .collect();
    ensure!(
        residue.is_empty(),
        "verify after convergence: unfinished work a background loop should have healed: {}",
        show(&residue)
    );
    Ok(())
}

/// The **real attempts check (b)** on the sim: each `Flaky` `paused` 575 must
/// have an SDK that saw every `ctx.attempt` `0 … maxAttempts-1` (counting
/// distinct **values**, not turns: BIDI replays the same attempt). A missing
/// value is an attempt spent without the handler being called — the shape of an
/// earlier ghost-attempt bug and of a turn throttled before `Running` that
/// still spends an attempt. Returns one line per violation.
async fn real_attempts(
    cluster: &Cluster,
    sdk: &FakeSdk,
    flaky: &[String],
    max_attempts: u32,
    hide_first: bool,
    trace: &Trace,
) -> Vec<String> {
    let starts = sdk.starts();
    let mut out = Vec::new();
    for (j, id) in flaky.iter().enumerate() {
        let mut paused = false;
        for (_, n) in cluster.alive() {
            if let Ok((200, st)) = get(format!("{}/invocations/{id}", n.node.url())).await {
                paused = st["status"] == "paused" && st["code"] == 575;
                break;
            }
        }
        let seen: std::collections::BTreeSet<u32> = starts
            .iter()
            .filter(|s| s.id_hex() == *id && !(hide_first && s.attempt == 0))
            .map(|s| s.attempt)
            .collect();
        let outcome = if paused { "paused575" } else { "completed" };
        trace.push(format_args!("flaky {j} {outcome} attempts {seen:?}"));
        if !paused {
            continue;
        }
        let missing: Vec<u32> = (0..max_attempts).filter(|a| !seen.contains(a)).collect();
        if !missing.is_empty() {
            out.push(format!(
                "{id}: paused 575 after maxAttempts {max_attempts}, SDK saw attempts {seen:?}, missing {missing:?}"
            ));
        }
    }
    out
}

/// "A background loop can heal it" findings allowed to remain after
/// convergence — known holes, each with a name (only named known holes are
/// exempt).
const KNOWN_RESIDUE: &[&str] = &[];

/// The startup gate does not retry store errors (the binary exits, the
/// orchestrator restarts it) — the sim plays the orchestrator.
async fn start_with_retries(
    bucket: &Arc<InMemory>,
    name: &'static str,
    sim: &Shared,
    plan: &Plan,
) -> Result<SimNode> {
    let mut tries = 0;
    loop {
        match start_node(bucket, name, sim, plan).await {
            Ok(n) => return Ok(n),
            Err(e) if tries < 20 => {
                tries += 1;
                sim.trace.push(format_args!("node {name} start error: {e}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Register the deployment through node `i` until it works, or the node dies.
async fn register(cluster: &mut Cluster, i: usize, uri: &str, trace: &Trace) {
    loop {
        cluster.reap(trace);
        if cluster.dead[i] {
            return;
        }
        match post(
            format!("{}/deployments", cluster.nodes[i].node.url()),
            serde_json::json!({ "uri": uri }),
        )
        .await
        {
            Ok((code, _)) if (200..300).contains(&code) => {
                trace.push(format_args!("load register via {}", NODES[i]));
                return;
            }
            got => {
                let code = got
                    .map(|(c, _)| c.to_string())
                    .unwrap_or_else(|_| "error".into());
                trace.push(format_args!("load register via {} -> {code}", NODES[i]));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// The URL layer of the egress policy runs under the sim: every node denies
/// private addresses and allows `127.0.0.1/32` (the sim network's address, see
/// `start_node`). Registering at `10.0.0.1:<SDK port>` must give `400
/// egress_denied` — blocked **before** the `LocalNet::route` branch; without
/// this layer the sim network returns the network error of an unregistered
/// address (`400` with another code).
async fn egress_denied_before_local_net(
    cluster: &mut Cluster,
    sdk_url: &str,
    trace: &Trace,
) -> Result<()> {
    let port = sdk_url.rsplit(':').next().unwrap_or("80");
    let uri = format!("http://10.0.0.1:{port}");
    for attempt in 0..200usize {
        cluster.reap(trace);
        let i = attempt % cluster.nodes.len();
        if cluster.dead[i] {
            continue;
        }
        let got = post(
            format!("{}/deployments", cluster.nodes[i].node.url()),
            serde_json::json!({ "uri": uri }),
        )
        .await;
        match got {
            Ok((400, body)) => {
                ensure!(
                    body["code"] == cairn_server::egress::EGRESS_DENIED,
                    "internal destination via {}: must be {}, got {body}",
                    NODES[i],
                    cairn_server::egress::EGRESS_DENIED
                );
                trace.push(format_args!("egress denied via {}", NODES[i]));
                return Ok(());
            }
            got => {
                let code = got
                    .map(|(c, _)| c.to_string())
                    .unwrap_or_else(|_| "error".into());
                trace.push(format_args!("egress probe via {} -> {code}", NODES[i]));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(anyhow!("no node answered the egress probe"))
}

/// The **virtual** time limit of one run.
const SIM_DEADLINE: Duration = Duration::from_secs(300);

/// One sim run at a time in the process. The sim uses some **process-wide**
/// state — the metering book (`reset_for_sim` clears it at the start), `static`
/// metric counters — so two runs in parallel (this file's `#[test]`s under
/// `cargo test` run on several threads) flush each other's numbers and diverge
/// in their traces: the `same_seed_same_trace` criterion run with the whole file
/// went red on seeds 1–2 (a `metering/…` PUT swapping places with a
/// `sessions/…` PUT), while run alone it was 50/50. Nextest runs each test in
/// its own process, so it does not touch this lock.
static ONE_SIM: Mutex<()> = Mutex::new(());

/// Virtual time stands still for this much **real** time ⇒ the sim is stuck.
const STALL: Duration = Duration::from_secs(60);

/// **The real-time limit** of one run (seen with seeds 44/45/47 of one swarm
/// run).
///
/// Every limit of the sim — [`SIM_DEADLINE`], [`CONVERGE`], each step's
/// `timeout` — is **virtual** time: the `start_paused` runtime only advances
/// time when every task is waiting. A hot loop (a task waking on a mark already
/// in the past, doing a store operation that fails at once, waking again) never
/// lets the runtime go idle, so virtual time stands still and **no limit
/// fires**: the run hangs at 100% CPU until killed from outside, without a
/// single line.
///
/// A watchdog thread (real time, outside the runtime) reads the virtual ms of
/// the latest event in the trace — it adds no task to the runtime, so it does
/// not shuffle the schedule (every seed's trace stays the same). If that mark
/// does not move for [`STALL`] of real time — a hot loop at the same virtual ms,
/// or a deadlock — ⇒ print the reason with the last 20 events and exit the
/// process (a spinning thread cannot be interrupted): the run goes red with a
/// reason instead of hanging.
struct Watchdog {
    done: Arc<AtomicBool>,
}

impl Watchdog {
    #[allow(
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "the sim's real-time limit — stands outside virtual time, not in the trace"
    )]
    fn start(seed: u64, trace: Trace) -> Watchdog {
        let done = Arc::new(AtomicBool::new(false));
        let tick = trace.last_ms.clone();
        {
            let done = done.clone();
            std::thread::spawn(move || {
                let mut last = (tick.load(Ordering::SeqCst), std::time::Instant::now());
                while !done.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(500));
                    let now = tick.load(Ordering::SeqCst);
                    if now != last.0 {
                        last = (now, std::time::Instant::now());
                    } else if last.1.elapsed() >= STALL && !done.load(Ordering::SeqCst) {
                        let lines = trace.lines();
                        let tail = lines[lines.len().saturating_sub(20)..].join("\n");
                        eprintln!(
                            "seed {seed}: SIM STUCK — virtual time stands at ~{} ms for {STALL:?} of real time \
                             (a hot loop that never yields time: every sim limit is virtual)\n— last 20 events:\n{tail}",
                            now
                        );
                        std::process::exit(101);
                    }
                }
            });
        }
        Watchdog { done }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
    }
}

/// Build the sim's runtime and run one seed with the `off` features switched off.
fn simulate(seed: u64, off: Off) -> Result<Trace> {
    // A test going red with `panic!` while holding the lock poisons it; the next
    // run still works — the protected state (metering) clears itself at the start.
    let _one = ONE_SIM.lock().unwrap_or_else(|e| e.into_inner());
    let mut rt = tokio::runtime::Builder::new_current_thread();
    rt.enable_all().start_paused(true);
    // A non-`biased` `select!` picks its branch with the runtime's RNG — which can
    // only be seeded with `--cfg tokio_unstable` (see `docs/testing.md`).
    #[cfg(tokio_unstable)]
    {
        let rt_seed = std::env::var("CAIRN_SIM_RT_SEED")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(seed);
        rt.rng_seed(tokio::runtime::RngSeed::from_bytes(&rt_seed.to_le_bytes()));
    }
    let rt = rt.build()?;
    let local = tokio::task::LocalSet::new();
    // The metering book is process-wide `static`: an earlier run must not leak
    // into the next one.
    cairn_server::metering::reset_for_sim();
    local.block_on(&rt, async {
        let rng = SimRng::new(seed);
        let _net = LocalNet::install();
        let _entropy = cairn_store::entropy::install(std::rc::Rc::new(SimEntropy {
            rng: rng.clone(),
            t0: tokio::time::Instant::now(),
        }));
        let trace = Trace::new();
        let _watchdog = Watchdog::start(seed, trace.clone());
        let res = match tokio::time::timeout(SIM_DEADLINE, run(rng, trace.clone(), off)).await {
            Ok(r) => r,
            Err(_) => Err(anyhow!("over {SIM_DEADLINE:?} virtual")),
        };
        match res {
            Ok(()) => Ok(trace),
            Err(e) => {
                dump(seed, "fail", &trace);
                let lines = trace.lines();
                let tail = lines[lines.len().saturating_sub(20)..].join("\n");
                Err(anyhow!("seed {seed}: {e}\n— last 20 events:\n{tail}"))
            }
        }
    })
}

fn dump(seed: u64, tag: &str, trace: &Trace) {
    if let Ok(dir) = std::env::var("CAIRN_SIM_TRACE_DIR") {
        let path = std::path::Path::new(&dir).join(format!("seed-{seed}-{tag}.trace"));
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(&path, trace.lines().join("\n"));
    }
}

fn first_divergence(a: &[String], b: &[String]) -> String {
    let i = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    format!(
        "diverges at event #{i} (lengths {} / {}):\n  a: {}\n  b: {}",
        a.len(),
        b.len(),
        a.get(i).map_or("<end>", String::as_str),
        b.get(i).map_or("<end>", String::as_str),
    )
}

/// Shrink a red seed **along the swarm dimensions**: switch off each enabled
/// feature in turn, keeping the switch-off if it is still red. Returns the
/// smallest switched-off set found and a `#[test]` snippet reproducing it.
///
/// Without `tokio_unstable` a run is not deterministic: a green run after a
/// switch-off only means "not seen red this time", and the `#[test]` snippet may
/// not reproduce — rerun with a `tokio_unstable` target (doc of
/// [`same_seed_same_trace`]).
fn shrink(seed: u64, base: Off) -> (Off, String) {
    let mut off = base;
    let rng = SimRng::new(seed);
    let plan = Plan::draw(&rng, off);
    for (name, knock) in Off::FEATURES {
        if !plan.enabled(name) {
            continue;
        }
        let mut tried = off;
        knock(&mut tried);
        if simulate(seed, tried).is_err() {
            off = tried;
        }
    }
    let snippet = format!(
        "#[test]\nfn seed_{seed}() -> Result<()> {{\n    simulate({seed}, {off:?}).map(|_| ())\n}}"
    );
    (off, snippet)
}

/// One line for a green run: the plan, and the faults that really happened.
fn summary(seed: u64, t: &Trace) -> String {
    let lines = t.lines();
    let count = |pat: &str| lines.iter().filter(|l| l.contains(pat)).count();
    let at = |pat: &str| {
        lines
            .iter()
            .find(|l| l.contains(pat))
            .and_then(|l| l.split_whitespace().next())
            .unwrap_or("-")
            .to_string()
    };
    format!(
        "seed {seed}: {} events; killed {} (slot {}), revived {}, put/delete errors {}, store faults {}, dropped messages {}; rules {}, throttled sends {}, flaky paused 575 {}/{FLAKY}, ghost attempts {}; healed at {} ms, converged at {} ms",
        lines.len(),
        count("killed"),
        count("SST gap"),
        count("revived"),
        count("put-err") + count("delete-err"),
        lines.iter().filter(|l| l.ends_with(" fail")).count(),
        count("Drop"),
        count("load rules"),
        count("-> 429"),
        count("paused575 attempts"),
        lines
            .iter()
            .find_map(|l| l.split("real-attempts phantom ").nth(1))
            .unwrap_or("-"),
        at(" heal"),
        at(" converged"),
    )
}

/// One seed run to the end (build the cluster, cross-range load under faults,
/// heal, converge, `verify`) — runs in every build.
#[test]
fn one_seed_runs_to_completion() -> Result<()> {
    let seed = std::env::var("CAIRN_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    simulate(seed, Off::default()).map(|_| ())
}

/// Swarm: `CAIRN_SIM_SEEDS` seeds (default 4) from `CAIRN_SIM_SEED` (default
/// 1), each seed a combination of features. A red seed is shrunk along the
/// swarm dimensions and printed as a reproducing `#[test]`. Nightly runs 1 000
/// seeds with `tokio_unstable` (`nightly.yml`).
#[test]
fn swarm() -> Result<()> {
    let env = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<u64>().ok());
    let base = env("CAIRN_SIM_SEED").unwrap_or(1);
    let mut failed = Vec::new();
    for seed in base..base + env("CAIRN_SIM_SEEDS").unwrap_or(4) {
        // Report only (not in the trace — the counters are process-wide, and
        // other tests running in parallel under `cargo test` add to them too):
        // turns throttled and blocked at the `concurrency` limit in this run.
        use cairn_server::metrics::flow::{BLOCKED_CONCURRENCY, THROTTLED};
        let (t0, b0) = (
            THROTTLED.load(Ordering::Relaxed),
            BLOCKED_CONCURRENCY.load(Ordering::Relaxed),
        );
        match simulate(seed, Off::default()) {
            Ok(t) => eprintln!(
                "{}; throttled turns ~{}, blocked at concurrency ~{}",
                summary(seed, &t),
                THROTTLED.load(Ordering::Relaxed) - t0,
                BLOCKED_CONCURRENCY.load(Ordering::Relaxed) - b0
            ),
            Err(e) => {
                let (_, snippet) = shrink(seed, Off::default());
                eprintln!("{e}\n— shrunk along the swarm dimensions:\n{snippet}");
                failed.push(seed);
            }
        }
    }
    ensure!(failed.is_empty(), "red seeds: {failed:?}");
    Ok(())
}

/// Acceptance of the liveness phase: a cluster that cannot converge (the
/// healing phase freezes every node's store) makes the run red with a list of
/// unfinished work with reasons, not a silent green.
#[test]
fn liveness_phase_catches_non_converging_cluster() {
    let off = Off {
        sabotage_heal: true,
        ..Off::default()
    };
    let Err(err) = simulate(5, off) else {
        panic!("frozen cluster, yet green");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("did not converge"), "{msg}");
    assert!(msg.contains("has no live leader yet"), "{msg}");
}

/// Acceptance of the real attempts check (b): an attempt the SDK did not see
/// (hidden sabotage: attempt 0 is hidden from the SDK's trace) makes the run
/// red, with the id and the missing attempt. Seed 4: 3/4 `Flaky` run out of
/// `maxAttempts`.
#[test]
fn real_attempt_count_catches_ghost_attempts() {
    let off = Off {
        sabotage_attempts: true,
        ..Off::default()
    };
    let Err(err) = simulate(4, off) else {
        panic!("an attempt was hidden, yet (b) is green");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("real attempts (b)"), "{msg}");
    assert!(msg.contains("missing [0]"), "{msg}");
}

/// Acceptance of shrinking: a red run that depends on no feature (hidden
/// sabotage in the healing phase) gets every enabled feature of its plan
/// switched off.
#[test]
fn shrinking_disables_every_unneeded_feature() {
    let base = Off {
        sabotage_heal: true,
        ..Off::default()
    };
    let (off, snippet) = shrink(5, base);
    let plan = Plan::draw(&SimRng::new(5), Off::default());
    for (name, knock) in Off::FEATURES {
        let mut want = off;
        knock(&mut want);
        assert!(
            !plan.enabled(name) || want == off,
            "feature {name} is not needed for the red run but was not switched off:\n{snippet}"
        );
    }
    assert!(snippet.contains("fn seed_5()"), "{snippet}");
}

/// The determinism criterion: the same seed run twice gives the same trace; a
/// different seed gives a different trace.
///
/// Needs `--cfg tokio_unstable`: a non-`biased` `select!` picks its branch with
/// the runtime's RNG, and only `Builder::rng_seed` (an unstable API) can seed
/// it. Without that flag about 9 in 10 seeds diverge. Command (a separate
/// target so the normal build is not dirtied):
///
/// ```sh
/// CARGO_TARGET_DIR=target/sim-unstable RUSTFLAGS="--cfg tokio_unstable" \
///   cargo test -p cairn-server --features testkit --test sim_cluster
/// ```
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs --cfg tokio_unstable to seed select!'s RNG — see the test's doc"
)]
fn same_seed_same_trace() -> Result<()> {
    let env = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<u64>().ok());
    let base = env("CAIRN_SIM_SEED").unwrap_or(1);
    for seed in base..base + env("CAIRN_SIM_SEEDS").unwrap_or(1) {
        one_seed(seed)?;
    }
    Ok(())
}

fn one_seed(seed: u64) -> Result<()> {
    let a = simulate(seed, Off::default())?;
    dump(seed, "a", &a);
    let b = simulate(seed, Off::default())?;
    dump(seed, "b", &b);
    let c = simulate(seed + 1, Off::default())?;
    dump(seed + 1, "c", &c);
    eprintln!(
        "seed {seed}: {} events, hash a={:016x} b={:016x}; seed {}: hash {:016x}",
        a.lines().len(),
        a.hash(),
        b.hash(),
        seed + 1,
        c.hash()
    );
    ensure!(
        a.hash() == b.hash(),
        "same seed {seed} but different traces — {}",
        first_divergence(&a.lines(), &b.lines())
    );
    ensure!(
        a.hash() != c.hash(),
        "seeds {seed} and {} give the same trace",
        seed + 1
    );
    Ok(())
}
