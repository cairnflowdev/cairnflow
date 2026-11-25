// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A deterministic (seeded) simulation of the per-node shared WAL.
//!
//! One bucket, up to three node "processes" (each with its own `FaultyStore`
//! wrapping the shared bucket, a `Session`, a `NodeWals`), four ranges. Each
//! round, by seed: install a [`Tripwire`] on a node at one **protocol step**
//! (PUT of a log object, PUT of a seal/floor, owner-record CAS, PUT of a
//! manifest/SST, a GC DELETE, a GET while sealing, a session renewal) with a
//! death kind `Before` (dies before the request leaves), `After` (the request
//! reaches the store, then it dies) or `Torn` (the request reaches the store but
//! the client gets an error — and lives on); take ranges that have no owner
//! (seal + replay), write, hand over cooperatively, wait for GC/forced flushes.
//! A dead node restarts (a new incarnation) in the next round. Part of the
//! writes are **bursts**: several versions of the same key back to back,
//! sometimes flushing the memtable before they are durable (G25).
//!
//! Invariants checked after every range take-over and at the end ("0 lost, 0
//! wrong"):
//!
//! - the value read for each key is the **last acked value** or a value written
//!   **after** it that was not acked (unknown outcome) — never a value older
//!   than the last ack, never a value that was never written;
//! - after each take-over, the value read becomes the new mark (it is down in
//!   L0): a later take-over must not go back before it.
//!
//! The seed fixes the schedule (node choice, step, death kind, number of
//! requests skipped, keys, values); the interleaving of background tasks still
//! follows real time, so a seed reproduces the **scenario**, not every request.
//! A failure prints the seed and the scenario log.
//!
//! `CAIRN_SIM_SEED=<n>` runs exactly one seed; `CAIRN_SIM_SEEDS=<k>` runs k seeds
//! from `CAIRN_SIM_BASE` (1 by default); `CAIRN_SIM_ROUNDS` rounds per seed.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{anyhow, bail, Context, Result},
    cairn_store::{
        keys,
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule, TripAt, Tripwire},
        Claim, LeaseConfig, LeaseKeeper, Mutations, NodeWalConfig, NodeWals, ObjectStores,
        PartitionId, PartitionWriter, Session, StoreLocation,
    },
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
    std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration},
};

const TTL: Duration = Duration::from_millis(400);
const PACE: Duration = Duration::from_millis(50);
const RANGES: u32 = 4;
const KEYS: u8 = 4;
const SLOTS: usize = 3;

// ───────────────────────────────────────────────────────── seeds

/// splitmix64 — enough for a simulation schedule, no external crate.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[self.below(xs.len() as u64) as usize]
    }
}

// ───────────────────────────────────────────────────────── protocol steps

/// The protocol step a tripwire targets.
#[derive(Clone, Copy, Debug)]
enum Point {
    /// PUT of a node-log object (data, seal, floor).
    LogPut,
    /// PUT of a GC floor object.
    FloorPut,
    /// GC DELETE of a log object.
    LogDelete,
    /// GET of a log object (seal + replay on a take-over, the tailer).
    LogGet,
    /// Owner-record CAS (take-over, adding/dropping segments, clean seal, release).
    OwnerPut,
    /// PUT of a Db manifest (opening a Db, memtable flush).
    ManifestPut,
    /// PUT of an SST (memtable flush, compaction).
    SstPut,
    /// Session renewal.
    SessionPut,
}

impl Point {
    const ALL: [Point; 8] = [
        Point::LogPut,
        Point::FloorPut,
        Point::LogDelete,
        Point::LogGet,
        Point::OwnerPut,
        Point::ManifestPut,
        Point::SstPut,
        Point::SessionPut,
    ];

    fn rule(self, wire: Tripwire) -> Rule {
        let (ops, path) = match self {
            Point::LogPut => (OpSet::PUT, "nodewal/"),
            Point::FloorPut => (OpSet::PUT, "/floor/"),
            Point::LogDelete => (OpSet::DELETE, "nodewal/"),
            Point::LogGet => (OpSet::GET, "nodewal/"),
            Point::OwnerPut => (OpSet::PUT, "owners/"),
            Point::ManifestPut => (OpSet::PUT, "/manifest/"),
            Point::SstPut => (OpSet::PUT, "/compacted/"),
            Point::SessionPut => (OpSet::PUT, "sessions/"),
        };
        Rule::new(ops, PathMatch::Contains(path.into()), Action::Trip(wire))
    }

    /// `Torn` makes sense only for writes.
    fn kinds(self) -> &'static [TripAt] {
        match self {
            Point::LogGet => &[TripAt::Before],
            _ => &[TripAt::Before, TripAt::After, TripAt::Torn],
        }
    }
}

// ───────────────────────────────────────────────────────── node

struct Node {
    name: String,
    faulty: Arc<FaultyStore>,
    stores: ObjectStores,
    session: Session,
    /// The incarnation at start-up. Once the session ends it (a renewal later
    /// than the TTL under load, a fail-stop after a tripped write), its shared
    /// WAL stops and every range it holds returns `Unavailable` — the node is
    /// dead, as when the store freezes.
    inc: Option<cairn_store::SessionId>,
    wals: NodeWals,
}

impl Node {
    async fn start(bucket: &Arc<InMemory>, name: &str, cfg: &NodeWalConfig) -> Result<Node> {
        let faulty = FaultyStore::wrap(bucket.clone());
        let stores = ObjectStores::single(StoreLocation::from_store(
            faulty.clone(),
            "cairn",
            "memory://cairn",
        ));
        // Default poll intervals (`DEFAULT_POLLS`, compactor 300 s). Before the
        // fork's follow-up patch to F2, a Db reopened with L0 at the cap behind
        // an orphan compaction job stalled until that poll (seed 33); this test
        // used to lower the poll to 1 s to dodge it. Its own regression test:
        // `store.rs::reopen_at_l0_cap_with_orphan_compaction_job_does_not_wait_for_poll`.
        let session = Session::establish(
            faulty.clone() as Arc<dyn ObjectStore>,
            &Path::from("cairn"),
            LeaseConfig::new()
                .with_node_id(name)
                .with_ttl(TTL)
                .with_renew_interval(PACE),
        )
        .await?;
        let wals = NodeWals::follow(&stores, &session, cfg.clone());
        let inc = session.current();
        Ok(Node {
            name: name.to_string(),
            faulty,
            stores,
            session,
            inc,
            wals,
        })
    }

    fn dead(&self) -> bool {
        self.faulty.is_frozen() || self.session.current() != self.inc
    }
}

/// A range currently held by a node.
struct Held {
    slot: usize,
    writer: PartitionWriter,
    keeper: LeaseKeeper,
}

// ───────────────────────────────────────────────────────── model

#[derive(Default, Clone)]
struct KeyModel {
    /// The last value acked (or read after a take-over).
    acked: Option<Vec<u8>>,
    /// Values written after `acked` with an unknown outcome.
    pending: Vec<Vec<u8>>,
}

impl KeyModel {
    fn allows(&self, got: &Option<Vec<u8>>) -> bool {
        match got {
            None => self.acked.is_none(),
            Some(v) => self.acked.as_ref() == Some(v) || self.pending.contains(v),
        }
    }
}

fn id(range: u32, key: u8) -> [u8; 24] {
    let mut id = [0u8; 24];
    id[22] = range as u8;
    id[23] = key;
    id
}

async fn within<T>(d: Duration, f: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(d, f)
        .await
        .map_err(|_| anyhow!("over {d:?}"))?
}

struct Sim {
    seed: u64,
    rng: Rng,
    bucket: Arc<InMemory>,
    cfg: NodeWalConfig,
    nodes: Vec<Option<Node>>,
    restarts: usize,
    held: BTreeMap<u32, Held>,
    model: BTreeMap<(u32, u8), KeyModel>,
    counter: u64,
    log: Vec<String>,
    /// Every tripwire installed — to count which steps were really "killed".
    wires: Vec<(Point, TripAt, Tripwire)>,
    /// The step the take-over in progress has reached — so the log can say where
    /// a timed-out take-over stood.
    progress: Arc<std::sync::Mutex<String>>,
}

impl Sim {
    fn note(&mut self, s: String) {
        self.log.push(s);
    }

    fn live(&self) -> Vec<usize> {
        (0..SLOTS)
            .filter(|i| self.nodes[*i].as_ref().is_some_and(|n| !n.dead()))
            .collect()
    }

    /// Drop ranges whose holding node is dead or whose grant is lost; drop dead nodes for good.
    fn reap(&mut self) {
        let stale: Vec<u32> = self
            .held
            .iter()
            .filter(|(_, h)| {
                !h.keeper.is_valid() || self.nodes[h.slot].as_ref().is_none_or(|n| n.dead())
            })
            .map(|(r, _)| *r)
            .collect();
        for r in stale {
            let h = self.held.remove(&r).expect("just seen");
            self.note(format!("range {r}: dropped (node {} lost/died)", h.slot));
            // Drop: a live node releases dirty (keeps its segments), a dead node writes nothing.
            drop(h);
        }
        for i in 0..SLOTS {
            if self.nodes[i].as_ref().is_some_and(|n| n.dead()) {
                let n = self.nodes[i].take().expect("just seen");
                self.note(format!("node {} died", n.name));
                n.session.abort();
            }
        }
    }

    async fn restart_some(&mut self) -> Result<()> {
        for i in 0..SLOTS {
            if self.nodes[i].is_some() {
                continue;
            }
            if self.live().len() >= 2 && !self.rng.chance(50) {
                continue;
            }
            self.restarts += 1;
            let name = format!("n{i}");
            self.nodes[i] = Some(Node::start(&self.bucket, &name, &self.cfg).await?);
            self.note(format!("node {name} started"));
        }
        Ok(())
    }

    fn arm(&mut self) {
        let live = self.live();
        if live.is_empty() || !self.rng.chance(70) {
            return;
        }
        let slot = self.rng.pick(&live);
        let point = self.rng.pick(&Point::ALL);
        let at = self.rng.pick(point.kinds());
        let skip = self.rng.below(8);
        let wire = Tripwire::new(skip, at);
        self.wires.push((point, at, wire.clone()));
        let node = self.nodes[slot].as_ref().expect("alive");
        node.faulty.add_rule(point.rule(wire));
        let name = node.name.clone();
        self.note(format!(
            "trap on node {name}: {point:?} {at:?} after {skip} requests"
        ));
    }

    /// Take a range on node `slot`: claim until it wins, seal + replay.
    async fn acquire(&mut self, slot: usize, range: u32) -> Result<()> {
        let node = self.nodes[slot].as_ref().expect("alive");
        let progress = Arc::clone(&self.progress);
        let step = |s: String| *progress.lock().unwrap() = s;
        step("claim".into());
        let (grant, mut keeper) = loop {
            match node
                .session
                .claim(PartitionId(range))
                .await
                .context("claim")?
            {
                Claim::Won(g, k) => break (g, k),
                Claim::Held(h) => {
                    step(format!("claim: range held by {h:?}"));
                    tokio::time::sleep(PACE).await
                }
            }
        };
        step(format!("acquire_shared, record {:?}", keeper.wal()));
        let wal = node.wals.get(keeper.session()).context("NodeWals::get")?;
        let hint = node
            .stores
            .adopt_shared_wal(range)
            .await
            .context("adopt_shared_wal")?;
        let db =
            cairn_store::acquire_shared_with_hint(&node.stores, &wal, &mut keeper, range, hint)
                .await
                .context("acquire_shared")?;
        let writer = PartitionWriter::open(grant, db)
            .await
            .context("PartitionWriter::open")?;
        self.held.insert(
            range,
            Held {
                slot,
                writer,
                keeper,
            },
        );
        Ok(())
    }

    /// Read every key of the range just taken and check it against the model; the
    /// values read become the new mark.
    async fn verify_after_takeover(&mut self, range: u32) -> Result<()> {
        for key in 0..KEYS {
            let slot = self.held[&range].slot;
            let read = {
                let h = &self.held[&range];
                within(Duration::from_secs(5), async {
                    Ok(h.writer
                        .get(&keys::invocation_status(&id(range, key)))
                        .await?)
                })
                .await
            };
            let got = match read {
                Ok(v) => v.map(|b| b.to_vec()),
                // The node's tripwire fires **during** the check (e.g. a background
                // manifest PUT of that same node): the store freezes like
                // `kill -9`, so a hanging read is correct — the node is dead, the
                // system is not stuck. The range stays in `held` so the next round
                // drops it like every range of a dead node; the marks of keys not
                // yet read stay as they were.
                Err(e) if self.nodes[slot].as_ref().is_some_and(|n| n.dead()) => {
                    self.note(format!(
                        "range {range}: the node died while checking after the take-over ({e:#})"
                    ));
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            let m = self.model.entry((range, key)).or_default();
            if !m.allows(&got) {
                bail!(
                    "range {range} key {key}: read {:?}, model acked {:?} pending {:?}",
                    got.as_deref().map(String::from_utf8_lossy),
                    m.acked.as_deref().map(String::from_utf8_lossy),
                    m.pending
                        .iter()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .collect::<Vec<_>>()
                );
            }
            m.acked = got;
            m.pending.clear();
        }
        Ok(())
    }

    async fn acquire_unowned(&mut self) -> Result<()> {
        for range in 0..RANGES {
            if self.held.contains_key(&range) {
                continue;
            }
            let live = self.live();
            if live.is_empty() {
                return Ok(());
            }
            let slot = self.rng.pick(&live);
            let name = self.nodes[slot].as_ref().expect("alive").name.clone();
            // A dead old owner needs ~TTL for its session to be considered dead.
            match within(TTL * 8, self.acquire(slot, range)).await {
                Ok(()) => {
                    let segs = format!("{:?}", self.held[&range].keeper.wal());
                    self.note(format!("range {range}: node {name} takes it — {segs}"));
                    self.verify_after_takeover(range).await?;
                }
                Err(e) => {
                    let mut at = self.progress.lock().unwrap().clone();
                    if std::env::var("CAIRN_SIM_TRACE").is_ok() {
                        use futures::TryStreamExt;
                        let listed: Vec<_> = self
                            .bucket
                            .list(Some(&Path::from("cairn/nodewal")))
                            .try_collect()
                            .await
                            .unwrap_or_default();
                        let names: Vec<String> =
                            listed.iter().map(|m| m.location.to_string()).collect();
                        at.push_str(&format!(" — nodewal: {names:?}"));
                        let l0 = self.nodes[slot].as_ref().map(|n| n.stores.clone());
                        if let Some(st) = l0 {
                            let v = st.range_l0_views(PartitionId(range)).await;
                            at.push_str(&format!(" — L0 views {v:?} / cap {}", st.l0_max_ssts()));
                        }
                    }
                    self.note(format!(
                        "range {range}: node {name} could not take it ({e:#}) — reached step: {at}"
                    ))
                }
            }
        }
        Ok(())
    }

    async fn writes(&mut self, n: usize) {
        for _ in 0..n {
            let held: Vec<u32> = self.held.keys().copied().collect();
            if held.is_empty() {
                return;
            }
            let range = self.rng.pick(&held);
            let key = self.rng.below(KEYS as u64) as u8;
            // A burst (G25, SlateDB #2208): several versions of the same key in one
            // memtable without waiting for durability between them, sometimes
            // flushing the memtable before they are durable — L0 keeps only the
            // last version, and a later take-over must not go back to an old
            // version or lose the key.
            let burst = if self.rng.chance(30) {
                2 + self.rng.below(3) as usize
            } else {
                1
            };
            let flush = burst > 1 && self.rng.chance(50);
            let values: Vec<Vec<u8>> = (0..burst)
                .map(|_| {
                    self.counter += 1;
                    format!("s{}-r{range}-k{key}-{}", self.seed, self.counter).into_bytes()
                })
                .collect();
            self.model
                .entry((range, key))
                .or_default()
                .pending
                .extend(values.iter().cloned());
            let h = self.held.get_mut(&range).expect("just picked");
            let res = within(Duration::from_secs(3), async {
                for value in &values {
                    let mut m = Mutations::new();
                    m.set_invocation_status(&id(range, key), value);
                    h.writer.commit(m).await?;
                }
                if flush {
                    h.writer.flush_memtable().await?;
                }
                h.writer.wait_durable().await?;
                Ok(())
            })
            .await;
            let value = values.last().expect("at least one version").clone();
            match res {
                Ok(()) => {
                    if std::env::var("CAIRN_SIM_TRACE").is_ok() {
                        let slot = self.held[&range].slot;
                        self.note(format!(
                            "range {range} key {key}: ack {} (node {slot}, burst {burst}, flush {flush})",
                            String::from_utf8_lossy(&value)
                        ));
                    }
                    let m = self.model.get_mut(&(range, key)).expect("just added");
                    m.acked = Some(value);
                    m.pending.clear();
                }
                Err(e) => {
                    self.note(format!("range {range} key {key}: write unknown ({e})"));
                }
            }
        }
    }

    async fn maybe_handoff(&mut self) {
        if self.held.is_empty() || !self.rng.chance(35) {
            return;
        }
        let held: Vec<u32> = self.held.keys().copied().collect();
        let range = self.rng.pick(&held);
        let Held {
            writer, mut keeper, ..
        } = self.held.remove(&range).expect("just picked");
        let sealed = within(Duration::from_secs(5), async { Ok(writer.seal().await?) }).await;
        let clean = match &sealed {
            Ok(()) => within(Duration::from_secs(3), async {
                Ok(keeper.writer_sealed().await?)
            })
            .await
            .is_ok(),
            Err(_) => false,
        };
        let _ = within(Duration::from_secs(3), async {
            keeper.release().await;
            Ok(())
        })
        .await;
        self.note(format!(
            "range {range}: released (seal {}, clean {clean})",
            sealed.is_ok()
        ));
    }

    async fn round(&mut self, i: usize) -> Result<()> {
        self.note(format!("── round {i}"));
        self.reap();
        self.restart_some().await?;
        self.arm();
        self.acquire_unowned().await?;
        self.reap();
        let n = 4 + self.rng.below(8) as usize;
        self.writes(n).await;
        self.maybe_handoff().await;
        // Let GC merge, the frame-age forced flush and renewals run.
        tokio::time::sleep(self.cfg.frame_age_flush + self.cfg.gc_interval * 3).await;
        self.reap();
        Ok(())
    }

    /// The end: remove every trap, enough live nodes, every range owned and read correctly.
    async fn settle(&mut self) -> Result<()> {
        self.note("── end".into());
        self.reap();
        for i in 0..SLOTS {
            if let Some(n) = &self.nodes[i] {
                n.faulty.clear_rules();
            }
        }
        // After a fail-stop a range can be taken only after a few TTLs: be patient.
        for _ in 0..8 {
            self.reap();
            if self.live().len() < 2 {
                for i in 0..SLOTS {
                    if self.nodes[i].is_none() {
                        self.nodes[i] =
                            Some(Node::start(&self.bucket, &format!("n{i}"), &self.cfg).await?);
                        self.note(format!("node n{i} started (end)"));
                    }
                }
            }
            self.acquire_unowned().await?;
            if self.held.len() == RANGES as usize {
                break;
            }
        }
        if self.held.len() != RANGES as usize {
            bail!(
                "at the end of the simulation some ranges could not be taken: {:?}",
                self.held.keys().collect::<Vec<_>>()
            );
        }
        // Read back through the holding writer: 0 lost, 0 wrong.
        for range in 0..RANGES {
            for key in 0..KEYS {
                let got = self.held[&range]
                    .writer
                    .get(&keys::invocation_status(&id(range, key)))
                    .await?
                    .map(|b| b.to_vec());
                let m = self.model.entry((range, key)).or_default();
                if !m.allows(&got) {
                    bail!("end: range {range} key {key} read {got:?}, does not match the model");
                }
            }
        }
        Ok(())
    }
}

async fn run_seed(seed: u64, rounds: usize) -> Result<Sim> {
    let mut rng = Rng(seed);
    let cfg = NodeWalConfig {
        window: Duration::from_millis(5),
        stripes: 1 + rng.below(2) as u16,
        inflight: 1 + rng.below(2) as usize,
        object_cap: 64 << 10,
        // Linger by seed without drawing from `rng` (old seeds keep their
        // schedule): 0 (the old behaviour), 1 ms (default), 4 ms (near the window).
        linger: Duration::from_micros([0, 1_000, 4_000][(seed % 3) as usize]),
        frame_age_flush: Duration::from_millis(300),
        put_timeout: Duration::from_millis(300),
        put_attempts: 2,
        gc_interval: Duration::from_millis(50),
        meter: None,
        rotation_net: None,
    };
    let mut sim = Sim {
        seed,
        rng,
        bucket: Arc::new(InMemory::new()),
        cfg: cfg.clone(),
        nodes: (0..SLOTS).map(|_| None).collect(),
        restarts: 0,
        held: BTreeMap::new(),
        model: BTreeMap::new(),
        counter: 0,
        wires: Vec::new(),
        progress: Default::default(),
        log: vec![format!(
            "seed {seed}: S = {}, W = {}, L = {:?}",
            cfg.stripes, cfg.inflight, cfg.linger
        )],
    };
    let res = async {
        for i in 0..rounds {
            sim.round(i).await?;
        }
        sim.settle().await
    }
    .await;
    if let Err(e) = res {
        bail!(
            "seed {seed} failed: {e:#}\n  reproduce: CAIRN_SIM_SEED={seed}\n  scenario:\n    {}",
            sim.log.join("\n    ")
        );
    }
    Ok(sim)
}

fn env(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

async fn run(default_seeds: u64, default_rounds: usize) -> Result<()> {
    // `CAIRN_SIM_LOG=<filter>` (e.g. `cairn_store=debug`) prints the store's log.
    if let Ok(filter) = std::env::var("CAIRN_SIM_LOG") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_writer(std::io::stderr)
            .try_init();
    }
    let rounds = env("CAIRN_SIM_ROUNDS").map_or(default_rounds, |r| r as usize);
    let seeds: Vec<u64> = match env("CAIRN_SIM_SEED") {
        Some(s) => vec![s],
        None => {
            let base = env("CAIRN_SIM_BASE").unwrap_or(1);
            (base..base + env("CAIRN_SIM_SEEDS").unwrap_or(default_seeds)).collect()
        }
    };
    let (mut writes, mut restarts) = (0, 0);
    let mut fired: BTreeMap<String, usize> = BTreeMap::new();
    // The seeds share nothing (bucket, traps, RNG are their own), and most of a
    // seed's time is spent waiting for TTLs — running them concurrently lets the
    // gate run more seeds in the same time.
    let sims = futures::future::join_all(seeds.iter().map(|seed| run_seed(*seed, rounds))).await;
    for sim in sims {
        let sim = sim?;
        writes += sim.counter;
        restarts += sim.restarts;
        for (p, at, w) in &sim.wires {
            if w.fired() {
                *fired.entry(format!("{p:?}/{at:?}")).or_default() += 1;
            }
        }
        if std::env::var("CAIRN_SIM_VERBOSE").is_ok() {
            println!("{}", sim.log.join("\n"));
        }
    }
    println!("{} seeds × {rounds} rounds: {writes} writes, {restarts} node restarts — 0 values lost, 0 wrong", seeds.len());
    println!("traps fired (step/kind: count): {fired:?}");
    Ok(())
}

/// The gate run: 16 seeds × 8 rounds, concurrently — ~15 s, on par with the old
/// sequential 4 × 6 (~26 s) with six times as many writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_wal_survives_crash_at_every_step() -> Result<()> {
    run(16, 8).await
}

/// The soak run: `cargo test -p cairn-store --test shared_wal_sim -- --ignored`
/// (tune with `CAIRN_SIM_SEEDS`, `CAIRN_SIM_ROUNDS`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long soak — run by hand"]
async fn shared_wal_sim_soak() -> Result<()> {
    run(64, 12).await
}
