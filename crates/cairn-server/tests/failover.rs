// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Failover at the server layer.
//!
//! > Kill the leader of a partition → the invocation continues on another node, **no
//! > data lost, no file moved.**
//!
//! The two nodes here are two [`EngineHandle`]s in the same process, pointing at the
//! same object store. That model is enough to check everything that belongs to *this
//! layer*: who holds the lease, who answers `NotLeader`, who recovers suspended work,
//! and whether the old leader's commits blow up.
//!
//! What it does **not** check: real network partitions, and the object store's real
//! latency. Those need chaos tests with real processes, not done here.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{bail, Result},
    cairn_engine::Engine,
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness},
        registry::Registry,
    },
    cairn_store::{
        Claim, CrashHandle, LeaseConfig, ObjectStores, PartitionId, PartitionWriter, Session,
        StoreLocation,
    },
    slatedb::object_store::{memory::InMemory, ObjectStore},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const PARTITION: PartitionId = PartitionId(0);
const TTL: Duration = Duration::from_millis(600);

/// One node of the simulated cluster.
struct Node {
    handle: EngineHandle,
    /// How to kill the node's session renewal loop — like `kill -9`.
    crash: CrashHandle,
    session: Session,
}

async fn spawn_node(stores: &ObjectStores, node_id: &str) -> Result<Node> {
    let registry = Registry::new();
    let session = Session::establish(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
        LeaseConfig::new()
            .with_node_id(node_id)
            .with_ttl(TTL)
            .with_renew_interval(TTL / 6),
    )
    .await?;
    let crash = session.crash_handle();
    let node_session = session.clone();
    let stores = stores.clone();

    let handle = EngineHandle::spawn(
        Box::new(move || {
            let stores = stores.clone();
            let session = session.clone();
            Box::pin(async move {
                // Wait until the partition is obtained — while the old owner lives, wait forever.
                let (grant, keeper) = loop {
                    if let Claim::Won(g, k) = session.claim(PARTITION).await? {
                        break (g, k);
                    }
                    tokio::time::sleep(TTL / 6).await;
                };

                // Lease FIRST, `Db::open` AFTER. The other way round every starting node
                // would push `writer_epoch` up and fence the healthy leader, though it
                // has no right to anything.
                let db = stores.open_db(PARTITION.0).await?;
                let writer = PartitionWriter::open(grant, db).await?;
                Ok(Leadership {
                    engine: Engine::open(writer).await?,
                    keeper: Some(keeper),
                })
            })
        }),
        registry.clone(),
        None,
        // The node may be a follower — blocking here would turn it into a hung process.
        Readiness::Immediately,
    )?;

    Ok(Node {
        handle,
        crash,
        session: node_session,
    })
}

impl Node {
    /// `status` of an id that does not exist is the cheapest probe that still goes
    /// through the writer: the leader returns `Ok(None)`, a follower a lost-write-right
    /// error.
    ///
    /// Accepts **both** lost-write-right errors, because the boundary between them is
    /// only timing: the first command to hit the fence gets `Fenced`, later ones get
    /// `NotLeader`. Catching only `NotLeader` would make the test depend on exactly
    /// when it knocks.
    async fn is_leader(&self) -> Result<bool> {
        match self.handle.status([0x11; 24]).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_fenced() => Ok(false),
            Err(e) => bail!("unexpected error while probing leadership: {e}"),
        }
    }

    fn crash_lease(&self) {
        self.crash.crash();
    }

    /// Whether the engine is leading — asked through `flow`, answered in place, **not**
    /// through the read fence (LS-5): the fence refuses the old owner's read answers
    /// (so [`Self::is_leader`] sees `false` as soon as its session has been expired) but
    /// does not make it step down. This probe only changes when the grant is revoked.
    async fn leads(&self) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_millis(500), self.handle.flow()).await,
            Ok(Ok(_))
        )
    }

    /// The owner-change notice **within the process**: every time `self`'s session
    /// claims a range from `old`'s session, nudge `old`'s owner-record watch loop —
    /// exactly what `owner_notice` does through the internal port on a real node.
    fn tells(&self, old: &Node) {
        let old = old.session.clone();
        self.session
            .on_owner_taken(move |t| old.owner_changed(t.range));
    }
}

async fn wait_leader(node: &Node, what: &str) -> Result<Duration> {
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(10) {
        if node.is_leader().await? {
            return Ok(t0.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bail!("past 10s and {what} is still not leader");
}

/// The store for one run.
///
/// By default `InMemory` — fast, runs in CI, needs no infrastructure. Set
/// `CAIRN_TEST_S3_URI` (with `CAIRN_TEST_S3_ENDPOINT`/`_KEY`/`_SECRET`/`_REGION`) and
/// **the same scenario runs on a real store**:
///
/// ```sh
/// scripts/dev-store/up.sh
/// CAIRN_TEST_S3_ENDPOINT=http://localhost:18333 CAIRN_TEST_S3_URI=s3://walbench/failover \
/// CAIRN_TEST_S3_KEY=dev-only-access-key CAIRN_TEST_S3_SECRET=dev-only-not-a-secret CAIRN_TEST_S3_REGION=us-east-1 \
///   cargo test -p cairn-server --test failover -- --test-threads=1 --nocapture
/// ```
///
/// Not split into two tests: the scenario is identical, and two copies would drift
/// apart. `InMemory` answers instantly, so it **cannot** check what the object store's
/// real latency produces — that is why the real-store path must exist, even though it
/// does not run by default.
fn test_stores(what: &str) -> ObjectStores {
    let Ok(uri) = std::env::var("CAIRN_TEST_S3_URI") else {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        return ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    };
    for (k, v) in [
        ("AWS_ENDPOINT", "CAIRN_TEST_S3_ENDPOINT"),
        ("AWS_ACCESS_KEY_ID", "CAIRN_TEST_S3_KEY"),
        ("AWS_SECRET_ACCESS_KEY", "CAIRN_TEST_S3_SECRET"),
        ("AWS_REGION", "CAIRN_TEST_S3_REGION"),
    ] {
        if let Ok(val) = std::env::var(v) {
            std::env::set_var(k, val);
        }
    }
    std::env::set_var("AWS_ALLOW_HTTP", "true");
    std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false");
    // A prefix of its own per test and per run: the bucket is shared, and a lease or a
    // manifest left over from a previous run would skew the scenario in ways that are
    // very hard to spot.
    let uri = format!("{uri}/{what}-{}", std::process::id());
    ObjectStores::single(StoreLocation::parse(&uri).expect("parse CAIRN_TEST_S3_URI"))
}

/// Exactly **one** node is leader; the other answers `NotLeader`, without hanging.
///
/// The second half matters as much as the first. A follower that *queues and waits*
/// instead of answering is also "not the leader" in every sense, but it turns every
/// request sent to the wrong node into a hung client — and a load balancer has no way
/// of knowing to redirect.
#[tokio::test]
async fn stopping_a_leader_remaining_node_returns_notleader() -> Result<()> {
    let stores = test_stores("one-leader");
    stores.assert_conditional_put().await?;
    stores.assert_compare_and_swap().await?;

    let a = spawn_node(&stores, "a").await?;
    wait_leader(&a, "A").await?;
    let b = spawn_node(&stores, "b").await?;

    // B must answer **at once**, without waiting out the TTL. Measure the time too.
    let t0 = Instant::now();
    let b_leader = b.is_leader().await?;
    let latency = t0.elapsed();

    assert!(!b_leader, "both nodes believe they are the leader");
    assert!(
        latency < TTL,
        "B took {latency:?} to answer — it is queueing rather than answering NotLeader"
    );
    assert!(a.is_leader().await?, "A must still be the leader");

    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// **The leader freezes → the other node takes over → the data is intact, the old
/// leader stops writing.**
///
/// A "dies" by stopping its lease renewal loop **without** lowering the grant's
/// validity — exactly like `kill -9` or a long GC pause. A still believes it is the
/// leader, so layer 1 does not stop it; what stops it is the `writer_epoch` B pushes up
/// when opening the store.
///
/// Three things are asserted, in order of importance:
///
/// 1. B becomes leader without anyone intervening.
/// 2. B reads what A wrote — **0 bytes of data movement**. The state stays in place on
///    object storage; no file is moved.
/// 3. A **notices by itself** that it has lost the right and switches to answering
///    `NotLeader`, instead of endlessly repeating a commit that never succeeds.
#[tokio::test]
async fn frozen_leader_other_node_takes_over_with_data_intact() -> Result<()> {
    let stores = test_stores("frozen");
    stores.assert_conditional_put().await?;
    stores.assert_compare_and_swap().await?;

    let a = spawn_node(&stores, "a").await?;
    wait_leader(&a, "A").await?;

    // A writes something that can be found again, through **its own API** — no second
    // writer opened from the test, because that would fence A and the test would break
    // its own scenario.
    //
    // `invoke` writes entry 0 (`Input`) **before** looking up the deployment, so it
    // creates real state without needing an SDK behind it. The call will fail with
    // 404 because there is no service — fine, what we check is whether the journal
    // survives the failover, not whether the invocation finishes.
    let marker: cairn_server::actor::InvocationId = [0xA1; 24];
    let _ = a
        .handle
        .invoke(
            marker,
            cairn_engine::Target {
                tenant: Default::default(),
                scope: None,
                service: "nonexistent".into(),
                handler: "h".into(),
                object_key: String::new(),
            },
            None,
            None,
            bytes::Bytes::from_static(b"hello"),
        )
        .await?;
    assert_eq!(
        a.handle.journal(marker).await?.len(),
        1,
        "A must be able to write the Input entry"
    );

    // A freezes.
    a.crash_lease();

    let b = spawn_node(&stores, "b").await?;
    b.tells(&a);
    let took = wait_leader(&b, "B").await?;
    assert!(
        took >= TTL,
        "B became leader after {took:?}, earlier than the TTL {TTL:?} — the CAS condition was not respected"
    );

    // (2) A's data is intact at B — **0 bytes of data movement**. The state stays in
    // place on object storage; B merely opens it.
    assert_eq!(
        b.handle.journal(marker).await?.len(),
        1,
        "B does not see what A wrote before dying"
    );

    // (3) A notices by itself. A's lease validity is pinned open (layer 1 off), its
    // renewal loop is dead (layer 2 off), and A writes nothing so it never touches the
    // CAS. What remains is the node's **owner-record watch loop** (layer 3): when B
    // claims the range from A's session it tells A (here within the process, `tells`),
    // A GETs the owner record, sees it has moved to B's session and revokes the grant.
    // The loop's safety net is 10 minutes, so without the notice A would believe it
    // leads for the whole test. Probed with `leads` (not through the read fence): the
    // read fence refuses A's answers as soon as its session is expired — correct, but a
    // different layer (LS-5), and before the notice existed it hid whether layer 3 ran
    // at all.
    let bound = TTL * 3 + Duration::from_secs(1);
    let t0 = Instant::now();
    loop {
        if !a.leads().await {
            break;
        }
        if t0.elapsed() > bound {
            bail!("A still believes it is the leader after {bound:?} — it is writing blind");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    Ok(())
}

/// **The read fence at the leader: a node waking up after losing its range does not
/// return a stale copy.**
///
/// Interleaving:
/// 1. A leads range 0. A's store wraps the shared bucket to inject faults for A alone.
/// 2. A "freezes" (`crash_lease`): the renewal loop dies, the grant validity is pinned
///    open — A's clock as if stopped (a sleeping machine, a paused VM:
///    `CLOCK_MONOTONIC` does not advance), so A's validity check (`alive`) still
///    passes. A's LIST and GET of `owners/` fail, and B does not tell A: the
///    owner-record watch loop (layer 3 of LS-6) is blind — its worst case (the notice
///    lost, the safety net not yet due).
/// 3. After a silent TTL, B CASes A's session to `expired`, claims range 0, becomes leader.
/// 4. B creates invocation `M` (`invoke` returns: the `Input` entry is durable, the
///    caller has seen it).
/// 5. A caller asks A for `journal(M)` — **after** (4).
///
/// Must see: A does not answer with an empty journal — a stale copy, since `M` was
/// acked before the question was sent; A returns a lost-right error. Without the read
/// fence: A returned `Ok([])`.
#[tokio::test]
async fn leader_waking_after_losing_its_range_never_serves_a_stale_read() -> Result<()> {
    use cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
    let bucket = Arc::new(InMemory::new());
    let raw: Arc<dyn ObjectStore> = bucket.clone();
    let shared = ObjectStores::single(StoreLocation::from_store(raw, "", "memory://"));
    let faulty = FaultyStore::wrap(bucket.clone());
    let a_stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));

    let a = spawn_node(&a_stores, "a").await?;
    wait_leader(&a, "A").await?;

    faulty.add_rule(Rule::new(
        OpSet::LIST | OpSet::GET,
        PathMatch::Contains("owners".into()),
        Action::Fail { times: None },
    ));
    a.crash_lease();

    let b = spawn_node(&shared, "b").await?;
    wait_leader(&b, "B").await?;
    let m: cairn_server::actor::InvocationId = [0xB2; 24];
    let _ = b
        .handle
        .invoke(
            m,
            cairn_engine::Target {
                tenant: Default::default(),
                scope: None,
                service: "nonexistent".into(),
                handler: "h".into(),
                object_key: String::new(),
            },
            None,
            None,
            bytes::Bytes::from_static(b"after A lost the range"),
        )
        .await?;
    assert_eq!(b.handle.journal(m).await?.len(), 1, "B must write M");

    let stale = a.handle.journal(m).await;
    a.handle.shutdown().await?;
    b.handle.shutdown().await?;
    match stale {
        Ok(rows) => bail!(
            "A answered a read after losing the range: journal(M) has {} rows — M was acked at B before the question (stale read)",
            rows.len()
        ),
        Err(e) if e.is_fenced() => Ok(()),
        Err(e) => bail!("unexpected error: {e}"),
    }
}
