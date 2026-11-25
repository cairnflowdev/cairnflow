// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Leadership balancing — dynamic rebalancing.
//!
//! The scenario is exactly as in real operation: node A starts alone and **takes
//! every** partition (correctly — a lone node's fair share is everything). Node B
//! joins. Without anyone telling anyone, A must gradually release down to its fair
//! share and B must pick up exactly its own share — and then the distribution must
//! **stay put**, because a balancing policy that oscillates causes its own failover
//! storm.
//!
//! As in `failover.rs`: the two nodes are two [`EngineHandle`] sets in the same
//! process, sharing one object store, one session per node (D20). Ground truth comes
//! from the owner records in the bucket themselves, not from the balancer's books.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    cairn_engine::Engine,
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness},
        balance::Balancer,
        registry::Registry,
    },
    cairn_store::{
        read_owner, LeaseConfig, ObjectStores, PartitionId, PartitionWriter, Session, StoreLocation,
    },
    std::{
        collections::BTreeMap,
        sync::Arc,
        time::{Duration, Instant},
    },
};

const PARTITIONS: u16 = 4;
const TTL: Duration = Duration::from_millis(600);

struct Node {
    session: Session,
    handles: BTreeMap<u32, EngineHandle>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Node {
    /// Dies **for real**: the session stops renewing, the engine shuts down. Others
    /// mark its session after a silent TTL — like a killed process.
    async fn die(self) {
        self.session.abort();
        for t in self.tasks {
            t.abort();
        }
        for h in self.handles.values() {
            let _ = h.shutdown().await;
        }
    }
}

async fn session_of(stores: &ObjectStores, node_id: &str) -> Result<Session> {
    Ok(Session::establish(
        Arc::clone(stores.main().store()),
        stores.main().prefix(),
        LeaseConfig::new()
            .with_node_id(node_id)
            .with_ttl(TTL)
            .with_renew_interval(TTL / 6),
    )
    .await?)
}

/// An engine for every partition, claimed through `balancer`.
fn spawn_engines(
    stores: &ObjectStores,
    session: &Session,
    balancer: &Balancer,
) -> Result<BTreeMap<u32, EngineHandle>> {
    let mut handles = BTreeMap::new();
    for p in 0..PARTITIONS {
        let partition = PartitionId(p as u32);
        let stores = stores.clone();
        let session = session.clone();
        let balancer = balancer.clone();
        let handle = EngineHandle::spawn(
            Box::new(move || {
                let stores = stores.clone();
                let session = session.clone();
                let balancer = balancer.clone();
                Box::pin(async move {
                    let (grant, keeper) = balancer
                        .acquire(&session, partition, TTL * 2, TTL / 6)
                        .await?;
                    let db = stores.open_db(partition.0).await?;
                    let writer = PartitionWriter::open(grant, db).await?;
                    Ok(Leadership {
                        engine: Engine::open(writer).await?,
                        keeper: Some(keeper),
                    })
                })
            }),
            Registry::new(),
            None,
            Readiness::Immediately,
        )?;
        handles.insert(partition.0, handle);
    }
    Ok(handles)
}

async fn spawn_node(stores: &ObjectStores, node_id: &str) -> Result<Node> {
    let session = session_of(stores, node_id).await?;
    let balancer = Balancer::new(
        cairn_store::ClusterConfig::equal(PARTITIONS),
        node_id.into(),
        session.membership(),
    );
    let handles = spawn_engines(stores, &session, &balancer)?;
    let releasing =
        balancer.spawn_release_loop(cairn_server::ingress::Partitions::new(handles.clone()), TTL);
    Ok(Node {
        session,
        handles,
        tasks: vec![releasing],
    })
}

/// Who holds which partition — read straight from the owner records, as the routing table does.
async fn holders(stores: &ObjectStores) -> Result<BTreeMap<u32, String>> {
    let mut out = BTreeMap::new();
    for p in 0..PARTITIONS {
        let rec = read_owner(
            stores.main().store(),
            stores.main().prefix(),
            PartitionId(p as u32),
        )
        .await?;
        if let Some(r) = rec.filter(|r| !r.released) {
            out.insert(p as u32, r.session.node);
        }
    }
    Ok(out)
}

fn count_of(holders: &BTreeMap<u32, String>, node: &str) -> usize {
    holders.values().filter(|h| h.as_str() == node).count()
}

async fn wait_for(
    stores: &ObjectStores,
    what: &str,
    deadline: Duration,
    ok: impl Fn(&BTreeMap<u32, String>) -> bool,
) -> Result<BTreeMap<u32, String>> {
    let t0 = Instant::now();
    let mut last = BTreeMap::new();
    while t0.elapsed() < deadline {
        last = holders(stores).await?;
        if ok(&last) {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("past {deadline:?} and still not {what}; last distribution: {last:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn late_joining_node_gets_a_share_and_assignment_settles() -> Result<()> {
    let stores = ObjectStores::single(StoreLocation::parse("memory://")?);

    // ── Alone: holding everything is correct, not a bug.
    let _a = spawn_node(&stores, "node-a").await?;
    let dist = wait_for(
        &stores,
        "A holds all 4 partitions",
        Duration::from_secs(10),
        |h| count_of(h, "node-a") == PARTITIONS as usize,
    )
    .await?;
    assert_eq!(dist.len(), PARTITIONS as usize);

    // ── B joins: A must release down to its fair share, B picks up its own.
    let b = spawn_node(&stores, "node-b").await?;
    wait_for(
        &stores,
        "distribution converges to 2/2",
        Duration::from_secs(20),
        |h| {
            h.len() == PARTITIONS as usize
                && count_of(h, "node-a") == 2
                && count_of(h, "node-b") == 2
        },
    )
    .await?;

    // ── And stays put: after a few more TTLs still 2/2, no partitions swapped.
    let settled = holders(&stores).await?;
    tokio::time::sleep(TTL * 4).await;
    let later = holders(&stores).await?;
    assert_eq!(
        settled, later,
        "distribution oscillates after balancing — the policy is causing failovers itself"
    );

    // ── B dies: its session is marked after a TTL ⇒ A's fair share becomes 4 ⇒
    // A takes everything back. The reverse of the division above.
    b.die().await;
    wait_for(
        &stores,
        "A takes all 4 back after B dies",
        Duration::from_secs(20),
        |h| count_of(h, "node-a") == PARTITIONS as usize,
    )
    .await?;
    Ok(())
}

/// The rescue path: a "ghost" node with a live session that never takes its share
/// (dead / wedged engine). The healthy node's fair share is still halved, but an
/// empty partition **must not be orphaned forever** — after a grace period the
/// healthy node takes more than its share.
#[tokio::test(flavor = "multi_thread")]
async fn unclaimed_partition_rescued_even_beyond_fair_share() -> Result<()> {
    let stores = ObjectStores::single(StoreLocation::parse("memory://")?);

    // The ghost node: only a session.
    let _ghost = session_of(&stores, "node-ma").await?;

    // The real node, WITHOUT the release loop (so the measurement is not mixed with
    // the periodic yield-and-take of this pathological state — see the comment in `balance.rs`).
    let session = session_of(&stores, "node-a").await?;
    // Wait until A sees the ghost, otherwise the initial fair share is 4 and the
    // scenario does not go through the rescue path.
    let tracker = session.tracker();
    let t0 = Instant::now();
    while !tracker.live_nodes().contains("node-ma") {
        assert!(t0.elapsed() < TTL * 3, "A does not see the ghost node");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let balancer = Balancer::new(
        cairn_store::ClusterConfig::equal(PARTITIONS),
        "node-a".into(),
        session.membership(),
    );
    // Keep the handle alive for the whole test: a closed command channel shuts the engine down.
    let _handles = spawn_engines(&stores, &session, &balancer)?;

    // Two live "nodes" ⇒ fair share = 2 ⇒ A takes 2 at once. The other two
    // partitions stay empty because the ghost never takes them — once the 2×TTL grace
    // has passed A must take them too, even though it is over its share.
    wait_for(
        &stores,
        "A rescues all 4 partitions even though the ghost still has a session",
        Duration::from_secs(20),
        |h| count_of(h, "node-a") == PARTITIONS as usize,
    )
    .await?;
    Ok(())
}
