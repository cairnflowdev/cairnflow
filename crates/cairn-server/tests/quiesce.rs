// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The stop path of a range:
//! `Quiesce` stops accepting work, waits for work in flight, closes the Db and only then
//! answers; `Retire` leaves a tombstone and nobody reopens the parent Db; `Shutdown`/`Resign`
//! answer **after** the seal is done, not before.
//!
//! Formerly `Shutdown`/`Resign` answered and then sealed: the caller believed the Db was
//! closed while it was still open — and cloning a Db that is still open misses acked
//! data. The split protocol stands on exactly this promise: "answered ⇒ closed".

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_engine::InvocationStatus,
    cairn_server::node::{self, NodeMode},
    cairn_store::{
        read_owner, ObjectStores, PartitionId, PartitionKey, PartitionWriter, StoreLocation,
    },
    common::{
        cluster_nodes,
        fake_sdk::{FakeSdk, Step},
        load_of, single_leaders, wait_probe, ClusterNode,
    },
    std::time::{Duration, Instant},
};

const TTL: Duration = Duration::from_secs(1);

fn stores_of(n: &ClusterNode) -> ObjectStores {
    ObjectStores::single(StoreLocation::from_store(n.store.clone(), "", "memory://"))
}

/// Opens the Db of range `range` (a freshly cloned child, outside the table) for reading: a
/// separate session takes the range and opens it through `acquire_shared` — the child Db
/// carries the parent's shared-WAL mark. Keeps the session and the WAL log alive with the writer.
async fn open_child(
    stores: &ObjectStores,
    range: u32,
) -> Result<(
    PartitionWriter,
    cairn_store::LeaseKeeper,
    cairn_store::NodeWal,
    cairn_store::Session,
)> {
    let session = cairn_store::Session::establish(
        stores.main().store().clone(),
        stores.main().prefix(),
        cairn_store::LeaseConfig::new()
            .with_node_id("quiesce-check")
            .with_ttl(Duration::from_secs(2)),
    )
    .await?;
    let wal = cairn_store::NodeWal::start(
        stores,
        session
            .current()
            .ok_or_else(|| anyhow!("session not open yet"))?,
        cairn_store::NodeWalConfig::default(),
    );
    let (grant, mut keeper) = match session.claim(PartitionId(range)).await? {
        cairn_store::Claim::Won(g, k) => (g, k),
        cairn_store::Claim::Held(_) => return Err(anyhow!("could not take range {range}")),
    };
    let db = cairn_store::acquire_shared(stores, &wal, &mut keeper, range).await?;
    Ok((
        PartitionWriter::open(grant, db).await?,
        keeper,
        wal,
        session,
    ))
}

/// **Quiesce waits for the turn in flight before closing.** An SDK turn stopped at the gate ⇒
/// `quiesce` does not answer; a new call is refused (`503`, not run); opening the
/// gate ⇒ the turn completes, `quiesce` answers, and by then the Db **is closed**: a
/// projection clone accepts it (F3 refuses an open Db) and the child sees the completed invocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesce_waits_for_inflight_turn_then_closes() -> Result<()> {
    let cfg = common::test_config(NodeMode::single(1));
    let stores = cfg.stores.clone();
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("build node: {e}"))?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .gated_service("Slow", "run", vec![Step::output("done")])
        .service("Fast", "run", vec![Step::output("fast")])
        .spawn()
        .await?;
    let gate = sdk.gate();

    let result = common::within(
        Duration::from_secs(30),
        "quiesce a range with a turn in flight",
        async {
            common::register(&url, &sdk.url).await?;
            let (code, body) = common::send(&url, "Slow", "run", None, "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            while sdk.starts_of("Slow", "run").is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let h = node.partitions.get(PartitionId(0)).expect("range 0 engine");
            let quiescing = tokio::spawn(async move { h.quiesce(Duration::from_secs(20)).await });
            tokio::time::sleep(Duration::from_millis(300)).await;
            ensure!(
                !quiescing.is_finished(),
                "quiesce answered while a turn was still in flight"
            );

            // No new work accepted: a new call does not run.
            let (code, body) = common::invoke(&url, "Fast", "run", None, "y").await?;
            ensure!(code == 503, "quiescing range accepts work: {code} {body}");
            ensure!(sdk.starts_of("Fast", "run").is_empty(), "a new turn ran");

            gate.open();
            tokio::time::timeout(Duration::from_secs(10), quiescing)
                .await
                .map_err(|_| anyhow!("quiesce did not answer after the turn completed"))???;

            // Really closed: a projection clone accepts the Db as its source.
            stores
                .clone_range(
                    PartitionId(0),
                    PartitionId(7),
                    (PartitionKey(0), PartitionKey(u64::MAX)),
                )
                .await?;
            let (child, _keeper, _wal, _session) = open_child(&stores, 7).await?;
            // The status row is in the hot zone, the journal in the block zone — the projection
            // keeps both: the turn completed, and its result.
            let raw = cairn_server::ingress::unhex(&id).ok_or_else(|| anyhow!("bad id"))?;
            let status = child
                .get(&cairn_store::keys::invocation_status(&raw))
                .await?
                .and_then(InvocationStatus::decode);
            ensure!(
                matches!(status, Some(InvocationStatus::Completed(_))),
                "the clone after quiesce must see the completed turn: {status:?}"
            );
            let journal = child.journal_unbounded(&raw).await?;
            ensure!(
                journal
                    .iter()
                    .any(|(_, entry)| entry.windows(4).any(|w| w == b"done")),
                "the clone after quiesce must see the turn's result: {} entries",
                journal.len()
            );
            Ok(())
        },
    )
    .await;
    gate.open();
    node.shutdown().await;
    result
}

/// **A retired range is never claimed again.** Alpha retires its
/// range ⇒ the owner record becomes a tombstone (`retired`, yielded); beta sees the
/// yielded record and does **not** take it, does not open the parent Db — even past the
/// rescue grace — and the range's engine exits for good on both nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_range_is_never_reacquired() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 2, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "each node leads one range",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let p = PartitionId(
        *leaders
            .iter()
            .find(|(_, n)| **n == 0)
            .expect("alpha holds a range")
            .0,
    );
    let stores = stores_of(&nodes[0]);

    let result = async {
        nodes[0]
            .node
            .partitions
            .get(p)
            .expect("the range's engine")
            .retire()
            .await?;
        let tomb = read_owner(stores.main().store(), stores.main().prefix(), p)
            .await?
            .ok_or_else(|| anyhow!("the owner record disappeared"))?;
        ensure!(
            tomb.retired && tomb.released,
            "the owner record must become a tombstone: {tomb:?}"
        );

        // Past the rescue grace (2×TTL) plus a few observation ticks: nobody takes it.
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 5 {
            let now = read_owner(stores.main().store(), stores.main().prefix(), p).await?;
            ensure!(
                now.as_ref() == Some(&tomb),
                "a retired range was taken again: {now:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for (i, n) in nodes.iter().enumerate() {
            let h = n.node.partitions.get(p).expect("engine still in the set");
            ensure!(
                h.status([0x11; 24]).await.is_err(),
                "the retired range's engine on node {i} still answers"
            );
        }
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **Quiesce keeps the lease and keeps the Db closed** (LS-8). The seal lowers the validity of the
/// *writer* — it must not lower the validity of the *lease*: formerly the quiesce loop saw the
/// lease "lost" as soon as the seal was done, claimed the range again and **reopened the parent
/// Db** while the coordinator was cloning it. After quiesce, for more than one TTL: no
/// manifest write, the engine does not serve, the owner record is still this node's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesced_range_keeps_lease_and_stays_closed() -> Result<()> {
    use cairn_store::testkit::{Op, PathClass};
    let (_bucket, nodes) = cluster_nodes(&["node-solo"], 1, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(&refs, Duration::from_secs(20), "range 0 leader", |p| {
        single_leaders(p).is_some()
    })
    .await?;
    let stores = stores_of(&nodes[0]);
    let p = PartitionId(0);
    let result = async {
        let h = nodes[0].node.partitions.get(p).expect("the range's engine");
        let owner = read_owner(stores.main().store(), stores.main().prefix(), p)
            .await?
            .ok_or_else(|| anyhow!("no owner record yet"))?;
        h.quiesce(Duration::from_secs(5)).await?;
        let before = nodes[0].store.counts();
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let manifests = nodes[0]
                .store
                .counts()
                .since(&before)
                .get(Op::Put, PathClass::Manifest);
            ensure!(
                manifests == 0,
                "the parent Db was reopened after quiesce ({manifests} manifest PUTs)"
            );
        }
        ensure!(
            h.status([0x11; 24]).await.is_err(),
            "a quiesced range still serves"
        );
        let now = read_owner(stores.main().store(), stores.main().prefix(), p)
            .await?
            .ok_or_else(|| anyhow!("the owner record disappeared"))?;
        ensure!(
            !now.released && now.session == owner.session,
            "quiesce must keep the lease: before {owner:?}, after {now:?}"
        );
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **`Shutdown` and `Resign` answer after the seal.** As soon as the command answers,
/// the owner record has changed — the yield may only be written after the seal is done, so
/// answering before that would lie to the caller that the Db is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_and_resign_reply_after_seal() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha"], 2, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(20),
        "alpha holds both ranges",
        |p| single_leaders(p).is_some_and(|m| m.len() == 2),
    )
    .await?;
    let stores = stores_of(&nodes[0]);
    let owner = |p: u32| {
        let stores = stores.clone();
        async move {
            read_owner(
                stores.main().store(),
                stores.main().prefix(),
                PartitionId(p),
            )
            .await
        }
    };

    let result = async {
        let before = owner(0).await?;
        nodes[0]
            .node
            .partitions
            .get(PartitionId(0))
            .expect("range 0")
            .resign()
            .await?;
        let after = owner(0).await?;
        ensure!(
            after != before,
            "resign answered before yielding: {after:?}"
        );

        nodes[0]
            .node
            .partitions
            .get(PartitionId(1))
            .expect("range 1")
            .shutdown()
            .await?;
        let after = owner(1).await?;
        ensure!(
            after.as_ref().is_some_and(|r| r.released),
            "shutdown answered before yielding: {after:?}"
        );
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **Quiesce does not wait for a parked ticket that was just granted.**
///
/// Interleaving:
/// 1. Rule `acme = 1`; two `StartCall`s to `Work` (scope `acme`, SDK gate
///    closed): the first turn enters the SDK, the second parks its ticket in the scheduler's
///    queue (budget).
/// 2. Quiesce range 0, limit 20 s: the serve loop stops spawning new turns.
/// 3. Open the gate: the first turn completes and releases its slot — the scheduler grants the
///    slot to the ticket.
///
/// Must see: quiesce answers within 5 s of opening the gate — the ticket's slot is returned
/// at once, it does not become a turn (`Scheduler::retire_ready`); the SDK receives only one `Start`.
/// Must not see: the slot granted to the ticket sitting in the ready queue with nobody running it,
/// the turn limit stuck there and quiesce waiting the full 20 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesce_does_not_wait_for_a_ticket_granted_meanwhile() -> Result<()> {
    let store = cairn_store::testkit::FaultyStore::wrap(std::sync::Arc::new(
        slatedb::object_store::memory::InMemory::new(),
    ));
    let node = common::start_led(common::bench::config(&store)).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .gated_service("Work", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let gate = sdk.gate();

    let result = common::within(
        Duration::from_secs(60),
        "quiesce with a ticket granted midway",
        async {
            common::register(&url, &sdk.url).await?;
            let r = reqwest::Client::new()
                .post(format!("{url}/rules"))
                .json(&serde_json::json!({ "pattern": "acme", "concurrency": 1 }))
                .send()
                .await?;
            ensure!(r.status().is_success(), "set rule: {}", r.status());
            common::bench::send(&node, 5, "Work", Some("acme"), 2).await?;
            let t0 = Instant::now();
            loop {
                let h = node.partitions.get(PartitionId(0)).expect("range 0 engine");
                let s = h.flow().await?.scheduler;
                if sdk.starts_count_of("Work", "run") == 1 && s.parked == 1 {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "not yet one turn in the SDK and one parked ticket"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            let h = node.partitions.get(PartitionId(0)).expect("range 0 engine");
            let quiescing = tokio::spawn(async move { h.quiesce(Duration::from_secs(20)).await });
            tokio::time::sleep(Duration::from_millis(300)).await;
            ensure!(
                !quiescing.is_finished(),
                "quiesce answered while a turn was still in flight"
            );
            gate.open();
            tokio::time::timeout(Duration::from_secs(5), quiescing)
                .await
                .map_err(|_| anyhow!("quiesce waited for a mid-way ticket's slot"))???;
            ensure!(
                sdk.starts_count_of("Work", "run") == 1,
                "a ticket granted during quiesce still became a turn"
            );
            Ok(())
        },
    )
    .await;
    gate.open();
    node.shutdown().await;
    result
}
