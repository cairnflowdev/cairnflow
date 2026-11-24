// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A range table that **changes at run time**: nodes follow the new
//! table without restarting.
//!
//! This was the stepping stone for splits: a "one-child split" — replacing range X with
//! range Y over the **same key span**, Y's Db being a clone (no projection) of X's
//! closed Db. The real protocol (intent, quiesce, rollback on dying midway) is the
//! split code; here a test-only helper does the steps by hand, and the only question is
//! **whether nodes follow the table**.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Context, Result},
    cairn_store::{
        read_owner, ObjectStores, PartitionId, PartitionKey, PartitionTable, RangeEntry,
        StoreLocation, TableUpdate,
    },
    common::{
        cluster_nodes,
        fake_sdk::{FakeSdk, Step},
        single_leaders, wait_probe, ClusterNode,
    },
    std::time::{Duration, Instant},
};

const TTL: Duration = Duration::from_secs(1);

/// The shared bucket's store, seen the way every node sees it.
fn stores_of(n: &ClusterNode) -> ObjectStores {
    ObjectStores::single(StoreLocation::from_store(n.store.clone(), "", "memory://"))
}

/// **A test-only helper**: replaces range `x` with a new range over the same key
/// span. Returns the new range id.
///
/// 1. Shut down `x`'s engine on every node. `Shutdown` answers **after** the leader
///    has sealed (closed the Db) and yielded — so by the time it answers, the owner
///    record is `released` and the Db is closed. Cloning a Db that is still open silently
///    misses data that is only in the WAL — closing first is a precondition.
/// 2. Clone `ranges/x` → `ranges/y` with `clone_range` — a projection of the key span's two
///    entity zones. The derived zone does not come along: `y`'s engine
///    opens with a new `(y, gen)` and so rebuilds it before serving.
/// 3. CAS the table: drop `x`, add `y` (`gen + 1` like the left child of a split),
///    `next_range_id + 1`.
async fn replace_range(nodes: &[ClusterNode], x: PartitionId) -> Result<PartitionId> {
    for n in nodes {
        if let Some(h) = n.node.partitions.get(x) {
            let _ = h.shutdown().await;
        }
    }
    let stores = stores_of(&nodes[0]);
    let rec = read_owner(stores.main().store(), stores.main().prefix(), x).await?;
    ensure!(
        rec.as_ref().is_none_or(|r| r.released),
        "Shutdown answered but range {} was not yielded: {rec:?}",
        x.0
    );

    let cluster = &nodes[0].node.cluster;
    let before = cluster.table();
    let y = PartitionId(before.next_range_id());
    let span = before.range(x).expect("x still in the table");
    stores
        .clone_range(x, y, (PartitionKey(span.start), PartitionKey(span.end)))
        .await
        .context("clone the old range's Db")?;

    let replaced = cluster
        .update(before.version(), |t| {
            let ranges = t
                .ranges()
                .iter()
                .map(|r| {
                    if r.id == x {
                        RangeEntry {
                            id: y,
                            gen: r.gen + 1,
                            ..*r
                        }
                    } else {
                        *r
                    }
                })
                .collect();
            PartitionTable::from_entries(t.version() + 1, t.next_range_id() + 1, ranges)
        })
        .await?;
    ensure!(
        matches!(replaced, TableUpdate::Applied(_)),
        "table CAS missed: {replaced:?}"
    );
    Ok(y)
}

/// Nodes follow a new table without restarting: replacing range X with range Y over the same
/// key span ⇒ within a limit, **every** node runs Y's engine and no X engine is left,
/// Y has exactly one leader, and invocations written into X are read back intact
/// through Y.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_follows_table_change_without_restart() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 2, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_probe(
        &refs,
        Duration::from_secs(20),
        "one leader per range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let first = single_leaders(&first).expect("leaders exist");

    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    for n in &nodes {
        common::register(&n.node.url(), &sdk.url).await?;
    }

    let x = PartitionId(1);
    let leader_x = &nodes[first[&x.0]].node;
    let key = common::key_on_partition(leader_x, "Counter", x.0, "k");
    let (code, body) = common::invoke(&leader_x.url(), "Counter", "get", Some(&key), "v").await?;
    ensure!(code == 200 && body["output"] == "ok", "{code} {body}");
    let id = body["id"].as_str().unwrap_or_default().to_string();
    let old: Vec<_> = nodes
        .iter()
        .filter_map(|n| n.node.partitions.get(x))
        .collect();

    let y = replace_range(&nodes, x).await?;
    ensure!(y != x);

    // The limit: one table reload tick (TTL) + claiming the lease + opening the Db, generously.
    let bound = TTL * 3 + Duration::from_secs(10);
    let t0 = Instant::now();
    let probe = wait_probe(&refs, bound, "every node runs Y, no X left", |p| {
        nodes
            .iter()
            .all(|n| n.node.partitions.contains(y) && !n.node.partitions.contains(x))
            && !p.contains_key(&x.0)
            && p.get(&y.0).is_some_and(|v| v.len() == 1)
    })
    .await?;
    let took = t0.elapsed();
    let leader_y = &nodes[probe[&y.0][0]].node;

    // Every node: new table, X's engine fully exited.
    for n in &nodes {
        ensure!(
            n.node.cluster.table().contains(y),
            "{} has not applied the new table",
            n.node.node_id
        );
        ensure!(!n.node.cluster.table().contains(x));
    }
    for h in old {
        ensure!(
            h.status([0x11; 24]).await.is_err(),
            "the engine of a range that left the table still answers"
        );
    }

    // Data intact: the invocation written into X reads back through Y, on Y's leader.
    let (code, st) = common::invocation(&leader_y.url(), &id).await?;
    ensure!(code == 200 && st["status"] == "completed", "{code} {st}");
    ensure!(st["output"] == "ok", "{st}");
    ensure!(
        common::partition_of(&leader_y.url(), &id).await? == y.0,
        "{id} must be in range {}",
        y.0
    );
    // And a new call with the same key runs on Y.
    let (code, body) = common::invoke(&leader_y.url(), "Counter", "get", Some(&key), "w").await?;
    ensure!(code == 200 && body["output"] == "ok", "{code} {body}");

    tracing::info!(?took, "node follows the new table");
    for n in nodes {
        n.node.shutdown().await;
    }
    drop(sdk);
    Ok(())
}

/// Replaces per-run values (node names, ports) with placeholders, so the whole JSON can
/// be compared against a literal.
fn normalize(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, x) in m.iter_mut() {
                match (k.as_str(), &*x) {
                    ("leader", serde_json::Value::String(_)) => *x = "<node>".into(),
                    ("address", serde_json::Value::String(_)) => *x = "<addr>".into(),
                    _ => normalize(x),
                }
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(normalize),
        _ => {}
    }
}

/// **A snapshot of `/cluster`**: the table version, `next_range_id`, and
/// one row per range — id, key span (16 hex digits, like `cluster.conf`),
/// `gen`, the intent if any, the leader. `partitionCount`/`partitions` stay unchanged for
/// old clients.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_view_snapshot() -> Result<()> {
    let node = common::start_single_process(2).await?;
    // One intent on range 1 — table version 2, `next_range_id` 4.
    let applied = node
        .cluster
        .update(1, |t| {
            let mut r: Vec<RangeEntry> = t.ranges().to_vec();
            r[1].intent = Some(cairn_store::RangeIntent::Splitting {
                at: 0xc000_0000_0000_0000,
                left: PartitionId(2),
                right: PartitionId(3),
            });
            PartitionTable::from_entries(2, 4, r)
        })
        .await?;
    ensure!(matches!(applied, TableUpdate::Applied(_)), "{applied:?}");

    let mut got: serde_json::Value = reqwest::get(format!("{}/cluster", node.url()))
        .await?
        .json()
        .await?;
    normalize(&mut got);
    let want = serde_json::json!({
        "version": 2,
        "nextRangeId": 4,
        "ranges": [
            {
                "range": 0,
                "start": "0000000000000000",
                "end": "7fffffffffffffff",
                "gen": 0,
                "leader": "<node>",
                "address": "<addr>",
                "term": 1
            },
            {
                "range": 1,
                "start": "8000000000000000",
                "end": "ffffffffffffffff",
                "gen": 0,
                "intent": {
                    "kind": "splitting",
                    "at": "c000000000000000",
                    "left": 2,
                    "right": 3
                },
                "leader": "<node>",
                "address": "<addr>",
                "term": 1
            }
        ],
        "partitionCount": 2,
        "partitions": [
            { "partition": 0, "leader": "<node>", "address": "<addr>", "term": 1 },
            { "partition": 1, "leader": "<node>", "address": "<addr>", "term": 1 }
        ],
        "readers": []
    });
    ensure!(
        got == want,
        "/cluster differs from the snapshot:\n{}\n— want —\n{}",
        serde_json::to_string_pretty(&got)?,
        serde_json::to_string_pretty(&want)?
    );
    node.shutdown().await;
    Ok(())
}
