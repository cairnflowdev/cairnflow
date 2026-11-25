// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `WrongRange` contract (D23).
//!
//! The range table can change at run time, so the two sides of a send may hold
//! two different versions, and a range may be retiring. The tests here assert
//! three things: the receiver refuses **with information** (`409` with its
//! table) — checked inside the actor too, not only at HTTP; the sender learns
//! the new table from the refusal and resends at once; and ingress retries
//! exactly once on the right engine.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_engine::{DeliverCancel, InterPartition},
    cairn_server::ActorError,
    cairn_store::{make_invocation_id, PartitionId, PartitionKey, PartitionTable, TableUpdate},
};

/// A cancel sent to a (nonexistent) invocation carrying key `key` — enough to
/// walk the whole receiving path without any real invocation.
fn cancel_to(key: u64) -> InterPartition {
    InterPartition::Cancel(DeliverCancel {
        target: make_invocation_id(PartitionKey(key), [7; 16]),
        parent: make_invocation_id(PartitionKey(key), [8; 16]),
        id_completion: 1,
    })
}

async fn post_inbound(
    node: &cairn_server::node::RunningNode,
    range: u32,
    table: u64,
    msg: &InterPartition,
) -> Result<(u16, serde_json::Value)> {
    let resp = common::post_internal(
        node,
        &format!("/internal/inbound?range={range}&table={table}"),
        msg.encode().to_vec(),
    )
    .await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// `409 wrong_range` with the receiver's table, version `version`.
fn is_wrong_range(code: u16, body: &serde_json::Value, version: u64) -> bool {
    code == 409
        && body["code"] == "wrong_range"
        && body["table"]["version"] == version
        && body["table"]["ranges"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
}

/// **Receiver side.** A retiring range, a range in `Splitting`, or a sender
/// holding an older table ⇒ `409 {"code":"wrong_range","table":…}`. The check
/// runs **inside the actor too**: calling the engine of a retiring range
/// directly also gets `WrongRange`, not only the HTTP path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_rejected_while_range_retiring() -> Result<()> {
    let node = common::start_single_process(2).await?;
    let table = node.cluster.table();
    let (r0, r1) = (table.ranges()[0], table.ranges()[1]);
    let to_r0 = cancel_to(r0.start);
    let to_r1 = cancel_to(r1.start);

    let result = async {
        // Happy path: right range, right version.
        let (code, body) = post_inbound(&node, r1.id.0, 1, &to_r1).await?;
        ensure!(code == 200, "happy path: {code} {body}");

        // Range 1 retires ⇒ both HTTP and the actor refuse.
        let h1 = node.partitions.get(r1.id).expect("engine of range 1");
        h1.mark_retiring();
        let (code, body) = post_inbound(&node, r1.id.0, 1, &to_r1).await?;
        ensure!(
            is_wrong_range(code, &body, 1),
            "retiring range over HTTP: {code} {body}"
        );
        let direct = h1.inbound(to_r1.clone()).await;
        ensure!(
            matches!(direct, Err(ActorError::WrongRange)),
            "the actor of a retiring range accepted the message: {direct:?}"
        );

        // Range 0 has a Splitting intent (table v2) ⇒ refused, even for a sender holding v2.
        let applied = node
            .cluster
            .update(1, |t| {
                let mut r = t.ranges().to_vec();
                r[0].intent = Some(cairn_store::RangeIntent::Splitting {
                    at: r[0].start + (r[0].end - r[0].start) / 2,
                    left: PartitionId(2),
                    right: PartitionId(3),
                });
                PartitionTable::from_entries(2, 4, r)
            })
            .await?;
        ensure!(matches!(applied, TableUpdate::Applied(_)), "{applied:?}");
        let (code, body) = post_inbound(&node, r0.id.0, 2, &to_r0).await?;
        ensure!(
            is_wrong_range(code, &body, 2),
            "range in Splitting: {code} {body}"
        );

        // A sender holding an older table ⇒ refused, with the new table for it to learn.
        let (code, body) = post_inbound(&node, r0.id.0, 1, &to_r0).await?;
        ensure!(
            is_wrong_range(code, &body, 2),
            "sender's table is older: {code} {body}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    result
}

/// **Sender side.** Node A holds table v1 and cannot reload it by itself
/// (every GET of its `cluster.conf` fails); the bucket is already at v2. A
/// `ctx.call` from range A's leader to range B's leader must arrive **within a
/// cap**: B refuses `wrong_range` with table v2, A applies the table,
/// recomputes the destination and resends at once within the turn. Before the
/// fix A got `409` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_sender_recovers_after_table_change() -> Result<()> {
    use {
        cairn_store::testkit::{Action, OpSet, PathMatch, Rule},
        common::{
            cluster_nodes,
            fake_sdk::{FakeSdk, Step},
            load_of, single_leaders, wait_probe,
        },
        std::time::Duration,
    };
    let ttl = Duration::from_secs(1);
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 2, ttl).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "one range on each node",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let r_a = *leaders
        .iter()
        .find(|(_, n)| **n == 0)
        .expect("alpha holds a range")
        .0;
    let r_b = *leaders
        .iter()
        .find(|(_, n)| **n == 1)
        .expect("beta holds a range")
        .0;
    let (a, b) = (&nodes[0], &nodes[1]);

    let callee_key = common::key_on_partition(&b.node, "Callee", r_b, "callee");
    let caller_key = common::key_on_partition(&a.node, "Caller", r_a, "caller");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some(&callee_key)),
                Step::EchoCallResult,
            ],
        )
        .object("Callee", "echo", vec![Step::output("from-beta")])
        .spawn()
        .await?;
    for n in &nodes {
        common::register(&n.node.url(), &sdk.url).await?;
    }

    // A loses the path to reload the table; the table on the bucket goes to v2
    // (same ranges — only the version changes, so the only question is "does A
    // learn the new table").
    a.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cluster.conf".into()),
        Action::Fail { times: None },
    ));
    let bumped = b
        .node
        .cluster
        .update(1, |t| {
            PartitionTable::from_entries(2, t.next_range_id(), t.ranges().to_vec())
        })
        .await?;
    ensure!(matches!(bumped, TableUpdate::Applied(_)), "{bumped:?}");
    ensure!(a.node.cluster.version() == 1, "A must still hold v1");

    let result = common::within(
        Duration::from_secs(15),
        "ctx.call from a node holding an old table arrives",
        async {
            let (code, body) =
                common::invoke(&a.node.url(), "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            let st = common::wait_status(&a.node.url(), &id, "completed").await?;
            ensure!(st["output"] == "from-beta", "{st}");
            Ok(())
        },
    )
    .await;
    ensure!(
        result.is_err() || a.node.cluster.version() == 2,
        "A must have applied table v2 learned from the refusal"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **Ingress.** The table changes while the node still holds the old one:
/// range 1 is retiring, the bucket has replaced it with range 2 on the same
/// key span. An `/invoke` routed by the old table reaches range 1's engine,
/// gets `WrongRange` ⇒ ingress reloads the table and retries **exactly once**
/// on range 2's engine. The client sees only the result — no `503`, no `502` —
/// and the invocation lives on the new range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invoke_during_table_change_returns_result_not_503() -> Result<()> {
    use {
        cairn_server::node::{self, NodeMode},
        cairn_store::{ClusterConfig, RangeEntry},
        common::fake_sdk::{FakeSdk, Step},
    };
    let cfg = common::test_config(NodeMode::single(2));
    let stores = cfg.stores.clone();
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow::anyhow!("build node: {e}"))?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;

    let result = common::within(common::TEST_DEADLINE, "invoke on a table change", async {
        common::register(&url, &sdk.url).await?;
        let x = PartitionId(1);
        let key = common::key_on_partition(&node, "Counter", x.0, "k");

        // Range 1 retires; "another node" replaces it with range 2 on the
        // bucket. This node has not reloaded yet (10 s tick), so it still holds v1.
        node.partitions
            .get(x)
            .expect("engine of range 1")
            .mark_retiring();
        let other = ClusterConfig::open(stores.main().store(), stores.main().prefix(), 2).await?;
        let y = PartitionId(other.table().next_range_id());
        let replaced = other
            .update(1, |t| {
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
                PartitionTable::from_entries(2, t.next_range_id() + 1, ranges)
            })
            .await?;
        ensure!(matches!(replaced, TableUpdate::Applied(_)), "{replaced:?}");
        ensure!(node.cluster.version() == 1, "the node must still hold v1");

        let (code, body) = common::invoke(&url, "Counter", "get", Some(&key), "v").await?;
        ensure!(code == 200 && body["output"] == "ok", "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        ensure!(
            common::partition_of(&url, &id).await? == y.0,
            "{id} must live on the new range {}",
            y.0
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}
