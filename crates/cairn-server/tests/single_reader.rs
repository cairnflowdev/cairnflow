// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! One reader for every queue of a partition, and a background path that runs
//! no SDK turn.
//!
//! Before this change, cross-partition messages were only processed on the
//! background tick (`tick`), and that processing ran **the whole SDK turn** of
//! the invocation it had just created while holding the drain lock — which
//! every `/invoke` must also take (to drain its own signals). A slow callee in
//! this partition thus held the entrance of the whole partition.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    cairn_engine::{InterPartition, StartCall},
    cairn_server::node::{self, NodeMode},
    cairn_store::{make_invocation_id, PartitionKey},
    common::{
        fake_sdk::{FakeSdk, Step},
        TEST_DEADLINE,
    },
    std::time::Duration,
};

fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn start_call(callee: [u8; 24], service: &str) -> InterPartition {
    InterPartition::StartCall(StartCall {
        callee,
        service: service.into(),
        handler: "run".into(),
        object_key: String::new(),
        parameter: Bytes::from_static(b"{}"),
        reply_to: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
        tenant: None,
        sender: (make_invocation_id(PartitionKey(0), [0xEE; 16]), 1),
        schedule: None,
        lineage: Default::default(),
    })
}

async fn post_inbound(node: &cairn_server::node::RunningNode, msg: &InterPartition) -> Result<()> {
    let resp = common::post_internal(node, "/internal/inbound", msg.encode().to_vec()).await?;
    ensure!(resp.status().is_success(), "inbound: {}", resp.status());
    Ok(())
}

/// A cross-partition message is processed **when it arrives**, not on the
/// background tick.
///
/// The node here turns the background tick off entirely. Before the change, the
/// `inbound` rows were only scanned in `tick`, so this message sat still forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_processed_without_tick() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.tick_interval = None;
    // Wait for the node to lead the range: a message sent straight to the
    // internal port (as a peer sends it) before the claim gets `503`, and a real
    // peer resends — this test sends once (`start` does not wait for any claim).
    let node = common::start_led(cfg).await?;
    let url = node.url();

    let r = common::within(
        TEST_DEADLINE,
        "cross-partition message needs no background tick",
        async {
            common::register(&url, &sdk.url).await?;
            let callee = make_invocation_id(PartitionKey(0), [5; 16]);
            post_inbound(&node, &start_call(callee, "Echo")).await?;
            tokio::time::timeout(
                Duration::from_secs(3),
                common::wait_status(&url, &hex(&callee), "completed"),
            )
            .await
            .map_err(|_| anyhow::anyhow!("StartCall sat still 3s with no background tick"))??;
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// A slow callee created by a cross-partition message must **not** hold the
/// entrance of the whole partition.
///
/// Before the change: the background tick processed `StartCall` by running the
/// callee's first turn right away while holding the drain lock, and every
/// `/invoke` waited for that drain lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invoke_reply_not_blocked_by_inbound_processing() -> Result<()> {
    let sdk = FakeSdk::builder()
        .gated_service("Slow", "run", vec![Step::output("late")])
        .service("Y", "run", vec![Step::output("y")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let gate = sdk.gate();

    let r = common::within(
        TEST_DEADLINE,
        "entrance not stuck behind a slow callee",
        async {
            common::register(&url, &sdk.url).await?;
            let callee = make_invocation_id(PartitionKey(0), [6; 16]);
            post_inbound(&node, &start_call(callee, "Slow")).await?;
            // The callee's turn has entered the SDK and stands at the gate.
            while sdk.starts_of("Slow", "run").is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            let started = std::time::Instant::now();
            let res = tokio::time::timeout(
                Duration::from_secs(3),
                common::invoke(&url, "Y", "run", None, "{}"),
            )
            .await;
            let (code, body) = res.map_err(|_| {
                anyhow::anyhow!(
                    "an unrelated /invoke waited 3s behind the turn of a slow callee created by a \
                 cross-partition message"
                )
            })??;
            ensure!(
                code == 200 && body["output"] == "y",
                "{code} {body} after {:?}",
                started.elapsed()
            );
            gate.open();
            Ok(())
        },
    )
    .await;
    gate.open();
    node.shutdown().await;
    r
}

/// Becoming leader must **answer at once**, even when the old leader's unfinished
/// work has a slow SDK turn.
///
/// `recover` used to run the whole turn of each unfinished invocation, one by
/// one, **before** the serving loop — a slow SDK there meant a partition that
/// could not even answer `/invocations/{id}` until that turn finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_does_not_block_serving() -> Result<()> {
    let sdk = FakeSdk::builder()
        .gated_service("Slow", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let cfg = common::test_config(NodeMode::single(1));
    let gate = sdk.gate();

    let r = common::within(
        TEST_DEADLINE,
        "recovery does not block the serving loop",
        async {
            // Old leader: create unfinished work, leave its turn stuck at the gate, then die.
            let old = node::start(cfg.clone())
                .await
                .map_err(|e| anyhow::anyhow!("old node: {e}"))?;
            common::register(&old.url(), &sdk.url).await?;
            let (code, body) = common::send(&old.url(), "Slow", "run", None, "{}").await?;
            ensure!(code == 202, "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            while sdk.starts_of("Slow", "run").is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            old.shutdown().await;

            // New leader on the same store: recovery meets exactly that invocation, and
            // the gate is still closed.
            let new = node::start(cfg.clone())
                .await
                .map_err(|e| anyhow::anyhow!("new node: {e}"))?;
            let started = std::time::Instant::now();
            let res =
                tokio::time::timeout(Duration::from_secs(2), common::invocation(&new.url(), &id))
                    .await;
            let answered = res.is_ok();
            gate.open();
            new.shutdown().await;
            ensure!(
            answered,
            "the new leader did not answer /invocations/{{id}} within 2s — recovery is running an \
             SDK turn before the serving loop (after {:?})",
            started.elapsed()
        );
            Ok(())
        },
    )
    .await;
    gate.open();
    r
}
