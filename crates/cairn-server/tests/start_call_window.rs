// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The dedup window of `StartCall` at the target range — pins today's
//! behaviour.
//!
//! The target range deduplicates a resent `StartCall` message by **the callee's
//! own invocation row** (`turn/admit.rs`, branch `Source::Inbound`: row present
//! ⇒ `Attached`, nothing created). That row lives until it is purged
//! (retention, or `POST /invocations/{id}/purge`), so the dedup window equals
//! the retention — on purpose: a sender only resends outbox rows it has not
//! deleted, and an outbox row is deleted right after the ack, so a resend later
//! than the retention needs the sender to stall (or lose the ack) for longer
//! than the retention.
//!
//! This test does **not** say the behaviour is right; it pins it so that a
//! change is a deliberate `test:`/`fix:` pair. To change the behaviour, flip the
//! assertion in step 4 and fix the matching line in `docs/guides/flow.md`.

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
    cairn_server::node::{NodeMode, RunningNode},
    cairn_store::{make_invocation_id, PartitionKey},
    common::{
        fake_sdk::{FakeSdk, Step},
        TEST_DEADLINE,
    },
};

fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// A `StartCall` message as another range's outbox drainer sends it: the same
/// `sender` (the sender's outbox row) on every resend.
fn start_call(callee: [u8; 24], service: &str, sender_seq: u32) -> InterPartition {
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
        sender: (make_invocation_id(PartitionKey(0), [0xEE; 16]), sender_seq),
        schedule: None,
        lineage: Default::default(),
    })
}

async fn post_inbound(node: &RunningNode, msg: &InterPartition) -> Result<()> {
    let resp = common::post_internal(node, "/internal/inbound", msg.encode().to_vec()).await?;
    ensure!(resp.status().is_success(), "inbound: {}", resp.status());
    Ok(())
}

/// **A `StartCall` resent after the callee was purged runs the callee again.**
///
/// 1. A `StartCall(c)` message reaches the target range: callee `c` runs one
///    turn, `completed`.
/// 2. The sender resends exactly that message (it died after the receiver
///    acked, before deleting the outbox row — IR-2). Then another
///    `StartCall(p)` message arrives after it: the inbound drainer follows
///    `seq`, so when `p` completes the resend has been processed.
///    Must see: `c` does **not** run again (the invocation row is still there ⇒
///    `Attached`).
/// 3. `POST /invocations/{c}/purge` — as if the retention ran out.
/// 4. The sender resends the `StartCall(c)` message once more.
///    Must see (today's behaviour): the target range creates `c` again and the
///    SDK gets a second turn of `c` — the dedup window closed with the
///    invocation row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_call_resent_after_callee_purge_runs_callee_again() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .service("Probe", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::test_config(NodeMode::single(1))).await?;
    let url = node.url();
    let callee = make_invocation_id(PartitionKey(0), [7; 16]);
    let probe = make_invocation_id(PartitionKey(0), [8; 16]);
    let msg = start_call(callee, "Echo", 1);

    let r = common::within(TEST_DEADLINE, "StartCall resent around a purge", async {
        common::register(&url, &sdk.url).await?;

        // 1. The first receipt.
        post_inbound(&node, &msg).await?;
        common::wait_status(&url, &hex(&callee), "completed").await?;
        let runs = sdk.starts_of("Echo", "run").len();
        ensure!(runs == 1, "callee's first turn: {runs} turns");

        // 2. Resend while the invocation row is still there: no rerun.
        post_inbound(&node, &msg).await?;
        post_inbound(&node, &start_call(probe, "Probe", 2)).await?;
        common::wait_status(&url, &hex(&probe), "completed").await?;
        let runs = sdk.starts_of("Echo", "run").len();
        ensure!(
            runs == 1,
            "a resend inside the dedup window ran the callee again: {runs} turns"
        );

        // 3. Purge callee.
        let resp = reqwest::Client::new()
            .post(format!("{url}/invocations/{}/purge", hex(&callee)))
            .send()
            .await?;
        ensure!(resp.status().is_success(), "purge: {}", resp.status());

        // 4. Resend after the purge: today the callee runs again.
        post_inbound(&node, &msg).await?;
        common::wait_status(&url, &hex(&callee), "completed").await?;
        let runs = sdk.starts_of("Echo", "run").len();
        ensure!(
            runs == 2,
            "pinned dedup-window behaviour changed: a resend after the purge gave {runs} callee \
             turns (today: 2 — the target range creates the callee again). If deliberate, fix \
             this test and docs/guides/flow.md in the same commit"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
