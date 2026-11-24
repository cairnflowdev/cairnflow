// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! State read before a point where the engine is released, then used after
//! taking it back.
//!
//! Each test here is a place where a decision was made on stale data, or where
//! an in-RAM resource was not given back on a rare path.

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
    cairn_engine::{CompletionPayload, DeliverCompletion, InterPartition},
    common::{
        fake_sdk::{FakeSdk, Step},
        TEST_DEADLINE,
    },
    std::time::Duration,
};

fn unhex(s: &str) -> [u8; 24] {
    let mut out = [0u8; 24];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("id hex");
    }
    out
}

/// A late cross-partition completion for an invocation that has **already
/// completed** must not make it run again.
///
/// `deliver_inbound_completion` did not check the status: it loaded, saw a
/// completion never delivered before, then `drive` — that is, it put an
/// invocation that had already ended back in the queue and called the SDK for
/// one more turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_inbound_completion_does_not_rerun_completed_invocation() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "late completion", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::invoke(&url, "Echo", "run", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        ensure!(sdk.starts_of("Echo", "run").len() == 1);

        let late = InterPartition::Completion(DeliverCompletion {
            target: unhex(&id),
            completion_id: 99,
            result: CompletionPayload::Value(Bytes::from_static(b"\"late\"")),
        });
        let resp =
            common::post_internal(&node, "/internal/inbound", late.encode().to_vec()).await?;
        ensure!(resp.status().is_success(), "inbound: {}", resp.status());

        // A few ticks (50ms) for the inbound row to be processed.
        tokio::time::sleep(Duration::from_millis(800)).await;
        let starts = sdk.starts_of("Echo", "run").len();
        ensure!(
            starts == 1,
            "completed invocation got {} extra SDK turns from a late completion",
            starts - 1
        );
        let (_, st) = common::invocation(&url, &id).await?;
        ensure!(st["status"] == "completed", "{st}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// A cancel propagated down to an `EXCLUSIVE` child must **give back its VO
/// lock**.
///
/// An admin-level cancel dequeues and then releases the lock; a propagated
/// cancel (`cancel_child`) did not — the lock stayed in the scheduler's RAM
/// until the next leader change, and every other `EXCLUSIVE` call on the same
/// key waited for a dead invocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_child_releases_vo_lock() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Parent",
            "run",
            vec![
                Step::call("Child", "work", Some("k1")),
                Step::EchoCallResult,
            ],
        )
        .object("Child", "work", vec![Step::Suspend])
        .object("Child", "ping", vec![Step::output("pong")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "propagated cancel releases VO lock", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::send(&url, "Parent", "run", None, "{}").await?;
        ensure!(code == 202, "{code} {body}");
        let parent = body["id"].as_str().unwrap_or_default().to_string();

        // The child has run and is suspended — so it holds the VO lock of `k1`.
        let child = loop {
            if let Some(s) = sdk.starts_of("Child", "work").first() {
                break s.id_hex();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        common::wait_status(&url, &child, "suspended").await?;

        let resp = reqwest::Client::new()
            .post(format!("{url}/invocations/{parent}/cancel"))
            .send()
            .await?;
        ensure!(resp.status().is_success(), "cancel: {}", resp.status());
        common::wait_status(&url, &child, "failed").await?;

        let (code, body) = common::invoke(&url, "Child", "ping", Some("k1"), "{}").await?;
        ensure!(
            code == 200 && body["output"] == "pong",
            "a new EXCLUSIVE call on k1 cannot run ({code} {body}) — the VO lock of the \
             cancelled child was not given back"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Each invocation gets its own `random_seed`.
///
/// The seed used to be `id[..8]`, and for an id created over HTTP the first 8
/// bytes are 2 bytes of partition + 6 bytes of the process's start time —
/// identical for every invocation of the same partition in the same process.
/// Every `Math.random()` seeded from it produced the same sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_invocations_get_distinct_random_seeds() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "own seed", async {
        common::register(&url, &sdk.url).await?;
        for _ in 0..2 {
            let (code, body) = common::invoke(&url, "Echo", "run", None, "{}").await?;
            ensure!(code == 200, "{code} {body}");
        }
        let starts = sdk.starts_of("Echo", "run");
        ensure!(starts.len() == 2, "{starts:?}");
        ensure!(
            starts[0].random_seed != starts[1].random_seed,
            "two different invocations got the same random_seed {}",
            starts[0].random_seed
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// The outbox drainer **must not wait for the lock of a running invocation**.
///
/// Cancelling a parent whose child is in the middle of a turn (the child's SDK
/// is held at a gate) puts a `Cancel` row for the child at the head of the
/// outbox. The old drainer waited for the child's lock — that is, for its whole
/// SDK turn — and meanwhile no other row of the whole partition went out: even
/// an unrelated `/invoke` stood behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_of_busy_child_does_not_block_unrelated_dispatch() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Parent",
            "run",
            vec![
                Step::call("Child", "work", Some("c1")),
                Step::EchoCallResult,
            ],
        )
        .gated_object("Child", "work", vec![Step::output("late")])
        .service(
            "Y",
            "run",
            vec![Step::call("R", "run", None), Step::EchoCallResult],
        )
        .service("R", "run", vec![Step::output("r")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let gate = sdk.gate();

    let r = common::within(
        TEST_DEADLINE,
        "drainer does not wait for the lock of an in-flight turn",
        async {
            common::register(&url, &sdk.url).await?;
            let (code, body) = common::send(&url, "Parent", "run", None, "{}").await?;
            ensure!(code == 202, "{code} {body}");
            let parent = body["id"].as_str().unwrap_or_default().to_string();

            // The child has entered its turn and stands at the gate — holding its invocation lock.
            while sdk.starts_of("Child", "work").is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // Cancel the parent in its own task: the cancel waits for the drainer, and that is not what we measure.
            let cancel_url = format!("{url}/invocations/{parent}/cancel");
            let cancel =
                tokio::spawn(async move { reqwest::Client::new().post(cancel_url).send().await });
            tokio::time::sleep(Duration::from_millis(300)).await;

            let started = std::time::Instant::now();
            let (code, body) = common::invoke(&url, "Y", "run", None, "{}").await?;
            ensure!(
                code == 200 && body["output"] == "r",
                "an unrelated call stood behind a drainer waiting for the lock of an in-flight turn \
             ({code} {body}, sau {:?})",
                started.elapsed()
            );

            gate.open();
            let _ = cancel.await;
            Ok(())
        },
    )
    .await;
    gate.open();
    node.shutdown().await;
    r
}

/// **`DeploymentsChanged` does not dispatch again an invocation that has a turn
/// in flight.**
///
/// Interleaving:
/// 1. A lone node. `Held/run` (SDK gate closed, then suspends waiting for a
///    signal): the first turn reaches the SDK and stands at the gate — the
///    invocation is `running`, the turn holds its lock.
/// 2. Register the deployment again: `POST /deployments` reports
///    `DeploymentsChanged` to the engine.
/// 3. Open the gate: the first turn suspends (`suspended`).
///
/// Must see: the SDK receives exactly **one** `Start` for the invocation. Before
/// the fix the command ran a bare `recover` — scanning `running` and dispatching
/// again even an invocation with a turn in flight; the duplicate task waited for
/// the lock, then ran one more SDK turn after the first one suspended
/// (`gate_turn` only blocks terminal and `paused`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deployments_changed_does_not_rerun_an_invocation_mid_turn() -> Result<()> {
    let sdk = FakeSdk::builder()
        .gated_service("Held", "run", vec![Step::Suspend])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = common::within(TEST_DEADLINE, "re-register mid-turn", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::send(&url, "Held", "run", None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        while sdk.starts_of("Held", "run").is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        common::register(&url, &sdk.url).await?;
        sdk.gate().open();
        common::wait_status(&url, &id, "suspended").await?;
        // The duplicate turn (if any) waits for the first turn's lock and runs as soon as it is released:
        // a bounded poll.
        for _ in 0..30 {
            let starts = sdk.starts_of("Held", "run").len();
            ensure!(
                starts == 1,
                "invocation with a turn in flight dispatched again: {starts} Start"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
