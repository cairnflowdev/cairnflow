// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The node's replay RAM budget: the total journal bytes that the SDK turns
//! running now hold in RAM is capped (`--replay-ram-budget-bytes`,
//! `cairn_server::replay_budget::NODE`).
//!
//! The interleaving reproduced:
//!
//! 1. A limit of 1 byte. `a`'s turn loads its journal, takes a reservation (an empty budget
//!    always grants), calls the SDK — the SDK stalls 1.5 s.
//! 2. `b`'s turn arrives meanwhile: the budget is held, there is not enough room
//!    ⇒ **the SDK is not called**, rescheduled after 1 s, the attempt unchanged.
//! 3. `a`'s turn completes, returns its reservation; `b`'s rescheduled turn runs.
//!
//! Must see: both `completed` (nobody blocked forever, no attempt
//! counted), the metric `cairnflow_replay_budget_throttled_total` > 0, and
//! the budget back to 0 once the turns are over.
//!
//! A separate file, one test: `NODE` belongs to the whole process.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    common::{
        fake_sdk::{FakeSdk, Step},
        within, TEST_DEADLINE,
    },
    std::time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_budget_reschedules_without_calling_sdk() -> Result<()> {
    let budget = &cairn_server::replay_budget::NODE;
    budget.set_cap(1);
    let sdk = FakeSdk::builder()
        .object("Slow", "run", vec![Step::Stall(1_500), Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    within(TEST_DEADLINE, "replay budget", async {
        common::register(&url, &sdk.url).await?;
        let (code, a) = common::send(&url, "Slow", "run", Some("a"), "{}").await?;
        ensure!(code == 202 || code == 200, "a: {code} {a}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (code, b) = common::send(&url, "Slow", "run", Some("b"), "{}").await?;
        ensure!(code == 202 || code == 200, "b: {code} {b}");
        for v in [&a, &b] {
            let id = v["id"].as_str().unwrap_or_default();
            let st = common::wait_status(&url, id, "completed").await?;
            ensure!(st["lastFailure"].is_null(), "no attempt failed: {st}");
        }
        // The SDK sees only **one** `Start` per key, attempt 0: the rescheduled turn
        // does not call the SDK, does not count an attempt. And `b`'s `Start` arrives after
        // `a`'s turn (stalling 1.5 s) has completed — the two turns never hold RAM together.
        let starts = sdk.starts_of("Slow", "run");
        let of = |k: &str| starts.iter().filter(|s| s.key == k).collect::<Vec<_>>();
        let (sa, sb) = (of("a"), of("b"));
        ensure!(
            sa.len() == 1 && sb.len() == 1 && sa[0].attempt == 0 && sb[0].attempt == 0,
            "a: {} times, b: {} times",
            sa.len(),
            sb.len()
        );
        let gap = sb[0].at.duration_since(sa[0].at).unwrap_or_default();
        ensure!(
            gap >= Duration::from_millis(1_500),
            "b ran overlapping a: {gap:?} apart"
        );
        ensure!(budget.throttled() > 0, "b's turn must be rescheduled");
        let m = common::metric(&url, "cairnflow_replay_budget_throttled_total").await?;
        ensure!(m > 0, "metric: {m}");
        let t0 = std::time::Instant::now();
        while budget.used() != 0 {
            ensure!(
                t0.elapsed() < Duration::from_secs(5),
                "the budget did not return to 0"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    })
    .await
}
