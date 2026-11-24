// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-range flush windows through the API (D-h): the operator
//! sets a range's window, every node applies it after one registry refresh tick — that
//! range's commits follow its grid.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
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
        invoke, register, within,
    },
    std::time::{Duration, Instant},
};

async fn three_invokes(url: &str) -> Result<Duration> {
    let t0 = Instant::now();
    for i in 0..3 {
        let (code, body) = invoke(url, "Win", "go", Some(&format!("k{i}")), "{}").await?;
        ensure!(code == 200, "invoke: {code} {body}");
    }
    Ok(t0.elapsed())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_window_set_by_api_slows_that_range() -> Result<()> {
    let shared = common::bucket();
    let nodes = vec![common::start_cluster_node(&shared, "a", 1, Duration::from_secs(2)).await?];
    let node = &nodes[0].node;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Win", "go", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "per-range window", async {
        common::wait_probe(&[&nodes[0]], Duration::from_secs(20), "has a leader", |p| {
            common::single_leaders(p).is_some_and(|m| !m.is_empty())
        })
        .await?;
        register(&url, &sdk.url).await?;
        let base = three_invokes(&url).await?;
        let range = node.cluster.partitions()[0].0;
        let http = reqwest::Client::new();
        let bad = http
            .put(format!("{url}/ranges/{range}/window"))
            .json(&serde_json::json!({ "ms": 0 }))
            .send()
            .await?;
        ensure!(
            bad.status() == 400,
            "ms = 0 must be refused: {}",
            bad.status()
        );
        let ok = http
            .put(format!("{url}/ranges/{range}/window"))
            .json(&serde_json::json!({ "ms": 300 }))
            .send()
            .await?;
        ensure!(ok.status().is_success(), "{}", ok.text().await?);
        let listed: serde_json::Value = http
            .get(format!("{url}/ranges/windows"))
            .send()
            .await?
            .json()
            .await?;
        ensure!(listed[range.to_string()] == 300, "{listed}");
        // The node applies it after one registry refresh tick (default 5 s).
        let mut slow = Duration::ZERO;
        for _ in 0..20 {
            slow = three_invokes(&url).await?;
            if slow > Duration::from_millis(600) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        ensure!(
            slow > Duration::from_millis(600),
            "the 300 ms window was not applied: {slow:?} (baseline {base:?})"
        );
        http.put(format!("{url}/ranges/{range}/window"))
            .json(&serde_json::json!({ "ms": null }))
            .send()
            .await?;
        let mut back = slow;
        for _ in 0..20 {
            back = three_invokes(&url).await?;
            if back < slow / 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        ensure!(
            back < slow / 2,
            "window removed but still slow: {back:?} vs {slow:?}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
