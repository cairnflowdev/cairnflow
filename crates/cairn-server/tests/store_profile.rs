// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The simulated-S3 store profile on a **real process**: a binary built with
//! `testkit` reads `CAIRNFLOW_STORE_PROFILE`, wraps the store per the profile,
//! and exposes request counters on `/metrics`. A build without `testkit` has no
//! such path (the `compile_fail` doctest on `NodeConfig`).

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
    common::{
        binary::Binary,
        fake_sdk::{FakeSdk, Step},
    },
    std::{path::PathBuf, time::Duration},
};

fn profile(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../bench/s3-model/profiles")
        .join(name)
        .display()
        .to_string()
}

fn args() -> Vec<&'static str> {
    vec![
        "--main-store",
        "memory://",
        "--port",
        "0",
        "--internal-port",
        "0",
        "--no-auth",
    ]
}

/// Profile `s3-standard` (latency + throttling): the node comes up, `/metrics`
/// has counters by operation and path kind, plus the throttling rule's numbers.
#[tokio::test(flavor = "multi_thread")]
async fn binary_with_profile_exposes_store_counts() -> Result<()> {
    let p = profile("s3-standard.json");
    let node = Binary::spawn(&args(), &[("CAIRNFLOW_STORE_PROFILE", p.as_str())])?;
    let text = reqwest::get(format!("{}/metrics", node.url))
        .await?
        .text()
        .await?;
    for want in [
        "cairnflow_testkit_store_requests_total{tier=\"main\",op=\"put\"",
        "cairnflow_testkit_store_requests_total{tier=\"main\",op=\"get\"",
        "cairnflow_testkit_store_puts_total{tier=\"main\",kind=\"create\"",
        "cairnflow_testkit_store_slowdown_total{tier=\"main\",what=\"rejected\"} 0",
    ] {
        assert!(
            text.contains(want),
            "missing {want}:\n{text}\n--- log\n{}",
            node.dump()
        );
    }
    assert!(node.dump().contains("s3-standard"), "{}", node.dump());
    Ok(())
}

/// No variable set ⇒ no wrapping, no `cairnflow_testkit_store_*` line at all.
#[tokio::test(flavor = "multi_thread")]
async fn binary_without_profile_does_not_wrap() -> Result<()> {
    let node = Binary::spawn(&args(), &[("CAIRNFLOW_STORE_PROFILE", "")])?;
    let text = reqwest::get(format!("{}/metrics", node.url))
        .await?
        .text()
        .await?;
    assert!(!text.contains("cairnflow_testkit_store"), "{text}");
    Ok(())
}

/// Number of calls of [`invoke_polls_at_most_twice_per_call_on_s3_standard`].
const POLL_CALLS: u64 = 12;

/// Sum of `cairnflow_invoke_polls_total` over every `answer` label.
async fn invoke_polls(url: &str) -> Result<u64> {
    let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
    Ok(text
        .lines()
        .filter(|l| l.starts_with("cairnflow_invoke_polls_total{"))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum())
}

/// At level 1 under the `s3-standard` profile, one `/invoke` asks the range
/// (`Command::Settled`) at most twice. The outcome notifier is the main path;
/// the poll grid is only a safety net. Counts the polls, does not measure time.
///
/// 1. A one-node cluster, 4 partitions, a binary carrying the `s3-standard`
///    profile.
/// 2. `Front.go` calls `Back.leaf` (`placement: "spread"`, crossing ranges) and
///    returns its result. The first turn suspends on the call, so `/invoke`
///    enters its wait loop.
/// 3. [`POLL_CALLS`] calls in sequence, each must return `200`.
///
/// Must see: total `cairnflow_invoke_polls_total` ≤ 2 × the number of calls.
/// Before the notifier, the 5 → 50 ms grid asked "not yet" several times within
/// the ~250 ms of one call, before the notifier woke the last poll.
#[tokio::test(flavor = "multi_thread")]
async fn invoke_polls_at_most_twice_per_call_on_s3_standard() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Front",
            "go",
            vec![Step::call_spread("Back", "leaf"), Step::EchoCallResult],
        )
        .service("Back", "leaf", vec![Step::output("leaf")])
        .spawn()
        .await?;
    let p = profile("s3-standard.json");
    let mut a = args();
    a.extend([
        "--cluster",
        "--partitions",
        "4",
        "--invoke-wait-ms",
        "30000",
    ]);
    let node = Binary::spawn(&a, &[("CAIRNFLOW_STORE_PROFILE", p.as_str())])?;
    let r = common::within(Duration::from_secs(120), "call across ranges", async {
        common::register(&node.url, &sdk.url).await?;
        let before = invoke_polls(&node.url).await?;
        for i in 0..POLL_CALLS {
            let (code, body) = common::invoke(&node.url, "Front", "go", None, "{}").await?;
            ensure!(
                code == 200 && body["output"] == "leaf",
                "call {i}: {code} {body}"
            );
        }
        let polls = invoke_polls(&node.url).await? - before;
        ensure!(
            polls <= 2 * POLL_CALLS,
            "{polls} polls for {POLL_CALLS} calls — more than 2 per call"
        );
        Ok(())
    })
    .await;
    if r.is_err() {
        eprintln!("--- log\n{}", node.dump());
    }
    r
}

/// A broken profile ⇒ the binary refuses to start (exit code 2), naming the
/// file. A measurement run by mistake without a profile is worse than no run.
#[test]
fn bad_profile_refuses_to_start() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("cairn-profile-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let bad = dir.join("bad.json");
    std::fs::write(&bad, r#"{"latency": {"get": {"p50_ms": 1, "p99ms": 2}}}"#)?;
    let bad = bad.display().to_string();
    let (status, out) = Binary::run_to_exit(
        &args(),
        &[("CAIRNFLOW_STORE_PROFILE", bad.as_str())],
        Duration::from_secs(20),
    )?;
    assert_eq!(status.code(), Some(2), "{out}");
    assert!(out.contains("bad.json"), "{out}");
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
