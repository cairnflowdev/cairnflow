// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Handler contract: timeout and retry policy declared in the manifest.
//!
//! Formerly the SDK wait limit was **one number for the whole cluster**
//! (`--inactivity-timeout-ms`), so a `ctx.run` longer than it (an LLM call, a slow
//! API) was cancelled and re-ran its side effect every time; and retry was
//! unbounded, giving up only through user code.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, Result},
    cairn_server::{
        discovery::{Contract, OnMaxAttempts},
        node::{self, NodeMode, RunningNode},
    },
    common::{
        fake_sdk::{FakeSdk, Step},
        register, send, test_config, wait_status, within, TEST_DEADLINE,
    },
    std::time::Duration,
};

async fn node_with_timeout(sdk_timeout: Duration) -> Result<RunningNode> {
    let mut cfg = test_config(NodeMode::single(1));
    cfg.limits.sdk_timeout = Some(sdk_timeout);
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("could not build the node: {e}"))
}

/// `handler_timeout_overrides_global`: the cluster limit is 300 ms; a handler that
/// declares `inactivityTimeout` 5 s and stalls 900 ms mid-turn ⇒ completes on the
/// first turn, not cancelled. A handler declaring nothing, same scenario ⇒ cancelled and re-run.
#[tokio::test]
async fn handler_timeout_overrides_global() -> Result<()> {
    let node = node_with_timeout(Duration::from_millis(300)).await?;
    let sdk = FakeSdk::builder()
        .object(
            "Slow",
            "patient",
            vec![Step::Stall(900), Step::output("ok")],
        )
        .object(
            "Slow",
            "default",
            vec![Step::Stall(900), Step::output("ok")],
        )
        .contract(
            "Slow",
            "patient",
            Contract {
                inactivity_timeout: Some(5_000),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    register(&node.url(), &sdk.url).await?;

    let (_, r) = send(&node.url(), "Slow", "patient", Some("a"), "{}").await?;
    let id = r["id"].as_str().unwrap().to_string();
    let done = within(
        TEST_DEADLINE,
        "patient completes",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    assert_eq!(done["output"], "ok");
    assert_eq!(
        sdk.starts_of("Slow", "patient").len(),
        1,
        "a handler declaring 5 s was still cancelled by the cluster's 300 ms limit"
    );

    // Control: nothing declared ⇒ the cluster limit applies, the turn is dropped and rescheduled.
    let (_, r) = send(&node.url(), "Slow", "default", Some("b"), "{}").await?;
    let _ = r["id"].as_str().unwrap();
    within(TEST_DEADLINE, "default is re-run", async {
        while sdk.starts_of("Slow", "default").len() < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    })
    .await?;
    node.shutdown().await;
    Ok(())
}

fn failing(max: u32, on: OnMaxAttempts) -> Contract {
    Contract {
        retry_policy_initial_interval: Some(20),
        retry_policy_max_interval: Some(20),
        retry_policy_max_attempts: Some(max),
        retry_policy_on_max_attempts: Some(on),
        ..Default::default()
    }
}

/// `retry_policy_pauses_after_max_attempts`: `maxAttempts = 5` ⇒ exactly 5
/// runs then `paused` (attempt 6 does not run); `resume` grants another round of 5.
#[tokio::test]
async fn retry_policy_pauses_after_max_attempts() -> Result<()> {
    let node = node_with_timeout(Duration::from_secs(5)).await?;
    let sdk = FakeSdk::builder()
        .object("Flaky", "go", vec![Step::FailRetry])
        .contract("Flaky", "go", failing(5, OnMaxAttempts::Pause))
        .spawn()
        .await?;
    register(&node.url(), &sdk.url).await?;

    let (_, r) = send(&node.url(), "Flaky", "go", Some("k"), "{}").await?;
    let id = r["id"].as_str().unwrap().to_string();
    let st = within(
        TEST_DEADLINE,
        "paused",
        wait_status(&node.url(), &id, "paused"),
    )
    .await?;
    // Wait a little longer: attempt 6 must **not** run.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(sdk.starts_of("Flaky", "go").len(), 5, "{st}");

    let resumed = reqwest::Client::new()
        .post(format!("{}/invocations/{id}/resume", node.url()))
        .send()
        .await?;
    assert!(resumed.status().is_success(), "{}", resumed.text().await?);
    within(TEST_DEADLINE, "round two", async {
        while sdk.starts_of("Flaky", "go").len() < 10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    })
    .await?;
    within(
        TEST_DEADLINE,
        "paused again",
        wait_status(&node.url(), &id, "paused"),
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        sdk.starts_of("Flaky", "go").len(),
        10,
        "resume must grant exactly one more round"
    );
    node.shutdown().await;
    Ok(())
}

/// A retry turn **longer than the backoff** does not make the backoff swell into the
/// slot wait limit.
///
/// Interleaving:
/// 1. The background tick fires a retry row due at `D`: it schedules the next attempt
///    first at `D' = now + 20 ms` (`reschedule_retry`), then hands over the retry turn
///    (`drive_retry`, holding the invocation lock).
/// 2. The turn stalls 300 ms in the SDK. Meanwhile the background tick sees row `D'`
///    due while the lock is held ⇒ defers it to `now + busy_defer_ms` (≥ 1 s,
///    default 10 s).
/// 3. The turn fails retryably. The lock is released, but the row already sits at the deferred due time.
///
/// Must see: the next attempt runs as soon as the previous turn releases the lock
/// (due `D'` has passed), so 3 attempts finish within a few seconds. Before the fix,
/// every attempt from the second on waited an extra `busy_defer_ms`: two deferrals
/// ⇒ ≥ 20 s. This was the root of the intermittent failure of
/// `retry_policy_pauses_after_max_attempts` on slow machines (a 20 ms turn failing
/// later than 20 ms).
#[tokio::test]
async fn retry_backoff_survives_attempt_longer_than_backoff() -> Result<()> {
    let node = node_with_timeout(Duration::from_secs(5)).await?;
    let sdk = FakeSdk::builder()
        .object("Slow", "go", vec![Step::Stall(300), Step::FailRetry])
        .contract("Slow", "go", failing(3, OnMaxAttempts::Pause))
        .spawn()
        .await?;
    register(&node.url(), &sdk.url).await?;

    let (_, r) = send(&node.url(), "Slow", "go", Some("k"), "{}").await?;
    let id = r["id"].as_str().unwrap().to_string();
    // 3 turns × 300 ms plus the background tick: a few seconds is plenty. One deferral is ≥ 10 s.
    within(
        Duration::from_secs(6),
        "paused (backoff 20 ms, turn 300 ms)",
        wait_status(&node.url(), &id, "paused"),
    )
    .await?;
    assert_eq!(sdk.starts_of("Slow", "go").len(), 3);
    node.shutdown().await;
    Ok(())
}

/// `KILL`: once attempts run out the invocation ends with an error, not hanging.
#[tokio::test]
async fn retry_policy_kill_fails_after_max_attempts() -> Result<()> {
    let node = node_with_timeout(Duration::from_secs(5)).await?;
    let sdk = FakeSdk::builder()
        .object("Flaky", "go", vec![Step::FailRetry])
        .contract("Flaky", "go", failing(2, OnMaxAttempts::Kill))
        .spawn()
        .await?;
    register(&node.url(), &sdk.url).await?;
    let (_, r) = send(&node.url(), "Flaky", "go", Some("k"), "{}").await?;
    let id = r["id"].as_str().unwrap().to_string();
    within(
        TEST_DEADLINE,
        "failed",
        wait_status(&node.url(), &id, "failed"),
    )
    .await?;
    assert_eq!(sdk.starts_of("Flaky", "go").len(), 2);
    node.shutdown().await;
    Ok(())
}

/// An invalid contract is rejected **at registration**, not at run time.
#[tokio::test]
async fn bad_contract_is_rejected_at_registration() -> Result<()> {
    let node = node_with_timeout(Duration::from_secs(5)).await?;
    let sdk = FakeSdk::builder()
        .object("Bad", "go", vec![Step::output("x")])
        .contract(
            "Bad",
            "go",
            Contract {
                retry_policy_max_attempts: Some(0),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    assert!(register(&node.url(), &sdk.url).await.is_err());
    node.shutdown().await;
    Ok(())
}
