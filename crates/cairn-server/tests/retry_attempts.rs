// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `ctx.attempt` (`retry_count_since_last_stored_entry`) must be **monotonic across
//! leader changes**.
//!
//! The attempt count lives durably in the invocation's retry row, and the next
//! scheduled retry is set **before** running — so
//! after a leader change, the right thing is to let that scheduled retry fire with its count. The
//! former code let `recover` re-run every unfinished invocation at once with count 0: every
//! leader change (failover, and with range splits every split/merge) wiped the
//! attempt count, so an SDK's `maxRetryAttempts` never hit its limit if the cluster
//! changed leaders often enough — and the backoff went back to its first step too.

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
    cairn_server::node::{self, NodeConfig, NodeMode, RunningNode},
    cairn_store::{ObjectStores, StoreLocation},
    common::fake_sdk::{FakeSdk, Step},
    slatedb::object_store::memory::InMemory,
    std::{sync::Arc, time::Duration},
};

async fn start_on(bucket: &Arc<InMemory>) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("build node: {e}"))
}

/// One failed turn, two failed retries, a **leader change** (the node stops, another node
/// opens the same store), then the next attempt: the count must continue, not return to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attempt_count_survives_leader_handoff() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Flaky", "always", vec![Step::FailRetry])
        .spawn()
        .await?;

    let a = start_on(&bucket).await?;
    let attempts = |sdk: &FakeSdk| -> Vec<u32> {
        sdk.starts_of("Flaky", "always")
            .into_iter()
            .map(|s| s.attempt)
            .collect()
    };
    let before = common::within(
        Duration::from_secs(20),
        "three attempts on the first node",
        async {
            common::register(&a.url(), &sdk.url).await?;
            let (code, body) = common::send(&a.url(), "Flaky", "always", None, "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            loop {
                let seen = attempts(&sdk);
                if seen.len() >= 3 {
                    return Ok(seen);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await;
    a.shutdown().await;
    let before = before?;
    ensure!(
        before[..3] == [0, 1, 2],
        "before the leader change the count must be 0,1,2: {before:?}"
    );
    let last = *before.last().unwrap();

    // Leader change: a new node, the same store.
    let b = start_on(&bucket).await?;
    let after = common::within(
        Duration::from_secs(20),
        "first attempt on the new node",
        async {
            loop {
                let seen = attempts(&sdk);
                if seen.len() > before.len() {
                    return Ok(seen);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await;
    b.shutdown().await;
    let after = after?;
    let next = after[before.len()];
    ensure!(
        next > last,
        "the leader change wiped the attempt count: before {before:?}, the first after the change is {next} (all {after:?})"
    );
    ensure!(
        after.windows(2).all(|w| w[0] <= w[1]),
        "ctx.attempt must be monotonic: {after:?}"
    );
    Ok(())
}

/// **The first run sees `attempt` 0 even though the server closed the connection to the SDK**
/// (the TS e2e `ctx.attempt: …` failed intermittently in the
/// `REQUEST_RESPONSE` row, sequences `[1,0,0,0,1]` / `[1,2]`).
///
/// `ctx.attempt` is "0 on the first run" by the SDK contract. The runtime's h1
/// client (`REQUEST_RESPONSE`, discovery) keeps idle connections longer than the SDK server
/// does: `node:http` closes idle connections after 5 s (`keepAliveTimeout`).
/// Interleaving:
/// 1. Invocation A runs; the client keeps the connection in its pool.
/// 2. The connection exceeds the SDK server's idle limit (5 s).
/// 3. Invocation B: the client sends `/invoke` on the old connection just as the server closes
///    it (the fake SDK drops a request arriving on a connection idle for ≥ 5 s — the worst shape
///    of the race). The turn fails at the transport layer, **before** the SDK runs anything,
///    and the scheduled retry carries `attempt` 1.
///
/// Must see: B returns 200 (before the fix: the synchronous `/invoke` gave **502** "SDK
/// call failed: error sending request" for a healthy SDK) and B's first entry into the
/// handler carries `attempt` 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_run_sees_attempt_zero_after_sdk_closed_an_idle_connection() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Svc", "first", vec![Step::output("1")])
        .service("Svc", "second", vec![Step::output("2")])
        .idle_close(Duration::from_secs(5))
        .spawn()
        .await?;
    let node = start_on(&bucket).await?;
    let res = common::within(Duration::from_secs(30), "two invocations", async {
        common::register(&node.url(), &sdk.url).await?;
        let (code, body) = common::invoke(&node.url(), "Svc", "first", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        // The connection of the turn above exceeds the SDK server's idle limit — the scenario
        // itself, not a wait "to be sure".
        tokio::time::sleep(Duration::from_millis(5_300)).await;
        let (code, body) = common::invoke(&node.url(), "Svc", "second", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        Ok(sdk
            .starts_of("Svc", "second")
            .into_iter()
            .map(|s| s.attempt)
            .collect::<Vec<_>>())
    })
    .await;
    node.shutdown().await;
    let seen = res?;
    ensure!(
        seen.first() == Some(&0),
        "the handler's first run must see attempt 0, saw {seen:?}"
    );
    Ok(())
}

/// **`paused` 575 reports the last attempt's error**, and `GET /invocations/{id}`
/// returns a structured `lastFailure` — even when the leader changes between the last attempt and
/// the "attempts exhausted" check.
///
/// Order:
/// 1. Node `a`. `Flaky/limited` always fails retryably, with a message carrying the attempt
///    number (`… attempt N`); `retryPolicyMaxAttempts` 2, first backoff 1.5 s.
/// 2. The SDK receives attempt 0 then attempt 1 (both fail); attempt 1's failure
///    is durable together with the next scheduled retry (1.5 s later) — a `GET` on `a` sees its `lastFailure`.
/// 3. **Leader change** in that gap: `a` shuts down gracefully, `b` opens the same bucket.
/// 4. The scheduled retry fires on `b`: preflight sees 2 attempts used ⇒ `paused` 575, the SDK
///    is not called a third time.
///
/// Must see: the SDK receives exactly two attempts; the 575 `message` contains the message of
/// attempt 1; `lastFailure` = `{code 500, message: attempt 1's message, attempt 1}`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_after_max_attempts_shows_the_last_attempts_error() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Flaky", "limited", vec![Step::FailRetry])
        .contract(
            "Flaky",
            "limited",
            cairn_server::discovery::Contract {
                retry_policy_max_attempts: Some(2),
                retry_policy_initial_interval: Some(1_500),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let attempts = |sdk: &FakeSdk| -> Vec<u32> {
        sdk.starts_of("Flaky", "limited")
            .into_iter()
            .map(|s| s.attempt)
            .collect()
    };
    let a = start_on(&bucket).await?;
    let sent = common::within(Duration::from_secs(20), "two attempts on a", async {
        common::register(&a.url(), &sdk.url).await?;
        let (code, body) = common::send(&a.url(), "Flaky", "limited", None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        // Attempt 1's failure was written with the next scheduled retry: its `lastFailure`
        // shows on `a` (the invocation still `running`, waiting for the retry) — then shut down.
        loop {
            let (_, st) = common::invocation(&a.url(), &id).await?;
            if st["lastFailure"]["attempt"] == 1 {
                break;
            }
            ensure!(
                attempts(&sdk).len() <= 2,
                "SDK received more than two attempts on a: {st}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(id)
    })
    .await;
    a.shutdown().await;
    let id = sent?;

    let b = start_on(&bucket).await?;
    let r = common::within(Duration::from_secs(20), "paused 575 on b", async {
        let st = common::wait_status(&b.url(), &id, "paused").await?;
        ensure!(st["code"] == 575, "must be 575: {st}");
        ensure!(
            attempts(&sdk) == [0, 1],
            "the SDK must receive exactly two attempts: {:?}",
            attempts(&sdk)
        );
        let last = "transient failure (fake SDK, attempt 1)";
        let message = st["message"].as_str().unwrap_or_default();
        ensure!(
            message.contains(last),
            "the 575 message does not report the last attempt's error: {st}"
        );
        let lf = &st["lastFailure"];
        ensure!(
            lf["code"] == 500 && lf["message"] == last && lf["attempt"] == 1,
            "wrong lastFailure: {st}"
        );
        ensure!(
            lf["atMs"].as_u64().is_some_and(|t| t > 0),
            "missing atMs: {st}"
        );
        Ok(())
    })
    .await;
    b.shutdown().await;
    r
}

/// **A backing-off invocation says when it tries next**: `lastFailure.nextAttemptAtMs` of
/// `GET /invocations/{id}` is the due time of the pending scheduled retry.
///
/// Order:
/// 1. `Flaky/later` always fails retryably; `retryPolicyMaxAttempts` 2, first backoff 3 s.
/// 2. Attempt 0 fails: the invocation is `running` with a pending retry (backing off). Its
///    `lastFailure` carries `nextAttemptAtMs`, after the failure's `atMs` and in the future.
/// 3. Attempt 1 fails, the attempts run out ⇒ `paused` 575: `lastFailure` is still there, but
///    without `nextAttemptAtMs` — nothing is scheduled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backing_off_reports_when_the_next_attempt_is_due() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Flaky", "later", vec![Step::FailRetry])
        .contract(
            "Flaky",
            "later",
            cairn_server::discovery::Contract {
                retry_policy_max_attempts: Some(2),
                retry_policy_initial_interval: Some(3_000),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let a = start_on(&bucket).await?;
    let r = common::within(Duration::from_secs(30), "backing off, then paused", async {
        common::register(&a.url(), &sdk.url).await?;
        let (code, body) = common::send(&a.url(), "Flaky", "later", None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let st = loop {
            let (_, st) = common::invocation(&a.url(), &id).await?;
            if st["lastFailure"]["attempt"] == 0 {
                break st;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        ensure!(st["status"] == "running", "must be backing off: {st}");
        let lf = &st["lastFailure"];
        let at = lf["atMs"].as_u64().unwrap_or_default();
        let next = lf["nextAttemptAtMs"]
            .as_u64()
            .ok_or_else(|| anyhow!("a backing-off invocation lacks nextAttemptAtMs: {st}"))?;
        ensure!(
            next > at,
            "the next attempt must come after the failure: {st}"
        );
        ensure!(
            next > now_ms,
            "the next attempt must be in the future (now {now_ms}): {st}"
        );

        let st = common::wait_status(&a.url(), &id, "paused").await?;
        ensure!(st["code"] == 575, "must be 575: {st}");
        ensure!(
            st["lastFailure"]["attempt"] == 1,
            "lastFailure must be attempt 1's: {st}"
        );
        ensure!(
            st["lastFailure"].get("nextAttemptAtMs").is_none(),
            "a paused invocation has no next attempt: {st}"
        );
        Ok(())
    })
    .await;
    a.shutdown().await;
    r
}
