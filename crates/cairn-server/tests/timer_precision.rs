// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Timers fire on their due time, not on the tick.
//!
//! The background loop only scans timers on each tick (200 ms by default), so a
//! `ctx.sleep` wakes up to one tick late — being more precise would mean
//! scanning more often, costing CPU and store reads for every range, idle ones
//! included. The engine knows the nearest due time (it has just written the
//! timer, and the previous scan saw the next row), so the background loop
//! sleeps until exactly that time; the remaining tick serves as a net.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeMode},
    common::{
        fake_sdk::{FakeSdk, Step},
        invoke, register, test_config, within,
    },
    std::time::Duration,
};

/// The binary's default background tick — a safety net, not the precision.
const TICK: Duration = Duration::from_millis(200);
/// The handler's sleep time: more than one tick, so the timer cannot fire "by
/// chance" on the first tick after it was written.
const NAP_MS: u64 = 300;
/// Allowed **median** lateness, measured until the SDK gets the turn back —
/// including the commit that delivers the completion and the HTTP turn, not
/// only the moment the background loop wakes.
const SLACK: Duration = Duration::from_millis(20);
/// Limit for **each** run. Much wider than the median: under the parallel load
/// of the whole `cargo test --workspace`, an odd run is a few tens of ms late
/// because of the CPU — not because of the tick. This limit is still half a
/// tick, so a timer that waits for the tick still exceeds it.
const WORST: Duration = Duration::from_millis(100);
const RUNS: u64 = 9;

/// `ctx.sleep(300 ms)` with a 200 ms tick: the turn after the sleep reaches the
/// SDK within 20 ms of the due time — the **median** over many runs (each at a
/// different phase relative to the tick) ≤ 20 ms, and no run over 100 ms.
/// Before due-time sleeping the lateness was spread evenly over the whole tick
/// (measured 50–196 ms); after it, around 5–12 ms, mostly the commit's two
/// durability waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_fires_within_20ms_of_due() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Napper",
            "nap",
            vec![Step::Sleep(NAP_MS), Step::output("ok")],
        )
        .spawn()
        .await?;
    let mut cfg = test_config(NodeMode::single(1));
    cfg.tick_interval = Some(TICK);
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("cannot build the node: {e}"))?;
    let url = node.url();

    let r = within(Duration::from_secs(60), "the sleeps", async {
        register(&url, &sdk.url).await?;
        let mut lateness = Vec::new();
        for i in 0..RUNS {
            // Shift the phase relative to the tick between runs: otherwise every
            // run lands at the same place in the tick and the measured max is a single point.
            tokio::time::sleep(Duration::from_millis(37 * i)).await;
            let (code, body) = invoke(&url, "Napper", "nap", None, "").await?;
            ensure!(
                code == 200 && body["output"] == "ok",
                "run {i}: {code} {body}"
            );
            let id = body["id"]
                .as_str()
                .ok_or_else(|| anyhow!("no id: {body}"))?;
            let starts: Vec<_> = sdk
                .starts_of("Napper", "nap")
                .into_iter()
                .filter(|s| s.id_hex() == id)
                .collect();
            ensure!(
                starts.len() == 2,
                "run {i}: needs exactly two turns (sleep, wake): {starts:?}"
            );
            // Due time = when the fake SDK emits `Sleep` in the first turn + NAP_MS (it
            // computes `wake_up_time` right after receiving `Start`).
            let due = starts[0].at + Duration::from_millis(NAP_MS);
            let late = starts[1]
                .at
                .duration_since(due)
                .map_err(|_| anyhow!("run {i}: woke **before** due — fired early"))?;
            lateness.push(late);
        }
        Ok(lateness)
    })
    .await;
    node.shutdown().await;
    let lateness = r?;
    eprintln!("lateness after the due time: {lateness:?}");
    let mut sorted = lateness.clone();
    sorted.sort();
    let median = sorted[sorted.len() / 2];
    let worst = sorted[sorted.len() - 1];
    ensure!(
        median <= SLACK,
        "median lateness {median:?} after the due time (allowed {SLACK:?}, tick {TICK:?}): {lateness:?}"
    );
    ensure!(
        worst <= WORST,
        "one run was {worst:?} late (limit {WORST:?}, tick {TICK:?}): {lateness:?}"
    );
    Ok(())
}
