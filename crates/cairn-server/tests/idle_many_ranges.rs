// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The idle cost of **many ranges** on one node: packing many ranges onto few
//! threads must not raise the per-range cost, and the **long-run marginal cost** of an
//! idle range — including every hourly and daily safety net — is close to 0.
//!
//! A separate file on purpose: opening hundreds of Dbs burns a few seconds of CPU, and
//! running next to the cluster tests of `idle_cost.rs` skews their lease cadence — measuring both at once.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{ensure, Result},
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{testkit::FaultyStore, ObjectStores, PollIntervals, StoreLocation},
    slatedb::object_store::memory::InMemory,
    std::{sync::Arc, time::Duration},
};

/// Builds a single-process node with `ranges` ranges and cadence `polls`, leaves it idle
/// for `warmup`, then counts every request to the store within `window`.
async fn idle_requests(
    ranges: u16,
    polls: PollIntervals,
    warmup: Duration,
    window: Duration,
) -> Result<u64> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"))
        .with_polls(polls);
    let node = node::start(NodeConfig::new(stores, NodeMode::single(ranges)))
        .await
        .map_err(|e| anyhow::anyhow!("build node with {ranges} ranges: {e}"))?;
    tokio::time::sleep(warmup).await;
    first_scrub_done(&faulty).await?;
    let before = faulty.counts();
    tokio::time::sleep(window).await;
    let d = faulty.counts().since(&before);
    node.shutdown().await;
    Ok(d.total())
}

/// Waits for the state scrubber's first round to finish: it runs within
/// [`cairn_server::gc::RUNNER_CHECK_EVERY`] after the node becomes leader
/// (like every range safety net, it fires first when the Db opens) and reads every
/// range; the next round comes a full period (600 s) later — outside the measuring
/// window. Done = there are `scrub` source requests and no more arrive within 500 ms.
async fn first_scrub_done(store: &FaultyStore) -> Result<()> {
    let scrub = || -> u64 {
        store
            .counts_by_source()
            .into_iter()
            .filter(|(src, ..)| *src == "scrub")
            .map(|(.., c)| c)
            .sum()
    };
    let t0 = std::time::Instant::now();
    let mut last = scrub();
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = scrub();
        if now > 0 && now == last {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(60),
            "first scrub round not done after 60 s ({now} requests)"
        );
        last = now;
    }
}

/// Cap of 0.1 requests/s per range, like the old cap of `idle_cost.rs`.
///
/// Every poll loop fires first when the Db opens, and next at the earliest after 60 s —
/// a 10 s window after 3 s of warm-up and the first scrub round ([`first_scrub_done`])
/// still falls between the two.
#[tokio::test(flavor = "multi_thread")]
async fn idle_256_ranges_keep_the_ceiling() -> Result<()> {
    const RANGES: u16 = 256;
    const WINDOW: Duration = Duration::from_secs(10);
    let total = idle_requests(
        RANGES,
        cairn_store::DEFAULT_POLLS,
        Duration::from_secs(3),
        WINDOW,
    )
    .await?;
    let rate = total as f64 / WINDOW.as_secs_f64() / f64::from(RANGES);
    ensure!(
        rate <= 0.1,
        "256 idle ranges: {rate:.3} requests/s per range (cap 0.1); total {total} requests in {WINDOW:?}"
    );
    Ok(())
}

/// **The long-run marginal cost of an idle range is ≤ 0.005 requests/s.**
///
/// The remaining safety nets of each range run hourly (manifest poll, coordinator,
/// worker, GC) — measuring them for real would need a window of many hours. So the test
/// **divides every cadence by 720** (`PollIntervals::scaled`: 1 h → 5 s), measures for
/// 20 s, then multiplies back: real cost ≈ measured cost / 720. Dividing every cadence
/// evenly keeps the ratios between them, and the requests per round do not depend on the
/// cadence, so multiplying back is right for every loop whose period ≤ the window.
///
/// **Marginal** cost, not average: two nodes run side by side, with 4 and 36
/// ranges; the difference divided by 32. The node's own share (LIST of the deployment
/// registry every 5 s, background ticks) cancels out — it does not scale with ranges,
/// and the goal is about ranges.
///
/// Loops longer than the window do not show up in the measurement: add them by hand.
/// Today there is only one — the fallback GC pass every 24 h of an unchanged Db
/// (~23 requests/24 h ≈ 0.0003/s).
///
/// Before: cadence 60 s / 300 s / 300 s / GC 1 h ⇒ ~0.064 requests/s per range.
#[tokio::test(flavor = "multi_thread")]
async fn idle_marginal_cost_per_range_long_run_is_near_zero() -> Result<()> {
    const SCALE: u32 = 720;
    const SMALL: u16 = 4;
    const BIG: u16 = 36;
    const WINDOW: Duration = Duration::from_secs(20);
    const LONGER_THAN_WINDOW: f64 = 23.0 / 86_400.0;
    let polls = cairn_store::DEFAULT_POLLS.scaled(SCALE);
    // A freshly opened Db runs two GC passes (at open, then one period later — the
    // "settling" pass that deletes garbage younger than `min_age` at the first pass)
    // before it skips passes; 12 s at a 5 s cadence lets every Db go through both before
    // the window. Those two passes are a one-off cost after each Db change, not the
    // long-run cost of an idle range.
    let warmup = Duration::from_secs(12);
    let (small, big) = tokio::join!(
        idle_requests(SMALL, polls, warmup, WINDOW),
        idle_requests(BIG, polls, warmup, WINDOW)
    );
    let (small, big) = (small?, big?);
    let marginal_scaled =
        (big as f64 - small as f64) / f64::from(BIG - SMALL) / WINDOW.as_secs_f64();
    let real = marginal_scaled / f64::from(SCALE) + LONGER_THAN_WINDOW;
    println!(
        "long-run marginal cost of an idle range ≈ {real:.5} requests/s \
         ({SMALL} ranges: {small}, {BIG} ranges: {big} requests in {WINDOW:?} at cadence ×1/{SCALE})"
    );
    ensure!(
        real <= 0.005,
        "long-run marginal cost of an idle range {real:.5} requests/s (cap 0.005); \
         {SMALL} ranges: {small}, {BIG} ranges: {big} requests in {WINDOW:?} at cadence ×1/{SCALE}"
    );
    Ok(())
}
