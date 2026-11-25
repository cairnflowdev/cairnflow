// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Baseline benches: timer storm and permit queue. Not a pass/fail test:
//! `#[ignore]`, prints a table. Run by hand:
//!
//! ```sh
//! TIMER_N=1000,10000,100000 cargo test --release -p cairn-server --test arch_bench \
//!   timer_storm_same_due -- --ignored --nocapture
//! PERMIT_W=1000,10000,100000 cargo test --release -p cairn-server --test arch_bench \
//!   permit_queue_churn -- --ignored --nocapture
//! ```
//!
//! One node, one range on `memory://` wrapped in `FaultyStore`; invocations are
//! built through the real path (`/internal/inbound/batch` → create → fake SDK
//! turn). Engine hold time is read from `/metrics`
//! (`cairnflow_range_engine_hold_ms_total`, `_hold_max_ms`, `_holds_total` by
//! `holder`). `memory://` means no network is measured: the numbers are the
//! CPU/RAM cost and the number of engine holds, not S3 latency.

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
        bench,
        fake_sdk::{FakeSdk, Step},
    },
    slatedb::object_store::memory::InMemory,
    std::{sync::Arc, time::Duration},
};

/// An unlabelled node gauge from `/metrics` (0 if absent — an older binary).
async fn gauge(url: &str, name: &str) -> Result<f64> {
    let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
    let prefix = format!("{name} ");
    Ok(text
        .lines()
        .find_map(|l| l.strip_prefix(&prefix)?.trim().parse().ok())
        .unwrap_or(0.0))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// **Timer storm**: N invocations all sleep until **one** absolute deadline on
/// one range.
///
/// Deadline = start + 5 s + `TIMER_MARGIN_MS_PER_1K` (default 2000) ms per
/// thousand invocations — enough for the first turn of all N to finish before
/// the deadline (the test checks, and reports if not). Measures: the longest
/// engine hold **before** and **after** the deadline (`tick` is the holder of
/// the timer loop), the time from the deadline until the SDK has received the
/// wake-up turn of all N, peak RSS during delivery.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run by hand"]
async fn timer_storm_same_due() -> Result<()> {
    let ns = bench::levels("TIMER_N", &[1_000, 10_000]);
    let per_k: u64 = std::env::var("TIMER_MARGIN_MS_PER_1K")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    println!(
        "\n  {:>7} | {:>8} | {:>11} | {:>16} | {:>16} | {:>9} | {:>9} | engine holds after deadline (holder total/longest ms)",
        "N", "build s", "deliver ms", "max before", "max after", "RSS MiB", "peak MiB"
    );
    for n in ns {
        let store = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
        let t0 = now_ms();
        let due = t0 + 5_000 + n as u64 * per_k / 1000;
        let sdk = FakeSdk::builder()
            .service(
                "Timer",
                "run",
                vec![Step::SleepUntil(due), Step::output("ok")],
            )
            .spawn()
            .await?;
        let node = common::start_led(bench::config(&store)).await?;
        common::register(&node.url(), &sdk.url).await?;
        // In waves, polling by count (`bench::build`): sending all N at once and
        // copying the `Start` list on every poll makes 10⁵ miss the deadline.
        bench::build(&node, &sdk, 3, "Timer", n, 10_000, "suspended").await?;
        ensure!(
            now_ms() < due,
            "first turn of {n} invocations not done before the deadline — raise TIMER_MARGIN_MS_PER_1K"
        );
        // The first turn has run; wait for the last invocation to suspend (commit `suspended`).
        common::wait_status(&node.url(), &bench::hex(&bench::id(3, n - 1)), "suspended").await?;
        let built = (now_ms() - t0) as f64 / 1000.0;
        ensure!(
            now_ms() < due,
            "built after the deadline — raise TIMER_MARGIN_MS_PER_1K"
        );
        let before = bench::Holds::read(&node.url()).await?;
        let rss_before = bench::rss_mib();
        tokio::time::sleep(Duration::from_millis(due - now_ms())).await;
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let peak = bench::rss_peak(stopped);
        let t_due = std::time::Instant::now();
        while sdk.starts_count_of("Timer", "run") < 2 * n {
            ensure!(
                t_due.elapsed() < Duration::from_secs(1800),
                "{n} timers not all delivered after 1800 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let delivered = t_due.elapsed();
        let _ = stop.send(true);
        let peak = peak.await?;
        let after = bench::Holds::read(&node.url()).await?;
        let (bh, bm) = before.max();
        let (ah, am) = after.max();
        println!(
            "  {:>7} | {:>8.1} | {:>11} | {:>16} | {:>16} | {:>9.0} | {:>9.0} | {}",
            n,
            built,
            delivered.as_millis(),
            format!("{bm:.0} ({bh})"),
            format!("{am:.0} ({ah})"),
            rss_before,
            peak,
            after.summary(),
        );
        node.shutdown().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// **Permit queue**: W invocations wait for a permit on one range under a
/// `concurrency: 1` rule on scope `acme`.
///
/// The first turn holds the permit at the fake SDK's gate; the other W − 1 turns
/// wait for a permit. Measured over a `PERMIT_WINDOW_S` window (default 10 s)
/// while the gate is closed: engine holds per second (every holder — the cost of
/// the queue is tasks) and RSS. Then open the gate and measure the throughput of
/// granted turns (SDK turns per second) together with engine holds per second
/// in a second window.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run by hand"]
async fn permit_queue_churn() -> Result<()> {
    let ws = bench::levels("PERMIT_W", &[1_000, 10_000]);
    let window = Duration::from_secs(
        std::env::var("PERMIT_WINDOW_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10),
    );
    println!(
        "\n  {:>7} | {:>7} | {:>14} | {:>9} | {:>9} | {:>14} | {:>14} | engine holds while waiting (holder total/longest ms)",
        "W", "build s", "holds/s waiting", "RSS 0 MiB", "RSS MiB", "granted/s", "holds/s running"
    );
    for w in ws {
        let store = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
        let sdk = FakeSdk::builder()
            .gated_service("Work", "run", vec![Step::output("ok")])
            .spawn()
            .await?;
        let rss0 = bench::rss_mib();
        let node = common::start_led(bench::config(&store)).await?;
        let url = node.url();
        common::register(&url, &sdk.url).await?;
        let r = reqwest::Client::new()
            .post(format!("{url}/rules"))
            .json(&serde_json::json!({ "pattern": "acme", "concurrency": 1 }))
            .send()
            .await?;
        ensure!(r.status().is_success(), "set rule: {}", r.status());
        let t0 = std::time::Instant::now();
        bench::send(&node, 4, "Work", Some("acme"), w).await?;
        common::wait_status(&url, &bench::hex(&bench::id(4, w - 1)), "running").await?;
        let built = t0.elapsed().as_secs_f64();
        // Let the queue settle (every task is waiting for a permit).
        tokio::time::sleep(Duration::from_secs(2)).await;
        let started = sdk.starts_count_of("Work", "run");
        let h0 = bench::Holds::read(&url).await?;
        tokio::time::sleep(window).await;
        let h1 = bench::Holds::read(&url).await?;
        let rss = bench::rss_mib();
        // Tasks running background turns and tickets parked while waiting (0 / absent on an
        // older binary).
        let tasks = gauge(&url, "cairnflow_flow_turn_tasks").await?;
        let parked = gauge(&url, "cairnflow_flow_parked").await?;
        let waiting_rate = h1.count_since(&h0) / window.as_secs_f64();
        sdk.gate().open();
        let s0 = sdk.starts_count_of("Work", "run");
        tokio::time::sleep(window).await;
        let s1 = sdk.starts_count_of("Work", "run");
        let h2 = bench::Holds::read(&url).await?;
        let granted = (s1 - s0) as f64 / window.as_secs_f64();
        let running_rate = h2.count_since(&h1) / window.as_secs_f64();
        // Which holders account for the holds: while waiting and while running.
        println!(
            "  W = {w}: holds while waiting by holder: {}; while running: {}; granted turns {}; while waiting: {tasks} background-turn tasks, {parked} tickets",
            h1.counts_since(&h0),
            h2.counts_since(&h1),
            s1 - s0,
        );
        println!(
            "  {:>7} | {:>7.1} | {:>14.0} | {:>9.0} | {:>9.0} | {:>14.1} | {:>14.0} | {}{}",
            w,
            built,
            waiting_rate,
            rss0,
            rss,
            granted,
            running_rate,
            h1.summary(),
            if started > 1 {
                format!(" (WARNING: {started} turns ran with the gate shut — rule not set?)")
            } else {
                String::new()
            },
        );
        node.shutdown().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// **Permit queue with retry rows**: W permit waiters where each invocation
/// already had a retry row before parking — the first turn fails in a retryable
/// way (`next_retry_delay` `D`), the retry's firing hands out the turn, the turn
/// parks a ticket because of the rule `acme = 1` (one `Hold` turn keeps the
/// permit at the SDK gate), and the next retry (~200 ms) comes due while the
/// ticket is parked — the background tick sees the invocation busy and moves the row.
///
/// Measured over a `PERMIT_WINDOW_S` window (default 20 s), starting 15 s after
/// every ticket has parked: moves per second (`cairnflow_flow_retry_busy_total`
/// — each one a row rewritten in the tick's commit), engine holds per second by
/// holder, `tick` hold time per second.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run by hand"]
async fn permit_queue_retry_rows() -> Result<()> {
    let ws = bench::levels("PERMIT_W", &[1_000, 10_000]);
    let window = Duration::from_secs(
        std::env::var("PERMIT_WINDOW_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    );
    println!(
        "\n  {:>7} | {:>10} | {:>12} | {:>14} | {:>16} | holds in the window by holder",
        "W", "tickets", "moves/s", "holds/s", "`tick` ms/s"
    );
    for w in ws {
        let store = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
        let delay = 4_000 + w as u64 / 4;
        let sdk = FakeSdk::builder()
            .service(
                "Work",
                "run",
                vec![Step::FailBelowDelay(1, delay), Step::output("ok")],
            )
            .gated_service("Hold", "run", vec![Step::output("ok")])
            .spawn()
            .await?;
        let node = common::start_led(bench::config(&store)).await?;
        let url = node.url();
        common::register(&url, &sdk.url).await?;
        // The first turn of all W fails at once and is retried after `delay`.
        bench::send(&node, 6, "Work", Some("acme"), w).await?;
        let t0 = std::time::Instant::now();
        while sdk.starts_count_of("Work", "run") < w {
            ensure!(t0.elapsed() < Duration::from_secs(600), "first turns late");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        ensure!(
            (t0.elapsed().as_millis() as u64) < delay,
            "first turns done after the retry deadline — raise `delay`"
        );
        let r = reqwest::Client::new()
            .post(format!("{url}/rules"))
            .json(&serde_json::json!({ "pattern": "acme", "concurrency": 1 }))
            .send()
            .await?;
        ensure!(r.status().is_success(), "set rule: {}", r.status());
        bench::send(&node, 7, "Hold", Some("acme"), 1).await?;
        while sdk.starts_count_of("Hold", "run") < 1 {
            ensure!(t0.elapsed() < Duration::from_secs(600), "Hold not in SDK");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The firing: every turn parks a ticket (the `Hold` turn's does not — it has the permit).
        while gauge(&url, "cairnflow_flow_parked").await? < w as f64 {
            ensure!(t0.elapsed() < Duration::from_secs(900), "too few parked");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
        let busy0 = gauge(&url, "cairnflow_flow_retry_busy_total").await?;
        let h0 = bench::Holds::read(&url).await?;
        tokio::time::sleep(window).await;
        let busy1 = gauge(&url, "cairnflow_flow_retry_busy_total").await?;
        let h1 = bench::Holds::read(&url).await?;
        let secs = window.as_secs_f64();
        let tick_ms =
            h1.by.get("tick").map_or(0.0, |x| x.0) - h0.by.get("tick").map_or(0.0, |x| x.0);
        println!(
            "  {:>7} | {:>10.0} | {:>12.1} | {:>14.1} | {:>16.1} | {}",
            w,
            gauge(&url, "cairnflow_flow_parked").await?,
            (busy1 - busy0) / secs,
            h1.count_since(&h0) / secs,
            tick_ms / secs,
            h1.counts_since(&h0),
        );
        sdk.gate().open();
        node.shutdown().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// **Tenant purge threshold**: one range pre-built with `N` `completed`
/// invocations (`PURGE_N`, default 10⁴ and 10⁵ ≈ 10⁵ and 10⁶ keys).
///
/// Measures: (1) how many lots a full **check** pass (`find_tenant_rows`, the
/// same scan as a delete lot, `ROW_BUDGET` rows / `8 × ROW_BUDGET` keys per lot)
/// needs and how many keys it sees — tenant `acme` has no rows, so a scan–delete
/// pass is exactly that many lots too; (2) a separate `Purger` (default cap
/// `LOTS_PER_RANGE`, `keep` 1 s) runs `run_once` on `acme` (registry pre-set to
/// `deleting` + `purging-ms`) until the range reaches `compacted`: the number of
/// rounds, the number of full compactions requested (distinct ids that entered
/// `Purger::compactions`), then the number of rounds until `purged`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run by hand"]
async fn purge_pass_lots() -> Result<()> {
    use cairn_server::tenant_purge::{Purger, LOTS_PER_RANGE, ROW_BUDGET};
    let ns = bench::levels("PURGE_N", &[10_000, 100_000]);
    println!(
        "\n  {:>7} | {:>7} | {:>10} | {:>9} | {:>11} | {:>10} | {:>9} | {:>11}",
        "N", "build s", "keys", "lots/pass", "check s", "→compact", "compacts", "→purged"
    );
    for n in ns {
        let store = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
        let sdk = FakeSdk::builder()
            .service("Work", "run", vec![Step::output("ok")])
            .spawn()
            .await?;
        let mut cfg = bench::config(&store);
        // The node's purge loop is off: only the measurement's `Purger` runs.
        cfg.tenant_purge_keep = None;
        let node = common::start_led(cfg).await?;
        let url = node.url();
        common::register(&url, &sdk.url).await?;
        let t0 = std::time::Instant::now();
        bench::build(&node, &sdk, 9, "Work", n, 5_000, "completed").await?;
        let built = t0.elapsed().as_secs_f64();
        let range = node.cluster.partitions()[0];
        let h = node
            .partitions
            .get(range)
            .ok_or_else(|| anyhow::anyhow!("node does not run range {range:?}"))?;
        let acme = cairn_store::TenantId::new("acme")?;
        // (1) One full check pass.
        let t1 = std::time::Instant::now();
        let (mut lots, mut keys, mut cursor) = (0u64, 0u64, None);
        loop {
            let grant = cairn_store::proof::TenantPurgeGrant::assume(acme.clone());
            let step = h
                .find_tenant_rows(grant, cursor, ROW_BUDGET)
                .await
                .map_err(|e| anyhow::anyhow!("check lot: {e}"))?;
            lots += 1;
            keys += step.scanned;
            ensure!(step.found == 0, "acme has rows?");
            match step.next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let pass = t1.elapsed().as_secs_f64();
        // (2) A real Purger on `acme`.
        let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
            store.clone(),
            "",
            "memory://",
        ));
        let main = stores.main();
        let settings = cairn_server::tenants::TenantSettings::new(Some((
            Arc::clone(main.store()),
            main.prefix(),
        )));
        let old = (now_ms() - 10_000).to_string();
        settings
            .try_update(&acme, |f| {
                f.state_raw = Some("deleting".into());
                f.purge.deleting = Some(old.clone());
                f.purge.purging = Some(old.clone());
                Ok::<_, String>(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("registry: {e}"))?
            .map_err(|e| anyhow::anyhow!("registry: {e}"))?;
        let purger = Purger {
            stores,
            cluster: node.cluster.clone(),
            partitions: node.partitions.clone(),
            routing: node.routing.clone(),
            secret: None,
            client: reqwest::Client::new(),
            settings,
            keys: None,
            rules: None,
            keep: Duration::from_secs(1),
            last: Arc::default(),
            compactions: Arc::default(),
            watches: Arc::default(),
            lots_per_range: LOTS_PER_RANGE,
            sweeps: Arc::default(),
            checks: Arc::default(),
            bases: Arc::default(),
        };
        let mut ids = std::collections::BTreeSet::new();
        let (mut to_compacted, mut to_purged) = (None, None);
        for round in 1..=60u32 {
            let rounds = purger
                .run_once()
                .await
                .map_err(|e| anyhow::anyhow!("round {round}: {e}"))?;
            if let Ok(c) = purger.compactions.lock() {
                ids.extend(c.values().map(|(id, _)| id.clone()));
            }
            let r = rounds.iter().find(|r| r.tenant == "acme");
            if to_compacted.is_none() && r.is_some_and(|r| r.compacted.contains(&range.0)) {
                to_compacted = Some(round);
            }
            if r.is_some_and(|r| r.phase == "purged") {
                to_purged = Some(round);
                break;
            }
            // Let the range's compactor run the compaction just requested.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!(
            "  {:>7} | {:>7.1} | {:>10} | {:>9} | {:>11.1} | {:>10} | {:>9} | {:>11}",
            n,
            built,
            keys,
            lots,
            pass,
            to_compacted.map_or("—".into(), |r| r.to_string()),
            ids.len(),
            to_purged.map_or("—".into(), |r| r.to_string()),
        );
        node.shutdown().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
