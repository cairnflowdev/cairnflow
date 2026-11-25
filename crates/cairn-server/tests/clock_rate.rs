// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Measuring the clock rate at runtime.**
//!
//! Every lease assumes the nodes' monotonic clocks drift apart by less than 1.5×
//! (LS-1, LS-2). A node compares the elapsed time on its own clock with the elapsed
//! time by store time (`cairn_store::store_clock`, the `Date` header of requests it
//! makes anyway); past the threshold it **steps down from leadership** — releases its
//! ranges, releases the coordinator role, takes nothing back — and once back within
//! the threshold it takes ranges again as usual.
//!
//! The test's store is `memory://` (no `Date` header), so the test feeds samples to
//! node `a`'s `StoreClock` itself: store time = real wall time, send/receive marks =
//! `a`'s **fake** monotonic clock ([`RateClock`], running at `r`× real time).

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
    cairn_store::{
        clock::Clock,
        store_clock::{StoreClock, StoreClockConfig},
    },
    common::{wait_probe, ClusterNode},
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::time::Instant,
};

const TTL: Duration = Duration::from_secs(2);
const PARTITIONS: u16 = 6;

/// A monotonic clock running at `rate`× real time; `rate` changes at runtime without jumps.
#[derive(Debug)]
struct RateClock {
    base: Instant,
    /// (real time at the change, fake time at the change, rate).
    state: Mutex<(Instant, Duration, f64)>,
}

impl RateClock {
    fn new() -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            base: now,
            state: Mutex::new((now, Duration::ZERO, 1.0)),
        })
    }

    fn set_rate(&self, rate: f64) {
        let now = Instant::now();
        let mut s = self.state.lock().unwrap();
        let fake = s.1 + (now - s.0).mul_f64(s.2);
        *s = (now, fake, rate);
    }
}

impl Clock for RateClock {
    fn wall(&self) -> Duration {
        cairn_store::clock::SystemClock.wall()
    }

    fn mono(&self) -> Instant {
        let s = self.state.lock().unwrap();
        self.base + s.1 + (Instant::now() - s.0).mul_f64(s.2)
    }
}

/// Shortened knobs: a 3 s window instead of 5 minutes. Fed samples have 1 ms resolution.
fn quick() -> StoreClockConfig {
    StoreClockConfig {
        window: Duration::from_secs(3),
        min_span: Duration::from_secs(2),
        sample_every: Duration::from_millis(100),
        max_rtt: Duration::from_secs(1),
        check_every: Duration::from_millis(200),
    }
}

/// Feed samples to `sc` every 50 ms: store time = real wall time.
fn feed(sc: StoreClock) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let t = sc.now();
            sc.record(
                cairn_store::clock::SystemClock.wall(),
                Duration::from_millis(1),
                t,
                t,
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

/// Three nodes `a`, `b`, `c`, [`PARTITIONS`] ranges; `a` starts first (alone ⇒ the
/// cluster's coordinator, a sticky role) and measures its clock rate with `RateClock`.
/// Returns once every node leads at least one range.
async fn three(clock: Arc<RateClock>) -> Result<(common::Bucket, Vec<ClusterNode>, StoreClock)> {
    let bucket = common::bucket();
    let sc = StoreClock::new(clock, quick());
    let a = {
        let sc = sc.clone();
        common::start_cluster_node_tweaked(&bucket, "a", PARTITIONS, TTL, move |cfg| {
            cfg.stores = cfg.stores.clone().with_store_clock(Some(sc));
        })
        .await?
    };
    let t0 = std::time::Instant::now();
    while a.node.session.coordinating().is_none() {
        ensure!(t0.elapsed() < TTL * 10, "a not coordinator yet");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut nodes = vec![a];
    for id in ["b", "c"] {
        nodes.push(common::start_cluster_node(&bucket, id, PARTITIONS, TTL).await?);
    }
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(60),
        "every node leads a range",
        |p| common::single_leaders(p).is_some_and(|l| (0..3).all(|i| l.values().any(|n| *n == i))),
    )
    .await?;
    Ok((bucket, nodes, sc))
}

/// The number of ranges `a` (index 0) leads, asked straight from the engine.
async fn led_by_a(nodes: &[ClusterNode]) -> usize {
    let refs: Vec<_> = nodes.iter().collect();
    common::leaders_by_probe(&refs)
        .await
        .values()
        .filter(|v| v.contains(&0))
        .count()
}

/// **`a`'s clock running 1.3× fast or slow ⇒ `a` steps down; back at the right rate
/// ⇒ `a` takes ranges again.**
///
/// 1. The [`three`] cluster; `a` is the coordinator and leads at least one range; store
///    time samples fed steadily, `a`'s clock running right.
/// 2. `a`'s clock switches to `r`× (`r` = 1.3, then 1/1.3 on the next pass).
/// 3. After one measurement window (3 s), every sample pair of `a` gives a rate
///    clearly outside ±10%.
///
/// Must see: within the window + 15 s, `a` leads no range any more, is no longer the
/// coordinator, its session record announces `draining=1`, and every range has
/// exactly one leader on `b`/`c`; `/metrics` reports `cairnflow_clock_rate_untrusted`
/// and `cairnflow_clock_rate_stepdowns_total`. Then the clock returns to 1×: within
/// the window + 60 s `a` leads at least one range again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_with_a_skewed_clock_steps_down_and_comes_back() -> Result<()> {
    for rate in [1.3, 1.0 / 1.3] {
        let clock = RateClock::new();
        let (bucket, nodes, sc) = three(clock.clone()).await?;
        let feeder = feed(sc.clone());
        let r = async {
            // A full sample window at the right rate: `a` still leads.
            tokio::time::sleep(quick().window + Duration::from_secs(1)).await;
            ensure!(led_by_a(&nodes).await >= 1, "r=1: a stepped down");
            clock.set_rate(rate);
            let t0 = std::time::Instant::now();
            let bound = quick().window + Duration::from_secs(15);
            loop {
                let led = led_by_a(&nodes).await;
                let coord = nodes[0].node.session.coordinating();
                let rec = common::session_text(&bucket, &nodes[0])
                    .await
                    .unwrap_or_default();
                let refs: Vec<_> = nodes.iter().collect();
                let others = common::single_leaders(&common::leaders_by_probe(&refs).await)
                    .is_some_and(|l| l.len() == PARTITIONS as usize && l.values().all(|n| *n != 0));
                if led == 0 && coord.is_none() && rec.lines().any(|l| l == "draining=1") && others {
                    break;
                }
                ensure!(
                    t0.elapsed() < bound,
                    "r={rate}: {bound:?} and a has not stepped down — leads {led} ranges, \
                     coordinator {coord:?}, draining=1 {}, ranges on b/c {others}",
                    rec.lines().any(|l| l == "draining=1")
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // The clock-rate metrics (the process is shared with other tests ⇒ ≥, not =).
            let text = reqwest::get(format!("{}/metrics", nodes[0].node.url()))
                .await?
                .text()
                .await?;
            let gauge = |name: &str| {
                text.lines()
                    .find_map(|l| l.strip_prefix(&format!("{name} ")))
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0)
            };
            ensure!(
                gauge("cairnflow_clock_rate_untrusted") >= 1.0
                    && gauge("cairnflow_clock_rate_stepdowns_total") >= 1.0,
                "r={rate}: the metrics do not report a skewed clock"
            );
            clock.set_rate(1.0);
            let t1 = std::time::Instant::now();
            let back = quick().window + Duration::from_secs(60);
            while led_by_a(&nodes).await == 0 {
                ensure!(
                    t1.elapsed() < back,
                    "r={rate}: clock back at 1× but {back:?} later a has taken no range back"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        }
        .await;
        feeder.abort();
        for n in nodes {
            n.node.shutdown().await;
        }
        drop(bucket);
        r?;
    }
    Ok(())
}

/// **Within the threshold ⇒ no step-down.** `a`'s clock runs at 1.05× (beyond any
/// measurement noise, within ±10%) for three windows: `a` leads at least one range on
/// every poll, and never announces `draining=1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_within_tolerance_keeps_leadership() -> Result<()> {
    let clock = RateClock::new();
    let (bucket, nodes, sc) = three(clock.clone()).await?;
    let feeder = feed(sc.clone());
    let r = async {
        clock.set_rate(1.05);
        let t0 = std::time::Instant::now();
        while t0.elapsed() < quick().window * 4 {
            let led = led_by_a(&nodes).await;
            let rec = common::session_text(&bucket, &nodes[0])
                .await
                .unwrap_or_default();
            ensure!(
                led >= 1 && !rec.lines().any(|l| l == "draining=1"),
                "r=1.05 but a stepped down ({led} ranges) or drained: {rec}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }
    .await;
    feeder.abort();
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
