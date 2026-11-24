// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Sustained S3-style throttling: the bucket returns `503 SlowDown` when a
//! prefix exceeds its request/s budget. cairn's promise does not change — **no
//! ack is lost**: every invocation that received an id ends correctly, even
//! when the 503s are still there at check time.
//!
//! Interleaving of each test:
//!
//! 1. A two-node cluster (alpha, beta) on one `InMemory` bucket, 4 ranges, a
//!    registered deployment. Each node wraps the bucket in its own
//!    `FaultyStore`, but **the same** [`SlowDown`] — the budget belongs to the
//!    bucket, not to a node.
//! 2. Enable the throttling rule: the whole bucket is one partition (depth 0,
//!    no splitting), with write/read budgets well below what the load needs.
//! 3. `CONC` loops send `/send` continuously for `LOAD` seconds, each call to
//!    its own key of a virtual object (writes one state row, then returns
//!    `ok`). The returned id is the ack. Every other code (429/503/500/network
//!    error) is counted by kind.
//! 4. Stop the load; the throttling rule **stays on**. Poll every acked id until
//!    `completed`.
//!
//! Must see: every acked id reaches `completed` within the limit; no `500`
//! response; the throttling rule really fired. The behaviour of the L0
//! throttle, `durability_stall` and self-release is **recorded** (printed), not
//! asserted — that is what this test measures.

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
    cairn_store::testkit::{Action, ClientRetry, OpSet, PathMatch, Rule, SlowDown, SlowDownConfig},
    common::{
        fake_sdk::{FakeSdk, Step},
        ClusterNode,
    },
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    },
};

const TTL: Duration = Duration::from_secs(2);
const PARTITIONS: u16 = 4;
const CONC: usize = 8;
const LOAD: Duration = Duration::from_secs(12);
/// Limit for waiting until every acked id ends, while the throttling rule is
/// **still on**.
///
/// Usually done in a few seconds. The limit covers the slowest path that is
/// still valid: a turn that failed because of the store is rescheduled with a
/// backoff capped at `RETRY_CAP_MS` = 60 s. A real hang (a range with no leader,
/// nobody rescheduling) is not covered by any limit.
const SETTLE: Duration = Duration::from_secs(120);

/// Read a line `name{...} v` or `name v` from the `/metrics` text.
fn metric(text: &str, prefix: &str) -> Vec<String> {
    text.lines()
        .filter(|l| l.starts_with(prefix))
        .map(str::to_string)
        .collect()
}

struct Outcome {
    acked: usize,
    codes: BTreeMap<String, u64>,
    stats: cairn_store::testkit::SlowDownStats,
    settle: Duration,
    observed: Vec<String>,
}

async fn run(slow: SlowDown) -> Result<Outcome> {
    let (_bucket, nodes) =
        common::cluster_nodes(&["node-alpha", "node-beta"], PARTITIONS, TTL).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    common::wait_probe(&refs, TTL * 10, "one leader per range", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let sdk = FakeSdk::builder()
        .object(
            "Obj",
            "work",
            vec![
                Step::SetState {
                    key: "v".into(),
                    value: bytes::Bytes::from_static(b"1"),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let urls: Vec<String> = nodes.iter().map(|n| n.node.url()).collect();
    common::register(&urls[0], &sdk.url).await?;
    // The other node learns the deployment through the shared registry — wait to be sure, before throttling.
    for url in &urls {
        common::within(Duration::from_secs(30), "nodes know deployment", async {
            loop {
                let t = reqwest::get(format!("{url}/services"))
                    .await?
                    .text()
                    .await?;
                if t.contains("\"Obj\"") {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
    }

    for n in &nodes {
        n.store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::SlowDown(slow.clone()),
        ));
    }

    let acked = Arc::new(Mutex::new(Vec::<String>::new()));
    let codes = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    // A sample response body per code, to record what the 503/502 say.
    let samples = Arc::new(Mutex::new(BTreeMap::<String, String>::new()));
    let seq = Arc::new(AtomicU64::new(0));
    let end = Instant::now() + LOAD;
    let mut loops = Vec::new();
    for w in 0..CONC {
        let (acked, codes, seq, samples) =
            (acked.clone(), codes.clone(), seq.clone(), samples.clone());
        let url = urls[w % urls.len()].clone();
        loops.push(tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap();
            while Instant::now() < end {
                let i = seq.fetch_add(1, Ordering::Relaxed);
                let r = http
                    .post(format!("{url}/invoke/Obj/work/send?key=k{i}"))
                    .body("x")
                    .send()
                    .await;
                let code = match r {
                    Ok(r) => {
                        let code = r.status().as_u16();
                        let text = r.text().await.unwrap_or_default();
                        let body: serde_json::Value =
                            serde_json::from_str(&text).unwrap_or_default();
                        if code < 300 {
                            if let Some(id) = body["id"].as_str() {
                                acked.lock().unwrap().push(id.to_string());
                            }
                        } else {
                            samples
                                .lock()
                                .unwrap()
                                .entry(code.to_string())
                                .or_insert_with(|| text.chars().take(300).collect());
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        code.to_string()
                    }
                    Err(e) if e.is_timeout() => "timeout".into(),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        "net".into()
                    }
                };
                *codes.lock().unwrap().entry(code).or_default() += 1;
            }
        }));
    }
    for l in loops {
        l.await?;
    }
    let acked = acked.lock().unwrap().clone();
    let codes = codes.lock().unwrap().clone();
    let mut observed: Vec<String> = samples
        .lock()
        .unwrap()
        .iter()
        .map(|(c, b)| format!("sample {c}: {b}"))
        .collect();
    for (n, url) in nodes.iter().zip(&urls) {
        let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
        for prefix in [
            "cairnflow_draining",
            "cairnflow_slow_score",
            "cairnflow_invoke_throttled_total",
            "cairnflow_flow_throttled_total",
        ] {
            for l in metric(&text, prefix) {
                observed.push(format!("{}: {l}", n.node.node_id));
            }
        }
    }

    // The throttling rule is still on: every acked id must end.
    let t0 = Instant::now();
    let mut pending: Vec<String> = acked.clone();
    while !pending.is_empty() {
        ensure!(
            t0.elapsed() < SETTLE,
            "{} / {} acked invocations not ended after {SETTLE:?} (e.g. {:?}); 503: {:?}",
            pending.len(),
            acked.len(),
            &pending[..pending.len().min(3)],
            slow.stats()
        );
        let mut still = Vec::new();
        for id in pending {
            // Ask **every** node until one says `completed`. A node that does not
            // hold the range answers through a follower reader, and it also returns
            // a stale positive state (`running`) — stale until the leader flushes
            // its memtable, which when idle is `frame_age_flush` 60 s. Stopping at
            // the first 200 would measure the follower's staleness, not cairn.
            let mut done = false;
            for url in &urls {
                if let Ok((200, st)) = common::invocation(url, &id).await {
                    ensure!(
                        st["status"] != "failed",
                        "invocation {id} failed under 503: {st}"
                    );
                    if st["status"] == "completed" {
                        done = true;
                        break;
                    }
                }
            }
            if !done {
                still.push(id);
            }
        }
        pending = still;
        if !pending.is_empty() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let settle = t0.elapsed();
    let leaders = common::leaders_by_probe(&refs).await;
    observed.push(format!("leaders after the load: {leaders:?}"));
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(Outcome {
        acked: acked.len(),
        codes,
        stats: slow.stats(),
        settle,
        observed,
    })
}

fn report(name: &str, o: &Outcome) {
    eprintln!(
        "[{name}] ack {} · codes {:?} · 503 {:?} · all ended after {:?}",
        o.acked, o.codes, o.stats, o.settle
    );
    for l in &o.observed {
        eprintln!("[{name}]   {l}");
    }
}

fn throttle(client_retry: Option<ClientRetry>) -> SlowDown {
    SlowDown::new(SlowDownConfig {
        write_rate: 40.0,
        read_rate: 200.0,
        initial_depth: 0,
        max_depth: 0,
        split_after: Duration::from_secs(3600),
        client_retry,
        seed: 7,
    })
}

/// Like real S3: the 503 reaches `object_store`'s HTTP client first, the client
/// retries with backoff, cairn only sees latency — and an error when the client
/// runs out of attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_slowdown_with_client_retry_loses_no_ack() -> Result<()> {
    let o = run(throttle(Some(ClientRetry::default()))).await?;
    report("client_retry", &o);
    ensure!(
        o.acked > 0,
        "no ack under throttling — the load did not reach cairn: {:?}",
        o.codes
    );
    ensure!(o.stats.rejected > 0, "throttle never fired: {:?}", o.stats);
    ensure!(
        !o.codes.contains_key("500"),
        "500 response under 503: {:?}",
        o.codes
    );
    Ok(())
}

/// Harsher than S3: no client retry loop, every 503 goes straight to cairn
/// (SlateDB retries; the shared WAL, leases and the shared registry handle
/// their own).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_slowdown_surfaced_to_cairn_loses_no_ack() -> Result<()> {
    let o = run(throttle(None)).await?;
    report("surfaced", &o);
    ensure!(
        o.acked > 0,
        "no ack under throttling — the load did not reach cairn: {:?}",
        o.codes
    );
    ensure!(o.stats.surfaced > 0, "503 never hit cairn: {:?}", o.stats);
    ensure!(
        !o.codes.contains_key("500"),
        "500 response under 503: {:?}",
        o.codes
    );
    Ok(())
}
