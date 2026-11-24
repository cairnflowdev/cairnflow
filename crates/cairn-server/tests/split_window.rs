// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Split window bench** — the write pause of one split, measured with the
//! real engine.
//!
//! One node in one process, one range. Load `N` **live** invocations (an
//! object, each suspended on a one-hour `sleep` — status rows, timers, queues in
//! the hot region; journal and state in the bulk region), then
//! `POST /ranges/0/split` and read the response's `report`: each phase of the
//! coordinator, the parent's drain/seal, and each child's Db open + rebuild of
//! the derived region + queue recovery. In parallel, two probe clients (one per
//! half) keep calling a handler that returns at once — their largest latency is
//! the window **the client sees**. After the split: the first call to each
//! child, then merge back.
//!
//! Not run in the normal test suite (`#[ignore]`). Run:
//!
//! ```sh
//! SPLIT_BENCH_N=10000 SPLIT_BENCH_BULK=heavy \
//! SPLIT_BENCH_STORE=file:///tmp/cairn-split-bench \
//!   cargo test --release -p cairn-server --test split_window -- --ignored --nocapture
//! ```
//!
//! `SPLIT_BENCH_STORE=s3://bucket/prefix` (with the usual `AWS_*` variables) —
//! a timestamp is appended to the prefix so each run gets a new Db. `file://`
//! has no CAS, which the range table needs, so the bench wraps it in
//! `testkit::LocalCas` (correct for one process). The data is deleted at the
//! end.
//!
//! `SPLIT_BENCH_MERGE_STORM=0`: do not wait for the recovery storm after the
//! merge. `SPLIT_BENCH_STORM=0`: do not wait for the storm after the split
//! either (nor for the first call).
//!
//! `SPLIT_BENCH_BULK`: `heavy` (default — 1 KiB input, four 256 B state rows,
//! a 6-entry journal) or `light` (16 B input, a 2-entry journal, no state): the
//! same hot region, a bulk region ~6 times different — the rebuild must not
//! slow down with it.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Context, Result},
    cairn_server::node::{NodeConfig, NodeMode},
    cairn_store::{
        partition_key_for_target,
        testkit::{FaultyStore, LocalCas, Op, PathClass, StoreCounts},
        ObjectStores, StoreLocation,
    },
    common::fake_sdk::{FakeSdk, Step},
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    tokio::{sync::Semaphore, task::JoinSet},
};

const MID: u64 = 1 << 63;

/// Loading runs ahead of the part the SDK has run by at most this many invocations.
const LOAD_AHEAD: usize = 2_000;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn key_in_half(service: &str, left: bool, prefix: &str) -> String {
    (0..100_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| {
            let pk =
                partition_key_for_target(&cairn_store::TenantId::default(), None, service, k, None)
                    .expect("has a key")
                    .0;
            (pk < MID) == left
        })
        .expect("found a key")
}

/// The bench's store: counts requests + GET bytes (`FaultyStore` with no rules),
/// with `LocalCas` added on `file://`.
fn bench_store(
    uri: &str,
) -> Result<(
    ObjectStores,
    Arc<FaultyStore>,
    String,
    Option<std::path::PathBuf>,
)> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    if let Some(dir) = uri.strip_prefix("file://") {
        let dir = std::path::PathBuf::from(format!("{}/run-{stamp}", dir.trim_end_matches('/')));
        let loc = StoreLocation::parse(&format!("file://{}", dir.display()))?;
        let counted = FaultyStore::wrap(LocalCas::wrap(loc.store().clone()));
        let stores = ObjectStores::single(StoreLocation::from_store(counted.clone(), "", uri));
        Ok((
            stores,
            counted,
            format!("file ({})", dir.display()),
            Some(dir),
        ))
    } else {
        let uri = format!("{}-{stamp}", uri.trim_end_matches('/'));
        let loc = StoreLocation::parse(&uri)?;
        let counted = FaultyStore::wrap(loc.store().clone());
        let stores = ObjectStores::single(StoreLocation::from_store(
            counted.clone(),
            loc.prefix().clone(),
            &uri,
        ));
        Ok((stores, counted, uri, None))
    }
}

/// Total bytes of the objects under `prefix`.
async fn bytes_under(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<(u64, usize)> {
    let mut list = store.list(Some(prefix));
    let (mut bytes, mut n) = (0u64, 0usize);
    use futures_core::Stream;
    while let Some(meta) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut list).poll_next(cx)).await
    {
        let meta = meta?;
        bytes += meta.size;
        n += 1;
    }
    Ok((bytes, n))
}

/// Delete every object under `prefix` (cleans the SeaweedFS bucket after the bench).
async fn delete_under(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<usize> {
    let mut list = store.list(Some(prefix));
    let mut paths = Vec::new();
    use futures_core::Stream;
    while let Some(meta) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut list).poll_next(cx)).await
    {
        paths.push(meta?.location);
    }
    let sem = Arc::new(Semaphore::new(32));
    let mut tasks = JoinSet::new();
    let n = paths.len();
    for p in paths {
        let store = store.clone();
        let permit = sem.clone().acquire_owned().await?;
        tasks.spawn(async move {
            let _p = permit;
            store.delete(&p).await
        });
    }
    while let Some(r) = tasks.join_next().await {
        r??;
    }
    Ok(n)
}

async fn post(client: &reqwest::Client, url: &str, body: &str) -> Result<(u16, serde_json::Value)> {
    let resp = client.post(url).body(body.to_string()).send().await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// `POST`, retrying on `503`/`429` (range changing, L0 short of room, out of
/// slots), until `deadline`. Returns the last code, the body, the duration of
/// the last attempt, and the time spent on the attempts before it.
async fn post_retrying(
    client: &reqwest::Client,
    url: &str,
    body: &str,
    deadline: Duration,
) -> Result<(u16, serde_json::Value, Duration, Duration)> {
    let t0 = Instant::now();
    loop {
        let t = Instant::now();
        let (code, v) = post(client, url, body).await?;
        if (code != 503 && code != 429) || t0.elapsed() > deadline {
            return Ok((code, v, t.elapsed(), t - t0));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `Probe/ping` until **done**: `503` retries, `202` (no slot yet within
/// `invoke_wait`) polls the status until `completed`.
async fn ping_until_done(client: &reqwest::Client, url: &str, key: &str) -> Result<()> {
    let (code, body, _, _) = post_retrying(
        client,
        &format!("{url}/invoke/Probe/ping?key={key}"),
        "p",
        Duration::from_secs(60),
    )
    .await?;
    if code == 200 {
        return Ok(());
    }
    ensure!(code == 202, "probe: {code} {body}");
    let id = body["id"].as_str().unwrap_or_default().to_string();
    let t0 = Instant::now();
    loop {
        let (code, st) = common::invocation(url, &id).await?;
        if code == 200 && st["status"] == "completed" {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(300),
            "probe {id} not done: {st}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// One probe client: keeps calling `Probe/ping` on one half of the keys,
/// recording (start, latency) of each call — retry time included.
fn spawn_probe(
    url: String,
    left: bool,
    stop: Arc<AtomicBool>,
    origin: Instant,
    samples: Arc<std::sync::Mutex<Vec<(f64, f64)>>>,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut n = 0usize;
        while !stop.load(Ordering::Relaxed) {
            let key = key_in_half("Probe", left, &format!("probe-{left}-{n}"));
            n += 1;
            let t = Instant::now();
            ping_until_done(&client, &url, &key).await?;
            if let Ok(mut v) = samples.lock() {
                v.push((
                    (t - origin).as_secs_f64() * 1000.0,
                    t.elapsed().as_secs_f64() * 1000.0,
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    })
}

fn summarize(counts: &StoreCounts) -> serde_json::Value {
    serde_json::json!({
        "get": counts.op(Op::Get),
        "getCompacted": counts.get(Op::Get, PathClass::Compacted),
        "getBytesCompacted": counts.get_bytes(PathClass::Compacted),
        "getBytesWal": counts.get_bytes(PathClass::Wal),
        "put": counts.op(Op::Put),
        "list": counts.op(Op::List),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "split window bench — run by hand, see the top of the file"]
async fn split_window_live() -> Result<()> {
    let n: usize = env_or("SPLIT_BENCH_N", "10000").parse()?;
    let bulk = env_or("SPLIT_BENCH_BULK", "heavy");
    let conc: usize = env_or("SPLIT_BENCH_CONCURRENCY", "64").parse()?;
    let default_dir = std::env::temp_dir().join("cairn-split-bench");
    let uri = env_or(
        "SPLIT_BENCH_STORE",
        &format!("file://{}", default_dir.display()),
    );
    let (stores, counted, store_name, dir) = bench_store(&uri)?;
    let main_store = stores.main().store().clone();
    let range0 = stores.range_path(0);
    let root = stores.main().prefix().clone();

    // Shared WAL (the only WAL): a cluster node with one range.
    let wal = "shared";
    let mut cfg = NodeConfig::new(
        stores,
        NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(10),
            node_id: Some("bench".into()),
        },
    );
    cfg.tick_interval = Some(Duration::from_millis(100));
    let node = common::start_led(cfg).await?;
    let url = node.url();

    let (payload, steps) = match bulk.as_str() {
        "light" => ("x".repeat(16), vec![Step::Sleep(3_600_000)]),
        _ => {
            let mut steps: Vec<Step> = (0..4)
                .map(|i| Step::SetState {
                    key: format!("s{i}"),
                    value: bytes::Bytes::from(vec![b'v'; 256]),
                })
                .collect();
            steps.push(Step::Sleep(3_600_000));
            ("x".repeat(1024), steps)
        }
    };
    let sdk = FakeSdk::builder()
        .object("Live", "hold", steps)
        .object("Probe", "ping", vec![Step::output("ok")])
        .spawn()
        .await?;

    let result = async {
        common::register(&url, &sdk.url).await?;

        // ── Load.
        let t_load = Instant::now();
        let client = reqwest::Client::new();
        let sem = Arc::new(Semaphore::new(conc));
        let mut tasks = JoinSet::new();
        for i in 0..n {
            // Loading must not run too far ahead of the part the SDK has run: with
            // a range queue tens of thousands long, the scheduler rescans it every
            // time it releases a slot — loading slows down sharply (measured at
            // 100k: one core at 100% for the scheduler).
            while i > sdk.starts_len() + LOAD_AHEAD {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let permit = sem.clone().acquire_owned().await?;
            let (client, url, payload) = (client.clone(), url.clone(), payload.clone());
            tasks.spawn(async move {
                let _p = permit;
                let (code, body, _, _) = post_retrying(
                    &client,
                    &format!("{url}/invoke/Live/hold/send?key=live-{i}"),
                    &payload,
                    Duration::from_secs(120),
                )
                .await?;
                ensure!(code == 200 || code == 202, "load {i}: {code} {body}");
                Ok::<_, anyhow::Error>(())
            });
            while let Some(r) = tasks.try_join_next() {
                r??;
            }
        }
        while let Some(r) = tasks.join_next().await {
            r??;
        }
        // Each invocation runs exactly one turn (suspended on sleep): N turns means loading is done.
        let t_wait = Instant::now();
        while sdk.starts_of("Live", "hold").len() < n {
            ensure!(t_wait.elapsed() < Duration::from_secs(600), "load not done");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let load_s = t_load.elapsed().as_secs_f64();
        let (sst_bytes, objects) = bytes_under(&main_store, &range0).await?;
        eprintln!(
            "load {n} ({bulk}) on {store_name}: {load_s:.1} s, ranges/0 = {:.1} MiB / {objects} objects",
            sst_bytes as f64 / 1048576.0
        );

        // ── Probe + split.
        let stop = Arc::new(AtomicBool::new(false));
        let origin = Instant::now();
        let shared_samples = Arc::new(std::sync::Mutex::new(Vec::new()));
        let probes = [
            spawn_probe(url.clone(), true, stop.clone(), origin, shared_samples.clone()),
            spawn_probe(url.clone(), false, stop.clone(), origin, shared_samples.clone()),
        ];
        tokio::time::sleep(Duration::from_millis(500)).await;
        let before = counted.counts();
        let t_split = Instant::now();
        let (code, split, split_took, busy) = post_retrying(
            &client,
            &format!("{url}/ranges/0/split"),
            "",
            Duration::from_secs(180),
        )
        .await?;
        ensure!(code == 200, "split: {code} {split}");
        let split_ms = split_took.as_secs_f64() * 1000.0;
        // The successful attempt starts after `busy` (L0 short of room ⇒ 503).
        let split_at = (t_split + busy - origin).as_secs_f64() * 1000.0;
        let split_counts = counted.counts().since(&before);
        let split_end = (Instant::now() - origin).as_secs_f64() * 1000.0;

        // An early line: a run cut short in the middle of the recovery storm still leaves the split numbers.
        println!(
            "SPLIT_BENCH_PART {}",
            serde_json::json!({"split": {"wallMs": split_ms, "report": split["report"], "store": summarize(&split_counts)}})
        );
        // `SPLIT_BENCH_STORM=0`: do not wait for the first call and the recovery storm
        // (100k on SeaweedFS takes tens of minutes) — only the window and the probes.
        let wait_storm = env_or("SPLIT_BENCH_STORM", "1") == "1";

        // ── The first call to each child.
        let mut first = Vec::new();
        for left in [true, false].into_iter().filter(|_| wait_storm) {
            let key = key_in_half("Probe", left, "first");
            let t = Instant::now();
            ping_until_done(&client, &url, &key).await?;
            first.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        // Recovery storm: `Live/hold` turns that becoming leader dispatches again.
        // Before the fix, each child's `recover` dispatched a turn again for
        // **every** suspended invocation — N turns; now an invocation sleeping
        // for an hour has nothing to wake it, so it must be 0. Measure until the
        // turn count stays still for `QUIET` (or reaches `cap` turns — the old
        // storm), and return (time of the last turn, extra turns).
        const QUIET: Duration = Duration::from_secs(3);
        let storm = |base: usize, cap: usize| {
            let sdk = &sdk;
            async move {
                let t = Instant::now();
                let (mut seen, mut last) = (base, t);
                loop {
                    let now = sdk.starts_of("Live", "hold").len();
                    if now != seen {
                        (seen, last) = (now, Instant::now());
                    }
                    if seen >= cap || last.elapsed() >= QUIET {
                        return Ok::<_, anyhow::Error>((last, seen - base));
                    }
                    ensure!(t.elapsed() < Duration::from_secs(900), "recovery storm never ends");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        let (split_storm_ms, split_storm_turns) = if wait_storm {
            let (last, turns) = storm(n, 2 * n).await?;
            (Some(last.saturating_duration_since(t_split + busy).as_secs_f64() * 1000.0), Some(turns))
        } else {
            (None, None)
        };
        let storm_end = (Instant::now() - origin).as_secs_f64() * 1000.0;
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.store(true, Ordering::Relaxed);
        // A probe stuck behind the recovery storm must not hold the bench: wait one
        // beat, then drop it — the recorded samples remain.
        for p in probes {
            if let Ok(r) = tokio::time::timeout(Duration::from_secs(2), p).await {
                r??;
            }
        }
        let samples: Vec<(f64, f64)> = shared_samples.lock().map(|v| v.clone()).unwrap_or_default();
        let pick = |from: f64, to: f64| -> Vec<f64> {
            samples.iter().filter(|(t, l)| t + l >= from && *t <= to).map(|s| s.1).collect()
        };
        let stats = |mut v: Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            serde_json::json!({
                "n": v.len(),
                "p50Ms": v.get(v.len() / 2).copied().unwrap_or(0.0),
                "maxMs": v.last().copied().unwrap_or(0.0),
            })
        };
        let probe = serde_json::json!({
            "before": stats(pick(0.0, split_at - 1.0)),
            "duringSplit": stats(pick(split_at, split_end)),
            "duringStorm": stats(pick(split_end, storm_end)),
            "after": stats(pick(storm_end, f64::MAX)),
        });

        // ── Merge back.
        let children: Vec<u64> = split["ranges"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|r| r["range"].as_u64())
            .collect();
        ensure!(children.len() == 2, "{split}");
        let before = counted.counts();
        let (code, merge, merge_took, merge_busy) = post_retrying(
            &client,
            &format!("{url}/ranges/merge?left={}&right={}", children[0], children[1]),
            "",
            Duration::from_secs(180),
        )
        .await?;
        ensure!(code == 200, "merge: {code} {merge}");
        let merge_ms = merge_took.as_secs_f64() * 1000.0;
        let merge_counts = counted.counts().since(&before);
        let t_merged = Instant::now();
        // The storm after the merge (old version: N more turns) —
        // `SPLIT_BENCH_MERGE_STORM=0` skips waiting for it.
        let (merge_storm_ms, merge_storm_turns) =
            if wait_storm && env_or("SPLIT_BENCH_MERGE_STORM", "1") == "1" {
                let base = sdk.starts_of("Live", "hold").len();
                let (last, turns) = storm(base, base + n).await?;
                (
                    Some((last.saturating_duration_since(t_merged) + merge_took).as_secs_f64() * 1000.0),
                    Some(turns),
                )
            } else {
                (None, None)
            };

        let out = serde_json::json!({
            "store": store_name,
            "n": n,
            "bulk": bulk,
            "wal": wal,
            "loadS": load_s,
            "parentMiB": sst_bytes as f64 / 1048576.0,
            "parentObjects": objects,
            "split": {
                "wallMs": split_ms,
                "busyWaitMs": busy.as_secs_f64() * 1000.0,
                "report": split["report"],
                "store": summarize(&split_counts),
            },
            "probe": probe,
            "firstServeMs": first,
            "recoverStormMs": split_storm_ms,
            "recoverTurns": split_storm_turns,
            "merge": {
                "wallMs": merge_ms,
                "busyWaitMs": merge_busy.as_secs_f64() * 1000.0,
                "recoverStormMs": merge_storm_ms,
                "recoverTurns": merge_storm_turns,
                "report": merge["report"],
                "store": summarize(&merge_counts),
            },
        });
        println!("SPLIT_BENCH {}", serde_json::to_string(&out)?);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    node.shutdown().await;
    // ── Clean up.
    match &dir {
        Some(d) => {
            let _ = std::fs::remove_dir_all(d);
        }
        None => {
            let n = delete_under(&main_store, &root)
                .await
                .context("clean the bucket")?;
            eprintln!("deleted {n} objects under {root}");
        }
    }
    result
}
