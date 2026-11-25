// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Cache bench** — the RAM block cache hit rate, the disk cache hit rate, and SST
//! GETs to the store **per turn** across four scenarios, first without a disk cache,
//! then with one:
//!
//! 1. **steady:** one node with four ranges, `CACHE_BENCH_KEYS` virtual objects
//!    carrying state flushed to SSTs, then `CACHE_BENCH_TURNS` turns spread evenly
//!    over the keys (each turn reads the object's state — an SST read once it is no
//!    longer in the memtable);
//! 2. **restart:** stop the node, reopen it on the same bucket (and the same cache
//!    directory): opening the ranges + the first turns;
//! 3. **split:** `POST /ranges/0/split`, then turns on every key;
//! 4. **leader change under chaos:** add a second node; for `CACHE_BENCH_CHAOS_S`
//!    seconds, every `CACHE_BENCH_KILL_EVERY_S` seconds one node is "kill -9"ed
//!    (store frozen then dropped, like `common::freeze`) and rebuilt after 4 s with
//!    the same id and the same cache directory, while the load runs continuously.
//!
//! Numbers per scenario: completed turns, SST GETs (`compacted/*.sst`) to the object
//! store per turn, the RAM block cache hit rate (`cairnflow_range_block_cache_*`,
//! summed over every range of every node; the counters restart every time a range
//! becomes leader, so it is a lower bound when there are kills), the disk cache hit
//! rate (`cairnflow_disk_cache_*`).
//!
//! Not run in the normal test suite (`#[ignore]`). Run:
//!
//! ```sh
//! CACHE_BENCH_CHAOS_S=600 \
//!   cargo test --release -p cairn-server --test cache_bench -- --ignored --nocapture
//! ```
//!
//! Results: `bench/query/README.md`.

// The test runs on real time, network and processes; only `sim_cluster` must be deterministic.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::RunningNode,
    cairn_store::{
        disk_cache::DiskCacheConfig,
        testkit::{FaultyStore, Op, PathClass},
    },
    common::fake_sdk::{FakeSdk, Step},
    slatedb::object_store::memory::InMemory,
    std::{
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    },
};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

const TTL: Duration = Duration::from_millis(1500);
const PARTITIONS: u16 = 4;

/// The shared bucket, the node builds (a `FaultyStore` each time, to count GETs).
struct Bench {
    shared: Arc<InMemory>,
    stores: Vec<Arc<FaultyStore>>,
    cache_root: Option<PathBuf>,
}

impl Bench {
    async fn start(&mut self, id: &str) -> Result<RunningNode> {
        let store = FaultyStore::wrap(self.shared.clone());
        self.stores.push(store.clone());
        let cache = self
            .cache_root
            .as_ref()
            .map(|r| DiskCacheConfig::new(r.join(id)));
        common::start_cluster_node_on(&store, id, PARTITIONS, TTL, |cfg| {
            // Flush the memtable to L0 early: the next turn reads the state from an SST.
            cfg.node_wal.frame_age_flush = Duration::from_secs(1);
            cfg.disk_cache = cache;
        })
        .await
    }

    /// SST GETs that reached the object store, over every node build so far.
    fn sst_gets(&self) -> u64 {
        self.stores
            .iter()
            .flat_map(|s| s.counts_by_source_object())
            .filter(|(_, op, class, kind, _)| {
                *op == Op::Get && *class == PathClass::Compacted && *kind == "sst"
            })
            .map(|(.., n)| n)
            .sum()
    }
}

/// The sum of every sample line of `name` in `/metrics`.
fn sum_metric(text: &str, name: &str) -> u64 {
    text.lines()
        .filter(|l| {
            l.strip_prefix(name)
                .is_some_and(|r| r.starts_with(' ') || r.starts_with('{'))
        })
        .filter_map(|l| l.rsplit(' ').next()?.trim().parse::<f64>().ok())
        .map(|v| v as u64)
        .sum()
}

#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    sst_gets: u64,
    ram_hits: u64,
    ram_misses: u64,
    disk_hits: u64,
    disk_misses: u64,
}

async fn counters(bench: &Bench, urls: &[String]) -> Counters {
    let mut c = Counters {
        sst_gets: bench.sst_gets(),
        ..Default::default()
    };
    for u in urls {
        let Ok(r) = http().get(format!("{u}/metrics")).send().await else {
            continue;
        };
        let Ok(t) = r.text().await else { continue };
        c.ram_hits += sum_metric(&t, "cairnflow_range_block_cache_hits_total");
        c.ram_misses += sum_metric(&t, "cairnflow_range_block_cache_misses_total");
        c.disk_hits += sum_metric(&t, "cairnflow_disk_cache_hits_total");
        c.disk_misses += sum_metric(&t, "cairnflow_disk_cache_misses_total");
    }
    c
}

fn ratio(h: u64, m: u64) -> String {
    if h + m == 0 {
        "—".into()
    } else {
        format!("{:.1} %", 100.0 * h as f64 / (h + m) as f64)
    }
}

/// One result row: `after - before` for GETs, and the node's counters — `reset`
/// = the node's counters started again from 0 in the scenario (a new node), take `after` as is.
fn row(name: &str, turns: u64, before: Counters, after: Counters, reset: bool) -> String {
    let d = |a: u64, b: u64| if reset { a } else { a.saturating_sub(b) };
    let gets = after.sst_gets - before.sst_gets;
    format!(
        "| {name} | {turns} | {gets} | {:.2} | {} | {} |",
        gets as f64 / turns.max(1) as f64,
        ratio(
            d(after.ram_hits, before.ram_hits),
            d(after.ram_misses, before.ram_misses)
        ),
        ratio(
            d(after.disk_hits, before.disk_hits),
            d(after.disk_misses, before.disk_misses)
        ),
    )
}

/// One HTTP client for the whole bench, reusing connections (keep-alive). A new
/// client per turn (`common::invoke`) leaves one `TIME_WAIT` connection per request:
/// the tens of thousands of turns of the chaos scenario exhaust the machine's
/// ephemeral port range and break other tests running at the same time.
fn http() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Call `Obj/set` key `k` on whichever node accepts (503 ⇒ the other node), bounded.
async fn call(urls: &[String], k: u64) -> bool {
    let key = format!("k{k}");
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        for u in urls {
            let sent = http()
                .post(format!("{u}/invoke/Obj/set?key={key}"))
                .body("{}")
                .send()
                .await;
            if let Ok(r) = sent {
                let ok = r.status().as_u16() == 200;
                // Read the whole body so the connection goes back to the pool.
                let _ = r.bytes().await;
                if ok {
                    return true;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// `n` turns spread evenly over `keys` keys, `conc` in parallel. Returns completed turns.
async fn load(urls: &[String], keys: u64, n: u64, conc: u64, from: u64) -> u64 {
    let urls = Arc::new(urls.to_vec());
    let mut set = tokio::task::JoinSet::new();
    for w in 0..conc {
        let urls = urls.clone();
        set.spawn(async move {
            let mut ok = 0;
            let mut i = w;
            while i < n {
                if call(&urls, (from + i * 7919) % keys).await {
                    ok += 1;
                }
                i += conc;
            }
            ok
        });
    }
    let mut ok = 0;
    while let Some(r) = set.join_next().await {
        ok += r.unwrap_or(0);
    }
    ok
}

async fn scenario_run(disk: bool) -> Result<Vec<String>> {
    let keys = env_u64("CACHE_BENCH_KEYS", 2_000);
    let turns = env_u64("CACHE_BENCH_TURNS", 2_000);
    let chaos_s = env_u64("CACHE_BENCH_CHAOS_S", 120);
    let kill_every = env_u64("CACHE_BENCH_KILL_EVERY_S", 20);
    let conc = 16;
    let cache_root = disk.then(|| {
        std::env::temp_dir().join(format!(
            "cairn-cache-bench-{}-{}",
            std::process::id(),
            cairn_store::entropy::now_ms()
        ))
    });
    let mut bench = Bench {
        shared: Arc::new(InMemory::new()),
        stores: Vec::new(),
        cache_root: cache_root.clone(),
    };
    let blob = "x".repeat(2048);
    let sdk = FakeSdk::builder()
        .service(
            "Obj",
            "set",
            vec![
                Step::SetState {
                    key: "v".into(),
                    value: bytes::Bytes::from(format!("\"{blob}\"")),
                },
                Step::output("\"ok\""),
            ],
        )
        .spawn()
        .await?;
    let mut out = Vec::new();

    // Load: one object with state per key, then wait for the flush to SSTs.
    let a = bench.start("a").await?;
    let urls = vec![a.url()];
    common::register(&urls[0], &sdk.url).await?;
    let loaded = load(&urls, keys, keys, conc, 0).await;
    ensure!(loaded == keys, "loaded {loaded}/{keys}");
    tokio::time::sleep(Duration::from_secs(4)).await;

    // 1. Steady.
    let before = counters(&bench, &urls).await;
    let done = load(&urls, keys, turns, conc, 1).await;
    let after = counters(&bench, &urls).await;
    out.push(row("steady", done, before, after, false));

    // 2. Restart (same bucket, same cache directory).
    a.shutdown().await;
    let before = counters(&bench, &[]).await;
    let a = bench.start("a").await?;
    let urls = vec![a.url()];
    let done = load(&urls, keys, turns / 4, conc, 2).await;
    let after = counters(&bench, &urls).await;
    out.push(row("restart", done, before, after, true));

    // 3. Split range 0, then load on every key.
    let before = counters(&bench, &urls).await;
    let client = reqwest::Client::new();
    let t0 = Instant::now();
    loop {
        let r = client
            .post(format!("{}/ranges/0/split", urls[0]))
            .send()
            .await?;
        let code = r.status().as_u16();
        if code == 200 {
            break;
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(120),
            "split: {code} {}",
            r.text().await.unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let done = load(&urls, keys, turns / 4, conc, 3).await;
    let after = counters(&bench, &urls).await;
    out.push(row("split", done, before, after, false));

    // 4. Leader change under chaos: two nodes, killed/rebuilt in turn.
    let a_store = bench.stores.last().expect("store").clone();
    let b = bench.start("b").await?;
    let b_store = bench.stores.last().expect("store").clone();
    type Slot = (String, Option<RunningNode>, Arc<FaultyStore>);
    let mut nodes: Vec<Slot> = vec![
        ("a".into(), Some(a), a_store),
        ("b".into(), Some(b), b_store),
    ];
    let all_urls = |nodes: &[Slot]| {
        nodes
            .iter()
            .filter_map(|(_, n, _)| n.as_ref().map(|n| n.url()))
            .collect::<Vec<_>>()
    };
    let before = counters(&bench, &all_urls(&nodes)).await;
    let mut done = 0;
    let mut kills = 0;
    let mut acc = Counters::default();
    let end = Instant::now() + Duration::from_secs(chaos_s);
    let mut victim = 0;
    let mut round = 10;
    while Instant::now() < end {
        let urls = all_urls(&nodes);
        let until = Instant::now() + Duration::from_secs(kill_every);
        while Instant::now() < until {
            done += load(&urls, keys, 64, conc, round).await;
            round += 1;
        }
        // The counters of the node about to die: add them before they are lost.
        let c = counters(
            &bench,
            &[nodes[victim]
                .1
                .as_ref()
                .map(|n| n.url())
                .unwrap_or_default()],
        )
        .await;
        acc.ram_hits += c.ram_hits;
        acc.ram_misses += c.ram_misses;
        acc.disk_hits += c.disk_hits;
        acc.disk_misses += c.disk_misses;
        if let Some(n) = nodes[victim].1.take() {
            common::freeze(&nodes[victim].2);
            drop(n);
        }
        kills += 1;
        tokio::time::sleep(Duration::from_secs(4)).await;
        let id = nodes[victim].0.clone();
        nodes[victim].1 = Some(bench.start(&id).await?);
        nodes[victim].2 = bench.stores.last().expect("store").clone();
        victim = 1 - victim;
    }
    let live = counters(&bench, &all_urls(&nodes)).await;
    let after = Counters {
        sst_gets: live.sst_gets,
        ram_hits: acc.ram_hits + live.ram_hits,
        ram_misses: acc.ram_misses + live.ram_misses,
        disk_hits: acc.disk_hits + live.disk_hits,
        disk_misses: acc.disk_misses + live.disk_misses,
    };
    out.push(row(
        &format!("leader change under chaos ({chaos_s} s, {kills} kill)"),
        done,
        Counters {
            sst_gets: before.sst_gets,
            ..Default::default()
        },
        after,
        true,
    ));
    for (_, n, _) in nodes {
        if let Some(n) = n {
            n.shutdown().await;
        }
    }
    if let Some(r) = cache_root {
        let _ = std::fs::remove_dir_all(r);
    }
    Ok(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a bench — run by hand, see the top of the file"]
async fn cache_scenarios() -> Result<()> {
    let mut lines = vec![
        "| scenario | turns | SST GETs to store | GET/turn | RAM cache hits | disk cache hits |"
            .to_string(),
        "|---|---|---|---|---|---|".to_string(),
    ];
    for disk in [false, true] {
        let rows = scenario_run(disk)
            .await
            .map_err(|e| anyhow!("disk cache {disk}: {e:#}"))?;
        lines.push(format!(
            "| **{}** | | | | | |",
            if disk {
                "with disk cache"
            } else {
                "no disk cache"
            }
        ));
        lines.extend(rows);
    }
    println!("CACHE_BENCH\n{}", lines.join("\n"));
    Ok(())
}
