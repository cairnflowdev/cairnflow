// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Automatic split by load and by size.
//!
//! Four questions:
//!
//! 1. **Per-range load statistics**: `GET /ranges/stats` and the metric
//!    `cairnflow_range_turn_ms_total{range}` show which range eats the turn slots.
//! 2. **The right range, the right key**: skewed load (one range takes most
//!    turns, concentrated in a quarter of its key span) ⇒ suggest exactly that
//!    range, and the split key divides the **load**, not the key span — the
//!    midpoint would leave all the load on one side. One hot owner ⇒
//!    `hot_owner`, no split. A range over the size cap ⇒ suggested for `size`.
//! 3. **Mode `on`**: splits exactly once at the load-balancing key, then the
//!    cooldown and the per-node cap keep the cluster from splitting in a cascade.
//! 4. **One intent cluster-wide**: while the table still has an unfinished
//!    split/merge on another range there is no automatic split; once that intent
//!    is rolled back, the split goes ahead.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    cairn_server::{autosplit::AutoSplitMode, node::RunningNode},
    cairn_store::{
        partition_key_for_target, PartitionId, PartitionTable, RangeEntry, RangeIntent,
        TableUpdate, TenantId,
    },
    common::fake_sdk::{FakeSdk, Step},
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    },
};

/// The boundary between the two ranges of a two-range node: `[0, MID)` and `[MID, MAX]`.
const MID: u64 = 1 << 63;
const SERVICE: &str = "Load";

/// `(object key, partition key)`.
type Keys = Vec<(String, u64)>;

/// `n` object keys of [`SERVICE`] whose partition key lies in `[lo, hi)`, searched
/// with exactly the hash the node uses.
fn keys_in(lo: u64, hi: u64, prefix: &str, n: usize) -> Keys {
    (0..1_000_000)
        .map(|i| format!("{prefix}-{i}"))
        .filter_map(|k| {
            let pk = partition_key_for_target(&TenantId::default(), None, SERVICE, &k, None)
                .expect("a key exists")
                .0;
            (lo <= pk && pk < hi).then_some((k, pk))
        })
        .take(n)
        .collect()
}

/// A lone node, two ranges, automatic split per `tweak`.
async fn node_with(
    max_turns: usize,
    tweak: impl FnOnce(&mut cairn_server::autosplit::AutoSplitConfig),
) -> Result<RunningNode> {
    Ok(node_and_stores(max_turns, tweak).await?.0)
}

/// Like [`node_with`], plus the node's store (the same `memory://` bucket).
async fn node_and_stores(
    max_turns: usize,
    tweak: impl FnOnce(&mut cairn_server::autosplit::AutoSplitConfig),
) -> Result<(RunningNode, cairn_store::ObjectStores)> {
    let mut cfg = common::solo_config(2);
    cfg.limits.max_concurrent_turns = max_turns;
    tweak(&mut cfg.auto_split);
    let stores = cfg.stores.clone();
    Ok((common::start_led(cfg).await?, stores))
}

/// Fake SDK: every turn stands for 100 ms — the shape of a real `ctx.run`, enough
/// for the turn slots to be what gets used up.
async fn stalling_sdk(node: &RunningNode) -> Result<FakeSdk> {
    let sdk = FakeSdk::builder()
        .object(SERVICE, "work", vec![Step::Stall(100), Step::output("ok")])
        .spawn()
        .await?;
    common::register(&node.url(), &sdk.url).await?;
    Ok(sdk)
}

/// One client per key calling continuously until `stop`; counts completions per key.
struct Load {
    stop: Arc<AtomicBool>,
    done: Arc<Mutex<BTreeMap<String, u64>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Load {
    fn start(url: &str, keys: &[(String, u64)]) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(Mutex::new(BTreeMap::new()));
        let tasks = keys
            .iter()
            .map(|(k, _)| {
                let (url, k, stop, done) = (url.to_string(), k.clone(), stop.clone(), done.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        match common::invoke(&url, SERVICE, "work", Some(&k), "x").await {
                            Ok((200, _)) => {
                                *done.lock().unwrap().entry(k.clone()).or_default() += 1
                            }
                            _ => tokio::time::sleep(Duration::from_millis(20)).await,
                        }
                    }
                })
            })
            .collect();
        Self { stop, done, tasks }
    }

    fn counts(&self) -> BTreeMap<String, u64> {
        self.done.lock().unwrap().clone()
    }

    async fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

/// The share of the load (by completions) of `keys` whose partition key is `< at`.
fn left_share(counts: &BTreeMap<String, u64>, keys: &[(String, u64)], at: u64) -> f64 {
    let (mut left, mut total) = (0u64, 0u64);
    for (k, pk) in keys {
        let c = counts.get(k).copied().unwrap_or(0);
        total += c;
        if *pk < at {
            left += c;
        }
    }
    left as f64 / total.max(1) as f64
}

async fn get_json(url: String) -> Result<serde_json::Value> {
    let resp = reqwest::get(&url).await?;
    let code = resp.status();
    let text = resp.text().await?;
    ensure!(code.is_success(), "{url} returned {code}: {text}");
    serde_json::from_str(&text).with_context(|| format!("{url}: not JSON: {text}"))
}

/// Ask `GET /ranges/suggestions` until `pick` picks a row.
async fn wait_suggestion(
    node: &RunningNode,
    within: Duration,
    pick: impl Fn(&serde_json::Value) -> bool,
) -> Result<serde_json::Value> {
    let t0 = Instant::now();
    let mut last = serde_json::Value::Null;
    while t0.elapsed() < within {
        last = get_json(format!("{}/ranges/suggestions", node.url())).await?;
        if let Some(row) = last
            .as_array()
            .and_then(|rows| rows.iter().find(|r| pick(r)))
        {
            return Ok(row.clone());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(anyhow!(
        "no expected suggestion after {within:?}; last: {last}"
    ))
}

fn hex_u64(v: &serde_json::Value) -> Result<u64> {
    let s = v
        .as_str()
        .ok_or_else(|| anyhow!("value is not a hex string: {v}"))?;
    u64::from_str_radix(s, 16).map_err(|e| anyhow!("{s:?}: {e}"))
}

/// Skewed load: 12 hot keys packed into **the first quarter** of range 0, 2 cold
/// keys in range 1. Range 0 gets about 85 % of the turns.
fn skewed_keys() -> (Keys, Keys) {
    (
        keys_in(0, MID / 4, "hot", 12),
        keys_in(MID, u64::MAX, "cold", 2),
    )
}

/// `GET /ranges/stats` (the leader returns its own ranges' numbers) and the
/// per-range metric show which range eats the turn slots; a range with no turns does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_stats_show_turn_time_per_range() -> Result<()> {
    let node = node_with(8, |c| c.mode = AutoSplitMode::Off).await?;
    let _sdk = stalling_sdk(&node).await?;
    let keys = keys_in(0, MID, "count", 3);
    for (k, _) in &keys {
        for _ in 0..3 {
            let (code, body) = common::invoke(&node.url(), SERVICE, "work", Some(k), "x").await?;
            ensure!(code == 200, "invoke {k}: {code} {body}");
        }
    }
    // One aggregation tick (1 s) is enough for numbers to enter the window; totals are immediate.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let stats = get_json(format!("{}/ranges/stats", node.url())).await?;
    let rows = stats["ranges"]
        .as_array()
        .ok_or_else(|| anyhow!("no ranges: {stats}"))?;
    let row = |p: u64| rows.iter().find(|r| r["range"] == p).cloned();
    let hot = row(0).ok_or_else(|| anyhow!("range 0 missing: {stats}"))?;
    ensure!(
        hot["turnMsTotal"].as_f64().unwrap_or(0.0) >= 9.0 * 100.0,
        "range 0: {hot}"
    );
    ensure!(
        hot["opsTotal"].as_u64().unwrap_or(0) >= 9,
        "range 0 counts commands: {hot}"
    );
    ensure!(
        hot["windows"].as_u64().unwrap_or(0) >= 1,
        "range 0 has no window yet: {hot}"
    );
    let top = hex_u64(&hot["topOwner"]["ownerPk"])?;
    ensure!(
        keys.iter().any(|(_, pk)| *pk == top),
        "the hottest owner must be a key that was called: {hot}"
    );
    if let Some(cold) = row(1) {
        ensure!(
            cold["turnMsTotal"].as_f64().unwrap_or(0.0) == 0.0,
            "range 1 has no turns: {cold}"
        );
    }
    let metrics = reqwest::get(format!("{}/metrics", node.url()))
        .await?
        .text()
        .await?;
    let line = metrics
        .lines()
        .find(|l| l.starts_with("cairnflow_range_turn_ms_total{range=\"0\"}"))
        .ok_or_else(|| anyhow!("no per-range metric:\n{metrics}"))?;
    let v: f64 = line.rsplit(' ').next().unwrap_or("0").parse()?;
    ensure!(v >= 900.0, "range 0 metric: {line}");
    Ok(())
}

/// In mode `suggest`, skewed load ⇒ suggest exactly the hot range, reason `load`,
/// and a split key that divides the load within ±20 % (the left side keeps 30–70 %
/// of the range's completed turns). Range 0's midpoint (`MID/2`) leaves **all** the
/// load on the left — this test fails with the midpoint. Nothing is split.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn skewed_load_suggests_the_hot_range_with_a_load_balanced_key() -> Result<()> {
    let node = node_with(8, |c| {
        c.mode = AutoSplitMode::Suggest;
        c.hot_share = 0.4;
        c.hot_secs = 3;
    })
    .await?;
    let _sdk = stalling_sdk(&node).await?;
    let (hot, cold) = skewed_keys();
    let load = Load::start(&node.url(), &[hot.clone(), cold].concat());
    let row = wait_suggestion(&node, Duration::from_secs(30), |r| r["range"] == 0).await;
    let counts = load.counts();
    load.stop().await;
    let row = row?;
    ensure!(row["reason"] == "load", "reason must be load: {row}");
    let at = hex_u64(&row["at"])?;
    ensure!(0 < at && at < MID, "split key must lie in range 0: {row}");
    let share = left_share(&counts, &hot, at);
    ensure!(
        (0.3..=0.7).contains(&share),
        "key {at:016x} leaves {:.0} % of the load on the left — must be within 50 ± 20 % ({row})",
        share * 100.0
    );
    ensure!(
        row["balance"].as_f64().is_some_and(|b| b < 0.8),
        "balance point: {row}"
    );
    let rows = get_json(format!("{}/ranges/suggestions", node.url())).await?;
    ensure!(
        !rows
            .as_array()
            .into_iter()
            .flatten()
            .any(|r| r["range"] == 1),
        "the cold range must not be suggested: {rows}"
    );
    ensure!(
        node.cluster.table().ranges().len() == 2,
        "suggest must not split"
    );
    Ok(())
}

/// Every turn of the hot range goes to **one** owner ⇒ it cannot be split (an owner
/// does not straddle two ranges) — the endpoint returns reason `hot_owner` with its
/// `owner_pk`, and no split key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_hot_owner_is_reported_not_split() -> Result<()> {
    let node = node_with(2, |c| {
        c.mode = AutoSplitMode::On;
        c.hot_share = 0.3;
        c.hot_secs = 3;
    })
    .await?;
    let _sdk = stalling_sdk(&node).await?;
    let key = keys_in(0, MID, "one", 1);
    // Two clients on the same key: one turn is always running (VO `EXCLUSIVE`), i.e.
    // one of the node's two slots — 50 %.
    let load = Load::start(&node.url(), &[key.clone(), key.clone()].concat());
    let row = wait_suggestion(&node, Duration::from_secs(30), |r| r["range"] == 0).await;
    load.stop().await;
    let row = row?;
    ensure!(row["reason"] == "hot_owner", "must be hot_owner: {row}");
    ensure!(
        hex_u64(&row["owner"])? == key[0].1,
        "owner_pk of the hot owner: {row}"
    );
    ensure!(row["at"].is_null(), "hot_owner has no split key: {row}");
    ensure!(
        node.cluster.table().ranges().len() == 2,
        "hot_owner must not be split, not even in mode on"
    );
    Ok(())
}

/// A range over `max_range_bytes` (the visible part, manifest read after
/// `size_check_diff` has been written) ⇒ suggested for `size`, the key inside the range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_range_is_suggested_for_size() -> Result<()> {
    let node = node_with(8, |c| {
        c.mode = AutoSplitMode::Suggest;
        c.max_range_bytes = 1;
        c.size_check_diff = 1;
        c.size_check_every = Duration::ZERO;
    })
    .await?;
    let _sdk = stalling_sdk(&node).await?;
    for (k, _) in keys_in(0, MID, "big", 6) {
        let (code, body) = common::invoke(&node.url(), SERVICE, "work", Some(&k), "x").await?;
        ensure!(code == 200, "invoke {k}: {code} {body}");
    }
    let h = node
        .partitions
        .get(PartitionId(0))
        .ok_or_else(|| anyhow!("no engine for range 0"))?;
    h.flush_memtable().await?;
    // One write **after** the flush: a measurement may have run between the last
    // write and the flush (seeing 0 bytes, resetting the byte counter) — without
    // another write there would be no further measurement. This write pushes the
    // counter up, and the next measurement reads a manifest that has the flush's SST.
    let (k, _) = keys_in(0, MID, "after-flush", 1).remove(0);
    let (code, body) = common::invoke(&node.url(), SERVICE, "work", Some(&k), "x").await?;
    ensure!(code == 200, "invoke {k}: {code} {body}");
    let row = wait_suggestion(&node, Duration::from_secs(20), |r| r["range"] == 0).await?;
    ensure!(row["reason"] == "size", "reason must be size: {row}");
    ensure!(
        row["sizeBytes"].as_u64().unwrap_or(0) > 0,
        "measured size: {row}"
    );
    let at = hex_u64(&row["at"])?;
    ensure!(0 < at && at < MID, "split key inside range 0: {row}");
    Ok(())
}

/// **Mode `on`.** Skewed load ⇒ split **exactly once**, at the load-balancing
/// key; afterwards the two children's cooldown and the node's once-a-minute cap
/// keep the cluster from cascading splits even though the load keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn on_mode_splits_once_at_the_balanced_key_then_cools_down() -> Result<()> {
    let node = node_with(8, |c| {
        c.mode = AutoSplitMode::On;
        c.hot_share = 0.4;
        c.hot_secs = 3;
    })
    .await?;
    let _sdk = stalling_sdk(&node).await?;
    let (hot, cold) = skewed_keys();
    let load = Load::start(&node.url(), &[hot.clone(), cold].concat());
    let t0 = Instant::now();
    while node.cluster.table().ranges().len() < 3 && t0.elapsed() < Duration::from_secs(40) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let counts = load.counts();
    let table = node.cluster.table();
    ensure!(
        table.ranges().len() == 3,
        "range 0 must split on its own: {:?}",
        table.ranges()
    );
    let right = table
        .ranges()
        .iter()
        .filter(|r| r.end < MID && r.start > 0)
        .map(|r| r.start)
        .next()
        .ok_or_else(|| anyhow!("no right child of range 0: {:?}", table.ranges()))?;
    let share = left_share(&counts, &hot, right);
    ensure!(
        (0.3..=0.7).contains(&share),
        "split at {right:016x} leaves {:.0} % of the load on the left",
        share * 100.0
    );
    // The load keeps running: no second split within the cooldown.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let n = node.cluster.table().ranges().len();
    let rows = get_json(format!("{}/ranges/suggestions", node.url())).await?;
    load.stop().await;
    ensure!(n == 3, "cascade: {n} ranges after the cooldown — {rows}");
    for r in rows.as_array().into_iter().flatten() {
        if r["reason"] == "load" || r["reason"] == "size" {
            ensure!(
                matches!(r["blockedBy"].as_str(), Some("cooldown" | "node_rate")),
                "a hot child range must be blocked by cooldown/node_rate: {r}"
            );
        }
    }
    Ok(())
}

/// What [`split_then_restart`] saw after the restart.
struct AfterRestart {
    /// The ranges the first split created (range 0's children).
    children: Vec<u32>,
    /// The range ids in the table after the restart and `watch`.
    ranges: Vec<u32>,
    /// `GET /ranges/suggestions` at that moment.
    rows: serde_json::Value,
}

/// Splits range 0 once in `on` mode under skewed load (per `tweak`), restarts the node on
/// the same bucket, runs the same load for `watch`, and returns what the table and the
/// suggestions say then.
async fn split_then_restart(
    tweak: impl Fn(&mut cairn_server::autosplit::AutoSplitConfig),
    watch: Duration,
) -> Result<AfterRestart> {
    // A low threshold: both children of the first split must be hot after the restart,
    // or the test would pass without exercising what it checks.
    let base = |c: &mut cairn_server::autosplit::AutoSplitConfig| {
        c.mode = AutoSplitMode::On;
        c.hot_share = 0.2;
        c.hot_secs = 3;
        tweak(c);
    };
    let (node, stores) = node_and_stores(8, base).await?;
    let sdk = stalling_sdk(&node).await?;
    let ids = |node: &RunningNode| -> Vec<u32> {
        node.cluster
            .table()
            .ranges()
            .iter()
            .map(|r| r.id.0)
            .collect()
    };
    let start = ids(&node);
    let (hot, cold) = skewed_keys();
    let keys = [hot, cold].concat();
    let load = Load::start(&node.url(), &keys);
    let t0 = Instant::now();
    while node.cluster.table().ranges().len() < 3 && t0.elapsed() < Duration::from_secs(40) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    load.stop().await;
    ensure!(
        node.cluster.table().ranges().len() == 3,
        "range 0 must split once before the restart: {:?}",
        node.cluster.table().ranges()
    );
    let children: Vec<u32> = ids(&node)
        .into_iter()
        .filter(|r| !start.contains(r))
        .collect();
    node.shutdown().await;
    let mut cfg = common::solo_config(2);
    cfg.limits.max_concurrent_turns = 8;
    base(&mut cfg.auto_split);
    cfg.stores = stores;
    let node = common::start_led(cfg).await?;
    let load = Load::start(&node.url(), &keys);
    tokio::time::sleep(watch).await;
    let ranges = ids(&node);
    let rows = get_json(format!("{}/ranges/suggestions", node.url())).await?;
    load.stop().await;
    node.shutdown().await;
    drop(sdk);
    ensure!(
        ranges.len() > 3
            || rows
                .as_array()
                .is_some_and(|a| a.iter().any(|r| r["reason"] == "load")),
        "no range was hot after the restart, so nothing was checked: {ranges:?} — {rows}"
    );
    Ok(AfterRestart {
        children,
        ranges,
        rows,
    })
}

/// **The split cooldown survives a restart** (a 24 h soak: the cooldown lived only in
/// the RAM of the node that split, so every `kill -9` erased it, and the range count grew
/// from 8 to 528).
///
/// 1. `on` mode, cooldown 60 s, node interval 1 s (only the cooldown can block). Skewed
///    load splits range 0 into two hot children.
/// 2. Restart the node on the same bucket, keep the load running for 10 s.
///
/// Must see: both children still in the table — they were born less than the cooldown
/// ago, which the bucket still says after the restart. Range 1 never split and carries
/// ~14 % of the load, near the 0.2 threshold, so it may split on its own: that is
/// allowed and not counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn on_mode_keeps_the_cooldown_across_a_restart() -> Result<()> {
    let after = split_then_restart(
        |c| {
            c.cooldown = Duration::from_secs(60);
            c.node_interval = Duration::from_secs(1);
        },
        Duration::from_secs(10),
    )
    .await?;
    ensure!(
        after.children.len() == 2 && after.children.iter().all(|c| after.ranges.contains(c)),
        "a child split again after the restart: children {:?}, ranges {:?} — {}",
        after.children,
        after.ranges,
        after.rows
    );
    Ok(())
}

/// **The per-node split interval survives a restart.**
///
/// 1. `on` mode, no cooldown, node interval 60 s. Skewed load splits range 0.
/// 2. Restart the node on the same bucket, keep the load running for 10 s.
///
/// Must see: still 3 ranges — the latest split on the bucket is younger than the interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn on_mode_keeps_the_node_interval_across_a_restart() -> Result<()> {
    let after = split_then_restart(
        |c| {
            c.cooldown = Duration::ZERO;
            c.node_interval = Duration::from_secs(60);
        },
        Duration::from_secs(10),
    )
    .await?;
    ensure!(
        after.ranges.len() == 3,
        "split again right after the restart: {:?} — {}",
        after.ranges,
        after.rows
    );
    Ok(())
}

/// **One intent cluster-wide.** Range 1 has an unfinished split (an intent in the
/// table, its coordinator as if dead) ⇒ the hot range 0 must not split on its own,
/// the suggestion carries `blockedBy: intent`; once the intent is rolled back, it splits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn on_mode_waits_while_another_intent_is_in_the_table() -> Result<()> {
    let (node, stores) = node_and_stores(8, |c| {
        c.mode = AutoSplitMode::On;
        c.hot_share = 0.4;
        c.hot_secs = 3;
    })
    .await?;
    let _sdk = stalling_sdk(&node).await?;
    // An intent hanging on range 1, as from a coordinator that died after the intent CAS.
    let t = node.cluster.table();
    let marked = node
        .cluster
        .update(t.version(), |t| {
            let ranges = t
                .ranges()
                .iter()
                .map(|r| {
                    if r.id == PartitionId(1) {
                        RangeEntry {
                            intent: Some(RangeIntent::Splitting {
                                at: MID + (1 << 62),
                                left: PartitionId(t.next_range_id()),
                                right: PartitionId(t.next_range_id() + 1),
                            }),
                            ..*r
                        }
                    } else {
                        *r
                    }
                })
                .collect();
            PartitionTable::from_entries(t.version() + 1, t.next_range_id() + 2, ranges)
        })
        .await?;
    ensure!(
        matches!(marked, TableUpdate::Applied(_)),
        "could not plant the fake intent"
    );

    let (hot, _) = skewed_keys();
    let load = Load::start(&node.url(), &hot);
    let row = wait_suggestion(&node, Duration::from_secs(30), |r| {
        r["range"] == 0 && r["blockedBy"] == "intent"
    })
    .await;
    let still = node.cluster.table().ranges().len();
    if row.is_err() || still != 2 {
        load.stop().await;
        return Err(anyhow!("range 1 intent: {still} ranges, {row:?}"));
    }
    // The intent is rolled back (whoever claims the range does this) ⇒ the split may go ahead.
    cairn_server::split::roll_back(
        &node.cluster,
        &stores,
        PartitionId(1),
        cairn_server::split::Who::Opener,
    )
    .await?;
    let t0 = Instant::now();
    while node.cluster.table().ranges().len() < 3 && t0.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    load.stop().await;
    ensure!(
        node.cluster.table().ranges().len() == 3,
        "intent rolled back but no automatic split"
    );
    Ok(())
}

/// **Automatic split can cut a hot call tree inside its prefix.**
///
/// A lone node with two ranges, a new cluster (the batching gate on from the
/// start), automatic split `on`, two turn slots. One tree: `Tree/run` (unkeyed,
/// random pk) issues one turn of 40 unkeyed `Twig/work` (`ctx.call` not awaited),
/// each standing for 300 ms in its turn — both slots always busy, all on the root's
/// range. Every `Twig` keeps the root's high 32 bits, so the hot range's `owner_pk`
/// samples lie entirely in the tree's 2³² window. The root runs only a few short
/// turns: its weight does not drag the median towards its pk (with sequential
/// calls the root would run 41 turns, and under ambiguous faults each turn is heavy
/// enough for the split point to land right by the root — correct for load
/// balancing, wrong for the test's question).
///
/// Must see: the root's range splits on its own **once**, the split point lies
/// inside the tree's prefix window (high 32 bits equal to the root's), and
/// afterwards the tree's leaves are on **both** child ranges, each with a
/// sizeable share. Under the old spreading rule the leaves split evenly across
/// the two original ranges, no range is hot: no split.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn on_mode_splits_a_hot_call_tree_inside_its_prefix() -> Result<()> {
    const LEAVES: usize = 40;
    const HI: u64 = cairn_store::CALLEE_PREFIX_MASK;
    let node = node_with(2, |c| {
        c.mode = AutoSplitMode::On;
        c.hot_share = 0.4;
        c.hot_secs = 3;
    })
    .await?;
    let mut steps: Vec<Step> = (0..LEAVES)
        .map(|_| Step::Spawn {
            service: "Twig".into(),
            handler: "work".into(),
            spread: false,
        })
        .collect();
    steps.push(Step::AwaitSpawned);
    steps.push(Step::output("done"));
    let sdk = FakeSdk::builder()
        .service("Tree", "run", steps)
        .service("Twig", "work", vec![Step::Stall(300), Step::output("ok")])
        .spawn()
        .await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let (code, body) = common::send(&url, "Tree", "run", None, "x").await?;
    ensure!(code == 200 || code == 202, "{code} {body}");
    let root = body["id"].as_str().unwrap_or_default().to_string();
    let root_pk = u64::from_str_radix(&root[..16], 16)?;
    let window = root_pk & HI;

    let t0 = Instant::now();
    while node.cluster.table().ranges().len() < 3 && t0.elapsed() < Duration::from_secs(40) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let table = node.cluster.table();
    ensure!(
        table.ranges().len() == 3,
        "the tree's range must split on its own: {:?}",
        table.ranges()
    );
    let cut = table
        .ranges()
        .iter()
        .map(|r| r.start)
        .find(|s| *s != 0 && *s != MID)
        .ok_or_else(|| anyhow!("no new boundary: {:?}", table.ranges()))?;
    ensure!(
        cut & HI == window,
        "split point {cut:016x} must lie inside the tree's prefix window {window:016x}"
    );

    common::within(Duration::from_secs(60), "all 40 leaves ran", async {
        while sdk.starts_of("Twig", "work").len() < LEAVES {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    })
    .await?;
    let table = node.cluster.table();
    let mut per_range: BTreeMap<u32, usize> = BTreeMap::new();
    for s in sdk.starts_of("Twig", "work") {
        let pk = u64::from_str_radix(&s.id_hex()[..16], 16)?;
        ensure!(
            pk & HI == window,
            "leaf {} outside the tree's window",
            s.id_hex()
        );
        *per_range
            .entry(table.partition_of_key(cairn_store::PartitionKey(pk)).0)
            .or_default() += 1;
    }
    ensure!(
        per_range.len() == 2 && per_range.values().all(|n| *n * 100 >= LEAVES * 15),
        "the tree's leaves must be on both child ranges, each side ≥ 15 %: {per_range:?}"
    );
    Ok(())
}

/// **Automatic split stops at the depth cap**: a range standing on
/// `--lineage-auto-split-max-depth` ancestors or more (default 3 —
/// `lineage::tests::lineage_defaults_are_pinned`) is not split automatically: every
/// split makes the chain one longer, and a child only cuts the link when it
/// compacts itself. A manual split still works.
///
/// This test lowers the cap to 1 to pin that the cap is read from configuration
/// (and that a range without ancestors is not blocked); the default cap of 3 is in
/// `::auto_split_refuses_a_range_three_ancestors_deep_by_default`.
///
/// 1. A two-range node, automatic split `suggest`, a 1-byte size cap (every range
///    that has writes is "oversized"), auto-split depth cap 1; the child
///    self-standing loop off (so the depth stays put while we ask).
/// 2. Write into range 0, then fully compact it (data into a sorted run); manual
///    split of 0 ⇒ {a, b}: a stands on 0.
/// 3. Write into a and into range 1 (no ancestors), flush the memtable: the next
///    size measurement suggests splitting both for `size`.
///
/// Must see: a has depth ≥ 1 and a's suggestion carries `blockedBy: depth`; range
/// 1's suggestion is not blocked by depth. The manual split (range 0) succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_split_refuses_a_range_past_the_depth_cap() -> Result<()> {
    let mut cfg = common::solo_config(2);
    cfg.auto_split.mode = AutoSplitMode::Suggest;
    cfg.auto_split.max_range_bytes = 1;
    cfg.auto_split.size_check_diff = 1;
    cfg.auto_split.size_check_every = Duration::ZERO;
    cfg.lineage.depth = 0;
    cfg.lineage.max_auto_split_depth = 1;
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let _sdk = stalling_sdk(&node).await?;
    let write = |lo: u64, hi: u64, tag: &'static str| {
        let url = url.clone();
        async move {
            for (k, _) in keys_in(lo, hi, tag, 2) {
                let (code, body) = common::invoke(&url, SERVICE, "work", Some(&k), "x").await?;
                ensure!(code == 200, "invoke {k}: {code} {body}");
            }
            Ok::<_, anyhow::Error>(())
        }
    };
    let flush = |range: u32| {
        let node = &node;
        async move {
            node.partitions
                .get(PartitionId(range))
                .ok_or_else(|| anyhow!("no engine for range {range}"))?
                .flush_memtable()
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    };
    // Range 0 covers [0, MID): write, fully compact, split by hand.
    write(0, MID, "root").await?;
    flush(0).await?;
    let mut id = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .ok_or_else(|| anyhow!("range 0 has nothing to compact"))?;
    node.partitions
        .get(PartitionId(0))
        .ok_or_else(|| anyhow!("no engine for range 0"))?
        .wake_compactor()
        .await?;
    let t0 = Instant::now();
    while stores.full_compaction_done(0, &mut id).await? != Some(true) {
        ensure!(
            t0.elapsed() < Duration::from_secs(20),
            "full compaction of range 0 did not finish"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let resp = reqwest::Client::new()
        .post(format!("{url}/ranges/0/split"))
        .send()
        .await?;
    let code = resp.status().as_u16();
    let view: serde_json::Value = resp.json().await?;
    ensure!(code == 200, "manual split of range 0: {code} {view}");
    // The left child carries the smaller of the two ids just allocated.
    let a = view["nextRangeId"].as_u64().unwrap_or(0).saturating_sub(2) as u32;
    let depth = stores
        .range_external_depth(PartitionId(a))
        .await?
        .ok_or_else(|| anyhow!("range {a}'s Db has no manifest yet"))?;
    ensure!(depth >= 1, "range {a} must stand on its parent: {depth}");
    for (range, lo, hi) in [(a, 0, MID / 2), (1, MID, u64::MAX)] {
        write(lo, hi, "before-flush").await?;
        flush(range).await?;
        write(lo, hi, "after-flush").await?;
    }
    let row = wait_suggestion(&node, Duration::from_secs(20), |r| {
        r["range"] == a && r["blockedBy"] == "depth"
    })
    .await?;
    ensure!(row["reason"] == "size", "{row}");
    let other = wait_suggestion(&node, Duration::from_secs(20), |r| r["range"] == 1).await?;
    ensure!(
        other["blockedBy"] != "depth",
        "range 1 has no ancestors but is blocked by depth: {other}"
    );
    Ok(())
}

/// **Merge suggestions.** A two-range node, `merge_cold` 2 s.
///
/// 1. No load: both ranges cold for 2 s ⇒ one `merge` row, `range` 0,
///    `mergeWith` 1, `sizeBytes` measured (one manifest read per range), no
///    `at`. Nothing is merged: the table still has two ranges.
/// 2. Load on range 0 ⇒ its cold streak restarts: the `merge` row disappears
///    while the load runs.
/// 3. The load stops ⇒ after 2 s cold the row returns.
/// 4. `merge_cold` 0 ⇒ there is never a `merge` row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cold_adjacent_ranges_are_suggested_for_merge_never_merged() -> Result<()> {
    const COLD: Duration = Duration::from_secs(2);
    let node = node_with(8, |c| {
        c.mode = AutoSplitMode::Suggest;
        c.merge_cold = COLD;
    })
    .await?;
    let _sdk = stalling_sdk(&node).await?;
    let is_merge = |r: &serde_json::Value| r["reason"] == "merge";
    let row = wait_suggestion(&node, Duration::from_secs(20), is_merge).await?;
    ensure!(
        row["range"] == 0 && row["mergeWith"] == 1,
        "pair (0, 1): {row}"
    );
    ensure!(row["sizeBytes"].as_u64().is_some(), "measured size: {row}");
    ensure!(
        row["coldSecs"]
            .as_u64()
            .is_some_and(|s| s >= COLD.as_secs()),
        "cold ≥ 2 s: {row}"
    );
    ensure!(
        row.get("at").is_none_or(|a| a.is_null()),
        "a merge suggestion has no split key: {row}"
    );

    // Load on range 0: 8 turn slots, every turn stands for 100 ms, 12 keys ⇒ ≫ 5 % of
    // the slots.
    let load = Load::start(&node.url(), &keys_in(0, MID, "hot", 12));
    let gone = async {
        let t0 = Instant::now();
        loop {
            let rows = get_json(format!("{}/ranges/suggestions", node.url())).await?;
            if !rows.as_array().into_iter().flatten().any(is_merge) {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "load on range 0 did not remove the merge suggestion: {rows}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    .await;
    load.stop().await;
    gone?;
    wait_suggestion(&node, Duration::from_secs(20), is_merge).await?;
    ensure!(
        node.cluster.table().ranges().len() == 2,
        "a merge suggestion must not merge"
    );

    let off = node_with(8, |c| {
        c.mode = AutoSplitMode::Suggest;
        c.merge_cold = Duration::ZERO;
    })
    .await?;
    tokio::time::sleep(COLD * 3).await;
    let rows = get_json(format!("{}/ranges/suggestions", off.url())).await?;
    ensure!(
        !rows.as_array().into_iter().flatten().any(is_merge),
        "merge_cold 0 must not suggest a merge: {rows}"
    );
    Ok(())
}

/// **Automatic split stops at the default cap of 3.**
///
/// 1. A two-range node, automatic split `suggest`, a 1-byte size cap; the child
///    self-standing loop off; the auto-split depth cap at its **default** (3).
///    SlateDB GC at its default (1 h): no detach during the test.
/// 2. Range 0 has data in the region `[0, MID/512)`, fully compacted (sorted run).
/// 3. Build the chain: each level writes into the current range (inside that
///    region) and then splits it by hand **at once**, taking the left child — the
///    level's data is still in L0 (flushed at quiesce) when it is cloned, so the
///    child stands on one more ancestor. A level whose data was compacted together
///    with the inherited part before the clone (under `CAIRNFLOW_TEST_FAULTS`, the
///    retry delay opens that gap) does not make the child deeper: carry on from
///    that child, until a child stands on ≥ 3 ancestors (at most 9 splits — the
///    region stays inside the left child).
/// 4. Write into that child (e), flush the memtable: the next size measurement
///    suggests splitting e for `size`.
///
/// Must see: e has depth ≥ 3 and e's suggestion carries `blockedBy: depth`; every
/// manual split succeeds — including the final manual split of e itself (a manual
/// split is not blocked by the cap, only warned about).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_split_refuses_a_range_three_ancestors_deep_by_default() -> Result<()> {
    let mut cfg = common::solo_config(2);
    cfg.auto_split.mode = AutoSplitMode::Suggest;
    cfg.auto_split.max_range_bytes = 1;
    cfg.auto_split.size_check_diff = 1;
    cfg.auto_split.size_check_every = Duration::ZERO;
    cfg.lineage.depth = 0;
    assert_eq!(cfg.lineage.max_auto_split_depth, 3, "default cap");
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let _sdk = stalling_sdk(&node).await?;
    // The write region: inside the left child of every midpoint split, up to 9 of them.
    const HI: u64 = MID / 512;
    const MAX_SPLITS: usize = 9;
    let write = |tag: String| {
        let url = url.clone();
        async move {
            for (k, _) in keys_in(0, HI, &tag, 2) {
                let (code, body) = common::invoke(&url, SERVICE, "work", Some(&k), "x").await?;
                ensure!(code == 200, "invoke {k}: {code} {body}");
            }
            Ok::<_, anyhow::Error>(())
        }
    };
    let handle = |range: u32| {
        node.partitions
            .get(PartitionId(range))
            .ok_or_else(|| anyhow!("no engine for range {range}"))
    };
    // Split `range` by hand at its midpoint, return the left child (the smaller of
    // the two ids just allocated). `503`/`409` are transient (the range is changing,
    // the previous attempt rolled back) — retry, bounded.
    let split = |range: u32| {
        let url = url.clone();
        async move {
            let t0 = Instant::now();
            loop {
                let resp = reqwest::Client::new()
                    .post(format!("{url}/ranges/{range}/split"))
                    .send()
                    .await?;
                let code = resp.status().as_u16();
                let view: serde_json::Value = resp.json().await.unwrap_or_default();
                if code == 200 {
                    return Ok::<_, anyhow::Error>(
                        view["nextRangeId"].as_u64().unwrap_or(0).saturating_sub(2) as u32,
                    );
                }
                ensure!(
                    matches!(code, 409 | 503) && t0.elapsed() < Duration::from_secs(30),
                    "manual split of range {range}: {code} {view}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    let depth = |p: u32| {
        let stores = stores.clone();
        async move {
            stores
                .range_external_depth(PartitionId(p))
                .await?
                .ok_or_else(|| anyhow!("range {p}'s Db has no manifest yet"))
        }
    };
    write("root".into()).await?;
    handle(0)?.flush_memtable().await?;
    let mut id = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .ok_or_else(|| anyhow!("range 0 has nothing to compact"))?;
    handle(0)?.wake_compactor().await?;
    let t0 = Instant::now();
    while stores.full_compaction_done(0, &mut id).await? != Some(true) {
        ensure!(
            t0.elapsed() < Duration::from_secs(20),
            "full compaction of range 0 did not finish"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut e = split(0).await?;
    let mut chain = vec![(e, depth(e).await?)];
    while chain.last().is_some_and(|(_, d)| *d < 3) {
        ensure!(
            chain.len() < MAX_SPLITS,
            "could not build a range 3 deep after {MAX_SPLITS} splits: (range, depth) = {chain:?}"
        );
        write(format!("level-{}", chain.len())).await?;
        e = split(e).await?;
        chain.push((e, depth(e).await?));
    }
    println!("chain (range, depth) = {chain:?}");
    write("e".into()).await?;
    handle(e)?.flush_memtable().await?;
    write("e-after-flush".into()).await?;
    // Past one probe tick (`PROBE_EVERY` 30 s): a probe that failed on an injected
    // fault is probed again.
    let row = wait_suggestion(&node, Duration::from_secs(45), |r| {
        r["range"] == e && r["blockedBy"] == "depth"
    })
    .await?;
    ensure!(row["reason"] == "size", "{row}");
    // A manual split still works at the cap.
    split(e).await?;
    Ok(())
}

/// **Merge suggestions across nodes**: each node only sees the ranges it leads, so
/// a pair whose two leaders are on different nodes can only be matched with
/// `scope=cluster`, from the other node's facts (`GET /internal/ranges/merge-facts`).
///
/// 1. A two-node cluster `a`, `b`, four ranges, `merge_cold` 2 s; wait for one
///    leader per range and for `b` to hold at least one range.
/// 2. `GET /ranges/suggestions?scope=cluster` on `a`.
///
/// Must see: exactly two `merge` rows, the pairs (0, 1) and (2, 3) — the range
/// `b` leads appears only thanks to `b`'s facts. The table still has four ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn merge_suggestions_pair_ranges_led_by_different_nodes() -> Result<()> {
    let (_bucket, nodes) =
        common::cluster_nodes_tweaked(&["a", "b"], 4, Duration::from_secs(2), |c| {
            c.auto_split.mode = AutoSplitMode::Suggest;
            c.auto_split.merge_cold = Duration::from_secs(2);
        })
        .await?;
    let refs: Vec<_> = nodes.iter().collect();
    let leaders = common::wait_probe(
        &refs,
        Duration::from_secs(30),
        "one leader per range, b holds a range",
        |p| common::single_leaders(p).is_some_and(|l| l.values().any(|n| *n == 1)),
    )
    .await?;
    let a = &nodes[0].node;
    let t0 = Instant::now();
    let pairs = loop {
        let rows = get_json(format!("{}/ranges/suggestions?scope=cluster", a.url())).await?;
        let pairs: Vec<(u64, u64)> = rows
            .as_array()
            .into_iter()
            .flatten()
            .filter(|r| r["reason"] == "merge")
            .filter_map(|r| Some((r["range"].as_u64()?, r["mergeWith"].as_u64()?)))
            .collect();
        if pairs.len() == 2 {
            break pairs;
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(30),
            "not yet two merge suggestions (leaders {leaders:?}): {rows}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    ensure!(pairs == vec![(0, 1), (2, 3)], "pairs: {pairs:?}");
    ensure!(a.cluster.table().ranges().len() == 4, "nothing merged");
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}
