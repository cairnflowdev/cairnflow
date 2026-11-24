// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A soft throttle before the L0 wall.
//!
//! "Slow" compaction is built with a `FaultyStore` gate on every PUT under
//! `compactions/`: the coordinator cannot submit any compaction job, so L0 only
//! grows. L0 is built deterministically with `EngineHandle::flush_memtable` (each
//! flush of a non-empty memtable is one L0 view) — independent of SlateDB's memtable
//! size (64 MiB) or of any cadence.
//!
//! Two questions:
//!
//! 1. **Signal**: `cairnflow_range_l0_views{range}` follows the flush count,
//!    read from the writer's in-RAM manifest.
//! 2. **Throttle**: with L0 at `cap − 1`, **new** work at ingress gets
//!    `429 range_overloaded` with `Retry-After` and **no id** instead of hanging;
//!    resending with the same idempotency key after creation matches the duplicate,
//!    never 429; timers, cross-range completions, local child calls and `StartCall` to a
//!    throttled range for unfinished invocations still run to completion; the range does
//!    not step down.

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
    cairn_server::node::{NodeConfig, RunningNode},
    cairn_store::{
        partition_key_for_target,
        testkit::{Action, FaultyStore, Gate, OpSet, PathMatch, Rule},
        ObjectStores, PartitionId, StoreLocation, TenantId,
    },
    common::fake_sdk::{FakeSdk, Step},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// The boundary between the two ranges of a two-range node: `[0, MID)` and `[MID, MAX]`.
const MID: u64 = 1 << 63;
const OBJ: &str = "Obj";

/// `n` object keys of [`OBJ`] with a partition key in `[lo, hi)`.
fn keys_in(lo: u64, hi: u64, prefix: &str, n: usize) -> Vec<String> {
    (0..1_000_000)
        .map(|i| format!("{prefix}-{i}"))
        .filter(|k| {
            let pk = partition_key_for_target(&TenantId::default(), None, OBJ, k, None)
                .expect("a key")
                .0;
            lo <= pk && pk < hi
        })
        .take(n)
        .collect()
}

fn key0(prefix: &str) -> String {
    keys_in(0, MID, prefix, 1).remove(0)
}

fn key1(prefix: &str) -> String {
    keys_in(MID, u64::MAX, prefix, 1).remove(0)
}

/// A lone node, `partitions` ranges, on a `FaultyStore`. `tweak` adjusts the config
/// before building.
async fn faulty_node(
    partitions: u16,
    tweak: impl FnOnce(&mut NodeConfig),
) -> Result<(RunningNode, Arc<FaultyStore>)> {
    let store = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
    let mut cfg = common::solo_config(partitions);
    cfg.stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    tweak(&mut cfg);
    Ok((common::start_led(cfg).await?, store))
}

/// Blocks every PUT under `compactions/`: compaction stops running. The gate can be
/// reopened (at the end of the test, so the Db closes cleanly).
fn stall_compaction(store: &FaultyStore) -> Gate {
    let gate = Gate::closed();
    store.add_rule(Rule::new(
        OpSet::WRITES,
        PathMatch::Contains("compactions/".into()),
        Action::Gate(gate.clone()),
    ));
    gate
}

/// Reads one metric line with a `range` label from `/metrics`.
async fn metric(node: &RunningNode, name: &str, range: u32) -> Result<Option<f64>> {
    let text = reqwest::get(format!("{}/metrics", node.url()))
        .await?
        .text()
        .await?;
    let prefix = format!("{name}{{range=\"{range}\"}} ");
    Ok(text
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse().ok()))
}

async fn l0_views(node: &RunningNode, range: u32) -> Result<u64> {
    metric(node, "cairnflow_range_l0_views", range)
        .await?
        .map(|v| v as u64)
        .ok_or_else(|| anyhow!("/metrics has no cairnflow_range_l0_views for range {range}"))
}

/// `POST /invoke/{OBJ}/{handler}/send?key=` with `Idempotency-Key` if given.
/// Returns the code, the `Retry-After` header and the JSON.
async fn send(
    node: &RunningNode,
    handler: &str,
    key: &str,
    idem: Option<&str>,
) -> Result<(u16, Option<String>, serde_json::Value)> {
    let mut req = reqwest::Client::new()
        .post(format!(
            "{}/invoke/{OBJ}/{handler}/send?key={key}",
            node.url()
        ))
        .body("{}");
    if let Some(i) = idem {
        req = req.header("idempotency-key", i);
    }
    let resp = req.send().await?;
    let code = resp.status().as_u16();
    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let json = resp
        .json()
        .await
        .context("the /send response is not JSON")?;
    Ok((code, retry_after, json))
}

/// Sends until accepted (2xx) — new work that hits the throttle retries after a shortened
/// `Retry-After`. Returns the id.
async fn send_until_accepted(node: &RunningNode, handler: &str, key: &str) -> Result<String> {
    for _ in 0..200 {
        let (code, _, body) = send(node, handler, key, None).await?;
        match code {
            200..=299 => {
                return body["id"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("2xx without id: {body}"))
            }
            429 => tokio::time::sleep(Duration::from_millis(20)).await,
            _ => return Err(anyhow!("/send {handler} returned {code}: {body}")),
        }
    }
    Err(anyhow!("/send {handler} throttled 200 times in a row"))
}

/// Flushes range 0's memtable until there are at least `want` L0 views. Each round writes
/// a new invocation into the range (flushing an empty memtable produces no view).
async fn build_l0(node: &RunningNode, want: u64) -> Result<u64> {
    let engine = node
        .partitions
        .get(PartitionId(0))
        .ok_or_else(|| anyhow!("node has no engine for range 0"))?;
    for i in 0..4 * want {
        let have = l0_views(node, 0).await?;
        if have >= want {
            return Ok(have);
        }
        send_until_accepted(node, "plain", &key0(&format!("fill{i}"))).await?;
        engine
            .flush_memtable()
            .await
            .map_err(|e| anyhow!("flush memtable: {e}"))?;
    }
    Err(anyhow!("could not build {want} L0 views"))
}

/// The L0 signal: the per-range metric counts exactly the memtable flushes while
/// compaction is stalled; SlateDB's line is present (the recorder is wired).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l0_metric_follows_memtable_flushes() -> Result<()> {
    let (node, store) = faulty_node(1, |_| {}).await?;
    let sdk = FakeSdk::builder()
        .object(OBJ, "plain", vec![Step::output("ok")])
        .spawn()
        .await?;
    common::register(&node.url(), &sdk.url).await?;
    let gate = stall_compaction(&store);
    let engine = node.partitions.get(PartitionId(0)).expect("range 0");

    let base = l0_views(&node, 0).await?;
    for i in 0..3 {
        common::send(&node.url(), OBJ, "plain", Some(&format!("k{i}")), "{}").await?;
        engine.flush_memtable().await.map_err(|e| anyhow!("{e}"))?;
    }
    let after = l0_views(&node, 0).await?;
    ensure!(
        after == base + 3,
        "three flushes ⇒ three more L0 views: {base} → {after}"
    );
    let ssts = metric(&node, "cairnflow_range_slatedb_l0_ssts", 0).await?;
    ensure!(
        ssts.is_some(),
        "SlateDB recorder not wired: no cairnflow_range_slatedb_l0_ssts"
    );
    ensure!(
        metric(&node, "cairnflow_range_write_stalls_total", 0)
            .await?
            .is_some(),
        "missing cairnflow_range_write_stalls_total"
    );
    gate.open();
    Ok(())
}

/// The throttle end to end.
///
/// Order:
///
/// 1. Before throttling: create four kinds of **unfinished** invocations —
///    - `A` (range 0) sleeps 3 s (timer);
///    - `B` (range 0) calls a child on range 1, the child sleeps 3 s (cross-range completion back to range 0);
///    - six `D` (range 0) sleep 4 s then call a child on **range 0** (a local
///      `dispatch_call` while throttled).
/// 2. Stall compaction, flush range 0's memtable up to `cap − 1` = 7 views (refusal
///    probability 0.9).
/// 3. Fire 40 `/send` of new work into range 0 at once: each answers within 10 s
///    (no hang); at least one `429` carries `code: "range_overloaded"`,
///    `Retry-After` and **no** `id`.
/// 4. New work with an idempotency key: send until accepted, then resend 10 times
///    while still throttled — every time 2xx with the **same** id (once created it is
///    never 429).
/// 5. Six `C` (range 1, not throttled) call a child on range 0: the `StartCall` to the
///    throttled range is durably accepted, deferred, then run — no 429 at `/internal/inbound`.
/// 6. `A`, `B`, `C`, `D` all complete while L0 stays at 7 (the gate is still closed);
///    throughout, range 0 does not step down — asking its status every 100 ms never
///    meets `NotLeader`. `durability_stall` is lowered to 2 s, so the throttle window is
///    many times longer than the limit. The metric `cairnflow_range_l0_shed_total` for
///    `inbound` and `dispatch` > 0: both deferral paths really ran.
///
/// Must see: (3), (4), (5), (6). Open the gate at the end so the Db closes cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l0_throttle_sheds_new_work_but_not_in_flight() -> Result<()> {
    let (node, store) = faulty_node(2, |cfg| {
        cfg.limits.durability_stall = Some(Duration::from_secs(2));
    })
    .await?;
    let child0 = key0("child0");
    let child1 = key1("child1");
    let sdk = FakeSdk::builder()
        .object(OBJ, "plain", vec![Step::output("ok")])
        .object(
            OBJ,
            "sleep",
            vec![Step::Sleep(3_000), Step::output("slept")],
        )
        .object(OBJ, "child", vec![Step::output("child")])
        .object(
            OBJ,
            "slow_child",
            vec![Step::Sleep(3_000), Step::output("slow")],
        )
        .object(
            OBJ,
            "call_remote",
            vec![
                Step::call(OBJ, "slow_child", Some(&child1)),
                Step::EchoCallResult,
            ],
        )
        .object(
            OBJ,
            "sleep_then_call_local",
            vec![
                Step::Sleep(4_000),
                Step::call(OBJ, "child", Some(&child0)),
                Step::EchoCallResult,
            ],
        )
        .object(
            OBJ,
            "call_into_0",
            vec![
                Step::call(OBJ, "child", Some(&key0("child0b"))),
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    common::register(&node.url(), &sdk.url).await?;
    let url = node.url();

    // (1) Unfinished work, created before throttling.
    let a = send_until_accepted(&node, "sleep", &key0("a")).await?;
    let b = send_until_accepted(&node, "call_remote", &key0("b")).await?;
    let mut ds = Vec::new();
    for i in 0..6 {
        ds.push(
            send_until_accepted(&node, "sleep_then_call_local", &key0(&format!("d{i}"))).await?,
        );
    }

    // (2) L0 up to cap − 1.
    let gate = stall_compaction(&store);
    let views = build_l0(&node, 7).await?;
    ensure!(views >= 7, "L0 = {views}");

    // Watch for a step-down: ask range 0's status every 100 ms until the end of the test.
    let engine0 = node.partitions.get(PartitionId(0)).expect("range 0");
    let probe = cairn_store::make_invocation_id(cairn_store::PartitionKey(0), [0; 16]);
    let watching = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let (engine0, watching) = (engine0.clone(), Arc::clone(&watching));
        tokio::spawn(async move {
            let mut lost = 0u32;
            while watching.load(std::sync::atomic::Ordering::Relaxed) {
                if engine0.status(probe).await.is_err() {
                    lost += 1;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            lost
        })
    };

    // (3) New work: answers fast, mostly 429 without an id.
    let started = Instant::now();
    let replies = futures::future::join_all((0..40).map(|i| {
        let node = &node;
        async move {
            tokio::time::timeout(
                Duration::from_secs(10),
                send(node, "plain", &key0(&format!("new{i}")), None),
            )
            .await
        }
    }))
    .await;
    let mut shed = 0;
    for r in replies {
        let (code, retry_after, body) = r.map_err(|_| anyhow!("/send hung over 10 s"))??;
        match code {
            200..=299 => {}
            429 => {
                ensure!(body["code"] == "range_overloaded", "429 bad code: {body}");
                ensure!(
                    body.get("id").is_none(),
                    "429 range_overloaded must not carry an id: {body}"
                );
                ensure!(retry_after.is_some(), "429 missing Retry-After: {body}");
                shed += 1;
            }
            _ => return Err(anyhow!("/send returned {code}: {body}")),
        }
    }
    ensure!(shed > 0, "L0 = 7 yet no throttling among 40 new jobs");
    ensure!(started.elapsed() < Duration::from_secs(10));

    // (4) Once created, never 429.
    let idem_key = key0("idem");
    let mut first = None;
    for _ in 0..200 {
        let (code, _, body) = send(&node, "sleep", &idem_key, Some("k-1")).await?;
        if (200..300).contains(&code) {
            first = body["id"].as_str().map(str::to_string);
            break;
        }
        ensure!(code == 429, "the first send returned {code}: {body}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let first = first.ok_or_else(|| anyhow!("idempotency key throttled 200 times in a row"))?;
    for _ in 0..10 {
        let (code, _, body) = send(&node, "sleep", &idem_key, Some("k-1")).await?;
        ensure!(
            (200..300).contains(&code),
            "resending an existing invocation got {code}: {body}"
        );
        ensure!(
            body["id"].as_str() == Some(first.as_str()),
            "resend gave another id: {body}"
        );
    }

    // (5) StartCall to the throttled range.
    let mut cs = Vec::new();
    for i in 0..6 {
        cs.push(send_until_accepted(&node, "call_into_0", &key1(&format!("c{i}"))).await?);
    }

    // (6) Unfinished work completes while L0 stays at 7.
    let mut all = vec![
        ("A (timer)", a.clone(), "slept"),
        ("B (cross-range completion)", b.clone(), "slow"),
    ];
    all.extend(
        ds.iter()
            .map(|d| ("D (local child call)", d.clone(), "child")),
    );
    all.extend(
        cs.iter()
            .map(|c| ("C (StartCall to the throttled range)", c.clone(), "child")),
    );
    for (name, id, want) in &all {
        let (name, want) = (*name, *want);
        let st = common::within(
            Duration::from_secs(30),
            name,
            common::wait_status(&url, id.as_str(), "completed"),
        )
        .await?;
        ensure!(st["output"] == want, "{name}: {st}");
    }
    ensure!(
        l0_views(&node, 0).await? >= 7,
        "L0 must stay at the cap during the checks"
    );
    // The deferral paths really ran (6 calls of each kind at probability 0.9: all six
    // slipping through on the first try is 10⁻⁶), and still completed.
    for kind in ["inbound", "dispatch"] {
        let line = format!("cairnflow_range_l0_shed_total{{range=\"0\",kind=\"{kind}\"}} ");
        let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
        let n: u64 = text
            .lines()
            .find_map(|l| l.strip_prefix(line.as_str()))
            .and_then(|v| v.trim().parse().ok())
            .ok_or_else(|| anyhow!("/metrics missing {line}"))?;
        ensure!(n > 0, "no {kind} deferral — the deferral path never ran");
    }
    let held = started.elapsed();

    watching.store(false, std::sync::atomic::Ordering::Relaxed);
    let lost = watcher.await?;
    ensure!(
        lost == 0,
        "range 0 stepped down while throttled ({lost} status queries failed)"
    );
    ensure!(
        held >= Duration::from_secs(3),
        "the throttle window must be longer than durability_stall (2 s): {held:?}"
    );
    gate.open();
    Ok(())
}
