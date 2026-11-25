// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Payload-weighted billing units: every durable command, **once durable**, counts
//! `max(1, ⌈payload / 64 KiB⌉)` units, every invocation adds 2 (one at creation, one at
//! the end — the part of the result beyond 64 KiB is added to the end). Counted once per
//! entry, at commit; replays, retry turns and recovery after a leader change do not count
//! again.
//!
//! Measured with the **process**'s metering snapshot (`metering::snapshot`, tenant
//! `default`): every node of the test runs in one process, so that snapshot is the sum
//! over every node lifetime — exactly what "nothing lost, nothing double-counted" needs
//! to compare. `/metering` and `sys_metering` are checked separately in
//! [`units_reach_metering_and_sys_metering`]. The tests share the lock [`SERIAL`] and
//! measure the **difference** before/after: `cargo test` runs the tests of one binary in
//! one process, sharing the metering book.

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
    bytes::Bytes,
    cairn_server::metering,
    common::{
        fake_sdk::{FakeSdk, Step},
        register, send, wait_status, within, ClusterNode, TEST_DEADLINE,
    },
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
};

/// One test at a time in the process — the metering book belongs to the whole process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const KIB: usize = 1024;
const TTL: Duration = Duration::from_secs(2);

/// `units` of tenant `default` in the process's book, all hours. Read through the JSON
/// of [`metering::Usage`] (key `units`; absent = 0).
fn units_now() -> u64 {
    metering::snapshot()
        .into_iter()
        .filter(|((t, _), _)| t == "default")
        .map(|(_, u)| {
            serde_json::to_value(&u)
                .ok()
                .and_then(|v| v["units"].as_u64())
                .unwrap_or(0)
        })
        .sum()
}

/// Waits for `units_now() − base` to equal `want`, then for it to hold one more tick
/// (nothing counted late — recovery, cleanup, replay). Out of time ⇒ an error with the last count.
async fn expect_units(base: u64, want: u64, what: &str) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let got = units_now().saturating_sub(base);
        if got == want {
            break;
        }
        ensure!(
            t0.elapsed() < TEST_DEADLINE,
            "{what}: units = {got}, must be {want}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let got = units_now().saturating_sub(base);
    ensure!(
        got == want,
        "{what}: units drifted up to {got} after reaching {want}"
    );
    Ok(())
}

fn blob(n: usize) -> Bytes {
    Bytes::from(vec![b'x'; n])
}

/// The fake SDK of every test here (`REQUEST_RESPONSE`: each turn the runtime resends
/// the whole journal — replay on every turn).
///
/// - `U/all`: `ctx.run` returning 0 B, 64 KiB, 64 KiB + 1, 1 MiB; `sleep`; `call`
///   (waiting for the result) and `send` to `U/echo`; a small `Output`.
/// - `U/echo`: a small `Output`.
/// - `U/big`: a 1 MiB `Output`.
/// - `UO/set` (virtual object): `set` 64 KiB + 1, `set` 1 B, `Output`.
/// - `U/retry`: `ctx.run` fails transiently twice then returns 10 B; `ctx.run` 10 B; `Output`.
/// - `U/hold` / `UO/hold`: `ctx.run` 100 KiB, waits for a signal, `ctx.run` 1 B,
///   `Output`. `U/poke` sends a signal to the id in `cell`.
async fn sdk(cell: Arc<Mutex<String>>) -> Result<FakeSdk> {
    let hold = || {
        vec![
            Step::Run(blob(100 * KIB)),
            Step::Suspend,
            Step::Run(blob(1)),
            Step::output("\"held\""),
        ]
    };
    FakeSdk::builder()
        .service(
            "U",
            "all",
            vec![
                Step::Run(Bytes::new()),
                Step::Run(blob(64 * KIB)),
                Step::Run(blob(64 * KIB + 1)),
                Step::Run(blob(1024 * KIB)),
                Step::Sleep(1),
                Step::call("U", "echo", None),
                Step::Send {
                    service: "U".into(),
                    handler: "echo".into(),
                    tags: vec![],
                },
                Step::output("\"done\""),
            ],
        )
        .service("U", "echo", vec![Step::output("\"e\"")])
        .service("U", "big", vec![Step::Output(blob(1024 * KIB))])
        .service(
            "U",
            "retry",
            vec![
                Step::RunFailBelow(2, blob(10)),
                Step::Run(blob(10)),
                Step::output("\"ok\""),
            ],
        )
        .service("U", "hold", hold())
        .service(
            "U",
            "poke",
            vec![Step::SendSignal(cell), Step::output("\"sent\"")],
        )
        .object(
            "UO",
            "set",
            vec![
                Step::SetState {
                    key: "a".into(),
                    value: blob(64 * KIB + 1),
                },
                Step::SetState {
                    key: "b".into(),
                    value: blob(1),
                },
                Step::output("\"set\""),
            ],
        )
        .object("UO", "hold", hold())
        .spawn()
        .await
}

/// Units of one full run of `U/hold` (or `UO/hold`) plus the `U/poke` that signals it:
/// hold = 1 (creation) + 2 (`run` 100 KiB) + 1 (signal arrives) + 1 (`run` 1 B)
/// + 0 (small `Output`) + 1 (end) = 6; poke = 1 + 1 (`SendSignal`) + 1 = 3.
const HOLD_AND_POKE: u64 = 9;

async fn node(bucket: &common::Bucket, id: &str, partitions: u16) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, partitions, TTL, |cfg| {
        cfg.metering_flush = Duration::from_millis(100);
    })
    .await
}

/// `POST /invoke/{s}/{h}/send` until the node accepts (the range may have no leader yet).
async fn send_ok(url: &str, service: &str, handler: &str, key: Option<&str>) -> Result<String> {
    loop {
        let (code, v) = send(url, service, handler, key, "null").await?;
        if (200..300).contains(&code) {
            return v["id"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("send returned no id: {v}"));
        }
        ensure!(code == 503, "{service}/{handler}: {code} {v}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **Each command kind by payload, through RR replay.**
///
/// One node; `U/all`, `U/big`, `UO/set` run to completion. Each RR turn resends the whole
/// journal (turn `k` replays `k − 1` results) — replay does not count.
///
/// Must see (total 51):
/// - `U/all` = 1 (creation) + `run` 0 B → 1, 64 KiB → 1, 64 KiB + 1 → 2, 1 MiB → 16
///   + `sleep` 1 + `call` 1 + `send` 1 + small `Output` 0 + 1 (end) = 25;
///   two `U/echo` (one from `call`, one from `send`) at 2 each ⇒ 29.
/// - `U/big` = 1 + a 1 MiB result → 16 = 17.
/// - `UO/set` = 1 + `set` 64 KiB + 1 → 2 + `set` 1 B → 1 + 0 + 1 = 5.
///
/// A result ≥ `seal_bytes` (256 KiB) goes segmented: the entry carries only a ref,
/// but units count **new bytes** — the tail plus the segments just sealed from the
/// customer's bytes — so 1 MiB is still 16, as when inline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_command_counts_by_its_payload_once() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = sdk(Arc::default()).await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", 2).await?;
    let url = n.node.url();
    let base = units_now();
    within(TEST_DEADLINE, "each command kind", async {
        register(&url, &sdk.url).await?;
        for (s, h, k) in [
            ("U", "all", None),
            ("U", "big", None),
            ("UO", "set", Some("k")),
        ] {
            let id = send_ok(&url, s, h, k).await?;
            wait_status(&url, &id, "completed").await?;
        }
        Ok(())
    })
    .await?;
    expect_units(base, 29 + 17 + 5, "each command kind").await
}

/// **A retried `ctx.run` counts only the attempt that became durable.**
///
/// `U/retry`: attempts 0 and 1 write `Run` then fail transiently (`Error` RETRY, no result
/// proposed); attempt 2 proposes 10 B. Then one more 10 B `ctx.run`, `Output`.
///
/// Must see: 1 (creation) + 1 (`run` durable once) + 1 (second `run`) + 0 + 1 = 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retried_run_counts_once() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = sdk(Arc::default()).await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", 2).await?;
    let url = n.node.url();
    let base = units_now();
    within(TEST_DEADLINE, "retry", async {
        register(&url, &sdk.url).await?;
        let id = send_ok(&url, "U", "retry", None).await?;
        wait_status(&url, &id, "completed").await?;
        Ok(())
    })
    .await?;
    expect_units(base, 4, "retry").await
}

/// Waits for `id` to wait, then has `U/poke` send it a signal (via `url`).
async fn poke(url: &str, cell: &Arc<Mutex<String>>, id: &str) -> Result<()> {
    *cell.lock().unwrap() = id.to_string();
    let p = send_ok(url, "U", "poke", None).await?;
    wait_status(url, &p, "completed").await?;
    Ok(())
}

/// **Graceful restart mid-invocation** ("a restart neither loses nor
/// double-counts").
///
/// 1. Node `n1` runs `U/hold` up to its wait (the 100 KiB `run` is durable), then shuts down gracefully.
/// 2. A new node `n1` on the same bucket takes the range, recovers `U/hold` from the journal.
/// 3. `U/poke` sends the signal ⇒ `U/hold` finishes.
///
/// Must see: exactly 9 — what was counted before shutdown is not lost, and the recovery
/// turn (a full journal replay on the new node) does not count the 100 KiB `run` again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_restart_neither_loses_nor_doubles() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let cell: Arc<Mutex<String>> = Arc::default();
    let sdk = sdk(cell.clone()).await?;
    let bucket = common::bucket();
    let base = units_now();
    let n1 = node(&bucket, "n1", 2).await?;
    let url = n1.node.url();
    let id = within(TEST_DEADLINE, "before shutdown", async {
        register(&url, &sdk.url).await?;
        let id = send_ok(&url, "U", "hold", None).await?;
        wait_status(&url, &id, "suspended").await?;
        Ok(id)
    })
    .await?;
    n1.node.shutdown().await;
    let n1 = node(&bucket, "n1", 2).await?;
    let url = n1.node.url();
    within(TEST_DEADLINE, "after restart", async {
        poke(&url, &cell, &id).await?;
        wait_status(&url, &id, "completed").await?;
        Ok(())
    })
    .await?;
    expect_units(base, HOLD_AND_POKE, "graceful restart").await
}

/// **`kill -9` mid-invocation.**
///
/// 1. `n1` leads every range, runs `U/hold` up to its wait.
/// 2. `n2` comes up; `n1` is frozen (every store request hangs — `kill -9` as seen from the
///    bucket). `n2` claims the lease after the TTL, recovers `U/hold`.
/// 3. `U/poke` through `n2` ⇒ `U/hold` finishes on `n2`.
///
/// Must see: exactly 9. What `n1` counted before dying is what **was durable** (the
/// process's record — here the same process, so no RAM is lost); recovery on `n2` does
/// not count again; a frozen `n1` cannot make anything durable so it counts nothing
/// more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill9_failover_neither_loses_nor_doubles() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let cell: Arc<Mutex<String>> = Arc::default();
    let sdk = sdk(cell.clone()).await?;
    let bucket = common::bucket();
    let base = units_now();
    let n1 = node(&bucket, "n1", 2).await?;
    let url1 = n1.node.url();
    let id = within(TEST_DEADLINE, "before kill -9", async {
        register(&url1, &sdk.url).await?;
        let id = send_ok(&url1, "U", "hold", None).await?;
        wait_status(&url1, &id, "suspended").await?;
        Ok(id)
    })
    .await?;
    let n2 = node(&bucket, "n2", 2).await?;
    common::freeze(&n1.store);
    let url2 = n2.node.url();
    common::wait_probe(&[&n2], TEST_DEADLINE, "n2 leads every range", |p| {
        p.values().all(|v| v == &vec![0])
    })
    .await?;
    within(TEST_DEADLINE, "after kill -9", async {
        poke(&url2, &cell, &id).await?;
        wait_status(&url2, &id, "completed").await?;
        Ok(())
    })
    .await?;
    expect_units(base, HOLD_AND_POKE, "kill -9").await?;
    drop(n2);
    drop(n1);
    Ok(())
}

/// **Splitting then merging a range mid-invocation.**
///
/// 1. `UO/hold` (its key falls into range 0) runs up to its wait.
/// 2. Split range 0, then merge the two children back.
/// 3. `U/poke` ⇒ `UO/hold` finishes on the merged range.
///
/// Must see: exactly 9 — a child Db is a copy of the parent (a journal already counted is
/// not counted again in a child or merged range), and nothing of the parent is lost when it retires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_and_merge_neither_lose_nor_double() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let cell: Arc<Mutex<String>> = Arc::default();
    let sdk = sdk(cell.clone()).await?;
    let bucket = common::bucket();
    let base = units_now();
    let n = node(&bucket, "n1", 2).await?;
    let url = n.node.url();
    let http = reqwest::Client::new();
    within(TEST_DEADLINE, "split/merge", async {
        register(&url, &sdk.url).await?;
        let key = common::key_on_partition(&n.node, "UO", 0, "hold");
        let id = send_ok(&url, "UO", "hold", Some(&key)).await?;
        wait_status(&url, &id, "suspended").await?;
        let r = http.post(format!("{url}/ranges/0/split")).send().await?;
        let code = r.status();
        let v: serde_json::Value = r.json().await?;
        ensure!(code.is_success(), "split: {code} {v}");
        let kids: Vec<u64> = v["ranges"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|r| r["range"].as_u64())
                    .filter(|r| *r >= 2)
                    .collect()
            })
            .unwrap_or_default();
        ensure!(kids.len() == 2, "{v}");
        let (left, right) = (kids[0].min(kids[1]), kids[0].max(kids[1]));
        let r = http
            .post(format!("{url}/ranges/merge?left={left}&right={right}"))
            .send()
            .await?;
        let code = r.status();
        ensure!(code.is_success(), "merge: {code} {}", r.text().await?);
        poke(&url, &cell, &id).await?;
        wait_status(&url, &id, "completed").await?;
        Ok(())
    })
    .await?;
    expect_units(base, HOLD_AND_POKE, "split/merge").await
}

/// **`/metering` and `sys_metering` carry `units`.**
///
/// One node flushes metering every 100 ms; runs `U/all`. A fresh bucket has only this
/// process's flush objects, so the `units` of `/metering` (current hour) must equal the
/// process snapshot for that hour, and `sys_metering` reads the same number.
///
/// Must see: the three numbers are equal and > 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn units_reach_metering_and_sys_metering() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = sdk(Arc::default()).await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", 2).await?;
    let url = n.node.url();
    let http = reqwest::Client::new();
    within(TEST_DEADLINE, "/metering", async {
        register(&url, &sdk.url).await?;
        let id = send_ok(&url, "U", "all", None).await?;
        wait_status(&url, &id, "completed").await?;
        loop {
            let hour = metering::hour_now();
            let local: u64 = metering::snapshot()
                .into_iter()
                .filter(|((t, h), _)| t == "default" && *h == hour)
                .map(|(_, u)| {
                    serde_json::to_value(&u)
                        .ok()
                        .and_then(|v| v["units"].as_u64())
                        .unwrap_or(0)
                })
                .sum();
            let m: serde_json::Value = http
                .get(format!("{url}/metering?hour={hour}"))
                .send()
                .await?
                .json()
                .await?;
            let q: serde_json::Value = http
                .post(format!("{url}/query"))
                .json(&serde_json::json!({ "sql": "SELECT units FROM sys_metering" }))
                .send()
                .await?
                .json()
                .await?;
            let cell = &q["rows"][0][0];
            let sql = cell
                .as_u64()
                .or_else(|| cell.as_str().and_then(|s| s.parse().ok()));
            if local > 0 && m["units"].as_u64() == Some(local) && sql == Some(local) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
}
