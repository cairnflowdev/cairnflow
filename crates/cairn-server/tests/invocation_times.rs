// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Invocation timestamps: `createdAt` is the `create_ts` SlateDB attaches to entry 0 at
//! admit, `modifiedAt` is the `create_ts` of the status row. Not one byte of cairn's
//! own, no timestamp in the journal.
//!
//! The underlying library contract (timestamps survive compaction, clone, union,
//! replay) is pinned in `cairn-store/tests/library_contracts.rs::create_ts_*` (CG-7).
//! This file pins the meaning at the invocation level (CG-6).

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
    bytes::Bytes,
    cairn_server::node::RunningNode,
    cairn_store::{ObjectStores, PartitionId},
    common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
    std::time::Duration,
};

fn now_ms() -> i64 {
    common::wall_ms() as i64
}

fn unhex(s: &str) -> Result<[u8; 24]> {
    let v: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<std::result::Result<_, _>>()?;
    v.try_into()
        .map_err(|_| anyhow!("id is not 24 bytes long: {s}"))
}

/// Timestamps read **straight from the Db** of the range holding `id`: flush that range
/// engine's memtable (rows still in the shared WAL are invisible to an ordinary reader),
/// then open a `DbReader` on the bucket. Returns `(create_ts of entry 0, bytes of entry 0,
/// create_ts of the status row)`.
async fn raw_times(
    node: &RunningNode,
    stores: &ObjectStores,
    url: &str,
    id: &str,
) -> Result<(i64, Bytes, i64)> {
    let range = common::partition_of(url, id).await?;
    let h = node
        .partitions
        .get(PartitionId(range))
        .ok_or_else(|| anyhow!("node does not hold range {range}"))?;
    h.flush_memtable().await?;
    let reader =
        slatedb::DbReader::builder(stores.range_path(range), stores.main().store().clone())
            .with_reader_mode(slatedb::DbReaderMode::FollowLatest)
            .with_wal_reader(std::sync::Arc::new(NoWal))
            .build()
            .await?;
    let raw = unhex(id)?;
    let entry0 = reader
        .get_key_value(cairn_store::keys::journal_entry(&raw, 0))
        .await?
        .context("entry 0 absent")?;
    let status = reader
        .get_key_value(cairn_store::keys::invocation_status(&raw))
        .await?
        .context("status row absent")?;
    reader.close().await?;
    Ok((entry0.create_ts, entry0.value, status.create_ts))
}

/// An empty WAL carrying the shared-WAL name: the range's Db declares `cairnflow-shared/1`
/// in its manifest, and an ordinary `DbReader` refuses when the name does not match. The
/// test flushed the memtable (every row is in L0), so the WAL part has nothing to read.
struct NoWal;

struct NoRows;

#[async_trait::async_trait]
impl slatedb::wal::WalIterator for NoRows {
    async fn next(&mut self) -> Result<Option<slatedb::wal::WalRows>, slatedb::wal::WalError> {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl slatedb::wal::WalReader for NoWal {
    async fn iterator(
        &self,
        _: slatedb::wal::WalFileRange,
    ) -> Result<Box<dyn slatedb::wal::WalIterator>, slatedb::wal::WalError> {
        Ok(Box::new(NoRows))
    }

    async fn last_wal_file_id(&self, after: u64) -> Result<u64, slatedb::wal::WalError> {
        Ok(after)
    }

    fn wal_name(&self) -> Option<&str> {
        Some(cairn_store::SHARED_WAL_NAME)
    }
}

async fn post_json(url: String, body: serde_json::Value) -> Result<(u16, serde_json::Value)> {
    let r = reqwest::Client::new().post(url).json(&body).send().await?;
    Ok((r.status().as_u16(), r.json().await?))
}

/// **CG-6 — `createdAt` never moves.** A `Hold/wait` invocation waits for a
/// signal, sleeps 1.5 s, then completes. Read the entry-0 timestamp straight from the Db:
///
/// 1. after the first turn (waiting) — the timestamp lies within the real time of the `send`;
/// 2. after splitting its range — read in the child range;
/// 3. after merging the two children;
/// 4. deliver the signal ⇒ it continues ⇒ waits in `sleep` ⇒ completes.
///
/// Must see: the timestamp and bytes of entry 0 unchanged at every step; the status-row
/// timestamp (`modifiedAt`) moves forward when the status changes. Migrate, pause/resume
/// and fork do not rewrite entry 0 (`lifecycle.rs::migrate` only changes the pin row) —
/// the API test below covers them through `createdAt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn created_never_moves() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object(
            "Hold",
            "wait",
            vec![Step::Suspend, Step::Sleep(1500), Step::output("done")],
        )
        .spawn()
        .await?;
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "createdAt does not move", async {
        common::register(&url, &sdk.url).await?;
        let before = now_ms();
        let (code, body) = common::send(&url, "Hold", "wait", Some("k"), "{}").await?;
        ensure!(code == 202, "{code} {body}");
        let after = now_ms();
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;

        // (1)
        let (c0, e0, m0) = raw_times(&node, &stores, &url, &id).await?;
        ensure!(
            before - 5 <= c0 && c0 <= after + 5,
            "createdAt {c0} outside the real time of the send [{before}, {after}]"
        );
        ensure!(m0 >= c0, "modifiedAt {m0} before createdAt {c0}");

        // (2)
        let (code, view) =
            post_json(format!("{url}/ranges/0/split"), serde_json::json!({})).await?;
        ensure!(code == 200, "split: {code} {view}");
        let (c, e, _) = raw_times(&node, &stores, &url, &id).await?;
        ensure!(c == c0 && e == e0, "split moved createdAt {c0} → {c}");
        // (3)
        let ranges: Vec<u64> = view["ranges"]
            .as_array()
            .ok_or_else(|| anyhow!("{view}"))?
            .iter()
            .filter_map(|r| r["range"].as_u64())
            .collect();
        let (code, view) = post_json(
            format!("{url}/ranges/merge?left={}&right={}", ranges[0], ranges[1]),
            serde_json::json!({}),
        )
        .await?;
        ensure!(code == 200, "merge: {code} {view}");
        let (c, e, _) = raw_times(&node, &stores, &url, &id).await?;
        ensure!(c == c0 && e == e0, "merge moved createdAt {c0} → {c}");

        // (4)
        let (code, body) = post_json(
            format!("{url}/invocations/{id}/signal"),
            serde_json::json!({ "signal": HOLD_SIGNAL }),
        )
        .await?;
        ensure!(code == 200, "signal: {code} {body}");
        common::wait_status(&url, &id, "completed").await?;
        let (c, e, m) = raw_times(&node, &stores, &url, &id).await?;
        ensure!(c == c0 && e == e0, "continuing moved createdAt {c0} → {c}");
        ensure!(m > m0, "modifiedAt stuck on completion: {m0} → {m}");
        ensure!(
            m - c0 >= 1500,
            "modifiedAt {m} must be after the 1.5 s sleep counted from createdAt {c0}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

async fn get_json(url: String) -> Result<(u16, serde_json::Value)> {
    let r = reqwest::get(url).await?;
    Ok((r.status().as_u16(), r.json().await?))
}

/// **Timestamps on the API.** `GET /invocations/{id}` and the rows of `GET
/// /invocations` carry `createdAt` (= `created_ms` of the metadata, within a few ms of
/// entry 0's `create_ts` — read straight from the Db to compare), `modifiedAt` (status row), and `endedAt` only once ended.
///
/// 1. `send` ⇒ waiting: `createdAt` within the real time of the send, equal to the
///    timestamp in the Db; `modifiedAt ≥ createdAt`; no `endedAt`. The list row has the same timestamps.
/// 2. signal ⇒ completed: `createdAt` unchanged; `endedAt = modifiedAt ≥` the old timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_reports_created_modified_ended() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Hold", "wait", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "timestamps on the API", async {
        common::register(&url, &sdk.url).await?;
        let before = now_ms();
        let (code, body) = common::send(&url, "Hold", "wait", Some("k"), "{}").await?;
        ensure!(code == 202, "{code} {body}");
        let after = now_ms();
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;

        // (1)
        let (code, st) = get_json(format!("{url}/invocations/{id}")).await?;
        ensure!(code == 200, "{code} {st}");
        let created = st["createdAt"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing createdAt: {st}"))?;
        let modified = st["modifiedAt"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing modifiedAt: {st}"))?;
        ensure!(
            before - 5 <= created && created <= after + 5,
            "createdAt {created} outside [{before}, {after}]"
        );
        ensure!(modified >= created, "{st}");
        ensure!(st.get("endedAt").is_none(), "endedAt while running: {st}");
        let (c0, _, m0) = raw_times(&node, &stores, &url, &id).await?;
        // Since format 10 `createdAt` is the `created_ms` of the metadata row (engine
        // clock), read in the same commit in which SlateDB attaches `create_ts` to
        // entry 0: a few microseconds apart, possibly straddling a millisecond
        // boundary.
        ensure!(
            (created - c0).abs() <= 50 && modified == m0,
            "API {st} differs from the Db ({c0}, {m0})"
        );
        let (code, rows) = get_json(format!("{url}/invocations?limit=1000")).await?;
        ensure!(code == 200, "{code} {rows}");
        let row = rows
            .as_array()
            .and_then(|a| a.iter().find(|r| r["id"] == id.as_str()))
            .ok_or_else(|| anyhow!("{id} not found: {rows}"))?;
        ensure!(
            row["createdAt"] == created && row["modifiedAt"] == modified,
            "list row {row} differs from {st}"
        );
        ensure!(row.get("endedAt").is_none(), "{row}");

        // (2)
        let (code, body) = post_json(
            format!("{url}/invocations/{id}/signal"),
            serde_json::json!({ "signal": HOLD_SIGNAL }),
        )
        .await?;
        ensure!(code == 200, "signal: {code} {body}");
        common::wait_status(&url, &id, "completed").await?;
        let (_, st) = get_json(format!("{url}/invocations/{id}")).await?;
        ensure!(st["createdAt"] == created, "createdAt moved: {st}");
        let ended = st["endedAt"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing endedAt after completion: {st}"))?;
        ensure!(st["modifiedAt"] == ended && ended >= modified, "{st}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// The ids of the rows in a `GET /invocations` response (an array, or an object for
/// `?scope=cluster`), in the order returned.
fn row_ids(v: &serde_json::Value) -> Vec<String> {
    let rows = v
        .as_array()
        .or_else(|| v["invocations"].as_array())
        .cloned()
        .unwrap_or_default();
    rows.iter()
        .filter_map(|r| r["id"].as_str().map(str::to_owned))
        .collect()
}

/// **Filtering by time.** Three completed invocations (`i0`, `i1`, `i2`, ≥ 20 ms
/// apart) then one waiting (`i3`). Timestamps are taken from the API itself.
///
/// Must see: `created_after`/`created_before`/`ended_after`/`ended_before` are
/// half-open `[after, before)`; results by descending `createdAt` (the default when
/// filtering by time); `ended_*` drops unfinished invocations; combinable with
/// `status`; header `x-cairn-untimed: 0`; a bad parameter ⇒ `400`; `?scope=cluster`
/// gives the same results, with `untimed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_filters_by_created_and_ended_time() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Svc", "quick", vec![Step::output("ok")])
        .object("Hold", "wait", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "filter by time", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for _ in 0..3 {
            let (code, body) = common::invoke(&url, "Svc", "quick", None, "{}").await?;
            ensure!(code == 200, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let (code, body) = common::send(&url, "Hold", "wait", Some("k"), "{}").await?;
        ensure!(code == 202, "{code} {body}");
        ids.push(body["id"].as_str().unwrap_or_default().to_string());
        common::wait_status(&url, &ids[3], "suspended").await?;
        let mut created = Vec::new();
        let mut ended = Vec::new();
        for id in &ids {
            let (_, st) = get_json(format!("{url}/invocations/{id}")).await?;
            created.push(st["createdAt"].as_i64().context("createdAt")?);
            ended.push(st["endedAt"].as_i64());
        }
        ensure!(created.windows(2).all(|w| w[0] < w[1]), "{created:?}");
        let (c, e) = (&created, &ended);
        let e1 = e[1].context("i1 completed")?;
        let list = |q: String| {
            let url = url.clone();
            async move {
                let r = reqwest::get(format!("{url}/invocations?{q}")).await?;
                let code = r.status().as_u16();
                let untimed = r
                    .headers()
                    .get("x-cairn-untimed")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let v: serde_json::Value = r.json().await?;
                Ok::<_, anyhow::Error>((code, untimed, v))
            }
        };
        let want = |is: &[usize]| is.iter().map(|&i| ids[i].clone()).collect::<Vec<_>>();
        for (q, expect) in [
            (format!("created_after={}", c[1]), want(&[3, 2, 1])),
            (format!("created_before={}", c[1]), want(&[0])),
            (
                format!("created_after={}&created_before={}", c[0], c[2]),
                want(&[1, 0]),
            ),
            (format!("ended_after={e1}"), want(&[2, 1])),
            (format!("ended_before={e1}"), want(&[0])),
            (
                format!("status=suspended&created_after={}", c[0]),
                want(&[3]),
            ),
            ("order=created_desc".to_string(), want(&[3, 2, 1, 0])),
        ] {
            let (code, untimed, v) = list(q.clone()).await?;
            ensure!(code == 200, "{q}: {code} {v}");
            ensure!(row_ids(&v) == expect, "{q}: {:?} ≠ {expect:?}", row_ids(&v));
            if !q.starts_with("order") {
                ensure!(
                    untimed.as_deref() == Some("0"),
                    "{q}: x-cairn-untimed {untimed:?}"
                );
            }
        }
        for q in ["created_after=abc", "ended_before=-1", "order=bogus"] {
            let (code, _, v) = list(q.to_string()).await?;
            ensure!(code == 400, "{q}: {code} {v}");
        }
        let (code, _, v) = list(format!("scope=cluster&created_after={}", c[1])).await?;
        ensure!(code == 200, "{v}");
        ensure!(row_ids(&v) == want(&[3, 2, 1]), "cluster: {v}");
        ensure!(v["untimed"] == 0 && v["partial"] == false, "cluster: {v}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **CG-9 — timestamps write nothing.** The timestamp is the existing `create_ts`:
/// reading `createdAt` and filtering by time commit nothing, flush no WAL, write no Db
/// object.
///
/// 20 completed invocations; then 50 `GET /invocations/{id}` and 10 time-filtered
/// queries on a counting store (`FaultyStore`). Must see: no PUT to the shared-WAL log,
/// the native WAL, the manifest or an SST within the read window. (The bytes written per
/// call did not change with timestamps: bench `core-gaps.ts`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_extra_writes() -> Result<()> {
    use cairn_store::testkit::{FaultyStore, Op, PathClass};
    let sdk = FakeSdk::builder()
        .service("Svc", "quick", vec![Step::output("ok")])
        .spawn()
        .await?;
    let faulty = FaultyStore::wrap(std::sync::Arc::new(
        slatedb::object_store::memory::InMemory::new(),
    ));
    let mut cfg = common::solo_config(1);
    cfg.stores = ObjectStores::single(cairn_store::StoreLocation::from_store(
        faulty.clone(),
        "",
        "memory://",
    ));
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "timestamps write nothing", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for _ in 0..20 {
            let (code, body) = common::invoke(&url, "Svc", "quick", None, "{}").await?;
            ensure!(code == 200, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        // The last commit is durable (the 200 answer comes after the commit) — wait one more
        // tick so that every flush of the last turn falls before the window.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let data_puts = || {
            let c = faulty.counts();
            [
                PathClass::NodeWal,
                PathClass::Wal,
                PathClass::Manifest,
                PathClass::Compacted,
            ]
            .iter()
            .map(|k| c.get(Op::Put, *k))
            .sum::<u64>()
        };
        let before = data_puts();
        for id in ids.iter().cycle().take(50) {
            let (code, st) = get_json(format!("{url}/invocations/{id}")).await?;
            ensure!(code == 200 && st["createdAt"].is_i64(), "{st}");
        }
        for _ in 0..10 {
            let (code, v) =
                get_json(format!("{url}/invocations?created_after=0&ended_after=0")).await?;
            ensure!(code == 200 && row_ids(&v).len() == 20, "{v}");
        }
        let after = data_puts();
        ensure!(
            after == before,
            "reading timestamps wrote {} data PUTs",
            after - before
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
