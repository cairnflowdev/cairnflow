// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Archiving before a purge, and tamper evidence.
//!
//! Purging an invocation (retention expired, or `POST /invocations/{id}/purge`)
//! writes the journal + final status as **one** compressed object in the bucket
//! *before* the delete commits; `GET /archive/{id}` reads it back. When the
//! tenant has the hash chain on, the archive carries the chain's last hash, and
//! `?verify=true` recomputes it.

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
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{ObjectStores, StoreLocation},
    common::{
        fake_sdk::{FakeSdk, Step},
        invoke, register, within,
    },
    futures::TryStreamExt,
    reqwest::{Client, StatusCode},
    serde_json::Value,
    slatedb::object_store::{
        memory::InMemory, path::Path, ObjectStore, ObjectStoreExt, PutPayload,
    },
    std::{sync::Arc, time::Duration},
};

/// The node's retention: long — an invocation is purged early only when its handler
/// declares a shorter `journalRetention` (the handler's contract overrides `--retention-ms`).
const NODE_RETENTION_MS: u64 = 600_000;

async fn start_on(bucket: &Arc<InMemory>) -> Result<cairn_server::node::RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(2));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.retention_ms = Some(NODE_RETENTION_MS);
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("could not build the node: {e}"))
}

/// SDK: `Greeter/hi` declares a 200 ms retention; `Greeter/slow` follows the node's.
async fn sdk() -> Result<FakeSdk> {
    FakeSdk::builder()
        .service("Greeter", "hi", vec![Step::output("hello")])
        .service("Greeter", "slow", vec![Step::output("slow")])
        .contract(
            "Greeter",
            "hi",
            cairn_server::discovery::Contract {
                journal_retention: Some(200),
                ..Default::default()
            },
        )
        .spawn()
        .await
}

/// Wait until `/invocations/{id}` returns 404 — the invocation has been purged.
async fn wait_purged(url: &str, id: &str) -> Result<()> {
    for _ in 0..200 {
        let (code, _) = common::invocation(url, id).await?;
        if code == 404 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!(
        "{id} not purged after 10 s — journalRetention 200 ms not applied"
    ))
}

async fn get_json(url: String) -> Result<(StatusCode, Value)> {
    let r = Client::new().get(url).send().await?;
    let code = r.status();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// Every object under the bucket's `archive/`.
async fn archived(bucket: &Arc<InMemory>) -> Result<Vec<Path>> {
    let listed: Vec<_> = bucket
        .list(Some(&Path::from("archive")))
        .try_collect()
        .await?;
    Ok(listed.into_iter().map(|m| m.location).collect())
}

/// Retention expired (the **handler's**, shorter than the node's) ⇒ the invocation
/// is purged from state, but reads back intact from the archive: final status,
/// output, journal. A purge by hand archives too. The object lives at
/// `archive/<tenant>/<yyyy-mm-dd>/<id>`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purged_invocation_is_readable_from_archive() -> Result<()> {
    let bucket = common::bucket();
    let sdk = sdk().await?;
    let node = start_on(&bucket).await?;
    let url = node.url();
    let r = within(
        Duration::from_secs(30),
        "purge then read back from the archive",
        async {
            register(&url, &sdk.url).await?;

            // ── Purge by the handler's retention.
            let (code, body) = invoke(&url, "Greeter", "hi", None, "{}").await?;
            ensure!(code == 200 && body["output"] == "hello", "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            wait_purged(&url, &id).await?;
            let (code, a) = get_json(format!("{url}/archive/{id}")).await?;
            ensure!(code == StatusCode::OK, "GET /archive/{id}: {code} {a}");
            ensure!(a["id"] == id.as_str(), "{a}");
            ensure!(a["status"] == "completed", "{a}");
            ensure!(a["output"] == "hello", "{a}");
            let journal = a["journal"]
                .as_array()
                .ok_or_else(|| anyhow!("no journal: {a}"))?;
            ensure!(journal.len() >= 2, "journal needs Input and Output: {a}");
            ensure!(journal[0]["index"] == 0, "{a}");

            // ── A purge by hand archives too, before deleting.
            let (code, body) = invoke(&url, "Greeter", "slow", None, "{}").await?;
            ensure!(code == 200, "{code} {body}");
            let id2 = body["id"].as_str().unwrap_or_default().to_string();
            let r = Client::new()
                .post(format!("{url}/invocations/{id2}/purge"))
                .send()
                .await?;
            ensure!(r.status() == StatusCode::OK, "purge: {}", r.status());
            let (code, a) = get_json(format!("{url}/archive/{id2}")).await?;
            ensure!(
                code == StatusCode::OK && a["output"] == "slow",
                "{code} {a}"
            );

            // ── Layout on the bucket.
            let paths = archived(&bucket).await?;
            for want in [&id, &id2] {
                let p = paths
                    .iter()
                    .find(|p| p.as_ref().ends_with(want.as_str()))
                    .ok_or_else(|| anyhow!("no archive object for {want}: {paths:?}"))?;
                let parts: Vec<&str> = p.as_ref().split('/').collect();
                ensure!(
                    parts.len() == 4
                        && parts[0] == "archive"
                        && parts[1] == "default"
                        && parts[2].len() == 10,
                    "archive path has the wrong layout: {p}"
                );
            }

            // ── Absent ⇒ 404.
            let (code, _) = get_json(format!("{url}/archive/{}", "0".repeat(48))).await?;
            ensure!(
                code == StatusCode::NOT_FOUND,
                "archive does not exist: {code}"
            );
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// **RD7** — a deterministic id runs again after being purged.
///
/// 1. `Greeter/hi` with `Idempotency-Key: K`, input `{"run":1}`; retention expires
///    (200 ms) ⇒ purged, the archive written at `archive/default/<today>/<id>`.
/// 2. The mapping of key `K` is now orphaned ⇒ calling `K` again with input
///    `{"run":2}` creates a new invocation **with the same id**.
/// 3. Run 2 is purged today too ⇒ the same path. The old code got `AlreadyExists`,
///    took it as already archived, and deleted run 2's journal.
///
/// Must see: `GET /archive/{id}` returns the latest run (input `{"run":2}`),
/// and both runs have an archive object that decodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rerun_of_idempotent_id_purged_the_same_day_is_archived() -> Result<()> {
    let bucket = common::bucket();
    let sdk = sdk().await?;
    let node = start_on(&bucket).await?;
    let url = node.url();
    let r = within(
        Duration::from_secs(30),
        "run again with the same key, then purge",
        async {
            register(&url, &sdk.url).await?;
            let call = |body: &'static str| {
                let url = url.clone();
                async move {
                    let r = Client::new()
                        .post(format!("{url}/invoke/Greeter/hi"))
                        .header("Idempotency-Key", "K")
                        .body(body)
                        .send()
                        .await?;
                    let code = r.status();
                    Ok::<_, anyhow::Error>((code, r.json::<Value>().await?))
                }
            };
            let (code, body) = call(r#"{"run":1}"#).await?;
            ensure!(code == 200, "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            wait_purged(&url, &id).await?;

            let (code, body) = call(r#"{"run":2}"#).await?;
            ensure!(code == 200, "{code} {body}");
            ensure!(
                body["id"] == id.as_str(),
                "key K must derive the same id: {body}"
            );
            wait_purged(&url, &id).await?;

            let (code, a) = get_json(format!("{url}/archive/{id}")).await?;
            ensure!(code == StatusCode::OK, "GET /archive/{id}: {code} {a}");
            let want: String = br#""run":2"#.iter().map(|b| format!("{b:02x}")).collect();
            let input = a["journal"][0]["payload"].as_str().unwrap_or_default();
            ensure!(
                input.contains(&want),
                "archive returns run 1 — run 2 was deleted without being archived: {a}"
            );

            let mine: Vec<_> = archived(&bucket)
                .await?
                .into_iter()
                .filter(|p| p.as_ref().contains(id.as_str()))
                .collect();
            ensure!(mine.len() == 2, "one archive object per run: {mine:?}");
            for p in &mine {
                let raw = bucket.get(p).await?.bytes().await?;
                cairn_store::archive::ArchiveRecord::decode(&raw)
                    .map_err(|e| anyhow!("{p}: {e}"))?;
            }
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// Tenant with the hash chain on ⇒ the archive carries the last hash, `?verify=true`
/// matches. Change one byte of the journal in the archive (decompress, edit,
/// recompress — a careful tamperer) ⇒ `verified: false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tampered_archive_fails_verification() -> Result<()> {
    let bucket = common::bucket();
    let sdk = sdk().await?;
    let node = start_on(&bucket).await?;
    let url = node.url();
    let r = within(Duration::from_secs(30), "tampered archive", async {
        let r = Client::new()
            .put(format!("{url}/admin/tenants/default"))
            .json(&serde_json::json!({ "hashChain": true }))
            .send()
            .await?;
        ensure!(r.status().is_success(), "enable hash chain: {}", r.status());
        register(&url, &sdk.url).await?;

        let (code, body) = invoke(&url, "Greeter", "hi", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        wait_purged(&url, &id).await?;

        let (code, a) = get_json(format!("{url}/archive/{id}?verify=true")).await?;
        ensure!(code == StatusCode::OK, "{code} {a}");
        ensure!(
            a["verified"] == true,
            "an intact archive must match the chain: {a}"
        );

        // A deliberate edit: change the output in the journal, recompress, overwrite.
        let path = archived(&bucket)
            .await?
            .into_iter()
            .find(|p| p.as_ref().ends_with(id.as_str()))
            .ok_or_else(|| anyhow!("no archive object"))?;
        let raw = bucket.get(&path).await?.bytes().await?;
        let mut plain = zstd::decode_all(raw.as_ref())?;
        let needle = b"hello";
        let mut hits = 0;
        for i in 0..=plain.len().saturating_sub(needle.len()) {
            if &plain[i..i + needle.len()] == needle {
                plain[i] = b'X';
                hits += 1;
            }
        }
        ensure!(hits > 0, "no output in the decompressed archive");
        let forged = zstd::encode_all(plain.as_slice(), 3)?;
        bucket.put(&path, PutPayload::from(forged)).await?;

        let (code, a) = get_json(format!("{url}/archive/{id}?verify=true")).await?;
        ensure!(code == StatusCode::OK, "{code} {a}");
        ensure!(a["verified"] == false, "tampered archive still passes: {a}");

        // Garbage bytes (cannot be decompressed) do not pass either.
        bucket.put(&path, PutPayload::from_static(b"junk")).await?;
        let (_, a) = get_json(format!("{url}/archive/{id}?verify=true")).await?;
        ensure!(a["verified"] != true, "garbage archive still passes: {a}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`?date=` of `GET /archive/{id}` must be a date.** The value goes into the
/// object path; `/archive?date=` already checked its form, `/archive/{id}` did not.
///
/// Must see: `../x`, `2026-1-01`, `20261101` ⇒ `400`; `2026-11-02` ⇒ `404`
/// (no archive) — the right form still goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_date_must_be_a_day() -> Result<()> {
    let bucket = common::bucket();
    let node = start_on(&bucket).await?;
    let url = node.url();
    let id = "0".repeat(48);
    let r = async {
        for bad in ["..%2Fx", "2026-1-01", "20261101", "2026-11-02%2F.."] {
            let (code, v) = get_json(format!("{url}/archive/{id}?date={bad}")).await?;
            ensure!(code == StatusCode::BAD_REQUEST, "date={bad}: {code} {v}");
        }
        let (code, v) = get_json(format!("{url}/archive/{id}?date=2026-11-02")).await?;
        ensure!(code == StatusCode::NOT_FOUND, "valid date: {code} {v}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// The number of store GETs to `archive/…` objects that `store` has counted.
fn archive_gets(store: &cairn_store::testkit::FaultyStore) -> u64 {
    store
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, kind, _)| *op == cairn_store::testkit::Op::Get && *kind == "archive")
        .map(|(.., n)| n)
        .sum()
}

/// **Looking up the archive of an unknown id costs a bounded number of GETs, and
/// is charged to the tenant's read bucket.**
///
/// 1. `GET /archive/<missing id>?days=366` with a read key: formerly one sequential
///    GET per day until `NotFound` ⇒ 366 store GETs for one request.
/// 2. Repeat 100 times in a row.
///
/// Must see: (1) ≤ 32 store GETs (`days` capped at 31); (2) some request gets
/// `429` (the tenant's read bucket, `TenantBudget`, runs dry) — this read path is
/// no longer outside the bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_id_costs_bounded_store_reads() -> Result<()> {
    let bucket = common::bucket();
    let inner: Arc<dyn ObjectStore> = (*bucket).clone();
    let faulty = cairn_store::testkit::FaultyStore::wrap(inner);
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(2));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("could not build the node: {e}"))?;
    let url = node.url();
    let id = "0".repeat(48);
    let r = async {
        let before = archive_gets(&faulty);
        let (code, v) = get_json(format!("{url}/archive/{id}?days=366")).await?;
        ensure!(code == StatusCode::NOT_FOUND, "unknown id: {code} {v}");
        let spent = archive_gets(&faulty) - before;
        ensure!(spent <= 32, "unknown id lookup cost {spent} store GETs");
        let mut throttled = 0;
        for _ in 0..100 {
            let (code, _) = get_json(format!("{url}/archive/{id}?days=366")).await?;
            if code == StatusCode::TOO_MANY_REQUESTS {
                throttled += 1;
            }
        }
        ensure!(
            throttled > 0,
            "100 lookups of an unknown id in a row never touched the tenant's read bucket"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}
