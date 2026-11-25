// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tenant purge: `PUT /admin/tenants/{t}/state deleting` ⇒ phase A (hold in
//! place, reversible) ⇒ after `keep`, phase B (the `purging-ms=` mark, delete
//! the rows in every range, delete the prefixes, remove the configuration) ⇒
//! the tombstone `state=purged`.
//!
//! The test's `keep` is a few seconds (`tenant_purge_keep`); product: 7 days.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    bytes::Bytes,
    cairn_server::auth::AuthConfig,
    common::{
        fake_sdk::{FakeSdk, Step},
        within, ClusterNode, TEST_DEADLINE,
    },
    futures::TryStreamExt,
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt},
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_tenant_purge_bootstrap_key_0123456";
const SECRET: &str = "cf_test_cluster_secret_tenant_purge_0123456789";
const TTL: Duration = Duration::from_secs(2);

async fn node(bucket: &common::Bucket, id: &str, keep: Duration) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, TTL, |cfg| configure(cfg, keep)).await
}

/// Like [`node`], on a `FaultyStore` **shared** between the nodes (the store's
/// rules — e.g. keeping empty directories — apply to every node).
async fn node_on(
    store: &std::sync::Arc<cairn_store::testkit::FaultyStore>,
    id: &str,
    keep: Duration,
) -> Result<cairn_server::node::RunningNode> {
    common::start_cluster_node_on(store, id, 2, TTL, |cfg| configure(cfg, keep)).await
}

fn configure(cfg: &mut cairn_server::node::NodeConfig, keep: Duration) {
    {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(100);
        cfg.tenant_purge_keep = Some(keep);
        // Durable statistics flush fast: `/stats/history` has numbers from before the purge.
        cfg.metering_flush = Duration::from_millis(200);
        // The product's compaction coordinator polls every hour — the full
        // compaction the purge loop asks for through `Admin` is seen on the next
        // poll. The test shortens it to 200 ms.
        cfg.stores = cfg.stores.clone().with_polls(cairn_store::PollIntervals {
            compactor: Duration::from_millis(200),
            compaction_worker: Duration::from_millis(200),
            ..cairn_store::DEFAULT_POLLS
        });
    }
}

#[derive(Clone)]
struct As {
    url: String,
    key: String,
    http: Client,
}

impl As {
    fn admin(url: &str) -> Self {
        Self {
            url: url.into(),
            key: ADMIN_KEY.into(),
            http: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    /// An `admin`-role key for `tenant` (retried until the node takes its lease).
    async fn tenant(url: &str, tenant: &str) -> Result<Self> {
        let admin = Self::admin(url);
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = admin
                .send(
                    admin
                        .http
                        .post(format!("{url}/admin/keys"))
                        .json(&json!({ "tenant": tenant, "role": "admin" })),
                )
                .await?;
            if code == StatusCode::OK {
                return Ok(Self {
                    key: v["secret"].as_str().unwrap_or_default().to_string(),
                    ..admin
                });
            }
            ensure!(
                t0.elapsed() < TEST_DEADLINE,
                "create key {tenant}: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
        let r = req.bearer_auth(&self.key).send().await?;
        let code = r.status();
        let text = r.text().await?;
        Ok((
            code,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    async fn post(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .post(format!("{}{path}", self.url))
                .body(body.to_string()),
        )
        .await
    }

    async fn put(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(self.http.put(format!("{}{path}", self.url)).json(&body))
            .await
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value)> {
        self.send(self.http.get(format!("{}{path}", self.url)))
            .await
    }

    async fn set_state(&self, tenant: &str, state: &str) -> Result<()> {
        let (code, v) = self
            .put(
                &format!("/admin/tenants/{tenant}/state"),
                json!({ "state": state }),
            )
            .await?;
        ensure!(
            code == StatusCode::NO_CONTENT,
            "{tenant}={state}: {code} {v}"
        );
        Ok(())
    }

    /// Wait until `GET /admin/tenants/{t}/purge` satisfies `ok`.
    async fn purge_until(
        &self,
        tenant: &str,
        what: &str,
        ok: impl Fn(&Value) -> bool,
    ) -> Result<Value> {
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = self.get(&format!("/admin/tenants/{tenant}/purge")).await?;
            if code == StatusCode::OK && ok(&v) {
                return Ok(v);
            }
            ensure!(t0.elapsed() < Duration::from_secs(60), "{what}: {code} {v}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn id_of(v: &Value) -> Result<String> {
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

async fn objects_under(bucket: &common::Bucket, prefix: &str) -> Result<Vec<String>> {
    let store: std::sync::Arc<dyn ObjectStore> = (**bucket).clone();
    Ok(store
        .list(Some(&Path::from(prefix)))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await?)
}

async fn book(bucket: &common::Bucket) -> Result<String> {
    Ok(
        String::from_utf8_lossy(&bucket.get(&Path::from("tenants")).await?.bytes().await?)
            .into_owned(),
    )
}

/// The shared SDK: an unkeyed service, an object writing state (one small key,
/// one segmented key of 300 KiB), a handler suspended waiting for a signal.
async fn sdk() -> Result<FakeSdk> {
    FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .object(
            "Counter",
            "add",
            vec![
                Step::SetState {
                    key: "n".into(),
                    value: Bytes::from_static(b"1"),
                },
                // A segmented value (≥ `seal_bytes` 256 KiB): a segment under
                // `values/<tenant>/`, a `SegmentedState` row and a `ValueRefs`
                // holder in the Db.
                Step::SetState {
                    key: "history".into(),
                    value: Bytes::from(format!("\"{}\"", "h".repeat(300 << 10))),
                },
                Step::output("added"),
            ],
        )
        .object(
            "Counter",
            "read",
            vec![
                Step::GetState {
                    key: "history".into(),
                },
                Step::EchoCallResult,
            ],
        )
        .service("Hold", "wait", vec![Step::Suspend, Step::output("woke")])
        .spawn()
        .await
}

/// Write `who`'s work (a tenant that has registered the SDK): one completed
/// invocation, one object with state, one suspended invocation, one schedule.
/// Returns the id of the completed invocation.
async fn work(who: &As, sdk: &FakeSdk) -> Result<String> {
    let (code, v) = who
        .send(
            who.http
                .post(format!("{}/deployments", who.url))
                .json(&json!({ "uri": sdk.url })),
        )
        .await?;
    ensure!(code == StatusCode::OK, "register: {code} {v}");
    let (code, v) = who.post("/invoke/Echo/hi", json!("x")).await?;
    ensure!(code == StatusCode::OK, "invoke: {code} {v}");
    let done = id_of(&v)?;
    let (code, v) = who.post("/invoke/Counter/add?key=c1", json!("x")).await?;
    ensure!(code == StatusCode::OK, "object: {code} {v}");
    let (code, v) = who.post("/invoke/Hold/wait/send", json!("x")).await?;
    ensure!(
        code == StatusCode::OK || code == StatusCode::ACCEPTED,
        "suspend: {code} {v}"
    );
    let (code, v) = who
        .put(
            "/schedules/tick",
            json!({ "everyMs": 3_600_000, "target": { "service": "Echo", "handler": "hi" } }),
        )
        .await?;
    ensure!(code == StatusCode::OK, "schedule: {code} {v}");
    Ok(done)
}

/// **A full purge**: two tenants both have a completed invocation, an object
/// with state, a suspended invocation, a schedule, a deployment. `acme` enters
/// `deleting` with a `keep` of 4 s.
///
/// Must see:
/// 1. phase A: every prefix of `acme` has a held-in-place entry (one entry per
///    `PrefixKind`), the `deployments/acme/…` objects are intact;
/// 2. after `keep`: the book becomes `state=purged` with three marks; no held
///    entry left, no object left under `acme`'s prefixes (including the
///    `values/acme` segments); `acme`'s keys are gone from `auth/keys`; a
///    cluster key reading an old `acme` invocation ⇒ `404`; an `audit/ctl`
///    record says `acme` was purged;
/// 3. `other` loses nothing: its old invocation is readable, a new invoke runs,
///    the `deployments/other/…` objects remain; the `Bucket`'s `verify` is
///    clean on teardown (Db rows do not diverge from indexes, nothing leaks).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_tenant_is_purged_after_keep() -> Result<()> {
    let sdk = sdk().await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", Duration::from_secs(4)).await?;
    let url = n.node.url();
    within(TEST_DEADLINE * 3, "full purge", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme").await?;
        let other = As::tenant(&url, "other").await?;
        let acme_done = work(&acme, &sdk).await?;
        let other_done = work(&other, &sdk).await?;
        ensure!(!objects_under(&bucket, "deployments/acme").await?.is_empty());
        // The `/stats/history` cache is warm with acme's numbers.
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = admin.get("/stats/history?days=30&tenant=acme").await?;
            if code == StatusCode::OK && v["rows"].as_array().is_some_and(|r| !r.is_empty()) {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "stats acme: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        admin.set_state("acme", "deleting").await?;
        // 1.
        let v = admin
            .purge_until("acme", "phase A holds every prefix", |v| {
                v["held"]
                    .as_array()
                    .is_some_and(|h| h.len() == cairn_store::tenant_purge::PrefixKind::ALL.len())
            })
            .await?;
        ensure!(
            v["state"] == "deleting" && v["purgingMs"].is_null(),
            "phase A: {v}"
        );
        ensure!(
            !objects_under(&bucket, "deployments/acme").await?.is_empty(),
            "phase A deleted objects"
        );
        // 2.
        let v = admin
            .purge_until("acme", "purged", |v| v["state"] == "purged")
            .await?;
        ensure!(
            v["held"].as_array().is_some_and(Vec::is_empty)
                && v["objects"]
                    .as_object()
                    .is_some_and(serde_json::Map::is_empty)
                && v["purgingMs"].is_u64()
                && v["purgedMs"].is_u64(),
            "after purge: {v}"
        );
        let line = book(&bucket).await?;
        ensure!(line.contains("acme state=purged"), "{line}");
        let (code, v) = admin.get("/stats/history?days=30&tenant=acme").await?;
        ensure!(
            code == StatusCode::OK && v["rows"].as_array().is_some_and(Vec::is_empty),
            "the /stats/history cache still has the purged tenant's numbers: {code} {v}"
        );
        for p in [
            "deployments/acme",
            "archive/acme",
            "audit/tenant/acme",
            "metering/acme",
            "stats/acme",
            "artifacts/acme",
            "values/acme",
        ] {
            let left = objects_under(&bucket, p).await?;
            ensure!(left.is_empty(), "{p} still has: {left:?}");
        }
        let keys =
            String::from_utf8_lossy(&bucket.get(&Path::from("auth/keys")).await?.bytes().await?)
                .into_owned();
        ensure!(
            !keys
                .lines()
                .any(|l| l.split('\t').any(|f| f == "acme") || l.contains(" acme ")),
            "acme keys left: {keys}"
        );
        let (code, v) = admin.get(&format!("/invocations/{acme_done}")).await?;
        ensure!(
            code == StatusCode::NOT_FOUND,
            "invocation acme after purge: {code} {v}"
        );
        let mut audited = false;
        for p in objects_under(&bucket, "audit/ctl").await? {
            let body = bucket.get(&Path::from(p)).await?.bytes().await?;
            audited |= String::from_utf8_lossy(&body).contains("purge tenant acme");
        }
        ensure!(audited, "no audit/ctl record");
        // 3.
        let (code, v) = other.get(&format!("/invocations/{other_done}")).await?;
        ensure!(
            code == StatusCode::OK && v["status"] == "completed",
            "other lost an invocation: {code} {v}"
        );
        let (code, v) = other.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "other post-purge: {code} {v}");
        ensure!(!objects_under(&bucket, "deployments/other")
            .await?
            .is_empty());
        let (code, v) = other.get("/schedules/tick").await?;
        ensure!(code == StatusCode::OK, "other's schedule: {code} {v}");
        // `other`'s segmented value is intact: the segment remains and reads back in full.
        ensure!(!objects_under(&bucket, "values/other").await?.is_empty());
        let (code, v) = other
            .post("/invoke/Counter/read?key=c1", json!("x"))
            .await?;
        ensure!(
            code == StatusCode::OK && v["output"].as_str().map(str::len) == Some((300 << 10) + 2),
            "other reads its segmented value back: {code}"
        );
        Ok(())
    })
    .await
}

/// **On an idle cluster phase B does not wait for the compactor's next poll.**
/// A purge measurement with a 60 s `keep` reached `purged` only after 3 566 s.
/// Before the tombstone the purge asks for a full compaction through
/// `Admin::submit_compaction`; that compaction stayed `Submitted` until the
/// compaction coordinator's next poll (`DEFAULT_POLLS.compactor` = 1 h),
/// because the in-process signal only reports **new** L0s, and the purge loop's
/// memtable flush runs **before** the compaction request. The other tests of
/// the file shorten the poll to 200 ms, so they do not see it.
///
/// 1. One node with the **default** poll intervals, a `keep` of 5 s; `acme` has
///    work like the tests above; no other load.
/// 2. `acme` enters `deleting`; wait for the `purging-ms=` mark (phase B).
///
/// Must see: the book becomes `purged` within 60 s of phase B (the limit of
/// `purge_until`), and `lastRound.compacted` lists both ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_cell_purge_does_not_wait_for_the_compactor_poll() -> Result<()> {
    let sdk = sdk().await?;
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "n1", 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(100);
        cfg.tenant_purge_keep = Some(Duration::from_secs(5));
    })
    .await?;
    let url = n.node.url();
    within(Duration::from_secs(150), "purge, idle cluster", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme").await?;
        work(&acme, &sdk).await?;
        admin.set_state("acme", "deleting").await?;
        admin
            .purge_until("acme", "phase B", |v| v["purgingMs"].is_u64())
            .await?;
        let v = admin
            .purge_until("acme", "purged on an idle cluster", |v| {
                v["state"] == "purged"
            })
            .await?;
        let mut compacted: Vec<u64> = v["lastRound"]["compacted"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        compacted.sort_unstable();
        ensure!(compacted == vec![0, 1], "both ranges compacted: {v}");
        Ok(())
    })
    .await
}

/// **Reversal in phase A** (`keep` 40 s — a 5 s loop beat): `deleting`, then
/// `suspended` before `keep` ⇒ the next round removes every held entry and
/// deletes nothing; `active` again ⇒ the old invocation is readable, new work
/// runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reverting_in_phase_a_releases_the_holds() -> Result<()> {
    let sdk = sdk().await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", Duration::from_secs(40)).await?;
    let url = n.node.url();
    within(TEST_DEADLINE * 2, "reversal", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme").await?;
        let done = work(&acme, &sdk).await?;
        let deployments = objects_under(&bucket, "deployments/acme").await?;
        admin.set_state("acme", "deleting").await?;
        admin
            .purge_until("acme", "phase A holds", |v| {
                v["held"]
                    .as_array()
                    .is_some_and(|h| h.len() == cairn_store::tenant_purge::PrefixKind::ALL.len())
            })
            .await?;
        admin.set_state("acme", "suspended").await?;
        admin
            .purge_until("acme", "held entries removed", |v| {
                v["held"].as_array().is_some_and(Vec::is_empty)
            })
            .await?;
        ensure!(objects_under(&bucket, "deployments/acme").await? == deployments);
        admin.set_state("acme", "active").await?;
        let (code, v) = acme.get(&format!("/invocations/{done}")).await?;
        ensure!(
            code == StatusCode::OK && v["status"] == "completed",
            "{code} {v}"
        );
        let (code, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        Ok(())
    })
    .await
}

/// **A state change wakes the purge loop** — the product's `keep` (7 days ⇒ a
/// 5-minute loop beat), two nodes, `PUT …/state` through each node in turn (one
/// of the two is not the runner: the registry-change notice to the peer must
/// wake the runner):
///
/// 1. Two nodes come up; the purge loop's first round runs when it takes the
///    role, then rests for 5 minutes.
/// 2. `acme` enters `deleting` through n1 ⇒ phase A's held entries appear
///    within a few seconds.
/// 3. `acme` goes back to `suspended` through n2 ⇒ the held entries are removed
///    within a few seconds.
///
/// Before the fix both waited for the next 5-minute beat: a backup taken in
/// that gap carried orphan held entries of a tenant that had been reversed, and
/// `verify` on the restored bucket found `tenant-residue` (seen in a test
/// environment).
///
/// Must see: both steps complete within the 60 s limit of `purge_until`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_change_wakes_the_purge_round() -> Result<()> {
    let sdk = sdk().await?;
    let bucket = common::bucket();
    let keep = Duration::from_secs(7 * 24 * 3600);
    let n1 = node(&bucket, "n1", keep).await?;
    let n2 = node(&bucket, "n2", keep).await?;
    within(TEST_DEADLINE * 3, "wake the purge loop", async {
        let acme = As::tenant(&n1.node.url(), "acme").await?;
        work(&acme, &sdk).await?;
        let via1 = As::admin(&n1.node.url());
        let via2 = As::admin(&n2.node.url());
        via1.set_state("acme", "deleting").await?;
        via1.purge_until("acme", "phase A holds", |v| {
            v["held"].as_array().is_some_and(|h| !h.is_empty())
        })
        .await?;
        via2.set_state("acme", "suspended").await?;
        via2.purge_until("acme", "held entries removed", |v| {
            v["held"].as_array().is_some_and(Vec::is_empty)
        })
        .await?;
        Ok(())
    })
    .await
}

/// **Force a compaction before the tombstone** (G23 in
/// `docs/reliability/irreversible.md`): after a range is clean of the tenant's
/// rows, the runner flushes the range's memtable (so the tombstones reach L0),
/// then asks for a full compaction into the last run, and only writes the
/// tombstone once every compaction has finished.
///
/// Must see: when the book becomes `purged`, the runner's `lastRound.compacted`
/// lists both of the node's ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_compacts_every_range_before_the_tombstone() -> Result<()> {
    let sdk = sdk().await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", Duration::from_secs(4)).await?;
    let url = n.node.url();
    within(TEST_DEADLINE * 3, "compact before the tombstone", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme").await?;
        work(&acme, &sdk).await?;
        admin.set_state("acme", "deleting").await?;
        let v = admin
            .purge_until("acme", "purged", |v| v["state"] == "purged")
            .await?;
        let mut compacted: Vec<u64> = v["lastRound"]["compacted"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        compacted.sort_unstable();
        ensure!(compacted == [0, 1], "compacted at the tombstone: {v}");
        Ok(())
    })
    .await
}

/// **Leftover progress entries after the tombstone are cleaned up**: two
/// runners at once during a leader change (the loser writes a range-clean proof
/// after the winner has written the tombstone), or an older binary writing the
/// tombstone, leave objects under `tenant-purge/<t>/` of a tenant that is
/// already `purged`. The test builds the situation directly: after `purged`,
/// write a proof to `tenant-purge/acme/99`.
///
/// Must see: the purge loop cleans it up (holds it in place, then deletes it
/// through `DeleteProof::purged_tenant_progress`) — no object left, no held
/// entry left; the `Bucket`'s `verify` is clean on teardown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leftover_purge_progress_after_the_tombstone_is_swept() -> Result<()> {
    let sdk = sdk().await?;
    let bucket = common::bucket();
    let n = node(&bucket, "n1", Duration::from_secs(4)).await?;
    let url = n.node.url();
    within(TEST_DEADLINE * 3, "clean leftover entries", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme").await?;
        work(&acme, &sdk).await?;
        admin.set_state("acme", "deleting").await?;
        let v = admin
            .purge_until("acme", "purged", |v| v["state"] == "purged")
            .await?;
        let purging = v["purgingMs"]
            .as_u64()
            .ok_or_else(|| anyhow!("no purgingMs: {v}"))?;
        let tenant = cairn_store::TenantId::new("acme")?;
        let late = cairn_store::tenant_purge::RangeProof {
            range: 99,
            purging_ms: purging,
            basis: cairn_store::tenant_purge::RangeBasis::Compacted("-".into()),
            by: "losing-runner#1".into(),
        };
        let path = cairn_store::tenant_purge::proof_path(&Path::from(""), &tenant, 99);
        bucket.put(&path, late.encode().into()).await?;
        ensure!(
            !objects_under(&bucket, "tenant-purge/acme")
                .await?
                .is_empty(),
            "cannot write the leftover entry"
        );
        let v = admin
            .purge_until("acme", "leftover entry cleaned up", |v| {
                v["objects"].as_object().is_some_and(|o| o.is_empty())
                    && v["held"].as_array().is_some_and(Vec::is_empty)
            })
            .await?;
        ensure!(v["state"] == "purged", "{v}");
        let left = objects_under(&bucket, "tenant-purge/acme").await?;
        ensure!(left.is_empty(), "left: {left:?}");
        Ok(())
    })
    .await
}

/// **The `deleting-ms` mark is written only when the node's clock checks out
/// against the store.** A node writes the mark with its own clock; a node slow
/// by Δ shortens the 7-day recovery window by Δ.
///
/// 1. A lone node; the clock-skew probe `PUT` (`gc-clock-probe`) always fails —
///    the node cannot check its clock.
/// 2. `PUT /admin/tenants/acme/state` `deleting`.
/// 3. Remove the fault, send again.
///
/// Must see: (2) `503`, the book carries no `deleting-ms`; (3) `204`.
///
/// Before the fix: the handler did not go through the clock gate ⇒ (2) `204`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_mark_needs_a_node_clock_checked_against_the_store() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let n = node(&bucket, "n1", Duration::from_secs(3600)).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE, "clock gate of the deleting mark", async {
        let admin = As::admin(&url);
        As::tenant(&url, "acme").await?;
        let rule = n.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(cairn_server::gc::CLOCK_PROBE.into()),
            Action::Fail { times: None },
        ));
        let (code, v) = admin
            .put("/admin/tenants/acme/state", json!({ "state": "deleting" }))
            .await?;
        ensure!(
            code == StatusCode::SERVICE_UNAVAILABLE,
            "clock not checked, yet deleting was accepted: {code} {v}"
        );
        // The book may not exist yet (no tenant has changed its flag): absent also means "no mark".
        let text = book(&bucket).await.unwrap_or_default();
        ensure!(
            !text.contains("deleting-ms="),
            "the book carries deleting-ms although it was refused: {text}"
        );
        n.store.remove_rule(rule);
        admin.set_state("acme", "deleting").await?;
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **The `purging-ms` mark also requires an age by the store's clock.** The
/// `deleting-ms` mark carries the clock of the node that wrote it (possibly days
/// slow); the runner only checks its own clock.
///
/// 1. The book: `acme` `deleting`, `deleting-ms` = 8 days ago (as if written by
///    a node 8 days slow). The node's purge loop is off.
/// 2. A separate `Purger` (`keep` 1 hour) runs `run_once` twice.
///
/// Must see: no `purging-ms=` in the book; the round reports why it is blocked —
/// phase A's held-in-place entries (written in round 1, `last_modified` by the
/// store's clock) are not `keep` old yet.
///
/// Before the fix: round 1 held in place and then wrote `purging-ms=` at once —
/// the point of no return after a few seconds instead of 7 days.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purging_mark_waits_for_the_store_clock_age_of_phase_a() -> Result<()> {
    use {
        cairn_store::{ObjectStores, StoreLocation, TenantId},
        std::sync::Arc,
    };
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "n1", 2, TTL, |cfg| {
        cfg.tenant_purge_keep = None;
    })
    .await?;
    let r = within(TEST_DEADLINE, "purging mark by the store clock", async {
        let stores = ObjectStores::single(StoreLocation::from_store(
            Arc::clone(&bucket) as Arc<dyn ObjectStore>,
            "",
            "memory://",
        ));
        let main = stores.main();
        let settings = cairn_server::tenants::TenantSettings::new(Some((
            Arc::clone(main.store()),
            main.prefix(),
        )));
        let acme = TenantId::new("acme")?;
        let eight_days_ago = cairn_store::entropy::now_ms() - 8 * 24 * 3_600_000;
        settings
            .try_update(&acme, |f| {
                f.change_state(&acme, cairn_engine::TenantState::Deleting, eight_days_ago)
                    .map(|_| ())
            })
            .await
            .map_err(|e| anyhow!("book: {e}"))?
            .map_err(|e| anyhow!("book: {e:?}"))?;
        ensure!(book(&bucket).await?.contains("deleting-ms="));
        let purger = cairn_server::tenant_purge::Purger {
            stores,
            cluster: n.node.cluster.clone(),
            partitions: n.node.partitions.clone(),
            routing: n.node.routing.clone(),
            secret: None,
            client: Client::new(),
            settings,
            keys: None,
            rules: None,
            keep: Duration::from_secs(3600),
            last: Default::default(),
            compactions: Default::default(),
            watches: Default::default(),
            lots_per_range: cairn_server::tenant_purge::LOTS_PER_RANGE,
            sweeps: Default::default(),
            checks: Default::default(),
            bases: Default::default(),
        };
        let mut last = None;
        for _ in 0..2 {
            let rounds = purger.run_once().await.map_err(|e| anyhow!("{e}"))?;
            last = rounds.into_iter().find(|r| r.tenant == "acme");
        }
        let text = book(&bucket).await?;
        ensure!(
            !text.contains("purging-ms="),
            "purging-ms written by the slow node's deleting-ms mark: {text} — round: {last:?}"
        );
        ensure!(
            last.as_ref().is_some_and(|r| r.blocked.is_some()),
            "the round does not say why it has not moved to phase B: {last:?}"
        );
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// One `GET /admin/tenants/acme/purge` with a 15 s client limit like the chaos
/// harness (`chaos.ts`); returns the code, the body and the time taken.
async fn timed_status(url: &str) -> Result<(StatusCode, Value, Duration)> {
    let http = Client::builder().timeout(Duration::from_secs(15)).build()?;
    let t0 = std::time::Instant::now();
    let r = http
        .get(format!("{url}/admin/tenants/acme/purge"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await
        .map_err(|e| anyhow!("poll failed after {:?}: {e}", t0.elapsed()))?;
    let code = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    Ok((code, v, t0.elapsed()))
}

/// **A `GET …/purge` poll is bounded on a large trash with a cold cache** (seen
/// in a chaos run). Under chaos a node is killed every ~10 s, so the answering
/// node's entry classification memory (`Seen`) is always cold; the trash had
/// 8 000–9 000 entries (mostly `wal-log`). The poll GOT the `entry` of every
/// unclassified entry **in sequence** ⇒ 10/10 polls exceeded the harness's 15 s
/// limit.
///
/// 1. The bucket already has 10 000 ordinary trash entries (`op`, not held in
///    place).
/// 2. The store delays every GET under `trash/` by 60 ms (in sequence: 600 s for
///    one classification pass); the node comes up **after** the delay rule is
///    set (cold cache).
/// 3. Poll `GET /admin/tenants/acme/purge` repeatedly.
///
/// Must see: every poll returns `200` within 12 s (the server's limit, under the
/// harness's 15 s) and states the unclassified part clearly (`heldComplete`,
/// `heldUnchecked`); within 90 s one complete poll (`heldComplete: true`, no
/// held entry). Before the fix: the first poll took over 15 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_status_stays_bounded_on_a_cold_ten_thousand_entry_trash() -> Result<()> {
    use cairn_store::testkit::{Action, FaultyStore, LatencyDist, OpSet, PathMatch, Rule};
    const ENTRIES: u64 = 10_000;
    let bucket = common::bucket();
    let now = cairn_store::entropy::now_ms();
    for k in 0..ENTRIES {
        let id = cairn_store::trash::EntryId::new(now - 600_000 + k);
        let path = Path::from(format!(
            "trash/{}/{id}/entry",
            cairn_store::archive::utc_date(id.created_ms())
        ));
        let body = format!(
            "cairnflow-trash/1\nkind=op\nkey={}\nreason=test\n",
            1_000_000 + k
        );
        bucket.put(&path, body.into()).await?;
    }
    let store = FaultyStore::wrap((*bucket).clone());
    store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("trash/".into()),
        Action::Latency {
            dist: LatencyDist::fixed(Duration::from_millis(60)),
            seed: 7,
        },
    ));
    let n = node_on(&store, "n1", Duration::from_secs(3600)).await?;
    let url = n.url();
    let r = within(TEST_DEADLINE * 3, "bounded poll", async {
        As::tenant(&url, "acme").await?;
        let t0 = std::time::Instant::now();
        let mut polls = Vec::new();
        loop {
            let (code, v, took) = timed_status(&url).await?;
            polls.push((took, v["heldComplete"].clone(), v["heldUnchecked"].clone()));
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            ensure!(
                took < Duration::from_secs(12),
                "poll took {took:?} (server limit 12 s): {polls:?}"
            );
            ensure!(
                v["heldUnchecked"].is_u64() && v["heldComplete"].is_boolean(),
                "heldComplete/heldUnchecked missing: {v}"
            );
            if v["heldComplete"] == true {
                ensure!(v["heldUnchecked"] == 0, "{v}");
                ensure!(v["held"].as_array().is_some_and(Vec::is_empty), "{v}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(90),
                "no complete poll after 90 s: {polls:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    })
    .await;
    n.shutdown().await;
    r
}

/// **A node not running the purge loop does not report held entries another
/// node has deleted** (seen in a chaos run). Right after `purged`, node 0's API
/// (not the runner, `lastRound: null`) listed 17 held-in-place entries that the
/// runner (node 2) had really deleted: SeaweedFS still lists an empty entry
/// directory for 2–3 minutes after the last `entry` DELETE, and node 0's `Seen`
/// classification memory — which had classified those entries while they still
/// existed — is only told by the node that **deletes** them (`Seen::removed`).
///
/// 1. Two nodes on a store that keeps empty directories; `acme` enters
///    `deleting`, `keep` 4 s.
/// 2. Phase A: poll `GET …/purge` on **both** nodes until each node sees the
///    held entries (both classify them).
/// 3. The runner (one of the two) goes through phase B, really deletes every
///    prefix and held entry, and writes the tombstone.
///
/// Must see: after `purged`, a poll on **each** node returns an empty `held`.
/// Before the fix: the node that did not delete the entries reported them
/// forever (this store lists empty directories forever).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_status_on_another_node_does_not_report_purged_holds() -> Result<()> {
    let bucket = common::bucket();
    let store = cairn_store::testkit::FaultyStore::wrap((*bucket).clone());
    store.keep_empty_dirs();
    let keep = Duration::from_secs(4);
    let n1 = node_on(&store, "n1", keep).await?;
    let n2 = node_on(&store, "n2", keep).await?;
    let r = within(TEST_DEADLINE * 3, "held, deleted elsewhere", async {
        let via = [As::admin(&n1.url()), As::admin(&n2.url())];
        As::tenant(&n1.url(), "acme").await?;
        via[0].set_state("acme", "deleting").await?;
        for a in &via {
            let v = a
                .purge_until("acme", "phase A holds", |v| {
                    v["state"] == "purged" || v["held"].as_array().is_some_and(|h| !h.is_empty())
                })
                .await?;
            ensure!(
                v["state"] != "purged",
                "purged before the node saw the held entries: {v}"
            );
        }
        via[0]
            .purge_until("acme", "purged", |v| v["state"] == "purged")
            .await?;
        for a in &via {
            let (code, v) = a.get("/admin/tenants/acme/purge").await?;
            ensure!(code == StatusCode::OK, "{code} {v}");
            ensure!(
                v["held"].as_array().is_some_and(Vec::is_empty),
                "{}: purged, yet it still reports held entries: {v}",
                a.url
            );
        }
        Ok(())
    })
    .await;
    n1.shutdown().await;
    n2.shutdown().await;
    r
}

/// **A held-in-place entry written after the tombstone is removed.** Two runners
/// overlap when the runner role moves between nodes (`gc::is_runner` is only
/// asked at the start of a round; a chaos run saw node 0 and node 2 both ask
/// to compact range 8 within 150 ms). Interleaving:
///
/// 1. The old runner X starts a round: it reads `acme`'s held entries when the
///    new runner Y has already deleted part of them ⇒ X lacks the entry of the
///    `metering` prefix.
/// 2. X builds `DeleteProof::tenant_deleting` (reading the book: still
///    `deleting`).
/// 3. Y finishes its round: no held entry left, every prefix empty ⇒ CAS of the
///    `purged` tombstone.
/// 4. X PUTs the `metering` held entry, then X's round is blocked (phase B
///    rights require `deleting`).
///
/// The test builds the state step 4 leaves behind directly (a `metering` held
/// entry of a `purged` tenant, plus a late object under `metering/acme/`).
///
/// Must see: the purge loop removes the held entry (`held` back to empty) and
/// does **not** delete the object (removing the entry drops the promise to
/// delete; there is no proof for deleting data after the tombstone — rule 5 of
/// AGENTS.md; that object is a job for check (l) of `verify`). Before the fix:
/// the loop only cleaned the `tenant-purge` entries of a `purged` tenant ⇒ the
/// `metering` entry stayed forever, and the API reported a non-empty `held`
/// forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hold_written_after_the_tombstone_is_released() -> Result<()> {
    use {
        cairn_store::{
            proof::DeleteProof,
            tenant_purge::PrefixKind,
            trash::{EntryId, Subject, Trash},
            TenantId,
        },
        std::sync::Arc,
    };
    let bucket = common::bucket();
    bucket.allow(
        "tenant-residue",
        "the test deliberately leaves object metering/acme/late after the tombstone: removing the held entry must not delete it",
    );
    let n = node(&bucket, "n1", Duration::from_secs(4)).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 3, "held entry after the tombstone", async {
        let admin = As::admin(&url);
        As::tenant(&url, "acme").await?;
        admin.set_state("acme", "deleting").await?;
        admin
            .purge_until("acme", "purged", |v| v["state"] == "purged")
            .await?;
        let store: Arc<dyn ObjectStore> = (*bucket).clone();
        let late = Path::from("metering/acme/2026111300/late");
        store.put(&late, Bytes::from_static(b"late").into()).await?;
        Trash::new(store, &Path::from(""))
            .hold_in_place(
                &EntryId::new(cairn_store::entropy::now_ms()),
                &DeleteProof::assume(Subject::TenantPrefix {
                    tenant: TenantId::new("acme")?,
                    kind: PrefixKind::Metering,
                }),
                "old runner holds after the tombstone (test)",
            )
            .await?;
        let (_, v) = admin.get("/admin/tenants/acme/purge").await?;
        ensure!(
            v["held"].as_array().is_some_and(|h| !h.is_empty()),
            "bad setup: the API does not see the held entry just written: {v}"
        );
        let v = admin
            .purge_until("acme", "held entry after the tombstone removed", |v| {
                v["held"].as_array().is_some_and(Vec::is_empty)
            })
            .await?;
        ensure!(v["state"] == "purged", "{v}");
        ensure!(
            bucket.head(&late).await.is_ok(),
            "removing the held entry after the tombstone deleted the object (no proof)"
        );
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}
