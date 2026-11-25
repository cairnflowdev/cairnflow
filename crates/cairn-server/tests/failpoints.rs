// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Failpoints on a real node: `POST/GET /admin/failpoints` and the points of
//! cairn-server.
//!
//! Built only with `--features failpoints` (`required-features`). The node runs in
//! the test process, so the `fail` registry is **shared** between the test and the
//! node; every test opens with `StepController::new()` to hold the scenario lock (the
//! tests of this binary run one at a time) and to clear the points when done.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    cairn_server::node::{self, NodeMode},
    cairn_store::testkit::StepController,
    common::{test_config, within, TEST_DEADLINE},
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
};

const ADMIN_KEY: &str = "cf_test_failpoints_bootstrap_key_012345";

/// A one-range node with auth that **already leads** range 0: `start` does not wait
/// for the claim, and a split command sent right after needs a leader (without the
/// wait a busy machine answers `503` "not leading range 0").
async fn authed_node() -> Result<cairn_server::node::RunningNode> {
    let mut cfg = test_config(NodeMode::single(1));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    common::start_led(cfg).await
}

async fn call(req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
    let r = req.send().await?;
    let code = r.status();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// The endpoint enables a point on a real node, that point makes a split fail **and
/// roll back cleanly** (intent dropped, a later split succeeds); with the point removed
/// the split goes through. Unknown names, broken syntax and `pause` are rejected; only
/// `cluster-admin` may call it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_failpoints_drive_a_real_split_failure() -> Result<()> {
    let _ctl = StepController::new();
    let node = authed_node().await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "failpoint via /admin/failpoints", async {
        let http = Client::new();
        let fp = format!("{url}/admin/failpoints");
        let set = |name: &str, action: &str| {
            http.post(&fp)
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "name": name, "action": action }))
        };

        // Only cluster-admin.
        let (code, _) = call(http.get(&fp)).await?;
        ensure!(code == StatusCode::UNAUTHORIZED, "no key: {code}");
        let (code, key) = call(
            http.post(format!("{url}/admin/keys"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "tenant": "acme", "role": "admin" })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "create key: {code} {key}");
        let tenant_admin = key["secret"].as_str().unwrap_or_default().to_string();
        let (code, _) = call(http.get(&fp).bearer_auth(&tenant_admin)).await?;
        ensure!(
            code == StatusCode::FORBIDDEN,
            "a tenant's admin key: {code}"
        );

        // Rejected: unknown name, broken syntax, `pause`.
        for (name, action) in [
            ("split::nonexistent", "return"),
            ("split::cloneLeft", "not-an-action"),
            ("split::cloneLeft", "1*pause"),
        ] {
            let (code, body) = call(set(name, action)).await?;
            ensure!(
                code == StatusCode::BAD_REQUEST,
                "{name}={action}: {code} {body}"
            );
        }

        // Enable: the split fails in the window after cloning the left child, and rolls back.
        let (code, body) = call(set("split::cloneLeft", "return(chaos)")).await?;
        ensure!(code == StatusCode::OK, "enable: {code} {body}");
        let (_, list) = call(http.get(&fp).bearer_auth(ADMIN_KEY)).await?;
        ensure!(
            list["active"] == json!([{ "name": "split::cloneLeft", "action": "return(chaos)" }]),
            "{list}"
        );
        ensure!(
            list["names"]
                .as_array()
                .is_some_and(|n| n.contains(&json!("gc::between_check_and_delete"))),
            "{list}"
        );
        let split = || {
            http.post(format!("{url}/ranges/0/split"))
                .bearer_auth(ADMIN_KEY)
        };
        let (code, body) = call(split()).await?;
        ensure!(
            !code.is_success(),
            "the split must fail while the failpoint is on: {code} {body}"
        );
        // The `500` body only carries a log lookup code; the details (the failpoint
        // name) are in the log.
        ensure!(
            code == StatusCode::INTERNAL_SERVER_ERROR
                && body["error"].as_str().is_some_and(|e| e.contains("code")),
            "{code} {body}"
        );
        ensure!(
            node.cluster.partition_count() == 1,
            "the split failed yet the table changed"
        );
        ensure!(
            node.cluster
                .range(cairn_store::PartitionId(0))
                .is_some_and(|r| r.intent.is_none()),
            "the intent was not rolled back after the failed step"
        );

        // Remove: the split goes through.
        let (code, body) = call(set("split::cloneLeft", "off")).await?;
        ensure!(code == StatusCode::OK, "remove: {code} {body}");
        let (_, list) = call(http.get(&fp).bearer_auth(ADMIN_KEY)).await?;
        ensure!(list["active"] == json!([]), "{list}");
        // The rollback step reopens the range for the quiesced engine (`resign`, then
        // claim again): for a short while the range has no leader ⇒ 503, retry.
        let (code, body) = loop {
            let (code, body) = call(split()).await?;
            if code != StatusCode::SERVICE_UNAVAILABLE {
                break (code, body);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        ensure!(code == StatusCode::OK, "split after removal: {code} {body}");
        ensure!(node.cluster.partition_count() == 2, "{body}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────── range-level migration framework

static RANGE_STEP_RUNS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn range_step(
    _: u32,
    w: &mut cairn_store::PartitionWriter,
) -> futures::future::LocalBoxFuture<'_, cairn_store::Result<()>> {
    Box::pin(async move {
        let _ = w.get(b"\xfftest-r1").await?;
        RANGE_STEP_RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
}

const RANGE_LEDGER: &[cairn_store::migrate::Migration] = &[cairn_store::migrate::Migration {
    to: 1,
    name: "r1",
    run: cairn_store::migrate::Run::Range(range_step),
}];

/// **Dying mid-way through a range-level step, then running again.** The step has run
/// and is durable, but the `data=` mark was not written yet (failpoint
/// `migrate::range::before_mark`): the range does not open, the owner record carries no
/// `data`. The next open runs the step **again** (idempotent — the counter sees one more
/// run), and only then writes `data=1`.
///
/// 1. Node A (a one-node cluster — `start` does not wait for the claim, so a failed open
///    does not fail `start`) with the failpoint always firing.
/// 2. Wait for the step to run at least once; the leadership loop retries and fails
///    again at the same place. Shut A down, remove the failpoint.
/// 3. Node B on the same bucket, wait until it leads the range.
///
/// Must see: after A, the owner record carries no `data=`; B runs the step exactly once
/// more and the owner record carries `data=1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_step_reruns_after_death_before_data_mark() -> Result<()> {
    use {
        slatedb::object_store::{path::Path, ObjectStoreExt},
        std::sync::atomic::Ordering::SeqCst,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let cfg = || {
        let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
            bucket.clone(),
            "",
            "memory://",
        ));
        let mut cfg = cairn_server::node::NodeConfig::new(stores, NodeMode::single(1));
        cfg.flush_interval = Some(std::time::Duration::from_millis(5));
        cfg.migrations = RANGE_LEDGER;
        cfg
    };
    let owner = || {
        let bucket = bucket.clone();
        async move {
            match bucket.get(&Path::from("owners/range-0")).await {
                Ok(g) => Some(String::from_utf8(g.bytes().await.unwrap().to_vec()).unwrap()),
                Err(_) => None,
            }
        }
    };
    {
        let _die = ctl.fail("migrate::range::before_mark", "return(die)");
        let a = node::start(cfg()).await.map_err(|e| anyhow!("{e}"))?;
        let t0 = std::time::Instant::now();
        while RANGE_STEP_RUNS.load(SeqCst) == 0 {
            ensure!(
                t0.elapsed() < std::time::Duration::from_secs(20),
                "the range-level step did not run"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        a.shutdown().await;
    }
    ensure!(
        owner().await.is_none_or(|t| !t.contains("data=")),
        "data= written before the step finished"
    );
    let before = RANGE_STEP_RUNS.load(SeqCst);

    let node = common::start_led(cfg()).await?;
    let r = async {
        ensure!(
            RANGE_STEP_RUNS.load(SeqCst) == before + 1,
            "the step must run again exactly once more ({} → {})",
            before,
            RANGE_STEP_RUNS.load(SeqCst)
        );
        let text = owner()
            .await
            .ok_or_else(|| anyhow!("owner record missing"))?;
        ensure!(text.lines().any(|l| l == "data=1"), "{text}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────── upgrade gate

/// **The window between the intent and the upgrade lock.** A split that has written its
/// intent is stopped at `split::intentCas`; in exactly that window a migration runner
/// writes `upgrading`. The split, moving on, must see the lock, **roll back** the intent
/// and return `503` — no split runs on top of a migration. Symmetric to the runner's
/// side: when the intent is written before the lock, the split checks after the intent
/// and sees the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_backs_off_when_upgrade_starts_after_its_intent() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let ctl = StepController::new();
    let bucket = common::bucket();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    ));
    let mut cfg = cairn_server::node::NodeConfig::new(
        stores,
        NodeMode {
            partitions: 1,
            lease_ttl: std::time::Duration::from_secs(1),
            node_id: Some("node-a".into()),
        },
    );
    cfg.flush_interval = Some(std::time::Duration::from_millis(5));
    cfg.node_wal.window = std::time::Duration::from_millis(5);
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "split hits the upgrade lock", async {
        let paused = ctl.pause_at("split::intentCas");
        let split = tokio::spawn({
            let url = url.clone();
            async move { call(Client::new().post(format!("{url}/ranges/0/split"))).await }
        });
        paused
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        bucket
            .put(
                &Path::from("cluster-version"),
                PutPayload::from(
                    "cairnflow-cluster-version/1\nversion=0\nupgrading=0->1@ghost/1\n",
                ),
            )
            .await?;
        paused.resume();
        let (code, body) = split.await??;
        ensure!(
            code == StatusCode::SERVICE_UNAVAILABLE,
            "a split ran on top of a migration: {code} {body}"
        );
        // The intent rolled back: a one-range table, no intent.
        let table = node.cluster.table();
        ensure!(
            table.partition_count() == 1 && table.ranges().iter().all(|r| r.intent.is_none()),
            "{table:?}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────── three proof tests

async fn post_json(url: String) -> Result<(StatusCode, Value)> {
    call(Client::new().post(url)).await
}

/// **(1) — a cross-partition message arriving exactly while the target range is
/// quiesced.** Two ranges on one node. Split range 1 and stop it at `split::quiesce`:
/// the parent Db is closed, the two children not cloned yet, the table still has the
/// intent. An invocation on range 0 calls a key of range 1 — its outbox row goes through
/// `/internal/inbound` to a range that can no longer accept it. Release the split: the
/// message must reach the right child, **exactly once** — one callee invocation, one
/// result back to the caller.
///
/// `FaultyStore` cannot reach this window: no object write happens between the quiesce
/// and the clone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_during_split_quiesce_is_neither_lost_nor_duplicated() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    let ctl = StepController::new();
    let node = common::start_led(common::solo_config(2)).await?;
    let url = node.url();
    let callee_key = common::key_on_partition(&node, "Callee", 1, "callee");
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some(&callee_key)),
                Step::EchoCallResult,
            ],
        )
        .object("Callee", "echo", vec![Step::output("pong")])
        .spawn()
        .await?;

    let r = within(
        Duration::from_secs(60),
        "inbound between quiesce and clone",
        async {
            common::register(&url, &sdk.url).await?;
            let sends = Arc::new(AtomicU64::new(0));
            let counted = Arc::clone(&sends);
            let _count =
                cairn_store::testkit::FailGuard::callback("transport::send_once", move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                });

            let paused = ctl.pause_at("split::quiesce");
            let split_url = format!("{url}/ranges/1/split");
            let splitting = tokio::spawn(post_json(split_url));
            paused.expect_stopped(Duration::from_secs(10)).await;

            // Range 1 is quiesced. The caller on range 0 sends a call to it.
            let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let caller = body["id"].as_str().unwrap_or_default().to_string();
            // Wait for at least two sends: the first is rejected by the quiesced range,
            // the retry still falls into the window — the message really went through it.
            let t0 = Instant::now();
            while sends.load(Ordering::SeqCst) < 2 {
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "the outbox sent nothing to the quiescing range"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            ensure!(
                sdk.starts_of("Callee", "echo").is_empty(),
                "the callee ran while its range was quiescing"
            );

            paused.resume();
            let (code, view) = splitting.await??;
            ensure!(code == StatusCode::OK, "split: {code} {view}");
            ensure!(node.cluster.partition_count() == 3, "{view}");

            let st = loop {
                let (code, st) = common::invocation(&url, &caller).await?;
                if code == 200 && st["status"] == "completed" {
                    break st;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(40),
                    "caller not finished: {code} {st}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            ensure!(st["output"] == "pong", "{st}");
            // Give a duplicate message (if any) time to arrive and run.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let callees: std::collections::BTreeSet<String> = sdk
                .starts_of("Callee", "echo")
                .iter()
                .map(|s| s.id_hex())
                .collect();
            ensure!(
                callees.len() == 1,
                "the callee must be exactly one invocation, got {}: {callees:?}",
                callees.len()
            );
            let callee = callees.into_iter().next().unwrap_or_default();
            let p = common::partition_of(&url, &callee).await?;
            ensure!(
                p != 0 && p != 1,
                "the callee must be in a child range, not in range {p}"
            );
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// **The D-d plan relies on exactly one table snapshot** (a finding of the M2 model).
///
/// Interleaving:
/// 1. A lone node, one range 0 with data.
/// 2. `gc::plan` (`min_age` 0) loads the table — range 0 alive — then stops at
///    `gc::plan::after_snapshot`.
/// 3. Split range 0: commit, 0's tombstone written; the node's in-RAM table changes.
/// 4. Release the plan.
///
/// Must see: 0 is **not** in `eligible` — two children stand on its Db. Before the fix
/// the plan filtered candidates by the in-RAM table (0 had left the table) but read
/// references from the old snapshot (only 0, no children yet): 0 was "eligible", and
/// only the version check of `run_once` stopped the delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_plan_uses_one_table_snapshot() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = within(
        Duration::from_secs(60),
        "the D-d plan across one split",
        async {
            let paused = ctl.pause_at("gc::plan::after_snapshot");
            let (s, c) = (stores.clone(), node.cluster.clone());
            let planning = tokio::spawn(async move {
                cairn_server::gc::plan(&s, &c, Duration::ZERO, common::wall_ms()).await
            });
            paused.expect_stopped(TEST_DEADLINE).await;
            let (table, _) = node
                .ranges
                .split(cairn_store::PartitionId(0), None)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            ensure!(!table.contains(cairn_store::PartitionId(0)), "{table:?}");
            paused.resume();
            let plan = planning.await?.map_err(|e| anyhow!("{e}"))?;
            ensure!(
                plan.eligible.iter().all(|e| e.range != 0),
                "range 0 just retired with children still standing on it is eligible again: {plan:?}"
            );
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// **(2) — GC cutting in between check and delete while a split commits** (the safety
/// net of the re-check before delete). One split range (parent 0 retired, still
/// referenced by two children) and one retired range referenced by nobody (100). The
/// D-d round stops at `gc::between_check_and_delete` with 100; in that window the left
/// child splits again. Release the round: the table changed after the check ⇒ 100's
/// proof is rebuilt on the **new** snapshot (formerly the round skipped its turn and
/// deleted nothing): this very round deletes 100, and no Db still referenced (parent,
/// child, grandchild) is touched; acked invocations are still readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_round_reproves_when_split_commits_between_check_and_delete() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::time::Duration;

    let ctl = StepController::new();
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;

    let r = within(Duration::from_secs(60), "GC between check and delete", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for i in 0..6 {
            let (code, body) =
                common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
            ensure!(code == 200, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        let left = view["ranges"][0]["range"]
            .as_u64()
            .ok_or_else(|| anyhow!("{view}"))? as u32;
        common::fabricate_retired_range(
            &stores,
            100,
            Some(common::wall_ms() - common::LONG_RETIRED_MS),
        )
        .await?;

        let paused = ctl.pause_at("gc::between_check_and_delete");
        let (s, c) = (stores.clone(), node.cluster.clone());
        let round =
            tokio::spawn(async move { cairn_server::node::gc_retired_dbs_once(&s, &c).await });
        paused.expect_stopped(Duration::from_secs(10)).await;

        // In the window: the left child splits again — the table changes version.
        let (code, view) = loop {
            let (code, view) = post_json(format!("{url}/ranges/{left}/split")).await?;
            if code != StatusCode::SERVICE_UNAVAILABLE {
                break (code, view);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        ensure!(code == StatusCode::OK, "split of the left child: {code} {view}");
        paused.resume();
        let deleted = round.await?.map_err(|e| anyhow!("{e}"))?;
        ensure!(
            deleted == 1,
            "the table changed between check and delete: the round must rebuild the proof on the new snapshot and delete exactly range 100, deleted {deleted}"
        );
        ensure!(
            !stores
                .range_db_exists(cairn_store::PartitionId(100))
                .await?
        );
        let table = node.cluster.table();
        let mut keep: Vec<u32> = table.partitions().map(|p| p.0).collect();
        keep.extend([0, left]);
        for r in keep {
            ensure!(
                stores.range_db_exists(cairn_store::PartitionId(r)).await?,
                "Db of range {r} disappeared"
            );
        }
        for id in &ids {
            let (code, st) = common::invocation(&url, id).await?;
            ensure!(
                code == 200 && st["status"] == "completed",
                "{id}: {code} {st}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **D-d is not starved when the table changes every round** (seen in a 4 h soak:
/// auto split changed the table every ~31 s on average, and D-d dropped its whole turn
/// each time).
///
/// Interleaving, repeated for at most three rounds:
/// 1. One split range (parent 0 retired, two children standing on its Db) and two
///    retired ranges referenced by nobody (100, 101).
/// 2. The D-d round (`min_age` 0) plans — 100, 101 eligible; 0 blocked — then stops at
///    `gc::between_check_and_delete` before the first candidate.
/// 3. In the window a live range splits: the table changes version.
/// 4. Release the round.
///
/// Must see: after the rounds, Dbs 100 and 101 are deleted (the proof rebuilt on the
/// **new** table snapshot); the Dbs of every range in the table and of the ancestor 0
/// (children still stand on it) are intact, `verify` is clean (no Db deleted under the
/// feet of a live range — a just-retired parent that no child lists may be deleted);
/// acked invocations are still readable. Before the fix every round dropped its turn
/// at the first candidate — three rounds, nothing deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_keeps_reaping_when_the_table_moves_every_round() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::time::Duration;

    let ctl = StepController::new();
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;

    let r = within(Duration::from_secs(120), "D-d under a table change every round", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for i in 0..6 {
            let (code, body) =
                common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
            ensure!(code == 200, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        for r in [100, 101] {
            common::fabricate_retired_range(
                &stores,
                r,
                Some(common::wall_ms() - common::LONG_RETIRED_MS),
            )
            .await?;
        }
        let gc = cairn_server::gc::GcConfig {
            min_age: Duration::ZERO,
            ..cairn_server::gc::GcConfig::default()
        };

        let mut deleted = Vec::new();
        let mut skipped = Vec::new();
        for _ in 0..3 {
            let paused = ctl.pause_at("gc::between_check_and_delete");
            let (s, c, g) = (stores.clone(), node.cluster.clone(), gc.clone());
            let round = tokio::spawn(async move { cairn_server::gc::run_once(&s, &c, &g).await });
            paused.expect_stopped(Duration::from_secs(10)).await;

            // In the window: a live range splits — the table changes version.
            let victim = node
                .cluster
                .table()
                .partitions()
                .next()
                .ok_or_else(|| anyhow!("empty table"))?
                .0;
            let (code, view) = loop {
                let (code, view) = post_json(format!("{url}/ranges/{victim}/split")).await?;
                if code != StatusCode::SERVICE_UNAVAILABLE {
                    break (code, view);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            ensure!(code == StatusCode::OK, "split {victim}: {code} {view}");
            paused.resume();

            let round = round.await?.map_err(|e| anyhow!("{e}"))?;
            deleted.extend(round.deleted.iter().copied());
            skipped.extend(round.skipped);
            if deleted.contains(&100) && deleted.contains(&101) {
                break;
            }
        }
        ensure!(
            deleted.contains(&100) && deleted.contains(&101),
            "the table changed every round and D-d could not delete two Dbs nobody references: deleted {deleted:?}, skipped {skipped:?}"
        );
        let table = node.cluster.table();
        let mut keep: Vec<u32> = table.partitions().map(|p| p.0).collect();
        keep.push(0);
        for r in keep {
            ensure!(
                stores.range_db_exists(cairn_store::PartitionId(r)).await?,
                "Db of range {r} (still referenced) disappeared"
            );
        }
        let rep = cairn_server::ctl::Ctl::new(stores.clone())
            .verify(cairn_server::ctl::VerifyOptions {
                open_dbs: false,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(rep.clean(), "{:?}", rep.findings);
        for id in &ids {
            let (code, st) = common::invocation(&url, id).await?;
            ensure!(
                code == 200 && st["status"] == "completed",
                "{id}: {code} {st}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A barrier set between check and delete is honoured.** The D-d round stops at
/// `gc::between_check_and_delete` with an eligible range; in the window, a barrier
/// covering that range is set. Release: the Db is intact, the round does not drop its
/// turn (the table did not change) but only skips the covered range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_honors_barrier_set_between_check_and_delete() -> Result<()> {
    use std::time::Duration;

    let ctl = StepController::new();
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = within(Duration::from_secs(30), "barrier mid check/delete", async {
        common::fabricate_retired_range(
            &stores,
            100,
            Some(common::wall_ms() - common::LONG_RETIRED_MS),
        )
        .await?;
        let paused = ctl.pause_at("gc::between_check_and_delete");
        let (s, c) = (stores.clone(), node.cluster.clone());
        let gc = cairn_server::gc::GcConfig::default();
        let round = tokio::spawn(async move { cairn_server::gc::run_once(&s, &c, &gc).await });
        paused.expect_stopped(Duration::from_secs(10)).await;
        let (code, body) = call(
            Client::new()
                .put(format!("{url}/admin/gc/barriers/hold"))
                .json(&json!({ "holder": "test", "protects": [100], "ttlMs": 60_000 })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "{code} {body}");
        paused.resume();
        let round = round.await?.map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.deleted.is_empty() && round.skipped.is_none(),
            "{round:?}"
        );
        ensure!(
            stores
                .range_db_exists(cairn_store::PartitionId(100))
                .await?,
            "a barrier set in the window was ignored"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────── deployment registry at startup

fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let dst = to.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &dst)?;
        } else {
            std::fs::copy(e.path(), dst)?;
        }
    }
    Ok(())
}

/// **`node::start` returns once the deployment registry is loaded.** The registry was
/// loaded only by the background refresh loop; if that loop is slow (here: a failpoint
/// sleeping 3 s before each tick) the node accepts requests while the registry is still
/// empty — `/deployments` returns `[]`, and invocations cannot find their deployment.
/// Order: the format 10 fixture bucket has a deployment of `acme` → the background loop
/// is held → `start` returns → ask at once → the deployment must be there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_is_loaded_before_start_returns() -> Result<()> {
    let _ctl = StepController::new();
    let _slow =
        cairn_store::testkit::FailGuard::new("node::refresh_loop::before_tick", "sleep(3000)");
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format10");
    let tmp = std::env::temp_dir().join(format!("cairnflow-fp-registry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    copy_dir(&src, &tmp)?;
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::parse(&format!(
        "file://{}",
        tmp.display()
    ))?);
    let mut cfg = cairn_server::node::NodeConfig::new(stores, NodeMode::single(2));
    let key = "cf_fixture_bootstrap_key_do_not_use_0001";
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(key.into()),
        cluster_secret: Some("cf_fixture_cluster_secret_at_least_32b".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.retention_ms = None;
    let node = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let http = Client::new();
        let resp = http
            .get(format!("{}/deployments?tenant=acme", node.url()))
            .bearer_auth(key)
            .send()
            .await?;
        ensure!(
            resp.status() == StatusCode::OK,
            "deployments: {}",
            resp.status()
        );
        let deps: Value = resp.json().await?;
        ensure!(
            deps.as_array().is_some_and(|a| !a.is_empty()),
            "the node serves while the deployment registry is still empty: {deps}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    let _ = std::fs::remove_dir_all(&tmp);
    r
}

/// **Chaos fault injection: a deliberately broken GC is caught by `verify`.** The chaos
/// invariant "GC does not delete a Db still referenced" uses `cairn-ctl verify` as the
/// referee; this test proves the referee sees the fault when the fault is real.
///
/// 1. Write a few invocations, then split range 0: the parent retires, the two children
///    list the parent's Db in `external_dbs`.
/// 2. A D-d round with `min_age` 0, no failpoint: the parent is blocked by `reference`,
///    nothing deleted; `verify` is clean.
/// 3. Enable `gc::skip_reference_check` **and** `gc::skip_checkpoint_check`: the plan
///    drops both the reference gate and the named checkpoint barrier (the first gate
///    alone is held by the second barrier, see the next test), and the round deletes
///    the parent's Db.
///
/// Must see: at step 3, `verify` reports `external_db` for both children.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_catches_gc_that_skips_reference_check() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::time::Duration;

    let ctl = StepController::new();
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "GC skipping references", async {
        // The parent must have SSTs for the children to reference it (an empty parent
        // leaves nothing to the children, and deleting it is correct).
        common::register(&url, &sdk.url).await?;
        for i in 0..6 {
            let (code, body) =
                common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
            ensure!(code == 200, "{code} {body}");
        }
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        let gc = cairn_server::gc::GcConfig {
            min_age: Duration::ZERO,
            ..cairn_server::gc::GcConfig::default()
        };
        let verify = || async {
            cairn_server::ctl::Ctl::new(stores.clone())
                .verify(cairn_server::ctl::VerifyOptions {
                    open_dbs: false,
                    ..Default::default()
                })
                .await
                .map_err(|e| anyhow!("{e}"))
        };

        let round = cairn_server::gc::run_once(&stores, &node.cluster, &gc)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.deleted.is_empty(),
            "D-d deleted a Db still referenced: {:?}",
            round.deleted
        );
        ensure!(
            round
                .plan
                .blocked
                .iter()
                .any(|b| b.range == 0 && b.by == cairn_server::gc::BlockedBy::Reference),
            "the parent must be blocked by references: {:?}",
            round.plan.blocked
        );
        let rep = verify().await?;
        ensure!(rep.clean(), "{:?}", rep.findings);

        let _g = ctl.fail("gc::skip_reference_check", "return");
        let _c = ctl.fail("gc::skip_checkpoint_check", "return");
        let round = cairn_server::gc::run_once(&stores, &node.cluster, &gc)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.deleted == vec![0],
            "the broken GC must delete the parent's Db: {:?}",
            round.deleted
        );
        let rep = verify().await?;
        let hit: Vec<_> = rep
            .findings
            .iter()
            .filter(|f| f.check == "external_db")
            .collect();
        ensure!(
            hit.len() == 2,
            "verify must report external_db for both children: {:?}",
            rep.findings
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The second D-d barrier**: when the reference set scan is blinded, the retired Db's
/// **own** manifest still says who stands on it.
///
/// 1. Write a few invocations, then split range 0: the parent retires; the two children
///    list the parent's Db in `external_dbs`, and each clone puts a **named** final
///    checkpoint on the parent (the child Db's path, a patch of the SlateDB fork) for
///    each child.
/// 2. Enable `gc::skip_reference_check` — the scan says "no live range lists the
///    parent" (as if the table or `external_dbs` were read short); a D-d round with
///    `min_age` 0.
///
/// Must see: the parent is **not** deleted (the Db still has its manifest) — the plan
/// blocks it because of the checkpoint, the proof counts one disagreement (the
/// `gc::retired_db::proofs_disagree` tripwire does not enable `panic` here: the
/// disagreement is exactly what the test injects); `verify` is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dd_keeps_a_parent_its_children_name_when_the_reference_scan_is_blind() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::time::Duration;

    let ctl = StepController::new();
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "D-d with a blind scan", async {
        common::register(&url, &sdk.url).await?;
        for i in 0..6 {
            let (code, body) =
                common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
            ensure!(code == 200, "{code} {body}");
        }
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        let gc = cairn_server::gc::GcConfig {
            min_age: Duration::ZERO,
            ..cairn_server::gc::GcConfig::default()
        };
        let _g = ctl.fail("gc::skip_reference_check", "return");
        let round = cairn_server::gc::run_once(&stores, &node.cluster, &gc)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.deleted.is_empty(),
            "a blind scan and D-d still deleted the parent's Db under both children: {:?}",
            round.deleted
        );
        ensure!(
            round
                .plan
                .blocked
                .iter()
                .any(|b| b.range == 0 && b.by == cairn_server::gc::BlockedBy::Checkpoint),
            "the parent must be blocked by its named checkpoints: {:?}",
            round.plan.blocked
        );
        // The proof step (the plan already blocked it, so call it directly): disagree, keep, count.
        let before =
            cairn_store::proof::stats::PROOFS_DISAGREE.load(std::sync::atomic::Ordering::Relaxed);
        let proven = cairn_store::proof::DeleteProof::retired_db(
            &stores,
            &node.cluster,
            cairn_store::PartitionId(0),
            cairn_server::gc::wall_ms(),
        )
        .await?;
        ensure!(proven.is_err(), "the proof must refuse: {proven:?}");
        ensure!(
            cairn_store::proof::stats::PROOFS_DISAGREE.load(std::sync::atomic::Ordering::Relaxed)
                > before,
            "a scan/checkpoint disagreement must be counted"
        );
        ensure!(
            stores.range_db_exists(cairn_store::PartitionId(0)).await?,
            "the parent's Db must still be there"
        );
        let rep = cairn_server::ctl::Ctl::new(stores.clone())
            .verify(cairn_server::ctl::VerifyOptions {
                open_dbs: false,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(rep.clean(), "{:?}", rep.findings);
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────── SM-12: a merge rollback keeps the sources' checkpoints

/// POSTs repeatedly until there is no more `503` (a range that just changed leader after
/// a split/rollback — briefly led by nobody), within a 20 s cap.
async fn post_until_settled(url: String) -> Result<(StatusCode, Value)> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let (code, body) = post_json(url.clone()).await?;
        if code != StatusCode::SERVICE_UNAVAILABLE || std::time::Instant::now() > deadline {
            return Ok((code, body));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// **SM-12 — rolling back a merge intent does not delete checkpoints two live sources
/// stand on** (seen in a 20-minute chaos run: after a rollback of merge 40+41, `verify`
/// reported `external_db@40` and every later merge 40+41 returned
/// `500 checkpoint missing`).
///
/// Interleaving, following the chaos lineage (15 → 38 → 40, 41):
/// 1. One node, one range. A few invocations write rows into range 0; split 0 → `l`, `r`.
/// 2. Split `l` again → `a`, `b`. No invocation after that: `a` and `b` are alive but
///    own no SST in their pk range.
/// 3. Failpoint `merge::ownerClaim` = `return`, **only on this node**: merging
///    `a`+`b` goes through the intent, quiesce and union (the merged Db has a manifest
///    and puts final checkpoints on the ancestors), then fails right before the owner
///    claim — the shape of "the coordinator died after the union".
/// 4. The coordinator rolls back: a CAS drops the intent, `discard_range_db` discards
///    the orphaned merged Db. A callback at `split::roll_back` reports into a channel
///    when the rollback starts.
///
/// Must see: after the rollback, `verify` has no `external_db` (every checkpoint `a`
/// and `b` stand on is still there); with the failpoint removed merging `a`+`b`
/// succeeds; the D-d round deletes no Db the merged range still reads, and every acked
/// invocation is still readable. The `split::discard::referenced_checkpoint` tripwire
/// is set to `panic` for the whole test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_roll_back_keeps_checkpoints_the_sources_stand_on() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::time::Duration;

    let ctl = StepController::new();
    // SM-12 tripwire: a rollback about to delete a still-referenced checkpoint ⇒ panic.
    let _tripwire = ctl.fail("split::discard::referenced_checkpoint", "panic");
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(
        Duration::from_secs(90),
        "merge rollback keeps the sources' checkpoints",
        async {
            common::register(&url, &sdk.url).await?;
            let mut ids = Vec::new();
            for i in 0..16 {
                let (code, body) =
                    common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
                ensure!(code == 200, "{code} {body}");
                ids.push(body["id"].as_str().unwrap_or_default().to_string());
            }
            let children = |view: &Value| -> Result<(u32, u32)> {
                let r: Vec<u32> = view["report"]["children"]
                    .as_array()
                    .ok_or_else(|| anyhow!("{view}"))?
                    .iter()
                    .map(|r| r["range"].as_u64().unwrap_or(0) as u32)
                    .collect();
                ensure!(r.len() == 2, "{view}");
                Ok((r[0], r[1]))
            };
            let (code, view) = post_until_settled(format!("{url}/ranges/0/split")).await?;
            ensure!(code == StatusCode::OK, "split 0: {code} {view}");
            let (l, _) = children(&view)?;
            let (code, view) = post_until_settled(format!("{url}/ranges/{l}/split")).await?;
            ensure!(code == StatusCode::OK, "split {l}: {code} {view}");
            let (a, b) = children(&view)?;
            let verify = || async {
                cairn_server::ctl::Ctl::new(stores.clone())
                    .verify(cairn_server::ctl::VerifyOptions {
                        open_dbs: false,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| anyhow!("{e}"))
            };
            let rep = verify().await?;
            ensure!(rep.clean(), "before the merge: {:?}", rep.findings);

            // The merge fails after the union, and rolls back.
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let _rolled =
                cairn_store::testkit::FailGuard::callback("split::roll_back", move || {
                    let _ = tx.send(());
                });
            let fail = ctl.fail_on(&[&node.node_id], "merge::ownerClaim", "return(after-union)");
            let merge = format!("{url}/ranges/merge?left={a}&right={b}");
            let (code, body) = post_until_settled(merge.clone()).await?;
            ensure!(
                !code.is_success(),
                "the merge must fail while the failpoint is on: {code} {body}"
            );
            // The `500` body only carries a log lookup code; that the failure is right
            // after the union is proven by the rollback callback below.
            ensure!(
                code == StatusCode::INTERNAL_SERVER_ERROR
                    && body["error"].as_str().is_some_and(|e| e.contains("code")),
                "the merge failed some other way: {code} {body}"
            );
            tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .map_err(|_| anyhow!("the coordinator did not roll back the merge intent"))?;
            drop(fail);
            ensure!(
                node.cluster
                    .table()
                    .ranges()
                    .iter()
                    .all(|r| r.intent.is_none()),
                "the merge intent was not rolled back yet: {:?}",
                node.cluster.table()
            );

            let rep = verify().await?;
            let lost: Vec<_> = rep
                .findings
                .iter()
                .filter(|f| f.check == "external_db")
                .collect();
            ensure!(
                lost.is_empty(),
                "the merge rollback deleted a checkpoint a live range stands on: {lost:?}"
            );

            // The next merge must succeed.
            let (code, view) = post_until_settled(merge).await?;
            ensure!(code == StatusCode::OK, "merge after undo: {code} {view}");

            // D-d deletes no Db the merged range still reads; every acked invocation is still readable.
            let gc = cairn_server::gc::GcConfig {
                min_age: Duration::ZERO,
                ..cairn_server::gc::GcConfig::default()
            };
            cairn_server::gc::run_once(&stores, &node.cluster, &gc)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let rep = verify().await?;
            let bad: Vec<_> = rep
                .findings
                .iter()
                .filter(|f| f.check != "orphan_db")
                .collect();
            ensure!(bad.is_empty(), "after merge and D-d: {bad:?}");
            for id in &ids {
                let (code, st) = common::invocation(&url, id).await?;
                ensure!(
                    code == 200 && st["status"] == "completed",
                    "{id}: {code} {st}"
                );
            }
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────── SM-11: proxy tombstones

/// The owner record of range `r` on the shared bucket (empty prefix).
async fn owner_of(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    r: u32,
) -> Result<Option<cairn_store::OwnerRecord>> {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    Ok(cairn_store::read_owner(
        &store,
        &slatedb::object_store::path::Path::default(),
        cairn_store::PartitionId(r),
    )
    .await?)
}

/// Builds cluster node `id` on `bucket`, with the proxy tombstone round every 200 ms,
/// then waits until it leads every range (it is the only node still alive).
async fn start_alone(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    id: &str,
) -> Result<common::ClusterNode> {
    use std::time::Duration;
    let n = common::start_cluster_node_tweaked(bucket, id, 2, Duration::from_secs(1), |cfg| {
        cfg.retire_orphans_every = Some(Duration::from_millis(200));
        cfg.reconcile_intents_every = Some(Duration::from_millis(200));
    })
    .await?;
    let want = n.node.cluster.table().partition_count() as usize;
    common::wait_probe(
        &[&n],
        Duration::from_secs(20),
        "one node leads every range",
        |p| p.len() == want && p.values().all(|v| v == &[0]),
    )
    .await?;
    Ok(n)
}

/// **SM-11 — a parent range left the table and its leader died before writing the
/// tombstone** (seen in a 20-minute `--profile all` chaos run: `verify` reported
/// `orphan_db`).
///
/// Interleaving:
/// 1. Node A alone, two ranges. A leads range 0.
/// 2. Failpoint `session::retire::before_write` = `return`, **only on A**: A's
///    tombstone write bails out without writing anything — the shape of "A died right
///    after the table commit".
/// 3. A splits range 0: intent, quiesce, clone, child claims, **commit**; the
///    `follow_table` loop (or the coordinator itself) retires 0, and the tombstone
///    write stops at the point above. `split` returning is the "past the window" event.
///    The owner record of 0: A's session, not released, no tombstone.
/// 4. Node B joins the cluster, then A dies like `kill -9` (drop: renewal stops,
///    nothing more written).
/// 5. After a TTL of silence, B marks A's session expired, takes every live range and
///    becomes the D-d / proxy tombstone runner.
///
/// Must see: within the time cap, the owner record of 0 carries a tombstone
/// (`retired`, `released`, `retired_ms`) while the other fields stay unchanged (A's
/// session, term), except the WAL index (empty — a tombstone must not keep a dead
/// owner's segment); the two children are still in the table and carry no tombstone.
/// Before the fix nobody wrote the tombstone: range 0 was out of the table so nobody
/// took it over again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_left_without_tombstone_is_retired_after_its_leader_dies() -> Result<()> {
    use std::time::Duration;

    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = start_alone(&bucket, "orphan-a").await?;
    let a_session = a
        .node
        .session
        .current()
        .ok_or_else(|| anyhow!("A has no session"))?;
    let _no_tomb = ctl.fail_on(&["orphan-a"], "session::retire::before_write", "return");

    let (table, _) = within(TEST_DEADLINE, "A splits range 0", async {
        a.node
            .ranges
            .split(cairn_store::PartitionId(0), None)
            .await
            .map_err(|e| anyhow!("{e}"))
    })
    .await?;
    ensure!(
        !table.contains(cairn_store::PartitionId(0)),
        "range 0 still in the table: {table:?}"
    );
    let children: Vec<u32> = table
        .partitions()
        .map(|p| p.0)
        .filter(|p| *p != 1)
        .collect();
    ensure!(children.len() == 2, "{table:?}");
    let before = owner_of(&bucket, 0)
        .await?
        .ok_or_else(|| anyhow!("range 0 has no owner record"))?;
    ensure!(
        before.session == a_session && !before.released && !before.retired,
        "the failpoint did not hold back the tombstone: {before:?}"
    );

    let b = start_cluster_node_with_retirer(&bucket, "orphan-b").await?;
    drop(a); // kill -9

    let r = within(
        Duration::from_secs(40),
        "range 0's tombstone after A died",
        async {
            let since = common::wall_ms();
            loop {
                let now = owner_of(&bucket, 0)
                    .await?
                    .ok_or_else(|| anyhow!("0's owner record disappeared"))?;
                if now.retired {
                    ensure!(now.released, "the tombstone must carry released: {now:?}");
                    ensure!(
                        now.retired_ms
                            .is_some_and(|ms| ms >= since.saturating_sub(1_000)),
                        "the tombstone lacks retired_ms at write time: {now:?}"
                    );
                    ensure!(
                    now.session == before.session
                        && now.term == before.term
                        && now.wal_kind == before.wal_kind
                        && now.wal_segments.is_empty(),
                    "the tombstone keeps the record's other fields and drops the WAL segment: before {before:?}, after {now:?}"
                );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            for c in &children {
                let rec = owner_of(&bucket, *c)
                    .await?
                    .ok_or_else(|| anyhow!("child {c} has no owner record"))?;
                ensure!(!rec.retired, "child {c} in the table carries a tombstone: {rec:?}");
                ensure!(
                    b.node.cluster.contains(cairn_store::PartitionId(*c)),
                    "child {c} left the table"
                );
            }
            Ok(())
        },
    )
    .await;
    b.node.shutdown().await;
    r
}

async fn start_cluster_node_with_retirer(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    id: &str,
) -> Result<common::ClusterNode> {
    use std::time::Duration;
    common::start_cluster_node_tweaked(bucket, id, 2, Duration::from_secs(1), |cfg| {
        cfg.retire_orphans_every = Some(Duration::from_millis(200));
        cfg.reconcile_intents_every = Some(Duration::from_millis(200));
    })
    .await
}

/// **SM-11, negative test — a child of a split in flight never carries a tombstone.**
/// The children are created (Db cloned, owner record in the coordinator's session)
/// **before** the table commits; tombstoning them loses data if the commit wins.
///
/// Interleaving:
/// 1. Node A alone, two ranges. A splits range 0 and stops at `split::ownerClaim`
///    (only on A): children L, R already have a Db and an owner record of session A,
///    the table still has the intent, not committed.
/// 2. Node B joins the cluster; A dies like `kill -9`.
/// 3. After a TTL, B takes range 0 from A (dead) and enters the rollback path — stopping
///    at `split::roll_back` (only on B), before the CAS that drops the intent. At this
///    point the owner of L, R is dead **according to B itself**, and L, R are not in the
///    table.
/// 4. Run one proxy tombstone round from B, with the tripwire
///    `gc::retire_orphan::before_cas` = `panic`.
///
/// Must see: the round keeps L, R with reason `promised`, writes no tombstone, the
/// tripwire does not trip; the owner records of L, R carry no tombstone. Release the
/// rollback path: the intent is rolled back as usual.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_split_children_are_never_tombstoned() -> Result<()> {
    use std::time::Duration;

    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = start_alone(&bucket, "inflight-a").await?;
    let claimed = ctl.pause_at_on(&["inflight-a"], "split::ownerClaim");
    let ops = a.node.ranges.clone();
    let split = tokio::spawn(cairn_store::failpoint::in_node("inflight-a", async move {
        ops.split(cairn_store::PartitionId(0), None)
            .await
            .map(|_| ())
    }));
    claimed.expect_stopped(TEST_DEADLINE).await;
    let intent = a
        .node
        .cluster
        .range(cairn_store::PartitionId(0))
        .and_then(|r| r.intent)
        .ok_or_else(|| anyhow!("range 0 has no intent yet"))?;
    let (left, right) = match intent {
        cairn_store::RangeIntent::Splitting { left, right, .. } => (left.0, right.0),
        other => return Err(anyhow!("unexpected intent: {other:?}")),
    };
    for c in [left, right] {
        ensure!(
            owner_of(&bucket, c).await?.is_some(),
            "child {c} has no owner record yet at the ownerClaim window"
        );
    }

    let rolling = ctl.pause_at_on(&["inflight-b"], "split::roll_back");
    let b = start_cluster_node_with_retirer(&bucket, "inflight-b").await?;
    drop(a); // kill -9
    rolling.expect_stopped(Duration::from_secs(30)).await;

    let r =
        within(
            TEST_DEADLINE,
            "the proxy tombstone round keeps the children of a split in flight",
            async {
                let _tripwire = ctl.fail("gc::retire_orphan::before_cas", "panic");
                // The orphan child round's tripwire (SM-14): a child in flight is never discarded.
                let _discard_tripwire = ctl.fail("gc::discard_orphan::before_discard", "panic");
                let session = b.node.session.clone();
                let stores = cairn_store::ObjectStores::single(
                    cairn_store::StoreLocation::from_store(bucket.clone(), "", "memory://"),
                );
                let round = cairn_server::gc::OrphanRetirer::default()
                    .run_once(&stores, &b.node.cluster, &session)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                ensure!(
                    round.retired.is_empty(),
                    "tombstone round for {:?}: {round:?}",
                    round.retired
                );
                for c in [left, right] {
                    ensure!(
                round
                    .kept
                    .iter()
                    .any(|k| k.range == c && k.by == cairn_server::gc::OrphanKept::Promised),
                "child {c} must be kept since an intent promises it: {round:?}"
            );
                    let rec = owner_of(&bucket, c)
                        .await?
                        .ok_or_else(|| anyhow!("child {c} lost its owner record"))?;
                    ensure!(!rec.retired, "in-flight child {c} has a tombstone: {rec:?}");
                }
                Ok(())
            },
        )
        .await;
    // Release B's rollback path: the intent is rolled back as usual.
    drop(rolling);
    let undone = within(TEST_DEADLINE, "B rolls back the intent", async {
        loop {
            b.node.cluster.reload().await.map_err(|e| anyhow!("{e}"))?;
            if b.node
                .cluster
                .table()
                .ranges()
                .iter()
                .all(|r| r.intent.is_none())
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    // A's (dead) coordinator moves on: its commit CAS misses. No need for the result.
    drop(claimed);
    split.abort();
    b.node.shutdown().await;
    r.and(undone)
}

// ─────────────────────── RD5: rolling back for a coordinator that is still running

/// **A coordinator whose intent was rolled back by someone else does not recreate
/// children** (RD5).
///
/// Interleaving:
/// 1. Node A alone, two ranges, no orphan child round. A splits range 0 and stops at
///    `split::cloneLeft` (only on A): the left child L is cloned (a final checkpoint
///    on 0), the right child R is not yet.
/// 2. Someone else rolls the intent back — like a node taking range 0 after A hung for
///    more than 2×TTL (`node/spawner.rs::factory`), or `cairn-ctl intent clear`: a CAS
///    drops the intent, discards L.
/// 3. Release A: the coordinator moves on.
///
/// Must see: A's split returns an error **before cloning R** (`ensure_intent_ours`),
/// and afterwards L, R have no Db or owner record, and range 0 has no final checkpoint
/// left. Range 0 carries the closed mark (SM-17) while A is stopped, and the roller-back
/// removes it after the CAS — A, waking up, does not close it again (the intent is no
/// longer its own). Before the fix: A cloned R (a new Db with a final checkpoint on 0),
/// claimed L and R (owner records pointing at A's **live** session — SM-11 keeps them
/// forever as `ownerLive`), the commit missed, and `roll_back_here` found no intent
/// left so it discarded nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_rolled_back_by_another_does_not_recreate_children() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = common::start_cluster_node_tweaked(&bucket, "d5-a", 2, Duration::from_secs(1), |cfg| {
        cfg.retire_orphans_every = None;
        cfg.reconcile_intents_every = None;
    })
    .await?;
    common::wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A is leader of every range",
        |p| p.len() == 2 && p.values().all(|v| v == &[0]),
    )
    .await?;
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = slatedb::object_store::path::Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));

    let cloned = ctl.pause_at_on(&["d5-a"], "split::cloneLeft");
    let ops = a.node.ranges.clone();
    let split = tokio::spawn(cairn_store::failpoint::in_node("d5-a", async move {
        ops.split(cairn_store::PartitionId(0), None)
            .await
            .map(|_| ())
    }));
    cloned.expect_stopped(TEST_DEADLINE).await;
    let (left, right) = match a
        .node
        .cluster
        .range(cairn_store::PartitionId(0))
        .and_then(|r| r.intent)
    {
        Some(cairn_store::RangeIntent::Splitting { left, right, .. }) => (left, right),
        other => return Err(anyhow!("range 0 has no split intent yet: {other:?}")),
    };

    let r = within(
        TEST_DEADLINE,
        "roll back for someone, then release the coordinator",
        async {
            // The closed mark (SM-17): the coordinator has closed the source Db before
            // the clone.
            ensure!(
                stores.range_db_closed(cairn_store::PartitionId(0)).await? == Some(true),
                "the source must carry the closed mark from before the clone"
            );
            // Someone else rolls back, with its own table and store.
            let other = cairn_store::ClusterConfig::open(&store, &root, 2).await?;
            cairn_server::split::roll_back(
                &other,
                &stores,
                cairn_store::PartitionId(0),
                cairn_server::split::Who::Opener,
            )
            .await?;
            ensure!(!stores.range_db_exists(left).await?, "L must be discarded");
            ensure!(
                stores.range_db_closed(cairn_store::PartitionId(0)).await? == Some(false),
                "the rollback must remove the source's closed mark after the CAS"
            );
            // The coordinator waking up must stop at the next intent check,
            // **before** cloning R — not clone R and leave it to
            // `discard_own_orphans` to clean up (the outcome is the same, so checking
            // only the outcome stays green with `ensure_intent_ours` removed — a
            // mutation check showed it).
            let cloned_right = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let seen = cloned_right.clone();
            let _right =
                cairn_store::testkit::FailGuard::callback("split::cloneRight", move || {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
                .on_nodes("split::cloneRight", &["d5-a"]);
            drop(cloned);
            let done = split.await?;
            ensure!(
                done.is_err(),
                "the coordinator's split was rolled back yet still succeeded"
            );
            ensure!(
                cloned_right.load(std::sync::atomic::Ordering::SeqCst) == 0,
                "the coordinator was rolled back yet still cloned the right child"
            );
            for c in [left, right] {
                ensure!(
                    !stores.range_db_exists(c).await?,
                    "the Db of child {} was recreated after the rollback",
                    c.0
                );
                let rec = cairn_store::read_owner(&store, &root, c).await?;
                ensure!(
                    rec.is_none(),
                    "the owner record of child {} was recreated after the rollback: {rec:?}",
                    c.0
                );
            }
            let pinned = stores
                .range_checkpoints(cairn_store::PartitionId(0))
                .await?
                .into_iter()
                .filter(|c| !c.expires)
                .count();
            ensure!(
                pinned == 0,
                "range 0 still has {pinned} final checkpoints of orphan children"
            );
            Ok(())
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

// ───────────────────── SM-14: an orphan child after its source retired

/// **The orphan child of a half-rolled-back merge is still discarded after its source
/// retires** (seen in a `--slow-node` chaos run: an orphaned `ranges/28`, verify
/// failed).
///
/// Interleaving:
/// 1. Node A alone, two ranges 0 and 1, no orphan child round. A merges 0+1 → C and
///    stops at `merge::union` (only on A): C's Db exists (final checkpoints on 0 and
///    1), no owner claim yet, not committed.
/// 2. Node B joins the cluster, A dies like `kill -9`.
/// 3. After a TTL, B takes 0, sees an intent with no coordinator ⇒ rolls back: a CAS
///    drops the intent, then it discards C — **failing** at
///    `split::discard::delete_checkpoint` (only on B; the shape of "B was killed while
///    discarding the child"). C's Db stays, without an owner record.
/// 4. B merges 0+1 → D, commits: 0 and 1 leave the table and carry tombstones.
/// 5. One `OrphanRetirer` round from B.
///
/// Must see: the round discards C (`discarded == [C]`; C's Db gone), discards nothing
/// else (0 and 1 are retired with tombstones — they were in the table), and `verify`
/// no longer has `orphan_db`. Before the fix: C stood only on ranges that had left the
/// table so SM-14 could not prove it, and C had no owner record so SM-11 had nothing to
/// tombstone — C stayed orphaned forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_of_half_rolled_back_merge_is_discarded_after_its_sources_retire() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let no_retirer = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.retire_orphans_every = None;
        cfg.reconcile_intents_every = None;
        cfg.reconcile_intents_every = None;
    };
    let a = common::start_cluster_node_tweaked(
        &bucket,
        "sm14-a",
        2,
        Duration::from_secs(1),
        no_retirer,
    )
    .await?;
    common::wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A is leader of every range",
        |p| p.len() == 2 && p.values().all(|v| v == &[0]),
    )
    .await?;
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = slatedb::object_store::path::Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));
    let (p0, p1) = (cairn_store::PartitionId(0), cairn_store::PartitionId(1));
    // Both sources have data: the union lists both in `external_dbs` and puts final
    // checkpoints on them.
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Counter", "add", vec![common::fake_sdk::Step::output("ok")])
        .spawn()
        .await?;
    let url = a.node.url();
    common::register(&url, &sdk.url).await?;
    for i in 0..16 {
        let (code, body) =
            common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
        ensure!(code == 200, "{code} {body}");
    }

    let unioned = ctl.pause_at_on(&["sm14-a"], "merge::union");
    let ops = a.node.ranges.clone();
    let merge = tokio::spawn(cairn_store::failpoint::in_node("sm14-a", async move {
        ops.merge(p0, p1).await.map(|_| ())
    }));
    unioned.expect_stopped(TEST_DEADLINE).await;
    let orphan = match a.node.cluster.range(p0).and_then(|r| r.intent) {
        Some(cairn_store::RangeIntent::Merging { into, .. }) => into,
        other => return Err(anyhow!("range 0 has no merge intent yet: {other:?}")),
    };
    ensure!(
        stores.range_db_exists(orphan).await?,
        "the union has not created the Db of {}",
        orphan.0
    );

    let discard_fails = ctl.fail_on(
        &["sm14-b"],
        "split::discard::delete_checkpoint",
        "return(B was killed while discarding the child)",
    );
    let b = common::start_cluster_node_tweaked(
        &bucket,
        "sm14-b",
        2,
        Duration::from_secs(1),
        no_retirer,
    )
    .await?;
    drop(a); // kill -9

    let r = within(
        Duration::from_secs(60),
        "half rollback, merge again, then one cleanup round",
        async {
            // 3. B takes the range and rolls back the intent; discarding the child fails.
            common::wait_probe(
                &[&b],
                Duration::from_secs(30),
                "B is leader of every range",
                |p| p.len() == 2 && p.values().all(|v| v == &[0]),
            )
            .await?;
            loop {
                b.node.cluster.reload().await?;
                if b.node
                    .cluster
                    .table()
                    .ranges()
                    .iter()
                    .all(|r| r.intent.is_none())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            ensure!(
                stores.range_db_exists(orphan).await?,
                "the rollback managed to discard {} — the failpoint did not fire",
                orphan.0
            );
            ensure!(
                cairn_store::read_owner(&store, &root, orphan)
                    .await?
                    .is_none(),
                "child {} has an owner record",
                orphan.0
            );
            drop(discard_fails);

            // 4. Merge again: 0 and 1 leave the table.
            let (t, _) = b
                .node
                .ranges
                .merge(p0, p1)
                .await
                .map_err(|e| anyhow!("merge again: {e}"))?;
            ensure!(!t.contains(p0) && !t.contains(p1), "{t:?}");
            let verify = || async {
                cairn_server::ctl::Ctl::new(stores.clone())
                    .verify(cairn_server::ctl::VerifyOptions {
                        open_dbs: false,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| anyhow!("{e}"))
            };
            let orphaned = |rep: &cairn_server::ctl::VerifyReport| {
                rep.findings
                    .iter()
                    .any(|f| f.check == "orphan_db" && f.range == Some(orphan.0))
            };
            // The tombstones of 0 and 1 are written by the leader on retiring — wait for them.
            loop {
                let o0 = cairn_store::read_owner(&store, &root, p0).await?;
                let o1 = cairn_store::read_owner(&store, &root, p1).await?;
                if o0.is_some_and(|o| o.retired) && o1.is_some_and(|o| o.retired) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            ensure!(
                orphaned(&verify().await?),
                "verify must see {} orphaned before the cleanup round",
                orphan.0
            );

            // 5. One cleanup round. Tripwire: on a bucket created after the range-level
            // migration framework, no range has the "never entered the table" proof while
            // a live range still stands on it.
            let _referenced_tripwire = ctl.fail("gc::discard_orphan::referenced", "panic");
            let session = b.node.session.clone();
            let round = cairn_server::gc::OrphanRetirer::default()
                .run_once(&stores, &b.node.cluster, &session)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            ensure!(
                round.discarded == vec![orphan.0],
                "the round must discard exactly {}: {round:?}",
                orphan.0
            );
            ensure!(
                !stores.range_db_exists(orphan).await?,
                "the Db of {} is still there after the cleanup round",
                orphan.0
            );
            for p in [p0, p1] {
                ensure!(
                    stores.range_db_exists(p).await?,
                    "the cleanup round deleted the Db of retired source {}",
                    p.0
                );
            }
            let rep = verify().await?;
            ensure!(
                !orphaned(&rep),
                "verify still sees {} orphaned: {:?}",
                orphan.0,
                rep.findings
            );
            Ok(())
        },
    )
    .await;
    // A's (dead) coordinator moves on: the intent is no longer its own. No need for the result.
    drop(unioned);
    merge.abort();
    b.node.shutdown().await;
    r
}

/// **The `gc::discard_orphan::referenced` tripwire sits on the right branch**
/// (`gc.rs::orphan_round_keeps_retired_parent_without_owner_that_children_stand_on` is
/// the behaviour test).
///
/// 1. Node A alone, two ranges, no background cleanup round; write work to range 0.
/// 2. A splits 0 → `l`, `r` (the children stand on 0's SSTs); 0 becomes a tombstone.
/// 3. Delete 0's owner record — the shape of a bucket that a single-process node split
///    before the range-level migration framework.
/// 4. One `OrphanRetirer` round, the tripwire counted by a callback, and
///    `gc::discard_orphan::before_discard` = `panic`.
///
/// Must see: the tripwire fires exactly once (for 0), the round keeps 0 with reason
/// `referenced`, discards nothing, 0's Db is still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_round_trips_the_referenced_wire_on_a_pre_migration_parent() -> Result<()> {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let no_retirer = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.retire_orphans_every = None;
        cfg.reconcile_intents_every = None;
        cfg.reconcile_intents_every = None;
    };
    let a =
        common::start_cluster_node_tweaked(&bucket, "r1i-a", 2, Duration::from_secs(1), no_retirer)
            .await?;
    let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = slatedb::object_store::path::Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));
    let p0 = cairn_store::PartitionId(0);
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Counter", "add", vec![common::fake_sdk::Step::output("ok")])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "referenced tripwire", async {
        common::wait_probe(
            &[&a],
            Duration::from_secs(20),
            "A is leader of every range",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        let url = a.node.url();
        common::register(&url, &sdk.url).await?;
        for i in 0..16 {
            let key = common::key_on_partition(&a.node, "Counter", 0, &format!("k{i}"));
            let (code, body) = common::invoke(&url, "Counter", "add", Some(&key), "x").await?;
            ensure!(code == 200, "{code} {body}");
        }
        a.node
            .ranges
            .split(p0, None)
            .await
            .map_err(|e| anyhow!("split 0: {e}"))?;
        loop {
            if owner_of(&bucket, 0).await?.is_some_and(|o| o.retired) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cairn_store::delete_owner(
            &store,
            &root,
            cairn_store::proof::DeleteProof::assume_owner(p0),
        )
        .await?;

        let hits = Arc::new(AtomicUsize::new(0));
        let counted = hits.clone();
        let _wire = cairn_store::testkit::FailGuard::callback(
            "gc::discard_orphan::referenced",
            move || {
                counted.fetch_add(1, Ordering::SeqCst);
            },
        );
        let _no_discard = ctl.fail("gc::discard_orphan::before_discard", "panic");
        let session = a.node.session.clone();
        let round = cairn_server::gc::OrphanRetirer::default()
            .run_once(&stores, &a.node.cluster, &session)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            hits.load(Ordering::SeqCst) == 1,
            "the tripwire fired {} times: {round:?}",
            hits.load(Ordering::SeqCst)
        );
        ensure!(round.discarded.is_empty(), "{round:?}");
        ensure!(
            round
                .kept
                .iter()
                .any(|k| k.range == 0 && k.by == cairn_server::gc::OrphanKept::Referenced),
            "{round:?}"
        );
        ensure!(stores.range_db_exists(p0).await?, "Db of 0 lost");
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    r
}

// ───────────── an error after `acquire_shared` must not kill the node

/// The range-level migration step **fails deterministically** on range 1 of node
/// `n13-a` when the flag is on — the shape of a buggy step, or of a broken row the step
/// has to read: the error comes up from `node::factory` **after** `acquire_shared`.
static N13_FAIL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn n13_step(
    _: u32,
    w: &mut cairn_store::PartitionWriter,
) -> futures::future::LocalBoxFuture<'_, cairn_store::Result<()>> {
    Box::pin(async move {
        let on_a = cairn_store::failpoint::current_node().is_some_and(|n| &*n == "n13-a");
        if w.partition().0 == 1 && on_a && N13_FAIL.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(cairn_store::StoreError::Io("migration step failed".into()));
        }
        Ok(())
    })
}

const N13_LEDGER: &[cairn_store::migrate::Migration] = &[cairn_store::migrate::Migration {
    to: 1,
    name: "n13",
    run: cairn_store::migrate::Run::Range(n13_step),
}];

/// **A range open failing after `acquire_shared` must not release a live log dirty**
/// (shared WAL).
///
/// Interleaving:
/// 1. Node A alone, two ranges, a shared WAL with **one log stream** (ranges 0 and 1
///    share the log — with two streams the test does not go red: sealing range 1's log
///    does not touch range 0). Range 1's range-level migration step fails on A (the
///    error comes up from `factory` **after** `acquire_shared`: the Db is open on A's
///    log, the keeper is attached to the log). Range 0 opens normally and writes
///    through A's log. A retries range 1 every tick, failing at the same place each
///    time.
/// 2. Node B joins the cluster. B takes range 1 (the migration step does not fail on B).
/// 3. A keeps writing to range 0.
///
/// Must see: A keeps its incarnation, still leads range 0, and calls into range 0
/// through A are still acked. Before the fix: the failed open dropped the keeper
/// **dirty** (A's log segment still on range 1's owner record, the Db not sealed), B
/// taking range 1 sealed A's **live** log, the next PUT of range 0 missed and A
/// fail-stopped — losing its session, and range 0 with it. With a deterministic error
/// the nodes kill each other in turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_open_after_acquire_shared_does_not_kill_the_node() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::{sync::atomic::Ordering::SeqCst, time::Duration},
    };
    let _ctl = StepController::new();
    N13_FAIL.store(true, SeqCst);
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.migrations = N13_LEDGER;
        // One log stream per node: ranges 0 and 1 share a log, as on any node holding
        // more ranges than streams (`range % stripes`).
        cfg.node_wal.stripes = 1;
        cfg.retire_orphans_every = None;
        cfg.reconcile_intents_every = None;
    };
    let a = common::start_cluster_node_tweaked(&bucket, "n13-a", 2, Duration::from_secs(1), tweak)
        .await?;
    let r = within(
        Duration::from_secs(90),
        "A survives B taking the range A failed to open",
        async {
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "A is leader of range 0",
                |p| p.get(&0) == Some(&vec![0]),
            )
            .await?;
            let a_session = a
                .node
                .session
                .current()
                .ok_or_else(|| anyhow!("A has no session"))?;
            let sdk = FakeSdk::builder()
                .object("Counter", "add", vec![Step::output("ok")])
                .spawn()
                .await?;
            let url = a.node.url();
            common::register(&url, &sdk.url).await?;
            // Keys falling into range 0 (by A's table): a call through A is acked on A.
            let keys: Vec<String> = (0..400)
                .map(|i| format!("k-{i}"))
                .filter(|k| {
                    a.node
                        .cluster
                        .partition_of(&cairn_store::TenantId::default(), "Counter", k)
                        .0
                        == 0
                })
                .take(6)
                .collect();
            ensure!(keys.len() == 6, "not enough keys falling into range 0");
            for k in &keys[..2] {
                let (code, body) = common::invoke(&url, "Counter", "add", Some(k), "x").await?;
                ensure!(code == 200, "before B joins: {code} {body}");
            }

            let b = common::start_cluster_node_tweaked(
                &bucket,
                "n13-b",
                2,
                Duration::from_secs(1),
                tweak,
            )
            .await?;
            let r = async {
                common::wait_probe(
                    &[&a, &b],
                    Duration::from_secs(30),
                    "B leads range 1, A range 0",
                    |p| p.get(&0) == Some(&vec![0]) && p.get(&1) == Some(&vec![1]),
                )
                .await?;
                for k in &keys[2..] {
                    let (code, body) = common::invoke(&url, "Counter", "add", Some(k), "x").await?;
                    ensure!(code == 200, "after B took range 1: {code} {body}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                let now = a.node.session.current();
                ensure!(
                    now.as_ref() == Some(&a_session),
                    "A changed incarnation (fail-stop?): {a_session} → {now:?}"
                );
                Ok(())
            }
            .await;
            b.node.shutdown().await;
            r
        },
    )
    .await;
    N13_FAIL.store(false, SeqCst);
    a.node.shutdown().await;
    r
}

/// **A failed seal on release does not bring the node down** (shared WAL).
///
/// Interleaving:
/// 1. Two nodes, each with **one log stream** (all ranges of a node share its log, as
///    on a node holding more ranges than streams); balanced 2/2, one finished call on
///    each of alpha's ranges.
/// 2. `writer::seal::before_flush` returns an error once on alpha; `Resign` alpha's
///    range P: the seal fails ⇒ a dirty release, the owner record keeps a segment
///    pointing at alpha's log.
/// 3. P goes to some node (beta, or alpha on a new stream): the new owner seals
///    alpha's old log and then replays.
/// 4. Call into P and into alpha's other range Q (retrying until there is a leader).
///
/// Must see: no node changes incarnation; both calls finish. Before the fix: the new
/// owner of P sealed alpha's **live** log — the next PUT of Q met the seal
/// (`wal_fenced`), or alpha took P back and met its own log (`c1`): alpha fail-stopped
/// because of one range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_seal_on_resign_does_not_fail_stop_the_node() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.node_wal.stripes = 1;
    };
    let alpha =
        common::start_cluster_node_tweaked(&bucket, "node-alpha", 4, Duration::from_secs(1), tweak)
            .await?;
    let beta =
        common::start_cluster_node_tweaked(&bucket, "node-beta", 4, Duration::from_secs(1), tweak)
            .await?;
    let r = within(
        Duration::from_secs(90),
        "alpha survives a failed seal on release",
        async {
            let nodes = [&alpha, &beta];
            let probe =
                common::wait_probe(&nodes, Duration::from_secs(30), "balanced 2/2", |p| {
                    common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [2, 2])
                })
                .await?;
            let leaders =
                common::single_leaders(&probe).ok_or_else(|| anyhow!("no leader yet"))?;
            let mine: Vec<u32> = leaders
                .iter()
                .filter(|(_, i)| **i == 0)
                .map(|(p, _)| *p)
                .collect();
            ensure!(mine.len() == 2, "alpha must hold two ranges: {leaders:?}");
            let (p, q) = (mine[0], mine[1]);
            let sessions = [alpha.node.session.current(), beta.node.session.current()];
            let sdk = FakeSdk::builder()
                .object("Counter", "add", vec![Step::output("ok")])
                .spawn()
                .await?;
            let url = alpha.node.url();
            common::register(&url, &sdk.url).await?;
            let key_p = common::key_on_partition(&alpha.node, "Counter", p, "p");
            let key_q = common::key_on_partition(&alpha.node, "Counter", q, "q");
            for k in [&key_p, &key_q] {
                let (code, body) = common::invoke(&url, "Counter", "add", Some(k), "x").await?;
                ensure!(code == 200, "before release: {code} {body}");
            }
            // 2.
            let _seal = ctl.fail_on(
                &["node-alpha"],
                "writer::seal::before_flush",
                "1*return(seal)",
            );
            let resigned = alpha
                .node
                .partitions
                .get(cairn_store::PartitionId(p))
                .ok_or_else(|| anyhow!("alpha has no engine for range {p}"))?
                .resign()
                .await;
            ensure!(resigned.is_err(), "the seal must fail: {resigned:?}");
            // 3–4.
            for (k, range) in [(&key_p, p), (&key_q, q)] {
                let t0 = std::time::Instant::now();
                loop {
                    let (code, body) = common::invoke(&url, "Counter", "add", Some(k), "y").await?;
                    if code == 200 {
                        break;
                    }
                    ensure!(
                        t0.elapsed() < Duration::from_secs(30),
                        "range {range} did not finish a call after the dirty release: {code} {body}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            // A fail-stop follows the new owner's seal by one PUT tick: keep watching
            // for a few more seconds.
            let t0 = std::time::Instant::now();
            while t0.elapsed() < Duration::from_secs(3) {
                let now = [alpha.node.session.current(), beta.node.session.current()];
                ensure!(
                    now == sessions,
                    "a node changed incarnation after a failed seal on release: {sessions:?} → {now:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        },
    )
    .await;
    beta.node.shutdown().await;
    alpha.node.shutdown().await;
    r
}

// ─────────────────────────────── the term's safety nets

/// Builds cluster node `id` with one range on `bucket` and waits until it leads that range.
async fn start_led_alone(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    id: &str,
) -> Result<common::ClusterNode> {
    use std::time::Duration;
    let n = common::start_cluster_node(bucket, id, 1, Duration::from_secs(2)).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(20),
        "node is leader of range 0",
        |p| p.get(&0) == Some(&vec![0]),
    )
    .await?;
    Ok(n)
}

/// Sends `n` invocations `service/handler`, returns the ids.
async fn send_n(url: &str, service: &str, handler: &str, n: usize) -> Result<Vec<String>> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let (code, body) = common::send(url, service, handler, None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        ids.push(body["id"].as_str().unwrap_or_default().to_string());
    }
    Ok(ids)
}

/// The number of `Start`s the fake SDK received for each id in `ids`.
fn starts_per_id(sdk: &common::fake_sdk::FakeSdk, ids: &[String]) -> Vec<usize> {
    let starts = sdk.starts();
    ids.iter()
        .map(|id| starts.iter().filter(|s| &s.id_hex() == id).count())
        .collect()
}

/// **A failed `recover` scan is never rerun.**
///
/// Interleaving:
/// 1. Node A alone, one range. Three `Held/run` invocations: the SDK holds the first
///    turn of all three at its gate — they are `running`, their queue entries at
///    `Running`.
/// 2. A shuts down mid-turn (the turns are cancelled, as when a leader dies mid-turn):
///    in the store all three are still `running`, no timer, no retry, no outbox row
///    pointing at them — only the scan of the next leadership can pick them up.
/// 3. `actor::recover::scan` = `1*return`, only on B: that scan fails once (a passing
///    503).
/// 4. B becomes leader on the same bucket: `restore_flow` pushes `Running` back to
///    `Inbox`, `recover` fails its scan and only `warn`s.
/// 5. Open the SDK gate.
///
/// Must see: all three `completed` within the cap, each with exactly one turn on B
/// (two `Start`s counting the turn cancelled on A) — no duplicate turn. Before the fix
/// nothing woke them during B's term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_recover_scan_is_retried_within_the_term() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .gated_service("Held", "run", vec![Step::output("\"done\"")])
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "rescue-scan-a").await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let ids = send_n(&a.node.url(), "Held", "run", 3).await?;
    within(TEST_DEADLINE, "three first turns reach the SDK", async {
        while sdk.starts_len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    a.node.shutdown().await;

    let _scan = ctl.fail_on(&["rescue-scan-b"], "actor::recover::scan", "1*return");
    let b = start_led_alone(&bucket, "rescue-scan-b").await?;
    sdk.gate().open();
    let done = within(
        Duration::from_secs(15),
        "three invocations completed on B",
        async {
            for id in &ids {
                common::wait_status(&b.node.url(), id, "completed").await?;
            }
            Ok(())
        },
    )
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    b.node.shutdown().await;
    done?;
    ensure!(
        per_id.iter().all(|n| *n == 2),
        "each invocation exactly one turn on B: {per_id:?}"
    );
    Ok(())
}

/// **The turn of a `/send` fails before the point where it reschedules.**
///
/// Interleaving:
/// 1. Node C alone, one range. `turn::ensure_queued` = `1*return`, only on C.
/// 2. `/send` creates the invocation, answers (durable), then runs the first turn
///    behind the caller's back: `drive_client` meets the error in `ensure_queued` and
///    `?`s straight out — without going through `schedule_after_error`. The task only
///    `warn`s.
///
/// Must see: the invocation `completed` within the cap, exactly one `Start`. Before the
/// fix it stayed `running` until the next leader change: no retry, no timer, nobody
/// dispatching it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_turn_failing_before_reschedule_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let c = start_led_alone(&bucket, "rescue-queue-c").await?;
    common::register(&c.node.url(), &sdk.url).await?;
    let _q = ctl.fail_on(&["rescue-queue-c"], "turn::ensure_queued", "1*return");
    let ids = send_n(&c.node.url(), "Echo", "run", 1).await?;
    let done = within(Duration::from_secs(15), "invocation completed", async {
        common::wait_status(&c.node.url(), &ids[0], "completed").await?;
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    c.node.shutdown().await;
    done?;
    ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
    Ok(())
}

/// **A failed turn whose rescheduling also fails.**
///
/// Interleaving:
/// 1. Node D alone, one range. `turn::drive_one` and
///    `turn::schedule_after_error` = `1*return`, only on D.
/// 2. `/send` creates the invocation; the first turn gets a permit, moves to
///    `Running`, then fails in `drive_one` (an I/O error mid-turn). `settle_permit`
///    releases the permit, `schedule_after_error` fails to read `journal_len` — it logs
///    that it could not read `journal_len` to reschedule after the error, and gives up.
///
/// Must see: the invocation `completed` within the cap, exactly one `Start` (the failed
/// turn never reaches the SDK). Before the fix nothing was left to wake it during the
/// term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turn_whose_reschedule_also_fails_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let d = start_led_alone(&bucket, "rescue-resched-d").await?;
    common::register(&d.node.url(), &sdk.url).await?;
    let _turn = ctl.fail_on(&["rescue-resched-d"], "turn::drive_one", "1*return");
    let _resched = ctl.fail_on(
        &["rescue-resched-d"],
        "turn::schedule_after_error",
        "1*return",
    );
    let ids = send_n(&d.node.url(), "Echo", "run", 1).await?;
    let done = within(Duration::from_secs(15), "invocation completed", async {
        common::wait_status(&d.node.url(), &ids[0], "completed").await?;
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    d.node.shutdown().await;
    done?;
    ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
    Ok(())
}

// ─────────────────── reopening a range that must not be opened (RD1, RD2)

/// `POST /invoke/{service}/{handler}/send?key=…` with a time cap: `Some(id)` if the node
/// **acks** (`200`/`202`), `None` if it refuses, fails or runs past the cap — only an
/// ack promises the client anything.
async fn try_send(url: &str, service: &str, key: &str) -> Option<String> {
    let send = common::send(url, service, "run", Some(key), "\"x\"");
    match tokio::time::timeout(std::time::Duration::from_secs(3), send).await {
        Ok(Ok((code, body))) if code == 200 || code == 202 => {
            body["id"].as_str().map(str::to_string)
        }
        _ => None,
    }
}

/// **RD1 — a node holding a stale table because reloading the table failed must not
/// reopen a parent range that left the table.**
///
/// The engine open reloads `cluster.conf` to stop exactly this case, but
/// `ClusterConfig::reload` returned `LeaseIo` while `open_io` only retried `Io`, and the
/// error was swallowed: the open went on with the stale in-RAM table.
///
/// Interleaving:
/// 1. Node A alone, one range (0). Register the SDK. Node X joins the cluster: X's
///    in-RAM table is at the version before the intent, X has an engine for range 0
///    waiting to claim the lease.
/// 2. **X's** store: every GET of `cluster.conf` fails (a burst of 503s on a hot
///    object) — `follow` and every reload of X miss, X's in-RAM table stands still.
/// 3. `session::retire::before_write` = `return`, only on A: A splits 0 into two
///    children and commits, but 0's tombstone is not written (the shape of "A died
///    right after the commit").
/// 4. A dies like `kill -9`. After a TTL, X sees A's session expired and claims range
///    0 — X's in-RAM table has no intent so it does not wait for the rescue grace.
///    X's engine open reloads the table, and fails.
/// 5. In that window, a client sends `/send` to X for a key belonging to range 0.
/// 6. Remove X's fault: X learns the new table, leads the two children.
///
/// Must see: X **never** leads range 0 in the window (4)–(5), and every `/send` X acked
/// in (5) can be looked up after (6). Before the fix X opened 0's Db by the stale in-RAM
/// table, acked writes into it, then 0 retired when X learned the table: the acks were
/// lost (`404`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_table_never_reopens_a_parent_that_left_the_table() -> Result<()> {
    stale_table_scenario(false).await
}

/// **RD1 with only the close mark on the Db itself** (F8): the same interleaving as
/// [`stale_table_never_reopens_a_parent_that_left_the_table`], but the
/// `node::open::bypass_old_fences` knob turns off every older fence of the open on every
/// node — the table load error is swallowed as in the original RD1, X opens by the
/// stale in-RAM table.
///
/// Must see, as in the original test: X does not lead range 0, no ack is lost. What
/// stops X is the `closed` flag the coordinator wrote into range 0's manifest at
/// quiesce — SlateDB refuses the writer, whatever X's in-RAM table says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_flag_alone_keeps_a_stale_table_from_reopening_the_parent() -> Result<()> {
    stale_table_scenario(true).await
}

async fn stale_table_scenario(bypass: bool) -> Result<()> {
    use {
        cairn_store::{
            testkit::{Action, OpSet, PathMatch, Rule},
            PartitionId,
        },
        common::fake_sdk::{FakeSdk, Step},
        std::time::{Duration, Instant},
    };

    let ctl = StepController::new();
    let _bypass = bypass.then(|| ctl.fail("node::open::bypass_old_fences", "return"));
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "d1-a").await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let x = common::start_cluster_node(&bucket, "d1-x", 1, Duration::from_secs(2)).await?;
    let x_session = x
        .node
        .session
        .current()
        .ok_or_else(|| anyhow!("X has no session"))?;
    let key = common::key_on_partition(&x.node, "Obj", 0, "d1");

    let stale = x.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cluster.conf".into()),
        Action::Fail { times: None },
    ));
    let _no_tomb = ctl.fail_on(&["d1-a"], "session::retire::before_write", "return");
    let (table, _) = within(TEST_DEADLINE, "A splits range 0", async {
        a.node
            .ranges
            .split(PartitionId(0), None)
            .await
            .map_err(|e| anyhow!("{e}"))
    })
    .await?;
    ensure!(
        !table.contains(PartitionId(0)),
        "range 0 still in the table: {table:?}"
    );
    ensure!(
        x.node.cluster.contains(PartitionId(0)),
        "X must still hold the stale table"
    );
    let before = owner_of(&bucket, 0)
        .await?
        .ok_or_else(|| anyhow!("range 0 has no owner record"))?;
    ensure!(!before.retired, "tombstone not held back: {before:?}");
    drop(a); // kill -9

    // (4) X claims range 0.
    within(Duration::from_secs(30), "X claims range 0", async {
        loop {
            if owner_of(&bucket, 0)
                .await?
                .is_some_and(|r| r.session == x_session)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    // (5) In the window: X must not lead 0; record every ack.
    let mut led_zero = false;
    let mut acked = Vec::new();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(8) {
        led_zero |= common::leaders_by_probe(&[&x])
            .await
            .get(&0)
            .is_some_and(|v| !v.is_empty());
        if let Some(id) = try_send(&x.node.url(), "Obj", &key).await {
            acked.push(id);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // (6) Remove the fault: X learns the new table and leads the two children.
    x.store.remove_rule(stale);
    let children: Vec<u32> = table.partitions().map(|p| p.0).collect();
    let healed = within(Duration::from_secs(30), "X leads the two children", async {
        loop {
            let probe = common::leaders_by_probe(&[&x]).await;
            if !x.node.cluster.contains(PartitionId(0))
                && children
                    .iter()
                    .all(|c| probe.get(c).is_some_and(|v| v == &[0]))
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let mut lost = Vec::new();
    for id in &acked {
        let (code, _) = common::invocation(&x.node.url(), id).await?;
        if code != 200 {
            lost.push((id.clone(), code));
        }
    }
    x.node.shutdown().await;
    healed?;
    ensure!(
        !led_zero && lost.is_empty(),
        "X reopened range 0, which had left the table, by its stale in-RAM table (leading: {led_zero}); {} acks, lost {lost:?}",
        acked.len()
    );
    Ok(())
}

/// The table-changing operation of [`quiesced_source_never_reopens_while_coordinator_lives`].
#[derive(Clone, Copy, Debug)]
enum RangeOp {
    /// Split range 0, stopping at `split::cloneRight` (both children cloned, not claimed).
    Split,
    /// Merge ranges 0 and 1, stopping at `merge::union` (the merged Db exists, not claimed).
    Merge,
}

/// **RD2 — a quiescing source range must not be reopened while the split/merge
/// coordinator is alive.** `ActiveIntents` only blocked the **rollback** path of the
/// engine open, not the open itself.
///
/// Interleaving:
/// 1. Node A alone, leading every range. Register the SDK.
/// 2. A's coordinator splits range 0 (or merges 0 + 1): intent, flush, quiesce (the
///    source Db closed, the lease held), clone (or union) done — then it **stops** at
///    `split::cloneRight` (`merge::union`), only on A. The table still has the intent.
/// 3. `session::renew::before_put` = `return`, only on A: A's session runs past its
///    deadline and steps down, A opens a new incarnation; the point is removed as soon
///    as the new incarnation is up. The source engine in `quiesced()` sees the lease
///    lost and claims again from scratch.
/// 4. After the rescue grace (2×TTL, the table has an intent), **A's own** balancer
///    claims the source again (the record points at A's abandoned incarnation).
///    `factory` sees an intent the coordinator is working on (`ActiveIntents`) so it does
///    not roll back.
/// 5. In that window, a client sends `/send` for a key belonging to range 0.
/// 6. Release the coordinator: child claims, commit CAS, the source retires.
///
/// Must see: the source **never** leads in the window (4)–(5), every `/send` acked in
/// (5) can be looked up after (6), the operation commits, and the tripwire
/// `node::open::quiesced_source` does not trip. Before the fix `factory` opened the
/// source's Db and served, the commit CAS still won (the table had not changed), and
/// the acks sat in a retired source Db: lost (`404`).
async fn quiesced_source_never_reopens_while_coordinator_lives(
    op: RangeOp,
    bypass: bool,
) -> Result<()> {
    quiesced_source_scenario(op, bypass, false).await
}

/// The shared body of the RD2 tests. `before_close`: the coordinator stops **before** the
/// close mark (`split::before_close` / `merge::before_close`) instead of after the
/// clone/union — the window the M1 model found; there the operation may roll back
/// instead of committing, but no ack may be lost.
async fn quiesced_source_scenario(op: RangeOp, bypass: bool, before_close: bool) -> Result<()> {
    use {
        cairn_store::PartitionId,
        common::fake_sdk::{FakeSdk, Step},
        std::time::{Duration, Instant},
    };

    let ctl = StepController::new();
    let _bypass = bypass.then(|| ctl.fail("node::open::bypass_old_fences", "return"));
    let node_id = match op {
        RangeOp::Split => "d2-split-a",
        RangeOp::Merge => "d2-merge-a",
    };
    let ranges: u16 = match op {
        RangeOp::Split => 1,
        RangeOp::Merge => 2,
    };
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    // `direct` explicitly: step (3) makes A lose its session by blocking the renewal
    // PUT (`session::renew::before_put`) — a lone node in `solo` (the default for new
    // buckets) renews by GET, without going through that point.
    let a = common::start_cluster_node_tweaked(
        &bucket,
        node_id,
        ranges,
        Duration::from_secs(2),
        common::direct,
    )
    .await?;
    common::wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A is leader of every range",
        |p| p.len() == ranges as usize && p.values().all(|v| v == &[0]),
    )
    .await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let session = a.node.session.clone();
    let key = common::key_on_partition(&a.node, "Obj", 0, "d2");
    let sources: Vec<u32> = (0..ranges as u32).collect();

    // SM-13 tripwire: `factory` is about to open the Db of a quiescing source.
    let tripped = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count = std::sync::Arc::clone(&tripped);
    let _tripwire =
        cairn_store::testkit::FailGuard::callback("node::open::quiesced_source", move || {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });

    // (2) The coordinator stops after clone/union.
    let point = match (op, before_close) {
        (RangeOp::Split, false) => "split::cloneRight",
        (RangeOp::Merge, false) => "merge::union",
        (RangeOp::Split, true) => "split::before_close",
        (RangeOp::Merge, true) => "merge::before_close",
    };
    let mut paused = Some(ctl.pause_at_on(&[node_id], point));
    let ops = a.node.ranges.clone();
    let coordinator = tokio::spawn(cairn_store::failpoint::in_node(node_id, async move {
        match op {
            RangeOp::Split => ops.split(PartitionId(0), None).await.map(|(t, _)| t),
            RangeOp::Merge => ops
                .merge(PartitionId(0), PartitionId(1))
                .await
                .map(|(t, _)| t),
        }
    }));
    paused
        .as_ref()
        .expect("just set")
        .expect_stopped(TEST_DEADLINE)
        .await;

    // (3) A loses its session, opens a new incarnation.
    let old = session
        .current()
        .ok_or_else(|| anyhow!("A has no incarnation"))?;
    let renew = ctl.fail_on(&[node_id], "session::renew::before_put", "return");
    let new = within(Duration::from_secs(20), "A opens new incarnation", async {
        loop {
            if let Some(c) = session.current().filter(|c| *c != old) {
                return Ok(c);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    drop(renew);

    // (4) The source engine claims range 0 again in the new incarnation.
    within(
        Duration::from_secs(30),
        "A claims range 0 again in the new incarnation",
        async {
            loop {
                if owner_of(&bucket, 0)
                    .await?
                    .is_some_and(|r| r.session == new)
                {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        },
    )
    .await?;
    if before_close {
        // (4b) The source reopens its Db **with nothing written yet** (the clone gets
        // past F3), then release the coordinator to after clone/union: the acks of (5)
        // reach the writer opened **after** the clone. A close conditional on the epoch ⇒
        // the coordinator rolls back right at the close command, never reaching the
        // second point.
        common::wait_probe(&[&a], Duration::from_secs(30), "source reopened", |p| {
            sources
                .iter()
                .all(|s| p.get(s).is_some_and(|v| !v.is_empty()))
        })
        .await?;
        let after_clone = match op {
            RangeOp::Split => "split::cloneRight",
            RangeOp::Merge => "merge::union",
        };
        let second = ctl.pause_at_on(&[node_id], after_clone);
        if let Some(p) = paused.take() {
            p.resume();
        }
        within(
            TEST_DEADLINE,
            "the coordinator reaches after-clone or rolls back",
            async {
                while second.hits() == 0 && !coordinator.is_finished() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(())
            },
        )
        .await?;
        paused = Some(second);
    }
    // (5) In the window: the source must not lead; record every ack.
    let mut reopened = Vec::new();
    let mut acked = Vec::new();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(6) {
        let probe = common::leaders_by_probe(&[&a]).await;
        for s in &sources {
            if probe.get(s).is_some_and(|v| !v.is_empty()) && !reopened.contains(s) {
                reopened.push(*s);
            }
        }
        if let Some(id) = try_send(&a.node.url(), "Obj", &key).await {
            acked.push(id);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // (6) Release the coordinator.
    if let Some(p) = paused.take() {
        p.resume();
    }
    let done = within(TEST_DEADLINE, "coordinator done", async {
        coordinator.await?.map_err(|e| anyhow!("{e}"))
    })
    .await;
    let mut lost = Vec::new();
    let found = within(TEST_DEADLINE, "the new range has a leader", async {
        loop {
            let probe = common::leaders_by_probe(&[&a]).await;
            let table = a.node.cluster.table();
            if table
                .partitions()
                .all(|p| probe.get(&p.0).is_some_and(|v| v == &[0]))
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    for id in &acked {
        let (code, _) = common::invocation(&a.node.url(), id).await?;
        if code != 200 {
            lost.push((id.clone(), code));
        }
    }
    a.node.shutdown().await;
    // Before the close mark, the coordinator may roll back (the source has reopened).
    // After clone/union it must commit.
    // The source reopened before the close command (the epoch changed) ⇒ the close
    // conditional on the epoch misses and the coordinator rolls back **right at the
    // close command**: its error is a fence, not F3 refusing the clone ("sources with
    // WAL") — F3 is a hidden fence behind it, and a test that only checks the outcome
    // cannot tell the two apart (a mutation check: `scripts/mutants.py sm17-close-epoch`).
    if let (true, Err(e)) = (before_close, &done) {
        let msg = format!("{e:#}");
        ensure!(
            msg.contains("fence") && !msg.contains("sources with WAL"),
            "{op:?}: the coordinator must roll back at the epoch-conditional close, error: {msg}"
        );
    }
    let committed = match done {
        Ok(table) if before_close => {
            return Err(anyhow!(
                "{op:?}: the source reopened before the close yet the operation still committed: {table:?}"
            ));
        }
        Ok(table) => {
            ensure!(
                sources.iter().all(|s| !table.contains(PartitionId(*s))),
                "{op:?} returned Ok without committing: {table:?}"
            );
            true
        }
        Err(e) if before_close => {
            let _ = e;
            false
        }
        Err(e) => return Err(e),
    };
    found?;
    if before_close {
        // The test only means something if the source really reopened and accepted acks.
        ensure!(
            !acked.is_empty() && !reopened.is_empty(),
            "{op:?}: the source did not reopen or accepted no ack — the test did not build the window"
        );
        // The source reopening in the window is what the test causes on purpose; what
        // must hold is that no ack is lost, whether the operation commits or rolls back.
        ensure!(
            lost.is_empty(),
            "{op:?} (commit={committed}): {} acks, lost {lost:?}",
            acked.len()
        );
        return Ok(());
    }
    ensure!(
        reopened.is_empty() && lost.is_empty(),
        "{op:?}: source {reopened:?} reopened while the coordinator was alive; {} acks, lost {lost:?}",
        acked.len()
    );
    let tripped = tripped.load(std::sync::atomic::Ordering::SeqCst);
    // With the knob on, the branch with the tripwire is skipped: nothing to count.
    ensure!(
        bypass || tripped == 0,
        "{op:?}: tripwire node::open::quiesced_source tripped {tripped} times"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesced_split_source_never_reopens_while_coordinator_lives() -> Result<()> {
    quiesced_source_never_reopens_while_coordinator_lives(RangeOp::Split, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesced_merge_sources_never_reopen_while_coordinator_lives() -> Result<()> {
    quiesced_source_never_reopens_while_coordinator_lives(RangeOp::Merge, false).await
}

/// **RD2 with only the close mark on the Db itself** (F8): the same interleaving as
/// [`quiesced_split_source_never_reopens_while_coordinator_lives`], but the
/// `node::open::bypass_old_fences` knob turns off the SM-13 wait and its tripwire: in
/// (4) A's `factory` goes straight to opening the source's Db.
///
/// Must see, as in the original test: the source never leads in the window, no ack is
/// lost, the operation commits. What stops it is the `closed` flag the coordinator
/// writes into the source's manifest right after quiesce, before the clone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_flag_alone_keeps_a_quiesced_split_source_closed() -> Result<()> {
    quiesced_source_never_reopens_while_coordinator_lives(RangeOp::Split, true).await
}

/// **The source reopens between quiesce and the close mark** (a finding of the M1
/// model). Like [`closed_flag_alone_keeps_a_quiesced_split_source_closed`] — the
/// `node::open::bypass_old_fences` knob on — but the coordinator stops at
/// `split::before_close`: the source engine claims again and **opens** the Db before the
/// flag exists; then the coordinator moves on to after the clone, and the acks reach the
/// writer just opened. The close command only pushed `writer_epoch`: the writer just
/// opened was fenced only at its next manifest update — in the M1 model, it could ack
/// after the clone and the commit won.
///
/// Must see: no ack lost, and the coordinator rolls back **at the close command** (the
/// fence error of the epoch-conditional close), never reaching the clone. Behind it is
/// one more hidden fence — F3: a writer open always writes the WAL, so a clone with a
/// projection refuses the source ("sources with WAL") — so a check of only "no ack
/// lost" stays green with the epoch-conditional close removed (a mutation check).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_reopened_before_the_close_loses_no_ack() -> Result<()> {
    quiesced_source_scenario(RangeOp::Split, true, true).await
}

/// Like [`source_reopened_before_the_close_loses_no_ack`], for a merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_source_reopened_before_the_close_loses_no_ack() -> Result<()> {
    quiesced_source_scenario(RangeOp::Merge, true, true).await
}

/// Like [`closed_flag_alone_keeps_a_quiesced_split_source_closed`], for a merge: both
/// sources carry the `closed` flag from after the quiesce.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_flag_alone_keeps_quiesced_merge_sources_closed() -> Result<()> {
    quiesced_source_never_reopens_while_coordinator_lives(RangeOp::Merge, true).await
}

/// Writes a split intent on range `p` (allocating child ids like a real coordinator)
/// through `cluster`; returns the intent written.
async fn mark_split_intent(
    cluster: &cairn_store::ClusterConfig,
    p: cairn_store::PartitionId,
    nth: u64,
) -> Result<cairn_store::RangeIntent> {
    cluster.reload().await?;
    let t = cluster.table();
    let r = *t
        .range(p)
        .ok_or_else(|| anyhow!("no range {}: {t:?}", p.0))?;
    let intent = cairn_store::RangeIntent::Splitting {
        at: r.start + (r.end - r.start) / (2 + nth) + 1,
        left: cairn_store::PartitionId(t.next_range_id()),
        right: cairn_store::PartitionId(t.next_range_id() + 1),
    };
    let marked = cluster
        .update(t.version(), |t| {
            let ranges = t
                .ranges()
                .iter()
                .map(|e| {
                    if e.id == p {
                        cairn_store::RangeEntry {
                            intent: Some(intent),
                            ..*e
                        }
                    } else {
                        *e
                    }
                })
                .collect();
            cairn_store::PartitionTable::from_entries(
                t.version() + 1,
                t.next_range_id() + 2,
                ranges,
            )
        })
        .await?;
    ensure!(
        matches!(marked, cairn_store::TableUpdate::Applied(_)),
        "could not write the intent: {marked:?}"
    );
    Ok(intent)
}

/// **A slow roller-back removes the close mark of a later intent** (a case the M1
/// model did not cover: a slow remover).
///
/// Interleaving (no node runs; the steps of the coordinator and of the open are written
/// directly through `ObjectStores`):
/// 1. Range 1 has a Db. Intent I1 (split) goes onto the table; its coordinator closes
///    the source Db.
/// 2. I1's roller-back (`split::roll_back_if`) CASes I1 away, then stops at
///    `split::roll_back::before_reopen` — before removing the mark.
/// 3. In the window: the range's next open removes the old mark itself
///    (`heal_stale_close`) and opens a writer (the epoch grows); intent I2 goes onto the
///    table and its coordinator closes the source again — the close mark is now I2's.
/// 4. Release I1's roller-back.
///
/// Must see: range 1's Db still carries the close mark (I2's) — I1's roller-back may only
/// remove the mark of the close it saw before its CAS. Before the fix,
/// `reopen_for_writes` unconditionally removed I2's mark too: the source engine could
/// reopen while I2's coordinator cloned (SM-17 left with only SM-13 and a fresh table to
/// hold it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_roll_back_keeps_the_close_of_a_later_intent() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    bucket.allow(
        "stuck_intent",
        "the test stops mid-way through the second operation: intent I2 is still on the table at cleanup",
    );
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = slatedb::object_store::path::Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));
    let cluster = cairn_store::ClusterConfig::open(&store, &root, 2).await?;
    let p = cairn_store::PartitionId(1);
    // (1)
    stores.open_db(p.0).await?.close().await?;
    let i1 = mark_split_intent(&cluster, p, 0).await?;
    stores.close_range_db(p).await?;
    // (2)
    let paused = ctl.pause_at("split::roll_back::before_reopen");
    let roller = {
        let (cluster, stores) = (
            cairn_store::ClusterConfig::open(&store, &root, 2).await?,
            stores.clone(),
        );
        tokio::spawn(async move {
            cairn_server::split::roll_back_if(
                &cluster,
                &stores,
                p,
                i1,
                cairn_server::split::Who::Settle,
            )
            .await
        })
    };
    paused.expect_stopped(TEST_DEADLINE).await;
    // (3)
    stores.reopen_range_db(p).await?;
    stores.open_db(p.0).await?.close().await?;
    mark_split_intent(&cluster, p, 1).await?;
    stores.close_range_db(p).await?;
    ensure!(stores.range_db_closed(p).await? == Some(true));
    // (4)
    paused.resume();
    let undone = within(TEST_DEADLINE, "I1's roller-back done", async {
        Ok(roller.await??)
    })
    .await?;
    ensure!(undone, "the roller-back must CAS I1 away");
    ensure!(
        stores.range_db_closed(p).await? == Some(true),
        "I1's slow roller-back removed I2's close mark"
    );
    Ok(())
}

/// **`cairn-ctl intent clear` removes the close mark of a later intent** (the same
/// shape on the operator's path; a finding of the M2 model).
///
/// Interleaving (no node runs):
/// 1. Range 1 has a Db. Intent I1 goes onto the table; its coordinator closes the source Db.
/// 2. `intent clear 1 --apply --force` CASes I1 away, then stops at
///    `ctl::intent_clear::before_reopen` — before removing the mark.
/// 3. In the window: the next open removes the old mark itself and opens a writer;
///    intent I2 goes onto the table and its coordinator closes the source again.
/// 4. Release ctl.
///
/// Must see: range 1's Db still carries the close mark (I2's) — ctl may only remove the
/// mark of the close it saw before its CAS. Before the fix ctl removed it
/// unconditionally (`reopen_range_db`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ctl_intent_clear_keeps_the_close_of_a_later_intent() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    bucket.allow(
        "stuck_intent",
        "the test stops mid-way through the second operation: intent I2 is still on the table at cleanup",
    );
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = slatedb::object_store::path::Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));
    let cluster = cairn_store::ClusterConfig::open(&store, &root, 2).await?;
    let p = cairn_store::PartitionId(1);
    // (1)
    stores.open_db(p.0).await?.close().await?;
    mark_split_intent(&cluster, p, 0).await?;
    stores.close_range_db(p).await?;
    // (2)
    let paused = ctl.pause_at("ctl::intent_clear::before_reopen");
    let clearing = {
        let stores = stores.clone();
        tokio::spawn(async move {
            cairn_server::ctl::Ctl::new(stores)
                .intent_clear(
                    p.0,
                    &cairn_server::ctl::RepairOptions {
                        apply: true,
                        force: true,
                        who: "test".into(),
                        command: "intent clear 1".into(),
                        expect_version: None,
                        now_ms: cairn_server::gc::wall_ms(),
                    },
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        })
    };
    paused.expect_stopped(TEST_DEADLINE).await;
    // (3)
    stores.reopen_range_db(p).await?;
    stores.open_db(p.0).await?.close().await?;
    mark_split_intent(&cluster, p, 1).await?;
    stores.close_range_db(p).await?;
    ensure!(stores.range_db_closed(p).await? == Some(true));
    // (4)
    paused.resume();
    let rep = within(TEST_DEADLINE, "ctl done", async { clearing.await? }).await?;
    ensure!(rep.applied, "ctl must CAS I1 away: {rep:?}");
    ensure!(
        stores.range_db_closed(p).await? == Some(true),
        "ctl removed I2's close mark"
    );
    Ok(())
}

// ───────────────── operations do not die with their request

/// `POST path` without a body, written directly to a TCP socket — so the test **itself
/// closes** the connection exactly when it wants (`drop` is `close()`: the FIN goes out
/// at once), bypassing the HTTP client's connection pool.
async fn raw_post(addr: std::net::SocketAddr, path: &str) -> Result<tokio::net::TcpStream> {
    use tokio::io::AsyncWriteExt;
    let mut s = tokio::net::TcpStream::connect(addr).await?;
    s.write_all(
        format!("POST {path} HTTP/1.1\r\nHost: cairn\r\nContent-Length: 0\r\n\r\n").as_bytes(),
    )
    .await?;
    s.flush().await?;
    Ok(s)
}

/// **A client disconnecting mid split/merge does not leave the source range stuck.**
///
/// Interleaving:
/// 1. Node A alone, leading every range.
/// 2. A client `POST /ranges/0/split` (or `/ranges/merge?left=0&right=1`) on a raw
///    socket. The coordinator goes through intent, flush, quiesce (the source closes its
///    Db, keeps the lease), clones the left child (or the union) — then **stops** at
///    `split::cloneLeft` (`merge::union`), only on A.
/// 3. The client closes the socket (out of patience) while the coordinator is stopped.
/// 4. Release the coordinator; the children's owner claim is delayed by 200 ms (as on S3).
///
/// Must see: within the time cap, the table on the bucket has no intent left (the
/// operation completed or rolled back), every range of the table has a leader on A, and
/// `GET /ranges/ops` lists that operation with its final state. Before the fix: hyper
/// dropped the handler's future at the first `await` after the release — nobody rolled
/// back, the intent stayed, the source in `quiesced()` kept the lease and returned
/// `WrongRange` forever.
async fn client_disconnect_mid_op_never_strands_the_source(op: RangeOp) -> Result<()> {
    use std::time::Duration;

    let ctl = StepController::new();
    let (node_id, ranges, point, path) = match op {
        RangeOp::Split => (
            "l1-split-a",
            1,
            "split::cloneLeft",
            "/ranges/0/split".to_string(),
        ),
        RangeOp::Merge => (
            "l1-merge-a",
            2,
            "merge::union",
            "/ranges/merge?left=0&right=1".to_string(),
        ),
    };
    let bucket = common::bucket();
    let a = common::start_cluster_node(&bucket, node_id, ranges, Duration::from_secs(1)).await?;
    let r = within(
        Duration::from_secs(60),
        "client disconnects mid-operation",
        async {
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "A is leader of every range",
                |p| p.len() == ranges as usize && p.values().all(|v| v == &[0]),
            )
            .await?;
            // (2)
            let paused = ctl.pause_at_on(&[node_id], point);
            let socket = raw_post(a.node.addr, &path).await?;
            paused.expect_stopped(TEST_DEADLINE).await;
            ensure!(
                a.node
                    .cluster
                    .table()
                    .ranges()
                    .iter()
                    .any(|r| r.intent.is_some()),
                "{op:?}: stopped at {point} but the table has no intent yet"
            );
            // (3) The client leaves while the coordinator is stopped.
            drop(socket);
            // The in-memory store answers at once, so from the stop to the commit CAS
            // there may be no `await` that actually blocks — on S3 there is. Delay the
            // children's owner claim: the handler's future (if still tied to the request)
            // is dropped there, **before** the commit.
            a.store.add_rule(cairn_store::testkit::Rule::new(
                cairn_store::testkit::OpSet::PUT,
                cairn_store::testkit::PathMatch::Contains("owners/range-".into()),
                cairn_store::testkit::Action::Delay(Duration::from_millis(200)),
            ));
            // (4)
            paused.resume();

            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            let table = loop {
                a.node.cluster.reload().await?;
                let t = a.node.cluster.table();
                if t.ranges().iter().all(|r| r.intent.is_none()) {
                    break t;
                }
                ensure!(
                    std::time::Instant::now() < deadline,
                    "{op:?}: intent stuck on the bucket after the client disconnected: {t:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            let want: Vec<u32> = table.partitions().map(|p| p.0).collect();
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "every range of the table has a leader on A",
                |p| want.iter().all(|r| p.get(r).is_some_and(|v| v == &[0])),
            )
            .await
            .with_context(|| format!("{op:?}: source stuck after client left (table {table:?})"))?;
            // The node's operation log lists this one, with its final state.
            let (code, ops) =
                call(Client::new().get(format!("{}/ranges/ops", a.node.url()))).await?;
            ensure!(code == StatusCode::OK, "GET /ranges/ops: {code} {ops}");
            let list = ops["ops"].as_array().ok_or_else(|| anyhow!("{ops}"))?;
            ensure!(
                list.len() == 1,
                "{op:?}: the operation log must have exactly one operation: {ops}"
            );
            let state = list[0]["state"].as_str().unwrap_or_default();
            ensure!(
                state == "committed" || state == "failed",
                "{op:?}: final state {state}: {ops}"
            );
            let id = list[0]["id"].as_str().unwrap_or_default();
            let (code, one) =
                call(Client::new().get(format!("{}/ranges/ops/{id}", a.node.url()))).await?;
            ensure!(
                code == StatusCode::OK && one["state"] == list[0]["state"],
                "GET /ranges/ops/{id}: {code} {one}"
            );
            Ok(())
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_disconnect_mid_split_never_strands_the_source() -> Result<()> {
    client_disconnect_mid_op_never_strands_the_source(RangeOp::Split).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_disconnect_mid_merge_never_strands_the_sources() -> Result<()> {
    client_disconnect_mid_op_never_strands_the_source(RangeOp::Merge).await
}

/// **A merge failing before quiesce whose first rollback also fails does not leave an
/// orphaned intent.**
///
/// Interleaving:
/// 1. Node A alone, two ranges, leading both.
/// 2. Only on A: `merge::gather` = `return` (gathering fails, nothing quiesced yet) and
///    `split::roll_back` = `1*return` (the first rollback fails — the shape of a 503 on
///    `cluster.conf`).
/// 3. A merges 0 + 1: the intent goes onto the table, gathering fails, the coordinator
///    rolls back, the rollback fails.
///
/// Must see: within the time cap, the table on the bucket has no intent left, and both
/// ranges still have a leader. Before the fix: the failed rollback only left an
/// `error!` line saying the next node to claim the range would roll back; no handle had
/// quiesced so nobody `Resign`ed, both ranges still had a leader, no `factory` ran —
/// the intent stayed until the next leader change, and auto split stopped cluster-wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_failing_before_quiesce_with_failed_roll_back_leaves_no_intent() -> Result<()> {
    use {cairn_store::PartitionId, std::time::Duration};

    let ctl = StepController::new();
    let node_id = "l4-a";
    let bucket = common::bucket();
    let a = common::start_cluster_node(&bucket, node_id, 2, Duration::from_secs(1)).await?;
    let r = within(
        Duration::from_secs(60),
        "merge fails before quiesce, rollback fails",
        async {
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "A is leader of every range",
                |p| p.len() == 2 && p.values().all(|v| v == &[0]),
            )
            .await?;
            let _gather = ctl.fail_on(&[node_id], "merge::gather", "return(chaos)");
            let _undo = ctl.fail_on(&[node_id], "split::roll_back", "1*return(chaos)");
            let ops = a.node.ranges.clone();
            let done = cairn_store::failpoint::in_node(node_id, async move {
                ops.merge(PartitionId(0), PartitionId(1)).await.map(|_| ())
            })
            .await;
            ensure!(done.is_err(), "merge must fail with merge::gather on");
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            loop {
                a.node.cluster.reload().await?;
                let t = a.node.cluster.table();
                if t.ranges().iter().all(|r| r.intent.is_none()) {
                    break;
                }
                ensure!(
                    std::time::Instant::now() < deadline,
                    "orphaned merge intent on the bucket — auto split stops cluster-wide: {t:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "both ranges still have a leader",
                |p| p.len() == 2 && p.values().all(|v| v == &[0]),
            )
            .await?;
            Ok(())
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

/// **SM-15 — a quiesced source range is tied to the coordinator's lifetime.** An engine
/// in `quiesced()` only leaves on a command or on losing the lease; a coordinator that
/// vanished without `Retire`/`Resign` left the source with its Db closed, holding the
/// lease and returning `WrongRange` forever.
///
/// Interleaving:
/// 1. Node A alone, one range, leading it.
/// 2. Only on A: `split::cloneLeft` = `panic`. A splits 0: intent, flush, quiesce (the
///    source keeps the lease), clone of the left child — then the coordinator
///    **panics**.
///
/// Must see: the call fails (does not hang); within the time cap the table has no intent
/// left, range 0 has a leader on A, the left child has no Db or owner record left (the
/// holding wire snaps ⇒ the engine releases and claims again ⇒ `factory` sees an intent
/// nobody coordinates ⇒ rolls back); the operation log records `failed`. Before the fix:
/// a panic in the handler, the guard dropped, but nothing sent `Resign` to the quiesced
/// engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_panic_after_quiesce_releases_and_rolls_back_the_source() -> Result<()> {
    use {cairn_store::PartitionId, std::time::Duration};

    let ctl = StepController::new();
    let node_id = "sm15-panic-a";
    let bucket = common::bucket();
    let a = common::start_cluster_node(&bucket, node_id, 1, Duration::from_secs(1)).await?;
    let r = within(
        Duration::from_secs(60),
        "coordinator panics after quiesce",
        async {
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "A is leader of range 0",
                |p| p.get(&0) == Some(&vec![0]),
            )
            .await?;
            let t = a.node.cluster.table();
            let (left, right) = (
                PartitionId(t.next_range_id()),
                PartitionId(t.next_range_id() + 1),
            );
            let _panic = ctl.fail_on(&[node_id], "split::cloneLeft", "panic");
            // Any error will do — a `500`, or a broken connection — as long as it does not succeed.
            if let Ok((code, body)) = post_json(format!("{}/ranges/0/split", a.node.url())).await {
                ensure!(!code.is_success(), "split must fail (panic): {code} {body}");
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            loop {
                a.node.cluster.reload().await?;
                let t = a.node.cluster.table();
                if t.ranges().iter().all(|r| r.intent.is_none()) {
                    ensure!(t.contains(PartitionId(0)), "{t:?}");
                    break;
                }
                ensure!(
                    std::time::Instant::now() < deadline,
                    "intent stuck after the coordinator panicked: {t:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "range 0 has a leader again",
                |p| p.get(&0) == Some(&vec![0]),
            )
            .await?;
            let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
                bucket.clone(),
                "",
                "memory://",
            ));
            for c in [left, right] {
                ensure!(
                    !stores.range_db_exists(c).await?,
                    "the Db of child {} is still there after the rollback",
                    c.0
                );
                ensure!(
                    owner_of(&bucket, c.0).await?.is_none(),
                    "the owner record of child {} is still there",
                    c.0
                );
            }
            let (code, ops) =
                call(Client::new().get(format!("{}/ranges/ops", a.node.url()))).await?;
            ensure!(
                code == StatusCode::OK && ops["ops"][0]["state"] == "failed",
                "{code} {ops}"
            );
            Ok(())
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

/// **SM-15 — the intent reconciliation round does not roll back an intent that a
/// coordinator on another node still holds.**
///
/// Interleaving:
/// 1. Two nodes A, B on one bucket, two ranges; the D-d runner's round every 200 ms.
///    Wait until each node leads exactly one range: the leader of range 0 is the runner
///    R, the other node is O.
/// 2. O splits its range and **stops** at `split::cloneLeft` (only on O): the intent is
///    on the table, the source quiesced.
/// 3. R reconciles: an intent on O's range; it asks O through `GET
///    /internal/ranges/{id}/coordinator` — O is holding it.
/// 4. After three rounds of R seeing "held" (callback `split::reconcile::held` counting
///    through a channel), release O.
///
/// Must see: the tripwire `split::reconcile::roll_back` (set to `panic`) does not trip,
/// and O's split commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciler_never_rolls_back_an_intent_a_live_coordinator_holds() -> Result<()> {
    use {cairn_store::PartitionId, std::time::Duration};

    let ctl = StepController::new();
    let _tripwire = ctl.fail("split::reconcile::roll_back", "panic");
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.retire_orphans_every = Some(Duration::from_millis(200));
        cfg.reconcile_intents_every = Some(Duration::from_millis(200));
    };
    let a = common::start_cluster_node_tweaked(
        &bucket,
        "sm15-held-a",
        2,
        Duration::from_secs(1),
        tweak,
    )
    .await?;
    let b = common::start_cluster_node_tweaked(
        &bucket,
        "sm15-held-b",
        2,
        Duration::from_secs(1),
        tweak,
    )
    .await?;
    let nodes = [&a, &b];
    let r = within(
        Duration::from_secs(90),
        "reconciliation does not roll back a held intent",
        async {
            let probe = common::wait_probe(
                &nodes,
                Duration::from_secs(40),
                "each node leads one range",
                |p| common::single_leaders(p).is_some_and(|l| l.len() == 2 && l[&0] != l[&1]),
            )
            .await?;
            let leaders = common::single_leaders(&probe).ok_or_else(|| anyhow!("{probe:?}"))?;
            let other = nodes[leaders[&1]];
            let other_id = other.node.node_id.clone();

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let _held =
                cairn_store::testkit::FailGuard::callback("split::reconcile::held", move || {
                    let _ = tx.send(());
                });
            // (2)
            let paused = ctl.pause_at_on(&[other_id.as_str()], "split::cloneLeft");
            let ops = other.node.ranges.clone();
            let coordinator = other_id.clone();
            let split = tokio::spawn(async move {
                cairn_store::failpoint::in_node(&coordinator, async move {
                    ops.start(cairn_server::split::OpKind::Split {
                        range: PartitionId(1),
                        at: None,
                    })
                    .wait()
                    .await
                })
                .await
            });
            paused.expect_stopped(TEST_DEADLINE).await;
            // (3)–(4)
            for i in 0..3 {
                tokio::time::timeout(Duration::from_secs(10), rx.recv())
                    .await
                    .map_err(|_| anyhow!("runner did not reconcile the held intent ({i})"))?;
            }
            paused.resume();
            let (table, _) = split.await?.map_err(|e| anyhow!("O's split failed: {e}"))?;
            ensure!(
                !table.contains(PartitionId(1)),
                "O's split must commit: {table:?}"
            );
            Ok(())
        },
    )
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

// ─────────────── a merge meeting an intent just rolled back by someone else returns 503

/// **A merge whose source another node claims and whose intent it rolls back mid-gather
/// returns `503`, not `500`** (seen in a `--slow-node` chaos run: merge 27+28 returned
/// `500` "could not gather range 28").
///
/// Interleaving:
/// 1. Two nodes, two ranges; wait until each node leads exactly one range: the leader
///    of range 0 is coordinator A, of range 1 is B.
/// 2. Every range claim of A (`session::claim`, only on A) **is held** — the shape of a
///    slow node: A's owner record PUT/GETs do not come back.
/// 3. A client `POST /ranges/merge?left=0&right=1` on A: intent CAS, asks B to release
///    range 1 (B releases), then `gather_here`'s claim of range 1 is held.
/// 4. Range 1 stays vacant past the rescue grace ⇒ B's balancer takes it back, sees an
///    intent B does not coordinate ⇒ **rolls the intent back** (SM-3, by design): the
///    table has no intent left, B leads range 1.
/// 5. Release A's claims: `gather_here` sees range 1 with a live owner, B.
///
/// Must see: the merge returns `503` (a legitimate race — retryable); `GET
/// /ranges/ops/{id}` records `failed` with `error.kind == "busy"`; the table has no
/// intent left, A no longer holds any range (`coordinates`), each range has exactly one
/// leader (no stuck quiesce); and resending the same merge commits. Before the fix:
/// `gather_here` waited out `settle` and returned `RangeOpError::Failed` — `500`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn merge_whose_source_is_taken_and_rolled_back_mid_gather_is_503() -> Result<()> {
    use {
        cairn_store::PartitionId,
        std::{
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc,
            },
            time::Duration,
        },
    };

    let _ctl = StepController::new();
    let bucket = common::bucket();
    let a = common::start_cluster_node(&bucket, "r1h-a", 2, Duration::from_secs(1)).await?;
    let b = common::start_cluster_node(&bucket, "r1h-b", 2, Duration::from_secs(1)).await?;
    let nodes = [&a, &b];
    let hold = Arc::new(AtomicBool::new(false));
    let r = within(
        Duration::from_secs(90),
        "merge meeting an intent rolled back by someone else returns 503",
        async {
            // (1)
            let probe = common::wait_probe(
                &nodes,
                Duration::from_secs(40),
                "each node leads one range",
                |p| common::single_leaders(p).is_some_and(|l| l.len() == 2 && l[&0] != l[&1]),
            )
            .await?;
            let leaders = common::single_leaders(&probe).ok_or_else(|| anyhow!("{probe:?}"))?;
            let (coord, other) = (nodes[leaders[&0]], nodes[leaders[&1]]);
            let coord_id = coord.node.node_id.clone();

            // (2) The callback blocks the thread until `hold` is off; it reports each hold through a channel.
            let (tx, mut held) = tokio::sync::mpsc::unbounded_channel();
            let gate = hold.clone();
            let _claims = cairn_store::testkit::FailGuard::callback("session::claim", move || {
                if !gate.load(Ordering::SeqCst) {
                    return;
                }
                let _ = tx.send(());
                let wait = || {
                    let t0 = std::time::Instant::now();
                    while gate.load(Ordering::SeqCst) && t0.elapsed() < Duration::from_secs(60) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                };
                match tokio::runtime::Handle::try_current() {
                    Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                        tokio::task::block_in_place(wait)
                    }
                    _ => wait(),
                }
            })
            .on_nodes("session::claim", &[coord_id.as_str()]);
            hold.store(true, Ordering::SeqCst);

            // (3)
            let merge_url = format!("{}/ranges/merge?left=0&right=1", coord.node.url());
            let merge = tokio::spawn(post_json(merge_url.clone()));
            tokio::time::timeout(Duration::from_secs(20), held.recv())
                .await
                .map_err(|_| anyhow!("A's gather did not reach the range claim step"))?;

            // (4)
            let t0 = std::time::Instant::now();
            loop {
                other.node.cluster.reload().await?;
                let clean = other
                    .node
                    .cluster
                    .table()
                    .ranges()
                    .iter()
                    .all(|r| r.intent.is_none());
                let led = common::leaders_by_probe(&[other])
                    .await
                    .get(&1)
                    .is_some_and(|v| v == &[0]);
                if clean && led {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(30),
                    "B has not taken range 1 back and rolled back the intent"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // (5)
            hold.store(false, Ordering::SeqCst);
            let (code, body) = merge.await??;
            ensure!(
                code == StatusCode::SERVICE_UNAVAILABLE,
                "a merge meeting an intent rolled back by someone else must be 503: {code} {body}"
            );

            let (code, ops) =
                call(Client::new().get(format!("{}/ranges/ops", coord.node.url()))).await?;
            ensure!(code == StatusCode::OK, "GET /ranges/ops: {code} {ops}");
            let op = ops["ops"]
                .as_array()
                .and_then(|l| l.iter().rev().find(|o| o["kind"] == "merge"))
                .ok_or_else(|| anyhow!("the operation log has no merge: {ops}"))?;
            let id = op["id"].as_str().unwrap_or_default();
            let (code, one) =
                call(Client::new().get(format!("{}/ranges/ops/{id}", coord.node.url()))).await?;
            ensure!(code == StatusCode::OK, "GET /ranges/ops/{id}: {code} {one}");
            ensure!(
                one["state"] == "failed" && one["error"]["kind"] == "busy",
                "the log must record failed/busy: {one}"
            );

            coord.node.cluster.reload().await?;
            let table = coord.node.cluster.table();
            ensure!(
                table.ranges().iter().all(|r| r.intent.is_none()),
                "orphaned intent: {table:?}"
            );
            for p in [PartitionId(0), PartitionId(1)] {
                ensure!(table.contains(p), "range {} left the table: {table:?}", p.0);
                ensure!(
                    !coord.node.ranges.coordinates(p),
                    "A still holds range {} after the failed merge",
                    p.0
                );
            }
            common::wait_probe(
                &nodes,
                Duration::from_secs(20),
                "each range exactly one leader",
                |p| {
                    common::single_leaders(p)
                        .is_some_and(|l| l.contains_key(&0) && l.contains_key(&1))
                },
            )
            .await?;

            // Retryable: the same command, this time it commits.
            let (code, view) = post_until_settled(merge_url).await?;
            ensure!(code == StatusCode::OK, "re-merge after 503: {code} {view}");
            Ok(())
        },
    )
    .await;
    hold.store(false, Ordering::SeqCst);
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

// ─────────────────── a session lost while claiming a child ⇒ 503

/// How the coordinator's session breaks in the window after the CAS of a child's owner
/// record ([`session_lost_while_claiming_a_child_is_503`]).
#[derive(Clone, Copy, Debug)]
enum SessionFault {
    /// A renewal missed past the deadline and the session stepped down: the incarnation
    /// is still there but `is_valid()` is off (the pause window) — the shape seen in a
    /// chaos run.
    Paused,
    /// A renewal missed past the pause cap: the session is lost, the node opens a new incarnation.
    Lost,
}

/// **A merge/split whose coordinator's session is lost (or paused) exactly while
/// claiming a child range returns `503`, not `500`** (seen in a chaos run with
/// `--failpoints --gc`, `CHAOS_SPLIT_EVERY_S=60`: merge 157+158 returned
/// `500 {"error":"internal node error …"}`, and the log said the rollback could not
/// claim child range 160: `None`).
///
/// Interleaving:
/// 1. Node A alone, `direct` renewal (PUT — a lone node in `solo` renews by GET, not
///    through `session::renew::before_put`), leading every range.
/// 2. `POST /ranges/merge?left=0&right=1` (or `POST /ranges/0/split`) stops at
///    `merge::union` (`split::cloneRight`), only on A: the child Db exists, not claimed.
/// 3. Set the stop point `session::claim::after_write` (only on A), then release step
///    2: `claim_children` CASes `Create` of the child's owner record (pointing at the
///    current incarnation) and stops right after, before the "session lost while
///    writing" check.
/// 4. `session::renew::before_put` = `return`, only on A: wait until A's session pauses
///    (`Paused`: the old incarnation, validity off) or is lost for good (`Lost`: a new
///    incarnation).
/// 5. Release the claim: `claim_with` sees the session changed/invalid ⇒
///    `Claim::Held(None)`; the coordinator rolls back the intent and discards the
///    child.
///
/// Must see: the call returns `503` (temporary — retryable), the operation log records
/// `failed`/`busy`, the table has no intent left, the child's owner record (pointing at
/// an abandoned or paused incarnation) has been cleaned up, resending the same command
/// commits, and `verify` on the bucket at cleanup is clean. Before the fix:
/// `claim_children` mapped every `Held` to `RangeOpError::Failed` — `500`.
async fn session_lost_while_claiming_a_child_is_503(
    op: RangeOp,
    fault: SessionFault,
) -> Result<()> {
    use {cairn_store::PartitionId, std::time::Duration};

    let ctl = StepController::new();
    let node_id = match (op, fault) {
        (RangeOp::Merge, SessionFault::Paused) => "l9-merge-paused",
        (RangeOp::Merge, SessionFault::Lost) => "l9-merge-lost",
        (RangeOp::Split, SessionFault::Paused) => "l9-split-paused",
        (RangeOp::Split, SessionFault::Lost) => "l9-split-lost",
    };
    let ranges: u16 = match op {
        RangeOp::Split => 1,
        RangeOp::Merge => 2,
    };
    let bucket = common::bucket();
    let a = common::start_cluster_node_tweaked(
        &bucket,
        node_id,
        ranges,
        Duration::from_secs(2),
        common::direct,
    )
    .await?;
    let r = within(
        Duration::from_secs(120),
        "session lost while claiming a child returns 503",
        async {
            // (1)
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "A is leader of every range",
                |p| p.len() == ranges as usize && p.values().all(|v| v == &[0]),
            )
            .await?;
            let session = a.node.session.clone();
            let url = match op {
                RangeOp::Split => format!("{}/ranges/0/split", a.node.url()),
                RangeOp::Merge => format!("{}/ranges/merge?left=0&right=1", a.node.url()),
            };

            // (2)
            let before_claim = match op {
                RangeOp::Split => "split::cloneRight",
                RangeOp::Merge => "merge::union",
            };
            let union = ctl.pause_at_on(&[node_id], before_claim);
            let pending = tokio::spawn(post_json(url.clone()));
            union.expect_stopped(TEST_DEADLINE).await;
            a.node.cluster.reload().await?;
            let children: Vec<u32> = match a.node.cluster.table().range(PartitionId(0)) {
                Some(e) => match e.intent {
                    Some(cairn_store::RangeIntent::Splitting { left, right, .. }) => {
                        vec![left.0, right.0]
                    }
                    Some(cairn_store::RangeIntent::Merging { into, .. }) => vec![into.0],
                    None => anyhow::bail!("range 0 has no intent while stopped at {before_claim}"),
                },
                None => anyhow::bail!("range 0 left the table while stopped at {before_claim}"),
            };

            // (3)
            let claimed = ctl.pause_at_on(&[node_id], "session::claim::after_write");
            union.resume();
            claimed.expect_stopped(TEST_DEADLINE).await;
            let old = session
                .current()
                .ok_or_else(|| anyhow!("A has no incarnation"))?;
            ensure!(
                owner_of(&bucket, children[0])
                    .await?
                    .is_some_and(|r| r.session == old),
                "the owner record of child {} does not point at the current incarnation yet",
                children[0]
            );

            // (4)
            let renew = ctl.fail_on(&[node_id], "session::renew::before_put", "return");
            let live = session.live_changes();
            within(Duration::from_secs(20), "A's session breaks", async {
                loop {
                    let now = live.borrow().clone();
                    match (fault, now) {
                        (SessionFault::Paused, Some((id, v))) if id == old && !v.is_valid() => {
                            return Ok(())
                        }
                        (SessionFault::Paused, Some((id, _))) if id != old => {
                            anyhow::bail!("session lost for good before the pause window was seen")
                        }
                        (SessionFault::Lost, Some((id, _))) if id != old => return Ok(()),
                        _ => {}
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await?;

            // (5)
            claimed.resume();
            let (code, body) = within(TEST_DEADLINE, "the call gets an answer", async {
                pending.await?
            })
            .await?;
            drop(renew);
            ensure!(
                code == StatusCode::SERVICE_UNAVAILABLE,
                "a session lost while claiming a child must be 503: {code} {body}"
            );

            let (code, ops) = call_get(format!("{}/ranges/ops", a.node.url())).await?;
            ensure!(code == StatusCode::OK, "GET /ranges/ops: {code} {ops}");
            let kind = match op {
                RangeOp::Split => "split",
                RangeOp::Merge => "merge",
            };
            let entry = ops["ops"]
                .as_array()
                .and_then(|l| l.iter().rev().find(|o| o["kind"] == kind))
                .ok_or_else(|| anyhow!("the operation log has no {kind}: {ops}"))?;
            let id = entry["id"].as_str().unwrap_or_default();
            let (code, one) = call_get(format!("{}/ranges/ops/{id}", a.node.url())).await?;
            ensure!(code == StatusCode::OK, "GET /ranges/ops/{id}: {code} {one}");
            ensure!(
                one["state"] == "failed" && one["error"]["kind"] == "busy",
                "the log must record failed/busy: {one}"
            );

            a.node.cluster.reload().await?;
            let table = a.node.cluster.table();
            ensure!(
                table.ranges().iter().all(|r| r.intent.is_none()),
                "orphaned intent: {table:?}"
            );
            // The child is abandoned: its owner record (pointing at an abandoned or paused
            // incarnation) goes with its Db — the rollback's child cleanup, or the orphan child round.
            within(
                Duration::from_secs(30),
                "the child's owner record cleaned up",
                async {
                    loop {
                        let mut left = Vec::new();
                        for c in &children {
                            if owner_of(&bucket, *c).await?.is_some() {
                                left.push(*c);
                            }
                        }
                        if left.is_empty() {
                            return Ok(());
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
            )
            .await?;

            // Retryable: the same command, this time it commits.
            let (code, view) = post_until_settled(url).await?;
            ensure!(code == StatusCode::OK, "{kind} after 503: {code} {view}");
            Ok(())
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

/// GET `url`: the HTTP code and the JSON body.
async fn call_get(url: String) -> Result<(StatusCode, Value)> {
    call(Client::new().get(url)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn merge_whose_session_pauses_while_claiming_the_child_is_503() -> Result<()> {
    session_lost_while_claiming_a_child_is_503(RangeOp::Merge, SessionFault::Paused).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn merge_whose_session_is_lost_while_claiming_the_child_is_503() -> Result<()> {
    session_lost_while_claiming_a_child_is_503(RangeOp::Merge, SessionFault::Lost).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn split_whose_session_pauses_while_claiming_a_child_is_503() -> Result<()> {
    session_lost_while_claiming_a_child_is_503(RangeOp::Split, SessionFault::Paused).await
}

// ─────────────────── turn/actor safety nets

/// `POST /invocations/{id}/cancel` (a hard cancel, 572): the HTTP code and the body.
async fn admin_cancel(url: &str, id: &str) -> Result<(StatusCode, Value)> {
    call(Client::new().post(format!("{url}/invocations/{id}/cancel"))).await
}

/// **An admin cancel failing at the queue removal step must not leave the VO lock
/// stuck.**
///
/// Interleaving:
/// 1. Node E alone, one range. `Obj/hold` (EXCLUSIVE) with key `k` suspends waiting for
///    a signal: `suspended`, holding the VO lock of `Obj/k`.
/// 2. `turn::release_queue_slot` = `1*return`, only on E.
/// 3. Hard cancel `hold`: the terminal commit (572) succeeds, the queue removal commit
///    fails.
/// 4. `Obj/quick` with the same key `k`.
///
/// Must see: the cancel returns `200` (it did succeed), and `quick` is `completed`
/// within the cap. Before the fix `?` returned before `release_lock`: the client got a
/// 500 for a cancel that had succeeded, the VO lock stayed in RAM until the end of the
/// term, and `quick` waited forever — cancelling again returned `AlreadyFinished` before
/// the release step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_cancel_failing_at_dequeue_frees_the_object() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend])
        .object("Obj", "quick", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let e = start_led_alone(&bucket, "cancel-dequeue-e").await?;
    let url = e.node.url();
    common::register(&url, &sdk.url).await?;
    let (code, body) = common::send(&url, "Obj", "hold", Some("k"), "{}").await?;
    ensure!(code == 200 || code == 202, "{code} {body}");
    let held = body["id"].as_str().unwrap_or_default().to_string();
    within(TEST_DEADLINE, "hold suspended", async {
        common::wait_status(&url, &held, "suspended").await?;
        Ok(())
    })
    .await?;

    let _fp = ctl.fail_on(
        &["cancel-dequeue-e"],
        "turn::release_queue_slot",
        "1*return",
    );
    let (cancel_code, cancel_body) = admin_cancel(&url, &held).await?;
    let (code, body) = common::send(&url, "Obj", "quick", Some("k"), "{}").await?;
    ensure!(code == 200 || code == 202, "{code} {body}");
    let quick = body["id"].as_str().unwrap_or_default().to_string();
    let done = within(Duration::from_secs(10), "quick completed", async {
        common::wait_status(&url, &quick, "completed").await?;
        Ok(())
    })
    .await;
    e.node.shutdown().await;
    done?;
    ensure!(
        cancel_code == StatusCode::OK,
        "a cancel that succeeded must return 200: {cancel_code} {cancel_body}"
    );
    Ok(())
}

/// **An undecodable inbound row is not deleted, and whoever waits for it gets an
/// outcome** (asserting the negative space: what must never happen is checked too).
///
/// Interleaving:
/// 1. A lone node, two ranges. `Caller/run` (range 0) calls `Callee/echo` (range 1)
///    and waits for the result.
/// 2. `turn::drain_inbound::corrupt` = `return`: every inbound row of type
///    `Completion` the drainer reads comes out as garbage bytes — the shape of a row
///    written by a newer binary, or damaged on the store.
/// 3. The callee finishes, the result reaches range 0's inbound and sits there,
///    undecodable.
///
/// Must see: the caller `paused` with code 577 (`INFRA_STUCK`) within the cap — not
/// silently `suspended` forever. Remove the point, then `resume`: the caller is
/// `completed` with the callee's result — the row was **kept**, not deleted. Before the
/// fix the drainer deleted the row and the caller waited for a completion that never
/// came.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undecodable_inbound_row_is_kept_and_pauses_its_waiter() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let node = common::start_led(common::solo_config(2)).await?;
    let url = node.url();
    let callee_key = common::key_on_partition(&node, "Callee", 1, "callee");
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some(&callee_key)),
                Step::EchoCallResult,
            ],
        )
        .object("Callee", "echo", vec![Step::output("pong")])
        .spawn()
        .await?;
    let r = async {
        common::register(&url, &sdk.url).await?;
        let corrupt = ctl.fail("turn::drain_inbound::corrupt", "return");
        let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let caller = body["id"].as_str().unwrap_or_default().to_string();
        let st = within(Duration::from_secs(15), "caller paused", async {
            common::wait_status(&url, &caller, "paused").await
        })
        .await?;
        ensure!(st["code"] == 577, "paused code must be INFRA_STUCK: {st}");
        drop(corrupt);
        let (code, body) =
            call(Client::new().post(format!("{url}/invocations/{caller}/resume"))).await?;
        ensure!(code == StatusCode::OK, "resume: {code} {body}");
        let st = within(
            Duration::from_secs(15),
            "caller completed after resume",
            async { common::wait_status(&url, &caller, "completed").await },
        )
        .await?;
        ensure!(st["output"] == "pong", "{st}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// **A node's refresh loop does not stall behind an engine that has not answered.**
///
/// Interleaving:
/// 1. A two-node cluster with auth, one range: A leads, B follows. `--deploy-refresh`
///    200 ms. An `invoke` key K is created through B; A gets K after one refresh tick.
/// 2. `actor::recover` stops, only on A.
/// 3. Register a deployment through B. A's refresh loop sees it and reports
///    `DeploymentsChanged` to range 0's engine — that command runs `recover`, which
///    stops at (2).
/// 4. Delete K through B.
///
/// Must see: A rejects K (`401`) within the cap — the refresh loop moves on even though
/// one range's engine has not answered. Before the fix the loop stood at
/// `deployments_changed()` (the command only answered after a complete `recover`, with
/// no cap) so A's key registry never refreshed: revoking a key had no effect on A.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_revocation_reaches_a_node_whose_engine_is_stuck_in_recover() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(200);
    };
    let ttl = Duration::from_secs(2);
    let a = common::start_cluster_node_tweaked(&bucket, "refresh-a", 1, ttl, tweak).await?;
    common::wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A is leader of range 0",
        |p| p.get(&0) == Some(&vec![0]),
    )
    .await?;
    let b = common::start_cluster_node_tweaked(&bucket, "refresh-b", 1, ttl, tweak).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let (a_url, b_url) = (a.node.url(), b.node.url());
    let http = Client::new();
    let r = async {
        let (code, key) = call(
            http.post(format!("{b_url}/admin/keys"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "tenant": "default", "role": "invoke" })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "create key: {code} {key}");
        let (key_id, secret) = (
            key["id"].as_str().unwrap_or_default().to_string(),
            key["secret"].as_str().unwrap_or_default().to_string(),
        );
        // A's code for K on one call path (a non-existent service ⇒ 404, not 401).
        let a_says = |secret: String| {
            let req = http
                .post(format!("{a_url}/invoke/NoSuchService/run"))
                .bearer_auth(secret);
            async move { Ok::<_, anyhow::Error>(req.send().await?.status()) }
        };
        within(Duration::from_secs(10), "A gets K", async {
            while a_says(secret.clone()).await? == StatusCode::UNAUTHORIZED {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        })
        .await?;

        let paused = ctl.pause_at_on(&["refresh-a"], "actor::recover");
        let (code, body) = call(
            http.post(format!("{b_url}/deployments"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "uri": sdk.url })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "register deployment: {code} {body}");
        paused.expect_stopped(Duration::from_secs(10)).await;
        let (code, body) = call(
            http.delete(format!("{b_url}/admin/keys/{key_id}"))
                .bearer_auth(ADMIN_KEY),
        )
        .await?;
        ensure!(code.is_success(), "delete key: {code} {body}");
        let revoked = within(Duration::from_secs(8), "A rejects the deleted K", async {
            while a_says(secret.clone()).await? != StatusCode::UNAUTHORIZED {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        })
        .await;
        paused.resume();
        revoked
    }
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// Like [`start_led_alone`], with the config further adjusted by `tweak`.
async fn start_led_alone_with(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    id: &str,
    tweak: impl FnOnce(&mut cairn_server::node::NodeConfig),
) -> Result<common::ClusterNode> {
    use std::time::Duration;
    let n =
        common::start_cluster_node_tweaked(bucket, id, 1, Duration::from_secs(2), tweak).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(20),
        "node is leader of range 0",
        |p| p.get(&0) == Some(&vec![0]),
    )
    .await?;
    Ok(n)
}

fn rescued_total() -> u64 {
    cairn_server::metrics::flow::RESCUED.load(std::sync::atomic::Ordering::Relaxed)
}

fn nacked_total() -> u64 {
    cairn_server::metrics::flow::RESCUE_NACKED.load(std::sync::atomic::Ordering::Relaxed)
}

/// **(a) — the rescue net backs off per id.**
///
/// Interleaving:
/// 1. Node F alone, one range. `turn::ensure_queued` and
///    `turn::schedule_after_error` = `return` (every time), only on F: every turn fails
///    **before** the point where it reschedules, and the rescheduling itself fails too
///    — a deterministic failure that only the rescue net still wakes.
/// 2. `/send` one invocation. The first turn fails ⇒ the net is marked; the net
///    redispatches; the turn fails again ⇒ marked again; …
/// 3. Count how many times the net redispatches in 6 s.
///
/// Must see: the gap between two redispatches of **the same id** doubles (0.5 s, 1 s,
/// 2 s, … with jitter ⇒ at most 4 times in 6 s; a threshold of 6 for slow machines).
/// Before the fix every clean round brought the backoff back to 0.5 s, so a
/// deterministically failing id was redispatched every 0.25–0.5 s forever (12–24 times
/// in 6 s) — the net amplified the load exactly while the store was failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescue_backs_off_per_invocation() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let f = start_led_alone(&bucket, "rescue-backoff-f").await?;
    common::register(&f.node.url(), &sdk.url).await?;
    let _q = ctl.fail_on(&["rescue-backoff-f"], "turn::ensure_queued", "return");
    let _s = ctl.fail_on(
        &["rescue-backoff-f"],
        "turn::schedule_after_error",
        "return",
    );
    let before = rescued_total();
    let ids = send_n(&f.node.url(), "Echo", "run", 1).await?;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let requeued = rescued_total() - before;
    f.node.shutdown().await;
    ensure!(ids.len() == 1);
    ensure!(
        requeued >= 1,
        "the net must redispatch at least once: {requeued}"
    );
    ensure!(
        requeued <= 6,
        "a deterministically failing id was redispatched {requeued} times in 6 s — the net does not back off per id"
    );
    Ok(())
}

/// **(c) — over the threshold of consecutive infrastructure failures ⇒ `paused` 577.**
///
/// Interleaving:
/// 1. Node G alone, the threshold lowered to 3 consecutive marks.
///    `turn::ensure_queued` and `turn::schedule_after_error` = `return` (every
///    time), only on G.
/// 2. `/send` one invocation: the first turn fails before the point where it
///    reschedules; the net redispatches; it fails again; …
///
/// Must see: the invocation `paused` with code 577 (`INFRA_STUCK`) within the cap — the
/// net stops spinning. Remove the points, then `resume`: `completed`, exactly one turn
/// reaching the SDK. Before the fix the net redispatched forever, silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocation_stuck_on_infra_is_paused_577() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let g = start_led_alone_with(&bucket, "rescue-stuck-g", |cfg| {
        cfg.limits.infra_stuck = cairn_server::drainer::InfraStuck {
            strikes: 3,
            after: Duration::from_secs(3600),
        };
    })
    .await?;
    let url = g.node.url();
    let r = async {
        common::register(&url, &sdk.url).await?;
        let q = ctl.fail_on(&["rescue-stuck-g"], "turn::ensure_queued", "return");
        let sched = ctl.fail_on(&["rescue-stuck-g"], "turn::schedule_after_error", "return");
        let ids = send_n(&url, "Echo", "run", 1).await?;
        let st = within(Duration::from_secs(15), "invocation paused", async {
            common::wait_status(&url, &ids[0], "paused").await
        })
        .await?;
        ensure!(st["code"] == 577, "paused code must be INFRA_STUCK: {st}");
        drop((q, sched));
        let (code, body) =
            call(Client::new().post(format!("{url}/invocations/{}/resume", ids[0]))).await?;
        ensure!(code == StatusCode::OK, "resume: {code} {body}");
        within(Duration::from_secs(15), "completed after resume", async {
            common::wait_status(&url, &ids[0], "completed").await
        })
        .await?;
        let per_id = starts_per_id(&sdk, &ids);
        ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
        Ok(())
    }
    .await;
    g.node.shutdown().await;
    r
}

/// **Default nack: the turn of a `/resume` panics before the point where it
/// reschedules** (a dispatched piece of work only disappears once it is acked).
///
/// Interleaving:
/// 1. Node K alone, the `InfraStuck` threshold lowered to 3. `turn::ensure_queued`
///    and `turn::schedule_after_error` = `return` (every time), only on K: an
///    invocation just `/send` ends up `paused` 577 after three redispatches by the
///    rescue net (as in `invocation_stuck_on_infra_is_paused_577`).
/// 2. Remove both points; `turn::gate_turn` = `1*panic`, only on K.
/// 3. `/resume`: the engine writes `running` (durable), then the turn runs on right in
///    the command task and panics at the gate — before `begin_turn`, so there is no
///    watchdog retry row yet. The `/resume` task was not wrapped like `/invoke`,
///    `/send`, `run_dispatched` (`rescued_on_panic`): a forgotten marking point.
///
/// Must see: the invocation `completed` within the cap, exactly one `Start`, and the
/// rescue net marked **through the default nack** (`cairnflow_rescue_nacked_total`
/// grows). Before the fix nothing woke it during the term — `running`, no retry, no
/// watchdog row, no rescue mark; on a single node, forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_turn_that_panics_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let k = start_led_alone_with(&bucket, "nack-resume-k", |cfg| {
        cfg.limits.infra_stuck = cairn_server::drainer::InfraStuck {
            strikes: 3,
            after: Duration::from_secs(3600),
        };
    })
    .await?;
    let url = k.node.url();
    let r = async {
        common::register(&url, &sdk.url).await?;
        let q = ctl.fail_on(&["nack-resume-k"], "turn::ensure_queued", "return");
        let sched = ctl.fail_on(&["nack-resume-k"], "turn::schedule_after_error", "return");
        let ids = send_n(&url, "Echo", "run", 1).await?;
        within(Duration::from_secs(15), "invocation paused", async {
            common::wait_status(&url, &ids[0], "paused").await
        })
        .await?;
        drop((q, sched));
        let _panic = ctl.fail_on(&["nack-resume-k"], "turn::gate_turn", "1*panic");
        let nacked = nacked_total();
        // The command task panics: no answer can be trusted — the command only has to arrive.
        let _ = call(Client::new().post(format!("{url}/invocations/{}/resume", ids[0]))).await;
        within(
            Duration::from_secs(15),
            "completed after resume panic",
            async { common::wait_status(&url, &ids[0], "completed").await },
        )
        .await?;
        let per_id = starts_per_id(&sdk, &ids);
        ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
        ensure!(
            nacked_total() > nacked,
            "the rescue net must be marked through the default nack"
        );
        Ok(())
    }
    .await;
    k.node.shutdown().await;
    r
}

/// **(b) — a panicking turn task marks the rescue net for its id.**
///
/// Interleaving:
/// 1. Node H alone, one range. `turn::drive_one` = `1*panic`, only on H.
/// 2. `/send` creates the invocation, answers, then runs the first turn behind the
///    caller's back: the turn panics mid-way (after moving to `Running`). The task
///    dies; the `JoinSet` only sees a `JoinError`.
///
/// Must see: the invocation `completed` within the cap, exactly one `Start` (the
/// panicking turn never reaches the SDK). Before the fix the `join_next` branch only
/// logged: nothing woke the invocation during the term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_turn_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let h = start_led_alone(&bucket, "rescue-panic-h").await?;
    common::register(&h.node.url(), &sdk.url).await?;
    let _p = ctl.fail_on(&["rescue-panic-h"], "turn::drive_one", "1*panic");
    let ids = send_n(&h.node.url(), "Echo", "run", 1).await?;
    let done = within(Duration::from_secs(15), "invocation completed", async {
        common::wait_status(&h.node.url(), &ids[0], "completed").await?;
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    h.node.shutdown().await;
    done?;
    ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
    Ok(())
}

/// **(d) — every invocation that entered a turn has a durable wake-up** (the wake-up is
/// written in the same commit as the status).
///
/// Interleaving:
/// 1. Node I alone, `sdk_timeout` 1 s. `turn::after_running` = `1*return`, only on I:
///    the first turn exits through a `?` right after the `Running` commit — without
///    rescheduling (the shape of a newly forgotten error path).
/// 2. `actor::rescue_round::status` = `return` (every time), only on I: the rescue net
///    — marked through the default nack — cannot redispatch anything, so the watchdog
///    retry row is the only wake-up path (as when the process dies: the RAM is lost
///    together with the mark).
/// 3. `/send` one invocation.
///
/// Must see: the invocation `completed` within the cap (the watchdog row fires after
/// `sdk_timeout` + a margin), exactly one `Start`. Before the fix nothing was left to
/// wake it during the term: no retry, no net mark, no timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turn_lost_after_running_is_woken_by_its_watch() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let i = start_led_alone_with(&bucket, "watch-i", |cfg| {
        cfg.limits.sdk_timeout = Some(Duration::from_secs(1));
    })
    .await?;
    let url = i.node.url();
    common::register(&url, &sdk.url).await?;
    let _lost = ctl.fail_on(&["watch-i"], "turn::after_running", "1*return");
    let _blind = ctl.fail_on(&["watch-i"], "actor::rescue_round::status", "return");
    let ids = send_n(&url, "Echo", "run", 1).await?;
    let done = within(Duration::from_secs(20), "invocation completed", async {
        common::wait_status(&url, &ids[0], "completed").await?;
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    i.node.shutdown().await;
    done?;
    ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
    Ok(())
}

/// **A range job that panics is rebuilt.**
///
/// Interleaving:
/// 1. Node J alone, one range, SDK registered.
/// 2. `actor::step_down` = `1*panic`, only on J. Ask J to release range 0
///    (`/internal/ranges/0/resign`): the leadership loop panics while stepping down —
///    the range job dies, the `LocalSet` swallows the panic.
///
/// Must see: within the cap J leads range 0 again and a new `/send` is `completed`.
/// Before the fix the engine set loop only built engines for ranges **not yet** in the
/// set: the dead handle stayed there forever, and every command got `Stopped`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_job_that_panics_is_rebuilt() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let j = start_led_alone(&bucket, "panic-job-j").await?;
    let url = j.node.url();
    common::register(&url, &sdk.url).await?;
    let r = async {
        let fp = ctl.fail_on(&["panic-job-j"], "actor::step_down", "1*panic");
        let _ = common::post_internal(&j.node, "/internal/ranges/0/resign", Vec::new()).await;
        drop(fp);
        within(
            Duration::from_secs(30),
            "send completed after the job was rebuilt",
            async {
                loop {
                    let send = common::send(&url, "Echo", "run", None, "{}");
                    if let Ok(Ok((200 | 202, body))) =
                        tokio::time::timeout(Duration::from_secs(3), send).await
                    {
                        let id = body["id"].as_str().unwrap_or_default().to_string();
                        common::wait_status(&url, &id, "completed").await?;
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        )
        .await
    }
    .await;
    j.node.shutdown().await;
    r
}

// ─────────────────────── covering the remaining invariants

/// Writes `body` to `owners/range-<r>` of the shared bucket (empty prefix).
async fn put_owner_raw(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    r: u32,
    body: &str,
) -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    bucket
        .put(
            &Path::from(format!("owners/range-{r}")),
            PutPayload::from(body.to_string()),
        )
        .await?;
    Ok(())
}

/// **SM-11 — a tombstone CAS that misses keeps the range, the next round reads again**
/// (`OrphanKept::CasLost`).
///
/// Interleaving:
/// 1. Node A alone, two ranges, the background proxy tombstone round off.
///    `next_range_id` pushed to 20; an owner record built by hand for range 9 (outside
///    the table, allocated, promised by nobody), the owner released — all four
///    conditions of SM-11.
/// 2. A proxy tombstone round runs and stops at `gc::retire_orphan::before_cas`: it has
///    read the record (version V) and decided to tombstone.
/// 3. The test cuts into the window: it overwrites 9's owner record (another term) ⇒
///    version V+1 — the shape of a claimer/owner changing the record between the read
///    and the CAS.
/// 4. Release the round.
///
/// Must see: the round does not tombstone 9 but keeps it with reason `casLost`, and the
/// owner record is exactly the one the test just wrote (the tombstone does not
/// overwrite a newer record). The next round reads again and tombstones, keeping the
/// new term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_tombstone_whose_cas_loses_is_kept_and_retried() -> Result<()> {
    use cairn_server::gc::{OrphanKept, OrphanRetirer};
    use std::time::Duration;

    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = common::start_cluster_node_tweaked(
        &bucket,
        "caslost-a",
        2,
        Duration::from_secs(1),
        |cfg| {
            cfg.retire_orphans_every = None;
            cfg.reconcile_intents_every = None;
        },
    )
    .await?;
    let r = within(TEST_DEADLINE, "tombstone CAS misses", async {
        common::wait_probe(
            &[&a],
            Duration::from_secs(20),
            "A is leader of every range",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        let v = a.node.cluster.table().version();
        let ranges = a.node.cluster.table().ranges().to_vec();
        let up = a
            .node
            .cluster
            .update(v, |t| {
                cairn_store::PartitionTable::from_entries(t.version() + 1, 20, ranges)
            })
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(matches!(up, cairn_store::TableUpdate::Applied(_)), "{up:?}");
        put_owner_raw(
            &bucket,
            9,
            "cairnflow-owner/1\nterm=3\nsession=dead/1\nreleased=1\nwal_kind=shared\n",
        )
        .await?;

        let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
            bucket.clone(),
            "",
            "memory://",
        ));
        let session = a.node.session.clone();
        let at_cas = ctl.pause_at("gc::retire_orphan::before_cas");
        let round = tokio::spawn({
            let (stores, cluster, session) =
                (stores.clone(), a.node.cluster.clone(), session.clone());
            async move {
                OrphanRetirer::default()
                    .run_once(&stores, &cluster, &session)
                    .await
            }
        });
        at_cas.expect_stopped(TEST_DEADLINE).await;
        put_owner_raw(
            &bucket,
            9,
            "cairnflow-owner/1\nterm=4\nsession=dead/1\nreleased=1\nwal_kind=shared\n",
        )
        .await?;
        drop(at_cas);
        let round = round.await?.map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.retired.is_empty(),
            "a CAS on the old version must miss: {round:?}"
        );
        ensure!(
            round
                .kept
                .iter()
                .any(|k| k.range == 9 && k.by == OrphanKept::CasLost),
            "range 9 must be kept with reason casLost: {round:?}"
        );
        let now = owner_of(&bucket, 9)
            .await?
            .ok_or_else(|| anyhow!("9's owner record disappeared"))?;
        ensure!(
            !now.retired && now.term == 4,
            "the tombstone overwrote a newer record: {now:?}"
        );

        let again = OrphanRetirer::default()
            .run_once(&stores, &a.node.cluster, &session)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            again.retired == vec![9],
            "the next round must read again and tombstone: {again:?}"
        );
        let now = owner_of(&bucket, 9)
            .await?
            .ok_or_else(|| anyhow!("9's owner record disappeared"))?;
        ensure!(
            now.retired && now.term == 4,
            "the tombstone must copy the new record: {now:?}"
        );
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    r
}

/// **LS-9 — a failed `restore_flow` steps down and grants no permit.** Without the VO
/// locks rebuilt, nobody knows who holds which lock; granting a permit would let two
/// invocations run on one object at once. The rebuild runs in chunks while the range
/// serves commands (FL-8: the locks exist before the first permit grant, not before the
/// first command), so a failed term may already have answered reads — but no turn.
///
/// Interleaving:
/// 1. Failpoint `actor::restore_flow` = `1*return`, only on node A, set **before** A is
///    built.
/// 2. A alone, one range: it claims range 0 (term 1), becomes leader, `restore_flow`
///    fails the first time.
/// 3. A steps down (releases), the leadership loop claims again (term 2); the second
///    recovery succeeds.
///
/// Must see: an invoke (it needs a permit) goes all the way through, and at that point
/// the owner record has term ≥ 2 (the term whose `restore_flow` failed never grants a
/// permit).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_restore_flow_steps_down_instead_of_serving() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::time::Duration;

    let ctl = StepController::new();
    let _once = ctl.fail_on(&["restore-a"], "actor::restore_flow", "1*return");
    let bucket = common::bucket();
    let a = common::start_cluster_node(&bucket, "restore-a", 1, Duration::from_secs(1)).await?;
    let url = a.node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(TEST_DEADLINE, "restore_flow fails ⇒ step down", async {
        common::wait_probe(
            &[&a],
            Duration::from_secs(20),
            "A is leader of range 0",
            |p| p.len() == 1 && p.values().all(|v| v == &[0]),
        )
        .await?;
        while common::register(&url, &sdk.url).await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // A call in the failed term waits at the permit gate, then gets the step-down
        // error: call again.
        loop {
            if let Ok((200, body)) = common::invoke(&url, "Counter", "add", Some("k"), "1").await {
                ensure!(body["output"] == "ok", "{body}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let rec = owner_of(&bucket, 0)
            .await?
            .ok_or_else(|| anyhow!("range 0 has no owner record"))?;
        ensure!(
            rec.term >= 2,
            "range 0 granted a permit in a term whose restore_flow failed (term {}): {rec:?}",
            rec.term
        );
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    r
}

/// **FL-16 — a failed durable status check before the turn (`gate_turn`) marks the
/// rescue net.** `gate_turn` comes before `begin_turn`, so a turn failing there has no
/// watchdog retry row (FL-17) — only the rescue net wakes it.
///
/// Interleaving:
/// 1. Node A alone, one range. `turn::gate_turn` = `1*return`, only on A.
/// 2. `/send` creates the invocation (durable), the first turn behind the caller's
///    back: `drive_client` gets a permit, `drive_with_permit` fails in `gate_turn` and
///    `?`s out — through `lost_wake`.
///
/// Must see: the invocation `completed` within 15 s, exactly one `Start` (the failed
/// turn never reaches the SDK). Without the mark in `lost_wake` it stays `running` for
/// the whole term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turn_failing_at_gate_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "rescue-gate-a").await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let _gate = ctl.fail_on(&["rescue-gate-a"], "turn::gate_turn", "1*return");
    let ids = send_n(&a.node.url(), "Echo", "run", 1).await?;
    let done = within(Duration::from_secs(15), "invocation completed", async {
        common::wait_status(&a.node.url(), &ids[0], "completed").await?;
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    a.node.shutdown().await;
    done?;
    ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
    Ok(())
}

/// **FL-16 — a turn ending in "retry" that cannot read `journal_len` to reschedule
/// (`schedule_if_retry`/`retry_if`) marks the rescue net.** That turn's watchdog row
/// only fires after `sdk_timeout` + 5 s (65 s); the rescue net redispatches at once.
///
/// Interleaving:
/// 1. Node A alone, one range; handler `Flaky/run` always fails retryably.
///    `turn::retry_if` = `1*return`, only on A.
/// 2. `/send`; the first turn reaches the SDK, the SDK returns a retryable error,
///    `retry_if` fails reading `journal_len` ⇒ no durable retry, `?` out through
///    `lost_wake`.
///
/// Must see: the second turn reaches the SDK within 15 s (the rescue net, then the
/// ordinary retry). Without the mark the second turn only comes when the watchdog row
/// fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_that_cannot_be_scheduled_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Flaky", "run", vec![Step::FailRetry])
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "rescue-retry-a").await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let _retry = ctl.fail_on(&["rescue-retry-a"], "turn::retry_if", "1*return");
    let ids = send_n(&a.node.url(), "Flaky", "run", 1).await?;
    let r = within(Duration::from_secs(15), "2nd turn reaches the SDK", async {
        while starts_per_id(&sdk, &ids)[0] < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    r
}

/// **FL-16 — `drive_client` has to wait for a permit and the durability wait fails ⇒
/// it marks the rescue net.** That turn has not entered `begin_turn` (no watchdog row)
/// and its task exits.
///
/// Interleaving:
/// 1. Node A alone, one range; object `Held/run` blocks at the SDK gate.
/// 2. `/send` X on key `k`: X's turn holds `k`'s VO lock and stands at the gate.
/// 3. `turn::client_sync` = `1*return`, only on A. `/send` Y on the same `k`:
///    `QUEUE_GRACE` runs out without a permit (`k`'s lock is with X) ⇒ the
///    `engine.sync()` for an early answer fails ⇒ `?` out through `lost_wake`; Y's task
///    exits.
/// 4. Open the gate: X finishes, releases `k`'s lock.
///
/// Must see: both `completed` within 15 s, each with exactly one `Start`. Without the
/// mark nobody wakes Y during the term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_turn_whose_early_reply_sync_fails_is_rescued() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .gated_object("Held", "run", vec![Step::output("\"done\"")])
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "rescue-sync-a").await?;
    let url = a.node.url();
    common::register(&url, &sdk.url).await?;
    let (code, body) = common::send(&url, "Held", "run", Some("k"), "{}").await?;
    ensure!(code == 200 || code == 202, "{code} {body}");
    let x = body["id"].as_str().unwrap_or_default().to_string();
    within(TEST_DEADLINE, "X's turn reaches the SDK", async {
        while sdk.starts_len() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    let marked =
        || cairn_server::metrics::flow::RESCUE_MARKED.load(std::sync::atomic::Ordering::Relaxed);
    let marked_before = marked();
    let _sync = ctl.fail_on(&["rescue-sync-a"], "turn::client_sync", "1*return");
    let (code, body) = common::send(&url, "Held", "run", Some("k"), "{}").await?;
    ensure!(code == 200 || code == 202, "{code} {body}");
    let y = body["id"].as_str().unwrap_or_default().to_string();
    // The event "Y failed at the durability wait and was marked" — before X is released.
    within(Duration::from_secs(5), "the rescue net marks Y", async {
        while marked() == marked_before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    sdk.gate().open();
    let ids = vec![x, y];
    let done = within(Duration::from_secs(15), "X and Y completed", async {
        for id in &ids {
            common::wait_status(&url, id, "completed").await?;
        }
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    a.node.shutdown().await;
    done?;
    ensure!(per_id == [1, 1], "one turn per invocation: {per_id:?}");
    Ok(())
}

/// **FL-16 — the rescue net failing to read the status keeps the mark and backs off one
/// more step.** Dropping the mark here would lose exactly the only wake-up left.
///
/// Interleaving:
/// 1. Node A alone, one range. `turn::gate_turn` = `1*return` and
///    `actor::rescue_round::status` = `1*return`, only on A.
/// 2. `/send`: the first turn fails in `gate_turn` ⇒ the net is marked.
/// 3. The net's first round fails to read the failed id's status ⇒ it must keep the
///    mark (`strike`).
/// 4. The next round reads it, sees `running`, redispatches.
///
/// Must see: the invocation `completed` within 15 s, exactly one `Start`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescue_keeps_its_mark_when_the_status_read_fails() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "rescue-read-a").await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let _gate = ctl.fail_on(&["rescue-read-a"], "turn::gate_turn", "1*return");
    let _read = ctl.fail_on(
        &["rescue-read-a"],
        "actor::rescue_round::status",
        "1*return",
    );
    let ids = send_n(&a.node.url(), "Echo", "run", 1).await?;
    let done = within(Duration::from_secs(15), "invocation completed", async {
        common::wait_status(&a.node.url(), &ids[0], "completed").await?;
        Ok(())
    })
    .await;
    let per_id = starts_per_id(&sdk, &ids);
    a.node.shutdown().await;
    done?;
    ensure!(per_id == [1], "one turn reaches the SDK: {per_id:?}");
    Ok(())
}

// ───────────────────── mutation check: the version re-check of SM-14 (3)

/// **The orphan child round skips its turn when the table changes between the check
/// and the discard** (SM-14 (3); a mutation of this re-check survived: no test went
/// through this branch).
///
/// Interleaving:
/// 1. Node A alone, two ranges, the background cleanup round off. Every removal of a
///    final checkpoint fails (`split::discard::delete_checkpoint`), so nobody can
///    discard a child.
/// 2. A splits range 0, failing right after cloning the left child L
///    (`split::cloneLeft`): the coordinator rolls back the intent but discarding L
///    fails — L becomes an orphan child (standing on live range 0, no owner record).
/// 3. An `OrphanRetirer` round stops at `gc::discard_orphan::before_recheck`: L has
///    passed the proof, the references and the barriers.
/// 4. The test cuts into the window: change the table (version + 1).
/// 5. Release the round.
///
/// Must see: the round keeps L with reason `TableMoved`, discards nothing; L's Db is
/// still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_round_skips_a_candidate_when_the_table_moves_before_discard() -> Result<()> {
    use cairn_server::gc::{OrphanKept, OrphanRetirer};
    use std::time::Duration;

    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = common::start_cluster_node_tweaked(
        &bucket,
        "recheck-a",
        2,
        Duration::from_secs(1),
        |cfg| {
            cfg.retire_orphans_every = None;
            cfg.reconcile_intents_every = None;
        },
    )
    .await?;
    // The orphan child is left on purpose: the background cleanup round is off, so
    // `verify` at cleanup sees it (an SM-14 round would heal it).
    bucket.allow("orphan_db", "an orphan child the test keeps on purpose");
    let r = within(TEST_DEADLINE, "keep the child on table change", async {
        common::wait_probe(
            &[&a],
            Duration::from_secs(20),
            "A is leader of every range",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
            bucket.clone(),
            "",
            "memory://",
        ));
        let _keep = ctl.fail("split::discard::delete_checkpoint", "return");
        let _fail = ctl.fail_on(&["recheck-a"], "split::cloneLeft", "return");
        let before = a.node.cluster.table().next_range_id();
        let ops = a.node.ranges.clone();
        let split = cairn_store::failpoint::in_node("recheck-a", async move {
            ops.split(cairn_store::PartitionId(0), None)
                .await
                .map(|_| ())
        })
        .await;
        ensure!(
            split.is_err(),
            "the split must fail after cloning the left child: {split:?}"
        );
        let left = cairn_store::PartitionId(before);
        loop {
            a.node.cluster.reload().await.map_err(|e| anyhow!("{e}"))?;
            if a.node
                .cluster
                .table()
                .ranges()
                .iter()
                .all(|r| r.intent.is_none())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(
            stores.range_db_exists(left).await? && owner_of(&bucket, left.0).await?.is_none(),
            "L must still have a Db, and no owner record"
        );

        let session = a.node.session.clone();
        let at_recheck = ctl.pause_at("gc::discard_orphan::before_recheck");
        let round = tokio::spawn({
            let (stores, cluster, session) =
                (stores.clone(), a.node.cluster.clone(), session.clone());
            async move {
                OrphanRetirer::default()
                    .run_once(&stores, &cluster, &session)
                    .await
            }
        });
        at_recheck.expect_stopped(TEST_DEADLINE).await;
        let v = a.node.cluster.table().version();
        let ranges = a.node.cluster.table().ranges().to_vec();
        let next = a.node.cluster.table().next_range_id();
        let up = a
            .node
            .cluster
            .update(v, |t| {
                cairn_store::PartitionTable::from_entries(t.version() + 1, next + 1, ranges)
            })
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(matches!(up, cairn_store::TableUpdate::Applied(_)), "{up:?}");
        drop(at_recheck);
        let round = round.await?.map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.discarded.is_empty(),
            "the table changed yet the round still discarded: {round:?}"
        );
        ensure!(
            round
                .kept
                .iter()
                .any(|k| k.range == left.0 && k.by == OrphanKept::TableMoved),
            "L must be kept with reason tableMoved: {round:?}"
        );
        ensure!(stores.range_db_exists(left).await?, "Db of L lost");
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    r
}

/// **`continue_as_new` when the old run was cancelled mid-turn**: no new run is created
/// — exactly as the core says (`Finished { ContinuedAsNew }` on an invocation already
/// terminal ⇒ `Plan::default()`, no `spawn_successor`). The turn ends like the
/// cancelled run.
///
/// Order:
/// 1. `Loop/run` returns `ContinueAsNew` on its first run (the new run, if any, returns
///    `"done"`).
/// 2. `turn::continue_as_new::cancelled_mid_turn` = `1*return`: right before the turn's
///    end commit, a durable admin cancel cuts in (`failed` 572).
/// 3. The shell asks the core `Finished { ContinuedAsNew }`.
///
/// Must see: the old run `failed` 572 (not `completed {"continuedAsNew"}`), the SDK
/// receives only **one** `Start` of `Loop/run`, the node has only **one** invocation.
/// Before: the new run was still created and ran (two `Start`s, two invocations).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continue_as_new_after_mid_turn_cancel_spawns_nothing() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    use std::{
        sync::{atomic::AtomicBool, Arc},
        time::{Duration, Instant},
    };

    let ctl = StepController::new();
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object(
            "Loop",
            "run",
            vec![
                Step::ContinueAsNewOnce(
                    Arc::new(AtomicBool::new(false)),
                    bytes::Bytes::from_static(b"{}"),
                ),
                Step::output("done"),
            ],
        )
        .spawn()
        .await?;
    let r = within(Duration::from_secs(30), "continue_as_new, cancel", async {
        common::register(&url, &sdk.url).await?;
        let _cut = ctl.fail("turn::continue_as_new::cancelled_mid_turn", "1*return");
        let (code, body) = common::send(&url, "Loop", "run", Some("k"), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let t0 = Instant::now();
        let st = loop {
            let (_, st) = common::invocation(&url, &id).await?;
            if st["status"] != "running" && st["status"] != "suspended" {
                break st;
            }
            ensure!(t0.elapsed() < Duration::from_secs(10), "not stopped: {st}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        ensure!(
            st["status"] == "failed" && st["code"] == 572,
            "the old run must end as cancelled: {st}"
        );
        // The new run (if created) runs at once — give it time to reach the SDK.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let starts = sdk.starts_of("Loop", "run").len();
        ensure!(
            starts == 1,
            "the SDK received {starts} Starts — a new run was created after the cancel"
        );
        let (_, list) = call(reqwest::Client::new().get(format!("{url}/invocations"))).await?;
        ensure!(
            list.as_array().map(Vec::len) == Some(1),
            "there must be only the old run: {list}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────── the Db open path is bounded

/// A Db open **hanging** on node A — at `point` — must not hold the range forever.
///
/// Interleaving:
/// 1. Node A alone, two ranges, a Db open cap of 2 s. `point` = `return(1)` only on A:
///    every open by A of range 1's Db hangs (the shape of SlateDB retrying forever while
///    the store fails for a long time). Range 0 opens normally; A's session keeps
///    renewing.
/// 2. Node B joins the cluster (no failpoint).
///
/// Must see: within 40 s, B leads range 1 and A range 0; A keeps its incarnation (an
/// open past the cap releases the lease in an orderly way, no fail-stop). Before the
/// fix: the hanging open held range 1's lease forever, and no other node could claim it.
async fn hung_open_hands_the_range_over(point: &str) -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let _stall = ctl.fail_on(&["open-a"], point, "return(1)");
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.stores = cfg.stores.clone().with_open_bound(Duration::from_secs(2));
        cfg.retire_orphans_every = None;
        cfg.reconcile_intents_every = None;
    };
    let a = common::start_cluster_node_tweaked(&bucket, "open-a", 2, Duration::from_secs(1), tweak)
        .await?;
    let r = within(
        Duration::from_secs(90),
        "a range whose open hangs on A goes to B",
        async {
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "A is leader of range 0",
                |p| p.get(&0) == Some(&vec![0]),
            )
            .await?;
            let a_session = a
                .node
                .session
                .current()
                .ok_or_else(|| anyhow!("A has no session"))?;
            let b = common::start_cluster_node_tweaked(
                &bucket,
                "open-b",
                2,
                Duration::from_secs(1),
                tweak,
            )
            .await?;
            let r = async {
                common::wait_probe(
                    &[&a, &b],
                    Duration::from_secs(40),
                    "B leads range 1, A range 0",
                    |p| p.get(&0) == Some(&vec![0]) && p.get(&1) == Some(&vec![1]),
                )
                .await?;
                let now = a.node.session.current();
                ensure!(
                    now.as_ref() == Some(&a_session),
                    "A changed incarnation (fail-stop?): {a_session} → {now:?}"
                );
                Ok(())
            }
            .await;
            b.node.shutdown().await;
            r
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

/// [`hung_open_hands_the_range_over`] with SlateDB's `build` hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hung_db_build_hands_the_range_over() -> Result<()> {
    hung_open_hands_the_range_over("store::open_db::stall").await
}

/// [`hung_open_hands_the_range_over`] with the replay row flush of `acquire_shared`
/// hanging (A's WAL segment already CASed into the owner record).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hung_replay_flush_hands_the_range_over() -> Result<()> {
    hung_open_hands_the_range_over("nodewal::acquire::flush_stall").await
}

// ─────────────────────────────── a lone node (LS-16)

/// Enables the `control/quiet-lease` gate on the bucket, like `cairn-ctl cluster
/// enable-quiet-lease --apply`.
async fn quiet_lease_on(bucket: &common::Bucket) -> Result<()> {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (**bucket).clone();
    let on = cairn_store::quiet_lease::QuietLease {
        on: true,
        knows: cairn_store::migrate::QUIET_LEASE,
        by: "failpoints".into(),
        at_ms: 0,
        delegation: true,
        ..Default::default()
    };
    ensure!(
        cairn_store::quiet_lease::write(&store, &Default::default(), &on, None).await?,
        "could not write the gate"
    );
    Ok(())
}

/// The current session record of `node` on the bucket.
async fn session_record_of(
    bucket: &common::Bucket,
    node: &cairn_server::node::RunningNode,
) -> Result<(cairn_store::SessionId, cairn_store::SessionRecord)> {
    use slatedb::object_store::ObjectStoreExt;
    let id = node
        .session
        .current()
        .ok_or_else(|| anyhow!("node has no incarnation"))?;
    let path = slatedb::object_store::path::Path::from(format!("sessions/{}/{}", id.node, id.inc));
    let raw = bucket.get(&path).await?.bytes().await?;
    Ok((id, cairn_store::SessionRecord::decode(&raw)?))
}

/// Waits until a lone node enters `mode=solo` (not challenged yet).
async fn until_solo(
    bucket: &common::Bucket,
    node: &cairn_server::node::RunningNode,
) -> Result<cairn_store::SessionId> {
    within(TEST_DEADLINE, "a lone node enters mode=solo", async {
        loop {
            let (id, r) = session_record_of(bucket, node).await?;
            if r.mode == cairn_store::SessionMode::Solo && r.challenged.is_none() {
                return Ok(id);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
}

/// **Failpoint `session::before_answer_challenge`**: a `solo` owner challenged that
/// does not answer in time loses — never two leaders.
///
/// Interleaving:
/// 1. Gate on; A alone, leading every range, enters `mode=solo`.
/// 2. B joins; B's tracker CASes a `challenged=<B>` line onto A's record.
/// 3. A's next self-check GET sees `challenged` (no renewal) and stops at
///    `session::before_answer_challenge` (only on A), right before the answering CAS.
/// 4. One TTL after the challenge, B CASes `expired` onto A's record and then claims
///    every range.
/// 5. Release A: the answering CAS (If-Match an etag with the `challenged` line)
///    misses, reads again and sees `expired` ⇒ `lose`, opens a new incarnation.
///
/// Must see: throughout (3)–(5) no range has two answering leaders; B leads every
/// range after (4); A moves to a new incarnation after (5), it does not revive the old
/// one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn solo_owner_stuck_before_answering_loses_to_the_challenger() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let ttl = Duration::from_secs(2);
    let bucket = common::bucket();
    quiet_lease_on(&bucket).await?;
    let a = common::start_cluster_node(&bucket, "ans-a", 2, ttl).await?;
    common::wait_probe(&[&a], TEST_DEADLINE, "A is leader of every range", |p| {
        p.len() == 2 && p.values().all(|v| v == &[0])
    })
    .await?;
    let old = until_solo(&bucket, &a.node).await?;
    let paused = ctl.pause_at_on(&["ans-a"], "session::before_answer_challenge");
    let b = common::start_cluster_node(&bucket, "ans-b", 2, ttl).await?;
    paused.expect_stopped(TEST_DEADLINE).await;
    within(TEST_DEADLINE, "B expires A and leads every range", async {
        loop {
            let p = common::leaders_by_probe(&[&a, &b]).await;
            ensure!(p.values().all(|v| v.len() <= 1), "two leaders: {p:?}");
            if p.len() == 2 && p.values().all(|v| v == &[1]) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    drop(paused);
    within(TEST_DEADLINE, "A opens new incarnation", async {
        loop {
            if a.node.session.current().is_some_and(|c| c != old) {
                return Ok(());
            }
            let p = common::leaders_by_probe(&[&a, &b]).await;
            ensure!(p.values().all(|v| v.len() <= 1), "two leaders: {p:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    a.node.shutdown().await;
    b.node.shutdown().await;
    Ok(())
}

/// **Failpoint `session::before_challenge_cas`**: the challenger's τ counts from when the
/// challenge CAS **is confirmed**, not from the read before it.
///
/// Interleaving:
/// 1. Gate on; A alone in `solo`, leading every range.
/// 2. B joins; B's tracker reads A's `solo` record and stops at
///    `session::before_challenge_cas` (only on B).
/// 3. A freezes (every request to A's store hangs).
/// 4. Two TTLs later, release B: the challenge CAS reaches the store at `t_c`.
///
/// Must see: B leads **no** range before `t_c + TTL` (counting from the read in (2), B
/// would expire A as soon as it is released); after that B leads every range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn challenger_counts_ttl_from_its_confirmed_cas_not_its_read() -> Result<()> {
    use std::time::{Duration, Instant};
    let ctl = StepController::new();
    let ttl = Duration::from_secs(2);
    let bucket = common::bucket();
    quiet_lease_on(&bucket).await?;
    let a = common::start_cluster_node(&bucket, "chl-a", 2, ttl).await?;
    common::wait_probe(&[&a], TEST_DEADLINE, "A is leader of every range", |p| {
        p.len() == 2 && p.values().all(|v| v == &[0])
    })
    .await?;
    until_solo(&bucket, &a.node).await?;
    let paused = ctl.pause_at_on(&["chl-b"], "session::before_challenge_cas");
    let b = common::start_cluster_node(&bucket, "chl-b", 2, ttl).await?;
    paused.expect_stopped(TEST_DEADLINE).await;
    common::freeze(&a.store);
    tokio::time::sleep(ttl * 2).await;
    let released = Instant::now();
    drop(paused);
    let led = within(TEST_DEADLINE, "B leads every range", async {
        loop {
            let p = common::leaders_by_probe(&[&b]).await;
            if !p.values().all(|v| v.is_empty()) {
                let at = released.elapsed();
                ensure!(
                    at + Duration::from_millis(200) >= ttl,
                    "B leads {at:?} after the challenge CAS reached the store, earlier than a TTL: {p:?}"
                );
            }
            if p.len() == 2 && p.values().all(|v| v == &[0]) {
                return Ok(released.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    ensure!(led >= ttl, "B leads after {led:?}");
    b.node.shutdown().await;
    Ok(())
}

/// A registry writer dies between the main write and the registry generation bump.
///
/// Formerly the generation was bumped only **after** the main write. A writer dying in
/// that window bumped nothing and told nobody: peers waited out a full reconciliation
/// (10 minutes).
///
/// 1. Two nodes `a`, `b`, registry refresh tick 200 ms, default reconciliation.
///    Register deployment W on `a` (the registry generation exists from here on); `b`
///    lists W.
/// 2. Point `registry::between_write_and_seal` on `a`, once: the next registry write
///    request on `a` finishes the main write and then "dies" — it does not touch the
///    generation again, does not notify peers.
/// 3. Register deployment X on `a`.
///
/// Must see: `b` lists X within 3 s — an odd generation ("being written", bumped
/// **before** the main write) makes `b` run the full round every tick until it is
/// sealed even. Then (the "standing odd" threshold lowered to 1 s) the coordinator
/// removes `a`'s token within a few seconds, the generation goes back to even, and `b`
/// stops LISTing the registry when idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_writer_dying_before_the_seal_still_reaches_peers() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.deploy_refresh = Duration::from_millis(200);
        cfg.registry_stuck = Duration::from_secs(1);
    };
    let ttl = Duration::from_secs(2);
    let a = common::start_cluster_node_tweaked(&bucket, "reg-a", 2, ttl, tweak).await?;
    let b = common::start_cluster_node_tweaked(&bucket, "reg-b", 2, ttl, tweak).await?;
    let listed = |url: String| async move {
        let v: Value = reqwest::get(format!("{url}/deployments"))
            .await?
            .json()
            .await?;
        Ok::<_, anyhow::Error>(v.as_array().map_or(0, Vec::len))
    };
    let wait_listed = |url: String, n: usize, deadline: Duration| async move {
        within(deadline, "b lists every deployment", async {
            while listed(url.clone()).await? < n {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        })
        .await
    };
    let r = async {
        let w = common::fake_sdk::FakeSdk::builder()
            .object("W", "run", vec![common::fake_sdk::Step::output("x")])
            .spawn()
            .await?;
        common::register(&a.node.url(), &w.url).await?;
        wait_listed(b.node.url(), 1, Duration::from_secs(10)).await?;

        let _dies = ctl.fail_on(&["reg-a"], "registry::between_write_and_seal", "1*return");
        let x = common::fake_sdk::FakeSdk::builder()
            .object("X", "run", vec![common::fake_sdk::Step::output("x")])
            .spawn()
            .await?;
        common::register(&a.node.url(), &x.url).await?;
        wait_listed(b.node.url(), 2, Duration::from_secs(3)).await?;

        let gen_odd = || async {
            use slatedb::object_store::{path::Path, ObjectStoreExt};
            let raw = bucket
                .get(&Path::from("control/registry-gen"))
                .await?
                .bytes()
                .await?;
            let text = String::from_utf8_lossy(&raw).into_owned();
            let n: u64 = text
                .split_whitespace()
                .find_map(|kv| kv.strip_prefix("gen=")?.parse().ok())
                .context("gen=")?;
            Ok::<_, anyhow::Error>((n % 2 == 1, text))
        };
        ensure!(
            gen_odd().await?.0,
            "the writer died before the seal ⇒ odd generation"
        );
        within(
            Duration::from_secs(8),
            "the coordinator seals a standing odd generation",
            async {
                while gen_odd().await?.0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(())
            },
        )
        .await?;
        // The round in progress when the seal finished settles fully, then count `b`'s LISTs while idle.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let lists = |s: &cairn_store::testkit::FaultyStore| -> u64 {
            s.counts_by_source()
                .into_iter()
                .filter(|(src, op, ..)| {
                    *src == "node.refresh" && *op == cairn_store::testkit::Op::List
                })
                .map(|(.., n)| n)
                .sum()
        };
        let before = lists(&b.store);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let idle = lists(&b.store) - before;
        ensure!(
            idle <= 3,
            "the generation is sealed even yet b LISTed the registry {idle} times in 1.5 s idle ({:?})",
            gen_odd().await?.1
        );
        Ok(())
    }
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

// ─────────────────── delegated renewal (LS-14/LS-15)

/// Three nodes, four ranges, TTL 1 s, gate on; waits until every node other than the
/// coordinator has finished delegating and each node other than the coordinator holds
/// at least one range. Returns (bucket, nodes, coordinator index, owner of each range).
async fn delegated_three(
    prefix: &str,
) -> Result<(
    common::Bucket,
    Vec<common::ClusterNode>,
    usize,
    std::collections::BTreeMap<u32, usize>,
)> {
    use std::time::Duration;
    let ttl = Duration::from_secs(1);
    let bucket = common::bucket();
    quiet_lease_on(&bucket).await?;
    let mut nodes = Vec::new();
    for id in ["a", "b", "c"] {
        nodes.push(common::start_cluster_node(&bucket, &format!("{prefix}-{id}"), 4, ttl).await?);
    }
    let c = within(TEST_DEADLINE, "a coordinator exists", async {
        loop {
            if let Some(seen) = common::coordinator_of(&bucket)
                .await
                .filter(|s| !s.released)
            {
                if let Some(i) = nodes.iter().position(|n| {
                    n.node
                        .session
                        .current()
                        .is_some_and(|id| id.to_string() == seen.session)
                }) {
                    return Ok(i);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    common::until_delegated(&bucket, &nodes, c, ttl * 10).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, TEST_DEADLINE, "one range on each node", |p| {
        common::single_leaders(p).is_some_and(|m| common::load_of(&m, 3).iter().all(|n| *n >= 1))
    })
    .await?;
    common::until_delegated(&bucket, &nodes, c, ttl * 5).await?;
    let leaders = common::single_leaders(&probe).expect("balanced");
    Ok((bucket, nodes, c, leaders))
}

/// **Failpoint `coordinator::before_ack` (+ the tripwire
/// `coordinator::ack_after_expire`)**: the ack of a **zombie** coordinator — the batch
/// fence passed, then its session was expired before it answered — never makes two
/// leaders.
///
/// Interleaving:
/// 1. Three nodes, delegation to `C` finished; the tripwire
///    `coordinator::ack_after_expire` set to `panic` for the whole test.
/// 2. `C`'s next heartbeat batch passes the fence and stops at
///    `coordinator::before_ack` (only on `C`).
/// 3. The test CASes `expired` onto `C`'s session record (as if an observer with a
///    stale read had just won): from here on `C`'s role is lost for every observer.
/// 4. Release `C` after ~100 ms (below the sender's heartbeat wait cap): it answers the
///    batch — the ack of a lost role, for heartbeats received **before** the role was
///    lost.
/// 5. The other nodes see `C`'s session ended, yield one more TTL (the yield rule)
///    before touching the `delegated` record for `C`; `C`'s next batch fence sees its
///    session ended ⇒ it leaves the role, acks nothing more; a node that loses its ack
///    falls back.
///
/// Must see: for 5·TTL after the release no range has two leaders; every node other
/// than `C` keeps its incarnation (they fell back in time, nobody expired them); the
/// tripwire does not fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zombie_coordinator_ack_never_extends_an_expired_grant() -> Result<()> {
    use {
        slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt},
        std::time::{Duration, Instant},
    };
    let ctl = StepController::new();
    let _wire = ctl.fail("coordinator::ack_after_expire", "panic");
    let ttl = Duration::from_secs(1);
    let (bucket, nodes, c, _) = delegated_three("zomb").await?;
    let r = async {
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let cid = incs[c].clone().expect("C alive");
        let paused = ctl.pause_at_on(&[&cid.node], "coordinator::before_ack");
        paused.expect_stopped(TEST_DEADLINE).await;
        let path = Path::from(format!("sessions/{}/{}", cid.node, cid.inc));
        let got = bucket.get(&path).await?;
        let version = slatedb::object_store::UpdateVersion {
            e_tag: got.meta.e_tag.clone(),
            version: got.meta.version.clone(),
        };
        let text = String::from_utf8(got.bytes().await?.to_vec())?;
        ObjectStore::put_opts(
            &**bucket,
            &path,
            text.replace("state=live", "state=expired").into(),
            slatedb::object_store::PutMode::Update(version).into(),
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(paused);
        let refs: Vec<_> = nodes.iter().collect();
        let t0 = Instant::now();
        while t0.elapsed() < ttl * 5 {
            let p = common::leaders_by_probe(&refs).await;
            ensure!(
                p.values().all(|v| v.len() <= 1),
                "two leaders after zombie ack: {p:?}"
            );
            for (i, n) in nodes.iter().enumerate() {
                ensure!(
                    i == c || n.node.session.current() == incs[i],
                    "node {i} lost its session after a zombie ack"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **Failpoints `coordinator::before_ack` + `session::before_fallback_cas`**: a
/// delegating node that loses its ack and whose fallback CAS is stuck loses to the
/// coordinator — its grant runs out before the `expired` CAS (the expire rule: τ = the
/// last acked heartbeat).
///
/// Interleaving:
/// 1. Three nodes, delegation to `C` finished; `N`, other than `C`, holds at least one
///    range; the tripwire `coordinator::ack_after_expire` set to `panic`.
/// 2. `N` stops at `session::before_fallback_cas` (only on `N`) when it wants to fall
///    back.
/// 3. `C` stops at `coordinator::before_ack` once (~0.6·TTL): nobody gets acked; `N`
///    wants to fall back (the TTL/3 mark) and is stuck at (2); the remaining node falls
///    back to `direct`.
/// 4. Release `C`. `N`'s heartbeats (is its bucket-touch age kept fresh by the
///    self-check GET? — no: `N`'s keeper is stuck) are no longer acked; one TTL after
///    the last acked heartbeat `C` CASes `expired` onto `N`'s record; another node
///    claims `N`'s ranges.
/// 5. Release `N`: the fallback CAS misses, reads again and sees `expired` ⇒ `lose`.
///
/// Must see: no probe sees two leaders; `N`'s ranges move to another node; `N` moves to
/// a new incarnation after (5); the tripwire does not fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stuck_fallback_loses_to_the_coordinator_after_a_ttl() -> Result<()> {
    use std::time::{Duration, Instant};
    let ctl = StepController::new();
    let _wire = ctl.fail("coordinator::ack_after_expire", "panic");
    let ttl = Duration::from_secs(1);
    let (_bucket, nodes, c, leaders) = delegated_three("stuck").await?;
    let r = async {
        let n = *leaders
            .values()
            .find(|i| **i != c)
            .expect("a delegating node holds a range");
        let mine: Vec<u32> = leaders
            .iter()
            .filter(|(_, i)| **i == n)
            .map(|(r, _)| *r)
            .collect();
        let nid = nodes[n].node.session.current().expect("N alive");
        let cid = nodes[c].node.session.current().expect("C alive");
        let stuck = ctl.pause_at_on(&[&nid.node], "session::before_fallback_cas");
        let held = ctl.pause_at_on(&[&cid.node], "coordinator::before_ack");
        held.expect_stopped(TEST_DEADLINE).await;
        stuck.expect_stopped(TEST_DEADLINE).await;
        tokio::time::sleep(ttl.mul_f32(0.6)).await;
        drop(held);
        let refs: Vec<_> = nodes.iter().collect();
        let t0 = Instant::now();
        loop {
            let p = common::leaders_by_probe(&refs).await;
            ensure!(p.values().all(|v| v.len() <= 1), "two leaders: {p:?}");
            if mine
                .iter()
                .all(|r| p.get(r).is_some_and(|v| v.len() == 1 && v[0] != n))
            {
                break;
            }
            ensure!(
                t0.elapsed() < ttl * 5,
                "N's range did not move to another node: {p:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        drop(stuck);
        within(TEST_DEADLINE, "N opens new incarnation", async {
            loop {
                let p = common::leaders_by_probe(&refs).await;
                ensure!(p.values().all(|v| v.len() <= 1), "two leaders: {p:?}");
                if nodes[n].node.session.current().is_some_and(|id| id != nid) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

// ─────────────────────────────── the call tree colocation gate

/// The number of journal entries of `id` on `url`.
async fn journal_len(url: &str, id: &str) -> Result<usize> {
    let v: Value = reqwest::get(format!("{url}/invocations/{id}/journal"))
        .await?
        .json()
        .await?;
    Ok(v.as_array().map_or(0, Vec::len))
}

/// The id of `service/run` called over HTTP with `Idempotency-Key: idem` — exactly the
/// ingress computation — and the callee id **under the old rule** (gate off) of the
/// first unkeyed call (completion id 1) it sends to `callee`: exactly the computation
/// of `dispatch_call` for a v5 row.
fn caller_and_old_rule_callee(
    service: &str,
    idem: &str,
    callee: &str,
) -> (cairn_store::InvocationId, cairn_store::InvocationId) {
    let tenant = cairn_store::TenantId::default();
    let pk = cairn_store::partition_key_for_target(&tenant, None, service, "", Some(idem))
        .expect("the idempotency key yields a key");
    let caller = cairn_store::idempotent_invocation_id(pk, &tenant, None, service, "run", "", idem);
    let key = cairn_store::callee_partition_key(&tenant, None, callee, "", None, &caller, 1);
    (caller, cairn_store::callee_invocation_id(key, &caller, 1))
}

/// **No duplicate callee around enabling the colocation gate** (IR-9).
///
/// Interleaving:
/// 1. An **old** cluster (`cluster-version` already there, gate off). Node A alone, 4
///    ranges. Failpoint `turn::deliver_to_caller::call_id` = `return` on A and B: every
///    delivery of `CallInvocationId` to the caller fails — the `Call` outbox row stays
///    after the callee exists.
/// 2. `Caller/run` (its idempotency key chosen so that the callee under the old rule
///    falls into another range) issues a `ctx.call` to `Leaf/echo` **unkeyed** without
///    awaiting it, then `ctx.sleep(SLEEP_MS)`: the `Call` row is written at v5 (gate
///    off). The drainer sends `StartCall`, the receiver writes it durably and runs callee
///    X (`Leaf` suspends waiting for a signal), then the id delivery fails ⇒ the row
///    stays, time after time.
/// 3. `cairn-ctl cluster enable-call-prefix --apply` ⇒ the gate is on.
/// 4. A stops (the caller's timer has not fired yet); node B comes up and opens every
///    range **with the gate on**.
/// 5. The timer fires on B: the caller's turn replays the journal — the state machine
///    replays the `Task::Invoke` of the call still waiting for its id, that is it
///    **rewrites the outbox row** under the gate. The caller suspends again waiting for
///    the result.
/// 6. Remove the failpoint: B's drainer drains the row again.
///
/// Must see: the caller receives **exactly** the id X computed by the old rule, and
/// exactly **one** callee — no second callee under the prefix rule. A new call after
/// the gate is on follows the prefix rule: the callee keeps the caller's high 32 bits
/// (red before the fix).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_row_written_before_the_gate_keeps_its_callee_after_the_gate() -> Result<()> {
    /// Long enough for the caller's timer to fire after A has stopped (step 4).
    const SLEEP_MS: u64 = 4_000;
    use {
        common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
        slatedb::object_store::{path::Path as ObjPath, ObjectStoreExt, PutPayload},
        std::time::Duration,
    };

    let ctl = StepController::new();
    let bucket = common::bucket();
    // An old cluster: `cluster-version` exists before the first node ⇒ the gate is not
    // enabled up front (that only happens on an empty bucket).
    bucket
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from("cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n"),
        )
        .await?;
    let start = |id: &'static str| {
        let bucket = &bucket;
        async move {
            let n = common::start_cluster_node(bucket, id, 4, Duration::from_secs(2)).await?;
            common::wait_probe(
                &[&n],
                Duration::from_secs(20),
                "node is leader of every range",
                |p| p.len() == 4 && p.values().all(|v| v == &[0]),
            )
            .await?;
            Ok::<_, anyhow::Error>(n)
        }
    };
    let a = start("node-a").await?;
    let sdk = FakeSdk::builder()
        .service(
            "Caller",
            "run",
            vec![
                Step::Spawn {
                    service: "Leaf".into(),
                    handler: "echo".into(),
                    spread: false,
                },
                Step::Sleep(SLEEP_MS),
                Step::AwaitSpawned,
                Step::EchoCallResult,
            ],
        )
        .service("Leaf", "echo", vec![Step::Suspend, Step::output("la")])
        .spawn()
        .await?;
    let deliver = ctl.fail_on(
        &["node-a", "node-b"],
        "turn::deliver_to_caller::call_id",
        "return",
    );
    let table = a.node.cluster.table();
    let (idem, caller, callee) = (0..1000)
        .map(|i| {
            let idem = format!("call-{i}");
            let (caller, callee) = caller_and_old_rule_callee("Caller", &idem, "Leaf");
            (idem, caller, callee)
        })
        .find(|(_, caller, callee)| {
            table.partition_of_key(cairn_store::partition_key_of(caller))
                != table.partition_of_key(cairn_store::partition_key_of(callee))
        })
        .ok_or_else(|| anyhow!("found no caller with a callee in another range"))?;
    let (caller, callee) = (cairn_store::id_hex(&caller), cairn_store::id_hex(&callee));

    // 1–2.
    let url = a.node.url();
    let r = within(
        Duration::from_secs(60),
        "v5 Call row with a remote callee",
        async {
            common::register(&url, &sdk.url).await?;
            let resp = Client::new()
                .post(format!("{url}/invoke/Caller/run/send"))
                .header("Idempotency-Key", &idem)
                .body("x")
                .send()
                .await?;
            let code = resp.status();
            let body: Value = resp.json().await?;
            ensure!(code.is_success(), "{code} {body}");
            ensure!(
                body["id"] == caller.as_str(),
                "caller id differs from the computation: {body}"
            );
            let t0 = std::time::Instant::now();
            while !sdk
                .starts_of("Leaf", "echo")
                .iter()
                .any(|s| s.id_hex() == callee)
            {
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "callee {callee} has not run: {:?}",
                    sdk.starts_of("Leaf", "echo")
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // A few more drain rounds go through the window (resending `StartCall`, the id delivery failing).
            tokio::time::sleep(Duration::from_millis(300)).await;
            let (_, st) = common::invocation(&url, &caller).await?;
            ensure!(
                st["status"] != "completed",
                "the caller finished even though the id delivery failed: {st}"
            );
            journal_len(&url, &caller).await
        },
    )
    .await;
    let slept_at = match r {
        Ok(n) => n,
        Err(e) => {
            a.node.shutdown().await;
            return Err(e);
        }
    };

    // 3.
    let ctl_cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(bucket.clone(), "", "memory://"),
    ))
    .with_clock(std::sync::Arc::new(common::JumpClock::default()));
    let rep = ctl_cli
        .enable_call_prefix(&cairn_server::ctl::RepairOptions {
            apply: true,
            force: false,
            who: "test@failpoints".into(),
            command: "cairn-ctl cluster enable-call-prefix --apply".into(),
            expect_version: None,
            now_ms: cairn_store::entropy::now_ms(),
        })
        .await;
    if let Err(e) = rep {
        a.node.shutdown().await;
        return Err(anyhow!("enable the gate: {e}"));
    }

    // 4.
    let still = journal_len(&url, &caller).await;
    a.node.shutdown().await;
    ensure!(
        still? == slept_at,
        "the caller's timer fired on A (gate off) — the replay turn must run on B; raise SLEEP_MS"
    );
    let b = start("node-b").await?;
    let url = b.node.url();
    let r = within(Duration::from_secs(60), "redrain, gate on", async {
        // 5. The timer fires on B: its completion enters the journal in the replay turn;
        // that turn ends when the caller suspends again waiting for the result.
        let t0 = std::time::Instant::now();
        while journal_len(&url, &caller).await? <= slept_at {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the caller's timer has not fired on B yet"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        common::wait_status(&url, &caller, "suspended").await?;
        // 6.
        drop(deliver);
        let resp = Client::new()
            .post(format!("{url}/invocations/{callee}/signal"))
            .json(&json!({ "signal": HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(
            resp.status() == StatusCode::OK,
            "signal callee: {}",
            resp.text().await?
        );
        let st = common::wait_status(&url, &caller, "completed").await?;
        ensure!(st["output"] == "la", "{st}");
        // Give a second callee (if any) time to run.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let leaves: Vec<String> = sdk
            .starts_of("Leaf", "echo")
            .iter()
            .map(|s| s.id_hex())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        ensure!(
            leaves == vec![callee.clone()],
            "exactly one callee must run, with the old-rule id {callee}: got {leaves:?}"
        );
        // Not only "ran": a second callee may have been **created** without running
        // (its first id delivery failed ⇒ nobody scheduled it). Count the `Leaf`
        // invocations on every range (B leads them all), and the id the caller received.
        let rows: Vec<Value> = reqwest::get(format!("{url}/invocations?limit=1000"))
            .await?
            .json()
            .await?;
        let leaf_rows: Vec<&Value> = rows.iter().filter(|r| r["service"] == "Leaf").collect();
        ensure!(
            leaf_rows.len() == 1 && leaf_rows[0]["id"] == callee.as_str(),
            "there must be exactly one Leaf invocation ({callee}): {leaf_rows:?}"
        );
        let journal: Value = reqwest::get(format!("{url}/invocations/{caller}/journal"))
            .await?
            .json()
            .await?;
        let ascii_hex = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        ensure!(
            journal
                .as_array()
                .into_iter()
                .flatten()
                .any(|e| e["payload"]
                    .as_str()
                    .is_some_and(|p| p.contains(&ascii_hex(&callee)))),
            "the caller must receive id {callee}: {journal}"
        );

        // A new call after enabling the gate: the prefix rule.
        let (code, body) = common::send(&url, "Caller", "run", None, "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let fresh = body["id"].as_str().unwrap_or_default().to_string();
        let t0 = std::time::Instant::now();
        let leaf = loop {
            if let Some(s) = sdk
                .starts_of("Leaf", "echo")
                .into_iter()
                .find(|s| s.id_hex() != callee)
            {
                break s.id_hex();
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the callee after the gate has not run"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        ensure!(
            leaf[..8] == fresh[..8],
            "the callee after the gate must keep the caller's high 32 bits: {leaf} / {fresh}"
        );
        Ok(())
    })
    .await;
    b.node.shutdown().await;
    r
}

// ─────────────────────────────── cross-range batches

/// A sample of `cairnflow_interrange_messages_total{kind="start_call",outcome}`.
async fn start_calls(url: &str, outcome: &str) -> Result<u64> {
    common::metric(
        url,
        &format!(
            "cairnflow_interrange_messages_total{{kind=\"start_call\",outcome=\"{outcome}\"}}"
        ),
    )
    .await
}

/// `Caller` issues `keys.len()` unawaited calls to `Far`, in exactly the order of
/// `keys`, then suspends. `Far` suspends forever.
fn fan_to_far(keys: &[String]) -> Vec<common::fake_sdk::Step> {
    use common::fake_sdk::Step;
    let mut steps: Vec<Step> = keys
        .iter()
        .map(|k| Step::fire("Far", "hold", Some(k)))
        .collect();
    steps.push(Step::Suspend);
    steps
}

/// The distinct ids of the `Far` runs seen.
fn far_ids(far: &common::fake_sdk::FakeSdk) -> std::collections::BTreeSet<String> {
    far.starts_of("Far", "hold")
        .iter()
        .map(|s| s.id_hex())
        .collect()
}

/// Waits until `Far` has run `n` distinct ids, then one more tick for a duplicate
/// message (if any) to arrive; returns the number of ids.
async fn far_settles(far: &common::fake_sdk::FakeSdk, n: usize) -> Result<usize> {
    use std::time::{Duration, Instant};
    let t0 = Instant::now();
    while far_ids(far).len() < n {
        ensure!(
            t0.elapsed() < Duration::from_secs(30),
            "Far has only run {} / {n}",
            far_ids(far).len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(far_ids(far).len())
}

/// **A rejected batch deletes no row.**
///
/// Order:
/// 1. A lone node with 2 ranges. `Caller` (range 0) issues 12 calls to `Far` on range 1
///    in one turn; `Far` is not in the registry yet so all 12 rows stay.
/// 2. Enable `transport::send_once=1*return(<status>)`: the sender's **first** request
///    gets that code, as if the receiver rejected the whole batch.
/// 3. Register `Far`: a drain round gathers the 12 rows into one batch, the batch is
///    rejected; the next round sends again.
///
/// Must see: all 12 messages counted as `rejected` (the whole batch failed, not just the
/// first message), then all 12 `ok`, each `Far` exactly once. Before the fix: only the
/// first message was rejected (`rejected` = 1), the other 11 were not sent in that round.
async fn rejected_batch_scenario(status: u16) -> Result<()> {
    use common::fake_sdk::FakeSdk;
    let ctl = StepController::new();
    let node = common::start_led(common::solo_config(2)).await?;
    let url = node.url();
    let keys: Vec<String> = (0..12)
        .map(|i| common::key_on_partition(&node, "Far", 1, &format!("far-{i}")))
        .collect();
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let callers = FakeSdk::builder()
        .object("Caller", "fan", fan_to_far(&keys))
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![common::fake_sdk::Step::Suspend])
        .spawn()
        .await?;
    let r = within(TEST_DEADLINE, "rejected batch", async {
        common::register(&url, &callers.url).await?;
        let (code, body) = common::send(&url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        let (ok0, rej0) = (
            start_calls(&url, "ok").await?,
            start_calls(&url, "rejected").await?,
        );
        let _reject = ctl.fail("transport::send_once", &format!("1*return({status})"));
        common::register(&url, &far.url).await?;
        let n = far_settles(&far, keys.len()).await?;
        ensure!(n == keys.len(), "each Far exactly once: {n}");
        let (ok, rej) = (
            start_calls(&url, "ok").await? - ok0,
            start_calls(&url, "rejected").await? - rej0,
        );
        ensure!(
            rej == keys.len() as u64,
            "a batch rejected with {status} must count all {} messages: {rej}",
            keys.len()
        );
        ensure!(ok == keys.len() as u64, "resent exactly once: {ok}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_rejected_503_keeps_every_row() -> Result<()> {
    rejected_batch_scenario(503).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_rejected_409_keeps_every_row() -> Result<()> {
    rejected_batch_scenario(409).await
}

/// **A partial ack cannot happen.**
///
/// Order:
/// 1. A two-node cluster, one range per node: `Caller` on alpha's range, 8 `Far` keys
///    on beta's range. `Caller` issues 8 calls in one turn; `Far` is not in the registry
///    yet so the 8 rows stay on alpha.
/// 2. Enable on **beta** `engine::accept_messages::stage=1*off->return`: the batch's
///    first message is staged into the commit, the second fails — the shape of a
///    receiver dying while writing the batch.
/// 3. Register `Far`: alpha sends one batch of 8 messages; beta fails mid-batch and
///    returns an error.
///
/// Must see: while the point is on, **no** `Far` runs — the receiver writes no message
/// of the batch, not even the first one already staged; the sender counts all 8 as
/// failed. Remove the point: all 8 arrive, each exactly once. A receiver writing one
/// message at a time (one commit per message) would have made the first message durable
/// before failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_failing_mid_batch_writes_no_message_of_it() -> Result<()> {
    use {
        common::{cluster_nodes, fake_sdk::FakeSdk, load_of, single_leaders, wait_probe},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let ttl = Duration::from_secs(1);
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 2, ttl).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "one range on each node",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let range_of = |n: usize| *leaders.iter().find(|(_, i)| **i == n).expect("has range").0;
    let (r_a, r_b) = (range_of(0), range_of(1));
    let (a, b) = (&nodes[0], &nodes[1]);
    let keys: Vec<String> = (0..8)
        .map(|i| common::key_on_partition(&b.node, "Far", r_b, &format!("far-{i}")))
        .collect();
    let caller_key = common::key_on_partition(&a.node, "Caller", r_a, "caller");
    let callers = FakeSdk::builder()
        .object("Caller", "fan", fan_to_far(&keys))
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![common::fake_sdk::Step::Suspend])
        .spawn()
        .await?;
    let a_url = a.node.url();
    let r = within(Duration::from_secs(90), "receiver fails mid-batch", async {
        for n in &nodes {
            common::register(&n.node.url(), &callers.url).await?;
        }
        let (code, body) = common::send(&a_url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&a_url, &id, "suspended").await?;
        let rej0 = start_calls(&a_url, "rejected").await?;

        let midway = ctl.fail_on(
            &["node-beta"],
            "engine::accept_messages::stage",
            "1*off->return",
        );
        for n in &nodes {
            common::register(&n.node.url(), &far.url).await?;
        }
        // Until alpha sees a failed batch — or beta has written some message.
        let t0 = std::time::Instant::now();
        while start_calls(&a_url, "rejected").await? - rej0 < keys.len() as u64
            && far_ids(&far).is_empty()
        {
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "alpha has sent no batch to beta yet"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        ensure!(
            far_ids(&far).is_empty(),
            "the receiver failed mid-batch yet wrote part of it: {:?}",
            far_ids(&far)
        );
        drop(midway);
        let n = far_settles(&far, keys.len()).await?;
        ensure!(n == keys.len(), "each Far exactly once: {n}");
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A split cutting in between gathering and sending a batch.**
///
/// Interleaving:
/// 1. A two-node cluster, one range per node: `Caller` on alpha's range `A`; 12 `Far`
///    keys on beta's range `B`, 6 in the lower half and 6 in the upper half of point
///    `at`. `Caller` issues 12 calls alternating lower/upper in one turn; `Far` is not
///    in the registry yet so the 12 rows stay on alpha.
/// 2. Alpha loses its table reload path (every GET of `cluster.conf` fails): it can
///    only learn the new table from beta's rejection.
/// 3. Stop alpha at `transport::send_batch`. Register `Far`: alpha's drainer gathers
///    the 12 rows into **one** batch to `B` (pinned by the old table) and stops — the
///    batch gathered, not sent.
/// 4. Split `B` at `at` on beta: `B` leaves the table, two children `L`, `R`.
/// 5. Release alpha: the batch goes to `B` by the old table ⇒ `409 wrong_range` with
///    the new table; alpha learns the table, **regroups** the 12 messages by the new
///    table (6 to `L`, 6 to `R`) and sends again within the same send.
///
/// Must see: each `Far` runs exactly once, in the child of its own half (not in `B`);
/// alpha holds the post-split table; no message ends its send with `wrong_range`
/// (learned and resent within the send, not returned to the drainer).
///
/// Runs under the default (a new bucket: quiet-lease with delegation) and pinned to
/// `direct` (the `_direct` variant). With delegation, `view` reports beta no longer
/// holding `B` right after the split, so alpha's routing table loses `B`'s leader before
/// the batch goes — the batch only reaches beta through the ask-the-releaser path
/// (`after_split_a_stale_node_learns_the_table_from_the_releaser`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_split_between_gather_and_send_is_regrouped_by_the_new_table() -> Result<()> {
    split_between_gather_and_send(|_| {}).await
}

/// [`batch_split_between_gather_and_send_is_regrouped_by_the_new_table`], with the
/// cluster pinned to `direct` (the tracker's hint, delayed by up to TTL/3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_split_between_gather_and_send_is_regrouped_by_the_new_table_direct() -> Result<()> {
    split_between_gather_and_send(common::direct).await
}

async fn split_between_gather_and_send(
    tweak: impl Fn(&mut cairn_server::node::NodeConfig),
) -> Result<()> {
    use {
        cairn_store::testkit::{Action, OpSet, PathMatch, Rule},
        common::{cluster_nodes_tweaked, fake_sdk::FakeSdk, load_of, single_leaders, wait_probe},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let ttl = Duration::from_secs(1);
    let (_bucket, nodes) =
        cluster_nodes_tweaked(&["node-alpha", "node-beta"], 2, ttl, tweak).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "one range on each node",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let range_of = |n: usize| *leaders.iter().find(|(_, i)| **i == n).expect("has range").0;
    let (r_a, r_b) = (range_of(0), range_of(1));
    let (a, b) = (&nodes[0], &nodes[1]);

    // 12 keys of `B`, ordered by pk; `at` lies between the 6th and 7th keys.
    let pk = |k: &str| {
        cairn_store::partition_key_for_target(
            &cairn_store::TenantId::default(),
            None,
            "Far",
            k,
            None,
        )
        .expect("object has a key")
        .0
    };
    let mut on_b: Vec<String> = (0..12)
        .map(|i| common::key_on_partition(&b.node, "Far", r_b, &format!("far-{i}")))
        .collect();
    on_b.sort_by_key(|k| pk(k));
    let at = pk(&on_b[5]) + (pk(&on_b[6]) - pk(&on_b[5])) / 2 + 1;
    let (low, high) = on_b.split_at(6);
    let keys: Vec<String> = low
        .iter()
        .zip(high)
        .flat_map(|(l, h)| [l.clone(), h.clone()])
        .collect();
    let caller_key = common::key_on_partition(&a.node, "Caller", r_a, "caller");
    let callers = FakeSdk::builder()
        .object("Caller", "fan", fan_to_far(&keys))
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![common::fake_sdk::Step::Suspend])
        .spawn()
        .await?;
    let (a_url, b_url) = (a.node.url(), b.node.url());
    let r = within(Duration::from_secs(120), "split mid gather/send", async {
        for n in &nodes {
            common::register(&n.node.url(), &callers.url).await?;
        }
        let (code, body) = common::send(&a_url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&a_url, &id, "suspended").await?;
        let wrong0 = start_calls(&a_url, "wrong_range").await?;

        a.store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("cluster.conf".into()),
            Action::Fail { times: None },
        ));
        let old_version = a.node.cluster.version();
        let paused = ctl.pause_at_on(&["node-alpha"], "transport::send_batch");
        for n in &nodes {
            common::register(&n.node.url(), &far.url).await?;
        }
        paused.expect_stopped(Duration::from_secs(20)).await;

        let (code, view) = post_json(format!("{b_url}/ranges/{r_b}/split?at={at:016x}")).await?;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        ensure!(
            a.node.cluster.version() == old_version,
            "alpha must still hold the old table"
        );
        paused.resume();

        let n = far_settles(&far, keys.len()).await?;
        ensure!(n == keys.len(), "each Far exactly once: {n}");
        let table = b.node.cluster.table();
        ensure!(
            a.node.cluster.version() == table.version(),
            "alpha must have learned the post-split table: {} / {}",
            a.node.cluster.version(),
            table.version()
        );
        for s in far.starts_of("Far", "hold") {
            let p = common::partition_of(&b_url, &s.id_hex()).await?;
            let raw = cairn_store::id_from_hex(&s.id_hex()).expect("id hex");
            let want = table
                .partition_of_key(cairn_store::partition_key_of(&raw))
                .0;
            ensure!(
                p != r_b && p == want,
                "Far {} on range {p}, must be on child {want}",
                s.id_hex()
            );
        }
        ensure!(
            start_calls(&a_url, "wrong_range").await? == wrong0,
            "the batch must be regrouped within the send, not return wrong_range to the drainer"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A node holding a stale table, unable to load `cluster.conf`, still learns the new
/// table over the network after the target range has left the table.**
///
/// Interleaving:
/// 1. A two-node cluster, one range per node: `Caller` on alpha's range `A`; 4 `Far`
///    keys on beta's range `B`, 2 below and 2 above point `at`. Register `Caller` and
///    `Far` first.
/// 2. Alpha loses its table reload path (every GET of `cluster.conf` fails).
/// 3. Split `B` at `at` on beta: `B` leaves the table (its owner record becomes a
///    `released` tombstone of beta's session, still alive), two children `L`, `R` on
///    beta.
/// 4. Wait until alpha's routing table (built on the **old** table) has no leader left
///    for `B`: beta's `owns` hint no longer has `B`, the owner record is released. This
///    is the window of the bug: before the fix alpha returned `NoLeader(B)` forever,
///    sent nothing, and so never got a `409 wrong_range` to learn the table from.
/// 5. `Caller` issues 4 unawaited calls to `Far`.
///
/// Must see: each `Far` runs exactly once, in the child of its own half; alpha holds the
/// post-split table (learned from the rejection by beta — the owner that just released
/// `B`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_split_a_stale_node_learns_the_table_from_the_releaser() -> Result<()> {
    use {
        cairn_store::testkit::{Action, OpSet, PathMatch, Rule},
        common::{cluster_nodes, fake_sdk::FakeSdk, load_of, single_leaders, wait_probe},
        std::time::Duration,
    };
    // No failpoint is set; the controller is held only so as not to run at the same time
    // as other tests of this file (global registry, same node names).
    let _ctl = StepController::new();
    let ttl = Duration::from_secs(1);
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 2, ttl).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "one range on each node",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let range_of = |n: usize| *leaders.iter().find(|(_, i)| **i == n).expect("has range").0;
    let (r_a, r_b) = (range_of(0), range_of(1));
    let (a, b) = (&nodes[0], &nodes[1]);

    let pk = |k: &str| {
        cairn_store::partition_key_for_target(
            &cairn_store::TenantId::default(),
            None,
            "Far",
            k,
            None,
        )
        .expect("object has a key")
        .0
    };
    let mut keys: Vec<String> = (0..4)
        .map(|i| common::key_on_partition(&b.node, "Far", r_b, &format!("far-{i}")))
        .collect();
    keys.sort_by_key(|k| pk(k));
    let at = pk(&keys[1]) + (pk(&keys[2]) - pk(&keys[1])) / 2 + 1;
    let caller_key = common::key_on_partition(&a.node, "Caller", r_a, "caller");
    let callers = FakeSdk::builder()
        .object("Caller", "fan", fan_to_far(&keys))
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![common::fake_sdk::Step::Suspend])
        .spawn()
        .await?;
    let (a_url, b_url) = (a.node.url(), b.node.url());
    let r = within(
        Duration::from_secs(90),
        "stale table learned from the releasing owner",
        async {
            for n in &nodes {
                common::register(&n.node.url(), &callers.url).await?;
                common::register(&n.node.url(), &far.url).await?;
            }
            a.store.add_rule(Rule::new(
                OpSet::GET,
                PathMatch::Contains("cluster.conf".into()),
                Action::Fail { times: None },
            ));
            let old_version = a.node.cluster.version();
            let (code, view) =
                post_json(format!("{b_url}/ranges/{r_b}/split?at={at:016x}")).await?;
            ensure!(code == StatusCode::OK, "split: {code} {view}");

            // 4. The window: alpha still has the old table and no leader left for `B`.
            let t0 = std::time::Instant::now();
            while a
                .node
                .routing
                .leader_of(cairn_store::PartitionId(r_b))
                .is_some()
            {
                ensure!(
                    t0.elapsed() < Duration::from_secs(20),
                    "alpha's routing table still points B at {:?}",
                    a.node.routing.leader_of(cairn_store::PartitionId(r_b))
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            ensure!(
                a.node.cluster.version() == old_version,
                "alpha must still hold the old table"
            );

            // 5.
            let (code, body) =
                common::send(&a_url, "Caller", "fan", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let n = far_settles(&far, keys.len()).await?;
            ensure!(n == keys.len(), "each Far exactly once: {n}");
            let table = b.node.cluster.table();
            ensure!(
                a.node.cluster.version() == table.version(),
                "alpha must have learned the post-split table: {} / {}",
                a.node.cluster.version(),
                table.version()
            );
            for s in far.starts_of("Far", "hold") {
                let p = common::partition_of(&b_url, &s.id_hex()).await?;
                let raw = cairn_store::id_from_hex(&s.id_hex()).expect("id hex");
                let want = table
                    .partition_of_key(cairn_store::partition_key_of(&raw))
                    .0;
                ensure!(
                    p != r_b && p == want,
                    "Far {} on range {p}, must be on child {want}",
                    s.id_hex()
                );
            }
            Ok(())
        },
    )
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A `spread` call keeps exactly one callee across a failover** (IR-9).
///
/// Interleaving:
/// 1. A **new** cluster (an empty bucket ⇒ `calls/prefix` and `calls/placement` on from
///    the start). Node A alone, 4 ranges. Failpoint `turn::deliver_to_caller::call_id`
///    = `return` on A and B: every delivery of `CallInvocationId` fails — the `Call`
///    outbox row stays after the callee exists.
/// 2. `Caller/run` (its idempotency key chosen so that the callee under the old rule
///    falls into another range) issues `ctx.call(Leaf/echo, { placement: "spread" })`
///    without awaiting it, then `ctx.sleep(SLEEP_MS)`. The caller carries the
///    `calls/placement` mark ⇒ a v5 row, the old rule. The drainer sends `StartCall`,
///    callee X (old-rule id) runs and suspends waiting for a signal; the id delivery
///    fails ⇒ the row stays.
/// 3. A stops (the caller's timer has not fired yet); node B comes up and opens every
///    range.
/// 4. The timer fires on B: the journal replay turn — the state machine replays the
///    `Task::Invoke` of the call still waiting for its id, that is it **rewrites the
///    outbox row** on B.
/// 5. Remove the failpoint: B's drainer drains the row again.
///
/// Must see: callee X has **the old-rule id** (spread, a range other than the
/// caller's); the caller receives exactly X; exactly **one** `Leaf` invocation in the
/// cluster — the rewrite on B yields exactly A's row (no callee under the colocation
/// rule). Before the fix the engine ignored `placement` ⇒ the callee followed the
/// colocation rule, and X never ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spread_call_keeps_its_callee_across_failover() -> Result<()> {
    /// Long enough for the caller's timer to fire after A has stopped (step 3).
    const SLEEP_MS: u64 = 4_000;
    use {
        common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
        std::time::Duration,
    };

    let ctl = StepController::new();
    let bucket = common::bucket();
    let start = |id: &'static str| {
        let bucket = &bucket;
        async move {
            let n = common::start_cluster_node(bucket, id, 4, Duration::from_secs(2)).await?;
            common::wait_probe(
                &[&n],
                Duration::from_secs(20),
                "node is leader of every range",
                |p| p.len() == 4 && p.values().all(|v| v == &[0]),
            )
            .await?;
            Ok::<_, anyhow::Error>(n)
        }
    };
    let a = start("node-a").await?;
    let sdk = FakeSdk::builder()
        .service(
            "Caller",
            "run",
            vec![
                Step::Spawn {
                    service: "Leaf".into(),
                    handler: "echo".into(),
                    spread: true,
                },
                Step::Sleep(SLEEP_MS),
                Step::AwaitSpawned,
                Step::EchoCallResult,
            ],
        )
        .service("Leaf", "echo", vec![Step::Suspend, Step::output("la")])
        .spawn()
        .await?;
    let deliver = ctl.fail_on(
        &["node-a", "node-b"],
        "turn::deliver_to_caller::call_id",
        "return",
    );
    let table = a.node.cluster.table();
    let (idem, caller, callee) = (0..1000)
        .map(|i| {
            let idem = format!("left-{i}");
            let (caller, callee) = caller_and_old_rule_callee("Caller", &idem, "Leaf");
            (idem, caller, callee)
        })
        .find(|(_, caller, callee)| {
            table.partition_of_key(cairn_store::partition_key_of(caller))
                != table.partition_of_key(cairn_store::partition_key_of(callee))
        })
        .ok_or_else(|| anyhow!("found no caller with a callee in another range"))?;
    let (caller, callee) = (cairn_store::id_hex(&caller), cairn_store::id_hex(&callee));

    // 1–2.
    let url = a.node.url();
    let r = within(
        Duration::from_secs(60),
        "spread call with a remote callee",
        async {
            let cv: Value = reqwest::get(format!("{url}/admin/cluster-version"))
                .await?
                .json()
                .await?;
            ensure!(
                cv["callPlacement"] == true,
                "a new cluster must have calls/placement on from the start: {cv}"
            );
            common::register(&url, &sdk.url).await?;
            let resp = Client::new()
                .post(format!("{url}/invoke/Caller/run/send"))
                .header("Idempotency-Key", &idem)
                .body("x")
                .send()
                .await?;
            let code = resp.status();
            let body: Value = resp.json().await?;
            ensure!(code.is_success(), "{code} {body}");
            ensure!(
                body["id"] == caller.as_str(),
                "caller id differs from the computation: {body}"
            );
            let t0 = std::time::Instant::now();
            while !sdk
                .starts_of("Leaf", "echo")
                .iter()
                .any(|s| s.id_hex() == callee)
            {
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "old-rule callee {callee} has not run: {:?}",
                    sdk.starts_of("Leaf", "echo")
                        .iter()
                        .map(|s| s.id_hex())
                        .collect::<Vec<_>>()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // A few more drain rounds go through the window (resending `StartCall`, the id delivery failing).
            tokio::time::sleep(Duration::from_millis(300)).await;
            let (_, st) = common::invocation(&url, &caller).await?;
            ensure!(
                st["status"] != "completed",
                "the caller finished even though the id delivery failed: {st}"
            );
            journal_len(&url, &caller).await
        },
    )
    .await;
    let slept_at = match r {
        Ok(n) => n,
        Err(e) => {
            a.node.shutdown().await;
            return Err(e);
        }
    };

    // 3.
    let still = journal_len(&url, &caller).await;
    a.node.shutdown().await;
    ensure!(
        still? == slept_at,
        "the caller's timer fired on A — the replay turn must run on B; raise SLEEP_MS"
    );
    let b = start("node-b").await?;
    let url = b.node.url();
    let r = within(Duration::from_secs(60), "redrain after failover", async {
        // 4.
        let t0 = std::time::Instant::now();
        while journal_len(&url, &caller).await? <= slept_at {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the caller's timer has not fired on B yet"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        common::wait_status(&url, &caller, "suspended").await?;
        // 5.
        drop(deliver);
        let resp = Client::new()
            .post(format!("{url}/invocations/{callee}/signal"))
            .json(&json!({ "signal": HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(
            resp.status() == StatusCode::OK,
            "signal callee: {}",
            resp.text().await?
        );
        let st = common::wait_status(&url, &caller, "completed").await?;
        ensure!(st["output"] == "la", "{st}");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let leaves: std::collections::BTreeSet<String> = sdk
            .starts_of("Leaf", "echo")
            .iter()
            .map(|s| s.id_hex())
            .collect();
        ensure!(
            leaves.len() == 1 && leaves.contains(&callee),
            "exactly one callee must run, with the old-rule id {callee}: got {leaves:?}"
        );
        // A second callee may have been created without running: count the `Leaf`
        // invocations on every range (B leads them all), and the id the caller received.
        let rows: Vec<Value> = reqwest::get(format!("{url}/invocations?limit=1000"))
            .await?
            .json()
            .await?;
        let leaf_rows: Vec<&Value> = rows.iter().filter(|r| r["service"] == "Leaf").collect();
        ensure!(
            leaf_rows.len() == 1 && leaf_rows[0]["id"] == callee.as_str(),
            "there must be exactly one Leaf invocation ({callee}): {leaf_rows:?}"
        );
        let journal: Value = reqwest::get(format!("{url}/invocations/{caller}/journal"))
            .await?
            .json()
            .await?;
        let ascii_hex = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        ensure!(
            journal
                .as_array()
                .into_iter()
                .flatten()
                .any(|e| e["payload"]
                    .as_str()
                    .is_some_and(|p| p.contains(&ascii_hex(&callee)))),
            "the caller must receive id {callee}: {journal}"
        );
        Ok(())
    })
    .await;
    b.node.shutdown().await;
    r
}

/// **Dying mid-step of the `calls/placement` gate** (AGENTS.md §2, the migration
/// framework row: a new step in `MIGRATIONS` comes with a dying-mid-step test).
///
/// The gate's step does not run at startup; its "step" is the command
/// `cairn-ctl cluster enable-call-prefix`. Interleaving:
/// 1. An old cluster: `cluster-version` `min_binary=2`, no gate on, every session
///    knows step 5.
/// 2. `--apply` CASes `min_binary=5`, then dies at
///    `ctl::enable_call_prefix::raised` — before the `BOOT_WAIT` wait and before the
///    CAS that enables the gate.
/// 3. Run the command again.
///
/// Must see: after 2, `min_binary=5` already blocks a step-4 binary (`ensure_known`)
/// and **no** gate is on yet (the engine has marked no caller); after 3, both gates are
/// on in one write, and the rerun does **not** wait `BOOT_WAIT` again (no old binary can
/// have passed the check since step 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_call_prefix_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, CALL_PLACEMENT, CALL_PREFIX},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from("cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n"),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-call-prefix --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_call_prefix::raised", "1*return(die)");
        let r = cli.enable_call_prefix(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == CALL_PLACEMENT && !cv.call_prefix && !cv.call_placement,
        "died mid-step: min_binary set, gate not on yet — {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), CALL_PREFIX)
            .await
            .is_err(),
        "a step-4 binary passed the check after min_binary=5 was written"
    );
    let rep = cli.enable_call_prefix(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        !rep.cleanup.iter().any(|c| c.contains("waited")),
        "the rerun must not wait BOOT_WAIT again: {rep:?}"
    );
    let cv = cv_now().await?;
    ensure!(
        cv.call_prefix_on() && cv.call_placement_on() && cv.min_binary == CALL_PLACEMENT,
        "{cv:?}"
    );
    Ok(())
}

/// **Dying mid-step of the `streams/table` gate** (AGENTS.md §2, the migration
/// framework row: a new step in `MIGRATIONS` comes with a dying-mid-step test). The
/// gate's "step" is `cairn-ctl cluster enable-streams`.
///
/// 1. An old cluster: `min_binary=5`, the call gates on, `streams` not yet.
/// 2. `--apply` CASes `min_binary=6`, then dies at `ctl::enable_streams::raised`.
/// 3. Run the command again.
///
/// Must see: after 2, `min_binary=6` blocks a step-5 binary (it would leak `Streams`
/// rows on purge) and the gate is **not** on yet; after 3, `streams_on()`, no
/// `BOOT_WAIT` wait again, and both call gates intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_streams_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, CALL_PLACEMENT, STREAMS_TABLE},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=5\ncall_prefix=on\ncall_placement=on\n",
            ),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-streams --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_streams::raised", "1*return(die)");
        let r = cli.enable_streams(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == STREAMS_TABLE && !cv.streams && !cv.streams_on(),
        "died mid-step: min_binary set, gate not on yet — {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), CALL_PLACEMENT)
            .await
            .is_err(),
        "a step-5 binary passed the check after min_binary=6 was written"
    );
    let rep = cli.enable_streams(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        !rep.cleanup.iter().any(|c| c.contains("waited")),
        "the rerun must not wait BOOT_WAIT again: {rep:?}"
    );
    let cv = cv_now().await?;
    ensure!(
        cv.streams_on() && cv.call_prefix_on() && cv.call_placement_on(),
        "{cv:?}"
    );
    Ok(())
}

/// **Dying mid-step of the `schedules/table` gate** (AGENTS.md §2, the migration
/// framework row: a new step in `MIGRATIONS` comes with a dying-mid-step test). The
/// gate's "step" is `cairn-ctl cluster enable-schedules`.
///
/// 1. An old cluster: `min_binary=6`, the call and durable stream gates on,
///    `schedules` not yet.
/// 2. `--apply` CASes `min_binary=7`, then dies at `ctl::enable_schedules::raised`.
/// 3. Run the command again.
///
/// Must see: after 2, `min_binary=7` blocks a step-6 binary (it does not fire
/// schedules) and the gate is **not** on yet — the API still refuses `PUT /schedules`;
/// after 3, `schedules_on()`, no `BOOT_WAIT` wait again, the other gates intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_schedules_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, SCHEDULES_TABLE, STREAMS_TABLE},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=6\ncall_prefix=on\ncall_placement=on\nstreams=on\n",
            ),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-schedules --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_schedules::raised", "1*return(die)");
        let r = cli.enable_schedules(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == SCHEDULES_TABLE && !cv.schedules && !cv.schedules_on(),
        "died mid-step: min_binary set, gate not on yet — {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), STREAMS_TABLE)
            .await
            .is_err(),
        "a step-6 binary passed the check after min_binary=7 was written"
    );
    let rep = cli.enable_schedules(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        !rep.cleanup.iter().any(|c| c.contains("waited")),
        "the rerun must not wait BOOT_WAIT again: {rep:?}"
    );
    let cv = cv_now().await?;
    ensure!(
        cv.schedules_on() && cv.streams_on() && cv.call_prefix_on() && cv.call_placement_on(),
        "{cv:?}"
    );
    Ok(())
}

/// **Dying between the two CASes of the `auth/keys-v2` gate** (AGENTS.md §2, the
/// migration framework row: a new step in `MIGRATIONS` comes with a dying-mid-step
/// test). The gate's "step" is `cairn-ctl cluster enable-key-roles`.
///
/// 1. An old cluster: `min_binary=7`, every earlier gate on, `key_roles` not yet; key
///    registry `cairnflow-keys/1` with one `admin` key.
/// 2. `--apply` CASes `min_binary=8`, then dies at `ctl::enable_key_roles::raised`.
/// 3. Create a `provision` key with `cairn-ctl keys create`.
/// 4. Run the enable command again, then create the `provision` key again.
///
/// Must see: after 2, `min_binary=8` blocks a step-7 binary (it would misread a `/2`
/// registry) and the gate is **not** on yet; (3) is refused and the registry is still
/// `/1` — writers only write `/2` once the gate is fully on, not on `min_binary` alone;
/// after (4) the gate is on, no `BOOT_WAIT` wait again, the other gates intact, the
/// registry moved to `/2` with both keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_key_roles_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, KEY_ROLES, SCHEDULES_TABLE},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=7\ncall_prefix=on\ncall_placement=on\nstreams=on\nschedules=on\n",
            ),
        )
        .await?;
    let admin_line = format!("k_00 {} admin 1 acme", "ab".repeat(32));
    store
        .put(
            &ObjPath::from("auth/keys"),
            PutPayload::from(format!("cairnflow-keys/1\n{admin_line}\n")),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-key-roles --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let keys_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(String::from_utf8(
                store
                    .get(&ObjPath::from("auth/keys"))
                    .await?
                    .bytes()
                    .await?
                    .to_vec(),
            )?)
        }
    };
    {
        let _die = ctl.fail("ctl::enable_key_roles::raised", "1*return(die)");
        let r = cli.enable_key_roles(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == KEY_ROLES && !cv.key_roles_on(),
        "died mid-step: min_binary set, gate not on yet — {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), SCHEDULES_TABLE)
            .await
            .is_err(),
        "a step-7 binary passed the check after min_binary=8 was written"
    );
    ensure!(
        cli.create_key("default", "provision", &opts).await.is_err(),
        "provision key created while the gate is not fully on"
    );
    ensure!(keys_now().await? == format!("cairnflow-keys/1\n{admin_line}\n"));
    let rep = cli.enable_key_roles(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        !rep.cleanup.iter().any(|c| c.contains("waited")),
        "the rerun must not wait BOOT_WAIT again: {rep:?}"
    );
    let cv = cv_now().await?;
    ensure!(
        cv.key_roles_on()
            && cv.schedules_on()
            && cv.streams_on()
            && cv.call_prefix_on()
            && cv.call_placement_on(),
        "{cv:?}"
    );
    let made = cli.create_key("default", "provision", &opts).await?;
    let keys = keys_now().await?;
    ensure!(
        keys.starts_with("cairnflow-keys/2\n")
            && keys.contains(&admin_line)
            && keys.contains(&format!("{} ", made.id))
            && keys.contains(" provision "),
        "{keys}"
    );
    Ok(())
}

// ─────────────── /invoke: errors at admission keep their shape

/// **An admission error of `/invoke` keeps its shape: the shared error body with the
/// id, at once — no `202`, no waiting.**
///
/// The `/invoke` waiting path only changes a turn that fails **after** admit returns
/// `Created` (the invocation exists, the `waiting` answer still has to go through
/// `finish`'s durable ticket). An error of admit itself — the invocation may not exist
/// yet, an ambiguous outcome for the caller — still goes straight out as before.
///
/// Order:
/// 1. A one-range node, a fake SDK with `Svc/once`, registered.
/// 2. `writer::commit::before_write` = `return` while a `POST /invoke/Svc/once` is in
///    flight: the commit creating the invocation in admit fails before writing.
/// 3. Remove the point.
///
/// Must see: step 2 returns the shared error body with the id, no `status`, before the
/// 5 s wait cap; `GET /invocations/{id}` is `404` (nothing created); the handler does
/// not run. The failpoint's error is a store error (`StoreError::Io`), so it is a
/// `503 store_unavailable` whose body hides the details (formerly: a `502` with the
/// error verbatim).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_invoke_admit_error_is_answered_at_once() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Svc", "once", vec![Step::output("1")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "admission error", async {
        common::register(&url, &sdk.url).await?;
        let asked = std::time::Instant::now();
        let (code, body) = {
            let _fail = ctl.fail("writer::commit::before_write", "return");
            common::invoke(&url, "Svc", "once", None, "{}").await?
        };
        let answered_in = asked.elapsed();
        ensure!(
            code == 503
                && body.get("status").is_none()
                && body["code"] == "store_unavailable"
                && !body["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("writer::commit::before_write")),
            "admission error: /invoke must return 503 store_unavailable with a hiding body, got {code} {body}"
        );
        let id = body["id"]
            .as_str()
            .ok_or_else(|| anyhow!("error with an id: {body}"))?
            .to_owned();
        ensure!(
            answered_in < std::time::Duration::from_secs(4),
            "an admission error goes out at once, without waiting: {answered_in:?}"
        );
        let (code, st) = common::invocation(&url, &id).await?;
        ensure!(
            code == 404,
            "the create commit failed before writing: nothing exists, got {code} {st}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r?;
    ensure!(
        sdk.starts_of("Svc", "once").is_empty(),
        "an invocation not created means the handler does not run"
    );
    Ok(())
}

// ─────────────────────────────── pinned before changing the latency floor

/// Counts how many times the thread goes through a failpoint (callback), until the guard is dropped.
fn count_hits(
    name: &str,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicU64>,
    cairn_store::testkit::FailGuard,
) {
    let n = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let c = std::sync::Arc::clone(&n);
    let guard = cairn_store::testkit::FailGuard::callback(name, move || {
        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });
    (n, guard)
}

fn hits(n: &std::sync::atomic::AtomicU64) -> u64 {
    n.load(std::sync::atomic::Ordering::SeqCst)
}

/// Waits for `ok` to hold, polling every 10 ms, capped at `limit`.
async fn until(limit: std::time::Duration, what: &str, ok: impl Fn() -> bool) -> Result<()> {
    let t0 = std::time::Instant::now();
    while !ok() {
        ensure!(t0.elapsed() < limit, "over {limit:?} and still not: {what}");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Ok(())
}

/// A cluster node, two ranges, leading both; `Caller/run` on range 0 calls `Callee/echo`
/// on range 1 (the SDK holds the callee's turn at the fake SDK's gate), then returns
/// the call's result. Returns (node, SDK, caller key).
///
/// The shared WAL log PUT cap is raised to [`GATED_PUT_TIMEOUT`]: the test closes the
/// `nodewal/` PUT gate of the whole node, and a gate closed past `stall_max` (default
/// 2 s × 3) makes the stream rotate — the ranges step down, undurable commits are lost,
/// the callee's turn reruns and stands forever at a fake SDK gate with no permits left
/// (measured under load). That is a deliberate failure path of the shared WAL, not what
/// the test checks.
async fn call_across_solo(
    bucket: &common::Bucket,
) -> Result<(common::ClusterNode, common::fake_sdk::FakeSdk, String)> {
    use common::fake_sdk::{FakeSdk, Step};
    let n = common::start_cluster_node_tweaked(
        bucket,
        "node-solo",
        2,
        std::time::Duration::from_secs(2),
        |cfg| cfg.node_wal.put_timeout = GATED_PUT_TIMEOUT,
    )
    .await?;
    common::wait_probe(
        &[&n],
        std::time::Duration::from_secs(20),
        "node is leader of every range",
        |p| p.len() == 2 && p.values().all(|v| v == &[0]),
    )
    .await?;
    let caller_key = common::key_on_partition(&n.node, "Caller", 0, "caller");
    let callee_key = common::key_on_partition(&n.node, "Callee", 1, "callee");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some(&callee_key)),
                Step::EchoCallResult,
            ],
        )
        .gated_object("Callee", "echo", vec![Step::output("pong")])
        .spawn()
        .await?;
    common::register(&n.node.url(), &sdk.url).await?;
    Ok((n, sdk, caller_key))
}

/// Keep the shared WAL log PUT gate closed this long after the drainer has reached the ticket wait.
const UNDURABLE_HOLD: std::time::Duration = std::time::Duration::from_millis(500);

/// The shared WAL log PUT cap of [`call_across_solo`]: `stall_max` = 3 × this cap
/// exceeds the 60 s cap of every test that uses it, so the test's gate never makes the
/// stream rotate.
const GATED_PUT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The cap on reading the caller's status while the WAL gate is closed. The read goes
/// through `finish` (waiting for the durable ticket of the caller's range), so it can
/// only answer when range 0 has no write left standing at the gate.
const GATED_READ: std::time::Duration = std::time::Duration::from_secs(5);

/// **IR-1 — the other side only sees a message once the outbox row is durable**
/// (previously the IR-1 cell only had a test of releasing the engine).
///
/// Interleaving:
/// 1. One node, two ranges. `Caller/run` (range 0) calls `Callee/echo` (range 1); the
///    fake SDK holds the callee's turn at its gate.
/// 2. The callee's turn has reached the SDK (`Start` arrived) ⇒ its `Running` commit is
///    durable (FL-17). Range 0 has delivered the call id to the caller
///    (`turn::deliver_to_caller::call_id`) and the caller is `suspended` as read
///    through `finish` ⇒ every write of range 0 up to then is durable. Close the node's
///    shared WAL log PUT gate: from here on no write becomes durable.
/// 3. Release the SDK: the callee returns `pong` ⇒ the end-of-turn commit writes the
///    output and a `Reply` outbox row (to the caller on range 0 — no local key needed)
///    into the memtable, not durable.
/// 4. Range 1's drainer (nudged by the background tick) gathers the `Reply` row and
///    reaches the ticket wait (`turn::drain_outbox::before_sync`). Keep the gate closed
///    [`UNDURABLE_HOLD`] longer, then read the caller's status (range 0 has no write
///    standing at the gate, so the read through `finish` can answer — capped at
///    [`GATED_READ`]).
/// 5. Open the gate.
///
/// Must see: at (4) no send (`turn::drain_outbox::before_send`) and the caller not
/// finished; after (5) the caller finishes with `pong`, exactly one callee.
///
/// Why (2) waits for range 0 too: the callee's `Start` reaching the SDK does **not**
/// imply that the caller's id delivery commit (or `suspended`) is durable — under CPU
/// load it may still be in the memtable when the gate closes. Then the read at (4)
/// waits for the very gate the test only opens after the read ⇒ a self-deadlock;
/// formerly the gate stood until `stall_max`, the WAL stream rotated, both ranges
/// stepped down, the callee reran and stood at the fake SDK's gate ⇒ a full 60 s hang
/// (measured under load).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_peer_never_sees_a_row_before_it_is_durable() -> Result<()> {
    use cairn_store::testkit::{Action, Gate, Rule};
    use std::time::Duration;
    let _ctl = StepController::new();
    let bucket = common::bucket();
    let (n, sdk, caller_key) = call_across_solo(&bucket).await?;
    let url = n.node.url();
    let r = within(Duration::from_secs(60), "IR-1 on the Reply path", async {
        let (syncs, _s) = count_hits("turn::drain_outbox::before_sync");
        let (sends, _t) = count_hits("turn::drain_outbox::before_send");
        let (call_ids, _c) = count_hits("turn::deliver_to_caller::call_id");
        let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let caller = body["id"].as_str().unwrap_or_default().to_string();
        // 2.
        until(Duration::from_secs(10), "callee turn at the SDK", || {
            sdk.starts_of("Callee", "echo").len() == 1
        })
        .await?;
        until(
            Duration::from_secs(10),
            "range 0 delivers the call id",
            || hits(&call_ids) > 0,
        )
        .await?;
        common::wait_status(&url, &caller, "suspended").await?;
        let gate = Gate::closed();
        let rule = n
            .store
            .add_rule(Rule::nodewal_put(None, Action::Gate(gate.clone())));
        let (syncs0, sends0) = (hits(&syncs), hits(&sends));
        // 3.
        sdk.gate().release(1);
        // 4.
        until(
            Duration::from_secs(10),
            "the drainer gathers an undurable Reply row",
            || hits(&syncs) > syncs0,
        )
        .await?;
        let t0 = std::time::Instant::now();
        while t0.elapsed() < UNDURABLE_HOLD {
            ensure!(
                hits(&sends) == sends0,
                "the drainer sent a message while its outbox row was not durable — IR-1 broken"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        ensure!(
            gate.blocked() > 0,
            "no write standing at the WAL gate — empty scenario"
        );
        let (_, st) = tokio::time::timeout(GATED_READ, common::invocation(&url, &caller))
            .await
            .map_err(|_| {
                anyhow!(
                    "reading the caller took over {GATED_READ:?} with the WAL gate closed — range 0 still \
                     has writes at the gate ({} writes), the scenario drifted from (2)",
                    gate.blocked()
                )
            })??;
        ensure!(
            st["status"] != "completed",
            "the caller finished while the callee was not durable: {st}"
        );
        // 5.
        gate.open();
        n.store.remove_rule(rule);
        let st = common::wait_status(&url, &caller, "completed").await?;
        ensure!(st["output"] == "pong", "{st}");
        let callees: std::collections::BTreeSet<String> = sdk
            .starts_of("Callee", "echo")
            .iter()
            .map(|s| s.id_hex())
            .collect();
        ensure!(callees.len() == 1, "exactly one callee: {callees:?}");
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **A write carrying an outbox row loses its handle mid-term.**
/// `writer::commit::after_submit` makes `commit` return an error **after** the batch has
/// been submitted — the shape of a future cancelled there: SlateDB still applies the
/// batch, the writer's `latest` does not take it.
///
/// Interleaving: as in [`remote_peer_never_sees_a_row_before_it_is_durable`]
/// (1)–(2), then:
/// 3. Enable `after_submit` once for a batch with an outbox row; release the SDK: the
///    callee's end-of-turn commit (output + a `Reply` row) sits in the memtable, not
///    durable, and `commit` returns an error.
/// 4. Range 1's drainer gathers the `Reply` row and reaches the ticket wait. Keep the
///    gate closed [`UNDURABLE_HOLD`] longer.
/// 5. While the gate is still closed, release range 1 (`resign` ⇒ `seal`); then open
///    the gate.
///
/// Must see: at (4) no send; after (5) `seal` done, the caller finishes with `pong`,
/// exactly one callee **id**, and the callee `completed` on the new leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_outbox_commit_is_not_sent_before_it_is_durable() -> Result<()> {
    use cairn_store::testkit::{Action, Gate, Rule};
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, sdk, caller_key) = call_across_solo(&bucket).await?;
    let url = n.node.url();
    let r = within(
        Duration::from_secs(60),
        "write losing its handle on the Reply path",
        async {
            let (syncs, _s) = count_hits("turn::drain_outbox::before_sync");
            let (sends, _t) = count_hits("turn::drain_outbox::before_send");
            let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let caller = body["id"].as_str().unwrap_or_default().to_string();
            until(Duration::from_secs(10), "callee turn at the SDK", || {
                sdk.starts_of("Callee", "echo").len() == 1
            })
            .await?;
            let callee = sdk.starts_of("Callee", "echo")[0].id_hex();
            let gate = Gate::closed();
            let rule = n
                .store
                .add_rule(Rule::nodewal_put(None, Action::Gate(gate.clone())));
            let (syncs0, sends0) = (hits(&syncs), hits(&sends));
            // 3. The point fires (a batch with an outbox row ⇒ error, applied):
            // `cairn-store/tests/failpoints.rs::submitted_outbox_batch_with_lost_handle_…`.
            let _fail = ctl.fail("writer::commit::after_submit", "1*return(cancel)");
            sdk.gate().release(1);
            // 4.
            until(
                Duration::from_secs(10),
                "the drainer gathers an undurable Reply row",
                || hits(&syncs) > syncs0,
            )
            .await?;
            let t0 = std::time::Instant::now();
            while t0.elapsed() < UNDURABLE_HOLD {
                ensure!(
                    hits(&sends) == sends0,
                    "the drainer sent the row of a handle-less write before it was durable — LF-3/IR-1 broken"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // 5.
            let h = n
                .node
                .partitions
                .get(cairn_store::PartitionId(1))
                .ok_or_else(|| anyhow!("range 1 has no engine"))?;
            let (seals, _v) = count_hits("writer::seal::before_flush");
            let resigning = tokio::spawn(async move { h.resign().await });
            until(
                Duration::from_secs(10),
                "range 1 reaches the seal with the gate still closed",
                || hits(&seals) > 0,
            )
            .await?;
            gate.open();
            n.store.remove_rule(rule);
            let sealed = tokio::time::timeout(Duration::from_secs(10), resigning).await??;
            ensure!(sealed.is_ok(), "seal failed: {sealed:?}");
            let st = common::wait_status(&url, &caller, "completed").await?;
            ensure!(st["output"] == "pong", "{st}");
            let callees: std::collections::BTreeSet<String> = sdk
                .starts_of("Callee", "echo")
                .iter()
                .map(|s| s.id_hex())
                .collect();
            ensure!(callees.len() == 1, "exactly one callee: {callees:?}");
            let st = common::wait_status(&url, &callee, "completed").await?;
            ensure!(st["output"] == "pong", "callee after seal: {st}");
            Ok(())
        },
    )
    .await;
    n.node.shutdown().await;
    r
}

/// **The drainer acts on undurable messages — an interleaving that predates the
/// latency-floor work.** `on_inbound_batch` releases the engine while waiting for the
/// commit that accepts messages to become durable; the background tick nudges the
/// drainer, and the drainer reads inbound rows at the `Memory` level.
///
/// Interleaving:
/// 1. Two nodes alpha, beta; two ranges, one per node. `Caller/run` on alpha's range
///    calls `Callee/echo` on beta's range.
/// 2. Close beta's shared WAL log PUT gate; send `Caller/run`.
/// 3. Alpha's drainer sends `StartCall`; beta submits the accepting commit (not
///    durable) and waits. Beta's drainer creates the callee from the undurable inbound
///    row and dispatches the turn: the callee's turn gets past the `Running` commit
///    (`turn::after_running`, only on beta), not durable ⇒ the SDK has not been called.
/// 4. Freeze beta (every request to the store hangs — a `kill -9` as seen from the
///    bucket): beta loses its lease, alpha claims beta's range; no write of window (3)
///    reaches the store.
///
/// Must see: at (3) the callee has not run its SDK; alpha has no `200` so it resends to
/// the new leader; the caller finishes with `pong`, the callee runs its SDK **exactly
/// once**.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drained_inbound_on_a_lost_leader_runs_once() -> Result<()> {
    use {
        cairn_store::testkit::{Action, Gate, Rule},
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let _ctl = StepController::new();
    let (_bucket, nodes) =
        common::cluster_nodes(&["node-alpha", "node-beta"], 2, Duration::from_secs(2)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let r = within(
        Duration::from_secs(90),
        "undurable message on a leader that loses its lease",
        async {
            let leaders =
                common::wait_probe(&refs, Duration::from_secs(20), "balanced at 1/1", |p| {
                    common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1])
                })
                .await?;
            let leaders = common::single_leaders(&leaders).expect("has a leader");
            let on = |i: usize| *leaders.iter().find(|(_, n)| **n == i).expect("has range").0;
            let (pa, pb) = (on(0), on(1));
            let (alpha, beta) = (&nodes[0], &nodes[1]);
            let caller_key = common::key_on_partition(&alpha.node, "Caller", pa, "caller");
            let callee_key = common::key_on_partition(&alpha.node, "Callee", pb, "callee");
            let sdk = FakeSdk::builder()
                .object(
                    "Caller",
                    "run",
                    vec![
                        Step::call("Callee", "echo", Some(&callee_key)),
                        Step::EchoCallResult,
                    ],
                )
                .object("Callee", "echo", vec![Step::output("pong")])
                .spawn()
                .await?;
            for n in &nodes {
                common::register(&n.node.url(), &sdk.url).await?;
            }
            // 2.
            let gate = Gate::closed();
            beta.store.add_rule(Rule::nodewal_put(
                Some("node-beta"),
                Action::Gate(gate.clone()),
            ));
            let running = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let r2 = std::sync::Arc::clone(&running);
            let _running =
                cairn_store::testkit::FailGuard::callback("turn::after_running", move || {
                    r2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
                .on_nodes("turn::after_running", &["node-beta"]);
            let url = alpha.node.url();
            let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let caller = body["id"].as_str().unwrap_or_default().to_string();
            // 3.
            until(
                Duration::from_secs(10),
                "beta's drainer dispatches the callee's turn from an undurable message",
                || hits(&running) > 0,
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            ensure!(
                sdk.starts_of("Callee", "echo").is_empty(),
                "the callee's SDK was called while the Running commit (and the accepted message) were not durable"
            );
            // 4.
            common::freeze(&beta.store);
            common::wait_probe(
                &refs[..1],
                Duration::from_secs(20),
                "alpha claims beta's range",
                |p| p.get(&pb).is_some_and(|v| v.contains(&0)),
            )
            .await?;
            let st = common::wait_status(&url, &caller, "completed").await?;
            ensure!(st["output"] == "pong", "{st}");
            tokio::time::sleep(Duration::from_millis(500)).await;
            let starts = sdk.starts_of("Callee", "echo");
            ensure!(
                starts.len() == 1,
                "the callee's SDK must be called exactly once, got {}: {:?}",
                starts.len(),
                starts.iter().map(|s| s.id_hex()).collect::<Vec<_>>()
            );
            Ok(())
        },
    )
    .await;
    let mut nodes = nodes.into_iter();
    nodes.next().expect("alpha").node.shutdown().await;
    r
}

// ─────────────────────────── a narrow send ticket

/// **The drainer only waits for the commit holding the rows it sends, not for other
/// commits.**
///
/// Interleaving:
/// 1. Two nodes alpha, beta; one range each. `Caller/run` (alpha) calls `Callee/echo`
///    (beta) and suspends waiting for the result. Every send of alpha returns `503`
///    (`transport::send_once`, only alpha): the `Call` row stays.
/// 2. The caller is `suspended` (read through `/invocations` — the answer waits for
///    durability): the commit holding the `Call` row is durable. Close alpha's shared
///    WAL log PUT gate.
/// 3. Another invocation on the same range (`Other/noop`, `/send`): the commit creating
///    it (no rows to send) is submitted without becoming durable.
/// 4. Remove the send fault: the next drain round gathers the `Call` row again.
///
/// Must see: the callee runs (its SDK is called, on beta) **while** the gate is still
/// closed; open the gate ⇒ the caller finishes with `pong`, exactly one callee. Before
/// the fix: the drainer's ticket was the newest mark — the commit creating `Other` — ⇒
/// no message went out ⇒ red. (In the fake SDK's `REQUEST_RESPONSE` mode, the commit of
/// the `Call` row and the suspend commit follow each other with no wait in between, so
/// the end-of-turn commit itself cannot be separated.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_send_does_not_wait_for_the_turn_end_commit() -> Result<()> {
    use {
        cairn_store::testkit::{Action, Gate, Rule},
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let (_bucket, nodes) =
        common::cluster_nodes(&["node-alpha", "node-beta"], 2, Duration::from_secs(2)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let r = within(
        Duration::from_secs(60),
        "the send ticket does not wait for other commits",
        async {
            let leaders =
                common::wait_probe(&refs, Duration::from_secs(20), "balanced at 1/1", |p| {
                    common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1])
                })
                .await?;
            let leaders = common::single_leaders(&leaders).expect("has a leader");
            let on = |i: usize| *leaders.iter().find(|(_, n)| **n == i).expect("has range").0;
            let (alpha, pa, pb) = (&nodes[0], on(0), on(1));
            let caller_key = common::key_on_partition(&alpha.node, "Caller", pa, "caller");
            let callee_key = common::key_on_partition(&alpha.node, "Callee", pb, "callee");
            let other_key = common::key_on_partition(&alpha.node, "Other", pa, "other");
            let sdk = FakeSdk::builder()
                .object(
                    "Caller",
                    "run",
                    vec![
                        Step::call("Callee", "echo", Some(&callee_key)),
                        Step::EchoCallResult,
                    ],
                )
                .object("Callee", "echo", vec![Step::output("pong")])
                .object("Other", "noop", vec![Step::output("ok")])
                .spawn()
                .await?;
            for n in &nodes {
                common::register(&n.node.url(), &sdk.url).await?;
            }
            let url = alpha.node.url();
            // 1.
            let refuse = ctl.fail_on(&["node-alpha"], "transport::send_once", "return(503)");
            let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let caller = body["id"].as_str().unwrap_or_default().to_string();
            // 2.
            common::wait_status(&url, &caller, "suspended").await?;
            let gate = Gate::closed();
            alpha.store.add_rule(Rule::nodewal_put(
                Some("node-alpha"),
                Action::Gate(gate.clone()),
            ));
            // 3.
            let other = tokio::spawn({
                let url = url.clone();
                async move { common::send(&url, "Other", "noop", Some(&other_key), "x").await }
            });
            let t0 = std::time::Instant::now();
            while gate.blocked() == 0 {
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "the commit creating Other did not reach the WAL gate"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // 4.
            drop(refuse);
            // A short cap: holding the log PUT longer than `put_timeout` would make alpha
            // fail-stop and beta claim the range and send instead — not what is checked here.
            let held = until(
                Duration::from_secs(2),
                "the callee runs while another commit is not durable",
                || !sdk.starts_of("Callee", "echo").is_empty(),
            )
            .await;
            ensure!(!gate.is_open(), "the gate opened before the callee ran");
            gate.open();
            held?;
            other.await??;
            let st = common::wait_status(&url, &caller, "completed").await?;
            ensure!(st["output"] == "pong", "{st}");
            let callees: std::collections::BTreeSet<String> = sdk
                .starts_of("Callee", "echo")
                .iter()
                .map(|s| s.id_hex())
                .collect();
            ensure!(callees.len() == 1, "exactly one callee: {callees:?}");
            Ok(())
        },
    )
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **LF-3: a write carrying rows to send that loses its handle forces a barrier.**
///
/// Interleaving: as in [`cancelled_outbox_commit_is_not_sent_before_it_is_durable`] up
/// to step (4) — the callee's end-of-turn commit (output + a `Reply` row) is submitted
/// but `commit` returns an error, the WAL gate is closed, the drainer gathers the
/// `Reply` row; then open the gate.
///
/// Must see: before the send, the writer commits **a barrier** (`writer::barrier`) —
/// the `latest_send` mark does not include the handle-less write, so only a barrier
/// submitted after it makes the send ticket cover it; the tripwire
/// `turn::outbox::undurable_send` does not fire; the caller finishes with `pong`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_commit_with_an_outbox_row_forces_a_barrier() -> Result<()> {
    use cairn_store::testkit::{Action, Gate, Rule};
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, sdk, caller_key) = call_across_solo(&bucket).await?;
    let url = n.node.url();
    let r = within(
        Duration::from_secs(60),
        "barrier after a handle-less write",
        async {
            let _wire = ctl.fail("turn::outbox::undurable_send", "panic");
            let (syncs, _s) = count_hits("turn::drain_outbox::before_sync");
            let (sends, _t) = count_hits("turn::drain_outbox::before_send");
            let (barriers, _b) = count_hits("writer::barrier");
            let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let caller = body["id"].as_str().unwrap_or_default().to_string();
            until(Duration::from_secs(10), "callee turn at the SDK", || {
                sdk.starts_of("Callee", "echo").len() == 1
            })
            .await?;
            let gate = Gate::closed();
            let rule = n
                .store
                .add_rule(Rule::nodewal_put(None, Action::Gate(gate.clone())));
            let (syncs0, sends0, barriers0) = (hits(&syncs), hits(&sends), hits(&barriers));
            let _fail = ctl.fail("writer::commit::after_submit", "1*return(cancel)");
            sdk.gate().release(1);
            until(
                Duration::from_secs(10),
                "the drainer gathers an undurable Reply row",
                || hits(&syncs) > syncs0,
            )
            .await?;
            ensure!(hits(&sends) == sends0, "sent before durable barrier");
            gate.open();
            n.store.remove_rule(rule);
            let st = common::wait_status(&url, &caller, "completed").await?;
            ensure!(st["output"] == "pong", "{st}");
            ensure!(
                hits(&barriers) > barriers0,
                "a write with rows to send lost its handle and no barrier preceded the send (LF-3)"
            );
            Ok(())
        },
    )
    .await;
    n.node.shutdown().await;
    r
}

/// **A turn whose finishing commit landed but reported an error kept the VO lock.**
///
/// Interleaving (chaos, 40-minute run: two `ChaosEcho/ping` calls on one key stood
/// `running` for 6 minutes):
/// 1. One node, two ranges. `Caller/run` calls `Callee/echo` on key K (an EXCLUSIVE
///    virtual object); the fake SDK holds the callee's turn at its gate.
/// 2. `writer::commit::after_submit` fails once: the callee's end-of-turn commit
///    (status `completed`, the queue entry and retry row removed, a `Reply` row) is
///    applied, but `commit` returns an error ⇒ `TurnFailed { AfterRunning }`.
/// 3. The reply is delivered: the caller finishes with `pong`.
/// 4. A second `Callee/echo` on K, in the same term.
///
/// Must see: the second call completes — the failed turn read the status back, found it
/// terminal and released K's lock. Before the fix: the lock was kept until the next leader
/// change of the range (none here), the second call parked behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_turn_with_a_failed_commit_releases_the_object_lock() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, sdk, caller_key) = call_across_solo(&bucket).await?;
    let callee_key = common::key_on_partition(&n.node, "Callee", 1, "callee");
    let url = n.node.url();
    let r = within(
        Duration::from_secs(60),
        "a second call on the same object key after a failed finishing commit",
        async {
            let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let caller = body["id"].as_str().unwrap_or_default().to_string();
            until(Duration::from_secs(10), "callee turn at the SDK", || {
                sdk.starts_of("Callee", "echo").len() == 1
            })
            .await?;
            // The only batch with an outbox row from here on is the callee's finishing
            // commit (its `Reply`): the caller's call row was committed before.
            let _fail = ctl.fail("writer::commit::after_submit", "1*return(chaos)");
            sdk.gate().release(1);
            let st = common::wait_status(&url, &caller, "completed").await?;
            ensure!(st["output"] == "pong", "{st}");
            let (code, body) = common::send(&url, "Callee", "echo", Some(&callee_key), "y").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let second = body["id"].as_str().unwrap_or_default().to_string();
            until(
                Duration::from_secs(15),
                "second callee turn at the SDK",
                || sdk.starts_of("Callee", "echo").len() == 2,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e} — K's lock was not released"))?;
            sdk.gate().release(1);
            common::wait_status(&url, &second, "completed").await?;
            Ok(())
        },
    )
    .await;
    n.node.shutdown().await;
    r
}

// ─────────────────────────── nudging the drainer early

/// **A message the drainer handled before it was durable survives a split quiesce.**
///
/// Interleaving:
/// 1. One node, two ranges. `Caller/run` (range 0) calls `Callee/echo` (range 1).
/// 2. Right before sending `StartCall` (`turn::drain_outbox::before_send`) the test
///    closes the node's shared WAL log PUT gate: the accepting commit on range 1 is
///    submitted without becoming durable.
/// 3. Range 1's drainer creates the callee from the undurable inbound row and
///    dispatches the turn; the turn gets past the `Running` commit
///    (`turn::after_running`), not durable ⇒ the SDK has not been called.
/// 4. Split range 1: the quiesce cancels the accepting task (the sender gets no `200`)
///    and the callee's turn; `seal` (a barrier) waits at the gate.
/// 5. Open the gate (variant: `writer::seal::before_flush` returns an error once ⇒ the
///    split rolls back, range 1 carries on).
///
/// Must see: the caller finishes with `pong`; the callee runs its SDK, exactly **one**
/// id; in the main variant the callee lives in a child range.
async fn early_drained_inbound_through_split(seal_fails: bool) -> Result<()> {
    use cairn_store::testkit::{Action, Gate, Rule};
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let n = common::start_cluster_node(&bucket, "node-solo", 2, Duration::from_secs(2)).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(20),
        "node is leader of every range",
        |p| p.len() == 2 && p.values().all(|v| v == &[0]),
    )
    .await?;
    let url = n.node.url();
    let caller_key = common::key_on_partition(&n.node, "Caller", 0, "caller");
    let callee_key = common::key_on_partition(&n.node, "Callee", 1, "callee");
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                common::fake_sdk::Step::call("Callee", "echo", Some(&callee_key)),
                common::fake_sdk::Step::EchoCallResult,
            ],
        )
        .object(
            "Callee",
            "echo",
            vec![common::fake_sdk::Step::output("pong")],
        )
        .spawn()
        .await?;
    let r = within(Duration::from_secs(90), "early drain via split", async {
        common::register(&url, &sdk.url).await?;
        let gate = Gate::opened();
        n.store
            .add_rule(Rule::nodewal_put(None, Action::Gate(gate.clone())));
        // Only the **first** send closes the gate — the resend after it opens must not
        // close it again.
        let closer = gate.clone();
        let once = std::sync::atomic::AtomicBool::new(false);
        let _send = cairn_store::testkit::FailGuard::callback(
            "turn::drain_outbox::before_send",
            move || {
                if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    closer.close();
                }
            },
        );
        let (running, _r) = count_hits("turn::after_running");
        let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let caller = body["id"].as_str().unwrap_or_default().to_string();
        // 2–3. The caller's turn also reaches `Running` (once); the callee's turn is the
        // second time, after the gate has closed.
        until(
            Duration::from_secs(10),
            "the callee's turn reaches Running while the message is not durable",
            || !gate.is_open() && hits(&running) >= 2,
        )
        .await?;
        ensure!(
            sdk.starts_of("Callee", "echo").is_empty(),
            "the callee's SDK was called while the accepted message was not durable"
        );
        // 4. One point, one configuration: the error variant cannot count seals —
        // wait (capped) until the split command has answered.
        let split = || tokio::spawn(post_json(format!("{url}/ranges/1/split")));
        let (splitting, _seal) = if seal_fails {
            let fail = ctl.fail("writer::seal::before_flush", "1*return(seal)");
            let splitting = split();
            let t0 = std::time::Instant::now();
            while !splitting.is_finished() && t0.elapsed() < Duration::from_secs(5) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            (splitting, fail)
        } else {
            let (seals, guard) = count_hits("writer::seal::before_flush");
            let splitting = split();
            until(
                Duration::from_secs(10),
                "range 1 reaches the seal with the gate still closed",
                || hits(&seals) > 0,
            )
            .await?;
            (splitting, guard)
        };
        // 5.
        gate.open();
        let (mut code, mut view) = splitting.await??;
        // The callee's turn waits for durability throughout the quiesce (holding its
        // permit until the end of `QUIESCE_GRACE`), so the first split may return a
        // retryable `503` — the operator retries; the message and the callee live in the
        // sealed Db or in the reopened range.
        let t0 = std::time::Instant::now();
        while !seal_fails && code != StatusCode::OK {
            ensure!(
                code == StatusCode::SERVICE_UNAVAILABLE && t0.elapsed() < Duration::from_secs(40),
                "split: {code} {view}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
            (code, view) = post_json(format!("{url}/ranges/1/split")).await?;
        }
        let st = common::wait_status(&url, &caller, "completed").await?;
        ensure!(st["output"] == "pong", "{st}");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let callees: std::collections::BTreeSet<String> = sdk
            .starts_of("Callee", "echo")
            .iter()
            .map(|s| s.id_hex())
            .collect();
        ensure!(callees.len() == 1, "exactly one callee: {callees:?}");
        if !seal_fails {
            let callee = callees.into_iter().next().unwrap_or_default();
            let p = common::partition_of(&url, &callee).await?;
            ensure!(
                p != 0 && p != 1,
                "the callee must be in a child range, not in range {p}"
            );
        }
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_drained_inbound_survives_split_quiesce() -> Result<()> {
    early_drained_inbound_through_split(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_drained_inbound_survives_a_failed_seal() -> Result<()> {
    early_drained_inbound_through_split(true).await
}

// ───────────────────────────────── artifacts: dying between PUT and commit (AP-1, AP-8)

/// A fake `REQUEST_RESPONSE` wire v4 SDK: `Art/put` returns `ArtifactPut` + `Output`.
async fn artifact_sdk() -> Result<String> {
    use {
        axum::{
            routing::{get, post},
            Router,
        },
        bytes::{Bytes, BytesMut},
        cairn_protocol::{encode_message, pb, Flags, MessageType},
        cairn_server::{
            discovery::{
                EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
                DISCOVERY_V1,
            },
            invoker::INVOCATION_V1,
        },
        http::header,
    };
    async fn discover() -> ([(header::HeaderName, &'static str); 1], String) {
        let m = EndpointManifest {
            protocol_mode: ProtocolMode::RequestResponse,
            min_protocol_version: 1,
            max_protocol_version: 4,
            services: vec![ServiceManifest {
                name: "Art".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "put".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            }],
        };
        (
            [(header::CONTENT_TYPE, DISCOVERY_V1)],
            serde_json::to_string(&m).unwrap(),
        )
    }
    async fn invoke() -> ([(header::HeaderName, &'static str); 1], Bytes) {
        use sha2::Digest;
        let sha: [u8; 32] = sha2::Sha256::digest(ART_BODY).into();
        let mut b = BytesMut::new();
        b.extend_from_slice(&encode_message(
            MessageType::ARTIFACT_PUT,
            Flags::NONE,
            &pb::ArtifactPutMessage {
                sha256: Bytes::copy_from_slice(&sha),
                content: Bytes::from_static(ART_BODY),
                content_type: "text/plain".into(),
            },
        ));
        b.extend_from_slice(&encode_message(
            MessageType::OUTPUT,
            Flags::NONE,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"1")),
                )),
                name: "output".into(),
            },
        ));
        b.extend_from_slice(&encode_message(
            MessageType::END,
            Flags::NONE,
            &pb::EndMessage {},
        ));
        ([(header::CONTENT_TYPE, INVOCATION_V1)], b.freeze())
    }
    let app = Router::new()
        .route("/discover", get(discover))
        .route("/invoke/{service}/{handler}", post(invoke));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(url)
}

const ART_BODY: &[u8] = b"artifact survives the crash";

/// **Dying between the artifact PUT and the row commit.**
///
/// 1. One node, the artifact sweep every 100 ms, `min_age` 0, retention 0.
/// 2. Point `artifact::after_put` returns an error **once**: the object has been PUT at
///    epoch E1, the row not committed — the turn fails, a retry.
/// 3. The retry sees no row ⇒ epoch E2, PUT, row commit, `Output`.
///
/// Must see: the invocation `completed`; `GET …/artifacts/{sha}` returns the right
/// bytes (E2's); the E1 directory (orphaned) is held in place by the sweep and then
/// deleted — in the end only one directory is left under `artifacts/default/<id>/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn artifact_orphaned_between_put_and_commit_is_swept() -> Result<()> {
    use slatedb::object_store::ObjectStore;
    let ctl = StepController::new();
    let shared = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let n = common::start_cluster_node_tweaked(
        &shared,
        "a",
        1,
        std::time::Duration::from_secs(60),
        |cfg| {
            cfg.artifact_sweep = Some(std::time::Duration::from_millis(100));
            cfg.artifact_min_age = std::time::Duration::ZERO;
            cfg.trash_keep = Some(std::time::Duration::ZERO);
        },
    )
    .await?;
    common::wait_probe(
        &[&n],
        std::time::Duration::from_secs(30),
        "a is leader of range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &artifact_sdk().await?).await?;
    let _once = ctl.fail("artifact::after_put", "1*return(die)");
    let (code, r) = common::send(&url, "Art", "put", None, "null").await?;
    ensure!(code < 300, "{code} {r}");
    let id = r["id"].as_str().context("id")?.to_string();
    within(
        TEST_DEADLINE,
        "completed",
        common::wait_status(&url, &id, "completed"),
    )
    .await?;
    let sha: String = {
        use sha2::Digest;
        sha2::Sha256::digest(ART_BODY)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    let got = Client::new()
        .get(format!("{url}/invocations/{id}/artifacts/{sha}"))
        .send()
        .await?;
    ensure!(got.status() == StatusCode::OK, "GET {}", got.status());
    ensure!(&got.bytes().await?[..] == ART_BODY);
    // The point fired: two artifact PUTs (E1 then E2), not one.
    let metrics = Client::new()
        .get(format!("{url}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let puts = metrics
        .lines()
        .find_map(|l| l.strip_prefix("cairnflow_artifact_puts_total "))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    ensure!(puts >= 2, "artifact::after_put did not fire: {puts} PUTs");
    within(TEST_DEADLINE, "orphan E1 swept", async {
        loop {
            use futures::TryStreamExt;
            let dirs: std::collections::BTreeSet<String> = shared
                .list(Some(&slatedb::object_store::path::Path::from(format!(
                    "artifacts/default/{id}"
                ))))
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .filter_map(|m| m.location.parts().nth(3).map(|p| p.as_ref().to_string()))
                .collect();
            if dirs.len() == 1 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await?;
    let again = Client::new()
        .get(format!("{url}/invocations/{id}/artifacts/{sha}"))
        .send()
        .await?;
    ensure!(
        again.status() == StatusCode::OK,
        "after the sweep: {}",
        again.status()
    );
    n.node.shutdown().await;
    Ok(())
}

/// The `T_put` knob for tests.
fn short_put_deadline(cfg: &mut cairn_server::node::NodeConfig) {
    cfg.artifact_put_deadline = std::time::Duration::from_millis(50);
}

/// **`T_put` (AP-9).** An `Artifacts` row must be committed within `T_put` of the PUT;
/// past that it PUTs again (refreshing the object's age) before committing. Since
/// `min_age ≥ 2 × T_put`, the sweep never sees an object old enough whose row is still
/// on its way to a commit.
///
/// 1. One node, `T_put` = 50 ms.
/// 2. Point `artifact::after_put` sleeps 300 ms **once**: between the PUT and the row
///    commit, past `T_put`.
///
/// Must see: two artifact PUTs (the first and the refresh), the invocation
/// `completed`, `GET …/artifacts/{sha}` returns the right bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_commit_reputs_artifact() -> Result<()> {
    let ctl = StepController::new();
    let shared = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let n = common::start_cluster_node_tweaked(
        &shared,
        "a",
        1,
        std::time::Duration::from_secs(60),
        short_put_deadline,
    )
    .await?;
    common::wait_probe(
        &[&n],
        std::time::Duration::from_secs(30),
        "a is leader of range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &artifact_sdk().await?).await?;
    let puts = || async {
        let m = Client::new()
            .get(format!("{url}/metrics"))
            .send()
            .await?
            .text()
            .await?;
        Ok::<u64, anyhow::Error>(
            m.lines()
                .find_map(|l| l.strip_prefix("cairnflow_artifact_puts_total "))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0),
        )
    };
    let before = puts().await?;
    let _slow = ctl.fail("artifact::after_put", "1*sleep(300)");
    let (code, r) = common::send(&url, "Art", "put", None, "null").await?;
    ensure!(code < 300, "{code} {r}");
    let id = r["id"].as_str().context("id")?.to_string();
    within(
        TEST_DEADLINE,
        "completed",
        common::wait_status(&url, &id, "completed"),
    )
    .await?;
    let after = puts().await?;
    ensure!(
        after - before == 2,
        "past T_put it must PUT again exactly once: {} PUTs",
        after - before
    );
    let sha: String = {
        use sha2::Digest;
        sha2::Sha256::digest(ART_BODY)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    let got = Client::new()
        .get(format!("{url}/invocations/{id}/artifacts/{sha}"))
        .send()
        .await?;
    ensure!(got.status() == StatusCode::OK, "GET {}", got.status());
    ensure!(&got.bytes().await?[..] == ART_BODY);
    n.node.shutdown().await;
    Ok(())
}

/// **Listing invocations while a range's engine has just exited** (the error
/// `500 engine stopped` seen on the `cluster-4p+split` e2e row).
///
/// Interleaving:
/// 1. Node J alone, one range, leading it.
/// 2. `actor::step_down` = `1*panic`, only on J. Ask J to release range 0: the range
///    job dies, the old handle stays in the node's engine set until the engine set
///    loop rebuilds it (as in [`range_job_that_panics_is_rebuilt`]).
/// 3. The test cuts into the window: ask `GET /invocations?limit=5` repeatedly until J
///    leads range 0 again.
///
/// Must see: every ask returns `200` with an array — a range whose engine has exited is
/// not served by this node at that moment, like a `fenced` range. Before the fix:
/// `500 engine stopped` in the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listing_invocations_skips_a_range_whose_engine_just_stopped() -> Result<()> {
    use std::time::{Duration, Instant};
    let ctl = StepController::new();
    let bucket = common::bucket();
    let j = start_led_alone(&bucket, "list-stopped-j").await?;
    let url = j.node.url();
    let r = async {
        let fp = ctl.fail_on(&["list-stopped-j"], "actor::step_down", "1*panic");
        let _ = common::post_internal(&j.node, "/internal/ranges/0/resign", Vec::new()).await;
        drop(fp);
        let client = reqwest::Client::new();
        // The window lasts until the engine set loop rebuilds the job (about one tick);
        // 5 s cover it several times.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut asked = 0;
        while Instant::now() < deadline {
            let resp = client
                .get(format!("{url}/invocations?limit=5"))
                .send()
                .await?;
            let status = resp.status();
            let body = resp.text().await?;
            ensure!(
                status == reqwest::StatusCode::OK,
                "ask {asked}: {status} {body}"
            );
            ensure!(
                serde_json::from_str::<serde_json::Value>(&body)?.is_array(),
                "{body}"
            );
            asked += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }
    .await;
    j.node.shutdown().await;
    r
}

// ─────────────── server-side schedules

/// A schedule command on range 0 of a one-range node.
async fn schedule_op(
    node: &cairn_server::node::RunningNode,
    op: cairn_server::schedules::ScheduleOp,
) -> Result<cairn_server::schedules::ScheduleAnswer> {
    Ok(node
        .partitions
        .get(cairn_store::PartitionId(0))
        .ok_or_else(|| anyhow!("node does not hold range 0"))?
        .schedule(op)
        .await?)
}

fn every_minute(name: &str) -> cairn_server::schedules::PutSchedule {
    cairn_server::schedules::PutSchedule {
        tenant: Default::default(),
        scope: None,
        name: name.into(),
        spec: cairn_engine::schedule::SpecRow::Every(60_000),
        service: "Report".into(),
        handler: "build".into(),
        key: String::new(),
        limit_key: None,
        input: bytes::Bytes::from_static(b"{}"),
        overlap: cairn_engine::schedule::Overlap::Skip,
        paused: false,
        jitter_ms: 0,
    }
}

async fn schedule_view(
    node: &cairn_server::node::RunningNode,
    id: cairn_store::InvocationId,
) -> Result<cairn_server::schedules::ScheduleView> {
    match schedule_op(node, cairn_server::schedules::ScheduleOp::Get(id)).await? {
        cairn_server::schedules::ScheduleAnswer::View(Some(v)) => Ok(v),
        other => Err(anyhow!("GET schedule: {other:?}")),
    }
}

/// **Dying around the fire commit** (CG-12):
///
/// 1. `turn::schedule::before_fire` fires at mark `at` ⇒ no run, the schedule row
///    intact (`next_at = at`).
/// 2. Tick again at `at` ⇒ fires exactly at that mark, once.
/// 3. The next mark, `turn::schedule::after_fire` fires (the tick returns an error) ⇒
///    the run of the next mark has been created **and** the schedule row moved — in the
///    same commit.
/// 4. Tick again at the next mark ⇒ no second run.
///
/// The tripwire `schedule::live_without_next` is set to `panic` for the whole test: no
/// path writes a live schedule without a next due time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_fire_dies_around_the_commit() -> Result<()> {
    use cairn_server::schedules::{ScheduleAnswer, ScheduleOp};
    let ctl = StepController::new();
    let _wire = ctl.fail("schedule::live_without_next", "panic");
    let sdk = common::fake_sdk::FakeSdk::builder()
        .service(
            "Report",
            "build",
            vec![common::fake_sdk::Step::output("ok")],
        )
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "dying around the fire commit", async {
        common::register(&url, &sdk.url).await?;
        let ScheduleAnswer::Put { view: v, .. } =
            schedule_op(&node, ScheduleOp::Put(Box::new(every_minute("fp")))).await?
        else {
            anyhow::bail!("PUT");
        };
        let at = v.row.next_at;
        let tick = |now: u64| {
            let url = url.clone();
            async move {
                Ok::<_, anyhow::Error>(
                    Client::new()
                        .post(format!("{url}/admin/tick?now_ms={now}"))
                        .send()
                        .await?
                        .status(),
                )
            }
        };
        {
            let _die = ctl.fail("turn::schedule::before_fire", "1*return(die)");
            tick(at).await?;
        }
        ensure!(
            sdk.starts_len() == 0,
            "died before the commit yet a run exists"
        );
        let w = schedule_view(&node, v.id).await?;
        ensure!(w.row.next_at == at && w.row.fired == 0, "{w:?}");
        tick(at).await?;
        let run = cairn_store::id_hex(&cairn_store::schedule_run_id(&v.id, at));
        common::wait_status(&url, &run, "completed").await?;
        let at2 = at + 60_000;
        {
            let _die = ctl.fail("turn::schedule::after_fire", "1*return(die)");
            let s = tick(at2).await?;
            ensure!(!s.is_success(), "after-commit point did not fire: {s}");
        }
        let run2 = cairn_store::id_hex(&cairn_store::schedule_run_id(&v.id, at2));
        common::wait_status(&url, &run2, "completed").await?;
        let w = schedule_view(&node, v.id).await?;
        ensure!(w.row.next_at == at2 + 60_000 && w.row.fired == 2, "{w:?}");
        tick(at2).await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        ensure!(
            sdk.starts_len() == 2,
            "fired again after the commit: {}",
            sdk.starts_len()
        );
        // Pause, resume, delete: the tripwire stays silent.
        for op in [
            ScheduleOp::Pause(v.id),
            ScheduleOp::Resume(v.id),
            ScheduleOp::Delete(v.id),
        ] {
            schedule_op(&node, op).await?;
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`/query` does not wait for the engine**: the query layer reads through the range's
/// read handle (a Db clone, a snapshot at the `Remote` level), not through the engine
/// lock.
///
/// 1. A one-range node, one finished invocation; `/query` sees it.
/// 2. `writer::commit::before_write` = `sleep(6000)`: the next commit sleeps 6 s
///    **while holding the engine**.
/// 3. A new `/send` (its commit sleeps, holding the engine).
/// 4. Meanwhile, `POST /query`.
///
/// Must see: `/query` returns `200` within 2 s, while the `/send` still hangs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_does_not_wait_for_engine() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    let http = Client::new();
    let r = within(TEST_DEADLINE, "query does not wait for the engine", async {
        common::register(&url, &sdk.url).await?;
        let r = http
            .post(format!("{url}/invoke/Echo/run"))
            .body("{}")
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "{}", r.text().await?);
        let sql = json!({ "sql": "SELECT count(*) FROM sys_invocation" });
        let t0 = std::time::Instant::now();
        loop {
            let v: Value = http
                .post(format!("{url}/query"))
                .json(&sql)
                .send()
                .await?
                .json()
                .await?;
            if v["rows"] == json!([[1]]) && v["partial"] == false {
                break;
            }
            ensure!(
                t0.elapsed() < std::time::Duration::from_secs(20),
                "not seen yet: {v}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let _slow = ctl.fail("writer::commit::before_write", "sleep(6000)");
        let send = {
            let http = http.clone();
            let url = url.clone();
            tokio::spawn(async move {
                http.post(format!("{url}/invoke/Echo/run/send"))
                    .body("{}")
                    .send()
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let t = std::time::Instant::now();
        let r = http.post(format!("{url}/query")).json(&sql).send().await?;
        let took = t.elapsed();
        ensure!(r.status() == StatusCode::OK, "{}", r.text().await?);
        ensure!(
            took < std::time::Duration::from_secs(2),
            "/query waited {took:?}"
        );
        ensure!(!send.is_finished(), "/send must still hang in its commit");
        drop(_slow);
        let _ = send.await;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`cairn-ctl backup` dying mid-copy.**
///
/// Order:
/// 1. The format 10 fixture bucket with a split range. Enable `ctl::backup::mid_copy`
///    (`return`): the backup stops after the first object.
/// 2. `backup --to memory://r2/daily` returns an error.
/// 3. Disable the point, back up again.
///
/// Must see: (2) the target has objects under `<id>/data/` but **no** `<id>/BACKUP`;
/// `backup list` reports it incomplete; `restore` has no complete copy to use and
/// `restore --id` refuses the incomplete one (nothing written to the target); the
/// source no longer has the GC barrier of the dead run. (3) The new copy is complete,
/// `restore` picks it, skipping the incomplete one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_dying_mid_copy_leaves_an_unusable_partial() -> Result<()> {
    use {
        cairn_server::ctl::{Ctl, CtlError, BACKUP_MANIFEST},
        cairn_store::{ObjectStores, StoreLocation},
        futures::TryStreamExt,
        slatedb::object_store::{memory::InMemory, ObjectStore},
        std::sync::Arc,
    };
    let steps = StepController::new();
    let src = common::fixture::bucket(&common::fixture::dir("format10-cluster")).await?;
    let target = Arc::new(InMemory::new());
    let to = || StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
    let mem = |b: &common::Bucket| -> Arc<InMemory> { Arc::clone(b) };
    let on = |s: Arc<dyn ObjectStore>| {
        Ctl::new(ObjectStores::single(StoreLocation::from_store(
            s,
            "",
            "memory://",
        )))
    };
    let ctl = on(mem(&src));
    let list = |store: Arc<InMemory>, p: &'static str| async move {
        let p = (!p.is_empty()).then(|| slatedb::object_store::path::Path::from(p));
        Ok::<Vec<String>, anyhow::Error>(
            store
                .list(p.as_ref())
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .map(|m| m.location.to_string())
                .collect(),
        )
    };
    // 1–2.
    let die = steps.fail("ctl::backup::mid_copy", "return(die)");
    let r = ctl
        .backup(&to(), "test@failpoints", "cairn-ctl backup")
        .await;
    drop(die);
    ensure!(r.is_err(), "backup must fail at the failpoint: {r:?}");
    let left = list(target.clone(), "daily").await?;
    ensure!(
        !left.is_empty() && left.iter().all(|p| p.contains("/data/")),
        "the incomplete copy must have data, no manifest: {left:?}"
    );
    ensure!(!left.iter().any(|p| p.ends_with(BACKUP_MANIFEST)));
    let listed = ctl.backup_list(&to()).await?;
    ensure!(listed.len() == 1 && !listed[0].complete, "{listed:?}");
    let partial = listed[0].id.clone();
    let dst = common::bucket();
    let r = on(mem(&dst)).restore(&to(), None, "t", "c").await;
    ensure!(matches!(r, Err(CtlError::Refused(_))), "{r:?}");
    let r = on(mem(&dst)).restore(&to(), Some(&partial), "t", "c").await;
    ensure!(matches!(r, Err(CtlError::Refused(_))), "{r:?}");
    ensure!(
        list(mem(&dst), "").await?.is_empty(),
        "restore of the incomplete copy wrote something"
    );
    let barriers = list(mem(&src), "gc-barriers").await?;
    ensure!(
        barriers.is_empty(),
        "the dead run's barrier is still on the source: {barriers:?}"
    );
    // 3.
    let good = ctl
        .backup(&to(), "test@failpoints", "cairn-ctl backup")
        .await?;
    let r = on(mem(&dst)).restore(&to(), None, "t", "c").await?;
    ensure!(r.id == good.id && r.id != partial, "{r:?}");
    Ok(())
}

/// **A tenant state change ACK is never a fake `204`.**
///
/// Interleaving:
/// 1. A two-node cluster with auth and a cluster secret: `ack-a`, `ack-b`.
/// 2. `ingress::registry_apply::tenants` = `return`, only on `ack-b`: the request to
///    apply the registries reaches B, B can re-read `auth/keys` but **cannot** re-read
///    `tenants`.
/// 3. `PUT /admin/tenants/acme/state suspended` through A.
///
/// Must see: `202` with `ack-b` in `pending` — the registry was written, but B has not
/// confirmed applying the new state; not a `204` (an ACK that only re-read the key
/// registry would be a fake ACK for the tenant state). Remove the failpoint, send the
/// same command again ⇒ `204`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_state_ack_is_never_a_false_204() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some("cf_test_cluster_secret_failpoint_tenant_ack_0123".into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(200);
    };
    let ttl = Duration::from_secs(2);
    let a = common::start_cluster_node_tweaked(&bucket, "ack-a", 1, ttl, tweak).await?;
    let b = common::start_cluster_node_tweaked(&bucket, "ack-b", 1, ttl, tweak).await?;
    let a_url = a.node.url();
    let http = Client::new();
    let put = |state: &'static str| {
        call(
            http.put(format!("{a_url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": state })),
        )
    };
    let r = within(Duration::from_secs(30), "tenant state ACK", async {
        let fp = ctl.fail_on(&["ack-b"], "ingress::registry_apply::tenants", "return");
        let (code, v) = put("suspended").await?;
        ensure!(
            code == StatusCode::ACCEPTED
                && v["pending"]
                    .as_array()
                    .is_some_and(|p| p.iter().any(|n| n == "ack-b")),
            "B could not apply the tenants registry yet still: {code} {v}"
        );
        drop(fp);
        let (code, v) = put("suspended").await?;
        ensure!(code == StatusCode::NO_CONTENT, "point removed: {code} {v}");
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// Two-node cluster with auth and a cluster secret for the registry ACK tests.
async fn ack_pair(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    ttl: std::time::Duration,
) -> Result<(common::ClusterNode, common::ClusterNode)> {
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some("cf_test_cluster_secret_failpoint_tenant_ack_0123".into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = std::time::Duration::from_millis(200);
    };
    let a = common::start_cluster_node_tweaked(bucket, "ack-a", 1, ttl, tweak).await?;
    let b = common::start_cluster_node_tweaked(bucket, "ack-b", 1, ttl, tweak).await?;
    Ok((a, b))
}

/// **One missed apply does not turn a tenant state change into a `202`.**
///
/// Interleaving (seen in a 24 h soak: every retry of `PUT …/state` found one peer that
/// had missed its apply, so no `204` came within 60 s):
/// 1. Two nodes `ack-a`, `ack-b`.
/// 2. `ingress::registry_apply::tenants` = `1*return` on `ack-b`: B's **first** apply
///    fails (an injected GET fault), later ones succeed.
/// 3. `PUT /admin/tenants/acme/state suspended` through A.
///
/// Must see: `204` — A asks B again within the request instead of answering `202` for a
/// peer that would have confirmed a moment later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_state_ack_rides_out_one_failed_apply() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (a, b) = ack_pair(&bucket, Duration::from_secs(2)).await?;
    let a_url = a.node.url();
    let r = within(Duration::from_secs(30), "tenant state ACK", async {
        let _fp = ctl.fail_on(&["ack-b"], "ingress::registry_apply::tenants", "1*return");
        let (code, v) = call(
            Client::new()
                .put(format!("{a_url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "suspended" })),
        )
        .await?;
        ensure!(
            code == StatusCode::NO_CONTENT,
            "B missed only its first apply, yet: {code} {v}"
        );
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// **A peer that died during the request stops being waited for once its session ends.**
///
/// Interleaving (seen in a 24 h soak with a `kill -9` every ~10 s: each retry of
/// `PUT …/state` found one incarnation killed less than a TTL earlier, its session not yet
/// `expired`, so no `204` came within 60 s):
/// 1. Two nodes `ack-a`, `ack-b`, TTL 2 s.
/// 2. B's renewal dies (`crash_handle`, like `kill -9`) and B never applies again
///    (`ingress::registry_apply::tenants` = `return` on `ack-b`).
/// 3. Right away, `PUT /admin/tenants/acme/state suspended` through A — B's record is
///    still live.
///
/// Must see: `204` within the request — A keeps asking until B's record turns `expired`
/// on the bucket (a dead incarnation serves nothing), instead of `202 pending=[ack-b]`.
/// The permanent-`return` case of a **live** B stays `202`
/// (`tenant_state_ack_is_never_a_false_204`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_state_ack_waits_out_a_dead_peer() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (a, b) = ack_pair(&bucket, Duration::from_secs(2)).await?;
    let a_url = a.node.url();
    let r = within(Duration::from_secs(30), "tenant state ACK", async {
        let _fp = ctl.fail_on(&["ack-b"], "ingress::registry_apply::tenants", "return");
        b.node.session.crash_handle().crash();
        let (code, v) = call(
            Client::new()
                .put(format!("{a_url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "suspended" })),
        )
        .await?;
        ensure!(
            code == StatusCode::NO_CONTENT,
            "B's incarnation is dead, yet: {code} {v}"
        );
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// **Dying between the two CASes of the `tenants/state` gate** (AGENTS.md §2, the
/// migration framework row). The gate's "step" is `cairn-ctl cluster
/// enable-tenant-state`.
///
/// 1. An old cluster: `min_binary=8`, every earlier gate on, `tenant_state` not yet.
/// 2. `--apply` CASes `min_binary=9`, then dies at `ctl::enable_tenant_state::raised`.
/// 3. Run the enable command again.
///
/// Must see: after 2, `min_binary=9` blocks a step-8 binary (it ignores `state=`) and
/// the gate is **not** on yet (`PUT …/state` is still `409`); after 3 the gate is on, no
/// `BOOT_WAIT` wait again, the other gates intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_tenant_state_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, KEY_ROLES, TENANT_STATE},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=8\ncall_prefix=on\ncall_placement=on\nstreams=on\nschedules=on\nkey_roles=on\n",
            ),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-tenant-state --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_tenant_state::raised", "1*return(die)");
        let r = cli.enable_tenant_state(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == TENANT_STATE && !cv.tenant_state_on(),
        "died mid-step: min_binary set, gate not on yet — {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), KEY_ROLES)
            .await
            .is_err(),
        "a step-8 binary passed the check after min_binary=9 was written"
    );
    let settings =
        cairn_server::tenants::TenantSettings::new(Some((store.clone(), &ObjPath::default())));
    ensure!(
        !settings.state_enabled().await.map_err(|e| anyhow!(e))?,
        "the state gate is on with only min_binary written"
    );
    let rep = cli.enable_tenant_state(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        !rep.cleanup.iter().any(|c| c.contains("waited")),
        "the rerun must not wait BOOT_WAIT again: {rep:?}"
    );
    let cv = cv_now().await?;
    ensure!(
        cv.tenant_state_on()
            && cv.key_roles_on()
            && cv.schedules_on()
            && cv.streams_on()
            && cv.call_prefix_on()
            && cv.call_placement_on(),
        "{cv:?}"
    );
    ensure!(settings.state_enabled().await.map_err(|e| anyhow!(e))?);
    Ok(())
}

// ─────────────── data plane audit

/// `r-*.json` under `audit/tenant/` — the objects of the rows in the range.
async fn audit_row_objects(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
) -> Result<Vec<String>> {
    use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
    let all: Vec<_> = bucket
        .list(Some(&slatedb::object_store::path::Path::from(
            "audit/tenant",
        )))
        .try_collect()
        .await?;
    Ok(all
        .into_iter()
        .map(|m| m.location.to_string())
        .filter(|p| p.rsplit('/').next().is_some_and(|n| n.starts_with("r-")))
        .collect())
}

/// Waits until there are exactly `n` audit row objects, then holds that over a few more export ticks.
async fn audit_rows_settle(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    n: usize,
) -> Result<Vec<String>> {
    use std::time::Duration;
    within(Duration::from_secs(20), "object audit", async {
        loop {
            if audit_row_objects(bucket).await?.len() >= n {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    audit_row_objects(bucket).await
}

/// **Dying between writing an audit object and deleting the row** (exported exactly
/// once).
///
/// Interleaving:
/// 1. A one-range node; `audit::after_export` = `1*return`: the first export round
///    writes the object and then "dies" before the commit that deletes the row — the
///    row is intact.
/// 2. Cancel two sleeping invocations (two `Table::Audit` rows).
/// 3. The next export tick writes the same two objects again (the name is a function
///    of the row key, the content read back matches) and then deletes the rows.
///
/// Must see: exactly **two** `r-*.json` objects — no duplicates — and they stay stable
/// across many ticks (the rows are deleted, not exported again forever).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_export_dies_between_put_and_delete_writes_once() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let n = start_led_alone(&bucket, "audit-a").await?;
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(3_600_000), Step::output("x")],
        )
        .spawn()
        .await?;
    let r = within(Duration::from_secs(40), "audit exported once", async {
        common::register(&url, &sdk.url).await?;
        let ids = send_n(&url, "Nap", "once", 2).await?;
        for id in &ids {
            common::wait_status(&url, id, "suspended").await?;
        }
        let _die = ctl.fail_on(&["audit-a"], "audit::after_export", "1*return(die)");
        for id in &ids {
            let (code, v) =
                call(Client::new().post(format!("{url}/invocations/{id}/cancel"))).await?;
            ensure!(code.is_success(), "cancel: {code} {v}");
        }
        let rows = audit_rows_settle(&bucket, 2).await?;
        ensure!(rows.len() == 2, "object audit: {rows:#?}");
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **Audit rows survive a leader change and a range split** (rows follow the data,
/// deterministic names ⇒ one object).
///
/// Interleaving:
/// 1. A one-range node; `audit::before_export` = `return` (always): rows are not
///    exported.
/// 2. Cancel three sleeping invocations — three rows in range 0's Db.
/// 3. Split range 0: the rows follow the invocation pk into the two child ranges; the
///    `Audit` index is rebuilt in each child.
/// 4. Remove the failpoint.
///
/// Must see: exactly three `r-*.json` objects — each row exported exactly once by the
/// child range holding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_rows_follow_a_split_and_export_once() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let n = start_led_alone(&bucket, "audit-s").await?;
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(3_600_000), Step::output("x")],
        )
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "audit across a split", async {
        common::register(&url, &sdk.url).await?;
        let ids = send_n(&url, "Nap", "once", 3).await?;
        for id in &ids {
            common::wait_status(&url, id, "suspended").await?;
        }
        let hold = ctl.fail_on(&["audit-s"], "audit::before_export", "return(hold)");
        for id in &ids {
            let (code, v) =
                call(Client::new().post(format!("{url}/invocations/{id}/cancel"))).await?;
            ensure!(code.is_success(), "cancel: {code} {v}");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        ensure!(
            audit_row_objects(&bucket).await?.is_empty(),
            "exported while the failpoint holds"
        );
        let (code, v) = call(Client::new().post(format!("{url}/ranges/0/split"))).await?;
        ensure!(code == StatusCode::OK, "split: {code} {v}");
        drop(hold);
        let rows = audit_rows_settle(&bucket, 3).await?;
        ensure!(rows.len() == 3, "audit objects after the split: {rows:#?}");
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **Dying between the two CASes of the `audit/table` gate** (AGENTS.md §2, the
/// migration framework row): after the death, `min_binary=10` blocks a step-9 binary
/// and the gate is not on yet; a rerun enables it, the other gates intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_audit_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, AUDIT_TABLE, TENANT_STATE},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=9\ncall_prefix=on\ncall_placement=on\nstreams=on\nschedules=on\nkey_roles=on\ntenant_state=on\n",
            ),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-audit --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_audit::raised", "1*return(die)");
        let r = cli.enable_audit(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == AUDIT_TABLE && !cv.audit_on(),
        "died mid-step: {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), TENANT_STATE)
            .await
            .is_err(),
        "a step-9 binary passed the check after min_binary=10 was written"
    );
    let rep = cli.enable_audit(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    let cv = cv_now().await?;
    ensure!(
        cv.audit_on() && cv.tenant_state_on() && cv.key_roles_on() && cv.schedules_on(),
        "{cv:?}"
    );
    Ok(())
}

/// **Dying between the two CASes of the `tenants/purge` gate** (AGENTS.md §2, the
/// migration framework row): after the death, `min_binary=11` blocks a step-10 binary
/// and the gate is not on yet; a rerun enables it, the other gates intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_tenant_purge_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, AUDIT_TABLE, TENANT_PURGE},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=10\ncall_prefix=on\ncall_placement=on\nstreams=on\nschedules=on\nkey_roles=on\ntenant_state=on\naudit=on\n",
            ),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-tenant-purge --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_tenant_purge::raised", "1*return(die)");
        let r = cli.enable_tenant_purge(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == TENANT_PURGE && !cv.tenant_purge_on(),
        "died mid-step: {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), AUDIT_TABLE)
            .await
            .is_err(),
        "a step-10 binary passed the check after min_binary=11 was written"
    );
    let rep = cli.enable_tenant_purge(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    let cv = cv_now().await?;
    ensure!(
        cv.tenant_purge_on() && cv.audit_on() && cv.tenant_state_on() && cv.key_roles_on(),
        "{cv:?}"
    );
    Ok(())
}

/// **Dying between the two CASes of the `values/segmented` gate** (AGENTS.md §2, the
/// migration framework row): after the death, `min_binary=12` blocks a step-11 binary
/// and the gate is not on yet; a rerun enables it, the other gates intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_values_reruns_after_death_between_min_binary_and_gate() -> Result<()> {
    use {
        cairn_store::migrate::{self, TENANT_PURGE, VALUES},
        slatedb::object_store::{
            memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
        },
        std::sync::Arc,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from("cluster-version"),
            PutPayload::from(
                "cairnflow-cluster-version/1\nversion=2\nmin_binary=11\ncall_prefix=on\ncall_placement=on\nstreams=on\nschedules=on\nkey_roles=on\ntenant_state=on\naudit=on\ntenant_purge=on\n",
            ),
        )
        .await?;
    let cli = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store.clone(), "", "memory://"),
    ))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = cairn_server::ctl::RepairOptions {
        apply: true,
        force: false,
        who: "test@failpoints".into(),
        command: "cairn-ctl cluster enable-values --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let cv_now = || {
        let store = store.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &ObjPath::default())
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    {
        let _die = ctl.fail("ctl::enable_values::raised", "1*return(die)");
        let r = cli.enable_values(&opts).await;
        ensure!(r.is_err(), "failpoint did not fire: {r:?}");
    }
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == VALUES && !cv.values_on(),
        "died mid-step: {cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), TENANT_PURGE)
            .await
            .is_err(),
        "a step-11 binary passed the check after min_binary=12 was written"
    );
    let rep = cli.enable_values(&opts).await?;
    ensure!(rep.applied, "{rep:?}");
    let cv = cv_now().await?;
    ensure!(
        cv.values_on() && cv.tenant_purge_on() && cv.audit_on() && cv.tenant_state_on(),
        "{cv:?}"
    );
    Ok(())
}

// ─────────────────── cross-range messages lost after a restore

/// A one-node cluster with two ranges, SDK: `Caller/run` (object) = `ctx.call(Leaf/echo,
/// key)` then returns the result; `Leaf/echo` (object) suspends waiting for a signal,
/// then returns `"la"`. Keys chosen so that the caller is on range `caller_on`, the
/// callee on the other.
struct Calls {
    node: common::ClusterNode,
    sdk: common::fake_sdk::FakeSdk,
    caller_key: String,
    leaf_key: String,
}

impl Calls {
    async fn start(bucket: &common::Bucket, caller_on: u32) -> Result<Self> {
        use common::fake_sdk::{FakeSdk, Step};
        let node =
            common::start_cluster_node(bucket, "node-a", 2, std::time::Duration::from_secs(2))
                .await?;
        let caller_key = common::key_on_partition(&node.node, "Caller", caller_on, "c");
        let leaf_key = common::key_on_partition(&node.node, "Leaf", 1 - caller_on, "l");
        let sdk = FakeSdk::builder()
            .object(
                "Caller",
                "run",
                vec![
                    Step::call("Leaf", "echo", Some(&leaf_key)),
                    Step::EchoCallResult,
                ],
            )
            .object("Leaf", "echo", vec![Step::Suspend, Step::output("\"la\"")])
            .spawn()
            .await?;
        common::register(&node.node.url(), &sdk.url).await?;
        Ok(Self {
            node,
            sdk,
            caller_key,
            leaf_key,
        })
    }

    /// Sends the call; waits until the caller is `suspended` (it has the callee id and
    /// waits for the result) and the callee is `suspended`. Returns `(caller, callee)`.
    async fn call(&self) -> Result<(String, String)> {
        let url = self.node.node.url();
        let (code, body) =
            common::send(&url, "Caller", "run", Some(&self.caller_key), "\"x\"").await?;
        ensure!((200..300).contains(&code), "{code} {body}");
        let caller = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &caller, "suspended").await?;
        let callee = within(TEST_DEADLINE, "callee", async {
            loop {
                let list: Value = reqwest::get(format!("{url}/invocations"))
                    .await?
                    .json()
                    .await?;
                if let Some(id) = list.as_array().into_iter().flatten().find_map(|v| {
                    (v["service"] == "Leaf" && v["key"] == self.leaf_key.as_str())
                        .then(|| v["id"].as_str().map(str::to_string))
                        .flatten()
                }) {
                    return Ok(id);
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await?;
        common::wait_status(&url, &callee, "suspended").await?;
        Ok((caller, callee))
    }

    async fn flush(&self, range: u32) -> Result<()> {
        self.node
            .node
            .partitions
            .get(cairn_store::PartitionId(range))
            .context("range engine")?
            .flush_memtable()
            .await?;
        Ok(())
    }
}

/// Backs up `src` to an in-RAM target; failpoint `ctl::backup::after_range` stops the
/// backup after range 0's checkpoint while `between` runs. Then restores the copy to a
/// new bucket and runs `verify`. Returns the `restore.*` findings of the restored
/// bucket.
async fn backup_around(
    src: &common::Bucket,
    steps: &StepController,
    between: impl std::future::Future<Output = Result<()>>,
) -> Result<(common::Bucket, Vec<cairn_server::ctl::Finding>)> {
    use {
        cairn_server::ctl::{Ctl, VerifyOptions},
        cairn_store::{ObjectStores, StoreLocation},
        slatedb::object_store::memory::InMemory,
        std::sync::Arc,
    };
    let target = Arc::new(InMemory::new());
    let to = StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
    let on = |b: &common::Bucket| {
        Ctl::new(ObjectStores::single(StoreLocation::from_store(
            Arc::clone(b) as Arc<dyn slatedb::object_store::ObjectStore>,
            "",
            "memory://",
        )))
    };
    let paused = steps.pause_at("ctl::backup::after_range");
    let backup = {
        let ctl = on(src);
        let to = to.clone();
        tokio::spawn(async move { ctl.backup(&to, "test@failpoints", "cairn-ctl backup").await })
    };
    paused.expect_stopped(TEST_DEADLINE).await;
    between.await?;
    paused.resume();
    backup.await??;
    let dst = common::bucket();
    on(&dst).restore(&to, None, "t", "c").await?;
    let rep = on(&dst).verify(VerifyOptions::default()).await?;
    let found = rep
        .findings
        .into_iter()
        .filter(|f| f.check.starts_with("restore."))
        .collect();
    Ok((dst, found))
}

/// **A backup outlives the temporary checkpoints of the source.** Interleaving:
///
/// 1. The format 10 fixture bucket. On every range Db of the table, place a checkpoint
///    that expires (lifetime 15 min, unnamed) — what SlateDB's compactor places before
///    each compaction and what a `DbReader` replaces on every manifest change.
/// 2. The backup starts: it checkpoints the first range of the table, then stops at
///    `ctl::backup::after_range`.
/// 3. In the window, on every range: the temporary checkpoint goes away (it expired, or
///    the reader replaced it) and SlateDB's GC deletes the manifest it pointed to.
/// 4. Release; the backup runs to the end.
///
/// Must see: the backup succeeds; the restored copy carries no expiring checkpoint (it
/// belonged to a process of the source cluster, the copy cannot keep what it pins) and
/// `verify --deep` on the restored copy finds nothing missing.
///
/// Before the fix: the backup copied the manifest of every unexpired checkpoint in the
/// first range's manifest, found the one deleted in step 3 gone and failed with
/// "referenced by a checkpoint but the object is missing" — in a 24 h soak every cycle
/// after the first failed this way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_outlives_temporary_checkpoints_of_the_source() -> Result<()> {
    use {
        cairn_server::ctl::{Ctl, VerifyOptions},
        cairn_store::{ObjectStores, PartitionId, StoreLocation},
        slatedb::{
            admin::Admin,
            config::CheckpointOptions,
            object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
        },
        std::{sync::Arc, time::Duration},
    };
    let steps = StepController::new();
    let src = common::fixture::bucket(&common::fixture::dir("format10-cluster")).await?;
    let on = |b: &common::Bucket| {
        Ctl::new(ObjectStores::single(StoreLocation::from_store(
            Arc::clone(b) as Arc<dyn ObjectStore>,
            "",
            "memory://",
        )))
    };
    let store = Arc::clone(&src) as Arc<dyn ObjectStore>;
    let admin =
        |r: u32| Admin::builder(Path::from(format!("ranges/{r}")), Arc::clone(&store)).build();
    // 1.
    let mut temporary = Vec::new();
    for r in [1u32, 2, 3] {
        let cp = admin(r)
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(Duration::from_secs(15 * 60)),
                source: None,
                name: None,
            })
            .await?;
        temporary.push((r, cp));
    }
    let target = Arc::new(InMemory::new());
    let to = StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
    // 2.
    let paused = steps.pause_at("ctl::backup::after_range");
    let backup = {
        let ctl = on(&src);
        let to = to.clone();
        tokio::spawn(async move { ctl.backup(&to, "test@failpoints", "cairn-ctl backup").await })
    };
    paused.expect_stopped(TEST_DEADLINE).await;
    // 3.
    for (r, cp) in &temporary {
        admin(*r).delete_checkpoint(cp.id).await?;
        store
            .delete(&Path::from(format!(
                "ranges/{r}/manifest/{:020}.manifest",
                cp.manifest_id
            )))
            .await?;
    }
    // 4.
    paused.resume();
    let rep = backup.await??;
    let dst = common::bucket();
    let restored = on(&dst);
    restored.restore(&to, None, "t", "c").await?;
    for r in &rep.ranges {
        let cps = ObjectStores::single(StoreLocation::from_store(
            Arc::clone(&dst) as Arc<dyn ObjectStore>,
            "",
            "memory://",
        ))
        .range_checkpoints(PartitionId(*r))
        .await?;
        ensure!(
            cps.iter().all(|c| !c.expires),
            "range {r}: the restored copy still carries an expiring checkpoint: {cps:?}"
        );
    }
    let v = restored
        .verify(VerifyOptions {
            deep: true,
            ..VerifyOptions::default()
        })
        .await?;
    ensure!(
        v.findings
            .iter()
            .all(|f| f.check != cairn_server::ctl::SST_MISSING),
        "the restored copy misses objects: {:?}",
        v.findings
    );
    Ok(())
}

/// The backup's GC fence on the source, read back: `(expires_ms, now_ms)`.
async fn backup_fence(src: &common::Bucket) -> Result<Option<(u64, u64)>> {
    use {
        cairn_store::gc_barrier,
        slatedb::object_store::{path::Path, ObjectStore},
        std::sync::Arc,
    };
    let store = Arc::clone(src) as Arc<dyn ObjectStore>;
    let all = gc_barrier::list_barriers(&store, &Path::from("")).await?;
    let mine: Vec<_> = all.iter().filter(|b| b.id.starts_with("backup-")).collect();
    ensure!(mine.len() <= 1, "more than one backup fence: {all:?}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    match mine.first() {
        None => Ok(None),
        Some(b) => match &b.barrier {
            Ok(g) => Ok(Some((g.expires_ms, now))),
            Err(e) => anyhow::bail!("backup fence {} cannot be read: {e}", b.id),
        },
    }
}

/// **A killed backup releases the GC within minutes.** Interleaving:
///
/// 1. The format 10 fixture bucket; the backup's fence TTL is 3 s.
/// 2. The backup checkpoints the first range and stops at `ctl::backup::after_range`.
/// 3. The fence on the source expires at most one TTL ahead; while the copy stands
///    still (longer than the TTL), the backup keeps renewing it.
/// 4. The backup is killed (its task aborted — what a SIGKILL or a cron timeout does);
///    nothing removes the fence.
///
/// Must see: the fence never promises more than one TTL; it is renewed while the backup
/// lives; once the backup is gone, it expires within one TTL and the GC runs again.
///
/// Before the fix: the fence lived 24 h whatever happened to the backup. A 24 h soak
/// whose backups timed out every 15 min left 25 fences holding every range: no D-d, no
/// tenant purge, a source bucket growing ~13 GiB/h.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_backup_releases_the_gc_within_one_fence_ttl() -> Result<()> {
    use {
        cairn_server::ctl::Ctl,
        cairn_store::{ObjectStores, StoreLocation},
        slatedb::object_store::{memory::InMemory, ObjectStore},
        std::{sync::Arc, time::Duration},
    };
    const TTL: Duration = Duration::from_secs(3);
    let steps = StepController::new();
    // 1.
    let src = common::fixture::bucket(&common::fixture::dir("format10-cluster")).await?;
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&src) as Arc<dyn ObjectStore>,
        "",
        "memory://",
    )))
    .with_backup_fence_ttl(TTL);
    let to = StoreLocation::from_store(Arc::new(InMemory::new()), "daily", "memory://r2/daily");
    // 2.
    let paused = steps.pause_at("ctl::backup::after_range");
    let backup =
        tokio::spawn(async move { ctl.backup(&to, "test@failpoints", "cairn-ctl backup").await });
    paused.expect_stopped(TEST_DEADLINE).await;
    // 3.
    let (first, now) = backup_fence(&src)
        .await?
        .context("no backup fence while copying")?;
    let ttl_ms = TTL.as_millis() as u64;
    ensure!(
        first <= now + ttl_ms + 1_000,
        "the fence promises {} ms, more than one TTL ({ttl_ms} ms)",
        first.saturating_sub(now)
    );
    // Polling tick in a bounded loop: the copy stands still for two TTLs, the fence must
    // stay ahead of the clock the whole time.
    let until = std::time::Instant::now() + TTL * 2;
    let mut renewed = first;
    while std::time::Instant::now() < until {
        let (e, now) = backup_fence(&src)
            .await?
            .context("the fence went away while copying")?;
        ensure!(
            e > now,
            "the fence expired while the backup lives: expires {e}, now {now}"
        );
        renewed = e;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ensure!(
        renewed > first,
        "the fence was not renewed while the backup lives: {renewed} vs {first}"
    );
    // 4.
    backup.abort();
    paused.resume();
    let _ = backup.await;
    let gone = std::time::Instant::now();
    loop {
        match backup_fence(&src).await? {
            Some((expires, now)) if expires > now => ensure!(
                gone.elapsed() < TTL + Duration::from_secs(2),
                "a killed backup still holds the GC for {} ms",
                expires - now
            ),
            _ => return Ok(()),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// **A backup whose fence lapsed is not a backup.** Interleaving:
///
/// 1. The format 10 fixture bucket; the backup's fence TTL is 3 s, and every renewal
///    fails (`ctl::backup::renew_fence` returns an error — the source store refuses).
/// 2. The backup stops at `ctl::backup::after_range` for longer than the TTL: the fence
///    expires, GC may delete what the copy still has to read.
/// 3. Release.
///
/// Must see: the backup fails (no `BACKUP` manifest) and says the fence lapsed — it
/// never reports a copy taken while nothing held the GC.
///
/// Before the fix: the fence lived 24 h and was never renewed, so nothing checked it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_fails_when_its_fence_lapses() -> Result<()> {
    use {
        cairn_server::ctl::Ctl,
        cairn_store::{ObjectStores, StoreLocation},
        futures::TryStreamExt,
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
        std::{sync::Arc, time::Duration},
    };
    const TTL: Duration = Duration::from_secs(3);
    let steps = StepController::new();
    // 1.
    let src = common::fixture::bucket(&common::fixture::dir("format10-cluster")).await?;
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&src) as Arc<dyn ObjectStore>,
        "",
        "memory://",
    )))
    .with_backup_fence_ttl(TTL);
    let target = Arc::new(InMemory::new());
    let to = StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
    let _refuse = steps.fail("ctl::backup::renew_fence", "return(refused)");
    // 2.
    let paused = steps.pause_at("ctl::backup::after_range");
    let backup =
        tokio::spawn(async move { ctl.backup(&to, "test@failpoints", "cairn-ctl backup").await });
    paused.expect_stopped(TEST_DEADLINE).await;
    // Wait (bounded) for the fence to expire on the source: GC may run from here on.
    let lapse_by = std::time::Instant::now() + TTL * 3;
    loop {
        match backup_fence(&src).await? {
            Some((expires, now)) if expires > now => ensure!(
                std::time::Instant::now() < lapse_by,
                "the fence never expired although every renewal failed"
            ),
            _ => break,
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // 3.
    paused.resume();
    let r = backup.await?;
    let err = match r {
        Ok(rep) => anyhow::bail!("a backup whose fence lapsed reported success: {}", rep.id),
        Err(e) => e.to_string(),
    };
    ensure!(
        err.contains("fence"),
        "the error does not name the lapsed fence: {err}"
    );
    let manifests: Vec<_> = target
        .list(Some(&Path::from("daily")))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|m| m.location.as_ref().ends_with("/BACKUP"))
        .collect();
    ensure!(
        manifests.is_empty(),
        "a lapsed backup wrote a manifest: {manifests:?}"
    );
    Ok(())
}

/// **A backup during a range split.** Interleaving:
///
/// 1. A one-node cluster with two ranges; `Counter/add` with a key on range 0 finished
///    (`completed`); flush range 0's memtable (the copy only takes what is in L0).
/// 2. The backup starts: it reads table V, checkpoints range 0, stops at
///    `ctl::backup::after_range`.
/// 3. In the window: split range 0 up to the commit (table V+1: two children with
///    cloned Dbs).
/// 4. Release; the backup runs to the end.
/// 5. If the backup reports success: restore to a new bucket, build a node, read the
///    invocation of step 1.
///
/// Must see: either the backup reports an error, or the restored copy reads back the
/// `completed` invocation — never a "complete" copy that lost range 0's key range.
///
/// Before the fix: the copy combined range 0's checkpoint (table V) with the live
/// `cluster.conf` (V+1) and the owner records of the two children; after the restore
/// the two children opened as empty ranges ⇒ `404` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_during_a_split_never_loses_the_split_range() -> Result<()> {
    use {
        cairn_server::ctl::Ctl,
        cairn_store::{ObjectStores, StoreLocation},
        common::fake_sdk::{FakeSdk, Step},
        slatedb::object_store::memory::InMemory,
        std::{sync::Arc, time::Duration},
    };
    let steps = StepController::new();
    let src = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&src, "node-a", 2, Duration::from_secs(2), |_| {})
            .await?;
    let url = node.node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("added")])
        .spawn()
        .await?;
    let on = |b: &common::Bucket| {
        Ctl::new(ObjectStores::single(StoreLocation::from_store(
            Arc::clone(b) as Arc<dyn slatedb::object_store::ObjectStore>,
            "",
            "memory://",
        )))
    };
    let r = within(Duration::from_secs(120), "backup during a split", async {
        common::register(&url, &sdk.url).await?;
        let key = common::key_on_partition(&node.node, "Counter", 0, "k");
        let (code, v) = common::invoke(&url, "Counter", "add", Some(&key), "\"x\"").await?;
        ensure!(code == 200, "invoke: {code} {v}");
        let (code, list) = common::send(&url, "Counter", "add", Some(&key), "\"y\"").await?;
        ensure!((200..300).contains(&code), "send: {code} {list}");
        let id = list["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "completed").await?;
        let low = node.node.cluster.table().ranges()[0];
        node.node
            .partitions
            .get(low.id)
            .context("engine of range 0")?
            .flush_memtable()
            .await?;
        // 2–4.
        let target = Arc::new(InMemory::new());
        let to = StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
        let paused = steps.pause_at("ctl::backup::after_range");
        let backup = {
            let ctl = on(&src);
            let to = to.clone();
            tokio::spawn(
                async move { ctl.backup(&to, "test@failpoints", "cairn-ctl backup").await },
            )
        };
        paused.expect_stopped(TEST_DEADLINE).await;
        node.node
            .ranges
            .split(low.id, None)
            .await
            .map_err(|e| anyhow!("split {:?}: {e}", low.id))?;
        paused.resume();
        let rep = match backup.await? {
            Ok(rep) => rep,
            // Reporting an error is a correct outcome.
            Err(e) => {
                println!("backup reported an error (accepted): {e}");
                return Ok(());
            }
        };
        // 5.
        let dst = common::bucket();
        on(&dst).restore(&to, Some(&rep.id), "t", "c").await?;
        let b =
            common::start_cluster_node_tweaked(&dst, "node-b", 2, Duration::from_secs(2), |_| {})
                .await?;
        let burl = b.node.url();
        let read = within(Duration::from_secs(30), "read back after restore", async {
            loop {
                let r = Client::new()
                    .get(format!("{burl}/invocations/{id}"))
                    .send()
                    .await?;
                if r.status() == StatusCode::OK {
                    let v: Value = r.json().await?;
                    ensure!(v["status"] == "completed", "{v}");
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        b.node.shutdown().await;
        read.context(format!(
            "copy {} reported complete yet lost invocation {id} of the split range",
            rep.id
        ))
    })
    .await;
    node.node.shutdown().await;
    drop(sdk);
    r
}

/// **The same for `fork`.** Like [`backup_during_a_split_never_loses_the_split_range`],
/// with `fork --to fork1` stopping at `ctl::fork::after_range` (after range 0's
/// clone):
///
/// 1. `Counter/add` on range 0 finished; the fork starts, clones range 0 from table V,
///    stops.
/// 2. In the window: split range 0 up to the commit (table V+1).
/// 3. Release; the fork runs to the end.
///
/// Must see: either the fork reports an error, or a node built on the copy reads back
/// the `completed` invocation.
///
/// Before the fix (plus the `ctl::fork::after_range` stop point of the test commit):
/// as with the backup — the copy carried the live `cluster.conf` V+1 and the owner
/// records of the two children without their Dbs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_during_a_split_never_loses_the_split_range() -> Result<()> {
    use {
        cairn_server::ctl::Ctl,
        cairn_store::{ObjectStores, StoreLocation},
        common::fake_sdk::{FakeSdk, Step},
        std::{sync::Arc, time::Duration},
    };
    let steps = StepController::new();
    let src = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&src, "node-a", 2, Duration::from_secs(2), |_| {})
            .await?;
    let url = node.node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("added")])
        .spawn()
        .await?;
    src.allow(
        "unlisted_object",
        "the fork copy under fork1/ in the same bucket — the source's verify reads the child prefix too",
    );
    let r = within(Duration::from_secs(120), "fork during a split", async {
        common::register(&url, &sdk.url).await?;
        let key = common::key_on_partition(&node.node, "Counter", 0, "k");
        let (code, v) = common::send(&url, "Counter", "add", Some(&key), "\"y\"").await?;
        ensure!((200..300).contains(&code), "send: {code} {v}");
        let id = v["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "completed").await?;
        let low = node.node.cluster.table().ranges()[0];
        node.node
            .partitions
            .get(low.id)
            .context("engine of range 0")?
            .flush_memtable()
            .await?;
        let paused = steps.pause_at("ctl::fork::after_range");
        let fork = {
            let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
                Arc::clone(&src) as Arc<dyn slatedb::object_store::ObjectStore>,
                "",
                "memory://",
            )));
            tokio::spawn(
                async move { ctl.fork("fork1", "test@failpoints", "cairn-ctl fork").await },
            )
        };
        paused.expect_stopped(TEST_DEADLINE).await;
        node.node
            .ranges
            .split(low.id, None)
            .await
            .map_err(|e| anyhow!("split {:?}: {e}", low.id))?;
        paused.resume();
        if let Err(e) = fork.await? {
            println!("fork reported an error (accepted): {e}");
            return Ok(());
        }
        let b = common::start_cluster_node_tweaked(
            &src,
            "node-fork",
            2,
            Duration::from_secs(2),
            |cfg| {
                cfg.stores = ObjectStores::single(StoreLocation::from_store(
                    Arc::clone(&src) as Arc<dyn slatedb::object_store::ObjectStore>,
                    "fork1",
                    "memory://fork1",
                ));
            },
        )
        .await?;
        let burl = b.node.url();
        let read = within(Duration::from_secs(30), "read back on the fork", async {
            loop {
                let r = Client::new()
                    .get(format!("{burl}/invocations/{id}"))
                    .send()
                    .await?;
                if r.status() == StatusCode::OK {
                    let v: Value = r.json().await?;
                    ensure!(v["status"] == "completed", "{v}");
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        b.node.shutdown().await;
        read.context(format!("fork lost invocation {id} of the split range"))
    })
    .await;
    node.node.shutdown().await;
    drop(sdk);
    r
}

/// **A call message lost after a restore** (case A).
///
/// Order:
/// 1. The caller on range 1, the callee on range 0. The backup starts; a failpoint
///    stops it right after range 0's checkpoint — the callee does not exist in it yet.
/// 2. In the window: send the call; the caller gets the callee id (deleting the `Call`
///    outbox row), the callee suspends; flush range 1's memtable to L0.
/// 3. Release; range 1's checkpoint carries the caller waiting for the callee's result.
/// 4. Restore to a new bucket, `verify`.
///
/// Must see: exactly one `restore.lost_call` finding on range 1 naming the caller id
/// and the callee id; no `restore.lost_reply`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_finds_a_lost_call() -> Result<()> {
    let steps = StepController::new();
    let src = common::bucket();
    let c = Calls::start(&src, 1).await?;
    let ids = std::sync::Arc::new(std::sync::Mutex::new(None));
    let r = within(std::time::Duration::from_secs(90), "lost call", async {
        let (dst, found) = backup_around(&src, &steps, async {
            let got = c.call().await?;
            c.flush(1).await?;
            *ids.lock().unwrap() = Some(got);
            Ok(())
        })
        .await?;
        dst.allow("restore.lost_call", "a skewed copy built on purpose");
        let (caller, callee) = ids.lock().unwrap().clone().context("id")?;
        ensure!(found.len() == 1, "{found:?}");
        let f = &found[0];
        ensure!(
            f.check == "restore.lost_call"
                && f.range == Some(1)
                && f.detail.contains(&caller)
                && f.detail.contains(&callee),
            "{f:?}"
        );
        Ok(())
    })
    .await;
    c.node.node.shutdown().await;
    drop(c.sdk);
    r
}

/// **A reply lost after a restore** (case B, the reverse of [`restore_finds_a_lost_call`]).
///
/// Order:
/// 1. The caller on range 0, the callee on range 1. Send the call; both sides suspend;
///    flush range 0.
/// 2. The backup stops after range 0's checkpoint (the caller still waits for the
///    result).
/// 3. In the window: signal the callee ⇒ the callee finishes, the reply reaches the
///    caller, the `Reply` outbox row is deleted; the caller finishes; flush range 1.
/// 4. Release, restore, `verify`.
///
/// Must see: exactly one `restore.lost_reply` naming the caller and the callee; no
/// `restore.lost_call`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_finds_a_lost_reply() -> Result<()> {
    let steps = StepController::new();
    let src = common::bucket();
    let c = Calls::start(&src, 0).await?;
    let r = within(std::time::Duration::from_secs(90), "lost reply", async {
        let (caller, callee) = c.call().await?;
        c.flush(0).await?;
        let url = c.node.node.url();
        let (dst, found) = backup_around(&src, &steps, async {
            let r = Client::new()
                .post(format!("{url}/invocations/{callee}/signal"))
                .json(&json!({ "signal": common::fake_sdk::HOLD_SIGNAL }))
                .send()
                .await?;
            ensure!(r.status().is_success(), "signal: {}", r.status());
            common::wait_status(&url, &caller, "completed").await?;
            c.flush(1).await?;
            Ok(())
        })
        .await?;
        dst.allow("restore.lost_reply", "a skewed copy built on purpose");
        ensure!(found.len() == 1, "{found:?}");
        let f = &found[0];
        ensure!(
            f.check == "restore.lost_reply"
                && f.detail.contains(&caller)
                && f.detail.contains(&callee),
            "{f:?}"
        );
        Ok(())
    })
    .await;
    c.node.node.shutdown().await;
    drop(c.sdk);
    r
}

/// **While a range rebuilds its counters, `max-live=` lets work through.** Pins the
/// current behaviour: a soft limit, unchanged.
///
/// Order:
/// 1. `query::build_hold` is set **before** the node comes up: the background rebuild of
///    every range stands outside the engine (holding no mutex, blocking no thread), the
///    query counters not rebuilt yet.
/// 2. Limit `maxLive: 2` for tenant `default`; four invocations sleeping for an hour.
/// 3. Probe `Echo/hi` (finishes at once) across five counting ticks.
/// 4. Remove the point.
///
/// Must see: (2) all four are accepted; (3) every probe returns `200` — in the rebuild
/// window, the range is missing from the total so the limit lets work through; (4) once
/// the rebuild is done, new work gets `429 live_invocations_limit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_live_passes_while_counts_rebuild_then_refuses() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let steps = StepController::new();
    let hold = steps.fail("query::build_hold", "return");
    let bucket = common::bucket();
    let node = common::start_cluster_node_tweaked(
        &bucket,
        "node-a",
        2,
        std::time::Duration::from_secs(2),
        |cfg| cfg.tenant_usage_every = std::time::Duration::from_millis(200),
    )
    .await?;
    let url = node.node.url();
    let sdk = FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(3_600_000), Step::output("woke")],
        )
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(std::time::Duration::from_secs(60), "soft limit", async {
        common::register(&url, &sdk.url).await?;
        let r = Client::new()
            .put(format!("{url}/admin/tenants/default"))
            .json(&json!({ "maxLive": 2 }))
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "set limit: {}", r.status());
        for i in 0..4 {
            let (code, v) = common::send(&url, "Nap", "once", None, "\"x\"").await?;
            ensure!((200..300).contains(&code), "sleeper {i}: {code} {v}");
        }
        for i in 0..5 {
            let (code, v) = common::invoke(&url, "Echo", "hi", None, "\"x\"").await?;
            ensure!(code == 200, "probe {i} in the rebuild window: {code} {v}");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        drop(hold);
        loop {
            let (code, v) = common::invoke(&url, "Echo", "hi", None, "\"x\"").await?;
            if code == 429 {
                ensure!(v["code"] == "live_invocations_limit", "{v}");
                return Ok(());
            }
            ensure!(code == 200 || code == 503, "{code} {v}");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await;
    node.node.shutdown().await;
    r
}

// ─────────────────── tenant purge

/// A one-range node with auth, a purge round `keep` of 2 s, compaction coordinator poll
/// 200 ms (the full compaction of phase P3 is seen at once); tenant `acme` has one
/// finished invocation and one object with state. Returns (node, acme's key).
async fn purge_rig() -> Result<(
    cairn_server::node::RunningNode,
    String,
    common::fake_sdk::FakeSdk,
)> {
    use common::fake_sdk::{FakeSdk, Step};
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .object(
            "Counter",
            "add",
            vec![
                Step::SetState {
                    key: "n".into(),
                    value: bytes::Bytes::from_static(b"1"),
                },
                Step::output("added"),
            ],
        )
        .spawn()
        .await?;
    let mut cfg = test_config(NodeMode::single(1));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.tenant_purge_keep = Some(std::time::Duration::from_secs(2));
    cfg.stores = cfg.stores.clone().with_polls(cairn_store::PollIntervals {
        compactor: std::time::Duration::from_millis(200),
        compaction_worker: std::time::Duration::from_millis(200),
        ..cairn_store::DEFAULT_POLLS
    });
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let http = Client::new();
    let (code, v) = call(
        http.post(format!("{url}/admin/keys"))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({ "tenant": "acme", "role": "admin" })),
    )
    .await?;
    ensure!(code == StatusCode::OK, "acme key: {code} {v}");
    let key = v["secret"].as_str().unwrap_or_default().to_string();
    let (code, v) = call(
        http.post(format!("{url}/deployments"))
            .bearer_auth(&key)
            .json(&json!({ "uri": sdk.url })),
    )
    .await?;
    ensure!(code == StatusCode::OK, "register: {code} {v}");
    for path in ["/invoke/Echo/hi", "/invoke/Counter/add?key=c1"] {
        let (code, v) = call(
            http.post(format!("{url}{path}"))
                .bearer_auth(&key)
                .body("\"x\""),
        )
        .await?;
        ensure!(code == StatusCode::OK, "{path}: {code} {v}");
    }
    Ok((node, key, sdk))
}

/// `PUT …/state deleting`, then waits for `GET …/purge` to show `purged` (60 s cap).
async fn purge_to_the_end(url: &str) -> Result<Value> {
    let http = Client::new();
    let (code, v) = call(
        http.put(format!("{url}/admin/tenants/acme/state"))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({ "state": "deleting" })),
    )
    .await?;
    ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = call(
            http.get(format!("{url}/admin/tenants/acme/purge"))
                .bearer_auth(ADMIN_KEY),
        )
        .await?;
        if code == StatusCode::OK && v["state"] == "purged" {
            return Ok(v);
        }
        ensure!(
            t0.elapsed() < std::time::Duration::from_secs(60),
            "not purged after 60 s: {code} {v}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// **A tenant purge dying at each step of the round**: for each point
/// `tenant_purge::{after_hold, after_purging_mark, between_batches,
/// before_prefix_delete, after_range_proof, before_progress_delete}`, the failpoint
/// returns an error **once** — that round stops right after the step (the hold entry
/// written, the `purging-ms=` mark CASed, one batch committed, the delete proof built
/// but not deleted yet, the "range clean" proof of one range durable while the round has
/// not looked at the later ranges, every range proven but the progress entry not
/// deleted yet).
///
/// Must see: the next round reads the registry again and carries on to `purged`; the
/// bucket's `verify` at cleanup is clean (no residue, no hold entry).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_resumes_after_dying_at_every_step() -> Result<()> {
    let ctl = StepController::new();
    for point in [
        "tenant_purge::after_hold",
        "tenant_purge::after_purging_mark",
        "tenant_purge::between_batches",
        "tenant_purge::before_prefix_delete",
        "tenant_purge::after_range_proof",
        "tenant_purge::before_progress_delete",
    ] {
        let (node, _key, _sdk) = purge_rig().await?;
        let url = node.url();
        let r = {
            let _die = ctl.fail(point, "1*return(die)");
            within(TEST_DEADLINE * 3, point, purge_to_the_end(&url)).await
        };
        node.shutdown().await;
        let v = r?;
        ensure!(
            v["held"].as_array().is_some_and(Vec::is_empty),
            "{point}: {v}"
        );
    }
    Ok(())
}

/// **A range split between two row-deletion batches**: the purge round stops at
/// `tenant_purge::between_batches` after the first batch of range 0; meanwhile
/// `POST /ranges/0/split`; release the round.
///
/// Must see: the purge still reaches `purged` (the next round reloads the new table,
/// scans both child ranges, forces compaction of both); `lastRound.compacted` lists the
/// **child** ranges; `verify` at cleanup is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_survives_a_split_between_batches() -> Result<()> {
    let ctl = StepController::new();
    let (node, _key, _sdk) = purge_rig().await?;
    let url = node.url();
    let r = async {
        let paused = ctl.pause_at("tenant_purge::between_batches");
        let http = Client::new();
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        paused
            .expect_stopped(std::time::Duration::from_secs(30))
            .await;
        let (code, v) = call(
            http.post(format!("{url}/ranges/0/split"))
                .bearer_auth(ADMIN_KEY),
        )
        .await?;
        ensure!(code.is_success(), "split between two batches: {code} {v}");
        paused.resume();
        let v = within(
            TEST_DEADLINE * 3,
            "purge after split",
            purge_to_the_end_wait(&url),
        )
        .await?;
        let compacted: Vec<u64> = v["lastRound"]["compacted"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        ensure!(
            !compacted.is_empty() && !compacted.contains(&0),
            "the compacted ranges must be the child ranges: {v}"
        );
        // A split **before** range 0's compaction mark ⇒ the children do not inherit
        // "clean": each child scans and compacts itself.
        ensure!(
            v["lastRound"]["inherited"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "children of a split before the compaction mark must not inherit: {v}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// The cluster secret of the purge rig on the shared bucket (the purge round calls the
/// leader through the internal port).
const PURGE_SECRET: &str = "cf_test_cluster_secret_failpoint_purge_0123456789";

/// Like [`purge_rig`], on the **shared bucket** (the `Bucket`'s `verify` runs at
/// cleanup — check (l) scans the rows of a `purged` tenant in the Db of every live
/// range) with two ranges, and two tenants: `acme` (to be purged) and `other`. Each
/// tenant has one finished invocation and one object with state. Returns (node,
/// `other`'s key, the id of `other`'s finished invocation, SDK).
async fn purge_cluster_rig(
    bucket: &common::Bucket,
) -> Result<(
    common::ClusterNode,
    String,
    String,
    common::fake_sdk::FakeSdk,
)> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .object(
            "Counter",
            "add",
            vec![
                Step::SetState {
                    key: "n".into(),
                    value: bytes::Bytes::from_static(b"1"),
                },
                Step::output("added"),
            ],
        )
        .spawn()
        .await?;
    let n = common::start_cluster_node_tweaked(bucket, "n1", 2, Duration::from_secs(2), |cfg| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(PURGE_SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(100);
        cfg.tenant_purge_keep = Some(Duration::from_secs(2));
        // Metering of the work before `deleting` is flushed before the prefix is deleted
        // (with `keep` 2 s, the default flush tick comes after the tombstone ⇒ a
        // `metering/acme` residue).
        cfg.metering_flush = Duration::from_millis(200);
        cfg.stores = cfg.stores.clone().with_polls(cairn_store::PollIntervals {
            compactor: Duration::from_millis(200),
            compaction_worker: Duration::from_millis(200),
            ..cairn_store::DEFAULT_POLLS
        });
    })
    .await?;
    let url = n.node.url();
    let http = Client::new();
    let mut other = (String::new(), String::new());
    for tenant in ["acme", "other"] {
        let (code, v) = call(
            http.post(format!("{url}/admin/keys"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "tenant": tenant, "role": "admin" })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "key {tenant}: {code} {v}");
        let key = v["secret"].as_str().unwrap_or_default().to_string();
        let (code, v) = call(
            http.post(format!("{url}/deployments"))
                .bearer_auth(&key)
                .json(&json!({ "uri": sdk.url })),
        )
        .await?;
        ensure!(code == StatusCode::OK, "register {tenant}: {code} {v}");
        let mut done = String::new();
        for path in ["/invoke/Echo/hi", "/invoke/Counter/add?key=c1"] {
            let (code, v) = call(
                http.post(format!("{url}{path}"))
                    .bearer_auth(&key)
                    .body("\"x\""),
            )
            .await?;
            ensure!(code == StatusCode::OK, "{tenant} {path}: {code} {v}");
            if done.is_empty() {
                done = v["id"].as_str().map(str::to_string).unwrap_or_default();
            }
        }
        if tenant == "other" {
            other = (key, done);
        }
    }
    ensure!(!other.1.is_empty(), "no finished invocation id of other");
    Ok((n, other.0, other.1, sdk))
}

/// **A purge converges under splits/merges.** Interleaving:
///
/// 1. A cluster node with two ranges, `keep` 2 s; `acme` and `other` each have one
///    finished invocation and one object with state. `acme` goes `deleting`.
/// 2. Each time the purge round reaches `tenant_purge::before_final_check` — in that
///    round every range is clean of `acme`'s rows and fully compacted, every prefix is
///    empty, the tombstone not written yet — the test splits the range with the highest
///    keys and then merges its two children back (the table changes version four times;
///    the new range is a grandchild of a **compacted** range), then releases the round.
///    At most three times.
///
/// Must see: the registry becomes `purged` within those three splits/merges — the
/// children of a split made after the source range was compacted inherit "clean" (the
/// clone takes the compacted manifest), across a merge too (both sources are checked),
/// so they need no new scan and compaction; no hold entry left; `other` still reads its
/// old invocation; the `Bucket`'s `verify` at cleanup is clean — no `acme` row in the Db
/// of any live range, no object under its prefix (the purge's own progress entry
/// included).
///
/// On the old code: the tombstone demanded an unchanged table version for the whole
/// round ⇒ every split/merge dropped the round, the new range had to be scanned and
/// compacted again, then the round reached this window and met a split/merge again ⇒
/// never `purged`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_converges_while_ranges_split_and_merge() -> Result<()> {
    use std::time::Duration;
    const CHURNS: usize = 3;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, other_key, other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 4, "purge under splits/merges", async {
        let http = Client::new();
        let state = || async {
            let (code, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            Ok::<_, anyhow::Error>(v)
        };
        let mut paused = Some(ctl.pause_at("tenant_purge::before_final_check"));
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        let mut churns = 0;
        let last = loop {
            let t0 = std::time::Instant::now();
            // The round has reached the window (held there) or has written the tombstone.
            let v = loop {
                let v = state().await?;
                if v["state"] == "purged" || paused.as_ref().is_some_and(|p| p.hits() > 0) {
                    break v;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(60),
                    "the round did not reach the final check after 60 s ({churns} splits/merges): {v}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            if v["state"] == "purged" {
                break v;
            }
            ensure!(
                churns < CHURNS,
                "the purge does not converge: {churns} splits/merges at the final check and not purged: {v}"
            );
            // The round is stopped in the window: split the highest-key range, merge the two children back.
            n.node.cluster.reload().await?;
            let before = n.node.cluster.table();
            let top = before
                .ranges()
                .last()
                .map(|r| r.id)
                .ok_or_else(|| anyhow!("empty table"))?;
            let (after, _) = n
                .node
                .ranges
                .split(top, None)
                .await
                .map_err(|e| anyhow!("split {top:?}: {e}"))?;
            let kids: Vec<_> = after
                .ranges()
                .iter()
                .map(|r| r.id)
                .filter(|id| !before.contains(*id))
                .collect();
            ensure!(kids.len() == 2, "split {top:?} gave {kids:?}");
            n.node
                .ranges
                .merge(kids[0], kids[1])
                .await
                .map_err(|e| anyhow!("merge {kids:?}: {e}"))?;
            churns += 1;
            // Release the round; the next window is set again for the next round.
            drop(paused.take());
            paused = Some(ctl.pause_at("tenant_purge::before_final_check"));
        };
        drop(paused);
        ensure!(churns >= 1, "the round never got past the final check: {last}");
        ensure!(
            last["lastRound"]["inherited"]
                .as_array()
                .is_some_and(|a| !a.is_empty()),
            "the new range must inherit \"clean\" through its lineage: {last}"
        );
        ensure!(
            last["held"].as_array().is_some_and(Vec::is_empty),
            "hold entries left: {last}"
        );
        ensure!(
            last["objects"].as_object().is_some_and(|o| o.is_empty()),
            "objects left under acme's prefix: {last}"
        );
        let (code, v) = call(
            http.get(format!("{url}/invocations/{other_done}"))
                .bearer_auth(&other_key),
        )
        .await?;
        ensure!(
            code == StatusCode::OK,
            "other lost its old invocation: {code} {v}"
        );
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// A late `StartCall` message of `acme` arriving at the range holding partition key
/// `pk` — like another range's outbox drainer delivering an old row (a `StartCall` of a
/// refused tenant is still created normally, FL-24). Returns the callee id once the
/// range has created the invocation (`acme`'s status row + metadata).
async fn late_start_call(
    node: &cairn_server::node::RunningNode,
    pk: u64,
    tag: u8,
) -> Result<[u8; 24]> {
    use cairn_engine::{InterPartition, StartCall};
    let callee = cairn_store::make_invocation_id(cairn_store::PartitionKey(pk), [tag; 16]);
    let range = node
        .cluster
        .table()
        .partition_of_key(cairn_store::PartitionKey(pk));
    let h = node
        .partitions
        .get(range)
        .ok_or_else(|| anyhow!("node does not run range {range:?}"))?;
    h.inbound(InterPartition::StartCall(StartCall {
        callee,
        service: "Echo".into(),
        handler: "hi".into(),
        object_key: String::new(),
        parameter: bytes::Bytes::from_static(b"\"late\""),
        reply_to: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
        tenant: Some("acme".into()),
        sender: (
            cairn_store::make_invocation_id(cairn_store::PartitionKey(pk), [0xEE; 16]),
            u32::from(tag),
        ),
        schedule: None,
        lineage: Default::default(),
    }))
    .await
    .map_err(|e| anyhow!("inbound: {e}"))?;
    let t0 = std::time::Instant::now();
    while h
        .status(callee)
        .await
        .map_err(|e| anyhow!("{e}"))?
        .is_none()
    {
        ensure!(
            t0.elapsed() < std::time::Duration::from_secs(20),
            "the range has not created the invocation from the late StartCall"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(callee)
}

/// Whether the status row of `id` is still on the range holding it.
async fn has_status(node: &cairn_server::node::RunningNode, id: [u8; 24]) -> Result<bool> {
    let range = node
        .cluster
        .table()
        .partition_of_key(cairn_store::partition_key_of(&id));
    let h = node
        .partitions
        .get(range)
        .ok_or_else(|| anyhow!("node does not run range {range:?}"))?;
    Ok(h.status(id).await.map_err(|e| anyhow!("{e}"))?.is_some())
}

/// **Rows arriving late after the "range clean" proof** (a coordinator requirement: no
/// worse than the old code). Interleaving:
///
/// 1. A cluster node with two ranges; `acme` goes `deleting`, the purge round reaches
///    phase B.
/// 2. The first round reaches `tenant_purge::before_final_check` — every range proven.
///    The test splits the highest-key range and merges the two children back (the new
///    range carries an **inherited** proof), then delivers two late `StartCall`
///    messages of `acme`: one to range 0 (a **compaction** proof), one to the merged
///    range. Release the round.
/// 3. Wait for the next round to reach that window again.
///
/// Must see: the tombstone is **not** written in a round with rows (at step 3 the
/// registry is still `deleting`, the two late invocations have been deleted); in the
/// end `purged`, no `acme` row left (the `Bucket`'s `verify` at cleanup too).
///
/// On the first version of the convergence fix: a proven range was never scanned again
/// ⇒ the tombstone was written in that very round, and both rows survived.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_catches_rows_arriving_after_the_range_proof() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, _other_key, _other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 4, "rows arriving late", async {
        let http = Client::new();
        let state = || async {
            let (code, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            Ok::<_, anyhow::Error>(v)
        };
        // Wait for the window (held there); `purged` before it is a test bug.
        let reach = |p: &cairn_store::testkit::Paused| p.hits() > 0;
        let paused = ctl.pause_at("tenant_purge::before_final_check");
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        let t0 = std::time::Instant::now();
        while !reach(&paused) {
            let v = state().await?;
            ensure!(v["state"] != "purged", "purged before the window: {v}");
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "the round did not reach the final check: {v}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // 2. The merged range (an inherited proof), then two late rows.
        n.node.cluster.reload().await?;
        let before = n.node.cluster.table();
        let top = before
            .ranges()
            .last()
            .map(|r| r.id)
            .ok_or_else(|| anyhow!("empty table"))?;
        let (after, _) = n
            .node
            .ranges
            .split(top, None)
            .await
            .map_err(|e| anyhow!("split {top:?}: {e}"))?;
        let kids: Vec<_> = after
            .ranges()
            .iter()
            .map(|r| r.id)
            .filter(|id| !before.contains(*id))
            .collect();
        ensure!(kids.len() == 2, "split {top:?} gave {kids:?}");
        let (merged, _) = n
            .node
            .ranges
            .merge(kids[0], kids[1])
            .await
            .map_err(|e| anyhow!("merge {kids:?}: {e}"))?;
        let into = merged
            .ranges()
            .iter()
            .find(|r| !before.contains(r.id) && !kids.contains(&r.id))
            .copied()
            .ok_or_else(|| anyhow!("merged range not found"))?;
        let low = merged.ranges()[0];
        let late = [
            late_start_call(&n.node, low.start, 0xA1).await?,
            late_start_call(&n.node, into.start, 0xA2).await?,
        ];
        // Release first, then set the new window: dropping the old guard removes the
        // point by name, the new callback included. The round just released does not
        // reach the window again within its own round.
        drop(paused);
        let next = ctl.pause_at("tenant_purge::before_final_check");
        // 3. The next round reaches the window again (the dirty range scanned, compacted, newly proven).
        let t0 = std::time::Instant::now();
        while !reach(&next) {
            let v = state().await?;
            ensure!(
                v["state"] != "purged",
                "tombstone written while acme's late rows remain: {v}"
            );
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "the round did not reach the final check again: {v}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(state().await?["state"] == "deleting", "early tombstone");
        for id in late {
            ensure!(
                !has_status(&n.node, id).await?,
                "late row not deleted yet: {}",
                state().await?
            );
        }
        drop(next);
        let t0 = std::time::Instant::now();
        let v = loop {
            let v = state().await?;
            if v["state"] == "purged" {
                break v;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "not purged after 60 s: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        ensure!(
            v["held"].as_array().is_some_and(Vec::is_empty)
                && v["objects"].as_object().is_some_and(|o| o.is_empty()),
            "after purge: {v}"
        );
        for id in late {
            ensure!(!has_status(&n.node, id).await?, "late row survived");
        }
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **A tenant object written after the last LIST of phase B2** (a leak found in a
/// review). Interleaving:
///
/// 1. A cluster node with two ranges; `acme` goes `deleting`, the purge round reaches
///    phase B.
/// 2. The round reaches `tenant_purge::before_final_check`: B2 has deleted every prefix
///    of `acme`. The test writes `metering/acme/late` — the shape of a metering flush
///    (or `stats/`, `audit/tenant/`) for the `403 tenant_deleting` calls, or of the
///    archive of a retention round in flight — then releases the round.
/// 3. Wait for the next round to reach that window again.
///
/// Must see: the tombstone is **not** written in a round with a late object (at step 3
/// the registry is still `deleting`, the object has been deleted); in the end
/// `purged`, no object left under `metering/acme/` (the `Bucket`'s `verify` (l) at
/// cleanup too). Before the fix: the final check only re-read the ranges, without
/// LISTing the prefixes again ⇒ the tombstone was written in that round, and the object
/// stayed after `purged`, with no round to clean it up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_relists_prefixes_before_the_tombstone() -> Result<()> {
    use {
        slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, _other_key, _other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let late = Path::from("metering/acme/late");
    let r = within(TEST_DEADLINE * 4, "a late tenant object", async {
        let http = Client::new();
        let state = || async {
            let (code, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            Ok::<_, anyhow::Error>(v)
        };
        let paused = ctl.pause_at("tenant_purge::before_final_check");
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        wait_hit(&paused, &url, "the round reaches the final check").await?;
        // 2.
        bucket
            .put(&late, PutPayload::from_static(b"late metering"))
            .await?;
        drop(paused);
        let next = ctl.pause_at("tenant_purge::before_final_check");
        // 3.
        let t0 = std::time::Instant::now();
        while next.hits() == 0 {
            let v = state().await?;
            ensure!(
                v["state"] != "purged",
                "tombstone written while acme's prefix still has a late object: {v}"
            );
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "the round did not reach the final check again: {v}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(state().await?["state"] == "deleting", "early tombstone");
        ensure!(
            matches!(
                bucket.head(&late).await,
                Err(slatedb::object_store::Error::NotFound { .. })
            ),
            "the late object was not deleted in the next round"
        );
        drop(next);
        let t0 = std::time::Instant::now();
        loop {
            let v = state().await?;
            if v["state"] == "purged" {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "not purged after 60 s: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(
            matches!(
                bucket.head(&late).await,
                Err(slatedb::object_store::Error::NotFound { .. })
            ),
            "the late object survived the tombstone"
        );
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **Two runners: a dirty mark written after the round read the progress entries,
/// before the tombstone** (seen in a chaos run: two nodes ran the purge round at once
/// while the runner role moved between nodes). Interleaving — runner A is the node's
/// purge round, runner B is played by the test:
///
/// 1. A cluster node with two ranges; `acme` goes `deleting`; A's round reaches phase
///    B, every range proven (A reads the progress entries into RAM at the start of the
///    round), then stops at `tenant_purge::before_final_check`.
/// 2. B — whose final check sees a late row on the first range — marks that range
///    dirty (`tenant_purge::mark_dirty`), then (in B's next round) deletes the row
///    without compacting again. The test builds that part as a dirty mark on a range
///    with no row left: for A, a row already deleted by B is no longer seen by A's
///    check. Release A.
/// 3. Wait for A's next round to reach that window again.
///
/// Must see: A does **not** write the tombstone in a round where a range carries a
/// dirty mark (at step 3 the registry is still `deleting`); in the end `purged`. Before
/// the fix: A judged proof coverage by its read at the start of the round, the final
/// check was clean ⇒ the tombstone was written while the bytes of the deleted row were
/// still in the range's uncompacted SSTs (G23: a tombstone lying about the compaction).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_rereads_progress_before_the_tombstone() -> Result<()> {
    use {
        slatedb::object_store::{path::Path, ObjectStore},
        std::{sync::Arc, time::Duration},
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, _other_key, _other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 4, "the other runner's dirty mark", async {
        let http = Client::new();
        let state = || async {
            let (code, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            Ok::<_, anyhow::Error>(v)
        };
        let paused = ctl.pause_at("tenant_purge::before_final_check");
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        wait_hit(&paused, &url, "the round reaches the final check").await?;
        // 2.
        let purging = state().await?["purgingMs"]
            .as_u64()
            .ok_or_else(|| anyhow!("no purgingMs yet"))?;
        n.node.cluster.reload().await?;
        let first = n.node.cluster.table().ranges()[0].id.0;
        let store: Arc<dyn ObjectStore> = (*bucket).clone();
        cairn_store::tenant_purge::mark_dirty(
            &store,
            &Path::from(""),
            &cairn_store::TenantId::new("acme")?,
            first,
            purging,
            "runner-B#1".into(),
        )
        .await?;
        drop(paused);
        let next = ctl.pause_at("tenant_purge::before_final_check");
        // 3.
        let t0 = std::time::Instant::now();
        while next.hits() == 0 {
            let v = state().await?;
            ensure!(
                v["state"] != "purged",
                "tombstone written while range {first} carries the other runner's dirty mark: {v}"
            );
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "the round did not reach the final check again: {v}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(state().await?["state"] == "deleting", "early tombstone");
        drop(next);
        let t0 = std::time::Instant::now();
        loop {
            let v = state().await?;
            if v["state"] == "purged" {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "not purged after 60 s: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// Waits until a thread stands at `p`; `purged` before that is an error (60 s cap).
async fn wait_hit(p: &cairn_store::testkit::Paused, url: &str, what: &str) -> Result<()> {
    let http = Client::new();
    let t0 = std::time::Instant::now();
    while p.hits() == 0 {
        let (_, v) = call(
            http.get(format!("{url}/admin/tenants/acme/purge"))
                .bearer_auth(ADMIN_KEY),
        )
        .await?;
        ensure!(v["state"] != "purged", "{what}: purged too early: {v}");
        ensure!(
            t0.elapsed() < std::time::Duration::from_secs(60),
            "{what}: {v}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(())
}

/// The bytes of the progress entry `tenant-purge/acme/<range>` (missing ⇒ `None`).
async fn progress_bytes(bucket: &common::Bucket, range: u32) -> Result<Option<Vec<u8>>> {
    use slatedb::object_store::ObjectStoreExt;
    let tenant = cairn_store::TenantId::new("acme")?;
    let path = cairn_store::tenant_purge::proof_path(
        &slatedb::object_store::path::Path::from(""),
        &tenant,
        range,
    );
    match bucket.get(&path).await {
        Ok(g) => Ok(Some(g.bytes().await?.to_vec())),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// **Two runners: an old proof must not overwrite a new dirty mark.** Interleaving —
/// runner A is the node's purge round, runner B is played by the test (like the other
/// runner during a leader change):
///
/// 1. `acme` goes `deleting`; the round reaches `before_final_check` (every range
///    proven). The test delivers a late `StartCall` to range 0, then releases ⇒ A's
///    final check marks range 0 dirty (mark E0).
/// 2. In A's next round it reads E0, scans, deletes, compacts range 0 again, then stops
///    at `tenant_purge::before_range_proof` — the compaction done, the proof about to
///    be written.
/// 3. B: another late row arrives at range 0, B marks range 0 dirty again (mark E1, a
///    new id). Release A.
/// 4. Wait for A to reach `before_range_proof` again (a new scan).
///
/// Must see: at step 4 range 0's entry is **exactly the bytes** of E1 — A's proof write
/// of step 3 missed (a CAS on E0) and nobody wrote anything else; then the purge
/// converges, both late rows are deleted, the `Bucket`'s `verify` is clean.
///
/// Before the fix: B's dirty marking was a no-op when the range was already dirty
/// (E1 = E0), A overwrote the dirty mark with a proof built before the second late row,
/// then A's final check marked it dirty again — the entry at step 4 differed from E1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_stale_proof_never_overwrites_a_newer_dirty_mark() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, _other_key, _other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 4, "old proof overwrites a dirty mark", async {
        let http = Client::new();
        let state = || async {
            let (code, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            Ok::<_, anyhow::Error>(v)
        };
        // 1.
        let fin = ctl.pause_at("tenant_purge::before_final_check");
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        wait_hit(&fin, &url, "first final check").await?;
        let purging = state().await?["purgingMs"]
            .as_u64()
            .ok_or_else(|| anyhow!("no purgingMs"))?;
        let low = n.node.cluster.table().ranges()[0];
        let late1 = late_start_call(&n.node, low.start, 0xB1).await?;
        drop(fin);
        // 2. The final check marks range 0 dirty; A's next round scans, compacts, stops before writing.
        let proof_at = ctl.pause_at("tenant_purge::before_range_proof");
        wait_hit(&proof_at, &url, "A reaches the proof write window").await?;
        // 3. B: the second late row, marking dirty again.
        let late2 = late_start_call(&n.node, low.start, 0xB2).await?;
        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (*bucket).clone();
        cairn_store::tenant_purge::mark_dirty(
            &store,
            &slatedb::object_store::path::Path::from(""),
            &cairn_store::TenantId::new("acme")?,
            low.id.0,
            purging,
            "runner-B#e1".into(),
        )
        .await?;
        let e1 = progress_bytes(&bucket, low.id.0)
            .await?
            .ok_or_else(|| anyhow!("range 0 has no progress entry after B's dirty mark"))?;
        drop(proof_at);
        // 4.
        let again = ctl.pause_at("tenant_purge::before_range_proof");
        wait_hit(&again, &url, "A reaches the proof write window again").await?;
        let now = progress_bytes(&bucket, low.id.0).await?;
        ensure!(
            now.as_deref() == Some(e1.as_slice()),
            "range 0's entry changed after B's dirty mark E1 — an old proof overwrote a dirty mark: {:?}",
            now.map(|b| String::from_utf8_lossy(&b).into_owned())
        );
        drop(again);
        let t0 = std::time::Instant::now();
        loop {
            let v = state().await?;
            if v["state"] == "purged" {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "not purged after 60 s: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for id in [late1, late2] {
            ensure!(!has_status(&n.node, id).await?, "late row survived");
        }
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// The progress entry of range `range` as text (missing ⇒ empty).
async fn progress_text(bucket: &common::Bucket, range: u32) -> Result<String> {
    Ok(progress_bytes(bucket, range)
        .await?
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default())
}

/// **The children of a dirty range do not inherit.** Interleaving:
///
/// 1. `acme` goes `deleting`; the round reaches `tenant_purge::before_final_check`
///    (every range proven). The test splits range 0 → (a, b), delivers a late
///    `StartCall` of `acme` to `a`, releases ⇒ the final check sees the row and marks
///    `a` dirty.
/// 2. The next round scans `a` (a dirty range, the direct path): the first batch
///    deletes the late row, then stops at `tenant_purge::between_batches` — the row
///    deleted, **not** compacted again yet.
/// 3. The test splits `a` → (c, d) (the clone takes the uncompacted manifest: the late
///    row's bytes are still in the SSTs), then releases.
/// 4. Wait for the round to reach `before_final_check` again (every live range proven).
///
/// Must see: at step 4 the entries of `c` and `d` do **not** carry `basis=inherited`
/// (their source `a` is dirty: they must be scanned and compacted directly); then the
/// purge converges, the late row is deleted, the `Bucket`'s `verify` is clean.
///
/// Before the fix: `inherits(c)` went through `a` (no proof, the dirty mark not
/// considered) back to range 0 (a `compacted` proof) ⇒ `c`, `d` carried `inherited`,
/// and the tombstone was written while their SSTs still held `acme`'s bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_children_of_a_dirty_range_do_not_inherit() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, _other_key, _other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 4, "children of a dirty range", async {
        let http = Client::new();
        let state = || async {
            let (code, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            ensure!(code == StatusCode::OK, "GET purge: {code} {v}");
            Ok::<_, anyhow::Error>(v)
        };
        // 1.
        let fin = ctl.pause_at("tenant_purge::before_final_check");
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        wait_hit(&fin, &url, "first final check").await?;
        let split = |p: cairn_store::PartitionId| {
            let node = &n.node;
            async move {
                node.cluster.reload().await?;
                let before = node.cluster.table();
                let (after, _) = node
                    .ranges
                    .split(p, None)
                    .await
                    .map_err(|e| anyhow!("split {p:?}: {e}"))?;
                let kids: Vec<_> = after
                    .ranges()
                    .iter()
                    .copied()
                    .filter(|r| !before.contains(r.id))
                    .collect();
                ensure!(kids.len() == 2, "split {p:?} gave {kids:?}");
                Ok::<_, anyhow::Error>(kids)
            }
        };
        let low = n.node.cluster.table().ranges()[0];
        let ab = split(low.id).await?;
        let a = *ab
            .iter()
            .find(|r| r.start == low.start)
            .ok_or_else(|| anyhow!("left child of {low:?} not found: {ab:?}"))?;
        let late = late_start_call(&n.node, a.start, 0xC1).await?;
        // 2. Release first, then set the new window (dropping removes the point by name).
        drop(fin);
        let between = ctl.pause_at("tenant_purge::between_batches");
        wait_hit(&between, &url, "first delete batch on the dirty range").await?;
        ensure!(
            progress_text(&bucket, a.id.0)
                .await?
                .contains("basis=dirty"),
            "range {a:?} not dirty when scanned again"
        );
        // 3.
        let cd = split(a.id).await?;
        drop(between);
        // 4.
        let fin = ctl.pause_at("tenant_purge::before_final_check");
        wait_hit(&fin, &url, "final check after splitting the dirty range").await?;
        for kid in &cd {
            let text = progress_text(&bucket, kid.id.0).await?;
            ensure!(
                !text.contains("basis=inherited"),
                "range {:?} inherits through the dirty source {:?}: {text}",
                kid.id,
                a.id
            );
        }
        drop(fin);
        let t0 = std::time::Instant::now();
        loop {
            let v = state().await?;
            if v["state"] == "purged" {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "not purged after 60 s: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(!has_status(&n.node, late).await?, "late row survived");
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **Variant: a direct scan marks dirty before deleting.** A range without an entry
/// that goes the scan–delete–compact path (the first round of phase B, or when the
/// lineage cannot be read) and is split before its compaction finishes must not let
/// its children inherit through it. Order:
///
/// 1. `acme` goes `deleting`; the first round of phase B stops at
///    `tenant_purge::between_batches` — after the first delete batch of the first range
///    (no range has a progress entry yet).
///
/// Must see: the range just scanned already has a `basis=dirty` entry, written
/// **before** the delete batch; then the purge converges.
///
/// Before the fix: no entry until the compaction finished ⇒ the children of a range
/// split at that moment went through it to an ancestor with a proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_marks_a_range_dirty_before_sweeping_it() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (n, _other_key, _other_done, _sdk) = purge_cluster_rig(&bucket).await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE * 4, "dirty before delete", async {
        let http = Client::new();
        let between = ctl.pause_at("tenant_purge::between_batches");
        let (code, v) = call(
            http.put(format!("{url}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "deleting" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "deleting: {code} {v}");
        wait_hit(&between, &url, "first delete batch").await?;
        let mut marked = Vec::new();
        for range in n.node.cluster.table().ranges() {
            let text = progress_text(&bucket, range.id.0).await?;
            if text.contains("basis=dirty") {
                marked.push(range.id);
            }
        }
        ensure!(
            !marked.is_empty(),
            "the range being scanned has no dirty mark before the delete batch"
        );
        drop(between);
        let t0 = std::time::Instant::now();
        loop {
            let (_, v) = call(
                http.get(format!("{url}/admin/tenants/acme/purge"))
                    .bearer_auth(ADMIN_KEY),
            )
            .await?;
            if v["state"] == "purged" {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "not purged after 60 s: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    r
}

/// Like [`purge_to_the_end`] without sending `deleting` again.
async fn purge_to_the_end_wait(url: &str) -> Result<Value> {
    let http = Client::new();
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = call(
            http.get(format!("{url}/admin/tenants/acme/purge"))
                .bearer_auth(ADMIN_KEY),
        )
        .await?;
        if code == StatusCode::OK && v["state"] == "purged" {
            return Ok(v);
        }
        ensure!(
            t0.elapsed() < std::time::Duration::from_secs(60),
            "not purged after 60 s: {code} {v}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Incremental replay: dying between a `409` miss and the full resend.
///
/// 1. The SDK declares `replayCache`, returns `cairn-replay-cache` but keeps nothing —
///    every delta turn gets `409`.
/// 2. The second turn (a delta) gets the miss; point `rr_replay::after_miss` fires
///    `return` **once** ⇒ the turn fails like a transport error, no resend.
/// 3. The next attempt (the core's backoff) must be a **full** replay — the hint has
///    been dropped.
///
/// Must see: the invocation finishes with the output of an uncached run; right after the
/// cut-off miss, the SDK's next turn is full with the same `known_entries`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rr_replay_dies_between_miss_and_full_resend() -> Result<()> {
    use common::rr_sdk::{expect_ok, run_one, CacheMode, Sdk, Spec};
    let ctl = StepController::new();
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    let r = async {
        let mut spec = Spec::new("FpMiss");
        spec.mode = CacheMode::Forget;
        spec.runs = 3;
        let sdk = Sdk::spawn(spec).await?;
        within(TEST_DEADLINE, "register", async {
            loop {
                if common::register(&url, &sdk.url).await.is_ok() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await?;
        let _die = ctl.fail("rr_replay::after_miss", "1*return(die)");
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 3)?;
        let turns = sdk.turns_of(&raw);
        let first = turns
            .iter()
            .position(|t| t.miss)
            .ok_or_else(|| anyhow!("no miss turn: {turns:?}"))?;
        let next = &turns[first + 1];
        ensure!(
            !next.miss && next.replay_from == 0 && next.known == turns[first].known,
            "after the death, the next attempt must be full for the same turn: {turns:?}"
        );
        // The death is a real failed attempt (the core reschedules): the next attempt
        // carries `retry_count` 1 — unlike the immediate resend of the normal path (0).
        ensure!(next.retry == 1, "failpoint did not fire: {turns:?}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

// ───────────── a `suspended` tenant's SDK is not called after `204`

const H1_SECRET: &str = "cf_test_cluster_secret_failpoint_h1_0123456789abc";
/// The reschedule tick for a suspended tenant's turns in these tests (product: 15 minutes).
const H1_SUSPENDED_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// A one-node cluster with auth, a cluster secret (a tenant state change demands the
/// ACK of every live session) and a short reschedule tick for suspended tenants.
async fn h1_node(bucket: &common::Bucket, id: &str) -> Result<common::ClusterNode> {
    h1_node_with(bucket, id, H1_SUSPENDED_RETRY).await
}

/// Like [`h1_node`], with the suspended tenant reschedule tick `suspended_retry`.
async fn h1_node_with(
    bucket: &common::Bucket,
    id: &str,
    suspended_retry: std::time::Duration,
) -> Result<common::ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 1, std::time::Duration::from_secs(2), |cfg| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(H1_SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.limits.suspended_retry = suspended_retry;
        cfg.deploy_refresh = std::time::Duration::from_millis(100);
    })
    .await
}

/// An `admin` role key of `tenant` created with the bootstrap key — retried until the
/// node accepts writes (has claimed the lease); returns `(id, secret)`.
async fn h1_key(url: &str, tenant: &str) -> Result<(String, String)> {
    let http = Client::new();
    within(TEST_DEADLINE, "create tenant key", async {
        loop {
            let (code, v) = call(
                http.post(format!("{url}/admin/keys"))
                    .bearer_auth(ADMIN_KEY)
                    .json(&json!({ "tenant": tenant, "role": "admin" })),
            )
            .await?;
            if code == StatusCode::OK {
                return Ok((
                    v["id"].as_str().unwrap_or_default().to_string(),
                    v["secret"].as_str().unwrap_or_default().to_string(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
}

/// `PUT /admin/tenants/{tenant}/state` through `url`; demands `204` (every node applied it).
async fn h1_set_state(url: &str, tenant: &str, state: &str) -> Result<()> {
    let (code, v) = call(
        Client::new()
            .put(format!("{url}/admin/tenants/{tenant}/state"))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({ "state": state })),
    )
    .await?;
    ensure!(
        code == StatusCode::NO_CONTENT,
        "set {tenant}={state}: {code} {v}"
    );
    Ok(())
}

/// Registers a deployment with the tenant's key, then sends one `/send` to
/// `service/handler`; returns the id.
async fn h1_register_and_send(url: &str, key: &str, sdk: &str, target: &str) -> Result<String> {
    let http = Client::new();
    let (code, v) = call(
        http.post(format!("{url}/deployments"))
            .bearer_auth(key)
            .json(&json!({ "uri": sdk })),
    )
    .await?;
    ensure!(code == StatusCode::OK, "register deployment: {code} {v}");
    let (code, v) = call(
        http.post(format!("{url}/invoke/{target}/send"))
            .bearer_auth(key)
            .json(&json!("x")),
    )
    .await?;
    ensure!(code.is_success(), "/send {target}: {code} {v}");
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

/// The status of invocation `id` read with the tenant's key.
async fn h1_status(url: &str, key: &str, id: &str) -> Result<Value> {
    let (code, v) = call(
        Client::new()
            .get(format!("{url}/invocations/{id}"))
            .bearer_auth(key),
    )
    .await?;
    ensure!(code == StatusCode::OK, "GET /invocations/{id}: {code} {v}");
    Ok(v)
}

/// Throughout `window`, the endpoint receives exactly `starts` `Start`s of
/// `service/handler` and invocation `id` does not finish.
async fn h1_hold_still(
    url: &str,
    key: &str,
    sdk: &common::fake_sdk::FakeSdk,
    (service, handler): (&str, &str),
    id: &str,
    starts: usize,
    window: std::time::Duration,
) -> Result<()> {
    let t0 = std::time::Instant::now();
    while t0.elapsed() < window {
        let got = sdk.starts_of(service, handler).len();
        ensure!(
            got == starts,
            "the endpoint of a suspended (`204`) tenant got `Start` number {got} (expected exactly {starts}) after {:?}",
            t0.elapsed()
        );
        let st = h1_status(url, key, id).await?;
        ensure!(
            st["status"] != "completed",
            "the suspended tenant's invocation finished while suspended: {st}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Waits for invocation `id` to become `completed` (read with the tenant's key).
async fn h1_wait_completed(url: &str, key: &str, id: &str) -> Result<Value> {
    within(
        TEST_DEADLINE,
        "invocation finishes after the tenant is active again",
        async {
            loop {
                let st = h1_status(url, key, id).await?;
                if st["status"] == "completed" {
                    return Ok(st);
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        },
    )
    .await
}

/// **A turn granted a permit before the tenant was suspended does not call the SDK
/// after `204`.**
///
/// A soak saw a `suspended` tenant's handler called 6.5 s after the grace. The node log
/// showed that turn was granted its permit ~7 s **before** the suspend command, then
/// stood 16.6 s waiting for the WAL to become durable (failpoint
/// `writer::commit::before_write` + slow PUTs) and only opened its stream after that:
/// the tenant state was only asked at permit grant (`poll_acquire`), not when calling
/// out to the endpoint.
///
/// Interleaving:
/// 1. A one-node cluster with auth; tenant `acme` has deployment `Job/run` =
///    `[Run, Output]` (`REQUEST_RESPONSE`).
/// 2. `turn::sdk::before_call` stops the first turn reaching it: the permit granted,
///    `Running` committed, preflight passed, the durability wait done, the engine
///    released — right before the request to the endpoint.
/// 3. `/send` `Job/run` of `acme`; wait for the turn to stand at (2).
/// 4. `PUT /admin/tenants/acme/state suspended` ⇒ `204`.
/// 5. Release (2).
///
/// Must see: the endpoint receives **no** `Start` within two reschedule ticks of the
/// suspended tenant, the invocation not finished; `active` again ⇒ the invocation
/// finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_tenant_turn_granted_before_suspension_never_calls_the_sdk() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let bucket = common::bucket();
    let node = h1_node(&bucket, "h1-a").await?;
    let url = node.node.url();
    let sdk = FakeSdk::builder()
        .service(
            "Job",
            "run",
            vec![Step::Run("1".into()), Step::output("\"ok\"")],
        )
        .spawn()
        .await?;
    let r = async {
        let (_, key) = h1_key(&url, "acme").await?;
        let paused = ctl.pause_at("turn::sdk::before_call");
        let id = h1_register_and_send(&url, &key, &sdk.url, "Job/run").await?;
        paused
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        ensure!(
            sdk.starts_of("Job", "run").is_empty(),
            "the turn must stand **before** the first request"
        );
        h1_set_state(&url, "acme", "suspended").await?;
        paused.resume();
        h1_hold_still(
            &url,
            &key,
            &sdk,
            ("Job", "run"),
            &id,
            0,
            H1_SUSPENDED_RETRY * 2 + std::time::Duration::from_millis(500),
        )
        .await?;
        h1_set_state(&url, "acme", "active").await?;
        h1_wait_completed(&url, &key, &id).await?;
        Ok(())
    }
    .await;
    node.node.shutdown().await;
    r
}

/// **A follow-up turn within the same run also asks the tenant state again.** One permit
/// covers a whole run: on `REQUEST_RESPONSE` each `ctx.run` ends one request and the
/// runtime sends the next request at once (`Step::Again`), on BIDI the runtime resumes
/// in place — each time the handler reruns from the start in the SDK. A soak saw two
/// such calls (17:38:18.7 and 17:38:20.9) in one run granted its permit 36 s before
/// the suspend command.
///
/// Interleaving:
/// 1. As in [`suspended_tenant_turn_granted_before_suspension_never_calls_the_sdk`],
///    but `Job/run` goes through the fake SDK's gate: the first request stands in the SDK.
/// 2. `/send`; wait for the SDK to get the first `Start` (standing at the gate).
/// 3. Set a stop at `turn::sdk::before_call`; open the gate for one turn: the first
///    request returns `Run` + a proposed result, the runtime writes it and prepares the
///    next request — stopping there.
/// 4. `PUT …/state suspended` ⇒ `204`; release (3).
///
/// Must see: the endpoint receives exactly **one** `Start` within two reschedule ticks;
/// `active` again ⇒ the invocation finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_tenant_continuation_in_the_same_run_never_calls_the_sdk() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let bucket = common::bucket();
    let node = h1_node(&bucket, "h1-b").await?;
    let url = node.node.url();
    let sdk = FakeSdk::builder()
        .gated_service(
            "Job",
            "run",
            vec![Step::Run("1".into()), Step::output("\"ok\"")],
        )
        .spawn()
        .await?;
    let r = async {
        let (_, key) = h1_key(&url, "acme").await?;
        let id = h1_register_and_send(&url, &key, &sdk.url, "Job/run").await?;
        within(
            std::time::Duration::from_secs(10),
            "first request reaches the SDK",
            async {
                while sdk.starts_of("Job", "run").is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Ok(())
            },
        )
        .await?;
        let paused = ctl.pause_at("turn::sdk::before_call");
        sdk.gate().release(1);
        paused
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        h1_set_state(&url, "acme", "suspended").await?;
        paused.resume();
        h1_hold_still(
            &url,
            &key,
            &sdk,
            ("Job", "run"),
            &id,
            1,
            H1_SUSPENDED_RETRY * 2 + std::time::Duration::from_millis(500),
        )
        .await?;
        sdk.gate().open();
        h1_set_state(&url, "acme", "active").await?;
        h1_wait_completed(&url, &key, &id).await?;
        Ok(())
    }
    .await;
    node.node.shutdown().await;
    r
}

// ───── cluster-level registries apply in read order (the same class as above)

/// A two-node cluster with auth and a cluster secret for the registry apply order
/// tests. Refresh tick 2 s: the writer's notification nudges the round at once, and the
/// "fresh key registry" cap (two ticks) is wide enough that the check does not meet the
/// close-when-unsure `503` while B's round stands at the stop point.
async fn order_pair(
    bucket: &common::Bucket,
    a: &str,
    b: &str,
) -> Result<(common::ClusterNode, common::ClusterNode)> {
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(H1_SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = std::time::Duration::from_secs(2);
    };
    let ttl = std::time::Duration::from_secs(2);
    let na = common::start_cluster_node_tweaked(bucket, a, 1, ttl, tweak).await?;
    let nb = common::start_cluster_node_tweaked(bucket, b, 1, ttl, tweak).await?;
    Ok((na, nb))
}

/// `GET /tenant` through `url` with key `key`: `(code, tenant state)`.
async fn tenant_state_at(url: &str, key: &str) -> Result<(StatusCode, Value)> {
    let (code, v) = call(Client::new().get(format!("{url}/tenant")).bearer_auth(key)).await?;
    Ok((code, v["state"].clone()))
}

/// One registry write through `url` that demands no ACK (creating a key for another
/// tenant): the writer notifies every peer, and their refresh loop runs a full round at once.
async fn poke_registry(url: &str) -> Result<()> {
    let (code, v) = call(
        Client::new()
            .post(format!("{url}/admin/keys"))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({ "tenant": "other", "role": "invoke" })),
    )
    .await?;
    ensure!(
        code == StatusCode::OK,
        "create a key to nudge the refresh loop: {code} {v}"
    );
    Ok(())
}

/// **The `tenants` registry applies in read order, not arrival order.** A registry write
/// takes the generation to odd then even and notifies the peers: the peer's refresh
/// loop (`RefreshLoop`) runs a full round right then, and its `tenants` GET may return
/// the copy from **before** the write and arrive **after** the read of the apply
/// request (`/internal/registry/apply`) that the `204` relies on. Before the fix it
/// applied over it: a node that had acked the `suspended` state treated the tenant as
/// `active` again until the next round.
///
/// Interleaving:
/// 1. A two-node cluster `ord-a`, `ord-b`; tenant `acme` has a key.
/// 2. `tenants::refresh::loaded` stops B's first `tenants` re-read at that point
///    (between the GET and the apply); nudge B's refresh loop with a registry write
///    through A — the round reads the copy from **before** the suspension, then stops.
/// 3. `PUT /admin/tenants/acme/state suspended` through A ⇒ `204`: the apply request on
///    B re-reads (a second time, through the stop point) and applies the new copy.
/// 4. Set a stop at B's `node::refresh_loop::before_tick`, then release (2): the old
///    copy reaches the apply point; the next round stops before re-reading.
///
/// Must see: in the window between the two rounds, B still returns `state: suspended`
/// for `acme`'s `GET /tenant`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_tenants_read_never_undoes_an_acked_suspension() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (a, b) = order_pair(&bucket, "ord-a", "ord-b").await?;
    let (a_url, b_url) = (a.node.url(), b.node.url());
    let r = async {
        let (_, key) = h1_key(&a_url, "acme").await?;
        within(
            std::time::Duration::from_secs(10),
            "B gets acme's key",
            async {
                while tenant_state_at(&b_url, &key).await?.0 != StatusCode::OK {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Ok(())
            },
        )
        .await?;
        let stale = ctl.pause_at_on(&["ord-b"], "tenants::refresh::loaded");
        poke_registry(&a_url).await?;
        stale
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        h1_set_state(&a_url, "acme", "suspended").await?;
        let (code, st) = tenant_state_at(&b_url, &key).await?;
        ensure!(
            code == StatusCode::OK && st == "suspended",
            "B has acked: {code} {st}"
        );
        let next = ctl.pause_at_on(&["ord-b"], "node::refresh_loop::before_tick");
        stale.resume();
        next.expect_stopped(std::time::Duration::from_secs(10))
            .await;
        let (code, st) = tenant_state_at(&b_url, &key).await?;
        next.resume();
        ensure!(
            code == StatusCode::OK && st == "suspended",
            "a read from before the suspension overwrote the copy B acked: {code} {st}"
        );
        Ok(())
    }
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// **The key registry applies in read order**: as in
/// [`stale_tenants_read_never_undoes_an_acked_suspension`] for `DELETE
/// /admin/keys/{id}`. The old fence (`local_writes`) only stopped the node's **own**
/// writes; a peer's write does not go through it.
///
/// Interleaving:
/// 1. A two-node cluster `ordk-a`, `ordk-b`; `acme`'s key K works on B.
/// 2. `auth::keys::refresh::loaded` stops B's first key registry re-read; nudge B's
///    refresh loop — the round reads the registry still holding K, then stops.
/// 3. `DELETE /admin/keys/K` through A ⇒ `204` (the apply request on B reads the
///    registry without K).
/// 4. Set a stop at B's `node::refresh_loop::before_tick`, release (2).
///
/// Must see: in the window between the two rounds, B rejects K (`401`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_keys_read_never_undoes_an_acked_revocation() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    let (a, b) = order_pair(&bucket, "ordk-a", "ordk-b").await?;
    let (a_url, b_url) = (a.node.url(), b.node.url());
    let r = async {
        let (id, key) = h1_key(&a_url, "acme").await?;
        within(std::time::Duration::from_secs(10), "B gets key K", async {
            while tenant_state_at(&b_url, &key).await?.0 != StatusCode::OK {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Ok(())
        })
        .await?;
        let stale = ctl.pause_at_on(&["ordk-b"], "auth::keys::refresh::loaded");
        poke_registry(&a_url).await?;
        stale
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        let (code, v) = call(
            Client::new()
                .delete(format!("{a_url}/admin/keys/{id}"))
                .bearer_auth(ADMIN_KEY),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "revoke K: {code} {v}");
        let (code, _) = tenant_state_at(&b_url, &key).await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED,
            "B acked the revocation yet: {code}"
        );
        let next = ctl.pause_at_on(&["ordk-b"], "node::refresh_loop::before_tick");
        stale.resume();
        next.expect_stopped(std::time::Duration::from_secs(10))
            .await;
        let (code, st) = tenant_state_at(&b_url, &key).await?;
        next.resume();
        ensure!(
            code == StatusCode::UNAUTHORIZED,
            "a read from before the revocation overwrote the registry B acked — K works again: {code} {st}"
        );
        Ok(())
    }
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// **A suspected gap at startup does not exist** — the test pins the right invariant: a
/// node reading the registries at startup **before** a write always blocks that write's
/// `204` until it applies it itself. `node/start.rs` opens the session
/// (`sessions/<node>/<inc>`) **before** reading `auth/keys` and `tenants`, so the LIST
/// of `confirm_peers` after the write always sees it; while it does not serve HTTP yet,
/// the apply request runs past its cap ⇒ `202`.
///
/// Interleaving:
/// 1. Node `boot-a` runs; tenant `acme` has a key.
/// 2. Node `boot-b` starts, stopping at `node::start::after_registry_read`: the session
///    written, the registries read (`acme` active), not serving HTTP yet.
/// 3. `PUT /admin/tenants/acme/state suspended` through A.
///
/// Must see: `202` with `boot-b` in `pending` — not `204`. Release (2), send the same
/// command again ⇒ `204`, and B returns `state: suspended`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn booting_node_that_read_the_registry_blocks_the_ack() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(H1_SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = std::time::Duration::from_millis(200);
    };
    let ttl = std::time::Duration::from_secs(2);
    let a = common::start_cluster_node_tweaked(&bucket, "boot-a", 1, ttl, tweak).await?;
    let a_url = a.node.url();
    let r = async {
        let (_, key) = h1_key(&a_url, "acme").await?;
        let booting = ctl.pause_at("node::start::after_registry_read");
        let b = tokio::spawn({
            let shared = std::sync::Arc::clone(&*bucket);
            async move {
                common::start_cluster_node_tweaked(&shared, "boot-b", 1, ttl, tweak).await
            }
        });
        booting.expect_stopped(std::time::Duration::from_secs(20)).await;
        let put = || {
            call(
                Client::new()
                    .put(format!("{a_url}/admin/tenants/acme/state"))
                    .bearer_auth(ADMIN_KEY)
                    .json(&json!({ "state": "suspended" })),
            )
        };
        let (code, v) = put().await?;
        ensure!(
            code == StatusCode::ACCEPTED
                && v["pending"]
                    .as_array()
                    .is_some_and(|p| p.iter().any(|n| n == "boot-b")),
            "B read the registry before the write and has not applied it, yet: {code} {v}"
        );
        booting.resume();
        let b = b.await??;
        let b_url = b.node.url();
        let acked = within(TEST_DEADLINE, "B applies, then `204`", async {
            loop {
                let (code, v) = put().await?;
                if code == StatusCode::NO_CONTENT {
                    return Ok(());
                }
                ensure!(code == StatusCode::ACCEPTED, "{code} {v}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        })
        .await;
        let st = tenant_state_at(&b_url, &key).await;
        b.node.shutdown().await;
        acked?;
        let (code, st) = st?;
        ensure!(code == StatusCode::OK && st == "suspended", "B: {code} {st}");
        Ok(())
    }
    .await;
    a.node.shutdown().await;
    r
}

/// Check (i) of `cairn-ctl verify` reported false "orphaned" query index rows while
/// the cluster was still writing.
///
/// Interleaving:
/// 1. Range 0 has a few invocations, the index background build finished (the
///    `readable` mark); the engine does **not** `seal` — like a live node during a daily
///    `verify` or the final `verify` of a chaos run.
/// 2. `verify` reads the range's hot zone (metadata + status).
/// 3. The test cuts into `ctl::verify::between_query_scans`: the engine creates a new
///    invocation (metadata, status and index rows in **one** commit), waits until a
///    separate reader of the test sees it, adds a few tailer ticks for `verify`'s
///    reader, then releases.
/// 4. `verify` reads the index zone. Every pass through the point (including the
///    re-check after `settle()`) repeats step 3, otherwise the re-check is clean and the
///    test goes green by mistake.
///
/// Must see: no `query_index` finding — both zones must be read at the same mark. On the
/// old code: `3 query index rows orphaned or corrupt (no metadata)` and `extra 3` (the 3
/// `Always` rows of an invocation without attributes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_query_index_reads_one_snapshot_while_the_range_writes() -> Result<()> {
    use {
        bytes::Bytes,
        cairn_engine::{BuildCursor, BuildStep, CreateOptions, Engine, Target},
        cairn_runtime::StartParams,
        cairn_server::ctl::{Ctl, VerifyOptions},
        cairn_store::{
            make_invocation_id, query_index as qi, testkit::FailGuard, LeadershipGrant,
            ObjectStores, PartitionId, PartitionKey, PartitionWriter, StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path as ObjPath, ObjectStoreExt},
        std::{sync::Arc, time::Duration},
    };
    let _ctl = StepController::new();
    let mem = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(mem.clone(), "", "memory://"));
    mem.put(
        &ObjPath::from("cluster.conf"),
        "cairnflow-cluster/3\ntable_version=1\nnext_range_id=1\n\
         range=0000000000000000-ffffffffffffffff:0:0\n"
            .into(),
    )
    .await?;
    let target = Target {
        tenant: Default::default(),
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: String::new(),
    };
    let writer = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    let mut engine = Engine::open(writer).await?;
    let id = |n: u8| make_invocation_id(PartitionKey(u64::from(n)), [n; 16]);
    for n in 1..=3u8 {
        engine
            .create(
                id(n),
                &target,
                CreateOptions::default(),
                StartParams::default(),
                Bytes::new(),
            )
            .await?;
    }
    let mut cursor = BuildCursor::default();
    while engine.query_build_step(&mut cursor, 1024).await? != BuildStep::Idle {}

    // The stop point: reports "arrived", then waits for the go-ahead (60 s cap). The
    // callback runs synchronously on `verify`'s thread ⇒ `block_in_place`.
    let (arrived_tx, arrived_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let go_rx = std::sync::Mutex::new(go_rx);
    let _point = FailGuard::callback("ctl::verify::between_query_scans", move || {
        let _ = arrived_tx.send(());
        tokio::task::block_in_place(|| {
            let rx = go_rx.lock().unwrap_or_else(|e| e.into_inner());
            let _ = rx.recv_timeout(Duration::from_secs(60));
        });
    });

    let mut verify = tokio::spawn({
        let stores = stores.clone();
        async move { Ctl::new(stores).verify(VerifyOptions::default()).await }
    });
    let probe = stores.open_reader(0, Duration::from_millis(200)).await?;
    let metas = |rows: &[(Bytes, Bytes)]| {
        rows.iter()
            .filter(|(k, _)| qi::parse_meta_key(k).is_some())
            .count()
    };
    let mut arrived_rx = arrived_rx;
    let mut created = 3usize;
    let deadline = tokio::time::Instant::now() + TEST_DEADLINE;
    // Every pass of `verify` through the point (the first scan, the re-check after
    // `settle()`) cuts in one invocation; stop when `verify` finishes.
    let rep = loop {
        let arrived = tokio::select! {
            a = arrived_rx.recv() => a,
            r = &mut verify => break r??,
            () = tokio::time::sleep_until(deadline) => anyhow::bail!("verify did not finish"),
        };
        ensure!(arrived.is_some(), "the stop point was removed");
        created += 1;
        engine
            .create(
                id(u8::try_from(created)?),
                &target,
                CreateOptions::default(),
                StartParams::default(),
                Bytes::new(),
            )
            .await?;
        let start = std::time::Instant::now();
        loop {
            let rows = probe.scan_raw(&[0x00], 100_000, |_| true).await?;
            if metas(&rows) >= created {
                break;
            }
            ensure!(
                start.elapsed() < Duration::from_secs(30),
                "the test's reader does not see the new invocation"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // `verify`'s reader polls on the same 200 ms tick: five more ticks. Too few and
        // the test goes green by mistake on the old code, not red by mistake on the new.
        tokio::time::sleep(Duration::from_millis(1000)).await;
        go_tx.send(())?;
    };
    let found: Vec<_> = rep
        .findings
        .iter()
        .filter(|f| f.check == "query_index")
        .collect();
    ensure!(
        found.is_empty(),
        "verify reports falsely while the range is still writing: {found:?}"
    );
    let _ = probe.close().await;
    engine.seal().await?;
    Ok(())
}

/// **Lifting a suspension grants permits to parked tickets, pulling no retry row.**
/// Formerly lifting a suspension pulled the 15-minute retry rows back in batches of
/// ≤ 256 per tick, because every wake-up turn refused a permit had rescheduled a row.
/// Now a background turn refused a permit parks a ticket in the scheduler's line:
/// lifting the suspension (`set_refused`) brings the line back into the eligible round
/// and the tickets are granted under the turn limit like every waiter — one turn per
/// ticket, no background tick pulling rows, no engine hold proportional to the number of
/// tickets. The batch pulling path remains for retry rows of turns that **had** a
/// permit and were stopped at preflight, or of throttled turns with a caller.
///
/// 1. A one-range node, the suspended tenant reschedule tick 600 s. `acme` sends `N` =
///    300 `Nap/once` calls (a 15 s timer); wait until the SDK has received all `N`
///    Starts, suspend `acme`.
/// 2. The timers come due: every wake-up turn is refused a permit (wait for
///    `cairnflow_flow_blocked_total{resource="tenant"}` to grow by ≥ `N`) and parks a
///    ticket.
/// 3. Attach callbacks at `turn::wake_lifted::step` (opening a pulling tick with rows)
///    and `turn::wake_lifted::row` (one row firing); lift the suspension (`204`).
///
/// Must see: all `N` `completed` within 60 s — no turn waiting for the 600 s tick; no
/// row firing through the pulling path. Before the fix: all `N` rows fired through the
/// pulling path, ≤ 256 per tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsuspend_grants_parked_tickets_without_sweeping_rows() -> Result<()> {
    use {
        cairn_store::testkit::FailGuard,
        common::fake_sdk::{FakeSdk, Step},
        std::{
            sync::{Arc, Mutex},
            time::Duration,
        },
    };
    const N: usize = 300;
    let _ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(15_000), Step::output("woke")],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&bucket, "lift", 1, Duration::from_secs(2), |cfg| {
            cfg.auth = cairn_server::auth::AuthConfig {
                required: true,
                admin_key: Some(ADMIN_KEY.into()),
                cluster_secret: Some("cf_test_cluster_secret_failpoint_lift_0123456789".into()),
                metrics_key: None,
                admin_key_sha256: None,
                provision_key_sha256: None,
            };
            cfg.limits.suspended_retry = Duration::from_secs(600);
            cfg.deploy_refresh = Duration::from_millis(100);
        })
        .await?;
    let url = node.node.url();
    let http = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let steps: Arc<Mutex<Vec<usize>>> = Arc::default();
    let r = within(Duration::from_secs(180), "lifting pulls retry rows in batches", async {
        // An `admin` key of `acme` (retried until the node has claimed the lease).
        let t0 = std::time::Instant::now();
        let key = loop {
            let (code, v) = call(
                http.post(format!("{url}/admin/keys"))
                    .bearer_auth(ADMIN_KEY)
                    .json(&json!({ "tenant": "acme", "role": "admin" })),
            )
            .await?;
            if code == StatusCode::OK {
                break v["secret"].as_str().unwrap_or_default().to_string();
            }
            ensure!(t0.elapsed() < TEST_DEADLINE, "create acme key: {code} {v}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let (code, v) = call(
            http.post(format!("{url}/deployments"))
                .bearer_auth(&key)
                .json(&json!({ "uri": sdk.url })),
        )
        .await?;
        ensure!(code.is_success(), "register deployment: {code} {v}");
        let set_state = |state: &'static str| {
            call(
                http.put(format!("{url}/admin/tenants/acme/state"))
                    .bearer_auth(ADMIN_KEY)
                    .json(&json!({ "state": state })),
            )
        };
        let blocked = || async {
            let text = http
                .get(format!("{url}/metrics"))
                .bearer_auth(ADMIN_KEY)
                .send()
                .await?
                .text()
                .await?;
            anyhow::Ok(
                text.lines()
                    .find(|l| {
                        l.starts_with("cairnflow_flow_blocked_total{resource=\"tenant\"}")
                    })
                    .and_then(|l| l.rsplit(' ').next())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0),
            )
        };

        // 1. `N` calls into timers, sent in parallel in batches of 25.
        for chunk in 0..N / 25 {
            let sends = (0..25).map(|i| {
                call(
                    http.post(format!("{url}/invoke/Nap/once/send"))
                        .bearer_auth(&key)
                        .body(json!(format!("n{}", chunk * 25 + i)).to_string()),
                )
            });
            for r in futures::future::join_all(sends).await {
                let (code, v) = r?;
                ensure!(code.is_success(), "send: {code} {v}");
            }
        }
        let t0 = std::time::Instant::now();
        while sdk.starts_len() < N {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the SDK has only received {} / {N} Starts",
                sdk.starts_len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let blocked0 = blocked().await?;
        let (code, v) = set_state("suspended").await?;
        ensure!(code == StatusCode::NO_CONTENT, "suspend acme: {code} {v}");

        // 2. Every wake-up turn is refused a permit and parks a ticket.
        let t0 = std::time::Instant::now();
        while blocked().await? < blocked0 + N as u64 {
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "only {} / {N} turns refused a permit so far",
                blocked().await? - blocked0
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 3. Count the rows firing per pulling tick, then lift the suspension.
        let s = steps.clone();
        let _step = FailGuard::callback("turn::wake_lifted::step", move || {
            s.lock().unwrap_or_else(|e| e.into_inner()).push(0);
        });
        let s = steps.clone();
        let _row = FailGuard::callback("turn::wake_lifted::row", move || {
            if let Some(n) = s.lock().unwrap_or_else(|e| e.into_inner()).last_mut() {
                *n += 1;
            }
        });
        let (code, v) = set_state("active").await?;
        ensure!(code == StatusCode::NO_CONTENT, "lift acme's suspension: {code} {v}");
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = call(
                http.post(format!("{url}/query"))
                    .bearer_auth(&key)
                    .json(&json!({ "sql": "SELECT count(*) FROM sys_invocation WHERE status = 'completed'" })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "query: {code} {v}");
            if v["rows"] == json!([[N]]) {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "60 s after lifting only {} / {N} completed (retry rows waiting for the 600 s tick); pulling ticks: {:?}",
                v["rows"],
                steps.lock().unwrap_or_else(|e| e.into_inner())
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let seen = steps.lock().unwrap_or_else(|e| e.into_inner()).clone();
        ensure!(
            seen.iter().sum::<usize>() == 0,
            "the pulling path fired retry rows — a turn refused a permit rescheduled instead of parking a ticket: {seen:?}"
        );
        Ok(())
    })
    .await;
    node.node.shutdown().await;
    r
}

/// **Dying between a segment COPY and the commit of the caller's completion** (AP-1).
///
/// 1. One node with one range, the values sweep every 100 ms, `min_age` 0, retention 0;
///    tenant `default` `max-payload=1 MiB`; a fake wire v6 SDK (an SDK before v6
///    receiving 1.5 MiB assembled > the cap gets 413).
/// 2. `Front/go[f]` calls `Chat/messages[c]`, which returns 1.5 MiB ⇒ a sealed `Output`;
///    the `Reply` row is larger than the caller's cap ⇒ the shell must COPY.
/// 3. Point `value::copy_after_put` returns an error **once**: the COPY is durable under
///    owner `Front[f]`, the completion not committed ⇒ the outbox row stays; the next
///    drain COPYs again under a new `gen` and delivers.
///
/// Must see: `go` `completed` with the whole 1.5 MiB; `cairnflow_value_copies_total`
/// ≥ 2 (the point fired); the orphaned COPY of the first attempt
/// is deleted by the sweep — in the end owner `Front[f]` has exactly two segments (the
/// delivered COPY + `go`'s output); `verify` is clean at cleanup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn value_copied_between_put_and_commit_is_swept() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    const HISTORY: usize = 3 << 19;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(
        &bucket,
        "a",
        1,
        std::time::Duration::from_secs(60),
        |cfg| {
            cfg.value_sweep = Some(std::time::Duration::from_millis(100));
            cfg.value_min_age = std::time::Duration::ZERO;
            cfg.trash_keep = Some(std::time::Duration::ZERO);
        },
    )
    .await?;
    common::wait_probe(
        &[&n],
        std::time::Duration::from_secs(30),
        "a is leader of range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let mut history = vec![b'h'; HISTORY];
    history[0] = b'"';
    history[HISTORY - 1] = b'"';
    let history = bytes::Bytes::from(history);
    let sdk = FakeSdk::builder()
        .wire(6)
        .object("Chat", "messages", vec![Step::Output(history.clone())])
        .object(
            "Front",
            "go",
            vec![
                Step::Call {
                    service: "Chat".into(),
                    handler: "messages".into(),
                    key: Some("c".into()),
                    idempotency_key: None,
                    spread: false,
                },
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let url = n.node.url();
    let r = within(TEST_DEADLINE, "COPY dying mid-way", async {
        common::register(&url, &sdk.url).await?;
        let r = Client::new()
            .put(format!("{url}/admin/tenants/default"))
            .json(&json!({ "maxPayload": 1 << 20 }))
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "set limit: {}", r.status());
        let _once = ctl.fail("value::copy_after_put", "1*return(die)");
        let (code, v) = common::send(&url, "Front", "go", Some("f"), "{}").await?;
        ensure!(code < 300, "{code} {v}");
        let id = v["id"].as_str().context("id")?.to_string();
        common::wait_status(&url, &id, "completed").await?;
        let (code, s) = common::invocation(&url, &id).await?;
        ensure!(
            code == 200 && s["output"].as_str().map(str::len) == Some(HISTORY),
            "go: {code} {}",
            s.to_string().chars().take(200).collect::<String>()
        );
        // The point fired: two COPYs (the dying one, then the delivery), not one.
        let metrics = Client::new()
            .get(format!("{url}/metrics"))
            .send()
            .await?
            .text()
            .await?;
        let copies = metrics
            .lines()
            .find_map(|l| l.strip_prefix("cairnflow_value_copies_total "))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        ensure!(
            copies >= 2,
            "value::copy_after_put did not fire: {copies} COPYs"
        );
        let tenant = cairn_store::TenantId::default();
        let (pk, owner) = cairn_store::value::object_owner(&tenant, None, "Front", "f")
            .context("Front[f] has an owner")?;
        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (*bucket).clone();
        let values = cairn_store::value::ValueStore::new(
            store,
            &slatedb::object_store::path::Path::default(),
        );
        loop {
            let mine = values.objects("default", pk, &owner).await?;
            if mine.len() == 2 {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **Tripwire: the engine never needs the whole bytes of a segmented value**
/// (AGENTS.md §6.4).
///
/// `value::engine_needs_bytes` sits on every engine branch that meets a segmented value
/// where it would have to assemble it (a promise/signal completion, an output delivered
/// directly under the engine, a promise record). The test sets `panic`, then runs every
/// segmented value path on a two-range node, a fake wire v6 SDK (the node's default
/// read path ⇒ the shell assembles):
///
/// 1. A tenant without a cap: `Chat/seal[c]` writes 1.5 MiB of state (sealed),
///    `Chat/read[c]` reads it lazily and returns it (a sealed output, the ingress
///    assembles); `Chat/messages` returns 1.5 MiB; `Front/{same,other}[f]` call it on the
///    same range and on another range (delivered inline: the shell assembles);
///    `Front/att[f]` `attach` + `getOutput` of a finished `messages` (a `Reply` row for
///    the shell).
/// 2. Set `maxPayload` 1 MiB: `Front/same` once more ⇒ 1.5 MiB > the cap ⇒ the shell
///    COPYs.
///
/// Must see: every turn `completed` with all the bytes; the point never fires (one fire
/// is a `panic` on the engine thread — the turn would never finish).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_never_needs_the_bytes_of_a_segmented_value() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::sync::{Arc, Mutex},
    };
    const H: usize = 3 << 19;
    let ctl = StepController::new();
    let _wire = ctl.fail("value::engine_needs_bytes", "panic");
    let mut h = vec![b'h'; H];
    h[0] = b'"';
    h[H - 1] = b'"';
    let h = bytes::Bytes::from(h);
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(
        &bucket,
        "a",
        2,
        std::time::Duration::from_secs(60),
        |_| {},
    )
    .await?;
    common::wait_probe(
        &[&n],
        std::time::Duration::from_secs(30),
        "a is leader of both ranges",
        |p| {
            [0, 1]
                .iter()
                .all(|r| p.get(r).is_some_and(|v| v == &vec![0]))
        },
    )
    .await?;
    let range_of = |service: &str, key: &str| {
        let (pk, _) =
            cairn_store::value::object_owner(&cairn_store::TenantId::default(), None, service, key)
                .expect("owner");
        n.node.cluster.partition_of_key(pk)
    };
    let home = range_of("Front", "f");
    let same = (0..200)
        .map(|i| format!("s{i}"))
        .find(|k| range_of("Chat", k) == home)
        .expect("a key on the same range");
    let other = (0..200)
        .map(|i| format!("o{i}"))
        .find(|k| range_of("Chat", k) != home)
        .expect("a key on another range");
    let call = |key: &str| Step::Call {
        service: "Chat".into(),
        handler: "messages".into(),
        key: Some(key.into()),
        idempotency_key: None,
        spread: false,
    };
    let target = Arc::new(Mutex::new(String::new()));
    let sdk = FakeSdk::builder()
        .wire(6)
        .object(
            "Chat",
            "seal",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: h.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "read",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::EchoCallResult,
            ],
        )
        .object("Chat", "messages", vec![Step::Output(h.clone())])
        .object("Front", "same", vec![call(&same), Step::EchoCallResult])
        .object("Front", "other", vec![call(&other), Step::EchoCallResult])
        .object(
            "Front",
            "att",
            vec![
                Step::Attach(target.clone()),
                Step::GetOutput(target.clone()),
                Step::OutputResults,
            ],
        )
        .spawn()
        .await?;
    let url = n.node.url();
    let r = within(
        TEST_DEADLINE,
        "segmented value paths under the tripwire",
        async {
            common::register(&url, &sdk.url).await?;
            let full = |v: &Value, what: &str| {
                ensure!(
                    v["output"].as_str().map(str::len) == Some(H),
                    "{what}: {}",
                    v.to_string().chars().take(200).collect::<String>()
                );
                Ok(())
            };
            // 1.
            for (svc, handler, key) in [
                ("Chat", "seal", "c"),
                ("Chat", "read", "c"),
                ("Front", "same", "f"),
                ("Front", "other", "f"),
            ] {
                let (code, v) = common::invoke(&url, svc, handler, Some(key), "{}").await?;
                ensure!(code == 200, "{svc}/{handler}: {code} {v}");
                if handler != "seal" {
                    full(&v, &format!("{svc}/{handler}"))?;
                }
            }
            let (code, v) = common::invoke(&url, "Chat", "messages", Some("done"), "{}").await?;
            ensure!(code == 200, "messages: {code}");
            full(&v, "messages")?;
            *target.lock().unwrap() = v["id"].as_str().context("id")?.to_string();
            let (code, v) = common::invoke(&url, "Front", "att", Some("f"), "{}").await?;
            ensure!(
                code == 200 && v["output"].as_str().map(str::len).unwrap_or(0) > 2 * H,
                "att: {code} {}",
                v.to_string().chars().take(200).collect::<String>()
            );
            // 2.
            let r = Client::new()
                .put(format!("{url}/admin/tenants/default"))
                .json(&json!({ "maxPayload": 1 << 20 }))
                .send()
                .await?;
            ensure!(r.status() == StatusCode::OK, "set limit: {}", r.status());
            let (code, v) = common::send(&url, "Front", "same", Some("f2"), "{}").await?;
            ensure!(code < 300, "{code} {v}");
            let id = v["id"].as_str().context("id")?.to_string();
            common::wait_status(&url, &id, "completed").await?;
            let (code, s) = common::invocation(&url, &id).await?;
            ensure!(code == 200, "{code}");
            full(&s, "Front/same via COPY")?;
            Ok(())
        },
    )
    .await;
    n.node.shutdown().await;
    r
}

/// **A cluster-wide listing has a cap on its local part** (AGENTS.md §6.5): the fan-out
/// to other nodes has a 5 s cap, the node's own part (`local_invocations`) did not — a
/// range stuck in its engine held the whole request until the range let go.
///
/// 1. A one-range node, already leading, one deployment (without a deployment the
///    background loop runs no tick).
/// 2. Stop the background tick at `turn::fire_retries` — the range's engine **is held**.
/// 3. `GET /invocations?scope=cluster`.
///
/// Must see: `200` within 20 s, `partial: true`, range 0 in `missingRanges`. On the old
/// code: the request hung until the stop point released itself (120 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_list_answers_when_a_local_range_is_stuck() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let paused = ctl.pause_at("turn::fire_retries");
    paused
        .expect_stopped(std::time::Duration::from_secs(10))
        .await;
    let r = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        call(Client::new().get(format!("{url}/invocations?scope=cluster"))).await
    })
    .await;
    drop(paused);
    let (code, v) = r.map_err(|_| {
        anyhow!("GET /invocations?scope=cluster hung past 20 s while range 0's engine was held")
    })??;
    ensure!(code == StatusCode::OK, "{code} {v}");
    ensure!(v["partial"] == true, "must report missing: {v}");
    ensure!(
        v["missingRanges"]
            .as_array()
            .is_some_and(|a| a.iter().any(|r| r == 0)),
        "range 0 must be in missingRanges: {v}"
    );
    Ok(())
}

/// **An abandoned listing request does not scan the range.** The 4 s per-range cap of
/// `local_invocations` gives up waiting, but the `ListInvocations` command already sent
/// stays in the engine queue and, when its turn comes, scans the range's whole status
/// index **while holding the engine** — for a requester that has left. Seen in a
/// `--large-values` chaos run: scanning a cold range on a saturated store took longer
/// than the cap, the harness asked again every few seconds, each ask adding another
/// scan to the queue ⇒ the queue piled up hundreds of seconds of work, the drain phase
/// failed (ranges 3, 4, 5 missing in all 6 asks) and every later `GET /invocations/{id}`
/// queued behind ~17 minutes of abandoned scans.
///
/// 1. A one-range node, already leading, one deployment. A callback at
///    `actor::list::scan` counts how many listing scans actually run.
/// 2. Stop the background tick at `turn::fire_retries` — the range's engine **is held**.
/// 3. Three concurrent `GET /invocations?status=running`: each returns `200` after the
///    4 s cap, `x-cairn-partial: true` — the requester has left, the command is still
///    queued.
/// 4. Release the background tick. One more `GET /invocations?status=running`: it queues
///    **behind** the three abandoned commands (the engine lock is FIFO), and returns in
///    full (no range missing).
///
/// Must see: exactly **one** scan (the ask of step 4). On the old code: four — the three
/// abandoned commands still scanned, each holding the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_list_does_not_scan_the_range() -> Result<()> {
    use {
        cairn_store::testkit::FailGuard,
        common::fake_sdk::{FakeSdk, Step},
        std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let scans = Arc::new(AtomicUsize::new(0));
    let _count = {
        let scans = Arc::clone(&scans);
        FailGuard::callback("actor::list::scan", move || {
            scans.fetch_add(1, Ordering::SeqCst);
        })
    };
    let list = || {
        let url = url.clone();
        async move {
            let r = Client::new()
                .get(format!("{url}/invocations?status=running"))
                .send()
                .await?;
            let partial = r
                .headers()
                .get("x-cairn-partial")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let code = r.status();
            Ok::<_, anyhow::Error>((code, partial))
        }
    };
    let r = within(TEST_DEADLINE, "abandoned listing does not scan", async {
        // 2.
        let paused = ctl.pause_at("turn::fire_retries");
        paused
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        // 3.
        for answer in futures::future::join_all((0..3).map(|_| list())).await {
            let (code, partial) = answer?;
            ensure!(
                code == StatusCode::OK,
                "ask while the engine is held: {code}"
            );
            ensure!(
                partial.as_deref() == Some("true"),
                "engine held ⇒ the range runs past its cap, must report missing: {partial:?}"
            );
        }
        ensure!(
            scans.load(Ordering::SeqCst) == 0,
            "no command has got the engine yet"
        );
        // 4.
        drop(paused);
        let (code, partial) = list().await?;
        ensure!(code == StatusCode::OK, "ask after the release: {code}");
        ensure!(
            partial.as_deref() != Some("true"),
            "engine released ⇒ the range must answer: {partial:?}"
        );
        let n = scans.load(Ordering::SeqCst);
        ensure!(
            n == 1,
            "{n} listing scans — three commands whose requesters left still scanned the range while holding the engine"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A listing by status releases the engine between chunks**: a filter by status alone
/// had no scan cap, and the scan read the range's whole status index within **one**
/// engine hold — a range with 26 k invocations held the engine for seconds for one
/// `GET /invocations?status=running`, and every turn of the range waited.
///
/// 1. A one-range node; 2 100 invocations (`Echo/hi`).
/// 2. A callback at `actor::list::chunk` counts how many times a listing scan takes
///    the engine.
/// 3. `GET /invocations?status=running` — matching no row once every turn has
///    finished, so the scan goes through the whole index.
///
/// Must see: the answer is complete (no range missing), and the scan takes the engine at
/// least 3 times (each time ≤ 1 024 index rows). Old code: 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_list_releases_the_engine_between_chunks() -> Result<()> {
    use {
        cairn_store::testkit::FailGuard,
        common::fake_sdk::{FakeSdk, Step},
        std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    const N: usize = 2_100;
    let _ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    // A hang guard, not a speed check: creating 2 100 invocations alone is ~70 s of one
    // core in a debug build on a slower x86 machine (~33 ms per invocation), past the
    // usual `TEST_DEADLINE`.
    let r = within(std::time::Duration::from_secs(180), "chunked listing", async {
        for _ in 0..N / 50 {
            let sends = (0..50).map(|_| common::send(&url, "Echo", "hi", None, "{}"));
            for s in futures::future::join_all(sends).await {
                let (code, v) = s?;
                ensure!(code < 300, "send: {code} {v}");
            }
        }
        // Every turn finished (no `running` left): the scan does not stop early at `limit`.
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) =
                call(Client::new().get(format!("{url}/invocations?status=running"))).await?;
            ensure!(code == StatusCode::OK, "{code} {v}");
            if v.as_array().is_some_and(Vec::is_empty) {
                break;
            }
            ensure!(
                t0.elapsed() < std::time::Duration::from_secs(60),
                "invocations still running after 60 s: {v}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let chunks = Arc::new(AtomicUsize::new(0));
        let _count = {
            let chunks = Arc::clone(&chunks);
            FailGuard::callback("actor::list::chunk", move || {
                chunks.fetch_add(1, Ordering::SeqCst);
            })
        };
        let r = Client::new()
            .get(format!("{url}/invocations?status=running"))
            .send()
            .await?;
        let partial = r
            .headers()
            .get("x-cairn-partial")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        ensure!(r.status() == StatusCode::OK, "{}", r.status());
        ensure!(
            partial.as_deref() != Some("true"),
            "listing missing a range: {partial:?}"
        );
        let n = chunks.load(Ordering::SeqCst);
        ensure!(
            n >= 3,
            "scanned {N} index rows in {n} engine holds — must release between chunks of ≤ 1 024 rows"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The query counters are only built when someone asks**: formerly every term, right
/// after the index became `readable`, the background build scanned the range's whole
/// `ByCreated` (O(number of invocations), each chunk holding the engine) to build the
/// in-RAM counters — under chaos with a `kill -9` every 10 s, ~110 times per 5 minutes
/// across the cluster, 0.6 s → 4.4 s each time as the range grew. The counters' users
/// are `sys_invocation_counts`, the route chooser and the `max-live=` limit; when nobody
/// asks there is no need to build them.
///
/// 1. A callback at `query::count_step` counts the counter build chunks.
/// 2. A one-range node, 5 finished `Echo/hi` invocations; wait for every index to be
///    `readable` (`cairnflow_query_index_state`), then watch for 1 s.
/// 3. Ask `sys_invocation_counts` until the answer is complete.
///
/// Must see: no counter build chunk before the first question; a complete answer within
/// 20 s counting exactly 5. Old code: the counters were built as soon as `readable`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_counts_are_built_on_demand() -> Result<()> {
    use {
        cairn_store::testkit::FailGuard,
        common::fake_sdk::{FakeSdk, Step},
        std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        std::time::{Duration, Instant},
    };
    let _ctl = StepController::new();
    let steps = Arc::new(AtomicUsize::new(0));
    let _count = {
        let steps = Arc::clone(&steps);
        FailGuard::callback("query::count_step", move || {
            steps.fetch_add(1, Ordering::SeqCst);
        })
    };
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let r = within(TEST_DEADLINE, "counters on demand", async {
        for _ in 0..5 {
            let (code, v) = common::send(&url, "Echo", "hi", None, "{}").await?;
            ensure!(code < 300, "send: {code} {v}");
            let id = v["id"].as_str().context("id")?.to_string();
            common::wait_status(&url, &id, "completed").await?;
        }
        let http = Client::new();
        let t0 = Instant::now();
        loop {
            let text = http
                .get(format!("{url}/metrics"))
                .send()
                .await?
                .text()
                .await?;
            let readable = cairn_store::query_index::QUERY_INDEXES.iter().all(|ix| {
                let p = format!(
                    "cairnflow_query_index_state{{index=\"{}\",state=\"readable\"}} ",
                    ix.name
                );
                text.lines()
                    .find_map(|l| l.strip_prefix(p.as_str()))
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    == Some(1)
            });
            if readable {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "index not readable after 30 s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(1) {
            let n = steps.load(Ordering::SeqCst);
            ensure!(
                n == 0,
                "{n} counter build chunks while nobody asked — every term scans the whole ByCreated"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let t0 = Instant::now();
        loop {
            let (code, v) = call(
                http.post(format!("{url}/query"))
                    .json(&json!({ "sql": "SELECT sum(count) AS n FROM sys_invocation_counts" })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "{code} {v}");
            if v["partial"] == false {
                ensure!(v["rows"][0][0] == 5, "wrong count: {v}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "counters not built 20 s after the question: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(
            steps.load(Ordering::SeqCst) > 0,
            "a complete answer while no build chunk ran"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The hint cleanup round goes in small paced chunks and carries its cursor across
/// terms**: formerly every term, 60 s after the background build, `hint_sweep` rescanned
/// **from the start** every `ObjectKeys` row of the range, chunks of 1 024 rows × up to
/// 3 reads within **one** engine hold — under chaos with a `kill -9` every 60 s: up to
/// 1.3 s of engine holds per second across the cluster, 12.96 missed blocks per commit,
/// noisy throughput −78 %.
///
/// 1. A one-range node; 40 objects (`Obj/run`, keys `k00`..`k39`) ⇒ 40 hint rows. The
///    cleanup round of the first term is still waiting for `HINT_SWEEP_DELAY`.
/// 2. Enable `query::hint_sweep_now` (no wait), a callback at `query::hint_sweep_step`
///    records the time of each chunk; release the range ⇒ a new term.
/// 3. Once there are ≥ 3 chunks: release the range again ⇒ a third term in the middle
///    of the cleanup round.
/// 4. Wait until 3 s pass without a chunk.
///
/// Must see: ≥ 3 chunks within 10 s (small chunks — each ≤ 4 rows), the gap between two
/// consecutive chunks of one term ≥ 200 ms (paced), and the total chunks of the whole
/// round ≤ 40/4 + 2 (the third term **carries on** the cursor, it does not rescan from
/// the start). Old code: 1 chunk scanning all 40 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hint_sweep_is_paced_and_resumes_across_terms() -> Result<()> {
    use {
        cairn_store::testkit::FailGuard,
        common::fake_sdk::{FakeSdk, Step},
        std::{
            sync::{Arc, Mutex},
            time::{Duration, Instant},
        },
    };
    const N: usize = 40;
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let resign = || async {
        let h = node
            .partitions
            .get(cairn_store::PartitionId(0))
            .ok_or_else(|| anyhow!("range 0 has no engine"))?;
        h.resign().await.map_err(|e| anyhow!("resign 0: {e}"))?;
        Ok::<_, anyhow::Error>(())
    };
    let r = within(TEST_DEADLINE, "paced hint cleanup round", async {
        // 1.
        for i in 0..N {
            let key = format!("k{i:02}");
            let (code, v) = common::invoke(&url, "Obj", "run", Some(&key), "{}").await?;
            ensure!(code == 200, "Obj/run {key}: {code} {v}");
        }
        // 2.
        let steps: Arc<Mutex<Vec<Instant>>> = Arc::default();
        let _now = ctl.fail("query::hint_sweep_now", "return");
        let _step = {
            let steps = Arc::clone(&steps);
            FailGuard::callback("query::hint_sweep_step", move || {
                steps.lock().unwrap().push(Instant::now());
            })
        };
        let count = || steps.lock().unwrap().len();
        resign().await?;
        // 3.
        let t0 = Instant::now();
        while count() < 3 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "{} chunks after 10 s — one chunk scanned all {N} hint rows in one engine hold",
                count()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let first: Vec<Instant> = steps.lock().unwrap().clone();
        for w in first.windows(2) {
            let gap = w[1].duration_since(w[0]);
            ensure!(
                gap >= Duration::from_millis(200),
                "two chunks {gap:?} apart — no pacing between chunks"
            );
        }
        resign().await?;
        // 4.
        let t0 = Instant::now();
        let mut last = (count(), Instant::now());
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let n = count();
            if n != last.0 {
                last = (n, Instant::now());
            } else if last.1.elapsed() >= Duration::from_secs(3) {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "the cleanup round not settled after 60 s ({n} chunks)"
            );
        }
        let total = count();
        ensure!(
            total <= N / 4 + 2,
            "{total} chunks for {N} rows — a new term rescanned from the start instead of carrying the cursor"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A leader change while a tenant is suspended, then lifting the suspension.** A
/// background turn refused a permit **parks a ticket** in the scheduler's line — the
/// term's RAM — instead of rescheduling 15 minutes ahead, so there is no batched pull of
/// retry rows left to interrupt with a leader change. The product's default
/// reschedule tick (15 minutes).
///
/// 1. `n1` leads the range (one range). `acme` sends `N` = 300 `Nap/once` calls (a 3 s
///    timer); suspend `acme`; every wake-up turn is refused a permit and parks a ticket.
/// 2. Kill `n1` while `acme` is still suspended; build `n2` — the tickets are lost with
///    `n1`, `n2`'s `recover` dispatches all `N` again, and they park tickets again (the
///    tenant is still suspended).
/// 3. Lift the suspension through `n2`.
///
/// Must see: the SDK gets no further `Start` before the suspension is lifted (tickets
/// do not give up, do not run while suspended); all `N` `completed` within 90 s of
/// killing `n1`; the SDK gets exactly `2N` `Start`s — no duplicate turn (one entry per
/// invocation in the line, FL-30).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_change_while_suspended_runs_every_parked_turn() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    const N: usize = 300;
    const TTL: Duration = Duration::from_secs(5);
    let _ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(3_000), Step::output("woke")],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some("cf_test_cluster_secret_failpoint_lift_mid_0123456789".into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(100);
    };
    let n1 = common::start_cluster_node_tweaked(&bucket, "n1", 1, TTL, tweak).await?;
    let url1 = n1.node.url();
    let http = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let blocked = |url: String| {
        let http = http.clone();
        async move {
            let text = http
                .get(format!("{url}/metrics"))
                .bearer_auth(ADMIN_KEY)
                .send()
                .await?
                .text()
                .await?;
            anyhow::Ok(
                text.lines()
                    .find(|l| l.starts_with("cairnflow_flow_blocked_total{resource=\"tenant\"}"))
                    .and_then(|l| l.rsplit(' ').next())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0),
            )
        }
    };
    let key = within(Duration::from_secs(120), "tickets park on n1", async {
        let t0 = std::time::Instant::now();
        let key = loop {
            let (code, v) = call(
                http.post(format!("{url1}/admin/keys"))
                    .bearer_auth(ADMIN_KEY)
                    .json(&json!({ "tenant": "acme", "role": "admin" })),
            )
            .await?;
            if code == StatusCode::OK {
                break v["secret"].as_str().unwrap_or_default().to_string();
            }
            ensure!(t0.elapsed() < TEST_DEADLINE, "create acme key: {code} {v}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let (code, v) = call(
            http.post(format!("{url1}/deployments"))
                .bearer_auth(&key)
                .json(&json!({ "uri": sdk.url })),
        )
        .await?;
        ensure!(code.is_success(), "register deployment: {code} {v}");
        for chunk in 0..N / 25 {
            let sends = (0..25).map(|i| {
                call(
                    http.post(format!("{url1}/invoke/Nap/once/send"))
                        .bearer_auth(&key)
                        .body(json!(format!("n{}", chunk * 25 + i)).to_string()),
                )
            });
            for r in futures::future::join_all(sends).await {
                let (code, v) = r?;
                ensure!(code.is_success(), "send: {code} {v}");
            }
        }
        let t0 = std::time::Instant::now();
        while sdk.starts_len() < N {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the SDK has only received {} Starts",
                sdk.starts_len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (code, v) = call(
            http.put(format!("{url1}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "suspended" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "suspend acme: {code} {v}");
        // Every wake-up turn is refused a permit (the 3 s timer due) and parks a ticket.
        let t0 = std::time::Instant::now();
        loop {
            let b = blocked(url1.clone()).await?;
            if b >= N as u64 {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "only {b} / {N} turns refused so far"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(key)
    })
    .await?;

    // 2.
    drop(n1);
    let killed = std::time::Instant::now();
    let n2 = common::start_cluster_node_tweaked(&bucket, "n2", 1, TTL, tweak).await?;
    let url2 = n2.node.url();
    let r = within(Duration::from_secs(120), "n2 comes up while suspended, then lift", async {
        let t0 = std::time::Instant::now();
        while !leads_first_range(&n2.node).await {
            ensure!(t0.elapsed() < Duration::from_secs(30), "n2 does not lead the range yet");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // n2's tickets park again (the tenant is still suspended): the process's refusal
        // counter grows by at least `N` compared with when n1 died.
        let before = blocked(url2.clone()).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let starts = sdk.starts_of("Nap", "once").len();
        ensure!(
            starts == N,
            "the SDK got {starts} Starts while acme was still suspended (expected {N})"
        );
        let (code, v) = call(
            http.put(format!("{url2}/admin/tenants/acme/state"))
                .bearer_auth(ADMIN_KEY)
                .json(&json!({ "state": "active" })),
        )
        .await?;
        ensure!(code == StatusCode::NO_CONTENT, "lift acme's suspension: {code} {v}");
        loop {
            let (code, v) = call(
                http.post(format!("{url2}/query"))
                    .bearer_auth(&key)
                    .json(&json!({ "sql": "SELECT count(*) FROM sys_invocation WHERE status = 'completed'" })),
            )
            .await?;
            if code == StatusCode::OK && v["rows"] == json!([[N]]) {
                break;
            }
            ensure!(
                killed.elapsed() < Duration::from_secs(90),
                "90 s after killing n1 only {} / {N} completed (refused on n2 since {before})",
                v["rows"]
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let starts = sdk.starts_of("Nap", "once").len();
        ensure!(
            starts == 2 * N,
            "the SDK got {starts} Starts (expected exactly {}): duplicate turns",
            2 * N
        );
        Ok(())
    })
    .await;
    n2.node.shutdown().await;
    r
}

/// `node` leads the first range of the table (probed like `common::start_led`).
async fn leads_first_range(node: &cairn_server::node::RunningNode) -> bool {
    let Some(r) = node.cluster.table().ranges().first().cloned() else {
        return false;
    };
    let probe = cairn_store::make_invocation_id(cairn_store::PartitionKey(r.start), [0; 16]);
    match node.partitions.get(r.id) {
        Some(h) => matches!(
            tokio::time::timeout(std::time::Duration::from_millis(300), h.status(probe)).await,
            Ok(Ok(_))
        ),
        None => false,
    }
}

/// **The local listing (`scope=local`, the default) also has a time cap**
/// (AGENTS.md §6.5): like [`cluster_list_answers_when_a_local_range_is_stuck`] but
/// without `?scope=cluster` — before the fix only the local part of the cluster-wide
/// listing had a cap.
///
/// 1. A one-range node, already leading, one deployment.
/// 2. Stop the background tick at `turn::fire_retries` — the range's engine **is held**.
/// 3. `GET /invocations`.
///
/// Must see: `200` within 20 s, header `x-cairn-partial: true` (a range that does not
/// answer is dropped — the listing is incomplete and says so). On the old code: the
/// request hung until the stop point released itself (120 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_list_answers_when_a_range_is_stuck() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let paused = ctl.pause_at("turn::fire_retries");
    paused
        .expect_stopped(std::time::Duration::from_secs(10))
        .await;
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        Client::new().get(format!("{url}/invocations")).send(),
    )
    .await;
    drop(paused);
    let r =
        r.map_err(|_| anyhow!("GET /invocations hung past 20 s while range 0's engine was held"))??;
    let code = r.status();
    let partial = r
        .headers()
        .get("x-cairn-partial")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = r.text().await?;
    ensure!(code == StatusCode::OK, "{code} {body}");
    ensure!(
        partial.as_deref() == Some("true"),
        "a range that does not answer must be reported missing, header x-cairn-partial = {partial:?}: {body}"
    );
    Ok(())
}

/// **The `failpoints` binary can run `--cluster` on `file://`**: the `DelayStore` layer
/// (`node::put_delay`, chaos `--slow-node`) wrapped the store with
/// `StoreLocation::wrap` and dropped `local_dir` — a node on a local directory then got
/// no directory lock and no emulated CAS, so it did not start.
///
/// Must see: the binary prints both ports and `GET /health` returns `200`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failpoints_binary_runs_a_cluster_on_a_local_dir() -> Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "cairn-fp-local-cluster-{}-{}",
        std::process::id(),
        cairn_store::entropy::now_ms()
    ));
    std::fs::create_dir_all(&dir)?;
    let uri = format!("file://{}", dir.display());
    let spawned = common::binary::Binary::spawn(
        &[
            "--cluster",
            "--partitions",
            "2",
            "--main-store",
            &uri,
            "--port",
            "0",
            "--internal-port",
            "0",
            "--no-auth",
        ],
        &[],
    );
    let r = async {
        let node = spawned?;
        let code = reqwest::get(format!("{}/health", node.url)).await?.status();
        ensure!(code == StatusCode::OK, "{code}\n{}", node.dump());
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&dir);
    r
}

/// **Dying between the table CAS and the history write ⇒ `table restore` refuses**
/// (SM-22/SM-23). The history is written **after** the CAS, so this window leaves the
/// history one version behind; the highest version then is not the last table, and
/// rebuilding it would take the cluster back to before one commit.
///
/// Order:
/// 1. Node A alone, two ranges; the history has version 1.
/// 2. Failpoint `cluster::history::before_write` = `1*off->return`: the first history
///    write (the split's intent CAS, version 2) goes through, every later one bails out
///    — the shape of "dying right after the commit CAS (version 3)".
/// 3. A splits range 0 (commit at version 3), then stops cleanly.
/// 4. Delete `cluster.conf` from outside; `table restore` (dry, then `--apply`).
///
/// Must see: the history stops at version 2; both `table restore` runs refuse
/// (`Refused`, "cannot prove … is the last range table") pointing at the gap, and no
/// `cluster.conf` is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_restore_refuses_when_history_lags_a_won_cas() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = start_alone(&bucket, "lag-a").await?;
    // No node condition: the split is called directly from the test's task, outside
    // every thread carrying a current node — and A is the only node.
    let lag = ctl.fail("cluster::history::before_write", "1*off->return");
    let split = within(TEST_DEADLINE, "A splits range 0", async {
        a.node
            .ranges
            .split(cairn_store::PartitionId(0), None)
            .await
            .map_err(|e| anyhow!("{e}"))
    })
    .await;
    drop(lag);
    a.node.shutdown().await;
    let (table, _) = split?;
    ensure!(table.version() == 3, "{table:?}");
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        (*bucket).clone(),
        "",
        "memory://",
    ));
    let main = stores.main();
    let history = cairn_store::cluster_history::versions(main.store(), main.prefix()).await?;
    ensure!(
        history == [1, 2],
        "the history must stop at version 2: {history:?}"
    );

    let conf = Path::from("cluster.conf");
    let saved = bucket.get(&conf).await?.bytes().await?;
    bucket.delete(&conf).await?;
    let ctl = cairn_server::ctl::Ctl::new(stores);
    for apply in [false, true] {
        let got = ctl
            .table_restore(&cairn_server::ctl::RepairOptions {
                apply,
                force: true,
                who: "test@failpoints".into(),
                command: "cairn-ctl table restore".into(),
                expect_version: None,
                now_ms: cairn_store::entropy::now_ms(),
            })
            .await;
        match got {
            Err(cairn_server::ctl::CtlError::Refused(why)) => ensure!(
                why.contains("cannot prove") && why.contains("version 2"),
                "apply={apply}: {why}"
            ),
            other => {
                bucket.put(&conf, saved.clone().into()).await?;
                return Err(anyhow!(
                    "apply={apply}: the history is behind yet restore returned {other:?}"
                ));
            }
        }
        ensure!(
            bucket.head(&conf).await.is_err(),
            "apply={apply}: refused yet still wrote cluster.conf"
        );
    }
    // Give the real table back so that `verify` at bucket cleanup does not mask the result.
    bucket.put(&conf, saved.into()).await?;
    Ok(())
}

/// **`table restore` rebuilds a table carrying an intent whose commit won.** Order:
///
/// 1. Node A alone, two ranges. Failpoint `cluster::history::before_write` =
///    `1*off->return`: the history has version 2 (the table with the split intent of
///    range 0), not version 3 (the commit). `split::commitCas` = `panic`: the
///    coordinator dies **right after** the commit CAS wins — the `ops/<key>` log is
///    still `Prepared`. `session::retire::before_write` = `return`, and A runs no
///    orphan round: range 0 carries no tombstone (as if the whole process died in that
///    window). With the round on, A itself writes the tombstone on behalf (SM-11) once
///    range 0 is released and off the table — whether that happens before step 3
///    depends on timing.
/// 2. A's `follow_table` loop opens the two children; create an invocation on the left
///    child and wait for it to be `completed` (a write acked into the child).
/// 3. Delete `cluster.conf` from outside, stop A. `table restore --apply --force`.
/// 4. Build node B on a bucket wrapped in a `FaultyStore` that fails the first GET of
///    `ops/` (a transient log read error — the core's `resolve` rolls back when the log
///    read fails).
///
/// Must see: B reads back the `completed` invocation of step 2.
///
/// Before the fix: `table restore` wrote back table version 2 **carrying the intent**
/// (all four checks passed: a `Prepared` log is in neither refused branch); B claimed
/// range 0, the log read failed ⇒ it rolled back: the two children's Dbs went into the
/// trash, range 0 reopened in its pre-split state ⇒ the invocation was lost (`404`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_restore_rolls_forward_a_prepared_split_whose_commit_won() -> Result<()> {
    use {
        cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        common::fake_sdk::{FakeSdk, Step},
        slatedb::object_store::{path::Path, ObjectStoreExt},
        std::time::Duration,
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let a = common::start_cluster_node_tweaked(&bucket, "c1-a", 2, Duration::from_secs(1), |cfg| {
        cfg.retire_orphans_every = None;
        cfg.reconcile_intents_every = Some(Duration::from_millis(200));
    })
    .await?;
    common::wait_probe(
        &[&a],
        Duration::from_secs(20),
        "one node leads every range",
        |p| p.len() == 2 && p.values().all(|v| v == &[0]),
    )
    .await?;
    let url = a.node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("added")])
        .spawn()
        .await?;
    common::register(&url, &sdk.url).await?;
    let lag = ctl.fail("cluster::history::before_write", "1*off->return");
    let die = ctl.fail("split::commitCas", "panic");
    let no_tomb = ctl.fail("session::retire::before_write", "return");
    let split = {
        let ranges = a.node.ranges.clone();
        tokio::spawn(async move { ranges.split(cairn_store::PartitionId(0), None).await })
    };
    ensure!(
        split.await.is_err_and(|e| e.is_panic()),
        "the coordinator did not die at split::commitCas"
    );
    drop(die);
    // 2. The live table is version 3; the left child accepts writes.
    a.node.cluster.reload().await?;
    let t = a.node.cluster.table();
    ensure!(t.version() == 3, "the commit has not won: {t:?}");
    let left = t.ranges()[0];
    ensure!(left.id.0 >= 2, "the first range is not a child: {t:?}");
    let key = common::key_on_partition(&a.node, "Counter", left.id.0, "k");
    let (code, v) = common::send(&url, "Counter", "add", Some(&key), "\"x\"").await?;
    ensure!((200..300).contains(&code), "send: {code} {v}");
    let id = v["id"].as_str().unwrap_or_default().to_string();
    common::wait_status(&url, &id, "completed").await?;
    a.node
        .partitions
        .get(left.id)
        .context("engine of the left child")?
        .flush_memtable()
        .await?;
    // 3.
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        (*bucket).clone(),
        "",
        "memory://",
    ));
    let main = stores.main();
    let history = cairn_store::cluster_history::versions(main.store(), main.prefix()).await?;
    ensure!(history == [1, 2], "history: {history:?}");
    bucket.delete(&Path::from("cluster.conf")).await?;
    a.node.shutdown().await;
    drop(lag);
    drop(no_tomb);
    let rep = cairn_server::ctl::Ctl::new(stores)
        .table_restore(&cairn_server::ctl::RepairOptions {
            apply: true,
            force: true,
            who: "test@failpoints".into(),
            command: "cairn-ctl table restore --apply --force".into(),
            expect_version: None,
            now_ms: cairn_store::entropy::now_ms(),
        })
        .await;
    println!("table restore: {rep:?}");
    // 4.
    let faulty = FaultyStore::wrap((*bucket).clone());
    faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("ops/".into()),
        Action::Fail { times: Some(1) },
    ));
    let b = common::start_cluster_node_on(&faulty, "c1-b", 2, Duration::from_secs(1), |cfg| {
        cfg.retire_orphans_every = Some(Duration::from_millis(200));
        cfg.reconcile_intents_every = Some(Duration::from_millis(200));
    })
    .await?;
    let burl = b.url();
    let read = within(
        Duration::from_secs(30),
        "read back after table restore",
        async {
            loop {
                let r = Client::new()
                    .get(format!("{burl}/invocations/{id}"))
                    .send()
                    .await?;
                if r.status() == StatusCode::OK {
                    let v: Value = r.json().await?;
                    ensure!(v["status"] == "completed", "{v}");
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        },
    )
    .await;
    b.shutdown().await;
    drop(sdk);
    read.context(format!(
        "invocation {id} acked into a child of a committed split was lost after table restore"
    ))
}

/// **`detach_now` does not write the manifest of a quiescing source.** Interleaving:
///
/// 1. A lone node, one range; the node's lineage round off. Split 0 → (L, R): both
///    children stand on ancestor 0 (depth 1).
/// 2. A separate `Lineage` of the test (depth 1) runs `tick` until a child S finishes a
///    full compaction (`Tick::Compacted`) — S enters `detaching`, not detached yet; wait
///    3 s for the compactor's checkpoint (1 s expiry) to expire — the detach now has
///    work to do.
/// 3. One more `tick` stops at `lineage::before_detach`: the round has read the list of
///    led ranges (S is in it), and is about to `detach_now(S)`.
/// 4. Split S, stopping at `split::cloneLeft` (after the close mark: S is quiesced).
/// 5. Record every write (`FaultyStore::record_writes`), release the lineage round.
///
/// Must see: no write under `ranges/<S>/manifest/` at step 5 — the manifest of a
/// quiesced source stands still (anchor G5 / `quiesced_source_frozen` of M2). Then
/// release the split, it commits.
///
/// Before the fix (plus the `lineage::before_detach` stop point of the test commit):
/// `detach_pending` did not consider intents ⇒ `detach_now(S)` CASed S's manifest,
/// dropping the ancestor entry while a child was cloning. The window is narrower than
/// the review said: a quiescing range leaves the led list, so a tick **starting** after
/// the close mark does not detach it (tried, not red) — only a tick that read the list
/// before that reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lineage_never_detaches_a_quiesced_split_source() -> Result<()> {
    use {
        cairn_server::lineage::{Lineage, LineageConfig, Tick},
        cairn_store::{ObjectStores, PartitionId, StoreLocation},
        common::fake_sdk::{FakeSdk, Step},
        std::time::{Duration, Instant},
    };
    let ctl = StepController::new();
    let bucket = common::bucket();
    let polls = cairn_store::PollIntervals {
        manifest: Duration::from_secs(3600),
        compactor: Duration::from_millis(100),
        compaction_worker: Duration::from_millis(100),
        gc: Duration::from_secs(3600),
        gc_idle_skip_max: Duration::from_secs(3600),
        gc_min_age: Duration::ZERO,
    };
    let n = common::start_cluster_node_tweaked(&bucket, "c4", 1, Duration::from_secs(2), |cfg| {
        cfg.lineage.depth = 0;
        cfg.stores = cfg
            .stores
            .clone()
            .with_polls(polls)
            .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    })
    .await?;
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(
        Duration::from_secs(120),
        "detach on a quiesced source",
        async {
            common::register(&url, &sdk.url).await?;
            for i in 0..8 {
                let (code, v) =
                    common::invoke(&url, "Counter", "add", Some(&format!("k{i}")), "\"x\"").await?;
                ensure!(code == 200, "invoke {i}: {code} {v}");
            }
            n.node
                .partitions
                .get(PartitionId(0))
                .context("engine of range 0")?
                .flush_memtable()
                .await?;
            // 1.
            n.node
                .ranges
                .split(PartitionId(0), None)
                .await
                .map_err(|e| anyhow!("split 0: {e}"))?;
            // 2.
            let stores =
                ObjectStores::single(StoreLocation::from_store(n.store.clone(), "", "memory://"))
                    .with_polls(polls);
            let probe = stores.clone();
            let mut lineage = Lineage::new(
                LineageConfig {
                    depth: 1,
                    ancestor_age: Duration::from_secs(365 * 24 * 3600),
                    every: Duration::from_millis(200),
                    retry_after: Duration::from_secs(600),
                    max_auto_split_depth: 0,
                },
                stores,
                n.node.cluster.clone(),
                n.node.load_book.clone(),
                n.node.partitions.clone(),
            );
            let wall = || cairn_store::entropy::now_ms();
            let t0 = Instant::now();
            let s = loop {
                match lineage
                    .tick(tokio::time::Instant::now(), wall())
                    .await
                    .map_err(|e| anyhow!("tick: {e}"))?
                {
                    Tick::Compacted { range } => break range,
                    other => {
                        ensure!(
                            t0.elapsed() < Duration::from_secs(60),
                            "no child has finished a full compaction yet: {other:?}"
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            };
            ensure!(
                probe
                    .range_external_depth(PartitionId(s))
                    .await?
                    .unwrap_or(0)
                    > 0,
                "S={s} no longer stands on an ancestor — nothing to detach"
            );
            // The compactor's checkpoint (1 s expiry) still pins the old manifest: wait for it to expire.
            tokio::time::sleep(Duration::from_secs(3)).await;
            // 3. The lineage round has read the range list (S still leads here) and
            // stands right before `detach_now(S)`.
            let before_detach = ctl.pause_at("lineage::before_detach");
            let tick = tokio::spawn(async move {
                lineage
                    .tick(tokio::time::Instant::now(), cairn_store::entropy::now_ms())
                    .await
            });
            before_detach.expect_stopped(TEST_DEADLINE).await;
            // 4. S's split reaches past the close mark.
            let paused = ctl.pause_at("split::cloneLeft");
            let split = {
                let ranges = n.node.ranges.clone();
                tokio::spawn(async move { ranges.split(PartitionId(s), None).await })
            };
            paused.expect_stopped(TEST_DEADLINE).await;
            // 5. Release the lineage round, recording every write until the tick ends.
            n.store.record_writes();
            before_detach.resume();
            let tick = tick.await?.map_err(|e| anyhow!("tick: {e}"))?;
            let writes = n.store.take_writes();
            paused.resume();
            split
                .await?
                .map_err(|e| anyhow!("split S={s} after the release: {e}"))?;
            println!(
                "detach: S={s}, tick = {tick:?}, writes {:?}",
                writes
                    .iter()
                    .map(|(_, p)| p.to_string())
                    .collect::<Vec<_>>()
            );
            let prefix = format!("ranges/{s}/manifest/");
            let touched: Vec<String> = writes
                .iter()
                .map(|(_, p)| p.to_string())
                .filter(|p| p.starts_with(&prefix))
                .collect();
            ensure!(
                touched.is_empty(),
                "lineage wrote the manifest of quiescing source S={s}: {touched:?}"
            );
            Ok(())
        },
    )
    .await;
    n.node.shutdown().await;
    drop(sdk);
    r
}

// ───────────── REQUEST_RESPONSE gzip does not run on the engine thread

/// The raw id of a 24-byte hex id.
fn unhex24(id: &str) -> Result<[u8; 24]> {
    let raw: Vec<u8> = (0..id.len() / 2)
        .map(|i| u8::from_str_radix(&id[2 * i..2 * i + 2], 16))
        .collect::<std::result::Result<_, _>>()?;
    raw.try_into()
        .map_err(|_| anyhow!("id is not 24 bytes: {id}"))
}

/// Sends one invocation to `service.run`, returns the hex id.
async fn send_run(url: &str, service: &str) -> Result<String> {
    let (code, v) = common::send(url, service, "run", None, "{}").await?;
    ensure!(code == 200 || code == 202, "send {service}: {code} {v}");
    v["id"]
        .as_str()
        .or_else(|| v["invocationId"].as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

/// Waits for invocation `id` to be `completed` within `bound`.
async fn completed_within(url: &str, id: &str, bound: std::time::Duration) -> Result<()> {
    within(bound, "invocation done", async {
        loop {
            let (_, st) = common::invocation(url, id).await?;
            match st["status"].as_str() {
                Some("completed") => return Ok(()),
                Some("failed") => return Err(anyhow!("invocation failed: {st}")),
                _ => tokio::time::sleep(std::time::Duration::from_millis(30)).await,
            }
        }
    })
    .await
}

/// While compressing/decompressing a `REQUEST_RESPONSE` body of range A stands at
/// `point`, turns of range B **on the same engine thread** still run.
///
/// 1. A lone node, two ranges, **one** engine thread (`engine_threads = 1`): every
///    range shares one `current_thread` runtime.
/// 2. SDK `GzBig` declares `requestCompression: gzip`, no cache; its single `ctx.run`
///    returns 8 MiB — the first turn's response decompresses to 8 MiB, the second turn's
///    body ≈ 8 MiB.
/// 3. `point` stops (a barrier, `pause_at`) the **first** thread reaching that
///    operation — `GzBig`'s 8 MiB body.
/// 4. While it stands: invocations to SDK `GzSmall` (uncompressed), resent until one
///    lands on a range other than `GzBig`'s.
///
/// Must see: every `GzSmall` invocation finishes within 10 s while the barrier is still
/// closed; with the barrier released `GzBig` finishes. Before the fix: compression and
/// decompression ran right on the engine thread, the barrier stopped the thread ⇒ every
/// range of the thread stopped ⇒ `GzSmall` did not finish.
async fn rr_gzip_does_not_stall_the_engine_thread(point: &str) -> Result<()> {
    use common::rr_sdk::{Sdk, Spec};
    let ctl = StepController::new();
    let mut cfg = test_config(NodeMode::single(2));
    cfg.engine_threads = Some(1);
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = async {
        let mut big = Spec::new("GzBig");
        big.declare = false;
        big.gzip = true;
        big.runs = 1;
        big.payload = 8 << 20;
        let big = Sdk::spawn(big).await?;
        let mut small = Spec::new("GzSmall");
        small.declare = false;
        small.runs = 1;
        let small = Sdk::spawn(small).await?;
        for sdk in [&big, &small] {
            within(TEST_DEADLINE, "register", async {
                loop {
                    if common::register(&url, &sdk.url).await.is_ok() {
                        return Ok(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            })
            .await?;
        }
        let paused = ctl.pause_at(point);
        let a = send_run(&url, "GzBig").await?;
        paused.expect_stopped(TEST_DEADLINE).await;
        let range_a = node.cluster.partition_of_id(&unhex24(&a)?);
        let mut other = false;
        for _ in 0..32 {
            // Both the send and the wait finish within the cap: sending goes through the engine too.
            let b = within(std::time::Duration::from_secs(10), "GzSmall done", async {
                let b = send_run(&url, "GzSmall").await?;
                completed_within(&url, &b, std::time::Duration::from_secs(10)).await?;
                Ok(b)
            })
            .await
            .with_context(|| format!("{point} of range A is standing"))?;
            if node.cluster.partition_of_id(&unhex24(&b)?) != range_a {
                other = true;
                break;
            }
        }
        ensure!(other, "all 32 invocations landed on GzBig's range");
        paused.resume();
        completed_within(&url, &a, TEST_DEADLINE).await
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rr_gzip_of_one_range_does_not_stall_its_engine_thread() -> Result<()> {
    rr_gzip_does_not_stall_the_engine_thread("invoker::rr_gzip").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rr_gunzip_of_one_range_does_not_stall_its_engine_thread() -> Result<()> {
    rr_gzip_does_not_stall_the_engine_thread("invoker::rr_gunzip").await
}

/// **Inbound rows throttled by L0 are not re-read on every drain pass.**
///
/// Interleaving:
/// 1. A lone node, one range. `l0::defer_inbound` = `return`: the L0 throttle defers
///    every `StartCall` (the shape of a range with L0 at the edge — once the throttle
///    defers, it defers the rest of the pass).
/// 2. The test sends [`SHED`] `StartCall` messages (service `Echo`, no object key)
///    directly to the internal port in batches of 64. Each durable batch nudges the
///    drainer; every row is deferred and stays.
/// 3. After the last batch is durable and two more drain passes (every row has been
///    read by one pass), the test counts the inbound rows the drainer reads from the
///    store (`cairnflow_inbound_rows_read_total`) and the drain passes
///    (`cairnflow_outbox_drain_passes_total`) over ≥ 4 passes — the background tick
///    nudges the drainer every 50 ms, the throttle tick every 250 ms.
/// 4. Remove the point.
///
/// Must see: rows read per pass while throttled stay below one batch (64), not growing
/// with [`SHED`]; with the point removed the throttled rows are read again and `Echo`
/// runs. Before the fix: every pass re-read from `seq` 0 — all [`SHED`] rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shed_inbound_rows_are_not_reread_every_pass() -> Result<()> {
    use {
        bytes::Bytes,
        cairn_engine::{InterPartition, StartCall},
        cairn_store::{make_invocation_id, PartitionKey},
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    /// The number of throttled `StartCall`s.
    const SHED: u32 = 10_000;
    let ctl = StepController::new();
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(test_config(NodeMode::single(1))).await?;
    let url = node.url();
    let counts = |url: String| async move {
        let read = common::metric(&url, "cairnflow_inbound_rows_read_total").await?;
        let passes = common::metric(&url, "cairnflow_outbox_drain_passes_total").await?;
        anyhow::Ok((read, passes))
    };
    let r = within(Duration::from_secs(180), "throttled rows are not re-read", async {
        common::register(&url, &sdk.url).await?;
        let shed = ctl.fail("l0::defer_inbound", "return");
        let msg = |i: u32| {
            let mut tail = [0u8; 16];
            tail[..4].copy_from_slice(&i.to_be_bytes());
            InterPartition::StartCall(StartCall {
                callee: make_invocation_id(PartitionKey(u64::from(i) << 32), tail),
                service: "Echo".into(),
                handler: "run".into(),
                object_key: String::new(),
                parameter: Bytes::from_static(b"{}"),
                reply_to: None,
                idempotency_key: None,
                scope: None,
                limit_key: None,
                tenant: None,
                sender: (make_invocation_id(PartitionKey(0), [0xEE; 16]), i),
                schedule: None,
                lineage: Default::default(),
            })
            .encode()
        };
        for start in (0..SHED).step_by(64) {
            let parts: Vec<Bytes> = (start..(start + 64).min(SHED)).map(msg).collect();
            let resp = common::post_internal(
                &node,
                "/internal/inbound/batch",
                InterPartition::encode_batch(&parts).to_vec(),
            )
            .await?;
            ensure!(resp.status().is_success(), "batch {start}: {}", resp.status());
        }
        // Every row has been read by a drain pass: a pass starting after the last batch.
        let (_, after_posts) = counts(url.clone()).await?;
        while counts(url.clone()).await?.1 < after_posts + 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (read0, passes0) = counts(url.clone()).await?;
        let (read1, passes1) = loop {
            let c = counts(url.clone()).await?;
            if c.1 >= passes0 + 4 {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let per_pass = (read1 - read0) / (passes1 - passes0);
        eprintln!(
            "{SHED} throttled rows — {} rows read over {} drain passes ({per_pass} per pass)",
            read1 - read0,
            passes1 - passes0
        );
        ensure!(
            per_pass < 64,
            "{per_pass} inbound rows read per drain pass with {SHED} throttled rows — rows that stay are re-read every pass"
        );
        drop(shed);
        within(Duration::from_secs(120), "Echo runs after the throttle is removed", async {
            while sdk.starts_of("Echo", "run").is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            anyhow::Ok(())
        })
        .await
    })
    .await;
    node.shutdown().await;
    r
}

// ───────────────────────────── VO lock rebuild in chunks

/// Range 0's `locksHeld` per `GET /scheduler`.
async fn locks_held(url: &str) -> Result<u64> {
    let (code, body) = call(Client::new().get(format!("{url}/scheduler"))).await?;
    ensure!(code == StatusCode::OK, "/scheduler: {code} {body}");
    body.as_array()
        .and_then(|rows| rows.iter().find(|r| r["partition"] == 0))
        .and_then(|r| r["locksHeld"].as_u64())
        .ok_or_else(|| anyhow!("/scheduler has no range 0: {body}"))
}

/// `/send` one call, returns the id.
async fn send_one(url: &str, service: &str, handler: &str, key: Option<&str>) -> Result<String> {
    let (code, body) = common::send(url, service, handler, key, "{}").await?;
    ensure!(
        code == 200 || code == 202,
        "{service}/{handler}: {code} {body}"
    );
    Ok(body["id"].as_str().unwrap_or_default().to_string())
}

/// Sends signal [`common::fake_sdk::HOLD_SIGNAL`] to `id`.
async fn hold_signal(url: &str, id: &str) -> Result<()> {
    let (code, body) = call(
        Client::new()
            .post(format!("{url}/invocations/{id}/signal"))
            .json(&json!({ "signal": common::fake_sdk::HOLD_SIGNAL })),
    )
    .await?;
    ensure!(code == StatusCode::OK, "signal {id}: {code} {body}");
    Ok(())
}

/// The fake SDK for the three tests below: `Obj/hold` suspends waiting for a signal
/// (holding the object's VO lock), then finishes, `Obj/quick` finishes at once,
/// `Echo/run` (no lock) finishes at once.
async fn restore_sdk() -> Result<common::fake_sdk::FakeSdk> {
    use common::fake_sdk::{FakeSdk, Step};
    FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("\"held\"")])
        .object("Obj", "quick", vec![Step::output("\"quick\"")])
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await
}

/// Ids in the order of their first `Start` reaching the SDK, only those in `ids`.
fn first_start_order(sdk: &common::fake_sdk::FakeSdk, ids: &[&String]) -> Vec<String> {
    let mut seen: Vec<(std::time::SystemTime, String)> = Vec::new();
    for s in sdk.starts() {
        let id = s.id_hex();
        if ids.contains(&&id) && !seen.iter().any(|(_, x)| x == &id) {
            seen.push((s.at, id));
        }
    }
    seen.sort();
    seen.into_iter().map(|(_, id)| id).collect()
}

/// **FL-8 — while the VO locks are being rebuilt, the range serves reads, creations,
/// cancels and status changes, but grants no permit.**
///
/// Interleaving:
/// 1. Node A alone, one range. `hold@k` (`Obj/hold`, key `k`) suspends waiting for a
///    signal — `suspended`, holding the VO lock of `Obj/k`; `q1@k` (`Obj/quick`) queues
///    behind it (`Inbox`). `hold@z` suspends, holding the lock of `Obj/z`. A shuts down.
/// 2. `actor::restore_hold` = `return`, only on B. B becomes leader on the same bucket:
///    the lock rebuild waits before its first chunk — no lock rebuilt yet.
/// 3. Cut into that window: read the status of `hold@k`; create `q2@k` (`Inbox`) and
///    `Echo/run` (no lock); cancel `hold@z` (its chunk not scanned yet); signal
///    `hold@k` — that command carries a wake-up into a turn of its own (written in the
///    `Running` commit), so it **waits at the permit gate**, sent in parallel.
/// 4. 300 ms: no new `Start` reaches the SDK, the signal has not answered,
///    `locksHeld` = 0.
/// 5. Turn the failpoint off: the rebuild runs to the end, the gate opens.
///
/// Must see: the read, the creations and the cancel are answered while the rebuild
/// waits; after 5, the signal answers, `hold@k` finishes, `q1@k` runs before `q2@k`
/// (FIFO by `seq` — `q2`, created during the rebuild, carries a `seq` above `q1`'s);
/// `q3@z` (sent after 5) finishes — the lock of the cancelled `hold@z` does not come
/// back; `Echo/run` finishes. Old version: the rebuild ran before the first command
/// (ignoring the failpoint), `Echo/run` ran at once and `locksHeld` = 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_are_served_but_nothing_is_granted_while_locks_are_rebuilt() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = restore_sdk().await?;
    let a = start_led_alone(&bucket, "restore-serve-a").await?;
    let url = a.node.url();
    common::register(&url, &sdk.url).await?;
    let hold_k = send_one(&url, "Obj", "hold", Some("k")).await?;
    let hold_z = send_one(&url, "Obj", "hold", Some("z")).await?;
    within(TEST_DEADLINE, "two holds suspended", async {
        common::wait_status(&url, &hold_k, "suspended").await?;
        common::wait_status(&url, &hold_z, "suspended").await?;
        Ok(())
    })
    .await?;
    let q1 = send_one(&url, "Obj", "quick", Some("k")).await?;
    a.node.shutdown().await;

    let hold = ctl.fail_on(&["restore-serve-b"], "actor::restore_hold", "return");
    let b = start_led_alone(&bucket, "restore-serve-b").await?;
    let url = b.node.url();
    let during = within(TEST_DEADLINE, "commands during the lock rebuild", async {
        let (code, st) = common::invocation(&url, &hold_k).await?;
        ensure!(code == 200 && st["status"] == "suspended", "{code} {st}");
        let starts = sdk.starts_len();
        let q2 = send_one(&url, "Obj", "quick", Some("k")).await?;
        let echo = send_one(&url, "Echo", "run", None).await?;
        let (code, body) = admin_cancel(&url, &hold_z).await?;
        ensure!(code == StatusCode::OK, "cancel hold@z: {code} {body}");
        let signal = tokio::spawn({
            let (url, id) = (url.clone(), hold_k.clone());
            async move { hold_signal(&url, &id).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        ensure!(
            sdk.starts_len() == starts,
            "turns reached the SDK during the lock rebuild: {} new Starts",
            sdk.starts_len() - starts
        );
        ensure!(
            !signal.is_finished(),
            "the signal (a turn of its own) answered before the permit gate opened"
        );
        let locks = locks_held(&url).await?;
        ensure!(
            locks == 0,
            "locks rebuilt while the rebuild was waiting: {locks}"
        );
        Ok((q2, echo, signal))
    })
    .await;
    drop(hold);
    let after = match during {
        Ok((q2, echo, signal)) => {
            within(TEST_DEADLINE, "all done after the rebuild", async {
                signal.await??;
                for id in [&hold_k, &q1, &q2, &echo] {
                    common::wait_status(&url, id, "completed").await?;
                }
                let q3 = send_one(&url, "Obj", "quick", Some("z")).await?;
                common::wait_status(&url, &q3, "completed").await?;
                Ok(q2)
            })
            .await
        }
        Err(e) => Err(e),
    };
    b.node.shutdown().await;
    let q2 = after?;
    let order = first_start_order(&sdk, &[&q1, &q2]);
    ensure!(
        order == vec![q1.clone(), q2.clone()],
        "q1 then q2 on Obj/k: {order:?}"
    );
    Ok(())
}

/// **FL-8 — cancelling lock holders whose chunk has or has not been scanned releases
/// exactly the right locks.**
///
/// Interleaving:
/// 1. Node A alone, one range. Four objects `k0..k3`: each with one suspended `hold`
///    (holding the VO lock) and one `quick` queued behind it. A shuts down.
/// 2. B: lock rebuild chunks of 4 index rows (`TurnLimits::restore_chunk`),
///    `actor::restore_hold` = `1*off->return`, only on B: the first chunk runs, the
///    rebuild waits before the second chunk. Eight unfinished invocations ⇒ the first
///    chunk goes through four of them: the locks of the `hold`s in the first chunk are
///    rebuilt, those of the others are not (random id order — the test prints the case
///    it met).
/// 3. Cut into that window: cancel all four `hold`s.
/// 4. Turn the failpoint off.
///
/// Must see: at 3, fewer than four locks are rebuilt (the cancels are served between two
/// chunks); after 4, all four `quick`s finish and no lock is still held — rebuilt locks
/// are released by the cancel, locks not rebuilt yet are never rebuilt for an
/// invocation already `failed`. Old version: four locks rebuilt before the first
/// command.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_holders_on_both_sides_of_a_restore_chunk_frees_every_lock() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = restore_sdk().await?;
    let a = start_led_alone(&bucket, "restore-chunk-a").await?;
    let url = a.node.url();
    common::register(&url, &sdk.url).await?;
    let keys = ["k0", "k1", "k2", "k3"];
    let mut holds = Vec::new();
    for k in keys {
        holds.push(send_one(&url, "Obj", "hold", Some(k)).await?);
    }
    within(TEST_DEADLINE, "four suspended holds", async {
        for id in &holds {
            common::wait_status(&url, id, "suspended").await?;
        }
        Ok(())
    })
    .await?;
    let mut quicks = Vec::new();
    for k in keys {
        quicks.push(send_one(&url, "Obj", "quick", Some(k)).await?);
    }
    a.node.shutdown().await;

    let hold = ctl.fail_on(&["restore-chunk-b"], "actor::restore_hold", "1*off->return");
    let b = start_led_alone_with(&bucket, "restore-chunk-b", |cfg| {
        cfg.limits.restore_chunk = 4;
    })
    .await?;
    let url = b.node.url();
    let during = within(TEST_DEADLINE, "cancel between two chunks", async {
        let locks = locks_held(&url).await?;
        eprintln!("locks rebuilt after the first chunk: {locks}/4");
        ensure!(locks < 4, "every lock rebuilt before the cancel: {locks}");
        for id in &holds {
            let (code, body) = admin_cancel(&url, id).await?;
            ensure!(code == StatusCode::OK, "cancel {id}: {code} {body}");
        }
        Ok(())
    })
    .await;
    drop(hold);
    let after = match during {
        Ok(()) => {
            within(TEST_DEADLINE, "four quicks finish", async {
                for id in &quicks {
                    common::wait_status(&url, id, "completed").await?;
                }
                let locks = locks_held(&url).await?;
                ensure!(locks == 0, "locks held after all finished: {locks}");
                Ok(())
            })
            .await
        }
        Err(e) => Err(e),
    };
    b.node.shutdown().await;
    after
}

/// **FL-8 — losing the lease mid lock rebuild: the next term rebuilds from scratch.**
///
/// Interleaving:
/// 1. Node A alone, one range. `hold@k` suspends holding the VO lock of `Obj/k`;
///    `q1@k` queues behind it. A shuts down.
/// 2. `actor::restore_hold` = `return`, only on B. B becomes leader: the rebuild waits;
///    B still answers reads, and creates `q2@k`.
/// 3. B shuts down mid-rebuild (the term's tasks are cancelled — as on losing the
///    lease).
/// 4. C becomes leader on the same bucket, no failpoint: a second rebuild, from
///    scratch, in a new term.
///
/// Must see: at 2, `locksHeld` = 0 and reads are served; on C, `q1` and `q2` do not run
/// before `hold@k` is signalled (its lock is rebuilt on C), then all three finish, `q1`
/// before `q2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_cut_by_a_lost_term_is_redone_by_the_next_term() -> Result<()> {
    use std::time::Duration;
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = restore_sdk().await?;
    let a = start_led_alone(&bucket, "restore-lost-a").await?;
    let url = a.node.url();
    common::register(&url, &sdk.url).await?;
    let hold_k = send_one(&url, "Obj", "hold", Some("k")).await?;
    within(TEST_DEADLINE, "hold suspended", async {
        common::wait_status(&url, &hold_k, "suspended").await?;
        Ok(())
    })
    .await?;
    let q1 = send_one(&url, "Obj", "quick", Some("k")).await?;
    a.node.shutdown().await;

    let hold = ctl.fail_on(&["restore-lost-b"], "actor::restore_hold", "return");
    let b = start_led_alone(&bucket, "restore-lost-b").await?;
    let url = b.node.url();
    let q2 = within(TEST_DEADLINE, "B serves mid-rebuild", async {
        let (code, st) = common::invocation(&url, &hold_k).await?;
        ensure!(code == 200 && st["status"] == "suspended", "{code} {st}");
        let locks = locks_held(&url).await?;
        ensure!(
            locks == 0,
            "locks rebuilt while the rebuild was waiting: {locks}"
        );
        send_one(&url, "Obj", "quick", Some("k")).await
    })
    .await;
    b.node.shutdown().await;
    drop(hold);
    let q2 = q2?;

    let c = start_led_alone(&bucket, "restore-lost-c").await?;
    let url = c.node.url();
    let r = within(TEST_DEADLINE, "C rebuilds the locks", async {
        while locks_held(&url).await? == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        for id in [&q1, &q2] {
            let (_, st) = common::invocation(&url, id).await?;
            ensure!(
                st["status"] != "completed",
                "{id} ran while hold@k still held the lock: {st}"
            );
        }
        hold_signal(&url, &hold_k).await?;
        for id in [&hold_k, &q1, &q2] {
            common::wait_status(&url, id, "completed").await?;
        }
        Ok(())
    })
    .await;
    c.node.shutdown().await;
    r?;
    let order = first_start_order(&sdk, &[&q1, &q2]);
    ensure!(
        order == vec![q1.clone(), q2.clone()],
        "q1 then q2: {order:?}"
    );
    Ok(())
}

// ─────────────────────────────── FL-31: a split waits for its children to restore their queues

/// A `service` key whose partition key is in the left (`left`) or right half of range 0.
fn key_in_half_of(service: &str, left: bool, prefix: &str) -> String {
    const MID: u64 = 1 << 63;
    (0..100_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| {
            let pk = cairn_store::partition_key_for_target(
                &cairn_store::TenantId::default(),
                None,
                service,
                k,
                None,
            )
            .expect("has a key")
            .0;
            (pk < MID) == left
        })
        .expect("key found")
}

/// **FL-31 — a split returns its report once the child ranges have restored their
/// queues, not merely once they have a leader.**
///
/// The probe command of `wait_led_here` (`flow`) goes ahead of the background work at
/// the start of a term (FL-31), so "the child answers a command" no longer implies "the
/// child's VO lock rebuild is done" as it did when the mutex was purely FIFO.
///
/// Interleaving:
/// 1. A lone node, one range, lock rebuild chunks of 1 row (`restore_chunk`). Two
///    suspended `hold`s in each key half — each child range has two index rows, two
///    chunks.
/// 2. `actor::restore_hold` = `return`, only on this node: each child's rebuild waits
///    before its second chunk (the parent finished its rebuild at step 1).
/// 3. Split range 0. The children become leaders and answer the probe; the permit gate
///    is still closed.
/// 4. After 500 ms, turn the failpoint off.
///
/// Must see: until 4 the split has not returned; after 4 it returns and its report has
/// the `restoreMs` of both children. Old version: the split returned at 3 with
/// `restoreMs` = null (the `each-once+nodewal` row, `split_reports_phase_timings` red
/// 2/12).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_reports_after_children_restore_their_queue() -> Result<()> {
    let ctl = StepController::new();
    let bucket = common::bucket();
    let sdk = restore_sdk().await?;
    let n = start_led_alone_with(&bucket, "split-restore", |cfg| {
        cfg.limits.restore_chunk = 1;
    })
    .await?;
    let url = n.node.url();
    let result = within(TEST_DEADLINE, "split awaits child queues", async {
        common::register(&url, &sdk.url).await?;
        let mut holds = Vec::new();
        for left in [true, false] {
            for i in 0..2 {
                let key = key_in_half_of("Obj", left, &format!("restore-{left}-{i}"));
                holds.push(send_one(&url, "Obj", "hold", Some(&key)).await?);
            }
        }
        for id in &holds {
            common::wait_status(&url, id, "suspended").await?;
        }
        let hold = ctl.fail_on(&["split-restore"], "actor::restore_hold", "return");
        let mut splitting = tokio::spawn(post_json(format!("{url}/ranges/0/split")));
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(500), &mut splitting).await;
        drop(hold);
        ensure!(early.is_err(), "split returned mid lock rebuild: {early:?}");
        let (code, view) = splitting.await??;
        ensure!(code == StatusCode::OK, "split: {code} {view}");
        let children = view["report"]["children"]
            .as_array()
            .ok_or_else(|| anyhow!("report.children missing: {view}"))?;
        ensure!(children.len() == 2, "{view}");
        for c in children {
            ensure!(
                c["restoreMs"].as_f64().is_some(),
                "a child has not finished restoring its queue: {c}"
            );
        }
        Ok(())
    })
    .await;
    n.node.shutdown().await;
    result
}

/// Enables two scheduler tripwires (`panic`): an invocation with two live permits, and a
/// VO lock released by a permit that does not hold it.
fn trip_wires(ctl: &StepController) -> [cairn_store::testkit::FailGuard; 2] {
    [
        ctl.fail("flow::second_live_permit", "panic"),
        ctl.fail("flow::lock_released_by_non_holder", "panic"),
    ]
}

/// Sends `/send` to `service/run` in scope `scope`, returns the id.
async fn send_scoped(url: &str, scope: &str, service: &str) -> Result<String> {
    let (code, body) = post_json_body(
        format!("{url}/scope/{scope}/invoke/{service}/run/send"),
        "{}",
    )
    .await?;
    ensure!(code == 200 || code == 202, "{code} {body}");
    body["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {body}"))
}

async fn post_json_body(url: String, body: &str) -> Result<(u16, Value)> {
    let r = Client::new()
        .post(url)
        .body(body.to_string())
        .send()
        .await?;
    let code = r.status().as_u16();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// **The turn of a ticket with a pre-set retry fails at the post-permit gate, after the
/// background tick has moved its row into a +24 h watchdog row.** The integration
/// version of `cairn-lifecycle/src/tests/turn.rs::broken_ticket_turn_rewrites_deferred_watch_row`.
///
/// Interleaving:
/// 1. Node A alone, rule `acme = 1`. `Work` (scope `acme`) runs its first turn and fails
///    retryably with `next_retry_delay` 2 s ⇒ a durable retry.
/// 2. `Hold` (scope `acme`) enters the SDK and stands there (gate closed) — holding the
///    only permit.
/// 3. `Work`'s retry fires: a `planned` turn passes the gate, gets no permit ⇒ **parks a
///    ticket**; the next retry (one backoff step) comes due while the ticket is parked ⇒
///    the background tick moves the row far into a watchdog row (`PARKED_DEFER_MS`, one
///    day).
/// 4. `turn::gate_turn` = `1*return` on A; open the gate for `Hold`.
/// 5. `Hold` finishes ⇒ `Work`'s ticket is granted ⇒ reading the status at the
///    post-permit gate fails.
///
/// Must see: `Work` reruns and is `completed` within a few seconds. Before the fix: the
/// wake-up duty had been acked from the start (the `planned` turn), nothing rewrote the
/// row ⇒ `Work` stayed `running` with its only retry a day away (until the next leader
/// change).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_planned_turn_failing_at_the_gate_is_rerun_soon() -> Result<()> {
    use {
        common::fake_sdk::{FakeSdk, Step},
        std::time::Duration,
    };
    let ctl = StepController::new();
    // The scheduler's tripwires (AGENTS.md §6.4): the scenario goes through a parked
    // ticket, a granted ticket and a returned permit — two points that must never be hit.
    let _wires = trip_wires(&ctl);
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .gated_service("Hold", "run", vec![Step::output("\"hold\"")])
        .service(
            "Work",
            "run",
            vec![Step::FailBelowDelay(1, 2_000), Step::output("\"done\"")],
        )
        .spawn()
        .await?;
    let a = start_led_alone(&bucket, "b2-gate-a").await?;
    let url = a.node.url();
    common::register(&url, &sdk.url).await?;
    let r = Client::new()
        .post(format!("{url}/rules"))
        .json(&json!({ "pattern": "acme", "concurrency": 1 }))
        .send()
        .await?;
    ensure!(r.status().is_success(), "set rule: {}", r.status());
    let out = within(Duration::from_secs(30), "deferred row", async {
        let work = send_scoped(&url, "acme", "Work").await?;
        while sdk.starts_count_of("Work", "run") < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let hold = send_scoped(&url, "acme", "Hold").await?;
        while sdk.starts_count_of("Hold", "run") < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The retry (2 s) fires, the ticket parks; the next retry (~200 ms) comes due while it is parked.
        use std::sync::atomic::Ordering;
        while cairn_server::metrics::flow::PARKED.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        ensure!(
            sdk.starts_count_of("Work", "run") == 1,
            "Work's ticket must still be parked"
        );
        Ok((work, hold))
    })
    .await;
    let (work, hold) = match out {
        Ok(x) => x,
        Err(e) => {
            a.node.shutdown().await;
            return Err(e);
        }
    };
    let _gate = ctl.fail_on(&["b2-gate-a"], "turn::gate_turn", "1*return");
    sdk.gate().release(1);
    let done = within(
        Duration::from_secs(10),
        "Work reruns after the gate error",
        async {
            common::wait_status(&url, &hold, "completed").await?;
            common::wait_status(&url, &work, "completed").await?;
            Ok(())
        },
    )
    .await;
    let starts = sdk.starts_count_of("Work", "run");
    a.node.shutdown().await;
    done?;
    ensure!(
        starts == 2,
        "Work: the failed turn + one rerun, got {starts}"
    );
    Ok(())
}

/// **A turn stopped at preflight comes back through `wake_lifted` when the suspension is
/// lifted.** Since background turns of a suspended tenant park tickets, the batched pull
/// of retry rows on lifting was no longer covered by any integration test — except for
/// the retry rows of turns that already had a permit.
///
/// Interleaving:
/// 1. As in [`suspended_tenant_turn_granted_before_suspension_never_calls_the_sdk`],
///    but the suspended tenant reschedule tick is the product's (15 minutes).
/// 2. A turn that already has a permit stands right before its request; `suspended`;
///    release ⇒ preflight sees the tenant suspended again ⇒ a retry row at
///    `now + 15 minutes` (the core's `Reply::Throttled`).
/// 3. `active`.
///
/// Must see: the invocation finishes within a few seconds — the next background tick
/// pulls back the retry rows of the tenant just lifted (`take_lifted` →
/// `turn::wake_lifted`), without waiting 15 minutes. Proof that the test goes through
/// that path: with the `wake_lifted` call removed (locally) the test goes red.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsuspend_pulls_the_h1_row_through_wake_lifted() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let ctl = StepController::new();
    let _wires = trip_wires(&ctl);
    let bucket = common::bucket();
    let node = h1_node_with(&bucket, "h1-lift", std::time::Duration::from_secs(900)).await?;
    let url = node.node.url();
    let sdk = FakeSdk::builder()
        .service(
            "Job",
            "run",
            vec![Step::Run("1".into()), Step::output("\"ok\"")],
        )
        .spawn()
        .await?;
    let r = async {
        let (_, key) = h1_key(&url, "acme").await?;
        let paused = ctl.pause_at("turn::sdk::before_call");
        let id = h1_register_and_send(&url, &key, &sdk.url, "Job/run").await?;
        paused
            .expect_stopped(std::time::Duration::from_secs(10))
            .await;
        h1_set_state(&url, "acme", "suspended").await?;
        paused.resume();
        h1_hold_still(
            &url,
            &key,
            &sdk,
            ("Job", "run"),
            &id,
            0,
            std::time::Duration::from_secs(2),
        )
        .await?;
        h1_set_state(&url, "acme", "active").await?;
        within(
            std::time::Duration::from_secs(10),
            "the preflight-stopped turn comes back through wake_lifted",
            async {
                loop {
                    let st = h1_status(&url, &key, &id).await?;
                    if st["status"] == "completed" {
                        return Ok(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            },
        )
        .await
    }
    .await;
    node.node.shutdown().await;
    r
}

/// **Moving the revision pointer to a deployment being removed.**
///
/// 1. A lone node. `Job` registered twice: X then Y — catalog `Job → Y`, X is no longer
///    the current revision, no invocation pinned to it.
/// 2. `DELETE /deployments/X` stops at `ingress::delete_deployment::before_remove`: the
///    pins counted (0), the catalog CASed ("X is nobody's revision"), object X not
///    deleted yet.
/// 3. In that window `PUT /services/Job/revision {deployment: X}` — the in-RAM registry
///    still has X, the catalog CAS wins ⇒ `Job → X`.
/// 4. Release the `DELETE`: object X is deleted.
///
/// Before the fix: (3) `200`, (4) `204` ⇒ the catalog points at a lost deployment;
/// `GET /services` no longer has `Job` (the service vanishes until someone moves the
/// pointer by hand).
///
/// Must see: (3) is refused (`409`) or (4) does not delete; in the end `Job` is still in
/// `GET /services` with a deployment still in `GET /deployments`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revision_moved_onto_a_deployment_being_removed_does_not_dangle() -> Result<()> {
    let ctl = StepController::new();
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Job", "run", vec![common::fake_sdk::Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "two-phase deployment removal", async {
        let x = common::register(&url, &sdk.url).await?["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let y = common::register(&url, &sdk.url).await?["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        ensure!(x != y, "two deployments");
        let paused = ctl.pause_at("ingress::delete_deployment::before_remove");
        let deleting = tokio::spawn(call(Client::new().delete(format!("{url}/deployments/{x}"))));
        paused.expect_stopped(std::time::Duration::from_secs(10)).await;
        let (moved, v) = call(
            Client::new()
                .put(format!("{url}/services/Job/revision"))
                .json(&json!({ "deployment": x })),
        )
        .await?;
        paused.resume();
        let (deleted, d) = deleting.await??;
        ensure!(
            matches!(moved.as_u16(), 200 | 409),
            "PUT revision: {moved} {v}"
        );
        ensure!(
            matches!(deleted.as_u16(), 204 | 409),
            "DELETE: {deleted} {d}"
        );
        let (_, services) = call(Client::new().get(format!("{url}/services"))).await?;
        let (_, deployments) = call(Client::new().get(format!("{url}/deployments"))).await?;
        let ids: Vec<&str> = deployments
            .as_array()
            .map(|a| a.iter().filter_map(|d| d["id"].as_str()).collect())
            .unwrap_or_default();
        let job = services
            .as_array()
            .and_then(|a| a.iter().find(|s| s["name"] == "Job"))
            .cloned();
        ensure!(
            job.as_ref()
                .and_then(|j| j["deploymentId"].as_str())
                .is_some_and(|id| ids.contains(&id)),
            "Job points at a removed deployment (PUT {moved}, DELETE {deleted}): services {services}, deployments {ids:?}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
