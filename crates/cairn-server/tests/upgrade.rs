// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Upgrading the binary without wiping the bucket.**
//!
//! - A binary that meets an object with a newer header than it understands
//!   **stops loudly** — does not start, does not overwrite — instead of running
//!   on with half a configuration.
//! - The `cluster-version` object (absent ≡ `version=0`, a format 9 bucket needs
//!   no change); a node writes `knows=` into its session record; a node that
//!   meets a `version` greater than it knows does not start.
//!
//! The die-mid-step tests that use failpoints live in `tests/failpoints.rs`.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{ObjectStores, StoreLocation},
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStoreExt, PutPayload},
    std::{sync::Arc, time::Duration},
};

fn single_on(bucket: &Arc<InMemory>) -> NodeConfig {
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg
}

async fn put(bucket: &Arc<InMemory>, path: &str, body: &str) -> Result<()> {
    bucket
        .put(&Path::from(path), PutPayload::from(body.to_string()))
        .await?;
    Ok(())
}

async fn get(bucket: &Arc<InMemory>, path: &str) -> Result<String> {
    let raw = bucket.get(&Path::from(path)).await?.bytes().await?;
    Ok(String::from_utf8(raw.to_vec())?)
}

/// **Newer header.** A strictly read object that the node reads at startup —
/// `range-windows`, `tenants`, `auth/keys` — with a newer header ⇒ the node
/// does not start, and the error names that header and says to upgrade the
/// binary. (`auth/keys`: this binary reads `/1` and `/2`, so the newer header
/// is `/3`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_refuses_to_start_on_newer_object_headers() -> Result<()> {
    for (path, header) in [
        ("range-windows", "cairnflow-range-windows/2"),
        ("tenants", "cairnflow-tenants/2"),
        ("auth/keys", "cairnflow-keys/3"),
    ] {
        let bucket = common::bucket();
        put(&bucket, path, &format!("{header}\n")).await?;
        let msg = match node::start(single_on(&bucket)).await {
            Ok(n) => {
                n.shutdown().await;
                return Err(anyhow!("{path} carries {header} yet the node started"));
            }
            Err(e) => e.to_string(),
        };
        ensure!(
            msg.contains(header) && msg.contains("upgrade this binary"),
            "{path}: the error must name {header:?} and say to upgrade the binary — got: {msg}"
        );
        ensure!(
            get(&bucket, path).await? == format!("{header}\n"),
            "{path} was overwritten"
        );
    }
    Ok(())
}

/// **Newer header.** A newer rule book (`cairnflow-rules/3`): `POST /rules`
/// refuses and does **not overwrite** — writing it back in the old layout would
/// silently drop the caps this binary does not understand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rule_write_refuses_newer_rule_book() -> Result<()> {
    let bucket = common::bucket();
    let node = node::start(single_on(&bucket))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let newer = "cairnflow-rules/3\nversion=3\nrule\tacme\t5\t0\t1\tquota=10\n";
    put(&bucket, "rules", newer).await?;
    let r = async {
        let resp = reqwest::Client::new()
            .post(format!("{}/rules", node.url()))
            .json(&serde_json::json!({ "pattern": "acme", "concurrency": 7 }))
            .send()
            .await?;
        let code = resp.status();
        let body = resp.text().await?;
        ensure!(
            !code.is_success(),
            "rule write onto a newer rule book: {code} {body}"
        );
        // A `500` body carries only a log lookup code; the sentence "newer
        // header — upgrade the binary" is in the node's log under that code.
        ensure!(
            code == reqwest::StatusCode::INTERNAL_SERVER_ERROR && body.contains("code"),
            "the error must be a 500 with a log lookup code: {code} {body}"
        );
        ensure!(
            get(&bucket, "rules").await? == newer,
            "the newer rule book was overwritten"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

fn cluster_on(bucket: &Arc<InMemory>, node_id: &str) -> NodeConfig {
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(
        stores,
        NodeMode {
            partitions: 2,
            lease_ttl: Duration::from_secs(1),
            node_id: Some(node_id.into()),
        },
    );
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.node_wal.window = Duration::from_millis(5);
    cfg
}

/// Every object under `prefix`, by path.
async fn objects_under(bucket: &Arc<InMemory>, prefix: &str) -> Result<Vec<String>> {
    use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
    let listed: Vec<_> = bucket.list(Some(&Path::from(prefix))).try_collect().await?;
    Ok(listed.into_iter().map(|m| m.location.to_string()).collect())
}

/// **Cluster version.** `cluster-version` carries a `version` greater than the
/// binary knows ⇒ the node does not start, both as a single process and as a
/// cluster; the object is left untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_refuses_cluster_version_it_does_not_know() -> Result<()> {
    let body = "cairnflow-cluster-version/1\nversion=1000\nmin_binary=1000\n";
    for cluster in [false, true] {
        let bucket = common::bucket();
        put(&bucket, "cluster-version", body).await?;
        let cfg = if cluster {
            cluster_on(&bucket, "node-a")
        } else {
            single_on(&bucket)
        };
        let msg = match node::start(cfg).await {
            Ok(n) => {
                n.shutdown().await;
                return Err(anyhow!(
                    "cluster-version=1000 yet the node (cluster={cluster}) started"
                ));
            }
            Err(e) => e.to_string(),
        };
        ensure!(
            msg.contains("cluster-version") && msg.contains("1000") && msg.contains("upgrade this binary"),
            "cluster={cluster}: the error must name cluster-version, the number 1000 and say to upgrade the binary — got: {msg}"
        );
        ensure!(
            get(&bucket, "cluster-version").await? == body,
            "cluster-version was overwritten"
        );
        ensure!(
            objects_under(&bucket, "ranges").await?.is_empty(),
            "cluster={cluster}: the node opened a Db before checking cluster-version"
        );
    }
    Ok(())
}

/// **Cluster version.** The session record carries `knows=<the last migration
/// step the binary knows>` (read leniently — old binaries ignore the line). The
/// real ledger has one step (`rules/2`): a new bucket goes up to `version=1`
/// on its very first startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_record_carries_knows() -> Result<()> {
    let bucket = common::bucket();
    let node = node::start(cluster_on(&bucket, "node-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let sessions = objects_under(&bucket, "sessions/node-a").await?;
        ensure!(sessions.len() == 1, "one session: {sessions:?}");
        let text = get(&bucket, &sessions[0]).await?;
        let knows = format!("knows={}", cairn_store::migrate::KNOWS);
        ensure!(cairn_store::migrate::KNOWS >= 1, "ledger lacks rules/2");
        ensure!(
            text.lines().any(|l| l == knows),
            "the session record lacks {knows}:\n{text}"
        );
        let cv = get(&bucket, "cluster-version").await?;
        ensure!(
            cv.lines()
                .any(|l| l == format!("version={}", cairn_store::migrate::VERSION)),
            "{cv}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// **`GET /admin/cluster-version`**: the cluster version, the binary's ledger,
/// and the `knows=` of every live session — what the operator reads before and
/// after each step of a rolling upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_version_endpoint_reports_version_ledger_and_nodes() -> Result<()> {
    let bucket = common::bucket();
    let node = node::start(cluster_on(&bucket, "node-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let v: serde_json::Value = reqwest::get(format!("{}/admin/cluster-version", node.url()))
            .await?
            .json()
            .await?;
        let knows = cairn_store::migrate::KNOWS;
        // `version` is the last data step; the gate steps at the tail of the
        // ledger (`lease/quiet`) do not run at startup. A new cluster has the
        // two call gates on from the start, as well as the durable-stream gate,
        // the schedules gate, the `/2` key-registry gate and the tenant-state
        // gate, so `requiresKnows` is the last gate step.
        let version = cairn_store::migrate::VERSION;
        let gate = cairn_store::migrate::KNOWS;
        ensure!(
            v["version"] == version
                && v["requiresKnows"] == gate
                && v["callPrefix"] == true
                && v["callPlacement"] == true
                && v["streams"] == true
                && v["schedules"] == true
                && v["keyRoles"] == true
                && v["tenantState"] == true
                && v["upgrading"].is_null(),
            "{v}"
        );
        ensure!(v["binary"]["knows"] == knows, "{v}");
        ensure!(
            v["binary"]["ledger"][0]["name"] == "rules/2"
                && v["binary"]["ledger"][0]["tier"] == "cluster",
            "{v}"
        );
        let nodes = v["nodes"].as_array().ok_or_else(|| anyhow!("{v}"))?;
        ensure!(
            nodes.len() == 1 && nodes[0]["node"] == "node-a" && nodes[0]["knows"] == knows,
            "{v}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

// ───────────────────────────────── cluster-tier runner at startup

/// The cluster-tier step of the test ledger: writes `seen/c1` = whether
/// `cluster.conf` existed when the step ran — the runner must come **before**
/// `ClusterConfig::open`.
fn c1(
    _: u32,
    ctx: cairn_store::migrate::ClusterCtx,
) -> futures::future::BoxFuture<'static, cairn_store::Result<()>> {
    Box::pin(async move {
        let had = ctx.store.head(&Path::from("cluster.conf")).await.is_ok();
        ctx.store
            .put(
                &Path::from("seen/c1"),
                PutPayload::from(if had { "yes" } else { "not-yet" }),
            )
            .await
            .map_err(|e| cairn_store::StoreError::Io(e.to_string()))?;
        Ok(())
    })
}

/// The fake step [`c1`] leaves `seen/c1` behind: an object of the test ledger,
/// not of the binary — having no line in `OBJECT_POLICIES` is correct.
fn allow_seen(bucket: &common::Bucket) {
    bucket.allow(
        "unlisted_object",
        "`seen/c1` is an object of the fake migration step in the test ledger, not of the binary",
    );
}

const CLUSTER_LEDGER: &[cairn_store::migrate::Migration] = &[cairn_store::migrate::Migration {
    to: 1,
    name: "c1",
    run: cairn_store::migrate::Run::Cluster(c1),
}];

/// **Cluster-tier runner.** A node starts on a format 9 bucket with a ledger
/// holding one cluster-tier step: the step runs before `cluster.conf` is
/// created, `cluster-version` goes to 1 with the lock released, the session
/// declares `knows=1` — both as a single process and as a cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_runs_cluster_steps_before_opening_cluster_conf() -> Result<()> {
    for cluster in [false, true] {
        let bucket = common::bucket();
        allow_seen(&bucket);
        let mut cfg = if cluster {
            cluster_on(&bucket, "node-a")
        } else {
            single_on(&bucket)
        };
        cfg.migrations = CLUSTER_LEDGER;
        let node = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
        let r = async {
            ensure!(
                get(&bucket, "seen/c1").await.ok().as_deref() == Some("not-yet"),
                "cluster={cluster}: the step did not run, or ran after cluster.conf"
            );
            let cv = get(&bucket, "cluster-version").await?;
            ensure!(cv.lines().any(|l| l == "version=1"), "cl={cluster}:\n{cv}");
            ensure!(
                !cv.contains("upgrading="),
                "cluster={cluster}: the lock was not released:\n{cv}"
            );
            if cluster {
                let s = objects_under(&bucket, "sessions/node-a").await?;
                ensure!(
                    get(&bucket, &s[0]).await?.lines().any(|l| l == "knows=1"),
                    "the session does not declare knows=1"
                );
            }
            Ok(())
        }
        .await;
        node.shutdown().await;
        r?;
    }
    Ok(())
}

// ───────────────────────────────── range-tier runner at range open

/// How many times the range-tier step `r1` of **each** test ran — one counter
/// per test (the tests of this file run in parallel).
static R_FRESH: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static R_SPLIT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn counting<'a>(
    counter: &'static std::sync::atomic::AtomicU32,
    w: &'a mut cairn_store::PartitionWriter,
) -> futures::future::LocalBoxFuture<'a, cairn_store::Result<()>> {
    Box::pin(async move {
        // Touch the Db through the range owner's writer, like a real step.
        let _ = w.get(b"\xfftest-r1").await?;
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
}

fn r_fresh(
    _: u32,
    w: &mut cairn_store::PartitionWriter,
) -> futures::future::LocalBoxFuture<'_, cairn_store::Result<()>> {
    counting(&R_FRESH, w)
}
fn r_split(
    _: u32,
    w: &mut cairn_store::PartitionWriter,
) -> futures::future::LocalBoxFuture<'_, cairn_store::Result<()>> {
    counting(&R_SPLIT, w)
}

fn range_ledger(
    step: cairn_store::migrate::RangeStep,
) -> &'static [cairn_store::migrate::Migration] {
    Box::leak(Box::new([
        cairn_store::migrate::Migration {
            to: 1,
            name: "c1",
            run: cairn_store::migrate::Run::Cluster(c1),
        },
        cairn_store::migrate::Migration {
            to: 2,
            name: "r2",
            run: cairn_store::migrate::Run::Range(step),
        },
    ]))
}

/// `data=` on the owner record of range `range`, if any.
async fn owner_data(bucket: &Arc<InMemory>, range: u32) -> Result<Option<u32>> {
    let text = match get(bucket, &format!("owners/range-{range}")).await {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    Ok(text
        .lines()
        .find_map(|l| l.strip_prefix("data="))
        .and_then(|v| v.parse().ok()))
}

async fn wait_owner_data(bucket: &Arc<InMemory>, ranges: &[u32], want: u32) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let mut all = true;
        for r in ranges {
            all &= owner_data(bucket, *r).await? == Some(want);
        }
        if all {
            return Ok(());
        }
        if t0.elapsed() > Duration::from_secs(20) {
            let mut got = Vec::new();
            for r in ranges {
                got.push((*r, owner_data(bucket, *r).await?));
            }
            return Err(anyhow!("range not at data={want} after 20 s: {got:?}"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **Range-tier runner.** Each range runs its range-tier step exactly once at
/// open, after which the owner record carries `data=2`; reopening (a node
/// restart) does not run it again. Both a cluster and a node without
/// `--cluster` (which is a one-node cluster — the same path writing `data=`
/// through the keeper).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ranges_run_range_steps_once_and_mark_owner_data() -> Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    let ledger = range_ledger(r_fresh);
    let bucket = common::bucket();
    allow_seen(&bucket);
    let mut runs = 0;
    for (i, cluster) in [true, true, false].into_iter().enumerate() {
        let mut cfg = if cluster {
            cluster_on(&bucket, "node-a")
        } else {
            single_on(&bucket)
        };
        if !cluster {
            // A node without `--cluster` reads the bucket's two-range table.
            cfg.mode = NodeMode::single(2);
        }
        cfg.migrations = ledger;
        let node = common::start_led(cfg).await?;
        let r = async {
            wait_owner_data(&bucket, &[0, 1], 2).await?;
            if i == 0 {
                runs = R_FRESH.load(SeqCst);
                ensure!(runs == 2, "first time: each range runs r2 once, got {runs}");
            } else {
                ensure!(
                    R_FRESH.load(SeqCst) == runs,
                    "open number {}: r2 ran again",
                    i + 1
                );
            }
            Ok(())
        }
        .await;
        node.shutdown().await;
        r?;
    }
    // A node without `--cluster` on a new bucket: the claim creates an owner
    // record of its own session, and the range-tier step writes `data=` onto
    // it (formerly a released record was created just to carry `data=`).
    let before = R_FRESH.load(SeqCst);
    let bucket = common::bucket();
    allow_seen(&bucket);
    let mut cfg = single_on(&bucket);
    cfg.mode = NodeMode::single(2);
    cfg.migrations = ledger;
    let node = common::start_led(cfg).await?;
    let r = async {
        wait_owner_data(&bucket, &[0, 1], 2).await?;
        ensure!(
            R_FRESH.load(SeqCst) == before + 2,
            "node without --cluster: each range runs r2 once"
        );
        let rec = get(&bucket, "owners/range-0").await?;
        ensure!(
            !rec.lines().any(|l| l == "released=1")
                && rec
                    .lines()
                    .any(|l| l.starts_with("session=") && l.contains(&node.node_id)),
            "the owner record must belong to the session of the node holding the range:\n{rec}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// **Range-tier runner.** Split: both children get `data = min(parent)` when
/// they take the range (the new owner record already carries `data=2`), so the
/// range-tier step does not run again on the child Dbs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_children_inherit_range_data() -> Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    let bucket = common::bucket();
    allow_seen(&bucket);
    let mut cfg = cluster_on(&bucket, "node-a");
    cfg.mode = NodeMode {
        partitions: 1,
        lease_ttl: Duration::from_secs(1),
        node_id: Some("node-a".into()),
    };
    cfg.migrations = range_ledger(r_split);
    let node = common::start_led(cfg).await?;
    let r = async {
        wait_owner_data(&bucket, &[0], 2).await?;
        ensure!(R_SPLIT.load(SeqCst) == 1, "range 0 runs r2 once");
        let resp = reqwest::Client::new()
            .post(format!("{}/ranges/0/split", node.url()))
            .send()
            .await?;
        ensure!(
            resp.status().is_success(),
            "split: {} {}",
            resp.status(),
            resp.text().await?
        );
        wait_owner_data(&bucket, &[1, 2], 2).await?;
        ensure!(
            R_SPLIT.load(SeqCst) == 1,
            "a child range ran r2 again: {}",
            R_SPLIT.load(SeqCst)
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────── upgrade gate — split/merge and GC stop

/// `cluster-version` holding the lock of a migration runner (`ghost/1`).
const UPGRADING: &str = "cairnflow-cluster-version/1\nversion=0\nupgrading=0->1@ghost/1\n";

async fn post_status(url: String) -> Result<(u16, String)> {
    let r = reqwest::Client::new().post(url).send().await?;
    Ok((r.status().as_u16(), r.text().await?))
}

/// **Upgrade gate.** While `upgrading` is present, split and merge refuse
/// (`503`, with the reason) and the range table does not change — no intent is
/// written. Once the lock is released they go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_and_merge_wait_for_upgrade() -> Result<()> {
    let bucket = common::bucket();
    let mut cfg = cluster_on(&bucket, "node-a");
    cfg.mode = NodeMode {
        partitions: 1,
        lease_ttl: Duration::from_secs(1),
        node_id: Some("node-a".into()),
    };
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = async {
        put(&bucket, "cluster-version", UPGRADING).await?;
        let before = get(&bucket, "cluster.conf").await?;
        let (code, body) = post_status(format!("{url}/ranges/0/split")).await?;
        ensure!(
            code == 503 && body.contains("upgrading"),
            "split during an upgrade: {code} {body}"
        );
        ensure!(
            get(&bucket, "cluster.conf").await? == before,
            "split wrote the table during an upgrade"
        );

        bucket.delete(&Path::from("cluster-version")).await?;
        let (code, body) = post_status(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split after upgrade: {code} {body}");
        let ranges: Vec<u32> = node.cluster.partitions().iter().map(|p| p.0).collect();
        ensure!(ranges.len() == 2, "{ranges:?}");

        put(&bucket, "cluster-version", UPGRADING).await?;
        let before = get(&bucket, "cluster.conf").await?;
        let merge = format!("{url}/ranges/merge?left={}&right={}", ranges[0], ranges[1]);
        let (code, body) = post_status(merge.clone()).await?;
        ensure!(
            code == 503 && body.contains("upgrading"),
            "merge during an upgrade: {code} {body}"
        );
        ensure!(
            get(&bucket, "cluster.conf").await? == before,
            "merge wrote the table during an upgrade"
        );
        bucket.delete(&Path::from("cluster-version")).await?;
        let (code, body) = post_status(merge).await?;
        ensure!(code == 200, "merge after upgrade: {code} {body}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// **Upgrade gate.** While `upgrading` is present, the retired-Db GC pass skips
/// its round: a retired Db that nobody references (and that would otherwise be
/// deleted) stays. Once the lock is released the next round deletes it.
///
/// GC honours `min_age` by `retired_ms=`, so the fake tombstone here carries a
/// `retired_ms` old enough for the control branch to delete it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_round_skipped_while_upgrading() -> Result<()> {
    let bucket = common::bucket();
    let mut cfg = single_on(&bucket);
    let stores = cfg.stores.clone();
    cfg.mode = NodeMode::single(1);
    let node = common::start_led(cfg).await?;
    let r = async {
        // The Db of a retired range (id 9) that nobody references, and its tombstone.
        let db = stores.open_db(9).await?;
        db.put(b"k", b"v").await?;
        db.close().await?;
        put(
            &bucket,
            "owners/range-9",
            // `retired_ms=1`: the tombstone is old enough for `min_age`, so the
            // control branch really deletes.
            "cairnflow-owner/1\nterm=1\nsession=x/1\nreleased=1\nretired=1\nwal_kind=slatedb\nretired_ms=1\n",
        )
        .await?;
        let gone = cairn_store::PartitionId(9);

        put(&bucket, "cluster-version", UPGRADING).await?;
        let deleted = node::gc_retired_dbs_once(&stores, &node.cluster).await.map_err(|e| anyhow!("{e}"))?;
        ensure!(deleted == 0, "GC deleted {deleted} Db during an upgrade");
        ensure!(stores.range_db_exists(gone).await?, "retired Db deleted during an upgrade");

        bucket.delete(&Path::from("cluster-version")).await?;
        let deleted = node::gc_retired_dbs_once(&stores, &node.cluster).await.map_err(|e| anyhow!("{e}"))?;
        ensure!(deleted == 1 && !stores.range_db_exists(gone).await?, "control: GC must delete the retired Db ({deleted})");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// **The node's real GC pass goes through the upgrade gate.** The node's pass
/// calls `gc::run_once` directly (`node/setup.rs`, the D-d `spawn`), not through
/// `node::gc_retired_dbs_once`; a gate that lived only in the wrapper would be
/// skipped by production. Order: an old enough retired range that nobody
/// references → `cluster-version` carries `upgrading=` → one `run_once` round
/// → the Db must remain and the plan must say `upgrading`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_loop_path_skipped_while_upgrading() -> Result<()> {
    use cairn_server::gc::{run_once, BlockedBy, GcConfig};
    let bucket = common::bucket();
    let mut cfg = single_on(&bucket);
    let stores = cfg.stores.clone();
    cfg.mode = NodeMode::single(1);
    let node = common::start_led(cfg).await?;
    let r = async {
        let db = stores.open_db(9).await?;
        db.put(b"k", b"v").await?;
        db.close().await?;
        put(
            &bucket,
            "owners/range-9",
            "cairnflow-owner/1\nterm=1\nsession=x/1\nreleased=1\nretired=1\nwal_kind=slatedb\nretired_ms=1\n",
        )
        .await?;
        let gone = cairn_store::PartitionId(9);
        put(&bucket, "cluster-version", UPGRADING).await?;
        let round = run_once(&stores, &node.cluster, &GcConfig::default()).await?;
        ensure!(round.deleted.is_empty(), "the node's GC pass deleted {:?} during an upgrade", round.deleted);
        ensure!(stores.range_db_exists(gone).await?, "retired Db deleted during an upgrade");
        ensure!(
            round.plan.blocked.iter().any(|b| b.range == 9 && b.by == BlockedBy::Upgrading),
            "the plan does not say range 9 is blocked by upgrading: {:?}",
            round.plan.blocked
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// The ledger of a binary from **before** the `lease/quiet` gate step: knows
/// every data step, not the gate step.
static BEFORE_QUIET_LEASE: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::QUIET_LEASE as usize - 1)
    .0;

/// The ledger of a binary from **before** the `calls/prefix` gate step: knows
/// `lease/quiet`, cannot read v6 outbox rows.
static BEFORE_CALL_PREFIX: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::CALL_PREFIX as usize - 1)
    .0;

/// The ledger of a binary from **before** the `calls/placement` gate step: knows
/// `calls/prefix`, ignores the `placement` of a call command.
static BEFORE_CALL_PLACEMENT: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::CALL_PLACEMENT as usize - 1)
    .0;

/// The ledger of a binary from **before** the `streams/table` gate step: knows
/// the two call gates, ignores table code `0x18` and leaks its rows on purge.
static BEFORE_STREAMS_TABLE: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::STREAMS_TABLE as usize - 1)
    .0;

/// The ledger of a binary from **before** the `schedules/table` gate step:
/// knows durable streams, not table `0x1A` — as leader of a range with
/// schedules it does not fire them.
static BEFORE_SCHEDULES_TABLE: &[cairn_store::migrate::Migration] =
    cairn_store::migrate::MIGRATIONS
        .split_at(cairn_store::migrate::SCHEDULES_TABLE as usize - 1)
        .0;

/// **Quiet-lease gate** `old_binary_cannot_start_when_quiet_lease_enabled`:
///
/// 1. an old binary (a ledger without the gate step) and this binary both start
///    on a bucket without the gate — a gate step does not raise `version` at
///    startup;
/// 2. `cairn-ctl cluster enable-quiet-lease --apply` when only sessions of this
///    binary are left ⇒ the old binary **does not start** (`NewerFormat`,
///    `min_binary`), this binary starts;
/// 3. the downgrade runbook (gate off, every session back to `direct`, lower
///    `min_binary`) ⇒ the old binary starts again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_binary_cannot_start_when_quiet_lease_enabled() -> Result<()> {
    use cairn_server::ctl::{Ctl, RepairOptions};
    let bucket = common::bucket();
    let old = |node: &str| {
        let mut cfg = cluster_on(&bucket, node);
        cfg.migrations = BEFORE_QUIET_LEASE;
        cfg
    };
    ensure!(
        cairn_store::migrate::latest(BEFORE_QUIET_LEASE) == cairn_store::migrate::QUIET_LEASE - 1
    );
    let n = node::start(old("old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    n.shutdown().await;
    let n = node::start(cluster_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(std::sync::Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-quiet-lease --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let r = async {
        ctl.enable_quiet_lease(&opts).await?;
        match node::start(old("old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                return Err(anyhow!("the old binary started after the gate was enabled"));
            }
            Err(e) => ensure!(
                e.to_string().contains("min_binary"),
                "the error must name min_binary: {e}"
            ),
        }
        let again = node::start(cluster_on(&bucket, "new-b"))
            .await
            .map_err(|e| anyhow!("the new binary did not start after enabling the gate: {e}"))?;
        again.shutdown().await;
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;
    ctl.disable_quiet_lease(&opts).await?;
    ctl.lower_min_binary(&opts).await?;
    let o = node::start(old("old-c"))
        .await
        .map_err(|e| anyhow!("the old binary did not start after the downgrade runbook: {e}"))?;
    o.shutdown().await;
    Ok(())
}

/// **The `calls/prefix` gate** (grouping call trees by pk prefix), shaped like
/// `lease/quiet` but **one-way**:
///
/// 1. an old binary (ledger stopping at `lease/quiet`) forms a cluster on an
///    empty bucket ⇒ the gate is off (its ledger does not know that step), and
///    this binary starts on that cluster;
/// 2. a live session of the old binary remains ⇒ `enable-call-prefix` refuses
///    and writes nothing;
/// 3. the old binary stops ⇒ `--apply` sets `min_binary=5`, `call_prefix=on`
///    and `call_placement=on` (one command enables every call gate); the old
///    binary **no longer starts**, this binary starts;
/// 4. `lower-min-binary` does not go below 5 while the gate is on — the old
///    binary still does not start (v6 outbox rows may already be in the Db);
/// 5. a **new cluster** formed by this binary has the gate on from the start:
///    `call_prefix=on`, `call_placement=on`, `min_binary=5`, and the old binary
///    does not start on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_prefix_gate_is_one_way_and_on_for_new_clusters() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, CALL_PLACEMENT, CALL_PREFIX};
    ensure!(migrate::latest(BEFORE_CALL_PREFIX) == CALL_PREFIX - 1);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = cluster_on(bucket, node);
        cfg.migrations = BEFORE_CALL_PREFIX;
        // The real binary of that step did not set up the quiet-lease gate (the
        // default since 2026-10-23): a truncated ledger running on this code must
        // turn it off explicitly.
        cfg.quiet_lease = cairn_store::quiet_lease::Bootstrap::Off;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let ctl_of = |bucket: &Arc<InMemory>| {
        Ctl::new(ObjectStores::single(StoreLocation::from_store(
            bucket.clone(),
            "",
            "memory://",
        )))
        .with_clock(Arc::new(common::JumpClock::default()))
    };
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-call-prefix --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    ensure!(
        !cv_of(&bucket).await?.call_prefix,
        "the old binary enabled a gate it does not know"
    );
    let n = node::start(cluster_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let ctl = ctl_of(&bucket);
    let r = async {
        match ctl.enable_call_prefix(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("an old binary is left yet no refusal: {other:?}"),
        }
        let cv = cv_of(&bucket).await?;
        ensure!(
            !cv.call_prefix && cv.min_binary < CALL_PREFIX,
            "refused yet wrote: {cv:?}"
        );
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_call_prefix(&opts).await?;
        ensure!(rep.applied && rep.audit.is_some(), "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.call_prefix_on() && cv.call_placement_on() && cv.min_binary == CALL_PLACEMENT,
            "{cv:?}"
        );
        let again = ctl.enable_call_prefix(&opts).await?;
        ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
        ensure!(ctl.cluster_status(opts.now_ms).await?.call_prefix);
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the old binary started after the prefix gate was enabled");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let again = node::start(cluster_on(&bucket, "new-b"))
            .await
            .map_err(|e| anyhow!("the new binary did not start after enabling the gate: {e}"))?;
        again.shutdown().await;
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    // 4.
    let rep = ctl.lower_min_binary(&opts).await?;
    ensure!(
        rep.warnings.iter().any(|w| w.contains("call_placement")),
        "{rep:?}"
    );
    let cv = cv_of(&bucket).await?;
    ensure!(
        cv.call_prefix_on() && cv.call_placement_on() && cv.min_binary == CALL_PLACEMENT,
        "{cv:?}"
    );
    if let Ok(o) = node::start(old(&bucket, "old-c")).await {
        o.shutdown().await;
        anyhow::bail!("old binary started after lower-min-binary with the prefix gate on");
    }

    // 5.
    let fresh = common::bucket();
    let n = node::start(cluster_on(&fresh, "new-fresh"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&fresh).await?;
        ensure!(
            cv.call_prefix_on() && cv.call_placement_on() && cv.min_binary == migrate::KNOWS,
            "a new cluster must have both call gates on from the start — along with every later gate \
             (durable streams, schedules, key registry /2, tenant state…), so min_binary is the last gate step: {cv:?}"
        );
        match node::start(old(&fresh, "old-fresh")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the old binary started on a new cluster with the prefix gate on");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// **The `calls/placement` gate** blocks the step-4 binary (the last one
/// before that gate step), which ignores the `placement` of a call command and replays
/// `spread` calls by the grouping rule (second callee):
///
/// 1. the step-4 binary forms a cluster on an empty bucket ⇒ only the
///    prefix gate (`min_binary=4`, no `call_placement`); this binary starts on
///    that cluster and does not enable the placement gate by itself;
/// 2. a live session of the step-4 binary remains ⇒ `enable-call-prefix`
///    refuses and writes nothing;
/// 3. the step-4 binary stops ⇒ `--apply` sets `min_binary=5`,
///    `call_placement=on`; the step-4 binary **no longer starts**, this
///    binary starts;
/// 4. a new cluster formed by this binary: the step-4 binary does not
///    start on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_placement_gate_stops_the_prefix_only_binary() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, CALL_PLACEMENT, CALL_PREFIX};
    ensure!(migrate::latest(BEFORE_CALL_PLACEMENT) == CALL_PREFIX);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = cluster_on(bucket, node);
        cfg.migrations = BEFORE_CALL_PLACEMENT;
        // The real binary of that step did not set up the quiet-lease gate (the
        // default since 2026-10-23): a truncated ledger running on this code must
        // turn it off explicitly.
        cfg.quiet_lease = cairn_store::quiet_lease::Bootstrap::Off;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-call-prefix --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let cv = cv_of(&bucket).await?;
    if !(cv.call_prefix_on() && !cv.call_placement && cv.min_binary == CALL_PREFIX) {
        o.shutdown().await;
        anyhow::bail!("the step-4 binary formed a new cluster: prefix gate only — {cv:?}");
    }
    let n = node::start(cluster_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&bucket).await?;
        ensure!(
            !cv.call_placement_on(),
            "the new binary enabled the placement gate by itself: {cv:?}"
        );
        match ctl.enable_call_prefix(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("a step-4 binary is left yet no refusal: {other:?}"),
        }
        let after = cv_of(&bucket).await?;
        ensure!(after == cv, "refused yet wrote: {after:?}");
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_call_prefix(&opts).await?;
        ensure!(rep.applied, "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.call_prefix_on() && cv.call_placement_on() && cv.min_binary == CALL_PLACEMENT,
            "{cv:?}"
        );
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-4 binary started after the placement gate was enabled");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let again = node::start(cluster_on(&bucket, "new-b"))
            .await
            .map_err(|e| anyhow!("the new binary did not start after enabling the gate: {e}"))?;
        again.shutdown().await;
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    // 4.
    let fresh = common::bucket();
    let n = node::start(cluster_on(&fresh, "new-fresh"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        match node::start(old(&fresh, "old-fresh")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-4 binary started on a new cluster");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

// ──────── quiet-lease gate of a new bucket

/// The bucket's `control/quiet-lease` gate (absent ⇒ `None`).
async fn gate_on(bucket: &Arc<InMemory>) -> Result<Option<cairn_store::quiet_lease::QuietLease>> {
    let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    cairn_store::quiet_lease::read(&store, &Path::from(""))
        .await?
        .map(|(q, _)| q.ok_or_else(|| anyhow!("the gate is unreadable")))
        .transpose()
}

/// Wait for `node` to enter mode `want` (until `deadline`).
async fn until_mode(
    node: &node::RunningNode,
    want: cairn_store::SessionMode,
    deadline: Duration,
) -> Result<()> {
    let t0 = std::time::Instant::now();
    while node.session.mode() != want {
        ensure!(
            t0.elapsed() < deadline,
            "the node did not enter {want:?} after {deadline:?} (currently {:?})",
            node.session.mode()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// **A new bucket built by this binary has the quiet-lease gate on, with
/// delegation.**
///
/// 1. The first node starts on an empty bucket, with default flags.
/// 2. Read `cluster-version` and `control/quiet-lease`.
///
/// Must see: `min_binary ≥ QUIET_LEASE`; the gate has `state=on`, `knows=K`,
/// `delegation=on`, a `delegation_unproven=` mark listing all five missing
/// items and `by=bootstrap`; a lone node enters `mode=solo` (delegation does
/// not apply to a single node — it is its own coordinator) without waiting
/// for `quiet_lease_poll`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_bucket_boots_with_the_delegation_gate_and_a_lone_node_goes_solo() -> Result<()> {
    use cairn_store::quiet_lease::{BOOTSTRAP_BY, EVIDENCE_ITEMS};
    let bucket = common::bucket();
    let n = node::start(cluster_on(&bucket, "a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cairn_store::migrate::ClusterVersion::decode(
            get(&bucket, "cluster-version").await?.as_bytes(),
        )?;
        ensure!(cv.min_binary >= cairn_store::migrate::QUIET_LEASE, "{cv:?}");
        let q = gate_on(&bucket)
            .await?
            .ok_or_else(|| anyhow!("the gate is missing"))?;
        ensure!(q.on && q.delegation, "{q:?}");
        ensure!(q.knows == cairn_store::migrate::QUIET_LEASE, "{q:?}");
        let u = q
            .delegation_unproven
            .as_ref()
            .ok_or_else(|| anyhow!("missing delegation_unproven mark: {q:?}"))?;
        ensure!(
            u.by == BOOTSTRAP_BY && u.missing == EVIDENCE_ITEMS.to_vec(),
            "{u:?}"
        );
        // Registry generation 0: readers get `304` instead of a full round.
        let gen = get(&bucket, cairn_server::registry_gen::OBJECT).await?;
        ensure!(
            gen.starts_with(&format!("{}\ngen=0 ", cairn_server::registry_gen::HEADER)),
            "{gen:?}"
        );
        until_mode(&n, cairn_store::SessionMode::Solo, Duration::from_secs(15)).await
    }
    .await;
    n.shutdown().await;
    r
}

/// **`--quiet-lease off` builds a `direct` cluster; on an existing bucket the
/// flag is ignored.**
///
/// 1. An `off` node builds an empty bucket ⇒ no gate; the node stays `direct`
///    for several TTLs.
/// 2. The node stops; a default node (`delegation`) restarts on that bucket ⇒
///    the bucket already has a cluster, no gate is written.
/// 3. A second bucket built with `solo` ⇒ the gate is on without delegation; a
///    default node restarts on it ⇒ the gate stays `solo`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiet_lease_flag_only_acts_on_a_fresh_bucket() -> Result<()> {
    use cairn_store::quiet_lease::Bootstrap;
    let bucket = common::bucket();
    let mut cfg = cluster_on(&bucket, "a");
    cfg.quiet_lease = Bootstrap::Off;
    let n = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
    let r = async {
        ensure!(gate_on(&bucket).await?.is_none(), "off yet a gate exists");
        tokio::time::sleep(Duration::from_secs(3)).await;
        ensure!(
            n.session.mode() == cairn_store::SessionMode::Direct,
            "off yet the node is in {:?}",
            n.session.mode()
        );
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;
    let n = node::start(cluster_on(&bucket, "b"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        ensure!(
            gate_on(&bucket).await?.is_none(),
            "the bucket already existed yet default flags wrote the gate"
        );
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    let bucket = common::bucket();
    let mut cfg = cluster_on(&bucket, "a");
    cfg.quiet_lease = Bootstrap::Solo;
    let n = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
    n.shutdown().await;
    let n = node::start(cluster_on(&bucket, "b"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let q = gate_on(&bucket)
            .await?
            .ok_or_else(|| anyhow!("solo ⇒ a gate exists"))?;
        ensure!(q.on && !q.delegation, "the solo gate was changed: {q:?}");
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// **Two nodes building one empty bucket at the same time reach exactly one
/// result.**
///
/// 1. alpha (`solo`) and beta (`delegation`) start at the same time on an
///    empty bucket.
/// 2. One node wins the `upgrading` lock on `cluster-version` (create CAS),
///    the other waits on the lock of a live session.
///
/// Must see: both start; exactly one gate, carrying the winner's choice
/// (`by=bootstrap@<node>/<inc>` matches `delegation`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_bootstrapping_one_empty_bucket_agree_on_one_gate() -> Result<()> {
    use cairn_store::quiet_lease::Bootstrap;
    let bucket = common::bucket();
    let mut a = cluster_on(&bucket, "alpha");
    a.quiet_lease = Bootstrap::Solo;
    let b = cluster_on(&bucket, "beta");
    let (a, b) = tokio::join!(node::start(a), node::start(b));
    let a = a.map_err(|e| anyhow!("alpha: {e}"))?;
    let b = b.map_err(|e| anyhow!("beta: {e}"))?;
    let r = async {
        let q = gate_on(&bucket)
            .await?
            .ok_or_else(|| anyhow!("the gate is missing"))?;
        ensure!(q.on, "{q:?}");
        let writer = q.by.rsplit('@').next().unwrap_or_default();
        let want = if q.delegation { "beta/" } else { "alpha/" };
        ensure!(
            writer.starts_with(want),
            "the gate mixes the two nodes' choices: {q:?}"
        );
        Ok(())
    }
    .await;
    a.shutdown().await;
    b.shutdown().await;
    r
}

/// **`--data-dir` (`file://`), one node on an empty directory lands in
/// `solo`**, like `memory://` — the gate set up at bootstrap is written through
/// the CAS emulated under the directory lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_dir_node_on_an_empty_directory_goes_solo() -> Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "cairn-quiet-fs-{}-{}",
        std::process::id(),
        cairn_store::entropy::now_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let loc = StoreLocation::parse(&format!("file://{}", dir.display()))?;
    let mut cfg = NodeConfig::new(
        ObjectStores::single(loc),
        NodeMode {
            partitions: 1,
            lease_ttl: Duration::from_secs(1),
            node_id: Some("dir".into()),
        },
    );
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    let n = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let raw = std::fs::read(dir.join("control/quiet-lease"))?;
        ensure!(
            cairn_store::quiet_lease::delegation_on(&raw),
            "{}",
            String::from_utf8_lossy(&raw)
        );
        until_mode(&n, cairn_store::SessionMode::Solo, Duration::from_secs(15)).await
    }
    .await;
    n.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    r
}

/// **The `streams/table` gate** blocks the step-5 binary:
///
/// 1. the step-5 binary forms a new cluster: the two call gates, **no**
///    durable-stream gate, `min_binary=5`; this binary joins the same cluster
///    without enabling the gate by itself;
/// 2. `cairn-ctl cluster enable-streams --apply` while the step-5 binary still
///    runs ⇒ refused, nothing written;
/// 3. the step-5 binary stops ⇒ `--apply` sets `min_binary=6`, `streams=on`,
///    the call gates stay as they are; the step-5 binary no longer starts,
///    this binary does;
/// 4. a new cluster formed by this binary: the gate is on from the start, the
///    step-5 binary does not start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_gate_stops_the_placement_binary() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, CALL_PLACEMENT, STREAMS_TABLE};
    ensure!(migrate::latest(BEFORE_STREAMS_TABLE) == CALL_PLACEMENT);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = cluster_on(bucket, node);
        cfg.migrations = BEFORE_STREAMS_TABLE;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-streams --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let cv = cv_of(&bucket).await?;
    if !(cv.call_placement_on() && !cv.streams && cv.min_binary == CALL_PLACEMENT) {
        o.shutdown().await;
        anyhow::bail!("the step-5 binary formed a new cluster: two call gates, no durable streams yet — {cv:?}");
    }
    let n = node::start(cluster_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&bucket).await?;
        ensure!(!cv.streams_on(), "new binary turned on stream gate: {cv:?}");
        match ctl.enable_streams(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("a step-5 binary is left yet no refusal: {other:?}"),
        }
        let after = cv_of(&bucket).await?;
        ensure!(after == cv, "refused yet wrote: {after:?}");
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_streams(&opts).await?;
        ensure!(rep.applied, "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.streams_on()
                && cv.call_prefix_on()
                && cv.call_placement_on()
                && cv.min_binary == STREAMS_TABLE,
            "{cv:?}"
        );
        let again = ctl.enable_streams(&opts).await?;
        ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("step-5 binary started after the stream gate");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let back = node::start(cluster_on(&bucket, "new-b"))
            .await
            .map_err(|e| anyhow!("the new binary did not start after enabling the gate: {e}"))?;
        back.shutdown().await;
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    // 4.
    let fresh = common::bucket();
    let n = node::start(cluster_on(&fresh, "new-fresh"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&fresh).await?;
        ensure!(cv.streams_on() && cv.min_binary == migrate::KNOWS, "{cv:?}");
        match node::start(old(&fresh, "old-fresh")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-5 binary started on a new cluster");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// **The `schedules/table` gate (CG-13)** blocks the step-6 binary:
///
/// 1. the step-6 binary forms a new cluster: three gates (calls, durable
///    streams), **no** schedules gate, `min_binary=6`; this binary joins the
///    same cluster without enabling the gate by itself;
/// 2. `cairn-ctl cluster enable-schedules --apply` while the step-6 binary
///    still runs ⇒ refused, nothing written;
/// 3. the step-6 binary stops ⇒ `--apply` sets `min_binary=7`, `schedules=on`,
///    the other gates stay as they are; the step-6 binary (which fires no
///    schedule when it is leader) no longer starts, this binary does;
/// 4. a new cluster formed by this binary: the gate is on from the start, the
///    step-6 binary does not start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedules_gate_stops_the_streams_binary() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, SCHEDULES_TABLE, STREAMS_TABLE};
    ensure!(migrate::latest(BEFORE_SCHEDULES_TABLE) == STREAMS_TABLE);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = cluster_on(bucket, node);
        cfg.migrations = BEFORE_SCHEDULES_TABLE;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-schedules --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let cv = cv_of(&bucket).await?;
    if !(cv.streams_on() && !cv.schedules && cv.min_binary == STREAMS_TABLE) {
        o.shutdown().await;
        anyhow::bail!("step-6 binary, new cluster: streams, no schedules — {cv:?}");
    }
    let n = node::start(cluster_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&bucket).await?;
        ensure!(!cv.schedules_on(), "new binary set schedules gate: {cv:?}");
        match ctl.enable_schedules(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("a step-6 binary is left yet no refusal: {other:?}"),
        }
        let after = cv_of(&bucket).await?;
        ensure!(after == cv, "refused yet wrote: {after:?}");
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_schedules(&opts).await?;
        ensure!(rep.applied, "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.schedules_on()
                && cv.streams_on()
                && cv.call_prefix_on()
                && cv.call_placement_on()
                && cv.min_binary == SCHEDULES_TABLE,
            "{cv:?}"
        );
        let again = ctl.enable_schedules(&opts).await?;
        ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-6 binary started after the schedules gate was enabled");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let back = node::start(cluster_on(&bucket, "new-b"))
            .await
            .map_err(|e| anyhow!("the new binary did not start after enabling the gate: {e}"))?;
        back.shutdown().await;
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    // 4.
    let fresh = common::bucket();
    let n = node::start(cluster_on(&fresh, "new-fresh"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&fresh).await?;
        ensure!(
            cv.schedules_on() && cv.min_binary == migrate::KNOWS,
            "{cv:?}"
        );
        match node::start(old(&fresh, "old-fresh")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-6 binary started on a new cluster");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// The ledger of a binary from **before** the `auth/keys-v2` gate step: it
/// decodes `cairnflow-keys/1` strictly — a `/2` registry makes it stop
/// confirming freshness and return `503` for every stored key.
static BEFORE_KEY_ROLES: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::KEY_ROLES as usize - 1)
    .0;

const GATE_BOOT: &str = "cf_test_upgrade_key_roles_bootstrap_0123";

/// Like [`cluster_on`], with auth enabled by [`GATE_BOOT`].
fn authed_on(bucket: &Arc<InMemory>, node_id: &str) -> NodeConfig {
    let mut cfg = cluster_on(bucket, node_id);
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(GATE_BOOT.into()),
        cluster_secret: Some("cf_test_cluster_secret_upgrade_key_roles".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.deploy_refresh = Duration::from_millis(100);
    cfg
}

/// `POST /admin/keys {tenant, role}` with `key`: (code, JSON).
async fn post_key(
    url: &str,
    key: &str,
    tenant: &str,
    role: &str,
) -> Result<(u16, serde_json::Value)> {
    let r = reqwest::Client::new()
        .post(format!("{url}/admin/keys"))
        .bearer_auth(key)
        .json(&serde_json::json!({ "tenant": tenant, "role": role }))
        .send()
        .await?;
    let code = r.status().as_u16();
    Ok((code, r.json().await.unwrap_or_default()))
}

/// **The `auth/keys-v2` gate** blocks the step-7 binary (this binary reads `/1`
/// and `/2`, writes `/1` until the gate is on, no `Run::Cluster`):
///
/// 1. the step-7 binary forms a cluster: no key gate, `min_binary=7`; this
///    binary joins the same cluster and does not enable the gate by itself.
/// 2. On this binary: creating a `provision`/`view` key ⇒ `409`
///    (`key_roles_disabled`); creating an `admin` key works, and `auth/keys`
///    still carries `cairnflow-keys/1` — the step-7 binary can still read it;
///    `cairn-ctl keys create --role provision` refuses; `cluster
///    enable-key-roles --apply` while the step-7 binary still runs ⇒ refused,
///    nothing written.
/// 3. the step-7 binary stops ⇒ `--apply` sets `min_binary=8`, `key_roles=on`,
///    the other gates stay as they are; the step-7 binary no longer starts;
///    creating a `provision` key through the API works and `auth/keys` moves to
///    `cairnflow-keys/2`, the old `admin` key remains; `cairn-ctl keys create
///    --role provision` yields a secret the node accepts as role `provision`.
/// 4. a new cluster formed by this binary: the gate is on from the start, the
///    step-7 binary does not start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_roles_gate_stops_the_schedules_binary() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, KEY_ROLES, SCHEDULES_TABLE};
    ensure!(migrate::latest(BEFORE_KEY_ROLES) == SCHEDULES_TABLE);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = authed_on(bucket, node);
        cfg.migrations = BEFORE_KEY_ROLES;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let header_of = |bucket: &Arc<InMemory>| {
        let bucket = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                get(&bucket, "auth/keys")
                    .await?
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_string(),
            )
        }
    };
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-key-roles --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let cv = cv_of(&bucket).await?;
    if !(cv.schedules_on() && !cv.key_roles_on() && cv.min_binary == SCHEDULES_TABLE) {
        o.shutdown().await;
        anyhow::bail!("step-7 binary, new cluster: schedules, no key gate — {cv:?}");
    }
    let n = node::start(authed_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let url = n.url();
    let r = async {
        let cv = cv_of(&bucket).await?;
        ensure!(!cv.key_roles_on(), "new binary set the key gate: {cv:?}");
        for role in ["provision", "view"] {
            let (code, v) = post_key(&url, GATE_BOOT, "default", role).await?;
            ensure!(
                code == 409 && v["code"] == "key_roles_disabled",
                "{role} before the gate: {code} {v}"
            );
        }
        let (code, v) = post_key(&url, GATE_BOOT, "acme", "admin").await?;
        ensure!(code == 200, "admin key before the gate: {code} {v}");
        ensure!(
            header_of(&bucket).await? == "cairnflow-keys/1",
            "before the gate the writer must keep /1"
        );
        match ctl.create_key("default", "provision", &opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("enable-key-roles"), "{msg}"),
            other => anyhow::bail!("cairn-ctl created a provision key before the gate: {other:?}"),
        }
        match ctl.enable_key_roles(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("a step-7 binary is left yet no refusal: {other:?}"),
        }
        let after = cv_of(&bucket).await?;
        ensure!(after == cv, "refused yet wrote: {after:?}");
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_key_roles(&opts).await?;
        ensure!(rep.applied, "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.key_roles_on()
                && cv.schedules_on()
                && cv.streams_on()
                && cv.call_prefix_on()
                && cv.call_placement_on()
                && cv.min_binary == KEY_ROLES,
            "{cv:?}"
        );
        let again = ctl.enable_key_roles(&opts).await?;
        ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-7 binary started after the key gate was enabled");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let (code, v) = post_key(&url, GATE_BOOT, "default", "provision").await?;
        ensure!(code == 200, "provision key after the gate: {code} {v}");
        let keys = get(&bucket, "auth/keys").await?;
        ensure!(
            keys.starts_with("cairnflow-keys/2\n")
                && keys.contains(" provision ")
                && keys.contains(" admin "),
            "{keys}"
        );
        let made = ctl.create_key("default", "provision", &opts).await?;
        ensure!(
            made.role == "provision" && made.secret.starts_with("cf_"),
            "{made:?}"
        );
        ensure!(
            !made.report.after.contains(&made.secret) && !made.report.before.contains(&made.secret),
            "the report carries the secret: {:?}",
            made.report
        );
        let t0 = std::time::Instant::now();
        loop {
            let r = reqwest::Client::new()
                .get(format!("{url}/auth/whoami"))
                .bearer_auth(&made.secret)
                .send()
                .await?;
            if r.status() == 200 {
                let me: serde_json::Value = r.json().await?;
                ensure!(me["role"] == "provision", "{me}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the node does not accept the key cairn-ctl created: {}",
                r.status()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    // 4.
    let fresh = common::bucket();
    let n = node::start(authed_on(&fresh, "new-fresh"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&fresh).await?;
        ensure!(
            cv.key_roles_on() && cv.min_binary == migrate::KNOWS,
            "{cv:?}"
        );
        match node::start(old(&fresh, "old-fresh")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-7 binary started on a new cluster");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// **CG-13** `schedules_gate_off_refuses_put`: a cluster formed by the step-6
/// binary (`schedules/table` gate off), with this binary as leader.
///
/// 1. `PUT /schedules/x` ⇒ `409`, `code: schedules_disabled` — even before the
///    deployment check (empty registry): a binary that does not know the table
///    may still be leader.
/// 2. `GET /schedules` still answers (a harmless read), with an empty list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedules_gate_off_refuses_put() -> Result<()> {
    let bucket = common::bucket();
    let mut old = cluster_on(&bucket, "old-a");
    old.migrations = BEFORE_SCHEDULES_TABLE;
    let o = node::start(old).await.map_err(|e| anyhow!("{e}"))?;
    o.shutdown().await;
    let n = node::start(cluster_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let url = n.url();
    let r = common::within(Duration::from_secs(60), "schedules gate off", async {
        // Wait for the node to lead every range (503 before that).
        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "everyMs": 60_000,
            "target": { "service": "Report", "handler": "build" }
        });
        let (code, v) = loop {
            let r = client
                .put(format!("{url}/schedules/x"))
                .json(&body)
                .send()
                .await?;
            let code = r.status().as_u16();
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            if code != 503 {
                break (code, v);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        ensure!(
            code == 409 && v["code"] == "schedules_disabled",
            "{code} {v}"
        );
        let l: serde_json::Value = client
            .get(format!("{url}/schedules"))
            .send()
            .await?
            .json()
            .await?;
        ensure!(l["schedules"].as_array().is_some_and(Vec::is_empty), "{l}");
        Ok(())
    })
    .await;
    n.shutdown().await;
    r
}

/// The ledger of a binary from **before** the `tenants/state` gate step: it
/// ignores `state=` in the `tenants` book — a suspended tenant still runs on
/// that node.
static BEFORE_TENANT_STATE: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::TENANT_STATE as usize - 1)
    .0;

/// `PUT /admin/tenants/{tenant}/state {state}` with `key`: (code, JSON).
async fn put_state(
    url: &str,
    key: &str,
    tenant: &str,
    state: &str,
) -> Result<(u16, serde_json::Value)> {
    let r = reqwest::Client::new()
        .put(format!("{url}/admin/tenants/{tenant}/state"))
        .bearer_auth(key)
        .json(&serde_json::json!({ "state": state }))
        .send()
        .await?;
    let code = r.status().as_u16();
    Ok((code, r.json().await.unwrap_or_default()))
}

/// **The `tenants/state` gate** blocks the step-8 binary (rolling upgrade: an
/// old binary ignores `state=`, so the new field may only be promised after the
/// gate; `Run::Gate` runs nothing at startup — the test is a mixed-binary
/// cluster, not a failpoint in the middle of the gate):
///
/// 1. the step-8 binary forms a cluster: key-registry gate on, state gate off,
///    `min_binary=8`; this binary joins the same cluster and does not enable
///    the gate by itself.
/// 2. On this binary: `PUT /admin/tenants/acme/state` ⇒ `409
///    tenant_state_disabled`, the `tenants` book carries no `state=`; `cluster
///    enable-tenant-state --apply` while the step-8 binary still runs ⇒
///    refused, nothing written.
/// 3. the step-8 binary stops ⇒ `--apply` sets `min_binary=9`,
///    `tenant_state=on`, the other gates stay as they are; running it again
///    reports it is already on; the step-8 binary no longer starts;
///    `PUT …/state suspended` ⇒ `204`, the book carries `state=suspended`.
/// 4. a new cluster formed by this binary: the gate is on from the start, the
///    step-8 binary does not start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_state_gate_stops_the_key_roles_binary() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, KEY_ROLES, TENANT_STATE};
    ensure!(migrate::latest(BEFORE_TENANT_STATE) == KEY_ROLES);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = authed_on(bucket, node);
        cfg.migrations = BEFORE_TENANT_STATE;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-tenant-state --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let cv = cv_of(&bucket).await?;
    if !(cv.key_roles_on() && !cv.tenant_state_on() && cv.min_binary == KEY_ROLES) {
        o.shutdown().await;
        anyhow::bail!("step-8 binary, new cluster: key gate, no state gate — {cv:?}");
    }
    let n = node::start(authed_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let url = n.url();
    let r = async {
        let cv = cv_of(&bucket).await?;
        ensure!(!cv.tenant_state_on(), "new binary enabled the gate: {cv:?}");
        let (code, v) = put_state(&url, GATE_BOOT, "acme", "suspended").await?;
        ensure!(
            code == 409 && v["code"] == "tenant_state_disabled",
            "before the gate: {code} {v}"
        );
        let book = get(&bucket, "tenants").await.unwrap_or_default();
        ensure!(!book.contains("state="), "refused yet wrote: {book}");
        match ctl.enable_tenant_state(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("a step-8 binary is left yet no refusal: {other:?}"),
        }
        let after = cv_of(&bucket).await?;
        ensure!(after == cv, "refused yet wrote: {after:?}");
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_tenant_state(&opts).await?;
        ensure!(rep.applied, "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.tenant_state_on()
                && cv.key_roles_on()
                && cv.schedules_on()
                && cv.streams_on()
                && cv.call_prefix_on()
                && cv.call_placement_on()
                && cv.min_binary == TENANT_STATE,
            "{cv:?}"
        );
        let again = ctl.enable_tenant_state(&opts).await?;
        ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-8 binary started after the state gate was enabled");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let (code, v) = put_state(&url, GATE_BOOT, "acme", "suspended").await?;
        ensure!(code == 204, "after the gate: {code} {v}");
        let book = get(&bucket, "tenants").await?;
        ensure!(book.contains("acme state=suspended"), "{book}");
        Ok(())
    }
    .await;
    n.shutdown().await;
    r?;

    // 4.
    let fresh = common::bucket();
    let n = node::start(authed_on(&fresh, "new-fresh"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let r = async {
        let cv = cv_of(&fresh).await?;
        ensure!(
            cv.tenant_state_on() && cv.min_binary == migrate::KNOWS,
            "{cv:?}"
        );
        match node::start(old(&fresh, "old-fresh")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-8 binary started on a new cluster");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// The ledger of a binary from **before** the `tenants/purge` gate step: it
/// reads `state=deleting` as `suspended` — it neither blocks reads nor deletes
/// rows.
static BEFORE_TENANT_PURGE: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::TENANT_PURGE as usize - 1)
    .0;

/// **The `tenants/purge` gate** blocks the step-10 binary (which would read
/// `state=deleting` as `suspended`, so the purge could not be promised while
/// it runs):
///
/// 1. the step-10 binary forms a cluster: audit gate on, purge gate off,
///    `min_binary=10`; this binary joins the same cluster and does not enable
///    the gate by itself.
/// 2. On this binary: `PUT /admin/tenants/acme/state deleting` ⇒ `409
///    tenant_purge_disabled`, the book carries no `deleting`; the old states
///    are still accepted (`suspended` ⇒ `204`); `cluster enable-tenant-purge
///    --apply` while the step-10 binary still runs ⇒ refused, nothing written.
/// 3. the step-10 binary stops ⇒ `--apply` sets `min_binary=11`,
///    `tenant_purge=on`, the other gates stay as they are; the step-10 binary
///    no longer starts; `PUT …/state deleting` ⇒ `204`, the book carries
///    `state=deleting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_purge_gate_stops_the_audit_binary() -> Result<()> {
    use cairn_server::ctl::{Ctl, CtlError, RepairOptions};
    use cairn_store::migrate::{self, AUDIT_TABLE, TENANT_PURGE};
    ensure!(migrate::latest(BEFORE_TENANT_PURGE) == AUDIT_TABLE);
    let bucket = common::bucket();
    let old = |bucket: &Arc<InMemory>, node: &str| {
        let mut cfg = authed_on(bucket, node);
        cfg.migrations = BEFORE_TENANT_PURGE;
        cfg
    };
    let cv_of = |bucket: &Arc<InMemory>| {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        async move {
            Ok::<_, anyhow::Error>(
                migrate::read_cluster_version(&store, &Path::from(""))
                    .await?
                    .map(|(cv, _)| cv)
                    .unwrap_or_default(),
            )
        }
    };
    let ctl = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()));
    let opts = RepairOptions {
        apply: true,
        force: false,
        who: "test@upgrade".into(),
        command: "cairn-ctl cluster enable-tenant-purge --apply".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };

    // 1–2.
    let o = node::start(old(&bucket, "old-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let cv = cv_of(&bucket).await?;
    if !(cv.audit_on() && !cv.tenant_purge_on() && cv.min_binary == AUDIT_TABLE) {
        o.shutdown().await;
        anyhow::bail!("step-10 binary, new cluster: audit, no purge gate — {cv:?}");
    }
    let n = node::start(authed_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let url = n.url();
    let r = async {
        let cv = cv_of(&bucket).await?;
        ensure!(!cv.tenant_purge_on(), "new binary enabled the gate: {cv:?}");
        let (code, v) = put_state(&url, GATE_BOOT, "acme", "deleting").await?;
        ensure!(
            code == 409 && v["code"] == "tenant_purge_disabled",
            "before the gate: {code} {v}"
        );
        let book = get(&bucket, "tenants").await.unwrap_or_default();
        ensure!(!book.contains("deleting"), "refused yet wrote: {book}");
        let (code, v) = put_state(&url, GATE_BOOT, "acme", "suspended").await?;
        ensure!(code == 204, "old state before the gate: {code} {v}");
        match ctl.enable_tenant_purge(&opts).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old-a"), "{msg}"),
            other => anyhow::bail!("a step-10 binary is left yet no refusal: {other:?}"),
        }
        let after = cv_of(&bucket).await?;
        ensure!(after == cv, "refused yet wrote: {after:?}");
        Ok(())
    }
    .await;
    o.shutdown().await;
    if let Err(e) = r {
        n.shutdown().await;
        return Err(e);
    }

    // 3.
    let r = async {
        let rep = ctl.enable_tenant_purge(&opts).await?;
        ensure!(rep.applied, "{rep:?}");
        let cv = cv_of(&bucket).await?;
        ensure!(
            cv.tenant_purge_on()
                && cv.audit_on()
                && cv.tenant_state_on()
                && cv.key_roles_on()
                && cv.min_binary == TENANT_PURGE,
            "{cv:?}"
        );
        match node::start(old(&bucket, "old-b")).await {
            Ok(o) => {
                o.shutdown().await;
                anyhow::bail!("the step-10 binary started after the purge gate was enabled");
            }
            Err(e) => ensure!(e.to_string().contains("min_binary"), "{e}"),
        }
        let (code, v) = put_state(&url, GATE_BOOT, "acme", "deleting").await?;
        ensure!(code == 204, "after the gate: {code} {v}");
        let book = get(&bucket, "tenants").await?;
        ensure!(book.contains("acme state=deleting"), "{book}");
        Ok(())
    }
    .await;
    n.shutdown().await;
    r
}

/// Rewrites the `tenant` line of the `tenants` book **the way a binary from
/// before the `refused-ms=`/`lifted-ms=` marks** writes it: it does not know
/// those marks, so it reads them as unknown flags, keeps them verbatim and
/// writes them **at the end of the line** (after every flag it knows); `edit`
/// is what it does to the remaining flags (change `state=`, `slow-ms=`). It
/// never updates the marks.
async fn old_binary_rewrite(
    bucket: &Arc<InMemory>,
    tenant: &str,
    edit: impl Fn(&mut Vec<String>),
) -> Result<String> {
    let raw = get(bucket, "tenants").await?;
    let mut out = Vec::new();
    for line in raw.lines() {
        let mut f: Vec<String> = line.split(' ').map(str::to_string).collect();
        if f.first().map(String::as_str) == Some(tenant) {
            let (marks, mut rest): (Vec<String>, Vec<String>) = f
                .drain(..)
                .partition(|x| x.starts_with("refused-ms=") || x.starts_with("lifted-ms="));
            edit(&mut rest);
            rest.extend(marks);
            out.push(rest.join(" "));
        } else {
            out.push(line.to_string());
        }
    }
    let body = out.join("\n") + "\n";
    put(bucket, "tenants", &body).await?;
    Ok(body)
}

/// **Rolling upgrade with an old binary also writing the `tenants` book**: a
/// binary from before the marks keeps `refused-ms=`/`lifted-ms=` verbatim
/// (unknown flags, at the end of the line) and does not update them when the
/// state changes. The product's default postpone interval (15 minutes).
///
/// 1. The new binary (`new-a`) suspends and then resumes `acme` through the
///    API ⇒ the book has both marks.
/// 2. The "old binary" rewrites the line: moves the marks to the end, adds
///    `slow-ms=50` and an unknown future flag, **suspends** `acme` (marks
///    unchanged). `new-a` re-reads the book; the `Retry`/`Nap` turns are
///    refused a permit — they park their tickets in the scheduler's queue (the
///    RAM of `new-a`) instead of being postponed 15 minutes ahead, as they were
///    before tickets were parked.
/// 3. `new-a` stops; the "old binary" **resumes** (drops `state=`, the marks
///    are still those of step 1 — within the postpone interval) and then moves
///    the marks back one day (as if the book had carried old marks for a long
///    time); `new-b` (the new binary) comes up on that book.
/// 4. `new-b` changes another field (`slowMs`) ⇒ the marks and the unknown
///    flag stay verbatim.
/// 5. `new-b` suspends and then resumes through the API.
///
/// Must see: `new-b` starts and reads the book (no `tenants_book_rejected`);
/// in step 3 `new-b` sees the tenant running and reruns the two turns (the
/// tickets were lost with `new-a`; `recover` and the ordinary retry postpone
/// row of `Retry`) — each exactly once: two `completed` turns within 30 s, the
/// SDK gets exactly two `Start`s per service; marks older than the postpone
/// interval make `new-b` fire nothing extra; step 4 keeps the marks and the
/// unknown flag; step 5 reruns nothing (the SDK still has exactly two `Start`s
/// per service). Before tickets were parked in RAM, step 3 kept the two turns
/// waiting until the 15-minute postpone row (a resume by the old binary takes
/// effect only at the leader that sees it, `docs/guides/flow.md` FL-24) and
/// step 5 ran them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_binary_keeps_lift_marks_and_new_binary_does_not_misfire() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let sdk = FakeSdk::builder()
        .gated_service(
            "Retry",
            "once",
            vec![Step::FailBelow(1), Step::output("ok")],
        )
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(1_500), Step::output("woke")],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let call = |req: reqwest::RequestBuilder| async move {
        let r = req.send().await?;
        let code = r.status().as_u16();
        let text = r.text().await?;
        anyhow::Ok((
            code,
            serde_json::from_str::<serde_json::Value>(&text)
                .unwrap_or(serde_json::Value::String(text)),
        ))
    };
    let blocked = |url: String| {
        let http = http.clone();
        async move {
            let text = http
                .get(format!("{url}/metrics"))
                .bearer_auth(GATE_BOOT)
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
    let status = |url: String, key: String, id: String| {
        let http = http.clone();
        async move {
            let (_, v) = call(http.get(format!("{url}/invocations/{id}")).bearer_auth(key)).await?;
            anyhow::Ok(v["status"].as_str().unwrap_or_default().to_string())
        }
    };
    let line_of = |raw: &str| {
        raw.lines()
            .find(|l| l.split(' ').next() == Some("acme"))
            .unwrap_or_default()
            .to_string()
    };

    // 1–2.
    let a = node::start(authed_on(&bucket, "new-a"))
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let url_a = a.url();
    let r = common::within(
        Duration::from_secs(90),
        "new binary, then the old one suspends",
        async {
            let t0 = std::time::Instant::now();
            let key = loop {
                let (code, v) = post_key(&url_a, GATE_BOOT, "acme", "admin").await?;
                if code == 200 {
                    break v["secret"].as_str().unwrap_or_default().to_string();
                }
                ensure!(t0.elapsed() < common::TEST_DEADLINE, "new key: {code} {v}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            let (code, v) = call(
                http.post(format!("{url_a}/deployments"))
                    .bearer_auth(&key)
                    .json(&serde_json::json!({ "uri": sdk.url })),
            )
            .await?;
            ensure!(code < 300, "register: {code} {v}");
            for state in ["suspended", "active"] {
                let (code, v) = put_state(&url_a, GATE_BOOT, "acme", state).await?;
                ensure!(code == 204, "{state}: {code} {v}");
            }
            let line = line_of(&get(&bucket, "tenants").await?);
            ensure!(
                line.contains("refused-ms=") && line.contains("lifted-ms="),
                "the new binary did not write the marks: {line:?}"
            );
            let mut ids = Vec::new();
            for svc in ["Retry", "Nap"] {
                let (code, v) = call(
                    http.post(format!("{url_a}/invoke/{svc}/once/send"))
                        .bearer_auth(&key)
                        .body("\"x\""),
                )
                .await?;
                ensure!(code < 300, "{svc}: {code} {v}");
                ids.push(v["id"].as_str().unwrap_or_default().to_string());
            }
            let t0 = std::time::Instant::now();
            while sdk.starts_len() < 2 {
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "the SDK has not received two Starts"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            while status(url_a.clone(), key.clone(), ids[1].clone()).await? != "suspended" {
                ensure!(t0.elapsed() < Duration::from_secs(20), "Nap not asleep yet");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let blocked0 = blocked(url_a.clone()).await?;
            old_binary_rewrite(&bucket, "acme", |f| {
                f.retain(|x| !x.starts_with("state="));
                f.insert(1, "state=suspended".into());
                f.push("slow-ms=50".into());
                f.push("future-flag=1".into());
            })
            .await?;
            // `new-a` re-reads the book on its refresh tick (`deploy_refresh` 100 ms).
            let t0 = std::time::Instant::now();
            loop {
                let (code, v) = call(
                    http.get(format!("{url_a}/admin/tenants"))
                        .bearer_auth(GATE_BOOT),
                )
                .await?;
                ensure!(code == 200, "{code} {v}");
                if v.to_string().contains("suspended") {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "new-a does not see the suspension yet: {v}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            sdk.gate().release(1);
            let t0 = std::time::Instant::now();
            while blocked(url_a.clone()).await? < blocked0 + 2 {
                ensure!(
                    t0.elapsed() < Duration::from_secs(20),
                    "the two turns were not refused a permit yet"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok((key, ids))
        },
    )
    .await;
    a.shutdown().await;
    let (key, ids) = r?;
    sdk.gate().open();

    // 3. The old binary resumes; the book's marks are older than the postpone interval.
    let day_ago = cairn_store::entropy::now_ms() - 24 * 3_600_000;
    let raw = old_binary_rewrite(&bucket, "acme", |f| {
        f.retain(|x| !x.starts_with("state="));
    })
    .await?;
    let raw = raw
        .lines()
        .map(|l| {
            if l.split(' ').next() != Some("acme") {
                return l.to_string();
            }
            l.split(' ')
                .map(|x| {
                    if x.starts_with("refused-ms=") {
                        format!("refused-ms={}", day_ago)
                    } else if x.starts_with("lifted-ms=") {
                        format!("lifted-ms={}", day_ago + 60_000)
                    } else {
                        x.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    put(&bucket, "tenants", &raw).await?;
    let b = node::start(authed_on(&bucket, "new-b"))
        .await
        .map_err(|e| anyhow!("new binary did not start on the old binary's book: {e}"))?;
    let url_b = b.url();
    let r = common::within(
        Duration::from_secs(120),
        "the new binary on the old binary's book",
        async {
            // Wait for `new-b` to lead: an invocation read gets an answer.
            let t0 = std::time::Instant::now();
            loop {
                let s = status(url_b.clone(), key.clone(), ids[0].clone()).await?;
                if !s.is_empty() {
                    break;
                }
                ensure!(t0.elapsed() < Duration::from_secs(30), "new-b is silent");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let text = http
                .get(format!("{url_b}/metrics"))
                .bearer_auth(GATE_BOOT)
                .send()
                .await?
                .text()
                .await?;
            ensure!(
                text.lines()
                    .any(|l| l.starts_with("cairnflow_tenants_book_rejected") && l.ends_with(" 0")),
                "the book was rejected"
            );
            // The tenant runs on `new-b`: the two turns rerun, each once.
            let t0 = std::time::Instant::now();
            for id in &ids {
                while status(url_b.clone(), key.clone(), id.clone()).await? != "completed" {
                    ensure!(
                        t0.elapsed() < Duration::from_secs(30),
                        "{id} not done 30 s after new-b took the lead (tenant running)"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            for svc in ["Retry", "Nap"] {
                let n = sdk.starts_of(svc, "once").len();
                ensure!(n == 2, "{svc}: SDK got {n} Starts in step 3, want 2");
            }

            // 4.
            let (code, v) = call(
                http.put(format!("{url_b}/admin/tenants/acme"))
                    .bearer_auth(GATE_BOOT)
                    .json(&serde_json::json!({ "slowMs": 70 })),
            )
            .await?;
            ensure!(code == 200, "{code} {v}");
            let line = line_of(&get(&bucket, "tenants").await?);
            ensure!(
                line.contains(&format!("refused-ms={day_ago}"))
                    && line.contains(&format!("lifted-ms={}", day_ago + 60_000))
                    && line.contains("future-flag=1")
                    && line.contains("slow-ms=70"),
                "the new binary lost the marks or the unknown flag: {line:?}"
            );

            // 5.
            for state in ["suspended", "active"] {
                let (code, v) = put_state(&url_b, GATE_BOOT, "acme", state).await?;
                ensure!(code == 204, "{state}: {code} {v}");
            }
            // The new marks of this suspend/resume rerun nothing already done.
            tokio::time::sleep(Duration::from_secs(2)).await;
            for id in &ids {
                let st = status(url_b.clone(), key.clone(), id.clone()).await?;
                ensure!(st == "completed", "{id} after step 5: {st}");
            }
            for svc in ["Retry", "Nap"] {
                let n = sdk.starts_of(svc, "once").len();
                ensure!(n == 2, "{svc}: the SDK got {n} Starts, must be exactly 2");
            }
            Ok(())
        },
    )
    .await;
    b.shutdown().await;
    r
}
