// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl backup` / `restore`: a **self-contained** backup into another store —
//! a copy that can rebuild the cluster after the source bucket is lost for good.

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
    cairn_server::ctl::{BackupManifest, Ctl, CtlError, VerifyOptions, BACKUP_MANIFEST},
    cairn_store::{ObjectStores, StoreLocation},
    common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
    futures::TryStreamExt,
    reqwest::{Client, StatusCode},
    serde_json::json,
    slatedb::object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt},
    std::{sync::Arc, time::Duration},
};

/// The test bucket's `Arc` (for `Ctl` and `ObjectStores`).
fn arc(b: &common::Bucket) -> Arc<InMemory> {
    Arc::clone(b)
}

fn ctl_on(store: Arc<dyn ObjectStore>) -> Ctl {
    Ctl::new(ObjectStores::single(StoreLocation::from_store(
        store,
        "",
        "memory://",
    )))
}

/// The backup target: an `InMemory` entirely separate from the source bucket, under prefix `daily`.
fn backup_target(mem: &Arc<InMemory>) -> StoreLocation {
    StoreLocation::from_store(mem.clone(), "daily", "memory://r2/daily")
}

/// Every object of `store` under `prefix`.
async fn list(store: &InMemory, prefix: &str) -> Result<Vec<String>> {
    let p = (!prefix.is_empty()).then(|| ObjPath::from(prefix));
    Ok(store
        .list(p.as_ref())
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|m| m.location.to_string())
        .collect())
}

/// Every commit has been flushed to L0: no shared WAL log is still pinned by any range
/// (`wal floors` — the floor may pass every frame, NW-7).
async fn flushed(ctl: &Ctl) -> Result<bool> {
    let logs = ctl.wal_floors().await?;
    Ok(!logs.is_empty() && logs.iter().all(|l| l.pinned_by_range.is_none()))
}

/// **Back up, lose the source, restore.**
///
/// Order:
/// 1. A cluster node on the source bucket (4 ranges, shared WAL, flushing forced by
///    frame age 100 ms). Fake SDK: `Svc/run` = suspend waiting for a signal, then
///    return `"done"`. Send two invocations; both `suspended`.
/// 2. Wait (bounded) until every range has flushed to L0 — the copy takes only that part.
/// 3. `backup --to memory://r2/daily` **while the node is still running**.
/// 4. Stop the node, **delete every object** of the source bucket.
/// 5. `restore` into a new bucket (the same empty prefix); `verify` clean.
/// 6. A new node on that bucket, the same fake SDK; signal both invocations.
///
/// Must see: the copy has a manifest listing every object with its hash; the source
/// keeps no checkpoint/barrier of the backup; on the rebuilt cluster, both
/// invocations go on to `completed` with output `"done"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_survives_loss_of_the_source_bucket() -> Result<()> {
    let src = common::bucket();
    let target = Arc::new(InMemory::new());
    let sdk = FakeSdk::builder()
        .service("Svc", "run", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.node_wal.frame_age_flush = Duration::from_millis(100);
        cfg.retention_ms = None;
    };
    // 1.
    let a = common::start_cluster_node_tweaked(&src, "node-a", 4, Duration::from_secs(2), tweak)
        .await?;
    let url = a.node.url();
    let r = common::within(Duration::from_secs(60), "build and send", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for i in 0..2 {
            let (code, body) = common::send(&url, "Svc", "run", None, &format!("\"{i}\"")).await?;
            ensure!((200..300).contains(&code), "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        for id in &ids {
            common::wait_status(&url, id, "suspended").await?;
        }
        // 2.
        let probe = ctl_on(arc(&src));
        let t0 = tokio::time::Instant::now();
        while !flushed(&probe).await? {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "ranges not flushed to L0 after 20 s: {:?}",
                probe.wal_floors().await?
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(ids)
    })
    .await;
    let ids = match r {
        Ok(ids) => ids,
        Err(e) => {
            a.node.shutdown().await;
            return Err(e);
        }
    };
    // 3.
    let ctl = ctl_on(arc(&src));
    let rep = ctl
        .backup(
            &backup_target(&target),
            "test@backup",
            "cairn-ctl backup --to memory://r2/daily",
        )
        .await;
    a.node.shutdown().await;
    let rep = rep?;
    ensure!(rep.ranges.len() == 4 && rep.objects > 0, "{rep:?}");
    ensure!(rep.audit.is_some(), "backup wrote no audit/ctl: {rep:?}");
    let raw = target
        .get(&ObjPath::from(format!(
            "daily/{}/{BACKUP_MANIFEST}",
            rep.id
        )))
        .await?
        .bytes()
        .await?;
    let m = BackupManifest::decode(&String::from_utf8_lossy(&raw)).map_err(|e| anyhow!(e))?;
    ensure!(m.objects.len() == rep.objects, "{m:?}");
    ensure!(
        m.as_of.iter().map(|(r, _)| *r).collect::<Vec<_>>() == rep.ranges
            // A SlateDB checkpoint's `create_time` is precise to the second.
            && m.as_of.iter().all(|(_, ms)| *ms + 1_000 > m.started_ms),
        "as_of per range: {:?}",
        m.as_of
    );
    for must in ["format", "cluster.conf", "owners/range-0"] {
        ensure!(m.objects.iter().any(|(r, _)| r == must), "missing {must}");
    }
    for never in ["sessions/", "nodewal/", "gc-barriers/", "incarnations/"] {
        ensure!(
            !m.objects.iter().any(|(r, _)| r.starts_with(never)),
            "the copy copied {never}"
        );
    }
    let barriers = list(&src, "gc-barriers").await?;
    ensure!(
        barriers.is_empty(),
        "the backup's GC barrier is still on the source: {barriers:?}"
    );

    // 4.
    let all = list(&src, "").await?;
    ensure!(!all.is_empty());
    for p in &all {
        src.delete(&ObjPath::from(p.as_str())).await?;
    }
    ensure!(list(&src, "").await?.is_empty());

    // 5.
    let restored = common::bucket();
    let into = ctl_on(arc(&restored));
    let rr = into
        .restore(
            &backup_target(&target),
            None,
            "test@backup",
            "cairn-ctl restore",
        )
        .await?;
    ensure!(rr.id == rep.id && rr.objects == rep.objects, "{rr:?}");
    let v = into.verify(VerifyOptions::default()).await?;
    ensure!(v.clean(), "verify on the rebuilt cluster: {:?}", v.findings);
    ensure!(
        matches!(
            into.restore(&backup_target(&target), None, "t", "c").await,
            Err(CtlError::Refused(_))
        ),
        "a second restore into a non-empty target must be refused"
    );

    // 6.
    let b =
        common::start_cluster_node_tweaked(&restored, "node-b", 4, Duration::from_secs(2), tweak)
            .await?;
    let url = b.node.url();
    let r = common::within(
        Duration::from_secs(60),
        "resume on the rebuilt cluster",
        async {
            for id in &ids {
                let t0 = tokio::time::Instant::now();
                loop {
                    let resp = Client::new()
                        .post(format!("{url}/invocations/{id}/signal"))
                        .json(&json!({ "signal": HOLD_SIGNAL }))
                        .send()
                        .await?;
                    if resp.status() == StatusCode::OK {
                        break;
                    }
                    let last = format!("{} {}", resp.status(), resp.text().await?);
                    ensure!(
                        t0.elapsed() < Duration::from_secs(20),
                        "signal {id}: {last}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let st = common::wait_status(&url, id, "completed").await?;
                ensure!(st["output"] == "done", "{id}: {st}");
            }
            Ok(())
        },
    )
    .await;
    b.node.shutdown().await;
    r
}

/// A sample bucket with split ranges (ranges 2, 3 stand on the SSTs of the retired
/// ancestor 0 through `external_dbs`): the copy is **self-contained** — restored into
/// another bucket, with no path left back to the source, `verify` clean and a node
/// reads the finished invocation back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_of_split_ranges_carries_ancestor_ssts() -> Result<()> {
    let dir = common::fixture::dir("format10-cluster");
    let expected = common::fixture::expected(&dir)?;
    let src = common::fixture::bucket(&dir).await?;
    let target = Arc::new(InMemory::new());
    let rep = ctl_on(arc(&src))
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await?;
    // The copy has SSTs under `ranges/0/` even though 0 is no longer in the table.
    let m = BackupManifest::decode(&String::from_utf8_lossy(
        &target
            .get(&ObjPath::from(format!(
                "daily/{}/{BACKUP_MANIFEST}",
                rep.id
            )))
            .await?
            .bytes()
            .await?,
    ))
    .map_err(|e| anyhow!(e))?;
    ensure!(
        m.objects
            .iter()
            .any(|(r, _)| r.starts_with("ranges/0/compacted/")),
        "the copy lacks ancestor 0's SSTs: {:?}",
        m.objects.iter().map(|(r, _)| r).collect::<Vec<_>>()
    );
    drop(src);

    let restored = common::bucket();
    let into = ctl_on(arc(&restored));
    into.restore(
        &backup_target(&target),
        Some(&rep.id),
        "test@backup",
        "cairn-ctl restore",
    )
    .await?;
    let v = into.verify(VerifyOptions::default()).await?;
    ensure!(v.clean(), "verify: {:?}", v.findings);

    let inv = expected["invocations"]
        .as_array()
        .and_then(|a| a.iter().find(|i| i["status"] == "completed"))
        .cloned()
        .ok_or_else(|| anyhow!("EXPECTED.json has no completed invocation"))?;
    let id = inv["id"].as_str().unwrap_or_default().to_string();
    let n = common::start_cluster_node_tweaked(
        &restored,
        "node-restore",
        2,
        Duration::from_secs(2),
        |cfg| {
            cfg.auth = common::fixture::auth();
            cfg.retention_ms = None;
        },
    )
    .await?;
    let url = n.node.url();
    let r = common::within(Duration::from_secs(30), "read the invocation back", async {
        loop {
            let (code, st) = common::fixture::call(
                common::fixture::ADMIN_KEY,
                Client::new().get(format!("{url}/invocations/{id}")),
            )
            .await?;
            if code == StatusCode::OK {
                ensure!(
                    st["status"] == inv["status"] && st["output"] == inv["output"],
                    "{st} ≠ {inv}"
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    n.node.shutdown().await;
    r
}

/// **`restore` refuses a copy that pairs one table's Dbs with a later table's
/// `cluster.conf`.** A copy of the sample bucket (live ranges 1, 2, 3; owner records
/// 0–3) with an edited manifest: range 1 dropped from `ranges` and its Db objects
/// dropped — exactly the shape of a copy the old backup took while a split was
/// committing (the table lists a range with an owner record for which the copy has
/// no Db).
///
/// Must see: `restore` returns `Refused` naming range 1, nothing is written to the
/// target. The intact copy still restores.
///
/// On the old code: `restore` copied everything without cross-checking ⇒ range 1
/// opened as an empty range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_refuses_a_backup_whose_table_lists_a_range_without_db() -> Result<()> {
    let dir = common::fixture::dir("format10-cluster");
    let src = common::fixture::bucket(&dir).await?;
    let target = Arc::new(InMemory::new());
    let rep = ctl_on(arc(&src))
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await?;
    ensure!(rep.ranges.contains(&1), "{rep:?}");
    let path = ObjPath::from(format!("daily/{}/{BACKUP_MANIFEST}", rep.id));
    let raw = String::from_utf8_lossy(&target.get(&path).await?.bytes().await?).into_owned();
    let good = BackupManifest::decode(&raw).map_err(|e| anyhow!(e))?;
    let mut bad = good.clone();
    bad.ranges.retain(|r| *r != 1);
    bad.as_of.retain(|(r, _)| *r != 1);
    bad.objects.retain(|(rel, _)| !rel.starts_with("ranges/1/"));
    ensure!(
        bad.objects.iter().any(|(rel, _)| rel == "owners/range-1"),
        "range 1's owner record must still be there"
    );
    target.put(&path, bad.encode().into()).await?;
    let a = common::bucket();
    let r = ctl_on(arc(&a))
        .restore(&backup_target(&target), Some(&rep.id), "t", "c")
        .await;
    ensure!(
        matches!(&r, Err(CtlError::Refused(m)) if m.contains("[1]")),
        "{r:?}"
    );
    ensure!(
        list(&a, "").await?.is_empty(),
        "restoring the mismatched copy wrote into the target"
    );
    target.put(&path, good.encode().into()).await?;
    ctl_on(arc(&a))
        .restore(&backup_target(&target), Some(&rep.id), "t", "c")
        .await?;
    Ok(())
}

/// An incomplete copy (no backup manifest) is never used: `restore --id` refuses
/// it, `restore` without `--id` skips it, `backup list` reports it incomplete, `prune`
/// does not delete a complete copy within the retention schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_backup_is_never_used() -> Result<()> {
    let dir = common::fixture::dir("format10-cluster");
    let src = common::fixture::bucket(&dir).await?;
    let target = Arc::new(InMemory::new());
    let ctl = ctl_on(arc(&src));
    let good = ctl
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await?;
    // A fake incomplete copy: data without a manifest, a newer id.
    let partial = "9999999999999-0000";
    target
        .put(
            &ObjPath::from(format!("daily/{partial}/data/format")),
            "cairnflow-format\nversion=10\n".into(),
        )
        .await?;
    let listed = ctl.backup_list(&backup_target(&target)).await?;
    ensure!(
        listed.len() == 2
            && listed.iter().any(|b| b.id == good.id && b.complete)
            && listed.iter().any(|b| b.id == partial && !b.complete),
        "{listed:?}"
    );
    let a = common::bucket();
    let r = ctl_on(arc(&a))
        .restore(&backup_target(&target), Some(partial), "t", "c")
        .await;
    ensure!(matches!(r, Err(CtlError::Refused(_))), "{r:?}");
    ensure!(
        list(&a, "").await?.is_empty(),
        "restoring an incomplete copy wrote into the target"
    );
    let r = ctl_on(arc(&a))
        .restore(&backup_target(&target), None, "t", "c")
        .await?;
    ensure!(
        r.id == good.id,
        "restore without --id must pick the newest complete copy: {r:?}"
    );
    let p = ctl
        .backup_prune(&backup_target(&target), 7, 4, true, "t", "c")
        .await?;
    ensure!(
        p.keep == vec![good.id.clone()] && p.delete.is_empty(),
        "{p:?}"
    );
    Ok(())
}

/// Exactly the SQL queries of `docs/runbooks/restore-from-backup.md` step 5 (look for
/// calls whose messages were lost after a restore) run and return the right rows on
/// a cluster with one pending call (an SQL query that has never run does not go
/// into a runbook).
///
/// Order:
/// 1. A lone node with 4 ranges, fake SDK: `Caller/run` = `ctx.call(Leaf/echo)` then
///    return the result; `Leaf/echo` = suspend waiting for a signal, then return `"leaf"`.
/// 2. Send `Caller/run`; wait for the caller to be `suspended` (waiting for the
///    `Call`'s completion) and the callee `suspended`.
/// 3. Run the runbook's five SQL queries.
///
/// Must see: query 1 has the caller; query 2 returns exactly the `Call` entry with
/// `"completed":false` and `invocationId` = the callee; query 3 sees the callee
/// `suspended`; query 4 has no outbox row for the caller (the call was delivered —
/// the case where **no** message was lost); query 5 runs (no `reply` waiting to be
/// delivered). Signalling the callee finishes the caller — clean for `verify`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runbook_queries_find_a_waiting_call() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service(
            "Caller",
            "run",
            vec![Step::call("Leaf", "echo", None), Step::EchoCallResult],
        )
        .service("Leaf", "echo", vec![Step::Suspend, Step::output("leaf")])
        .spawn()
        .await?;
    let a = common::start_cluster_node(&bucket, "node-a", 4, Duration::from_secs(2)).await?;
    let url = a.node.url();
    let r = common::within(Duration::from_secs(60), "runbook SQL queries", async {
        let sql = |q: String| {
            let url = url.clone();
            async move {
                let resp = Client::new()
                    .post(format!("{url}/query"))
                    .json(&json!({ "sql": q }))
                    .send()
                    .await?;
                let code = resp.status();
                let v: serde_json::Value = resp.json().await?;
                ensure!(code == StatusCode::OK, "{q}: {code} {v}");
                // `rows` is an array in `columns` order: zip into an object.
                let cols: Vec<String> = v["columns"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|c| {
                        c["name"]
                            .as_str()
                            .or(c.as_str())
                            .unwrap_or_default()
                            .to_string()
                    })
                    .collect();
                let rows = v["rows"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|r| {
                        cols.iter()
                            .cloned()
                            .zip(r.as_array().cloned().unwrap_or_default())
                            .collect::<serde_json::Map<_, _>>()
                            .into()
                    })
                    .collect::<Vec<serde_json::Value>>();
                Ok::<_, anyhow::Error>(rows)
            }
        };
        // 1–2.
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::send(&url, "Caller", "run", None, "\"x\"").await?;
        ensure!((200..300).contains(&code), "{code} {body}");
        let caller = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &caller, "suspended").await?;
        // 3. Query 1.
        let t0 = tokio::time::Instant::now();
        let rows = loop {
            let rows =
                sql("SELECT id FROM sys_invocation WHERE status = 'suspended'".into()).await?;
            if rows.len() == 2 {
                break rows;
            }
            ensure!(t0.elapsed() < Duration::from_secs(20), "query 1: {rows:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        ensure!(
            rows.iter().any(|r| r["id"] == caller.as_str()),
            "query 1: {rows:?}"
        );
        // Query 2.
        let rows = sql(format!(
            "SELECT \"index\", entry_lite FROM sys_journal WHERE id = '{caller}' \
             AND name = 'Call' AND entry_lite LIKE '%\"completed\":false%'"
        ))
        .await?;
        ensure!(rows.len() == 1, "query 2: {rows:?}");
        let lite: serde_json::Value =
            serde_json::from_str(rows[0]["entry_lite"].as_str().unwrap_or("{}"))?;
        let callee = lite["invocationId"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        ensure!(!callee.is_empty() && callee != caller, "query 2: {rows:?}");
        // Query 3.
        let rows = sql(format!(
            "SELECT status FROM sys_invocation WHERE id = '{callee}'"
        ))
        .await?;
        ensure!(
            rows.len() == 1 && rows[0]["status"] == "suspended",
            "query 3: {rows:?}"
        );
        // Query 4.
        let rows = sql(format!(
            "SELECT kind, completion_id FROM sys_outbox WHERE id = '{caller}'"
        ))
        .await?;
        ensure!(rows.is_empty(), "query 4: {rows:?}");
        // Query 5.
        let rows =
            sql("SELECT id, completion_id FROM sys_outbox WHERE kind = 'reply'".into()).await?;
        ensure!(rows.is_empty(), "query 5: {rows:?}");
        // Clean up: signal the callee, the caller finishes.
        let resp = Client::new()
            .post(format!("{url}/invocations/{callee}/signal"))
            .json(&json!({ "signal": HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(resp.status() == StatusCode::OK, "signal: {}", resp.status());
        let st = common::wait_status(&url, &caller, "completed").await?;
        ensure!(st["output"] == "leaf", "{st}");
        Ok(())
    })
    .await;
    a.node.shutdown().await;
    r
}

/// `backup prune --apply` really deletes: two complete copies on the same day, keep
/// 1 day + 0 weeks ⇒ every object of the old copy deleted (manifest first), the new
/// one intact and restorable; a dry run deletes nothing; an `audit/ctl` record on
/// the cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prune_deletes_old_backups_and_keeps_the_schedule() -> Result<()> {
    let src = common::fixture::bucket(&common::fixture::dir("format10-cluster")).await?;
    let target = Arc::new(InMemory::new());
    let ctl = ctl_on(arc(&src));
    let old = ctl
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await?;
    let new = ctl
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await?;
    ensure!(old.id < new.id, "{} {}", old.id, new.id);
    let dry = ctl
        .backup_prune(&backup_target(&target), 1, 0, false, "t", "c")
        .await?;
    ensure!(
        !dry.applied && dry.keep == vec![new.id.clone()] && dry.delete == vec![old.id.clone()],
        "{dry:?}"
    );
    ensure!(!list(&target, &format!("daily/{}", old.id))
        .await?
        .is_empty());
    let audits_before = list(&src, "audit/ctl").await?.len();
    let p = ctl
        .backup_prune(
            &backup_target(&target),
            1,
            0,
            true,
            "test@backup",
            "cairn-ctl backup prune --apply",
        )
        .await?;
    ensure!(
        p.applied && p.objects_deleted > 0 && p.audit.is_some(),
        "{p:?}"
    );
    ensure!(
        list(&target, &format!("daily/{}", old.id))
            .await?
            .is_empty(),
        "the old copy still has objects"
    );
    ensure!(list(&src, "audit/ctl").await?.len() == audits_before + 1);
    let listed = ctl.backup_list(&backup_target(&target)).await?;
    ensure!(
        listed.len() == 1 && listed[0].id == new.id && listed[0].complete,
        "{listed:?}"
    );
    let restored = common::bucket();
    let r = ctl_on(arc(&restored))
        .restore(&backup_target(&target), None, "t", "c")
        .await?;
    ensure!(r.id == new.id, "{r:?}");
    Ok(())
}

/// Ops reports `ops-reports/<id>` do not travel with the copy, and do not block
/// `restore`.
///
/// Order:
/// 1. The source cluster (fixture) has an old report `ops-reports/src-1`; `backup`.
/// 2. The target has only the report of an earlier failed
///    `restore --report-to-bucket` (`ops-reports/fail-1`); `restore`.
/// 3. Another target has an object that is not a report; `restore`.
///
/// Must see: (1) the manifest does not list `ops-reports/`; (2) the restore finishes,
/// report `fail-1` is intact, `src-1` does not come back; (3) refused as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ops_reports_stay_out_of_backups_and_do_not_block_restore() -> Result<()> {
    use cairn_store::ops_report;
    let dir = common::fixture::dir("format10-cluster");
    let src = common::fixture::bucket(&dir).await?;
    let src_store: Arc<dyn ObjectStore> = arc(&src);
    ops_report::write(&src_store, &ObjPath::from(""), "src-1", b"{}").await?;
    let target = Arc::new(InMemory::new());
    let ctl = ctl_on(src_store);
    let good = ctl
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await?;
    let in_backup = list(&target, &format!("daily/{}/data", good.id)).await?;
    ensure!(
        !in_backup.iter().any(|p| p.contains("ops-reports")),
        "the copy carries the source's reports: {in_backup:?}"
    );

    let a = common::bucket();
    let a_store: Arc<dyn ObjectStore> = arc(&a);
    ops_report::write(&a_store, &ObjPath::from(""), "fail-1", br#"{"ok":false}"#).await?;
    ctl_on(a_store.clone())
        .restore(&backup_target(&target), None, "t", "c")
        .await?;
    let left = ops_report::list(&a_store, &ObjPath::from("")).await?;
    ensure!(
        left.len() == 1 && left[0].id == "fail-1",
        "reports on the target after restore: {left:?}"
    );

    let b = common::bucket();
    b.put(&ObjPath::from("junk"), "x".into()).await?;
    let r = ctl_on(arc(&b))
        .restore(&backup_target(&target), None, "t", "c")
        .await;
    ensure!(matches!(r, Err(CtlError::Refused(_))), "{r:?}");
    b.allow("unlisted_object", "junk object planted so restore refuses");
    Ok(())
}

/// **A quiet cluster ⇒ a restore reports no lost message** (the control case of
/// `failpoints.rs::restore_finds_a_lost_call`/`_reply`).
///
/// Order:
/// 1. One node with two ranges, flushing forced by frame age. `Caller/run` (range 1)
///    calls `Leaf/echo` (range 0), which suspends waiting for a signal: a valid
///    **pending** call. Plus one cross-range call that has finished (its callee
///    signalled).
/// 2. Wait until every range has flushed to L0; back up; restore into a new bucket;
///    `verify`.
///
/// Must see: no `restore.*` finding — the pending call has its callee in the copy,
/// the finished call has no one waiting any more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_of_a_quiet_cluster_reports_no_lost_message() -> Result<()> {
    let bucket = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&bucket, "node-a", 2, Duration::from_secs(2), |cfg| {
            cfg.node_wal.frame_age_flush = Duration::from_millis(100);
            cfg.retention_ms = None;
        })
        .await?;
    let url = node.node.url();
    let caller_key = |p: &str| common::key_on_partition(&node.node, "Caller", 1, p);
    let leaf_key = |p: &str| common::key_on_partition(&node.node, "Leaf", 0, p);
    let (wait_leaf, done_leaf) = (leaf_key("wait"), leaf_key("done"));
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "wait",
            vec![
                Step::call("Leaf", "echo", Some(&wait_leaf)),
                Step::EchoCallResult,
            ],
        )
        .object(
            "Caller",
            "done",
            vec![
                Step::call("Leaf", "echo", Some(&done_leaf)),
                Step::EchoCallResult,
            ],
        )
        .object("Leaf", "echo", vec![Step::Suspend, Step::output("\"ok\"")])
        .spawn()
        .await?;
    let r = common::within(Duration::from_secs(90), "restore a quiet cluster", async {
        common::register(&url, &sdk.url).await?;
        let mut callers = Vec::new();
        for (h, k) in [("wait", caller_key("a")), ("done", caller_key("b"))] {
            let (code, body) = common::send(&url, "Caller", h, Some(&k), "\"x\"").await?;
            ensure!((200..300).contains(&code), "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            common::wait_status(&url, &id, "suspended").await?;
            callers.push(id);
        }
        // Signal the callee of the "done" call: that caller finishes.
        let list: serde_json::Value = reqwest::get(format!("{url}/invocations"))
            .await?
            .json()
            .await?;
        let done_callee = list
            .as_array()
            .into_iter()
            .flatten()
            .find(|v| v["service"] == "Leaf" && v["key"] == done_leaf.as_str())
            .and_then(|v| v["id"].as_str().map(str::to_string))
            .ok_or_else(|| anyhow!("no callee for the done call: {list}"))?;
        let r = Client::new()
            .post(format!("{url}/invocations/{done_callee}/signal"))
            .json(&json!({ "signal": HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(r.status().is_success(), "signal: {}", r.status());
        common::wait_status(&url, &callers[1], "completed").await?;
        // Every state of the two calls down to L0 (the copy only takes that part). Do
        // not wait for `flushed`: range 0 has continuous background writes, so it always
        // pins a new frame.
        for p in 0..2 {
            node.node
                .partitions
                .get(cairn_store::PartitionId(p))
                .ok_or_else(|| anyhow!("engine of range {p}"))?
                .flush_memtable()
                .await?;
        }
        let src = ctl_on(arc(&bucket));
        let target = Arc::new(InMemory::new());
        src.backup(&backup_target(&target), "t", "c").await?;
        let dst = common::bucket();
        ctl_on(arc(&dst))
            .restore(&backup_target(&target), None, "t", "c")
            .await?;
        let rep = ctl_on(arc(&dst)).verify(VerifyOptions::default()).await?;
        let lost: Vec<_> = rep
            .findings
            .iter()
            .filter(|f| f.check.starts_with("restore."))
            .collect();
        ensure!(lost.is_empty(), "a quiet cluster reports losses: {lost:?}");
        Ok(())
    })
    .await;
    node.node.shutdown().await;
    r
}

/// **Backup then restore keeps segmented values.**
///
/// 1. A cluster node with one range, flushing by frame age 100 ms. Fake SDK ≤ v5:
///    `Chat/seed` writes `messages` = H (1.5 MiB) ⇒ the shell seals it; `Chat/read`
///    reads lazily and returns it.
/// 2. Wait until every range has flushed to L0; `backup` while the node runs.
/// 3. Stop the node, wipe the source bucket; `restore` into a new bucket; `verify`.
/// 4. A control for the sweep: PUT an **orphan** segment (the same owner
///    `Chat[c1]`, no holder) into the rebuilt bucket.
/// 5. A new node on the rebuilt bucket, the same SDK, a fast values sweep (100 ms,
///    `min_age` 0, keep 0) — the sweep runs on the **restored** Db; `Chat/read`
///    before and after many rounds.
///
/// Must see: the copy carries the segment's `values/…` objects; `verify` on the
/// rebuilt cluster is clean (no `value_orphan`, no `value_dangling`); the sweep
/// deletes the orphan segment (so it does run) without deleting any segment that
/// still has a holder in the restored Db; `read` returns H byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_and_restore_keep_segmented_values() -> Result<()> {
    const H: usize = 3 << 19;
    let mut h = vec![b'h'; H];
    h[0] = b'"';
    h[H - 1] = b'"';
    let h = bytes::Bytes::from(h);
    let src = common::bucket();
    let target = Arc::new(InMemory::new());
    let sdk = FakeSdk::builder()
        .object(
            "Chat",
            "seed",
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
        .spawn()
        .await?;
    let tweak = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.node_wal.frame_age_flush = Duration::from_millis(100);
        cfg.retention_ms = None;
    };
    // The rebuilt node: a fast values sweep, deleting for real as soon as it can prove it.
    let tweak_restored = |cfg: &mut cairn_server::node::NodeConfig| {
        tweak(cfg);
        cfg.value_sweep = Some(Duration::from_millis(100));
        cfg.value_min_age = Duration::ZERO;
        cfg.trash_keep = Some(Duration::ZERO);
    };
    // 1.
    let a = common::start_cluster_node_tweaked(&src, "node-a", 1, Duration::from_secs(2), tweak)
        .await?;
    let url = a.node.url();
    let r = common::within(Duration::from_secs(120), "write and flush", async {
        common::register(&url, &sdk.url).await?;
        let (code, v) = common::invoke(&url, "Chat", "seed", Some("c1"), "{}").await?;
        ensure!(code == 200, "seed: {code} {v}");
        // 2.
        let probe = ctl_on(arc(&src));
        let t0 = tokio::time::Instant::now();
        while !flushed(&probe).await? {
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "ranges not flushed to L0 after 60 s: {:?}",
                probe.wal_floors().await?
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    })
    .await;
    if let Err(e) = r {
        a.node.shutdown().await;
        return Err(e);
    }
    let segments = list(&src, "values").await?;
    ensure!(!segments.is_empty(), "seed sealed no segment");
    let rep = ctl_on(arc(&src))
        .backup(&backup_target(&target), "test@backup", "cairn-ctl backup")
        .await;
    a.node.shutdown().await;
    let rep = rep?;
    let raw = target
        .get(&ObjPath::from(format!(
            "daily/{}/{BACKUP_MANIFEST}",
            rep.id
        )))
        .await?
        .bytes()
        .await?;
    let m = BackupManifest::decode(&String::from_utf8_lossy(&raw)).map_err(|e| anyhow!(e))?;
    for s in &segments {
        ensure!(
            m.objects.iter().any(|(r, _)| r == s),
            "the copy lacks segment {s}"
        );
    }
    // 3.
    for p in list(&src, "").await? {
        src.delete(&ObjPath::from(p.as_str())).await?;
    }
    let restored = common::bucket();
    let into = ctl_on(arc(&restored));
    into.restore(
        &backup_target(&target),
        None,
        "test@backup",
        "cairn-ctl restore",
    )
    .await?;
    let v = into.verify(VerifyOptions::default()).await?;
    ensure!(v.clean(), "verify on the rebuilt cluster: {:?}", v.findings);
    ensure!(
        list(&restored, "values").await? == segments,
        "the restored copy carries exactly the source's segments"
    );
    // 4.
    let values = cairn_store::value::ValueStore::new(
        Arc::clone(&restored) as Arc<dyn ObjectStore>,
        &ObjPath::default(),
    );
    let (pk, owner) =
        cairn_store::value::object_owner(&cairn_store::TenantId::default(), None, "Chat", "c1")
            .ok_or_else(|| anyhow!("Chat[c1] has an owner"))?;
    let orphan = cairn_store::value::Segment {
        tenant: "default".into(),
        pk,
        owner,
        sha: cairn_store::artifact::sha256(b"orphan"),
        gen: 7,
    };
    values
        .put(&orphan, bytes::Bytes::from_static(b"orphan"))
        .await?;
    let orphan_path = values.path(&orphan).to_string();
    // 5.
    let b = common::start_cluster_node_tweaked(
        &restored,
        "node-b",
        1,
        Duration::from_secs(2),
        tweak_restored,
    )
    .await?;
    let url = b.node.url();
    let read = || async {
        let (code, v) = common::invoke(&url, "Chat", "read", Some("c1"), "{}").await?;
        ensure!(
            code == 200 && v["output"].as_str().map(str::as_bytes) == Some(&h[..]),
            "read returns all {H} bytes: {code}"
        );
        Ok::<_, anyhow::Error>(())
    };
    let r = common::within(
        Duration::from_secs(90),
        "read and sweep on the rebuilt cluster",
        async {
            let t0 = tokio::time::Instant::now();
            while let Err(e) = read().await {
                ensure!(t0.elapsed() < Duration::from_secs(20), "{e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            let t0 = tokio::time::Instant::now();
            while list(&restored, "values").await?.contains(&orphan_path) {
                ensure!(
                    t0.elapsed() < Duration::from_secs(30),
                    "the sweep did not delete the orphan segment after 30 s"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // Many more rounds after the orphan is gone: every restored segment (still
            // held) stays. The sealed output of the extra `read` turns adds new segments.
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let left = list(&restored, "values").await?;
            let gone: Vec<_> = segments.iter().filter(|s| !left.contains(s)).collect();
            ensure!(
                gone.is_empty(),
                "the sweep deleted a segment that still has a holder: {gone:?}"
            );
            read().await
        },
    )
    .await;
    b.node.shutdown().await;
    r
}
