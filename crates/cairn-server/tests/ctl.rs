// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl`: offline bucket checks and repair commands.
//!
//! Every test loads the format-10 cluster sample bucket (`tests/fixtures/format10-cluster`)
//! into an `InMemory`, then calls `cairn_server::ctl` directly — the `cairn-ctl`
//! binary only parses arguments and prints. A broken bucket is built by editing
//! exactly one place of the sample bucket, so that each test sees **exactly one** error.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_server::ctl::{Ctl, CtlError, Finding, RepairOptions, VerifyOptions},
    cairn_store::{
        testkit::{FaultyStore, Op},
        ObjectStores, PartitionId, StoreLocation,
    },
    futures::TryStreamExt,
    slatedb::object_store::{
        memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
    },
    std::{
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format10-cluster")
}

/// Load a sample bucket directory into a new `InMemory` (without `EXPECTED.json`).
async fn load_fixture() -> Result<Arc<InMemory>> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
        for e in std::fs::read_dir(dir)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                walk(root, &e.path(), out)?;
            } else {
                let rel = e
                    .path()
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push((rel, e.path()));
            }
        }
        Ok(())
    }
    let dir = fixture_dir();
    let mut files = Vec::new();
    walk(&dir, &dir, &mut files)?;
    let bucket = Arc::new(InMemory::new());
    for (rel, path) in files {
        if rel == "EXPECTED.json" {
            continue;
        }
        bucket
            .put(&ObjPath::from(rel), PutPayload::from(std::fs::read(path)?))
            .await?;
    }
    Ok(bucket)
}

fn ctl_on(store: Arc<dyn ObjectStore>) -> Ctl {
    Ctl::new(ObjectStores::single(StoreLocation::from_store(
        store,
        "",
        "memory://",
    )))
    .with_clock(Arc::new(common::JumpClock::default()))
}

async fn put(bucket: &InMemory, path: &str, body: &str) -> Result<()> {
    bucket
        .put(
            &ObjPath::from(path),
            PutPayload::from(body.as_bytes().to_vec()),
        )
        .await?;
    Ok(())
}

async fn get(bucket: &InMemory, path: &str) -> Result<Option<String>> {
    match bucket.get(&ObjPath::from(path)).await {
        Ok(g) => Ok(Some(String::from_utf8(g.bytes().await?.to_vec())?)),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn delete_prefix(bucket: &InMemory, prefix: &str) -> Result<usize> {
    let listed: Vec<_> = bucket
        .list(Some(&ObjPath::from(prefix)))
        .try_collect()
        .await?;
    for m in &listed {
        bucket.delete(&m.location).await?;
    }
    Ok(listed.len())
}

/// The sample bucket's table (v3: ranges 2, 3 are children of 0; range 1 untouched),
/// with `next_range_id` and range 1's row replaceable.
fn table(next_range_id: u32, range_1: &str) -> String {
    format!(
        "cairnflow-cluster/3\ntable_version=3\nnext_range_id={next_range_id}\n\
         range=0000000000000000-3fffffffffffffff:2:1\n\
         range=4000000000000000-7fffffffffffffff:3:1\n\
         range=8000000000000000-ffffffffffffffff:{range_1}\n"
    )
}

fn repair(apply: bool, force: bool) -> RepairOptions {
    RepairOptions {
        apply,
        force,
        who: "test@ctl".into(),
        command: "cairn-ctl test".into(),
        expect_version: None,
        now_ms: 1_800_000_000_000,
    }
}

fn kinds(findings: &[Finding]) -> Vec<(&'static str, Option<u32>)> {
    findings.iter().map(|f| (f.check, f.range)).collect()
}

/// The `format10-cluster` sample bucket is clean: the table covers everything, every
/// range opens, every `external_dbs` (children 2, 3 → parent 0) points at a Db and a
/// checkpoint that still exist, the retired parent's Db is not taken for an orphan.
/// The read commands run on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_is_clean_on_format10_cluster_fixture() -> Result<()> {
    let bucket = load_fixture().await?;
    let faulty = FaultyStore::wrap(bucket.clone());
    let ctl = ctl_on(faulty.clone());
    let before = faulty.counts();
    let rep = ctl.verify(VerifyOptions::default()).await?;
    ensure!(rep.clean(), "sample bucket not clean: {:?}", rep.findings);
    ensure!(
        rep.table_version == Some(3) && rep.ranges_checked == 3,
        "{rep:?}"
    );
    ensure!(
        rep.external_refs_checked >= 2,
        "a split's children must reference the parent: {rep:?}"
    );

    let t = ctl.table_show().await?;
    ensure!(
        t.ranges.iter().map(|r| r.id).collect::<Vec<_>>() == vec![2, 3, 1],
        "{t:?}"
    );
    let owners = ctl.owners().await?;
    ensure!(
        owners.len() == 4
            && owners
                .iter()
                .any(|o| o.range == 0 && o.retired && !o.in_table),
        "{owners:?}"
    );
    ensure!(
        owners
            .iter()
            .all(|o| o.session_state.as_deref() == Some("closed")),
        "{owners:?}"
    );
    let retired = ctl.retired().await?;
    ensure!(
        retired.len() == 1 && retired[0].range == 0 && retired[0].db_exists,
        "{retired:?}"
    );
    ensure!(
        retired[0].referenced_by.is_some(),
        "the parent Db is still referenced by the children: {retired:?}"
    );
    let logs = ctl.wal_floors().await?;
    ensure!(
        logs.len() == 3,
        "three shared WAL logs in the sample bucket: {logs:?}"
    );
    let rows = ctl.range_dump(1, None, 5).await?;
    ensure!(!rows.is_empty() && rows.len() <= 5, "{rows:?}");
    let status = ctl.range_dump(1, Some("invocation_status"), 100).await?;
    ensure!(
        !status.is_empty()
            && status
                .iter()
                .all(|r| r.table.as_deref() == Some("InvocationStatus")),
        "{status:?}"
    );
    ensure!(matches!(
        ctl.range_dump(1, Some("missing"), 1).await,
        Err(CtlError::Bad(_))
    ));

    // Read commands write nothing to the bucket.
    let d = faulty.counts().since(&before);
    ensure!(
        d.op(Op::Put) == 0 && d.op(Op::PutMultipart) == 0 && d.op(Op::Delete) == 0,
        "a read command wrote: {d:?}"
    );
    Ok(())
}

/// Broken bucket (1): the range table has a **gap** — range 1 starts one key late.
/// `verify` reports exactly one `table` error, pointing at the gap.
#[tokio::test]
async fn verify_reports_range_gap() -> Result<()> {
    let bucket = load_fixture().await?;
    let gap = table(4, "1:0").replace(
        "range=8000000000000000-ffffffffffffffff",
        "range=8000000000000001-ffffffffffffffff",
    );
    put(&bucket, "cluster.conf", &gap).await?;
    let rep = ctl_on(bucket.clone())
        .verify(VerifyOptions::default())
        .await?;
    ensure!(
        kinds(&rep.findings) == vec![("table", None)],
        "{:?}",
        rep.findings
    );
    ensure!(
        rep.findings[0]
            .detail
            .contains("gap 8000000000000000-8000000000000000"),
        "{:?}",
        rep.findings
    );
    Ok(())
}

/// Broken bucket (2): the retired parent range's Db was deleted while both children
/// still reference it through `external_dbs`. `verify` reports exactly two
/// `external_db` errors, one per child, naming the source range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_reports_external_db_pointing_at_deleted_db() -> Result<()> {
    let bucket = load_fixture().await?;
    ensure!(delete_prefix(&bucket, "ranges/0/").await? > 0);
    let rep = ctl_on(bucket.clone())
        .verify(VerifyOptions {
            open_dbs: false,
            ..VerifyOptions::default()
        })
        .await?;
    ensure!(
        kinds(&rep.findings) == vec![("external_db", Some(2)), ("external_db", Some(3))],
        "{:?}",
        rep.findings
    );
    for f in &rep.findings {
        ensure!(
            f.detail.contains("range 0") && f.detail.contains("no manifest anymore"),
            "{f:?}"
        );
    }
    Ok(())
}

/// Broken bucket (3): a split intent stuck on range 1 (the coordinator died after the
/// intent CAS). The intent's age counts from `cluster.conf`'s `last_modified`: freshly
/// written it is not stuck yet; past 10 × `QUIESCE_GRACE` `verify` reports exactly one
/// `stuck_intent` error on range 1, with the command to clear it.
#[tokio::test]
async fn verify_reports_stuck_intent() -> Result<()> {
    let bucket = load_fixture().await?;
    put(
        &bucket,
        "cluster.conf",
        &table(6, "1:0:splitting(at=c000000000000000,left=4,right=5)"),
    )
    .await?;
    let ctl = ctl_on(bucket.clone());
    let fresh = ctl
        .verify(VerifyOptions {
            open_dbs: false,
            ..VerifyOptions::default()
        })
        .await?;
    ensure!(
        fresh.clean(),
        "a freshly written intent is not stuck yet: {:?}",
        fresh.findings
    );
    let later = VerifyOptions {
        open_dbs: false,
        now_ms: VerifyOptions::default().now_ms
            + cairn_server::ctl::INTENT_STUCK_AFTER.as_millis() as u64
            + 1_000,
        ..VerifyOptions::default()
    };
    let rep = ctl.verify(later).await?;
    ensure!(
        kinds(&rep.findings) == vec![("stuck_intent", Some(1))],
        "{:?}",
        rep.findings
    );
    ensure!(
        rep.findings[0].detail.contains("intent clear 1"),
        "{:?}",
        rep.findings
    );
    Ok(())
}

/// `intent clear` runs dry by default: prints the table before/after, **not a single
/// PUT** (counted with `FaultyStore`). `--apply`: one CAS dropping the intent
/// (version + 1), cleans up the Db and owner record of the orphaned child range,
/// leaves `audit/ctl/<ts>`; afterwards `verify` is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intent_clear_is_dry_run_by_default_then_applies_one_cas() -> Result<()> {
    let bucket = load_fixture().await?;
    put(
        &bucket,
        "cluster.conf",
        &table(6, "1:0:splitting(at=c000000000000000,left=4,right=5)"),
    )
    .await?;
    // The orphan child the coordinator managed to create before dying.
    put(&bucket, "ranges/4/compacted/junk.sst", "junk").await?;
    let faulty = FaultyStore::wrap(bucket.clone());
    let ctl = ctl_on(faulty.clone());

    let before = faulty.counts();
    let dry = ctl.intent_clear(1, &repair(false, false)).await?;
    let d = faulty.counts().since(&before);
    ensure!(
        d.op(Op::Put) == 0 && d.op(Op::PutMultipart) == 0 && d.op(Op::Delete) == 0,
        "the dry run wrote: {d:?}"
    );
    ensure!(!dry.applied && dry.audit.is_none(), "{dry:?}");
    ensure!(
        dry.before.contains(":splitting(") && !dry.after.contains(":splitting("),
        "{dry:?}"
    );
    ensure!(dry.after.contains("table_version=4"), "{dry:?}");
    ensure!(get(&bucket, "cluster.conf")
        .await?
        .is_some_and(|c| c.contains("splitting(")));

    // A mismatched expected version ⇒ refused, nothing written.
    let mut stale = repair(true, false);
    stale.expect_version = Some(2);
    ensure!(matches!(
        ctl.intent_clear(1, &stale).await,
        Err(CtlError::Refused(_))
    ));
    ensure!(matches!(
        ctl.intent_clear(2, &repair(false, false)).await,
        Err(CtlError::Refused(_))
    ));

    let applied = ctl.intent_clear(1, &repair(true, false)).await?;
    ensure!(applied.applied, "{applied:?}");
    let conf = get(&bucket, "cluster.conf").await?.unwrap_or_default();
    ensure!(
        conf.contains("table_version=4") && !conf.contains("splitting("),
        "{conf}"
    );
    ensure!(
        delete_prefix(&bucket, "ranges/4/").await? == 0,
        "the orphan child Db was not cleaned up"
    );
    let audit_path = applied.audit.clone().unwrap_or_default();
    let audit = get(&bucket, &audit_path).await?.unwrap_or_default();
    ensure!(
        audit.starts_with("cairnflow-ctl-audit/1\nwho=test@ctl\n"),
        "{audit_path}: {audit}"
    );
    ensure!(
        audit.contains("object=cluster.conf")
            && audit.contains("[before]")
            && audit.contains("[after]"),
        "{audit}"
    );
    let rep = ctl
        .verify(VerifyOptions {
            open_dbs: false,
            ..VerifyOptions::default()
        })
        .await?;
    ensure!(rep.clean(), "{:?}", rep.findings);
    Ok(())
}

/// The range's owner still has a `live` session ⇒ repair commands refuse (dry runs
/// too); with `--force` they act, with a warning.
#[tokio::test]
async fn repair_refuses_live_owner_without_force() -> Result<()> {
    let bucket = load_fixture().await?;
    put(
        &bucket,
        "cluster.conf",
        &table(6, "1:0:splitting(at=c000000000000000,left=4,right=5)"),
    )
    .await?;
    put(
        &bucket,
        "owners/range-1",
        "cairnflow-owner/1\nterm=3\nsession=node-beta/1791737773693\nwal_kind=shared\n",
    )
    .await?;
    put(
        &bucket,
        "sessions/node-beta/1791737773693",
        "cairnflow-session/1\nnode=node-beta\nbeat=31\nstate=live\n",
    )
    .await?;
    let ctl = ctl_on(bucket.clone());
    match ctl.intent_clear(1, &repair(false, false)).await {
        Err(CtlError::Refused(m)) => ensure!(m.contains("live") && m.contains("--force"), "{m}"),
        other => anyhow::bail!("must refuse: {other:?}"),
    }
    let forced = ctl.intent_clear(1, &repair(false, true)).await?;
    ensure!(
        forced.warnings.iter().any(|w| w.contains("--force")),
        "{forced:?}"
    );
    Ok(())
}

/// `range force-retire`: an orphaned `ranges/<id>` directory (not in the table, no
/// tombstone) — `verify` reports `orphan_db`; a dry run writes nothing; `--apply`
/// writes the tombstone with one CAS (creating the owner record) and `verify` is
/// clean. A range still in the table is refused.
#[tokio::test]
async fn force_retire_turns_orphan_into_tombstone() -> Result<()> {
    let bucket = load_fixture().await?;
    put(&bucket, "cluster.conf", &table(6, "1:0")).await?;
    put(&bucket, "ranges/5/compacted/junk.sst", "junk").await?;
    let faulty = FaultyStore::wrap(bucket.clone());
    let ctl = ctl_on(faulty.clone());
    let opts = VerifyOptions {
        open_dbs: false,
        ..VerifyOptions::default()
    };
    let rep = ctl.verify(opts).await?;
    ensure!(
        kinds(&rep.findings) == vec![("orphan_db", Some(5))],
        "{:?}",
        rep.findings
    );

    ensure!(matches!(
        ctl.force_retire(1, &repair(true, true)).await,
        Err(CtlError::Refused(_))
    ));
    ensure!(matches!(
        ctl.force_retire(9, &repair(false, false)).await,
        Err(CtlError::Refused(_))
    ));
    let before = faulty.counts();
    let dry = ctl.force_retire(5, &repair(false, false)).await?;
    ensure!(
        faulty.counts().since(&before).op(Op::Put) == 0,
        "the dry run wrote"
    );
    ensure!(
        !dry.applied && dry.before.is_empty() && dry.after.contains("retired=1"),
        "{dry:?}"
    );

    let applied = ctl.force_retire(5, &repair(true, false)).await?;
    ensure!(applied.applied && applied.audit.is_some(), "{applied:?}");
    let rec = cairn_store::read_owner(
        &(bucket.clone() as Arc<dyn ObjectStore>),
        &ObjPath::from(""),
        PartitionId(5),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("no owner record yet"))?;
    ensure!(
        rec.retired && rec.released && rec.retired_ms.is_some(),
        "{rec:?}"
    );
    ensure!(ctl.verify(opts).await?.clean());
    ensure!(matches!(
        ctl.force_retire(5, &repair(true, false)).await,
        Err(CtlError::Refused(_))
    ));
    Ok(())
}

const ADMIN_KEY: &str = "cf_fixture_bootstrap_key_do_not_use_0001";

/// `fork --to fork1`: every range cloned from a checkpoint into a new prefix of the
/// same bucket, with the same `cluster.conf`, the cluster's configuration and the
/// owner records with their leases scrubbed. The copy `verify`s clean, the source is
/// still clean, and a node built on the copy reads back the sample bucket's finished
/// invocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_makes_a_bootable_copy() -> Result<()> {
    let bucket = load_fixture().await?;
    let ctl = ctl_on(bucket.clone());
    let rep = ctl
        .fork("fork1", "test@ctl", "cairn-ctl fork --to fork1")
        .await?;
    ensure!(rep.ranges == vec![2, 3, 1], "{rep:?}");
    ensure!(rep.owners == vec![0, 1, 2, 3], "{rep:?}");
    // GC barriers on the source (RD6): the three live ranges plus the retired
    // ancestor 0 that 2, 3 still stand on. The copy carries no barrier.
    ensure!(
        rep.barrier
            .as_ref()
            .is_some_and(|b| b.id == "fork-fork1" && b.protects == "ranges:0,1,2,3"),
        "{rep:?}"
    );
    let raw = get(&bucket, "gc-barriers/fork-fork1")
        .await?
        .unwrap_or_default();
    ensure!(raw.contains("protects=ranges:0,1,2,3"), "{raw}");
    ensure!(get(&bucket, "fork1/gc-barriers/fork-fork1")
        .await?
        .is_none());
    ensure!(
        rep.copied.iter().any(|c| c == "cluster.conf")
            && rep.copied.iter().any(|c| c == "auth/keys"),
        "{rep:?}"
    );
    let owner = get(&bucket, "fork1/owners/range-1")
        .await?
        .unwrap_or_default();
    ensure!(
        owner.contains("session=cairn-ctl-fork/0") && owner.contains("released=1"),
        "{owner}"
    );
    ensure!(
        !owner.contains("wal_segment=") && !owner.contains("wal_floor_id="),
        "{owner}"
    );
    ensure!(get(&bucket, "fork1/sessions/node-alpha/1791737773686")
        .await?
        .is_none());
    ensure!(
        matches!(ctl.fork("fork1", "t", "c").await, Err(CtlError::Refused(_))),
        "the target already has objects"
    );

    let copy = Ctl::new(ObjectStores::single(StoreLocation::from_store(
        bucket.clone(),
        "fork1",
        "memory://fork1",
    )));
    let r = copy.verify(VerifyOptions::default()).await?;
    ensure!(r.clean(), "the copy: {:?}", r.findings);
    let r = ctl.verify(VerifyOptions::default()).await?;
    ensure!(r.clean(), "the source after the fork: {:?}", r.findings);

    // A node on the copy.
    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture_dir().join("EXPECTED.json"))?)?;
    let inv = expected["invocations"]
        .as_array()
        .and_then(|a| a.iter().find(|i| i["status"] == "completed"))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("EXPECTED.json has no completed invocation"))?;
    let id = inv["id"].as_str().unwrap_or_default().to_string();
    let n = common::start_cluster_node_tweaked(
        &bucket,
        "node-fork",
        2,
        Duration::from_secs(2),
        |cfg| {
            cfg.stores = ObjectStores::single(StoreLocation::from_store(
                bucket.clone(),
                "fork1",
                "memory://fork1",
            ));
            cfg.auth = cairn_server::auth::AuthConfig {
                required: true,
                admin_key: Some(ADMIN_KEY.into()),
                cluster_secret: None,
                metrics_key: None,
                admin_key_sha256: None,
                provision_key_sha256: None,
            };
            cfg.retention_ms = None;
        },
    )
    .await?;
    let http = reqwest::Client::new();
    let url = format!("{}/invocations/{id}", n.node.url());
    let mut last = (reqwest::StatusCode::OK, serde_json::Value::Null);
    for _ in 0..200 {
        let resp = http.get(&url).bearer_auth(ADMIN_KEY).send().await?;
        let code = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        if code == reqwest::StatusCode::OK {
            last = (code, body);
            break;
        }
        last = (code, body);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    n.node.shutdown().await;
    ensure!(
        last.0 == reqwest::StatusCode::OK,
        "invocation {id} on the copy: {last:?}"
    );
    ensure!(
        last.1["status"] == inv["status"] && last.1["output"] == inv["output"],
        "{last:?} ≠ {inv}"
    );
    // The node runs on the copy, without touching the source prefix.
    let listed: Vec<_> = bucket
        .list(Some(&ObjPath::from("fork1/sessions/node-fork")))
        .try_collect()
        .await?;
    ensure!(!listed.is_empty(), "the node wrote no session under fork1/");
    let listed: Vec<_> = bucket
        .list(Some(&ObjPath::from("sessions/node-fork")))
        .try_collect()
        .await?;
    ensure!(listed.is_empty(), "node wrote into the source prefix");
    Ok(())
}

/// **RD6** — the source cluster's D-d deletes a Db the fork stands on.
///
/// 1. A source cluster with one range `0`, with durable rows in the SSTs of Db `0`.
/// 2. `fork --to fork1`: the copy's Db `0` is cloned from the source's checkpoint —
///    it **reads the SSTs of the source's `ranges/0`** through `external_dbs`.
/// 3. The source splits: `0` is sealed, cloned into `1`, the table commits `[1]`, `0`
///    becomes a tombstone old enough.
/// 4. Child `1` writes and compacts until it cuts its link to `0` on its own — the
///    source's D-d loop only considers **its own** live ranges, so `0` has no
///    referrer left.
/// 5. One D-d round on the source (`min_age` 0).
///
/// Must see: the source's Db `0` is **not** deleted (the fork puts a GC barrier on
/// the source for every range the copy stands on), and the copy reads back every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_gc_keeps_dbs_that_a_fork_stands_on() -> Result<()> {
    use cairn_server::gc::{GcConfig, GcMode};
    use cairn_store::{
        Durability, LeadershipGrant, Mutations, PartitionKey, PartitionWriter, PollIntervals,
    };
    const INV: [u8; 24] = [0x42; 24];
    async fn durable(w: &PartitionWriter) -> Result<()> {
        if let Durability::Wait(p) = w.durability()? {
            p.durable().await?;
        }
        Ok(())
    }

    let mem = Arc::new(InMemory::new());
    let polls = PollIntervals {
        manifest: Duration::from_millis(100),
        compactor: Duration::from_millis(100),
        compaction_worker: Duration::from_millis(100),
        gc: Duration::from_millis(200),
        gc_idle_skip_max: Duration::from_millis(200),
        gc_min_age: Duration::from_millis(0),
    };
    let stores = ObjectStores::single(StoreLocation::from_store(mem.clone(), "", "memory://"))
        .with_polls(polls)
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    put(
        &mem,
        "cluster.conf",
        "cairnflow-cluster/3\ntable_version=1\nnext_range_id=1\n\
         range=0000000000000000-ffffffffffffffff:0:0\n",
    )
    .await?;

    // 1. Range 0's rows are in an SST (memtable flushed), not only in the WAL.
    let mut parent = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    for i in 0..20u32 {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"parents-own");
        parent.commit(m).await?;
    }
    durable(&parent).await?;
    parent.flush_memtable().await?;

    // 2. Fork while range 0 is still alive.
    let ctl = Ctl::new(stores.clone());
    let rep = ctl
        .fork("fork1", "test@ctl", "cairn-ctl fork --to fork1")
        .await?;
    ensure!(rep.ranges == vec![0], "{rep:?}");
    ensure!(
        rep.barrier
            .as_ref()
            .is_some_and(|b| b.id == "fork-fork1" && b.protects == "ranges:0"),
        "the fork must fence exactly the ranges it stands on: {rep:?}"
    );

    // 3. The source splits 0 → 1; 0 becomes a tombstone old enough.
    parent.seal().await?;
    stores
        .clone_range(
            PartitionId(0),
            PartitionId(1),
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    put(
        &mem,
        "cluster.conf",
        "cairnflow-cluster/3\ntable_version=2\nnext_range_id=2\n\
         range=0000000000000000-ffffffffffffffff:1:1\n",
    )
    .await?;
    put(
        &mem,
        "owners/range-0",
        "cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nretired=1\n\
         wal_kind=slatedb\nretired_ms=1000\n",
    )
    .await?;

    // 4. The child cuts its link to the parent.
    let mut child = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(1)),
        stores.open_db(1).await?,
    )
    .await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut i = 20u32;
    while !stores
        .retired_db_unreferenced(PartitionId(0), &[PartitionId(1)])
        .await?
    {
        ensure!(
            std::time::Instant::now() < deadline,
            "the child did not cut its link to the parent after 90 s (compaction + GC detach)"
        );
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"child-entry");
        child.commit(m).await?;
        durable(&child).await?;
        child.flush_memtable().await?;
        i += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5. One D-d round on the source.
    let cluster =
        cairn_store::ClusterConfig::read_only(stores.main().store(), stores.main().prefix())
            .await?
            .ok_or_else(|| anyhow::anyhow!("no cluster.conf"))?;
    let cfg = GcConfig {
        min_age: Duration::ZERO,
        mode: GcMode::Delete,
        ..GcConfig::default()
    };
    let round = cairn_server::gc::run_once(&stores, &cluster, &cfg).await?;
    ensure!(
        stores.range_db_exists(PartitionId(0)).await?,
        "the source's D-d deleted Db 0, which the fork stands on: {:?}",
        round.deleted
    );

    // The copy reads back every row of range 0.
    let fork = ObjectStores::single(StoreLocation::from_store(
        mem.clone(),
        "fork1",
        "memory://fork1",
    ));
    let copy = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        fork.open_db(0).await?,
    )
    .await?;
    let rows = copy.journal_unbounded(&INV).await?;
    ensure!(
        rows.len() == 20,
        "the fork read {} rows, want 20",
        rows.len()
    );
    drop(child);
    Ok(())
}

/// The `fork` barrier id: a prefix with `/` or too long still yields a valid id, and
/// two different prefixes do not share a barrier (nobody overwrites another copy's barrier).
#[test]
fn fork_barrier_ids_are_valid_and_distinct() {
    use cairn_server::ctl::fork_barrier_id;
    assert_eq!(fork_barrier_id("incident-1"), "fork-incident-1");
    let long = "x".repeat(80);
    let ids = [
        fork_barrier_id("a/b"),
        fork_barrier_id("a-b"),
        fork_barrier_id("a b"),
        fork_barrier_id(&long),
        fork_barrier_id(&format!("{long}y")),
    ];
    for id in &ids {
        assert!(
            cairn_store::gc_barrier::check_barrier_id(id).is_ok(),
            "{id}"
        );
    }
    let distinct: std::collections::BTreeSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len(), "{ids:?}");
}

/// Stores with real directories (SeaweedFS's filer, `LocalFileSystem`) keep an empty
/// directory after every object underneath has been deleted, and a LIST with a
/// delimiter still returns it as a common prefix. S3 does not: a prefix exists only
/// while it still has objects. Found in a chaos run: D-d deleted every Db of a
/// retired range, yet `ranges/<id>/` still showed up in SeaweedFS's LIST.
///
/// 1. The sample bucket copied out to a directory, opened with `LocalFileSystem`.
/// 2. `ranges/5/compacted/rac.sst` is written then deleted — what remains of
///    `ranges/5/` is an empty directory, no objects.
///
/// Must see: `verify` is clean (no `orphan_db` for range 5) and `range_dirs` does not
/// count range 5 — an empty directory is not an orphaned object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_range_dir_left_by_filesystem_store_is_not_an_orphan() -> Result<()> {
    fn copy_dir(from: &Path, to: &Path) -> Result<()> {
        std::fs::create_dir_all(to)?;
        for e in std::fs::read_dir(from)? {
            let e = e?;
            let dest = to.join(e.file_name());
            if e.file_type()?.is_dir() {
                copy_dir(&e.path(), &dest)?;
            } else if e.file_name() != "EXPECTED.json" {
                std::fs::copy(e.path(), dest)?;
            }
        }
        Ok(())
    }
    let root = std::env::temp_dir().join(format!(
        "cairn-ctl-empty-dir-{}-{}",
        std::process::id(),
        common::wall_ms()
    ));
    copy_dir(&fixture_dir(), &root)?;
    let fs: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(&root)?);
    let junk = ObjPath::from("ranges/5/compacted/garbage.sst");
    fs.put(&junk, PutPayload::from_static(b"junk")).await?;
    fs.delete(&junk).await?;
    ensure!(
        root.join("ranges/5/compacted").is_dir(),
        "LocalFileSystem must keep the empty directory — the test no longer reproduces anything"
    );
    let ctl = ctl_on(fs);
    let rep = ctl
        .verify(VerifyOptions {
            open_dbs: false,
            ..VerifyOptions::default()
        })
        .await;
    let _ = std::fs::remove_dir_all(&root);
    let rep = rep?;
    ensure!(rep.clean(), "{:?}", rep.findings);
    ensure!(
        rep.range_dirs == 4,
        "range_dirs counts an empty directory: {}",
        rep.range_dirs
    );
    Ok(())
}

// ─────────────────────────────────────────────── the trash

/// **`trash list` / `trash restore --range`.** Range 5 (Db + owner record) moved into
/// the trash by mistake.
///
/// Must see: `list` sees two entries (`range-db 5`, `owner 5`); a dry run touches
/// nothing; `--apply` brings back both the Db and the owner record, leaves an audit
/// record, and the trash is empty; restoring again while the Db is already back in
/// its original place is refused (unless `--force`).
#[tokio::test]
async fn trash_restore_by_range_brings_back_db_and_owner() -> Result<()> {
    let mem = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    common::fabricate_retired_range(&stores, 5, Some(1)).await?;
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(5)),
            None,
        )
        .await?;
    cairn_store::delete_owner(
        &store,
        stores.main().prefix(),
        cairn_store::proof::DeleteProof::assume_owner(PartitionId(5)),
    )
    .await?;
    ensure!(!stores.range_db_exists(PartitionId(5)).await?);

    let ctl = ctl_on(store.clone());
    let listed = ctl.trash_list(cairn_store::entropy::now_ms()).await?;
    // Two entries in the same millisecond are ordered by the random part of the id.
    let mut subjects: Vec<_> = listed.iter().filter_map(|e| e.subject.clone()).collect();
    subjects.sort();
    ensure!(subjects == ["owner 5", "range-db 5"], "list: {listed:?}");

    let target = cairn_server::ctl::TrashTarget::Range(5);
    let dry = ctl.trash_restore(&target, &repair(false, false)).await?;
    ensure!(!dry.applied && dry.audit.is_none(), "{dry:?}");
    ensure!(
        !stores.range_db_exists(PartitionId(5)).await?,
        "the dry run wrote"
    );

    let done = ctl.trash_restore(&target, &repair(true, false)).await?;
    ensure!(done.applied && done.audit.is_some(), "{done:?}");
    ensure!(stores.range_db_exists(PartitionId(5)).await?, "Db not back");
    ensure!(
        store.head(&ObjPath::from("owners/range-5")).await.is_ok(),
        "owner record not back"
    );
    ensure!(ctl.trash_list(0).await?.is_empty(), "trash not empty");

    // Delete by mistake again, then range 5's Db reappears in its original place before
    // the restore.
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(5)),
            None,
        )
        .await?;
    common::fabricate_retired_range(&stores, 5, Some(1)).await?;
    match ctl.trash_restore(&target, &repair(true, false)).await {
        Err(CtlError::Refused(_)) => {}
        other => anyhow::bail!("the Db is in its original place but was not refused: {other:?}"),
    }
    Ok(())
}

/// **`trash restore --range` on a cluster with a separate WAL store** (found in a
/// review, under "leaks").
///
/// 1. The main store and the WAL store are separate; range 5's Db has objects on both
///    (`ranges/5/wal/…` on the WAL store). D-d moves the Db into the trash: **one**
///    id, two entries — the entry on the WAL store first, the entry on the main store
///    (with the manifest) last.
/// 2. A third entry of `range-db 5`, with another id — the shape of a Db moved in two
///    halves.
///
/// Must see: `restore --range 5` copies the WAL-store entry back **before** the
/// main-store entry with the same id (the manifest comes back last: the Db does not
/// "appear" while it lacks objects); `restore <id>` of one entry of a Db that has
/// other entries warns. Before the fix: the main-store entry came back first (in
/// trash listing order), with no warning.
#[tokio::test]
async fn trash_restore_brings_wal_store_entries_back_before_the_manifest() -> Result<()> {
    let main_mem = Arc::new(InMemory::new());
    let wal_mem = Arc::new(InMemory::new());
    let main: Arc<dyn ObjectStore> = main_mem.clone();
    let wal: Arc<dyn ObjectStore> = wal_mem.clone();
    let stores = ObjectStores::split(
        StoreLocation::from_store(main.clone(), "", "memory://main"),
        StoreLocation::from_store(wal.clone(), "", "memory://wal"),
    )?;
    common::fabricate_retired_range(&stores, 5, Some(1)).await?;
    put(&wal_mem, "ranges/5/wal/00000000000000000001.sst", "w").await?;
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(5)),
            None,
        )
        .await?;
    // 2.
    put(&main_mem, "ranges/5/compacted/stray.sst", "s").await?;
    let stray = cairn_store::trash::EntryId::new(cairn_store::entropy::now_ms() + 1);
    cairn_store::trash::Trash::new(main.clone(), &ObjPath::from(""))
        .move_into(
            &stray,
            &cairn_store::trash::Subject::RangeDb(5),
            "test: the second move",
            &[ObjPath::from("ranges/5/compacted/stray.sst")],
        )
        .await?;
    let ctl = Ctl::new(stores.clone()).with_clock(Arc::new(common::JumpClock::default()));

    let one = ctl
        .trash_restore(
            &cairn_server::ctl::TrashTarget::Entry(stray.as_str().to_string()),
            &repair(false, false),
        )
        .await?;
    ensure!(
        one.warnings.iter().any(|w| w.contains("--range")),
        "restoring one entry of a Db with other entries must warn: {one:?}"
    );

    let done = ctl
        .trash_restore(
            &cairn_server::ctl::TrashTarget::Range(5),
            &repair(true, false),
        )
        .await?;
    ensure!(done.applied, "{done:?}");
    let lines: Vec<&str> = done.after.lines().collect();
    let at = |store: &str| {
        lines
            .iter()
            .position(|l| !l.starts_with(stray.as_str()) && l.contains(&format!("[{store}]")))
    };
    let (w, m) = (at("wal"), at("main"));
    ensure!(
        w.is_some() && m.is_some() && w < m,
        "the WAL-store entry must come back before the main-store entry with the same id:\n{}",
        done.after
    );
    ensure!(stores.range_db_exists(PartitionId(5)).await?, "Db not back");
    Ok(())
}

// ───────────────────────────────────── the quiet-lease gate

/// A `live` (or `state`) session record with an arbitrary `knows=` — an old binary
/// (`knows` < the gate step) or a new one.
fn session_body(node: &str, knows: Option<u32>, state: &str, mode: Option<&str>) -> String {
    let mut s = format!("cairnflow-session/1\nnode={node}\nbeat=3\nstate={state}\n");
    if let Some(k) = knows {
        s.push_str(&format!("knows={k}\n"));
    }
    if let Some(m) = mode {
        s.push_str(&format!("mode={m}\n"));
    }
    s
}

/// `cluster enable-quiet-lease` ("turning the feature on"):
///
/// 1. a `live` session of an old binary (`knows` < the gate step, including no
///    `knows=` at all) ⇒ refused, nothing written; an ended session does not block;
/// 2. every session knows the gate step ⇒ a dry run prints the change without writing;
/// 3. `--apply` ⇒ `min_binary` = the gate step, `control/quiet-lease` = `on`, one
///    audit record; running it again writes nothing more;
/// 4. an old binary no longer passes `ensure_known` (does not start), a new one passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_quiet_lease_checks_knows_then_raises_min_binary_and_opens_the_gate() -> Result<()> {
    use cairn_store::migrate::{self, QUIET_LEASE};
    let bucket = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let ctl = ctl_on(store.clone());
    put(
        &bucket,
        "cluster-version",
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n",
    )
    .await?;
    put(
        &bucket,
        "sessions/new/1",
        &session_body("new", Some(QUIET_LEASE), "live", None),
    )
    .await?;
    put(
        &bucket,
        "sessions/gone/1",
        &session_body("gone", Some(1), "closed", None),
    )
    .await?;
    for old in [Some(QUIET_LEASE - 1), None] {
        put(
            &bucket,
            "sessions/old/1",
            &session_body("old", old, "live", None),
        )
        .await?;
        match ctl.enable_quiet_lease(&repair(true, false)).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old/1"), "{msg}"),
            other => anyhow::bail!("knows={old:?}: must refuse, got {other:?}"),
        }
        ensure!(
            get(&bucket, "control/quiet-lease").await?.is_none(),
            "refused but still wrote the gate"
        );
        ensure!(
            get(&bucket, "cluster-version").await?.as_deref()
                == Some("cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n"),
            "refused but still changed cluster-version"
        );
    }
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(2), "expired", None),
    )
    .await?;
    let dry = ctl.enable_quiet_lease(&repair(false, false)).await?;
    ensure!(!dry.applied && dry.after.contains("state=on"), "{dry:?}");
    ensure!(
        get(&bucket, "control/quiet-lease").await?.is_none(),
        "the dry run wrote"
    );
    let rep = ctl.enable_quiet_lease(&repair(true, false)).await?;
    ensure!(rep.applied && rep.audit.is_some(), "{rep:?}");
    let cv = migrate::read_cluster_version(&store, &ObjPath::default())
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    ensure!(cv.min_binary == QUIET_LEASE && cv.version == 2, "{cv:?}");
    let gate = get(&bucket, "control/quiet-lease")
        .await?
        .unwrap_or_default();
    ensure!(cairn_store::quiet_lease::is_on(gate.as_bytes()), "{gate}");
    let again = ctl.enable_quiet_lease(&repair(true, false)).await?;
    ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
    // Delegation is turned on separately: without `--delegation` the gate only turns on
    // the lone-node mode; `--delegation` also writes `delegation=on`; running again
    // without the flag does not turn it off. This bucket has none of the delegation
    // evidence: turn it on with the override `--accept-missing-evidence` (the evidence
    // gate has its own tests below).
    ensure!(
        !cairn_store::quiet_lease::delegation_on(gate.as_bytes()),
        "gate turned on without --delegation but delegation is on: {gate}"
    );
    let deleg = ctl
        .enable_quiet_lease_with(&repair(true, false), true, true)
        .await?;
    ensure!(deleg.applied, "{deleg:?}");
    let gate = get(&bucket, "control/quiet-lease")
        .await?
        .unwrap_or_default();
    ensure!(
        cairn_store::quiet_lease::delegation_on(gate.as_bytes()),
        "{gate}"
    );
    let keep = ctl.enable_quiet_lease(&repair(true, false)).await?;
    ensure!(!keep.applied, "{keep:?}");
    let gate = get(&bucket, "control/quiet-lease")
        .await?
        .unwrap_or_default();
    ensure!(
        cairn_store::quiet_lease::delegation_on(gate.as_bytes()),
        "running again without --delegation turned delegation off: {gate}"
    );
    // An old binary (knowing up to the step before the gate) no longer starts; this binary passes.
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), QUIET_LEASE - 1)
            .await
            .is_err(),
        "an old binary still passes cluster-version after the gate is on"
    );
    migrate::ensure_known(&store, &ObjPath::default(), migrate::KNOWS).await?;
    let st = ctl.cluster_status(1_800_000_000_000).await?;
    ensure!(st.quiet_lease && st.min_binary == QUIET_LEASE, "{st:?}");
    Ok(())
}

/// `cluster enable-call-prefix`, on a hand-built bucket:
///
/// 1. a `live` session of a binary that does not know the last gate step — including
///    a binary knowing `calls/prefix` (4) but not `calls/placement` (5), and one with
///    no `knows=` — ⇒ refused, `cluster-version` unchanged byte for byte;
/// 2. a dry run prints `call_prefix=on`, `call_placement=on` without writing;
/// 3. `--apply` ⇒ `min_binary` = the last gate step **and** two lines in
///    `cluster-version` (no new object), one audit record; an unknown line from a
///    newer binary is kept; running again writes nothing;
/// 4. `lower-min-binary` does not lower `min_binary` below the last gate step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_call_prefix_checks_knows_then_writes_the_line_into_cluster_version() -> Result<()> {
    use cairn_store::migrate::{self, CALL_PLACEMENT, CALL_PREFIX};
    let bucket = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let ctl = ctl_on(store.clone());
    let before = "cairnflow-cluster-version/1\nversion=2\nmin_binary=3\nfuture=1\n";
    put(&bucket, "cluster-version", before).await?;
    put(
        &bucket,
        "sessions/new/1",
        &session_body("new", Some(CALL_PLACEMENT), "live", None),
    )
    .await?;
    for old in [Some(CALL_PREFIX), Some(CALL_PREFIX - 1), None] {
        put(
            &bucket,
            "sessions/old/1",
            &session_body("old", old, "live", None),
        )
        .await?;
        match ctl.enable_call_prefix(&repair(true, false)).await {
            Err(CtlError::Refused(msg)) => ensure!(msg.contains("old/1"), "{msg}"),
            other => anyhow::bail!("knows={old:?}: must refuse, got {other:?}"),
        }
        ensure!(
            get(&bucket, "cluster-version").await?.as_deref() == Some(before),
            "refused but still changed cluster-version"
        );
    }
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(2), "closed", None),
    )
    .await?;
    let dry = ctl.enable_call_prefix(&repair(false, false)).await?;
    ensure!(
        !dry.applied
            && dry.after.contains("call_prefix=on")
            && dry.after.contains("call_placement=on"),
        "{dry:?}"
    );
    ensure!(get(&bucket, "cluster-version").await?.as_deref() == Some(before));
    let rep = ctl.enable_call_prefix(&repair(true, false)).await?;
    ensure!(rep.applied && rep.audit.is_some(), "{rep:?}");
    let cv = migrate::read_cluster_version(&store, &ObjPath::default())
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    ensure!(
        cv.call_prefix_on()
            && cv.call_placement_on()
            && cv.min_binary == CALL_PLACEMENT
            && cv.version == 2,
        "{cv:?}"
    );
    ensure!(cv.extra == vec!["future=1".to_string()], "{cv:?}");
    ensure!(
        get(&bucket, "control/quiet-lease").await?.is_none(),
        "the batching gate must not touch the quiet-lease gate"
    );
    let again = ctl.enable_call_prefix(&repair(true, false)).await?;
    ensure!(!again.applied && !again.warnings.is_empty(), "{again:?}");
    let st = ctl.cluster_status(1_800_000_000_000).await?;
    ensure!(
        st.call_prefix
            && st.call_prefix_knows == CALL_PREFIX
            && st.call_placement
            && st.call_placement_knows == CALL_PLACEMENT,
        "{st:?}"
    );
    ctl.lower_min_binary(&repair(true, false)).await?;
    let cv = migrate::read_cluster_version(&store, &ObjPath::default())
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    ensure!(
        cv.min_binary == CALL_PLACEMENT && cv.call_prefix_on() && cv.call_placement_on(),
        "{cv:?}"
    );
    Ok(())
}

/// A cluster that turned on `calls/prefix` with a binary at migration step 4 (an
/// earlier build: `call_prefix=on`, `min_binary=4`, no `call_placement` line) runs
/// the **same** `cluster enable-call-prefix` again to also turn on `calls/placement`:
///
/// 1. a session with `knows=4` remains ⇒ refused, not a byte changed — that binary
///    ignores `placement` and would replay `spread` calls by the batching rule;
/// 2. every session knows step 5 ⇒ `min_binary=5`, `call_placement=on`, the
///    `call_prefix=on` line kept;
/// 3. `lower-min-binary` does not go below 5; a step-4 binary no longer passes
///    `ensure_known`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_call_prefix_finishes_call_placement_on_a_prefix_cluster() -> Result<()> {
    use cairn_store::migrate::{self, CALL_PLACEMENT, CALL_PREFIX};
    let bucket = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let ctl = ctl_on(store.clone());
    let before = "cairnflow-cluster-version/1\nversion=2\nmin_binary=4\ncall_prefix=on\n";
    put(&bucket, "cluster-version", before).await?;
    let cv = migrate::read_cluster_version(&store, &ObjPath::default())
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    ensure!(cv.call_prefix_on() && !cv.call_placement_on(), "{cv:?}");
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(CALL_PREFIX), "live", None),
    )
    .await?;
    match ctl.enable_call_prefix(&repair(true, false)).await {
        Err(CtlError::Refused(msg)) => ensure!(msg.contains("old/1"), "{msg}"),
        other => anyhow::bail!("knows=4: must refuse, got {other:?}"),
    }
    ensure!(get(&bucket, "cluster-version").await?.as_deref() == Some(before));
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(CALL_PLACEMENT), "live", None),
    )
    .await?;
    let rep = ctl.enable_call_prefix(&repair(true, false)).await?;
    ensure!(rep.applied, "{rep:?}");
    let cv = migrate::read_cluster_version(&store, &ObjPath::default())
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    ensure!(
        cv.call_prefix_on() && cv.call_placement_on() && cv.min_binary == CALL_PLACEMENT,
        "{cv:?}"
    );
    ctl.lower_min_binary(&repair(true, false)).await?;
    let cv = migrate::read_cluster_version(&store, &ObjPath::default())
        .await?
        .map(|(cv, _)| cv)
        .unwrap_or_default();
    ensure!(cv.min_binary == CALL_PLACEMENT, "{cv:?}");
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), CALL_PREFIX)
            .await
            .is_err(),
        "a step-4 binary still passes cluster-version after calls/placement is on"
    );
    migrate::ensure_known(&store, &ObjPath::default(), CALL_PLACEMENT).await?;
    Ok(())
}

/// `cluster enable-streams` on a cluster that already has both call gates:
///
/// 1. a session with `knows=5` remains ⇒ refused, not a byte changed — that binary
///    leaks `Streams` rows on purge;
/// 2. every session knows step 6 ⇒ `min_binary=6`, `streams=on`, both call-gate lines
///    kept;
/// 3. `lower-min-binary` does not go below 6; a step-5 binary no longer passes `ensure_known`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_streams_raises_min_binary_and_lower_keeps_the_floor() -> Result<()> {
    use cairn_store::migrate::{self, CALL_PLACEMENT, STREAMS_TABLE};
    let bucket = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let ctl = ctl_on(store.clone());
    let before =
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=5\ncall_prefix=on\ncall_placement=on\n";
    put(&bucket, "cluster-version", before).await?;
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(CALL_PLACEMENT), "live", None),
    )
    .await?;
    match ctl.enable_streams(&repair(true, false)).await {
        Err(CtlError::Refused(msg)) => ensure!(msg.contains("old/1"), "{msg}"),
        other => anyhow::bail!("knows=5: must refuse, got {other:?}"),
    }
    ensure!(get(&bucket, "cluster-version").await?.as_deref() == Some(before));
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(STREAMS_TABLE), "live", None),
    )
    .await?;
    let rep = ctl.enable_streams(&repair(true, false)).await?;
    ensure!(rep.applied, "{rep:?}");
    let cv_now = || async {
        Ok::<_, anyhow::Error>(
            migrate::read_cluster_version(&store, &ObjPath::default())
                .await?
                .map(|(cv, _)| cv)
                .unwrap_or_default(),
        )
    };
    let cv = cv_now().await?;
    ensure!(
        cv.streams_on()
            && cv.call_prefix_on()
            && cv.call_placement_on()
            && cv.min_binary == STREAMS_TABLE,
        "{cv:?}"
    );
    ctl.lower_min_binary(&repair(true, false)).await?;
    let cv = cv_now().await?;
    ensure!(cv.min_binary == STREAMS_TABLE && cv.streams_on(), "{cv:?}");
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), CALL_PLACEMENT)
            .await
            .is_err(),
        "a step-5 binary still passes cluster-version after streams/table is on"
    );
    migrate::ensure_known(&store, &ObjPath::default(), STREAMS_TABLE).await?;
    Ok(())
}

/// **CG-13** — `cluster enable-schedules` on a cluster that already has three gates:
///
/// 1. a session with `knows=6` remains ⇒ refused, not a byte changed — that binary,
///    leading a range with schedules, would fire nothing;
/// 2. every session knows step 7 ⇒ `min_binary=7`, `schedules=on`, the other gate
///    lines kept;
/// 3. `lower-min-binary` does not go below 7; a step-6 binary no longer passes `ensure_known`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_schedules_raises_min_binary_and_lower_keeps_the_floor() -> Result<()> {
    use cairn_store::migrate::{self, SCHEDULES_TABLE, STREAMS_TABLE};
    let bucket = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let ctl = ctl_on(store.clone());
    let before =
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=6\ncall_prefix=on\ncall_placement=on\nstreams=on\n";
    put(&bucket, "cluster-version", before).await?;
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(STREAMS_TABLE), "live", None),
    )
    .await?;
    match ctl.enable_schedules(&repair(true, false)).await {
        Err(CtlError::Refused(msg)) => ensure!(msg.contains("old/1"), "{msg}"),
        other => anyhow::bail!("knows=6: must refuse, got {other:?}"),
    }
    ensure!(get(&bucket, "cluster-version").await?.as_deref() == Some(before));
    put(
        &bucket,
        "sessions/old/1",
        &session_body("old", Some(SCHEDULES_TABLE), "live", None),
    )
    .await?;
    let rep = ctl.enable_schedules(&repair(true, false)).await?;
    ensure!(rep.applied, "{rep:?}");
    let cv_now = || async {
        Ok::<_, anyhow::Error>(
            migrate::read_cluster_version(&store, &ObjPath::default())
                .await?
                .map(|(cv, _)| cv)
                .unwrap_or_default(),
        )
    };
    let cv = cv_now().await?;
    ensure!(
        cv.schedules_on()
            && cv.streams_on()
            && cv.call_prefix_on()
            && cv.call_placement_on()
            && cv.min_binary == SCHEDULES_TABLE,
        "{cv:?}"
    );
    ctl.lower_min_binary(&repair(true, false)).await?;
    let cv = cv_now().await?;
    ensure!(
        cv.min_binary == SCHEDULES_TABLE && cv.schedules_on(),
        "{cv:?}"
    );
    ensure!(
        migrate::ensure_known(&store, &ObjPath::default(), STREAMS_TABLE)
            .await
            .is_err(),
        "a step-6 binary still passes cluster-version after schedules/table is on"
    );
    migrate::ensure_known(&store, &ObjPath::default(), SCHEDULES_TABLE).await?;
    Ok(())
}

/// `cluster enable-quiet-lease --apply` waits past a node's boot bound
/// ([`cairn_server::ctl::BOOT_WAIT`]) after setting `min_binary`, and only then
/// checks the sessions one last time.
///
/// The interleaving reproduced:
/// 1. Every session knows the gate step; `cluster-version` has `min_binary=2`.
/// 2. An old binary reads `cluster-version` (passes the `min_binary` check) then
///    stands in its boot path — no session record yet.
/// 3. The `--apply` command (a hand-driven `ManualClock`) checks the sessions a first
///    time (clean), CASes `min_binary` = the gate step.
/// 4. The test cuts in here: the old binary opens a session (`knows` = 2) — inside the
///    wait.
/// 5. The clock passes `BOOT_WAIT`; the command checks again.
///
/// Must see: while the clock has not passed `BOOT_WAIT` the gate is **not** written
/// yet (before the wait existed: written right after the second check, and the old
/// binary at step 4 slipped through); the command refuses, naming the old session,
/// the gate absent. A run without an old binary: the gate is turned on after the clock
/// passes `BOOT_WAIT`, the report records the waiting time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_quiet_lease_waits_out_the_boot_bound_before_the_last_check() -> Result<()> {
    use cairn_store::{clock::ManualClock, migrate::QUIET_LEASE};
    for late_old_binary in [true, false] {
        let bucket = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = bucket.clone();
        let clock = Arc::new(ManualClock::new(1_800_000_000_000));
        let ctl = Arc::new(ctl_on(store.clone()).with_clock(clock.clone()));
        let version = "cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n";
        put(&bucket, "cluster-version", version).await?;
        put(
            &bucket,
            "sessions/new/1",
            &session_body("new", Some(QUIET_LEASE), "live", None),
        )
        .await?;
        let run = {
            let ctl = ctl.clone();
            tokio::spawn(async move { ctl.enable_quiet_lease(&repair(true, false)).await })
        };
        // Step 3 done: `min_binary` is set.
        let t0 = std::time::Instant::now();
        while get(&bucket, "cluster-version").await?.as_deref() == Some(version) {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the command did not set min_binary"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        ensure!(
            get(&bucket, "control/quiet-lease").await?.is_none(),
            "the gate was written before the clock passed BOOT_WAIT"
        );
        ensure!(
            !run.is_finished(),
            "the command finished before the clock passed BOOT_WAIT"
        );
        if late_old_binary {
            put(
                &bucket,
                "sessions/old/1",
                &session_body("old", Some(QUIET_LEASE - 1), "live", None),
            )
            .await?;
        }
        clock.advance(cairn_server::ctl::BOOT_WAIT - Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(300)).await;
        ensure!(!run.is_finished(), "command done before BOOT_WAIT");
        clock.advance(Duration::from_secs(1));
        let out = tokio::time::timeout(Duration::from_secs(10), run).await??;
        if late_old_binary {
            match out {
                Err(CtlError::Refused(msg)) => ensure!(msg.contains("old/1"), "{msg}"),
                other => anyhow::bail!(
                    "an old binary opened a session during the wait and the command did not refuse: {other:?}"
                ),
            }
            ensure!(
                get(&bucket, "control/quiet-lease").await?.is_none(),
                "refused but still wrote the gate"
            );
        } else {
            let rep = out?;
            ensure!(rep.applied, "{rep:?}");
            ensure!(
                rep.cleanup.iter().any(|c| c.contains("waited")),
                "the report does not record the waiting time: {rep:?}"
            );
            let gate = get(&bucket, "control/quiet-lease")
                .await?
                .unwrap_or_default();
            ensure!(cairn_store::quiet_lease::is_on(gate.as_bytes()), "{gate}");
        }
    }
    Ok(())
}

/// `cluster enable-quiet-lease --apply` probes the store before turning the gate on
/// (the G18 contract of `docs/reliability/irreversible.md`): a store whose
/// `If-None-Match` GET returns a stale `304` (a proxy cache, a weakly consistent store
/// — `Action::StaleNotModified` on `control/probe/`) would make the lone-node mode
/// renew from a record that has already been challenged.
///
/// 1. Every session knows the gate step; the store has a caching layer in front of it
///    returning `304` for every conditional GET under `control/probe/`.
/// 2. `--apply` ⇒ refused, the reason mentions a stale 304; `cluster-version` and
///    `control/quiet-lease` unchanged.
/// 3. Remove the caching layer ⇒ `--apply` turns the gate on, the report records the
///    probe that passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_quiet_lease_refuses_a_store_that_serves_a_stale_304() -> Result<()> {
    use cairn_store::{
        migrate::QUIET_LEASE,
        testkit::{Action, OpSet, PathMatch, Rule},
    };
    let bucket = Arc::new(InMemory::new());
    let faulty = FaultyStore::wrap(bucket.clone());
    let ctl = ctl_on(faulty.clone());
    let version = "cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n";
    put(&bucket, "cluster-version", version).await?;
    put(
        &bucket,
        "sessions/new/1",
        &session_body("new", Some(QUIET_LEASE), "live", None),
    )
    .await?;
    let cache = faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("control/probe/".into()),
        Action::StaleNotModified,
    ));
    match ctl.enable_quiet_lease(&repair(true, false)).await {
        Err(CtlError::Refused(msg)) => ensure!(msg.contains("STALE 304"), "{msg}"),
        other => anyhow::bail!("stale 304 from the store, yet no refusal: {other:?}"),
    }
    ensure!(
        get(&bucket, "control/quiet-lease").await?.is_none(),
        "refused but still wrote the gate"
    );
    ensure!(
        get(&bucket, "cluster-version").await?.as_deref() == Some(version),
        "refused but still changed cluster-version"
    );
    faulty.remove_rule(cache);
    let rep = ctl.enable_quiet_lease(&repair(true, false)).await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        rep.cleanup
            .iter()
            .any(|c| c.contains("control/probe/") && c.contains("passed")),
        "the report does not record the probe: {rep:?}"
    );
    Ok(())
}

/// **The downgrade runbook.** Gate on ⇒ `lower-min-binary` refuses;
/// `disable-quiet-lease` writes `off`; a session not yet ended in `mode=solo` remains
/// ⇒ still refused (an old binary would apply the silence rule to it); every session
/// back on `direct` ⇒ `min_binary` back to `version` and an old binary can start again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiet_lease_downgrade_needs_gate_off_and_every_session_direct() -> Result<()> {
    use cairn_store::migrate::{self, QUIET_LEASE};
    let bucket = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let ctl = ctl_on(store.clone());
    put(
        &bucket,
        "cluster-version",
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n",
    )
    .await?;
    put(
        &bucket,
        "sessions/a/1",
        &session_body("a", Some(QUIET_LEASE), "live", Some("solo")),
    )
    .await?;
    ctl.enable_quiet_lease(&repair(true, false)).await?;
    ensure!(
        matches!(
            ctl.lower_min_binary(&repair(true, false)).await,
            Err(CtlError::Refused(_))
        ),
        "the gate is still on but min_binary was lowered"
    );
    let off = ctl.disable_quiet_lease(&repair(true, false)).await?;
    ensure!(off.applied, "{off:?}");
    let gate = get(&bucket, "control/quiet-lease")
        .await?
        .unwrap_or_default();
    ensure!(!cairn_store::quiet_lease::is_on(gate.as_bytes()), "{gate}");
    match ctl.lower_min_binary(&repair(true, false)).await {
        Err(CtlError::Refused(msg)) => ensure!(msg.contains("a/1"), "{msg}"),
        other => anyhow::bail!("session still solo: must refuse, got {other:?}"),
    }
    put(
        &bucket,
        "sessions/a/1",
        &session_body("a", Some(QUIET_LEASE), "live", None),
    )
    .await?;
    let rep = ctl.lower_min_binary(&repair(true, false)).await?;
    ensure!(rep.applied, "{rep:?}");
    migrate::ensure_known(&store, &ObjPath::default(), QUIET_LEASE - 1).await?;
    Ok(())
}

// ──────────────────────── the evidence gate of `--delegation`

/// A bucket on which `enable-quiet-lease` can be turned on: `cluster-version` at the
/// last data step, one `live` session knowing the gate step.
async fn quiet_ready_bucket() -> Result<Arc<InMemory>> {
    let bucket = Arc::new(InMemory::new());
    put(
        &bucket,
        "cluster-version",
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=2\n",
    )
    .await?;
    put(
        &bucket,
        "sessions/new/1",
        &session_body("new", Some(cairn_store::migrate::QUIET_LEASE), "live", None),
    )
    .await?;
    Ok(bucket)
}

/// `control/quiet-lease`, decoded (absent or broken ⇒ an error).
async fn gate_of(bucket: &InMemory) -> Result<cairn_store::quiet_lease::QuietLease> {
    let raw = get(bucket, "control/quiet-lease")
        .await?
        .ok_or_else(|| anyhow::anyhow!("control/quiet-lease absent"))?;
    cairn_store::quiet_lease::QuietLease::decode(raw.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("control/quiet-lease does not decode: {raw}"))
}

/// `enable-quiet-lease --delegation` while `control/quiet-lease` has not recorded all
/// five evidence items (a)–(e):
///
/// 1. a bucket without the gate: a dry run and `--apply` both refuse, the reason lists
///    the missing items; `cluster-version` and `control/quiet-lease` unchanged byte for byte;
/// 2. the gate already turned on in lone-node mode with four items (`d` missing):
///    `--apply` refuses, the reason names exactly `d`, the object unchanged byte for
///    byte, delegation still off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegation_refuses_without_evidence_and_writes_nothing() -> Result<()> {
    let bucket = quiet_ready_bucket().await?;
    let ctl = ctl_on(bucket.clone());
    let version = get(&bucket, "cluster-version").await?;
    for apply in [false, true] {
        match ctl
            .enable_quiet_lease_with(&repair(apply, false), true, false)
            .await
        {
            Err(CtlError::Refused(msg)) => ensure!(
                ["(a)", "(b)", "(c)", "(d)", "(e)"]
                    .iter()
                    .all(|k| msg.contains(k)),
                "the reason does not list the missing items: {msg}"
            ),
            other => anyhow::bail!("apply={apply}: missing evidence must refuse, got {other:?}"),
        }
        ensure!(
            get(&bucket, "control/quiet-lease").await?.is_none(),
            "refused but still wrote the gate"
        );
        ensure!(
            get(&bucket, "cluster-version").await? == version,
            "refused but still changed cluster-version"
        );
    }
    ctl.enable_quiet_lease(&repair(true, false)).await?;
    for k in ['a', 'b', 'c', 'e'] {
        ctl.record_evidence(k, &format!("item {k} green"), &repair(true, false))
            .await?;
    }
    let before = get(&bucket, "control/quiet-lease").await?;
    match ctl
        .enable_quiet_lease_with(&repair(true, false), true, false)
        .await
    {
        Err(CtlError::Refused(msg)) => ensure!(
            msg.contains("(d)") && !msg.contains("(a)"),
            "the reason must name exactly the missing item (d): {msg}"
        ),
        other => anyhow::bail!("(d) missing must refuse, got {other:?}"),
    }
    ensure!(
        get(&bucket, "control/quiet-lease").await? == before,
        "refused but still changed the gate"
    );
    ensure!(
        !cairn_store::quiet_lease::delegation_on(before.unwrap_or_default().as_bytes()),
        "delegation on while (d) is missing"
    );
    Ok(())
}

/// `record-evidence`:
///
/// 1. an unknown item or an empty note ⇒ bad arguments, nothing written;
/// 2. a dry run prints the object as it would be after writing, without writing;
/// 3. `--apply` on a bucket without the gate ⇒ creates `control/quiet-lease` with
///    `state=off` carrying that item, one audit record — recording evidence **turns
///    nothing on**: the gate off, delegation off;
/// 4. recording another item keeps the old one; `cluster status` lists the items
///    present and missing;
/// 5. turning it on and then `disable-quiet-lease` keeps the evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_evidence_is_dry_run_by_default_and_turns_nothing_on() -> Result<()> {
    let bucket = quiet_ready_bucket().await?;
    let ctl = ctl_on(bucket.clone());
    for (k, note) in [('f', "x"), ('a', ""), ('a', "  ")] {
        match ctl.record_evidence(k, note, &repair(true, false)).await {
            Err(CtlError::Bad(_)) => {}
            other => {
                anyhow::bail!("item {k:?} note {note:?}: must be bad arguments, got {other:?}")
            }
        }
    }
    ensure!(
        get(&bucket, "control/quiet-lease").await?.is_none(),
        "bad arguments but still wrote"
    );
    let note = "M1 --release 8f00aa1, cargo test -p cairn-model -- --ignored, 2026-10-23";
    let dry = ctl
        .record_evidence('a', note, &repair(false, false))
        .await?;
    ensure!(!dry.applied && dry.audit.is_none(), "{dry:?}");
    let after = cairn_store::quiet_lease::QuietLease::decode(dry.after.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("the after line does not decode: {}", dry.after))?;
    ensure!(
        after.evidence.get('a').map(|e| e.note.as_str()) == Some(note),
        "{dry:?}"
    );
    ensure!(
        get(&bucket, "control/quiet-lease").await?.is_none(),
        "the dry run wrote"
    );
    let rep = ctl.record_evidence('a', note, &repair(true, false)).await?;
    ensure!(rep.applied && rep.audit.is_some(), "{rep:?}");
    let gate = gate_of(&bucket).await?;
    ensure!(
        !gate.on && !gate.delegation,
        "recording evidence turned something on: {gate:?}"
    );
    let a = gate.evidence.get('a').cloned();
    ensure!(
        a.as_ref()
            .is_some_and(|e| e.note == note && e.by == "test@ctl" && e.at_ms == 1_800_000_000_000),
        "{gate:?}"
    );
    ctl.record_evidence('c', "chaos 30 min, run 20545", &repair(true, false))
        .await?;
    let gate = gate_of(&bucket).await?;
    ensure!(
        gate.evidence.get('a').cloned() == a && gate.evidence.missing() == vec!['b', 'd', 'e'],
        "{gate:?}"
    );
    let st = ctl.cluster_status(1_800_000_000_000).await?;
    ensure!(
        !st.quiet_lease && !st.delegation && st.evidence_missing == vec!['b', 'd', 'e'],
        "{st:?}"
    );
    ensure!(
        st.evidence
            .iter()
            .any(|e| e.item == 'a' && e.note.as_deref() == Some(note)),
        "{st:?}"
    );
    ctl.enable_quiet_lease(&repair(true, false)).await?;
    ctl.disable_quiet_lease(&repair(true, false)).await?;
    let gate = gate_of(&bucket).await?;
    ensure!(
        !gate.on && gate.evidence.missing() == vec!['b', 'd', 'e'],
        "disable-quiet-lease lost the evidence: {gate:?}"
    );
    Ok(())
}

/// All five items ⇒ `--delegation` turns delegation on, with no override mark; an
/// item broken on the bucket (someone edited it by hand) reads leniently as missing
/// and blocks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegation_turns_on_with_all_five_items() -> Result<()> {
    let bucket = quiet_ready_bucket().await?;
    let ctl = ctl_on(bucket.clone());
    for k in cairn_store::quiet_lease::EVIDENCE_ITEMS {
        ctl.record_evidence(k, &format!("item {k}: green"), &repair(true, false))
            .await?;
    }
    let rep = ctl
        .enable_quiet_lease_with(&repair(true, false), true, false)
        .await?;
    ensure!(rep.applied, "{rep:?}");
    let gate = gate_of(&bucket).await?;
    ensure!(
        gate.on
            && gate.delegation
            && gate.delegation_unproven.is_none()
            && gate.evidence.complete(),
        "{gate:?}"
    );
    let st = ctl.cluster_status(1_800_000_000_000).await?;
    ensure!(
        st.delegation && st.evidence_missing.is_empty() && st.delegation_unproven.is_none(),
        "{st:?}"
    );
    // A hand edit breaks item (b): read leniently — the object still reads, (b) becomes missing.
    let raw = get(&bucket, "control/quiet-lease")
        .await?
        .unwrap_or_default();
    let broken = raw.replacen(" b=", " b=x", 1);
    ensure!(broken != raw, "{raw}");
    put(&bucket, "control/quiet-lease", &broken).await?;
    let st = ctl.cluster_status(1_800_000_000_000).await?;
    ensure!(st.quiet_lease && st.evidence_missing == vec!['b'], "{st:?}");
    put(
        &bucket,
        "control/quiet-lease",
        &broken.replace("delegation=on\n", ""),
    )
    .await?;
    match ctl
        .enable_quiet_lease_with(&repair(true, false), true, false)
        .await
    {
        Err(CtlError::Refused(msg)) => ensure!(msg.contains("(b)"), "{msg}"),
        other => anyhow::bail!("broken item (b) must refuse, got {other:?}"),
    }
    Ok(())
}

/// `--delegation --accept-missing-evidence` (for staging, chaos and soak runs, which
/// must run with delegation on before the evidence exists) turns delegation on while
/// items are missing, warns, and records in the object the missing items, who, and
/// when; `cluster status` shows that mark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accept_missing_evidence_turns_on_and_records_what_was_missing() -> Result<()> {
    let bucket = quiet_ready_bucket().await?;
    let ctl = ctl_on(bucket.clone());
    for k in ['a', 'b'] {
        ctl.record_evidence(k, "pass", &repair(true, false)).await?;
    }
    let rep = ctl
        .enable_quiet_lease_with(&repair(true, false), true, true)
        .await?;
    ensure!(rep.applied, "{rep:?}");
    ensure!(
        rep.warnings
            .iter()
            .any(|w| w.contains("(c)") && w.contains("(d)") && w.contains("(e)")),
        "no warning about missing items: {rep:?}"
    );
    let gate = gate_of(&bucket).await?;
    ensure!(gate.on && gate.delegation, "{gate:?}");
    let u = gate
        .delegation_unproven
        .clone()
        .ok_or_else(|| anyhow::anyhow!("no override mark recorded: {gate:?}"))?;
    ensure!(
        u.missing == vec!['c', 'd', 'e'] && u.by == "test@ctl" && u.at_ms == 1_800_000_000_000,
        "{u:?}"
    );
    let st = ctl.cluster_status(1_800_000_000_000).await?;
    ensure!(
        st.delegation
            && st.delegation_unproven.is_some()
            && st.evidence_missing == vec!['c', 'd', 'e'],
        "{st:?}"
    );
    // All five items now: running `--delegation` again (without the override) removes the mark.
    for k in ['c', 'd', 'e'] {
        ctl.record_evidence(k, "pass", &repair(true, false)).await?;
    }
    let rep = ctl
        .enable_quiet_lease_with(&repair(true, false), true, false)
        .await?;
    ensure!(rep.applied, "{rep:?}");
    let gate = gate_of(&bucket).await?;
    ensure!(
        gate.delegation && gate.delegation_unproven.is_none(),
        "{gate:?}"
    );
    Ok(())
}

/// **Rule 7.** The conditional writes of `record-evidence` (creating and CASing onto
/// an existing object) return ambiguous results — written while the caller gets an
/// error, or the automatic retry gets `AlreadyExists`/`Precondition` — and are
/// resolved by reading back: the command reports written, the object carries the item.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_evidence_resolves_an_ambiguous_put() -> Result<()> {
    use cairn_store::testkit::{Action, Ambiguity, OpSet, PathMatch, Rule};
    for reply in [Ambiguity::Error, Ambiguity::Retried] {
        let bucket = quiet_ready_bucket().await?;
        let faulty = FaultyStore::wrap(bucket.clone());
        let ctl = ctl_on(faulty.clone());
        for (k, why) in [('a', "create"), ('b', "CAS onto an existing object")] {
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("control/quiet-lease".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let rep = ctl
                .record_evidence(k, &format!("item {k}"), &repair(true, false))
                .await
                .map_err(|e| anyhow::anyhow!("{reply:?}, {why}: {e:?}"))?;
            ensure!(rep.applied, "{reply:?}, {why}: {rep:?}");
            let gate = gate_of(&bucket).await?;
            ensure!(gate.evidence.get(k).is_some(), "{reply:?}, {why}: {gate:?}");
        }
    }
    Ok(())
}

/// **`pricing set` through ambiguous results (rule 7).**
///
/// 1. An empty bucket: `pricing set m-1 3000000 15000000 --apply`, the first `Create`
///    returns an error although it actually wrote (`Ambiguity::Error`).
/// 2. `pricing set m-2 1 2 --apply`, the first `Update` is retried by object_store and
///    returns the `Precondition` of that very write (`Ambiguity::Retried`).
/// 3. `pricing set m-1 0 0 --apply` removes `m-1`.
///
/// Must see: all three `applied`; the final table has only `m-2` (1, 2), `version=3`.
#[tokio::test]
async fn pricing_set_resolves_ambiguous_writes() -> Result<()> {
    use cairn_store::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule};
    let bucket = Arc::new(InMemory::new());
    let faulty = FaultyStore::wrap(bucket.clone());
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let ctl = ctl_on(store);
    let opts = repair(true, false);
    for (reply, (model, i, o)) in [
        (Some(Ambiguity::Error), ("m-1", 3_000_000, 15_000_000)),
        (Some(Ambiguity::Retried), ("m-2", 1, 2)),
        (None, ("m-1", 0, 0)),
    ] {
        if let Some(reply) = reply {
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("pricing".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
        }
        let rep = ctl.pricing_set(model, i, o, &opts).await?;
        ensure!(rep.applied, "{model}: {rep:?}");
    }
    let p = ctl.pricing_show().await?;
    assert_eq!(p.version, 3, "{p:?}");
    assert_eq!(
        p.models.into_iter().collect::<Vec<_>>(),
        vec![(
            "m-2".to_string(),
            cairn_server::pricing::Price {
                input: 1,
                output: 2
            }
        )]
    );
    let raw = get(&bucket, "pricing").await?.unwrap_or_default();
    ensure!(raw.starts_with("cairnflow-pricing/1\n"), "{raw}");
    Ok(())
}

/// **`fork` and artifacts.** The sample bucket has two artifact directories: A of a
/// live lifetime still alive, B already held in place in the trash (its lifetime has
/// died).
///
/// Must see: `fork --to fork1` (default `--artifacts=live`) copies A into
/// `fork1/artifacts/…` byte for byte, does not copy B, and reports the number copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_copies_live_artifacts_only() -> Result<()> {
    use cairn_store::{
        artifact::{ArtifactStore, Owner},
        proof::DeleteProof,
        trash::{EntryId, Subject, Trash},
    };
    let bucket = load_fixture().await?;
    let store: Arc<dyn ObjectStore> = bucket.clone();
    let arts = ArtifactStore::new(Arc::clone(&store), &ObjPath::from(""));
    let owner = |b: u8| Owner {
        tenant: "default".into(),
        id: [b; 24],
        epoch: u64::from(b),
    };
    let body = bytes::Bytes::from_static(b"live artifact");
    let sha = cairn_store::artifact::sha256(&body);
    arts.put(&owner(0xa), &sha, body.clone()).await?;
    arts.put(&owner(0xb), &sha, body.clone()).await?;
    Trash::new(Arc::clone(&store), &ObjPath::from(""))
        .hold_in_place(
            &EntryId::new(1_000),
            &DeleteProof::assume(Subject::Artifacts(owner(0xb))),
            "test",
        )
        .await?;
    let ctl = ctl_on(bucket.clone());
    ctl.fork("fork1", "test@ctl", "cairn-ctl fork --to fork1")
        .await?;
    let copy = ArtifactStore::new(Arc::clone(&store), &ObjPath::from("fork1"));
    assert_eq!(copy.get(&owner(0xa), &sha).await?, Some(body));
    assert_eq!(
        copy.get(&owner(0xb), &sha).await?,
        None,
        "B is dead: not copied"
    );
    // `all` copies B too; `none` copies nothing.
    let rep = ctl
        .fork_with(
            "fork2",
            "test@ctl",
            "c",
            cairn_server::ctl::ForkArtifacts::All,
        )
        .await?;
    assert_eq!(rep.artifacts, 2, "{rep:?}");
    let rep = ctl
        .fork_with(
            "fork3",
            "test@ctl",
            "c",
            cairn_server::ctl::ForkArtifacts::None,
        )
        .await?;
    assert_eq!(rep.artifacts, 0, "{rep:?}");
    assert_eq!(
        ArtifactStore::new(Arc::clone(&store), &ObjPath::from("fork3"))
            .get(&owner(0xa), &sha)
            .await?,
        None
    );
    Ok(())
}

/// **`verify` knows the query indexes.**
///
/// 1. A one-range bucket; a real engine creates four invocations (two cancelled),
///    builds the derived data to the end (the `readable` mark). `verify` is clean.
/// 2. Delete one `ByCreated` row of a live invocation. `verify`.
/// 3. Add a `ByCreated` row for an id without metadata. `verify`.
///
/// Must see: (1) no `query_index` finding; (2) one "missing 1" finding, no loop heals
/// it; (3) an extra finding for the orphaned row, no loop heals it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_flags_query_index_drift() -> Result<()> {
    use {
        bytes::Bytes,
        cairn_engine::{BuildCursor, BuildStep, CreateOptions, Engine, Target},
        cairn_runtime::StartParams,
        cairn_store::{
            make_invocation_id, query_index as qi, LeadershipGrant, Mutations, PartitionKey,
            PartitionWriter,
        },
    };
    let mem = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(mem.clone(), "", "memory://"));
    put(
        &mem,
        "cluster.conf",
        "cairnflow-cluster/3\ntable_version=1\nnext_range_id=1\n\
         range=0000000000000000-ffffffffffffffff:0:0\n",
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
    let ids: Vec<[u8; 24]> = (1..=4u8)
        .map(|n| make_invocation_id(PartitionKey(n as u64), [n; 16]))
        .collect();
    for id in &ids {
        engine
            .create(
                *id,
                &target,
                CreateOptions::default(),
                StartParams::default(),
                Bytes::new(),
            )
            .await?;
    }
    engine.cancel(&ids[0], "done").await?;
    engine.cancel(&ids[1], "cancel").await?;
    let mut cursor = BuildCursor::default();
    while engine.query_build_step(&mut cursor, 1024).await? != BuildStep::Idle {}
    let query_findings = |rep: &cairn_server::ctl::VerifyReport| -> Vec<Finding> {
        rep.findings
            .iter()
            .filter(|f| f.check == "query_index")
            .cloned()
            .collect()
    };

    let ctl = Ctl::new(stores.clone());
    engine.seal().await?;
    let rep = ctl.verify(VerifyOptions::default()).await?;
    ensure!(query_findings(&rep).is_empty(), "{:?}", rep.findings);

    // 2. Delete one row of ids[2] (still alive).
    let writer = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    let meta = writer
        .invocation_meta(&ids[2])
        .await?
        .expect("has metadata");
    let p = qi::Placement::new(qi::Class::Active, 0, &meta);
    let (key, _) = qi::rows(&qi::BY_CREATED, &ids[2], &meta, p).remove(0);
    let mut w = writer;
    let mut m = Mutations::new();
    m.delete_query_row(key.clone());
    w.commit(m).await?;
    w.seal().await?;
    let rep = ctl.verify(VerifyOptions::default()).await?;
    let f = query_findings(&rep);
    ensure!(
        f.len() == 1 && f[0].detail.contains("missing 1") && f[0].heals.is_none(),
        "{f:?}"
    );

    // 3. A row for an id without metadata.
    let mut w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    let ghost = make_invocation_id(PartitionKey(9), [9; 16]);
    let (k, v) = qi::rows(&qi::BY_CREATED, &ghost, &meta, p).remove(0);
    let mut m = Mutations::new();
    m.put_query_row(k, &v);
    w.commit(m).await?;
    w.seal().await?;
    let rep = ctl.verify(VerifyOptions::default()).await?;
    let f = query_findings(&rep);
    ensure!(
        f.iter()
            .any(|f| f.detail.contains("orphan") && f.heals.is_none()),
        "{f:?}"
    );
    Ok(())
}

/// `--version`: `cairnflow`, `cairn-ctl` and `cairn` from the same build print the
/// same version, `data=` and `knows=` — a control plane compares the `knows=` of the
/// `cairn-ctl` from an image with the cluster. `-V` is an alias; exit code 0, no store
/// touched.
#[test]
fn binaries_report_the_same_version() {
    let want = |bin: &str| cairn_server::version_line(bin);
    assert!(
        want("x").ends_with(&format!(
            "(data={}, knows={})",
            cairn_store::DATA_FORMAT,
            cairn_store::migrate::KNOWS
        )),
        "{}",
        want("x")
    );
    for (bin, path) in [
        ("cairnflow", env!("CARGO_BIN_EXE_cairnflow")),
        ("cairn-ctl", env!("CARGO_BIN_EXE_cairn-ctl")),
        ("cairn", env!("CARGO_BIN_EXE_cairn")),
    ] {
        for flag in ["--version", "-V"] {
            let out = std::process::Command::new(path)
                .arg(flag)
                .env_clear()
                .output()
                .expect("run the binary");
            assert!(out.status.success(), "{bin} {flag}: {out:?}");
            assert_eq!(
                String::from_utf8_lossy(&out.stdout).trim_end(),
                want(bin),
                "{bin} {flag}"
            );
        }
    }
}

/// **`verify` checks audit objects.** The sample bucket plus objects under
/// `audit/tenant/zz-verify/`:
///
/// 1. an unknown name (`hand-written.txt`) and an in-range record that is not JSON
///    (`r-00ab.json`);
/// 2. an intent without an outcome 1 hour old, and one 3 days old;
/// 3. an intent with an outcome, a valid range record.
///
/// Must see: (1) one `audit` finding each; (2) the young intent is an **informational**
/// line (`notes` — dying between intent and outcome is a valid case; the report does
/// not go dirty for it), the intent older than 24 h is a finding; (3) nothing.
#[tokio::test]
async fn verify_checks_audit_objects() -> Result<()> {
    let bucket = load_fixture().await?;
    let now: u64 = 1_793_620_000_000;
    let hour = 3_600_000;
    let day = |ms: u64| cairn_store::audit::day_of(ms);
    let put = |rel: String, body: &'static [u8]| {
        let bucket = bucket.clone();
        async move {
            bucket
                .put(&ObjPath::from(rel), PutPayload::from_static(body))
                .await
        }
    };
    let dir = |ms: u64| format!("audit/tenant/zz-verify/{}", day(ms));
    let name = |ms: u64, n: u32| format!("{ms}-node_a-00000000000000ab-{n}");
    put(format!("{}/hand-written.txt", dir(now)), b"x").await?;
    put(format!("{}/r-00ab.json", dir(now)), b"not json {").await?;
    put(
        format!("{}/{}.intent", dir(now - hour), name(now - hour, 0)),
        b"{}",
    )
    .await?;
    put(
        format!(
            "{}/{}.intent",
            dir(now - 72 * hour),
            name(now - 72 * hour, 1)
        ),
        b"{}",
    )
    .await?;
    put(
        format!("{}/{}.intent", dir(now - 2 * hour), name(now - 2 * hour, 2)),
        b"{}",
    )
    .await?;
    put(
        format!("{}/{}.done", dir(now - 2 * hour), name(now - 2 * hour, 2)),
        b"{}",
    )
    .await?;
    put(format!("{}/r-00cd.json", dir(now)), b"{\"a\":1}").await?;
    let rep = ctl_on(bucket.clone())
        // (k) runs with `open_dbs` (not in the background scrubber).
        .verify(VerifyOptions {
            now_ms: now,
            ..VerifyOptions::default()
        })
        .await?;
    let ours = |v: &[Finding]| -> Vec<Finding> {
        v.iter()
            .filter(|f| f.check == "audit" && f.detail.contains("zz-verify"))
            .cloned()
            .collect()
    };
    let (found, notes) = (ours(&rep.findings), ours(&rep.notes));
    let has = |v: &[Finding], part: &str| v.iter().any(|f| f.detail.contains(part));
    ensure!(
        found.len() == 3 && notes.len() == 1,
        "{found:#?} {notes:#?}"
    );
    ensure!(has(&found, "hand-written.txt"), "{found:#?}");
    ensure!(has(&found, "r-00ab.json"), "{found:#?}");
    ensure!(has(&found, &name(now - 72 * hour, 1)), "{found:#?}");
    ensure!(has(&notes, &name(now - hour, 0)), "{notes:#?}");
    Ok(())
}

/// **`verify` checks the residue of a purged tenant.** The sample bucket (tenant
/// `acme` has a `Cart` object, invocations, a deployment), then the `tenants`
/// registry hand-written as `acme state=purged …` as if the purge had finished without
/// deleting anything; plus a hold-in-place entry `TenantPrefix` of `beta` — a tenant
/// that is not `deleting`/`purged` (held by mistake).
///
/// Must see: a `tenant-residue` finding for `acme`'s rows in the Db (no loop heals
/// it) and for the objects under `deployments/acme/` (no loop heals it); `beta`'s
/// hold entry is a healable finding (the purge loop removes it); no finding about
/// tenant `default`.
#[tokio::test]
async fn verify_flags_residue_of_a_purged_tenant() -> Result<()> {
    use cairn_store::{
        proof::DeleteProof,
        tenant_purge::PrefixKind,
        trash::{EntryId, Subject, Trash},
        TenantId,
    };
    let bucket = load_fixture().await?;
    let book = get(&bucket, "tenants")
        .await?
        .unwrap_or_else(|| "cairnflow-tenants/1\n".into());
    let book: String = book
        .lines()
        .filter(|l| l.split(' ').next() != Some("acme"))
        .map(|l| format!("{l}\n"))
        .collect();
    put(
        &bucket,
        "tenants",
        &format!("{book}acme state=purged deleting-ms=1 purging-ms=2 purged-ms=3\n"),
    )
    .await?;
    let store: Arc<dyn ObjectStore> = bucket.clone();
    Trash::new(Arc::clone(&store), &ObjPath::from(""))
        .hold_in_place(
            &EntryId::new(1_793_620_000_000),
            &DeleteProof::assume(Subject::TenantPrefix {
                tenant: TenantId::new("beta")?,
                kind: PrefixKind::Stats,
            }),
            "held by mistake",
        )
        .await?;
    let rep = ctl_on(bucket.clone())
        .verify(VerifyOptions::default())
        .await?;
    let residue: Vec<&Finding> = rep
        .findings
        .iter()
        .filter(|f| f.check == "tenant-residue")
        .collect();
    let has = |part: &str, heals: bool| {
        residue
            .iter()
            .any(|f| f.detail.contains(part) && f.heals.is_some() == heals)
    };
    ensure!(
        residue
            .iter()
            .any(|f| f.range.is_some() && f.detail.contains("acme") && f.heals.is_none()),
        "acme's rows not found: {residue:#?}"
    );
    ensure!(has("deployments/acme", false), "{residue:#?}");
    ensure!(has("beta", true), "{residue:#?}");
    ensure!(
        !residue.iter().any(|f| f.detail.contains("default")),
        "{residue:#?}"
    );
    Ok(())
}

/// **`verify --deep`: a lost SST ⇒ exactly one `sst_missing` finding.** The sample
/// bucket is clean even with `--deep` (the external SSTs of children 2, 3 resolve to
/// the retired parent Db 0). Delete an SST that range 1's manifest mentions ⇒ exactly
/// one finding, naming exactly that object; without `--deep` no check sees it (the
/// manifest is still there).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_deep_finds_a_missing_sst() -> Result<()> {
    let deep = VerifyOptions {
        deep: true,
        ..VerifyOptions::default()
    };
    let bucket = load_fixture().await?;
    let rep = ctl_on(bucket.clone()).verify(deep).await?;
    ensure!(
        rep.clean(),
        "sample bucket not clean with --deep: {:?}",
        rep.findings
    );

    let sst = "ranges/1/compacted/01M5YZH91E1QCDTNCAD6Z3EZ9X.sst";
    bucket.delete(&ObjPath::from(sst)).await?;
    let shallow = ctl_on(bucket.clone())
        .verify(VerifyOptions {
            open_dbs: false,
            ..VerifyOptions::default()
        })
        .await?;
    ensure!(shallow.clean(), "{:?}", shallow.findings);
    let rep = ctl_on(bucket.clone())
        .verify(VerifyOptions {
            open_dbs: false,
            ..deep
        })
        .await?;
    ensure!(
        kinds(&rep.findings) == [("sst_missing", Some(1))],
        "{:?}",
        rep.findings
    );
    ensure!(rep.findings[0].detail.contains(sst), "{:?}", rep.findings);
    Ok(())
}
