// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The two-tier migration framework — the runner, on a ledger injected by the
//! test (the real `MIGRATIONS` ledger carries no fake entries).
//!
//! Each step of the test ledger counts how many times it ran with an object
//! **in the test's own bucket** (`steps/<name>`), so tests running in parallel
//! do not touch each other. The die-mid-step tests live in
//! `tests/failpoints.rs`.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    cairn_store::{
        migrate::{
            self, ClusterCtx, ClusterVersion, Migration, RangeDbCtx, Run, Runner, Upgrading,
        },
        LeaseConfig, PartitionWriter, Session, SessionId,
    },
    futures::future::{BoxFuture, LocalBoxFuture},
    slatedb::object_store::{
        memory::InMemory, path::Path, ObjectStore, ObjectStoreExt, PutPayload,
    },
    std::{sync::Arc, time::Duration},
};

const POLL: Duration = Duration::from_millis(50);

async fn count(store: &Arc<dyn ObjectStore>, name: &str) -> u32 {
    match store.get(&Path::from(format!("steps/{name}"))).await {
        Ok(g) => String::from_utf8(g.bytes().await.unwrap().to_vec())
            .unwrap()
            .parse()
            .unwrap(),
        Err(_) => 0,
    }
}

/// Count one run of step `name`, recording the `from` it received.
async fn bump(ctx: &ClusterCtx, name: &str, from: u32) -> cairn_store::Result<()> {
    let n = count(&ctx.store, name).await + 1;
    let put = |p: String, v: String| {
        let store = Arc::clone(&ctx.store);
        async move {
            store
                .put(&Path::from(p), PutPayload::from(v))
                .await
                .map_err(|e| cairn_store::StoreError::Io(e.to_string()))
        }
    };
    put(format!("steps/{name}"), n.to_string()).await?;
    put(format!("steps/{name}.from"), from.to_string()).await?;
    Ok(())
}

fn c1(from: u32, ctx: ClusterCtx) -> BoxFuture<'static, cairn_store::Result<()>> {
    Box::pin(async move { bump(&ctx, "c1", from).await })
}

fn r2(_: u32, _: &mut PartitionWriter) -> LocalBoxFuture<'_, cairn_store::Result<()>> {
    Box::pin(async { Ok(()) })
}

fn c3(from: u32, ctx: ClusterCtx) -> BoxFuture<'static, cairn_store::Result<()>> {
    Box::pin(async move { bump(&ctx, "c3", from).await })
}

/// Cluster 1 → range 2 → cluster 3.
const LEDGER: &[Migration] = &[
    Migration {
        to: 1,
        name: "c1",
        run: Run::Cluster(c1),
    },
    Migration {
        to: 2,
        name: "r2",
        run: Run::Range(r2),
    },
    Migration {
        to: 3,
        name: "c3",
        run: Run::Cluster(c3),
    },
];

fn alone(node: &str) -> Runner<'static> {
    Runner::Alone { node: node.into() }
}

async fn version_of(store: &Arc<dyn ObjectStore>) -> Option<ClusterVersion> {
    migrate::read_cluster_version(store, &Path::from(""))
        .await
        .unwrap()
        .map(|(cv, _)| cv)
}

async fn put_version(store: &Arc<dyn ObjectStore>, cv: &ClusterVersion) {
    store
        .put(
            &Path::from("cluster-version"),
            PutPayload::from(cv.encode()),
        )
        .await
        .unwrap();
}

/// **Cluster runner.** A format-9 bucket (no `cluster-version`) ⇒ the runner
/// runs every cluster-tier step exactly once, in order, each step receiving
/// `from` = the version when the lock was taken; afterwards `version` = the
/// last step, `min_binary` follows, and `upgrading` disappears. Running again
/// does nothing.
#[tokio::test]
async fn cluster_runner_runs_each_cluster_step_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("");
    let cv = migrate::run_cluster(&store, &root, alone("a"), LEDGER, POLL)
        .await
        .unwrap();
    assert_eq!(
        (cv.version, cv.min_binary, cv.upgrading.clone()),
        (3, 3, None),
        "{cv:?}"
    );
    assert_eq!(version_of(&store).await.as_ref(), Some(&cv));
    assert_eq!(
        (count(&store, "c1").await, count(&store, "c3").await),
        (1, 1)
    );
    assert_eq!(count(&store, "c1.from").await, 0);

    let again = migrate::run_cluster(&store, &root, alone("b"), LEDGER, POLL)
        .await
        .unwrap();
    assert_eq!(again, cv);
    assert_eq!(
        (count(&store, "c1").await, count(&store, "c3").await),
        (1, 1),
        "re-ran a finished step"
    );
}

/// **Cluster runner.** A cluster already at step 1 ⇒ only the missing part
/// runs, `from` = 1. An empty ledger on a format-9 bucket ⇒ no
/// `cluster-version` is written.
#[tokio::test]
async fn cluster_runner_runs_only_missing_steps() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("");
    let cv = migrate::run_cluster(&store, &root, alone("a"), &[], POLL)
        .await
        .unwrap();
    assert_eq!(cv, ClusterVersion::default());
    assert!(
        version_of(&store).await.is_none(),
        "empty ledger, yet cluster-version was written"
    );

    put_version(
        &store,
        &ClusterVersion {
            version: 1,
            min_binary: 1,
            ..Default::default()
        },
    )
    .await;
    let cv = migrate::run_cluster(&store, &root, alone("a"), LEDGER, POLL)
        .await
        .unwrap();
    assert_eq!(cv.version, 3);
    assert_eq!(
        (count(&store, "c1").await, count(&store, "c3").await),
        (0, 1)
    );
    assert_eq!(count(&store, "c3.from").await, 1);
}

/// **Cluster runner.** The lock of a runner that is **alive** ⇒ wait, do not
/// run concurrently; once that runner closes its session ⇒ we take over and run
/// everything. The lock of a dead session (no record left) ⇒ take over at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_runner_waits_for_live_holder_and_takes_over_dead_one() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("");
    let cfg = |node: &str| {
        LeaseConfig::new()
            .with_node_id(node)
            .with_ttl(Duration::from_secs(1))
            .with_renew_interval(Duration::from_millis(200))
    };
    let other = Session::establish(Arc::clone(&store), &root, cfg("other"))
        .await
        .unwrap();
    let me = Session::establish(Arc::clone(&store), &root, cfg("me"))
        .await
        .unwrap();
    put_version(
        &store,
        &ClusterVersion {
            upgrading: Some(Upgrading {
                from: 0,
                to: 3,
                by: other.current().unwrap(),
            }),
            ..Default::default()
        },
    )
    .await;
    let run = {
        let (store, me) = (Arc::clone(&store), me.clone());
        tokio::spawn(async move {
            migrate::run_cluster(&store, &Path::from(""), Runner::Session(&me), LEDGER, POLL).await
        })
    };
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(!run.is_finished(), "ran on top of a live lock holder");
    assert_eq!(count(&store, "c1").await, 0);
    other.close().await;
    let cv = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((cv.version, cv.upgrading), (3, None));
    assert_eq!(count(&store, "c1").await, 1);

    // The lock of a session with no record left ⇒ take it at once.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put_version(
        &store,
        &ClusterVersion {
            version: 1,
            upgrading: Some(Upgrading {
                from: 1,
                to: 3,
                by: SessionId {
                    node: "ghost".into(),
                    inc: 7,
                },
            }),
            ..Default::default()
        },
    )
    .await;
    let me2 = Session::establish(Arc::clone(&store), &root, cfg("me"))
        .await
        .unwrap();
    let cv = tokio::time::timeout(
        Duration::from_secs(5),
        migrate::run_cluster(&store, &root, Runner::Session(&me2), LEDGER, POLL),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!((cv.version, cv.upgrading), (3, None));
    assert_eq!(
        (count(&store, "c1").await, count(&store, "c3").await),
        (0, 1)
    );
    me.close().await;
    me2.close().await;
}

// ─────────────────────────────────────────── the range-tier runner

fn r1_row(_: u32, w: &mut PartitionWriter) -> LocalBoxFuture<'_, cairn_store::Result<()>> {
    Box::pin(async move {
        // Count runs right in the Db: read and write back — a re-run shows.
        let n = match w.get(b"\xfftest-r1").await? {
            Some(v) => v[0] + 1,
            None => 1,
        };
        let mut m = cairn_store::Mutations::new();
        m.raw_put(b"\xfftest-r1".to_vec(), &[n]);
        w.commit(m).await
    })
}

/// Cluster 1 → range 2 → range 3.
const RANGE_LEDGER: &[Migration] = &[
    Migration {
        to: 1,
        name: "c1",
        run: Run::Cluster(c1),
    },
    Migration {
        to: 2,
        name: "r2",
        run: Run::Range(r1_row),
    },
    Migration {
        to: 3,
        name: "r3",
        run: Run::Range(r1_row),
    },
];

async fn writer_on(store: Arc<dyn ObjectStore>) -> PartitionWriter {
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store,
        "",
        "memory://",
    ));
    PartitionWriter::open(
        cairn_store::LeadershipGrant::single_node(cairn_store::PartitionId(0)),
        stores.open_db(0).await.unwrap(),
    )
    .await
    .unwrap()
}

/// **Range runner.** A range at `data` runs exactly the range-tier steps with
/// `data < to ≤ version`, in order, durable before returning; cluster-tier
/// steps are skipped; `data ≥ version` does nothing.
#[tokio::test]
async fn range_runner_runs_missing_range_steps() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut w = writer_on(Arc::clone(&store)).await;
    // A new cluster at 2: only r2.
    assert_eq!(
        migrate::run_range(&mut w, 0, 2, RANGE_LEDGER)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        w.get(b"\xfftest-r1").await.unwrap().as_deref(),
        Some(&[1u8][..])
    );
    // The cluster moves to 3: only r3.
    assert_eq!(
        migrate::run_range(&mut w, 2, 3, RANGE_LEDGER)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        w.get(b"\xfftest-r1").await.unwrap().as_deref(),
        Some(&[2u8][..])
    );
    // Done: nothing runs.
    assert_eq!(
        migrate::run_range(&mut w, 3, 3, RANGE_LEDGER)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        w.get(b"\xfftest-r1").await.unwrap().as_deref(),
        Some(&[2u8][..])
    );
}

/// **Range runner.** A range carrying a `data` greater than the binary's ledger ⇒ stop loudly, do not open.
#[tokio::test]
async fn range_runner_refuses_data_newer_than_ledger() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut w = writer_on(store).await;
    let e = migrate::run_range(&mut w, 4, 3, RANGE_LEDGER)
        .await
        .unwrap_err();
    assert!(
        matches!(e, cairn_store::StoreError::NewerFormat { .. }),
        "{e}"
    );
}

// ──────────────── the range-Db tier: before the Db opens

/// Count one run of a range-Db-tier step on the range, in the test's bucket.
fn d2(from: u32, ctx: RangeDbCtx<'_>) -> LocalBoxFuture<'_, cairn_store::Result<()>> {
    Box::pin(async move {
        let store = Arc::clone(ctx.stores.main().store());
        let name = format!("d2-{}", ctx.range);
        bump(
            &ClusterCtx {
                store,
                prefix: Path::from(""),
            },
            &name,
            from,
        )
        .await
    })
}

/// Cluster 1 → range Db 2 → range 3.
const DB_LEDGER: &[Migration] = &[
    Migration {
        to: 1,
        name: "c1",
        run: Run::Cluster(c1),
    },
    Migration {
        to: 2,
        name: "d2",
        run: Run::RangeDb(d2),
    },
    Migration {
        to: 3,
        name: "r3",
        run: Run::Range(r1_row),
    },
];

/// **Range-Db runner.** The range-Db runner runs exactly the [`Run::RangeDb`]
/// steps with `data < to ≤ version`, each receiving `from = data`; it skips
/// cluster-tier and row-tier steps; `data` greater than the ledger ⇒ stop
/// loudly.
#[tokio::test]
async fn range_db_runner_runs_missing_db_steps() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        Arc::clone(&store),
        "",
        "memory://",
    ));
    let session = Session::establish(
        Arc::clone(&store),
        &Path::from(""),
        LeaseConfig::new()
            .with_node_id("a")
            .with_ttl(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    let mut keeper = match session.claim(cairn_store::PartitionId(0)).await.unwrap() {
        cairn_store::Claim::Won(_, k) => k,
        cairn_store::Claim::Held(_) => panic!("did not get range 0"),
    };
    // Cluster at 1: no Db step reached yet.
    migrate::run_range_db(&stores, 0, &mut keeper, 0, 1, DB_LEDGER)
        .await
        .unwrap();
    assert_eq!(count(&store, "d2-0").await, 0);
    // Cluster at 3: d2 runs once, receiving `from` = 0; r3 (row tier) does not run here.
    migrate::run_range_db(&stores, 0, &mut keeper, 0, 3, DB_LEDGER)
        .await
        .unwrap();
    assert_eq!(count(&store, "d2-0").await, 1);
    assert_eq!(count(&store, "d2-0.from").await, 0);
    // The range is already at 2: no re-run.
    migrate::run_range_db(&stores, 0, &mut keeper, 2, 3, DB_LEDGER)
        .await
        .unwrap();
    assert_eq!(count(&store, "d2-0").await, 1);
    let e = migrate::run_range_db(&stores, 0, &mut keeper, 4, 3, DB_LEDGER)
        .await
        .unwrap_err();
    assert!(
        matches!(e, cairn_store::StoreError::NewerFormat { .. }),
        "{e}"
    );
}

// ──────────────────────── the first real step — `rules/2` (the real ledger)

const RULES_V1: &str = "cairnflow-rules/1\nversion=4\nrule\tacme\t50\t0\t1791737779820\tsample bucket ✓\nrule\t*/*\tunlimited\t1\t7\tsay\ttab, forced\n";

async fn raw(store: &Arc<dyn ObjectStore>, path: &str) -> String {
    String::from_utf8(
        store
            .get(&Path::from(path))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

/// **`rules/2`.** The real ledger takes rule book `cairnflow-rules/1` to `/2`:
/// the same rules, the same rule-book `version`, the `rate`/`burst` columns
/// left empty (`-`, unset — no change of meaning); `cluster-version` moves to
/// the ledger's last step (step 2 — `wal/shared`, range-Db tier — touches
/// nothing at the cluster tier). Running again changes no byte.
#[tokio::test]
async fn real_ledger_rewrites_rules_v1_to_v2() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("");
    store
        .put(&Path::from("rules"), PutPayload::from(RULES_V1))
        .await
        .unwrap();
    let cv = migrate::run_cluster(&store, &root, alone("a"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    // A bucket with no `cluster.conf` and no range data ⇒ a new cluster: both
    // call gates come on (prefix grouping and placement), plus the durable
    // streams gate, the schedules gate and the `/2` key-book gate, and
    // `min_binary` moves to the last gate step.
    assert_eq!(
        (cv.version, cv.min_binary, cv.upgrading.clone()),
        (migrate::VERSION, migrate::KNOWS, None),
        "{cv:?}"
    );
    let v2 = raw(&store, "rules").await;
    assert_eq!(
        v2,
        "cairnflow-rules/2\nversion=4\n\
         rule\t*/*\tunlimited\t-\t-\t1\t7\tsay\ttab, forced\n\
         rule\tacme\t50\t-\t-\t0\t1791737779820\tsample bucket ✓\n",
    );
    // Idempotent: re-running the step (the runner died after the step, before the mark) changes nothing.
    store.delete(&Path::from("cluster-version")).await.unwrap();
    migrate::run_cluster(&store, &root, alone("b"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert_eq!(raw(&store, "rules").await, v2);
}

/// **`rules/2`.** A bucket with no rule book yet: the step does not create one
/// (absent ≡ empty, as before); `cluster-version` still moves to the ledger's
/// last step.
#[tokio::test]
async fn real_ledger_without_rules_only_bumps_version() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(
        &store,
        &Path::from(""),
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
    )
    .await
    .unwrap();
    assert_eq!(cv.version, migrate::VERSION);
    assert!(
        store.get(&Path::from("rules")).await.is_err(),
        "the step created a rule book"
    );
}

/// **`rules/2`.** A store without CAS (`LocalFileSystem`, the `--data-dir`
/// path): the runner and the `rules/2` step still work — the lack of CAS
/// already blocks forming a cluster, so there is only one process that could
/// race (the same reasoning as `RuleStore::update`).
#[tokio::test]
async fn real_ledger_runs_on_store_without_cas() {
    let dir = std::env::temp_dir().join(format!("cairn-migrate-fs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(&dir).unwrap());
    store
        .put(&Path::from("rules"), PutPayload::from(RULES_V1))
        .await
        .unwrap();
    let cv = migrate::run_cluster(
        &store,
        &Path::from(""),
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
    )
    .await
    .unwrap();
    assert_eq!((cv.version, cv.upgrading), (migrate::VERSION, None));
    assert!(raw(&store, "rules")
        .await
        .starts_with("cairnflow-rules/2\n"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ──────────────── the real step `wal/shared` — own-WAL Db to the shared WAL

/// Write status row `n` to the writer and wait until it is durable.
async fn put_status(w: &mut PartitionWriter, n: u8, value: &[u8]) {
    let mut id = [0u8; 24];
    id[23] = n;
    let mut m = cairn_store::Mutations::new();
    m.set_invocation_status(&id, value);
    w.commit(m).await.unwrap();
    w.wait_durable().await.unwrap();
}

async fn get_status(w: &PartitionWriter, n: u8) -> Option<Vec<u8>> {
    let mut id = [0u8; 24];
    id[23] = n;
    w.get(&cairn_store::keys::invocation_status(&id))
        .await
        .unwrap()
        .map(|b| b.to_vec())
}

/// **`wal/shared`.** An own-WAL range has acked rows that **live only in the
/// own WAL** (not yet down to L0 — the shape of a single-process bucket shut
/// down abruptly), and no owner record yet. The new owner runs the real ledger
/// before opening the Db: the own WAL must be fully flushed into SSTs before
/// the `wal_name` mark changes, the owner record changes `wal_kind` (CAS, rule
/// 7 — even when the owner-record write is ambiguous), and then the Db opened
/// with the shared WAL sees every row.
///
/// 1. Open range 0's Db with its own WAL, write two durable rows, close the Db
///    flushing only the WAL (not the memtable). Check the premise: the own WAL
///    still has frames (changing the mark is refused).
/// 2. `adopt_unowned_db` (the node's path when a Db has no owner record yet),
///    session `a` takes the range — the keeper carries `wal_kind=slatedb`.
/// 3. `run_range_db` with the real ledger; the owner-record write is ambiguous
///    per `reply`.
///
/// Must see: the keeper and the owner record carry `shared`, the manifest
/// carries the shared WAL, and the Db opened with the shared WAL through
/// `acquire_shared` reads back both rows and can keep writing.
#[tokio::test]
async fn native_wal_range_moves_to_shared_wal() {
    use cairn_store::{
        testkit::{Ambiguity, FaultyStore, Rule},
        Claim, LeadershipGrant, NodeWal, NodeWalConfig, ObjectStores, PartitionId, StoreLocation,
        WalKind,
    };
    for reply in [None, Some(Ambiguity::Retried), Some(Ambiguity::Error)] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let stores = ObjectStores::single(StoreLocation::from_store(
            Arc::clone(&store),
            "",
            "memory://",
        ));
        // ── 1. Rows only in the own WAL.
        let db = stores.open_db(0).await.unwrap();
        let mut w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db.clone())
            .await
            .unwrap();
        put_status(&mut w, 1, b"one").await;
        put_status(&mut w, 2, b"two").await;
        drop(w);
        // Close flushing only the WAL (SlateDB's default flushes the memtable too).
        db.close_with_options(slatedb::config::CloseOptions {
            flush_type: Some(slatedb::config::FlushType::Wal),
        })
        .await
        .unwrap();
        assert!(
            stores.adopt_shared_wal(0).await.is_err(),
            "{reply:?}: broken premise — the own WAL has no frames left"
        );
        // ── 2. An owner record for the old Db, then take the range.
        assert!(stores.adopt_unowned_db(PartitionId(0), "a").await.unwrap());
        let session = Session::establish(
            Arc::clone(&store),
            &Path::from(""),
            LeaseConfig::new()
                .with_node_id("a")
                .with_ttl(Duration::from_secs(2)),
        )
        .await
        .unwrap();
        let (grant, mut keeper) = match session.claim(PartitionId(0)).await.unwrap() {
            Claim::Won(g, k) => (g, k),
            Claim::Held(_) => panic!("did not get range 0"),
        };
        assert_eq!(keeper.wal().kind, WalKind::SlateDb);
        // ── 3. The real ledger.
        if let Some(reply) = reply {
            faulty.add_rule(Rule::ambiguous_put("owners/range-0", reply));
        }
        let data = keeper.data();
        migrate::run_range_db(
            &stores,
            0,
            &mut keeper,
            data,
            migrate::VERSION,
            migrate::MIGRATIONS,
        )
        .await
        .unwrap_or_else(|e| panic!("{reply:?}: the wal/shared step failed: {e}"));
        assert_eq!(keeper.wal().kind, WalKind::Shared, "{reply:?}");
        let rec = cairn_store::read_owner(&store, &Path::from(""), PartitionId(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.wal_kind, WalKind::Shared, "{reply:?}: {rec:?}");
        assert_eq!(
            stores.range_wal_kind(PartitionId(0)).await.unwrap(),
            Some(WalKind::Shared),
            "{reply:?}"
        );
        let wal = NodeWal::start(
            &stores,
            session.current().unwrap(),
            NodeWalConfig {
                window: Duration::from_millis(5),
                ..NodeWalConfig::default()
            },
        );
        let db = cairn_store::acquire_shared(&stores, &wal, &mut keeper, 0)
            .await
            .unwrap();
        let mut w = PartitionWriter::open(grant, db).await.unwrap();
        assert_eq!(get_status(&w, 1).await.as_deref(), Some(&b"one"[..]));
        assert_eq!(get_status(&w, 2).await.as_deref(), Some(&b"two"[..]));
        put_status(&mut w, 3, b"three").await;
        assert_eq!(get_status(&w, 3).await.as_deref(), Some(&b"three"[..]));
    }
}

/// **Gate steps.** A gate step (`Run::Gate`) in the ledger is not the cluster
/// runner's job: at the tail of the ledger, `version`/`min_binary` stop at the
/// last data step (an old binary can still start during a rolling upgrade); in
/// the middle of the ledger it is skipped; and a `min_binary` set by the
/// gate-enabling command (greater than `version`) does not block a runner that
/// knows that step.
#[tokio::test]
async fn gate_steps_are_skipped_by_the_cluster_runner() {
    use cairn_store::migrate::latest_data;
    static RAN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    fn c(_: u32, _: ClusterCtx) -> BoxFuture<'static, cairn_store::Result<()>> {
        RAN.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    const TAIL: &[Migration] = &[
        Migration {
            to: 1,
            name: "c1",
            run: Run::Cluster(c),
        },
        Migration {
            to: 2,
            name: "g2",
            run: Run::Gate,
        },
    ];
    const MIDDLE: &[Migration] = &[
        Migration {
            to: 1,
            name: "c1",
            run: Run::Cluster(c),
        },
        Migration {
            to: 2,
            name: "g2",
            run: Run::Gate,
        },
        Migration {
            to: 3,
            name: "c3",
            run: Run::Cluster(c),
        },
    ];
    assert_eq!((latest_data(TAIL), latest_data(MIDDLE)), (1, 3));
    assert_eq!(
        (migrate::VERSION, migrate::KNOWS),
        (migrate::QUIET_LEASE - 1, migrate::VALUES),
        "real ledger: ten gate steps lease/quiet, calls/prefix, calls/placement, streams/table, schedules/table, auth/keys-v2, tenants/state, audit/table, tenants/purge, values/segmented at the tail"
    );

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::default();
    let cv = migrate::run_cluster(&store, &root, alone("a"), TAIL, POLL)
        .await
        .unwrap();
    assert_eq!((cv.version, cv.min_binary), (1, 1), "{cv:?}");
    // The gate-enabling command sets `min_binary` = 2: a runner that knows step 2 still runs.
    let (mut raised, tag) = migrate::read_cluster_version(&store, &root)
        .await
        .unwrap()
        .unwrap();
    raised.min_binary = 2;
    migrate::write_cluster_version(&store, &root, &raised, Some(tag))
        .await
        .unwrap()
        .unwrap();
    let cv = migrate::run_cluster(&store, &root, alone("a"), TAIL, POLL)
        .await
        .unwrap();
    assert_eq!((cv.version, cv.min_binary), (1, 2), "{cv:?}");
    // A ledger with a data step after the gate: the gate is skipped, c3 runs.
    RAN.store(0, std::sync::atomic::Ordering::SeqCst);
    let cv = migrate::run_cluster(&store, &root, alone("a"), MIDDLE, POLL)
        .await
        .unwrap();
    assert_eq!((cv.version, cv.min_binary), (3, 3), "{cv:?}");
    assert_eq!(
        RAN.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "only c3 ran"
    );
}

/// **Call gates on a new bucket.** The cluster runner **creating**
/// `cluster-version` on a bucket with no cluster yet ⇒ the grouping gate comes
/// on at once (`call_prefix=on`, `min_binary` = the gate step) within the same
/// lock take. A bucket that already has a cluster but no `cluster-version`
/// (format 9 from a binary before the migration framework: it has
/// `cluster.conf` or range data) is **not** new — an old binary may be running
/// on it — so the gate stays off. A ledger that does not know the gate step ⇒
/// it does not come on, even on an empty bucket.
#[tokio::test]
async fn real_ledger_turns_call_prefix_on_only_for_fresh_buckets() {
    let root = Path::from("");
    let fresh: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(&fresh, &root, alone("a"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert!(
        cv.call_prefix_on()
            && cv.call_placement_on()
            && cv.streams_on()
            && cv.schedules_on()
            && cv.key_roles_on()
            && cv.tenant_state_on(),
        "{cv:?}"
    );
    assert_eq!(cv.min_binary, migrate::KNOWS);
    assert_eq!(version_of(&fresh).await, Some(cv));

    for existing in [
        "cluster.conf",
        "ranges/range-0/manifest/00000000000000000001.manifest",
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(existing), PutPayload::from("x"))
            .await
            .unwrap();
        let cv = migrate::run_cluster(&store, &root, alone("a"), migrate::MIGRATIONS, POLL)
            .await
            .unwrap();
        assert!(
            !cv.call_prefix
                && !cv.call_placement
                && !cv.streams
                && !cv.schedules
                && !cv.key_roles
                && !cv.tenant_state,
            "{existing}: {cv:?}"
        );
        assert_eq!(cv.min_binary, migrate::VERSION, "{existing}: {cv:?}");
    }

    // A ledger from a binary that does not know the gate step (stops at `lease/quiet`): not on.
    let before = &migrate::MIGRATIONS[..migrate::CALL_PREFIX as usize - 1];
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(&store, &root, alone("a"), before, POLL)
        .await
        .unwrap();
    assert!(!cv.call_prefix && !cv.call_placement, "{cv:?}");
    // A cluster that already has `cluster-version` (formed by an old binary): a new binary does not switch it on.
    let cv = migrate::run_cluster(&store, &root, alone("b"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert!(!cv.call_prefix && !cv.call_placement, "{cv:?}");

    // A ledger from a binary that knows `calls/prefix` but not `calls/placement`
    // forms a new cluster: only the grouping gate, `min_binary=4`. A newer
    // binary coming up later does not switch on the placement gate — that is
    // `enable-call-prefix`'s job.
    let upto_prefix = &migrate::MIGRATIONS[..migrate::CALL_PLACEMENT as usize - 1];
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(&store, &root, alone("a"), upto_prefix, POLL)
        .await
        .unwrap();
    assert!(cv.call_prefix_on() && !cv.call_placement, "{cv:?}");
    assert_eq!(cv.min_binary, migrate::CALL_PREFIX);
    let cv = migrate::run_cluster(&store, &root, alone("b"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert!(cv.call_prefix_on() && !cv.call_placement_on(), "{cv:?}");
    assert_eq!(cv.min_binary, migrate::CALL_PREFIX);

    // A ledger from a binary that knows up to `calls/placement` but not
    // `streams/table` forms a new cluster: both call gates, no durable-streams
    // gate, `min_binary=5`. A newer binary coming up later does not switch it on
    // — that is `enable-streams`'s job.
    let upto_placement = &migrate::MIGRATIONS[..migrate::STREAMS_TABLE as usize - 1];
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(&store, &root, alone("a"), upto_placement, POLL)
        .await
        .unwrap();
    assert!(cv.call_placement_on() && !cv.streams, "{cv:?}");
    assert_eq!(cv.min_binary, migrate::CALL_PLACEMENT);
    let cv = migrate::run_cluster(&store, &root, alone("b"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert!(cv.call_placement_on() && !cv.streams_on(), "{cv:?}");
    assert_eq!(cv.min_binary, migrate::CALL_PLACEMENT);

    // A ledger from a binary that knows up to `streams/table` but not
    // `schedules/table` forms a new cluster: three gates, no schedules gate,
    // `min_binary=6`. A newer binary coming up later does not switch it on —
    // that is `enable-schedules`'s job.
    let upto_streams = &migrate::MIGRATIONS[..migrate::SCHEDULES_TABLE as usize - 1];
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(&store, &root, alone("a"), upto_streams, POLL)
        .await
        .unwrap();
    assert!(cv.streams_on() && !cv.schedules, "{cv:?}");
    assert_eq!(cv.min_binary, migrate::STREAMS_TABLE);
    let cv = migrate::run_cluster(&store, &root, alone("b"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert!(cv.streams_on() && !cv.schedules_on(), "{cv:?}");
    assert_eq!(cv.min_binary, migrate::STREAMS_TABLE);
}

// ──────── the quiet-lease gate of a new bucket

/// The current `control/quiet-lease` gate (absent ⇒ `None`; unreadable ⇒ panic).
async fn gate_of(store: &Arc<dyn ObjectStore>) -> Option<cairn_store::quiet_lease::QuietLease> {
    cairn_store::quiet_lease::read(store, &Path::from(""))
        .await
        .unwrap()
        .map(|(q, _)| q.expect("the gate is readable"))
}

fn new_cluster(q: cairn_store::quiet_lease::Bootstrap) -> migrate::NewCluster {
    migrate::NewCluster {
        quiet_lease: q,
        probe_reader: None,
        ..Default::default()
    }
}

/// **A new bucket comes with the gate on, with delegation.** The cluster runner
/// **creating** `cluster-version` on an empty bucket ⇒ at the same time writes
/// `control/quiet-lease` = `state=on`, `knows=K`, `delegation=on`, the
/// `delegation_unproven=` mark (all five items missing, `by=bootstrap`), and
/// `min_binary ≥ K` (a binary that does not know the `solo`/delegation rules
/// cannot start). [`migrate::run_cluster`] defaults to delegation; `solo` ⇒
/// the gate on without delegation, no mark; `off` ⇒ nothing is written.
#[tokio::test]
async fn fresh_bucket_gets_the_quiet_lease_gate_with_delegation() {
    use cairn_store::quiet_lease::{Bootstrap, BOOTSTRAP_BY, EVIDENCE_ITEMS};
    let root = Path::from("");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster(&store, &root, alone("a"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert!(cv.min_binary >= migrate::QUIET_LEASE, "{cv:?}");
    let q = gate_of(&store).await.expect("a new bucket has the gate");
    assert!(q.on && q.delegation, "{q:?}");
    assert_eq!(q.knows, migrate::QUIET_LEASE);
    assert!(q.by.starts_with(BOOTSTRAP_BY), "{q:?}");
    assert!(q.at_ms > 0, "{q:?}");
    let u = q.delegation_unproven.expect("new bucket's override mark");
    assert_eq!(u.by, BOOTSTRAP_BY);
    assert_eq!(u.missing, EVIDENCE_ITEMS.to_vec());
    assert!(u.at_ms > 0);
    assert_eq!(q.evidence.missing(), EVIDENCE_ITEMS.to_vec());

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    migrate::run_cluster_with(
        &store,
        &root,
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
        &new_cluster(Bootstrap::Solo),
    )
    .await
    .unwrap();
    let q = gate_of(&store).await.expect("solo ⇒ there is a gate");
    assert!(
        q.on && !q.delegation && q.delegation_unproven.is_none(),
        "{q:?}"
    );
    assert_eq!(q.knows, migrate::QUIET_LEASE);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cv = migrate::run_cluster_with(
        &store,
        &root,
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
        &new_cluster(Bootstrap::Off),
    )
    .await
    .unwrap();
    assert_eq!(gate_of(&store).await, None, "off ⇒ no gate written");
    assert_eq!(cv.version, migrate::VERSION);
}

/// **An existing bucket changes nothing.** (1) a formed cluster (has
/// `cluster-version`, even one formed with `off`); (2) a format-9 bucket from
/// before the migration framework (has `cluster.conf` or range data, no
/// `cluster-version`); (3) a gate already there (`cairn-ctl cluster
/// record-evidence` creates `state=off` on a bucket with no cluster yet) — the
/// bootstrapper does not overwrite it; (4) a ledger that does not know the gate
/// step ⇒ nothing written. Green even before the fix — a fence for the
/// "changes nothing" side.
#[tokio::test]
async fn existing_bucket_keeps_its_quiet_lease_state() {
    use cairn_store::quiet_lease::{Bootstrap, QuietLease};
    let root = Path::from("");
    // (1)
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    migrate::run_cluster_with(
        &store,
        &root,
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
        &new_cluster(Bootstrap::Off),
    )
    .await
    .unwrap();
    migrate::run_cluster(&store, &root, alone("b"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert_eq!(gate_of(&store).await, None, "cluster formed with off");
    // (2)
    for existing in [
        "cluster.conf",
        "ranges/range-0/manifest/00000000000000000001.manifest",
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(existing), PutPayload::from("x"))
            .await
            .unwrap();
        migrate::run_cluster(&store, &root, alone("a"), migrate::MIGRATIONS, POLL)
            .await
            .unwrap();
        assert_eq!(gate_of(&store).await, None, "{existing}");
    }
    // (3)
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let off = QuietLease {
        on: false,
        knows: migrate::QUIET_LEASE,
        by: "op".into(),
        at_ms: 1,
        ..Default::default()
    };
    assert!(cairn_store::quiet_lease::write(&store, &root, &off, None)
        .await
        .unwrap());
    migrate::run_cluster(&store, &root, alone("a"), migrate::MIGRATIONS, POLL)
        .await
        .unwrap();
    assert_eq!(
        gate_of(&store).await,
        Some(off),
        "an existing gate is not overwritten"
    );
    // (4)
    let before = &migrate::MIGRATIONS[..migrate::QUIET_LEASE as usize - 1];
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    migrate::run_cluster(&store, &root, alone("a"), before, POLL)
        .await
        .unwrap();
    assert_eq!(gate_of(&store).await, None, "ledger lacks lease/quiet");
}

/// **Several bootstrappers on the same empty bucket at once reach exactly one
/// result.**
///
/// 1. Four nodes (two `delegation`, two `solo`), each with a live session, run
///    the cluster tier on one empty bucket at the same time.
/// 2. Each node CASes to create `cluster-version` carrying its own lock — one
///    node wins, the other three see the lock of a live session and wait.
/// 3. The winner writes the gate (`PutMode::Create`, rule 7), runs the steps,
///    removes the lock; the other three re-read and see the cluster complete.
///
/// Must see: every node finishes at the same `version`; exactly one gate,
/// readable, carrying the choice of exactly one bootstrapper
/// (`by=bootstrap@<node>/<inc>` matching `delegation`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_bootstrap_of_one_empty_bucket_gives_one_gate() {
    use cairn_store::quiet_lease::Bootstrap;
    let cfg = |node: &str| {
        LeaseConfig::new()
            .with_node_id(node)
            .with_ttl(Duration::from_secs(2))
            .with_renew_interval(Duration::from_millis(400))
    };
    for round in 0..4 {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut sessions = Vec::new();
        for (node, q) in [
            ("d1", Bootstrap::Delegation),
            ("s1", Bootstrap::Solo),
            ("d2", Bootstrap::Delegation),
            ("s2", Bootstrap::Solo),
        ] {
            let s = Session::establish(Arc::clone(&store), &Path::from(""), cfg(node))
                .await
                .unwrap();
            sessions.push((s, q));
        }
        let runs: Vec<_> = sessions
            .iter()
            .map(|(s, q)| {
                let (store, s, q) = (Arc::clone(&store), s.clone(), *q);
                tokio::spawn(async move {
                    migrate::run_cluster_with(
                        &store,
                        &Path::from(""),
                        Runner::Session(&s),
                        migrate::MIGRATIONS,
                        POLL,
                        &new_cluster(q),
                    )
                    .await
                })
            })
            .collect();
        for r in runs {
            let cv = tokio::time::timeout(Duration::from_secs(20), r)
                .await
                .expect("the runner hung")
                .unwrap()
                .unwrap();
            assert_eq!(cv.version, migrate::VERSION, "round {round}");
        }
        for (s, _) in &sessions {
            s.close().await;
        }
        let q = gate_of(&store).await.expect("exactly one gate");
        assert!(q.on, "round {round}: {q:?}");
        let writer = q.by.rsplit('@').next().unwrap_or_default().to_string();
        assert!(
            writer.starts_with(if q.delegation { "d" } else { "s" }),
            "round {round}: the gate mixes two bootstrappers' choices: {q:?}"
        );
        assert_eq!(q.delegation_unproven.is_some(), q.delegation, "{q:?}");
    }
}

/// **Rule 7 for creating the gate.** A `Create` PUT of `control/quiet-lease`
/// that reached the store while the caller got an error (`Ambiguity::Error`) or
/// the `AlreadyExists` of object_store's own retry (`Ambiguity::Retried`) ⇒ the
/// re-read sees exactly our content ⇒ the write is done, no error.
#[tokio::test]
async fn ambiguous_gate_create_resolves_by_reading_back() {
    use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
    for reply in [Ambiguity::Error, Ambiguity::Retried] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        faulty.add_rule(Rule::ambiguous_put(cairn_store::quiet_lease::OBJECT, reply));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let cv = migrate::run_cluster(
            &store,
            &Path::from(""),
            alone("a"),
            migrate::MIGRATIONS,
            POLL,
        )
        .await
        .unwrap();
        assert_eq!(cv.version, migrate::VERSION, "{reply:?}");
        let q = gate_of(&store).await.expect("the gate was written");
        assert!(q.on && q.delegation, "{reply:?}: {q:?}");
    }
}

/// **The G18 probe before enabling**: with a second read client (`s3://`) ⇒ the
/// bootstrapper tries a conditional GET on the bucket itself. A store serving a
/// stale `304` (a proxy cache, weak consistency) ⇒ the gate is **not** written —
/// the new cluster runs `direct` — but it still starts. A correct store ⇒ the
/// gate comes on, and the probe object is cleaned up.
#[tokio::test]
async fn bootstrap_skips_the_gate_on_a_store_that_serves_a_stale_304() {
    use {
        cairn_store::{
            quiet_lease::Bootstrap,
            testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        },
        futures::TryStreamExt,
    };
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains(cairn_store::store_probe::DIR.into()),
        Action::StaleNotModified,
    ));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let new = migrate::NewCluster {
        quiet_lease: Bootstrap::Delegation,
        probe_reader: Some(Arc::clone(&store)),
        ..Default::default()
    };
    let cv = migrate::run_cluster_with(
        &store,
        &Path::from(""),
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
        &new,
    )
    .await
    .unwrap();
    assert_eq!(cv.version, migrate::VERSION);
    assert_eq!(gate_of(&store).await, None, "store fails G18 ⇒ no gate");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let new = migrate::NewCluster {
        quiet_lease: Bootstrap::Delegation,
        probe_reader: Some(Arc::clone(&store)),
        ..Default::default()
    };
    migrate::run_cluster_with(
        &store,
        &Path::from(""),
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
        &new,
    )
    .await
    .unwrap();
    assert!(gate_of(&store).await.is_some_and(|q| q.on && q.delegation));
    let probes: Vec<_> = store
        .list(Some(&Path::from(cairn_store::store_probe::DIR)))
        .try_collect()
        .await
        .unwrap();
    assert!(probes.is_empty(), "probe objects left behind: {probes:?}");
}

/// **Other objects of a new bucket** (`NewCluster::on_fresh`; `cairn-server`
/// attaches `control/registry-gen` generation 0): the hook runs exactly once
/// when the runner builds an empty bucket, even with `--quiet-lease off`; it
/// does not run on a bucket that already has a cluster or on a format-9 bucket
/// from before the migration framework.
#[tokio::test]
async fn fresh_hooks_run_once_and_only_on_a_fresh_bucket() {
    use {
        cairn_store::quiet_lease::Bootstrap,
        std::sync::atomic::{AtomicU32, Ordering},
    };
    let runs = Arc::new(AtomicU32::new(0));
    let new = |q: Bootstrap| {
        let runs = Arc::clone(&runs);
        migrate::NewCluster {
            quiet_lease: q,
            on_fresh: vec![Arc::new(move || {
                runs.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })],
            ..Default::default()
        }
    };
    let root = Path::from("");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for q in [Bootstrap::Off, Bootstrap::Delegation] {
        migrate::run_cluster_with(
            &store,
            &root,
            alone("a"),
            migrate::MIGRATIONS,
            POLL,
            &new(q),
        )
        .await
        .unwrap();
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1, "new bucket: exactly once");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(&Path::from("cluster.conf"), PutPayload::from("x"))
        .await
        .unwrap();
    migrate::run_cluster_with(
        &store,
        &root,
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
        &new(Bootstrap::Delegation),
    )
    .await
    .unwrap();
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "bucket with a cluster: does not run"
    );
}

/// **Dying during bootstrap.** The bootstrapper creates `cluster-version` (its
/// lock, `min_binary` already enough) and dies before writing the gate. The next
/// one takes the dead one's lock on a bucket that is **still empty** (no
/// `cluster.conf`, no range yet) ⇒ writes the gate in its place. A bucket that
/// already has `cluster.conf` when the lock is taken ⇒ nothing is written (it
/// is no longer a new bucket).
#[tokio::test]
async fn takeover_of_a_dead_bootstrapper_still_writes_the_gate() {
    let dead = ClusterVersion {
        upgrading: Some(Upgrading {
            from: 0,
            to: migrate::VERSION,
            by: SessionId {
                node: "ghost".into(),
                inc: 0,
            },
        }),
        min_binary: migrate::CALL_PLACEMENT,
        call_prefix: true,
        call_placement: true,
        ..Default::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put_version(&store, &dead).await;
    let cv = migrate::run_cluster(
        &store,
        &Path::from(""),
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
    )
    .await
    .unwrap();
    assert_eq!((cv.version, cv.upgrading), (migrate::VERSION, None));
    assert!(gate_of(&store).await.is_some_and(|q| q.on && q.delegation));

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    put_version(&store, &dead).await;
    store
        .put(&Path::from("cluster.conf"), PutPayload::from("x"))
        .await
        .unwrap();
    migrate::run_cluster(
        &store,
        &Path::from(""),
        alone("a"),
        migrate::MIGRATIONS,
        POLL,
    )
    .await
    .unwrap();
    assert_eq!(
        gate_of(&store).await,
        None,
        "has cluster.conf ⇒ no longer new"
    );
}
