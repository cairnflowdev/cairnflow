// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Failpoint infrastructure on cairn-store's points.
//!
//! Only built with `--features failpoints` (`required-features`). `fail`'s registry is
//! global, so every test here starts with `StepController::new()` — it holds the scenario
//! lock, and this binary's tests run one after another.

// Tests run on real time, network and processes; only `sim_cluster` must be deterministic
// (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    cairn_store::{
        testkit::StepController, ClusterConfig, LeadershipGrant, Mutations, ObjectStores,
        PartitionId, PartitionTable, PartitionWriter, StoreLocation, TableUpdate,
    },
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
    std::{sync::Arc, time::Duration},
};

const STOP: Duration = Duration::from_secs(10);

/// The next table after `t`: the same ranges, version + 1.
fn bump(t: &PartitionTable) -> cairn_store::Result<PartitionTable> {
    PartitionTable::from_entries(t.version() + 1, t.next_range_id(), t.ranges().to_vec())
}

/// `StepController` blocks a gap that exists only in RAM — between reading the table and
/// the CAS of `ClusterConfig::update` — so another node changing the table slips into
/// exactly that gap. The blocked writer's CAS must lose (`Conflict`), not overwrite.
///
/// The writer slipping in writes a table **different** from the blocked writer's: a lost
/// CAS is resolved by reading back and comparing the content, so two writers writing
/// identical tables both see `Applied` (the bucket carries exactly the table each one
/// wanted — SM-2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_update_loses_cas_to_writer_in_the_gap() -> Result<()> {
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = ClusterConfig::open(&store, &Path::default(), 2).await?;
    let b = ClusterConfig::open(&store, &Path::default(), 2).await?;

    let paused = ctl.pause_at("cluster::update::before_cas");
    let slow = tokio::spawn({
        let a = a.clone();
        async move { a.update(1, bump).await }
    });
    paused.expect_stopped(STOP).await;
    // A thread arriving while the point is stopped goes through: b wins the CAS.
    let other = |t: &PartitionTable| {
        let mut r = t.ranges().to_vec();
        r[0].end -= 1;
        r[1].start -= 1;
        PartitionTable::from_entries(t.version() + 1, t.next_range_id(), r)
    };
    assert!(matches!(b.update(1, other).await?, TableUpdate::Applied(t) if t.version() == 2));
    assert_eq!(paused.hits(), 2);
    paused.resume();

    match slow.await?? {
        TableUpdate::Conflict(t) => assert_eq!(*t, *b.table()),
        TableUpdate::Applied(t) => panic!("CAS after the read gap overwrote the table: {t:?}"),
    }
    Ok(())
}

/// `return(msg)` makes the function return an error carrying the point's name; dropping the guard removes it.
#[tokio::test]
async fn fail_guard_returns_error_until_dropped() -> Result<()> {
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let c = ClusterConfig::open(&store, &Path::default(), 2).await?;
    {
        let _g = ctl.fail("cluster::update::before_cas", "return(boom)");
        let err = c.update(1, bump).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("failpoint cluster::update::before_cas: boom"),
            "{err}"
        );
    }
    assert!(matches!(c.update(1, bump).await?, TableUpdate::Applied(_)));
    Ok(())
}

/// A point never reached ⇒ the test goes **red** within the deadline, it does not hang.
#[tokio::test]
#[should_panic(expected = "no thread arrived")]
async fn expect_stopped_panics_when_point_never_reached() {
    let ctl = StepController::new();
    let paused = ctl.pause_at("split::quiesce");
    paused.expect_stopped(Duration::from_millis(50)).await;
}

/// `1*return` before the durability wait: the first wait fails, the next one succeeds —
/// the row is still durable and reads back. Also proof that the count (`1*`) works.
#[tokio::test]
async fn durable_wait_fails_once_then_succeeds() -> Result<()> {
    let ctl = StepController::new();
    let stores = ObjectStores::single(StoreLocation::parse("memory://")?);
    let mut w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    let _g = ctl.fail("writer::commit::before_durable", "1*return");
    let mut m = Mutations::new();
    m.set_invocation_status(&[7; 24], b"x");
    w.commit(m).await?;
    let err = w.wait_durable().await.unwrap_err();
    assert!(
        err.to_string().contains("writer::commit::before_durable"),
        "{err}"
    );
    w.wait_durable().await?;
    assert_eq!(
        w.get(&cairn_store::keys::invocation_status(&[7; 24]))
            .await?
            .as_deref(),
        Some(&b"x"[..])
    );
    Ok(())
}

/// **A write that lost its handle** — pins the writer's current behaviour.
/// `writer::commit::after_submit` makes `commit` return an error **after** a batch with an
/// outbox row was submitted (the shape of a future cancelled there).
///
/// Order: one durable write; close the WAL PUT gate; commit an outbox row with the point
/// on once.
///
/// Must see: `commit` returns the point's error; the row **is readable** (SlateDB applied
/// the batch) but not durable (stuck at the gate); and `durability()` issues a ticket **at
/// once** — `latest` did not get that write, so no ticket waits for it. Opening the gate ⇒
/// the row becomes durable.
#[tokio::test]
async fn submitted_outbox_batch_with_lost_handle_is_applied_but_not_waited_for() -> Result<()> {
    use cairn_store::testkit::{Action, FaultyStore, Gate, Rule};
    let ctl = StepController::new();
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let db = slatedb::Db::builder("p0", faulty.clone() as Arc<dyn ObjectStore>)
        .build()
        .await?;
    let mut w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&[7; 24], b"x");
    w.commit(m).await?;
    w.wait_durable().await?;

    let gate = Gate::closed();
    faulty.add_rule(Rule::wal_put(Action::Gate(gate.clone())));
    let pk = cairn_store::partition_key_of(&[7; 24]);
    let mut m = Mutations::new();
    m.push_outbox(pk, 1, &[7; 24], 3, b"tin");
    let err = {
        let _g = ctl.fail("writer::commit::after_submit", "1*return(cancel)");
        w.commit(m).await.unwrap_err()
    };
    assert!(
        err.to_string().contains("writer::commit::after_submit"),
        "{err}"
    );
    let key = cairn_store::keys::outbox(pk, &[7; 24], 3);
    assert!(w.get(&key).await?.is_some(), "batch must be applied");
    let t0 = std::time::Instant::now();
    while gate.blocked() == 0 {
        assert!(t0.elapsed() < STOP, "the write did not reach the WAL gate");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        matches!(w.durability()?, cairn_store::Durability::Ready(_)),
        "today: no ticket waits for a write that lost its handle"
    );
    gate.open();
    let t0 = std::time::Instant::now();
    while gate.blocked() > 0 {
        assert!(t0.elapsed() < STOP, "write stuck at WAL gate");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(())
}

// ─────────────────────────── migration framework

fn mig_c1(
    _: u32,
    ctx: cairn_store::migrate::ClusterCtx,
) -> futures::future::BoxFuture<'static, cairn_store::Result<()>> {
    use slatedb::object_store::{ObjectStoreExt, PutPayload};
    Box::pin(async move {
        let p = Path::from("steps/c1");
        let n: u32 = match ctx.store.get(&p).await {
            Ok(g) => String::from_utf8(g.bytes().await.unwrap().to_vec())
                .unwrap()
                .parse()
                .unwrap(),
            Err(_) => 0,
        };
        ctx.store
            .put(&p, PutPayload::from((n + 1).to_string()))
            .await
            .map_err(|e| cairn_store::StoreError::Io(e.to_string()))?;
        Ok(())
    })
}

const MIG_LEDGER: &[cairn_store::migrate::Migration] = &[cairn_store::migrate::Migration {
    to: 1,
    name: "c1",
    run: cairn_store::migrate::Run::Cluster(mig_c1),
}];

/// **Dying midway through a cluster-level step, then running again.** The runner dies
/// after the step finished but before `version` was written: `cluster-version` still has
/// its lock and `version=0`. The next run (another runner — the lock of a dead process)
/// takes over, runs the step **again** (idempotent: the second run shows in the counter),
/// then writes `version=1` and removes the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_step_reruns_after_death_before_version_bump() -> Result<()> {
    use {
        cairn_store::migrate::{self, Runner},
        slatedb::object_store::ObjectStoreExt,
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("");
    let poll = Duration::from_millis(20);
    {
        let _die = ctl.fail("migrate::cluster::after_step", "1*return(die)");
        let r = migrate::run_cluster(
            &store,
            &root,
            Runner::Alone { node: "a".into() },
            MIG_LEDGER,
            poll,
        )
        .await;
        anyhow::ensure!(r.is_err(), "the failpoint did not fire: {r:?}");
    }
    let (cv, _) = migrate::read_cluster_version(&store, &root)
        .await?
        .expect("lock written");
    anyhow::ensure!(
        cv.version == 0 && cv.upgrading.is_some(),
        "after dying midway through the step: {cv:?}"
    );
    let ran = |s: &Arc<dyn ObjectStore>| {
        let s = Arc::clone(s);
        async move {
            String::from_utf8(
                s.get(&Path::from("steps/c1"))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap()
        }
    };
    anyhow::ensure!(ran(&store).await == "1");

    let cv = migrate::run_cluster(
        &store,
        &root,
        Runner::Alone { node: "b".into() },
        MIG_LEDGER,
        poll,
    )
    .await?;
    anyhow::ensure!(
        cv.version == 1 && cv.upgrading.is_none() && cv.min_binary == 1,
        "{cv:?}"
    );
    anyhow::ensure!(
        ran(&store).await == "2",
        "the step must run again exactly once more"
    );
    Ok(())
}

/// **The real step `rules/2` dies midway, then runs again.** The runner rewrites `rules`
/// to `/2`, then dies before `version` is written: the bucket carries rule book `/2` while
/// `cluster-version` still has `version=0` and the dead runner's lock. The next runner
/// takes over, runs the step again on a rule book **already** at `/2` (no byte changes),
/// then goes on to the book's last step (step 2 `wal/shared` at the range Db level touches
/// nothing at the cluster level).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rules_2_step_reruns_after_death_before_version_bump() -> Result<()> {
    use {
        cairn_store::migrate::{self, Runner},
        slatedb::object_store::{ObjectStoreExt, PutPayload},
    };
    let ctl = StepController::new();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("");
    let poll = Duration::from_millis(20);
    store
        .put(
            &Path::from("rules"),
            PutPayload::from("cairnflow-rules/1\nversion=2\nrule\tacme\t5\t0\t9\ta note\n"),
        )
        .await?;
    let rules = |s: &Arc<dyn ObjectStore>| {
        let s = Arc::clone(s);
        async move {
            String::from_utf8(
                s.get(&Path::from("rules"))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap()
        }
    };
    {
        let _die = ctl.fail("migrate::cluster::after_step", "1*return(die)");
        let r = migrate::run_cluster(
            &store,
            &root,
            Runner::Alone { node: "a".into() },
            migrate::MIGRATIONS,
            poll,
        )
        .await;
        anyhow::ensure!(r.is_err(), "the failpoint did not fire: {r:?}");
    }
    let (cv, _) = migrate::read_cluster_version(&store, &root)
        .await?
        .expect("lock written");
    anyhow::ensure!(
        cv.version == 0 && cv.upgrading.is_some(),
        "after dying midway through the step: {cv:?}"
    );
    let after_death = rules(&store).await;
    anyhow::ensure!(
        after_death == "cairnflow-rules/2\nversion=2\nrule\tacme\t5\t-\t-\t0\t9\ta note\n",
        "the step had not rewritten the rule book before dying: {after_death:?}"
    );

    let cv = migrate::run_cluster(
        &store,
        &root,
        Runner::Alone { node: "b".into() },
        migrate::MIGRATIONS,
        poll,
    )
    .await?;
    anyhow::ensure!(
        cv.version == migrate::VERSION
            && cv.upgrading.is_none()
            // A bucket without a cluster yet (only `rules`): the first lock takeover turns on
            // the two call gates, the durable-stream gate, the schedule gate and the key book
            // `/2` gate right away — dying right after that takeover keeps them — so
            // `min_binary` is the last gate step.
            && cv.min_binary == migrate::KNOWS
            && cv.call_prefix_on()
            && cv.call_placement_on()
            && cv.streams_on()
            && cv.schedules_on()
            && cv.key_roles_on()
            && cv.tenant_state_on(),
        "{cv:?}"
    );
    anyhow::ensure!(
        rules(&store).await == after_death,
        "re-running the rule book step already at /2"
    );
    Ok(())
}

// ─────────────────────────────── node conditions

/// A test-only point: `return` ⇒ an error carrying the point's name.
fn scoped_point() -> cairn_store::Result<()> {
    cairn_store::failpoint!("test::node_scoped", err);
    Ok(())
}

/// Like [`scoped_point`], with its own condition at the placement (`failpoint!(name, cond, e)`).
fn conditional_point(cond: bool) -> cairn_store::Result<()> {
    cairn_store::failpoint!("test::conditional", cond, err);
    Ok(())
}

/// **Two nodes in one process, one point, firing on one node only.** The node of a piece
/// of code is task-local (`in_node`) or thread-local (`set_thread_node`, the engine
/// thread); code running outside every node never fires a restricted point. Removing the
/// restriction (dropping the guard) ⇒ the point fires everywhere as before.
#[tokio::test]
async fn node_scoped_point_fires_only_on_named_node() -> Result<()> {
    use cairn_store::failpoint::in_node;
    let ctl = StepController::new();
    {
        let _g = ctl.fail_on(&["node-a"], "test::node_scoped", "return(only-a)");
        let a = in_node("node-a", async { scoped_point() }).await;
        let b = in_node("node-b", async { scoped_point() }).await;
        assert!(a.unwrap_err().to_string().contains("only-a"));
        assert!(b.is_ok(), "a point restricted to node-a fired on node-b");
        assert!(scoped_point().is_ok(), "restricted point fired off-node");
        // A thread serving only one node (the engine thread).
        let t = std::thread::spawn(|| {
            cairn_store::failpoint::set_thread_node("node-a");
            scoped_point().is_err()
        });
        assert!(
            t.join().expect("test thread"),
            "a thread-local node-a did not make the point fire"
        );
    }
    {
        let _g = ctl.fail("test::node_scoped", "return");
        assert!(in_node("node-b", async { scoped_point() }).await.is_err());
        assert!(
            scoped_point().is_err(),
            "restriction removed yet the point still fires only on node-a"
        );
    }
    // The condition-at-placement form: `cond` false ⇒ the point is not evaluated.
    let _g = ctl.fail("test::conditional", "return");
    assert!(conditional_point(false).is_ok());
    assert!(conditional_point(true).is_err());
    Ok(())
}

/// `pause_at_on`: only the given node's thread stops; other nodes go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_on_node_stops_only_that_node() -> Result<()> {
    use cairn_store::failpoint::in_node;
    let ctl = StepController::new();
    let paused = ctl.pause_at_on(&["node-a"], "test::node_scoped");
    // node-b goes through at once, not counted.
    in_node("node-b", async { scoped_point() }).await?;
    assert_eq!(paused.hits(), 0);
    let a = tokio::spawn(in_node("node-a", async { scoped_point() }));
    paused.expect_stopped(STOP).await;
    assert!(!a.is_finished());
    paused.resume();
    a.await??;
    Ok(())
}

/// `DelayStore` (chaos `--slow-node`, `--l0-pressure`): `return(<ms>)` makes PUTs wait;
/// `return(<ms>:<string>)` only delays paths containing the string; `Writes` does not slow
/// GETs; removing the point ends the slowness. Tokio's paused clock measures the waits,
/// no real sleeping.
#[tokio::test(start_paused = true)]
async fn delay_store_waits_only_while_point_is_on() -> Result<()> {
    use {
        cairn_store::failpoint::{DelayOps, DelayStore},
        slatedb::object_store::{ObjectStoreExt, PutPayload},
        tokio::time::Instant,
    };
    let ctl = StepController::new();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let slow = DelayStore::wrap(Arc::clone(&inner), "node::put_delay", DelayOps::Writes);
    let both = DelayStore::wrap(inner, "compactor::io_delay", DelayOps::ReadsAndWrites);
    let put = |s: Arc<DelayStore>, p: &'static str| async move {
        let t = Instant::now();
        s.put(&Path::from(p), PutPayload::from_static(b"x")).await?;
        anyhow::Ok(t.elapsed())
    };
    let get = |s: Arc<DelayStore>, p: &'static str| async move {
        let t = Instant::now();
        s.get(&Path::from(p)).await?;
        anyhow::Ok(t.elapsed())
    };

    assert!(put(slow.clone(), "a/1").await? < Duration::from_millis(5));
    {
        let _g = ctl.fail("node::put_delay", "return(300)");
        assert!(put(slow.clone(), "a/1").await? >= Duration::from_millis(300));
        assert!(
            get(slow.clone(), "a/1").await? < Duration::from_millis(5),
            "Writes does not slow GETs"
        );
        assert!(
            put(both.clone(), "a/2").await? < Duration::from_millis(5),
            "another point does not fire"
        );
    }
    assert!(
        put(slow.clone(), "a/1").await? < Duration::from_millis(5),
        "removing the point ends the slowness"
    );
    {
        let _g = ctl.fail("node::put_delay", "return(200:nodewal)");
        assert!(put(slow.clone(), "nodewal/n/1/0/7").await? >= Duration::from_millis(200));
        assert!(put(slow.clone(), "ranges/1/manifest/1").await? < Duration::from_millis(5));
    }
    {
        let _g = ctl.fail("compactor::io_delay", "return(250)");
        assert!(get(both.clone(), "a/1").await? >= Duration::from_millis(250));
        assert!(put(both, "a/3").await? >= Duration::from_millis(250));
    }
    Ok(())
}

/// **The SM-12 tripwire is wired.** Builds a state that a split/merge rollback must never
/// meet: a Db standing on the very final checkpoint of the orphan child.
///
/// 1. Range 0 has rows, closed cleanly; clone 0 → 1 (an orphan child about to roll back).
/// 2. Clone again 1 → 2: Db 2 inherits ancestor entry 0 with `source_checkpoint_id` = the
///    final checkpoint of 1 on 0 — exactly the shape "someone stands on the orphan's
///    checkpoint" (which does not happen in real life: nobody splits an orphan child that
///    never made it into the table).
/// 3. A callback at `split::discard::referenced_checkpoint` sends into a channel.
///    `discard_range_db(1)` with 2 in the list of live sources.
///
/// Must see: the point fires; the final checkpoint of 1 on 0 remains; a later clone from 2
/// still works.
#[tokio::test]
async fn discard_tripwire_fires_and_keeps_a_referenced_checkpoint() -> Result<()> {
    use cairn_store::PartitionKey;
    let _ctl = StepController::new();
    let stores =
        ObjectStores::single(StoreLocation::parse("memory://").expect("memory:// always parses"));
    let db = stores.open_db(0).await?;
    let mut key = vec![0x00];
    key.extend_from_slice(&10u64.to_be_bytes());
    db.put(&key, b"n").await?;
    for flush_type in [
        slatedb::config::FlushType::Wal,
        slatedb::config::FlushType::MemTable,
    ] {
        db.flush_with_options(slatedb::config::FlushOptions { flush_type })
            .await?;
    }
    db.close().await?;
    let span = (PartitionKey(0), PartitionKey(u64::MAX));
    stores
        .clone_range(PartitionId(0), PartitionId(1), span)
        .await?;
    stores
        .clone_range(PartitionId(1), PartitionId(2), span)
        .await?;
    let admin = |r: u32| {
        slatedb::admin::AdminBuilder::new(stores.range_path(r), stores.main().store().clone())
            .build()
    };
    let orphan_final = admin(1)
        .read_manifest(None)
        .await?
        .expect("1 has a manifest")
        .external_dbs()
        .iter()
        .find(|e| e.path == stores.range_path(0).to_string())
        .and_then(|e| e.final_checkpoint_id)
        .expect("1 stands on a checkpoint of 0");

    let (tx, rx) = std::sync::mpsc::channel();
    let _fp = cairn_store::testkit::FailGuard::callback(
        "split::discard::referenced_checkpoint",
        move || {
            let _ = tx.send(());
        },
    );
    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(1)),
            &[PartitionId(2)],
            None,
        )
        .await?;
    rx.recv_timeout(STOP).expect("the tripwire did not fire");
    assert!(
        admin(0)
            .list_checkpoints(None)
            .await?
            .iter()
            .any(|c| c.id == orphan_final),
        "a checkpoint still referenced by 2 was deleted"
    );
    stores
        .clone_range(PartitionId(2), PartitionId(3), span)
        .await?;
    Ok(())
}

/// **A rollback keeps the orphan child when deleting its checkpoint fails.** The
/// interleaving:
/// 1. Range 0 is live; orphan child 1 was cloned (final checkpoint on 0).
/// 2. `discard_range_db(1, [0])`: deleting the checkpoint on 0 fails (failpoint
///    `split::discard::delete_checkpoint` = `return`).
/// 3. Remove the failpoint; clean up again.
///
/// Must see: step 2 returns an error and keeps the Db of 1 (the manifest is the only
/// record of that checkpoint); step 3 cleans up — 0 no longer has an unbounded
/// checkpoint. Before the fix: step 2 only logged at `debug`, deleted the Db of 1 and
/// returned `Ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discard_keeps_the_orphan_when_deleting_its_checkpoint_fails() -> Result<()> {
    use cairn_store::PartitionKey;
    let ctl = StepController::new();
    let stores =
        ObjectStores::single(StoreLocation::parse("memory://").expect("memory:// always parses"));
    let db = stores.open_db(0).await?;
    let mut key = vec![0x00];
    key.extend_from_slice(&10u64.to_be_bytes());
    db.put(&key, b"n").await?;
    for flush_type in [
        slatedb::config::FlushType::Wal,
        slatedb::config::FlushType::MemTable,
    ] {
        db.flush_with_options(slatedb::config::FlushOptions { flush_type })
            .await?;
    }
    db.close().await?;
    let (live, orphan) = (PartitionId(0), PartitionId(1));
    stores
        .clone_range(live, orphan, (PartitionKey(0), PartitionKey(u64::MAX)))
        .await?;
    let pinned = || async {
        Ok::<_, anyhow::Error>(
            stores
                .range_checkpoints(live)
                .await?
                .into_iter()
                .filter(|c| !c.expires)
                .count(),
        )
    };
    assert_eq!(pinned().await?, 1);

    let fp = ctl.fail("split::discard::delete_checkpoint", "return");
    let first = stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(orphan),
            &[live],
            None,
        )
        .await;
    drop(fp);
    assert!(first.is_err(), "failed delete, cleanup said Ok");
    assert!(
        stores.range_db_exists(orphan).await?,
        "the orphan child's Db was deleted before its checkpoint"
    );

    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(orphan),
            &[live],
            None,
        )
        .await?;
    assert!(!stores.range_db_exists(orphan).await?);
    assert_eq!(
        pinned().await?,
        0,
        "the live range still has the orphan child's final checkpoint"
    );
    Ok(())
}

// ─────────────── our own unserved segment after a failed range claim

mod unserved_segment {
    use {
        anyhow::{anyhow, bail, ensure, Result},
        cairn_store::{
            acquire_shared,
            testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule, StepController},
            Claim, LeaseConfig, LeaseKeeper, LogId, Mutations, NodeWal, NodeWalConfig,
            ObjectStores, OwnerRecord, PartitionId, PartitionWriter, Session, StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStoreExt, PutPayload},
        std::{sync::Arc, time::Duration},
    };

    const TTL: Duration = Duration::from_millis(400);
    const PACE: Duration = Duration::from_millis(50);

    struct Bucket {
        faulty: Arc<FaultyStore>,
        stores: ObjectStores,
    }

    fn bucket() -> Bucket {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let stores = ObjectStores::single(StoreLocation::from_store(
            faulty.clone(),
            "cairn",
            "memory://cairn",
        ))
        .with_open_bound(Duration::from_secs(2));
        Bucket { faulty, stores }
    }

    fn cfg() -> NodeWalConfig {
        NodeWalConfig {
            window: Duration::from_millis(5),
            stripes: 2,
            inflight: 1,
            object_cap: 1 << 20,
            frame_age_flush: Duration::from_secs(3600),
            put_timeout: Duration::from_millis(500),
            put_attempts: 2,
            gc_interval: Duration::from_millis(50),
            ..NodeWalConfig::default()
        }
    }

    struct Node {
        session: Session,
        wal: NodeWal,
    }

    async fn node(b: &Bucket, name: &str) -> Result<Node> {
        let session = Session::establish(
            b.faulty.clone(),
            &Path::from("cairn"),
            LeaseConfig::new()
                .with_node_id(name)
                .with_ttl(TTL)
                .with_renew_interval(PACE),
        )
        .await?;
        let id = session.current().expect("session just opened");
        let wal = NodeWal::start(&b.stores, id, cfg());
        Ok(Node { session, wal })
    }

    async fn claim(n: &Node, range: u32) -> Result<(cairn_store::LeadershipGrant, LeaseKeeper)> {
        for _ in 0..200 {
            match n.session.claim(PartitionId(range)).await? {
                Claim::Won(g, k) => return Ok((g, k)),
                Claim::Held(_) => tokio::time::sleep(PACE).await,
            }
        }
        bail!("could not claim range {range}")
    }

    async fn acquire(b: &Bucket, n: &Node, range: u32) -> Result<(PartitionWriter, LeaseKeeper)> {
        let (g, mut k) = claim(n, range).await?;
        let db = acquire_shared(&b.stores, &n.wal, &mut k, range).await?;
        Ok((PartitionWriter::open(g, db).await?, k))
    }

    async fn put(w: &mut PartitionWriter, n: u8) -> Result<()> {
        let mut id = [0u8; 24];
        id[23] = n;
        let mut m = Mutations::new();
        m.set_invocation_status(&id, b"v");
        w.commit(m).await?;
        w.wait_durable().await?;
        Ok(())
    }

    async fn owner(b: &Bucket, range: u32) -> Result<OwnerRecord> {
        cairn_store::read_owner(b.faulty.inner(), &Path::from("cairn"), PartitionId(range))
            .await?
            .ok_or_else(|| anyhow!("range {range} has no owner record"))
    }

    fn lists(rec: &OwnerRecord, log: &LogId) -> bool {
        rec.wal_segments.iter().any(|s| s.log == *log)
    }

    /// Node `b` claims range 0 (sealing every log the owner record lists), then `a` writes
    /// to range 2 — on the same stripe as range 0. Must see: `a` does not fail-stop.
    async fn another_node_takes_range_0(
        b: &Bucket,
        a: &Node,
        w2: &mut PartitionWriter,
    ) -> Result<()> {
        let bn = node(b, "f4-b").await?;
        let (_wb, _kb) = acquire(b, &bn, 0).await?;
        let wrote = put(w2, 9).await;
        ensure!(
            a.wal.fail_stopped().borrow().is_none(),
            "the claimer of range 0 sealed a's live log: {:?} (writing range 2: {wrote:?})",
            a.wal.fail_stopped().borrow()
        );
        wrote
    }

    /// **Step one: an ambiguous segment CAS whose read-back fails.**
    ///
    /// 1. Node `a` holds range 2 (stripe 0, `stripes = 2`), one durable row.
    /// 2. `a` claims range 0. The PUT of `owners/range-0` is ambiguous (written, the caller
    ///    gets an error) and every GET of `owners/range-0` fails: `add_wal_segment` does not
    ///    know that the segment `(a's stripe-0 log, f)` reached the bucket ⇒ the range claim
    ///    fails, the keeper has no `shared`, and does not mark the segment "unserved".
    /// 3. The store recovers; the keeper releases: the CAS loses (the version changed), the
    ///    read-back finds its own record ⇒ the two segment lists are merged.
    ///
    /// Must see: the released owner record does **not** list `a`'s live log (that log has
    /// no frame of range 0), and node `b` claiming range 0 does not bring `a` down. Before
    /// the fix: the merge kept the foreign segment ⇒ `b` sealed `a`'s live log ⇒ the next
    /// write on stripe 0 met the seal, `a` fail-stopped.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_segment_cas_then_release_does_not_list_the_live_log() -> Result<()> {
        let _ctl = StepController::new();
        let b = bucket();
        let a = node(&b, "f4-a").await?;
        let (mut w2, _k2) = acquire(&b, &a, 2).await?;
        put(&mut w2, 1).await?;
        let live = a.wal.log_of(0);
        ensure!(live == a.wal.log_of(2), "0 and 2 must share a stripe");
        // 2.
        let (_g, mut k) = claim(&a, 0).await?;
        let path = || PathMatch::Contains("owners/range-0".into());
        let amb = b.faulty.add_rule(Rule::new(
            OpSet::PUT,
            path(),
            Action::Ambiguous {
                reply: Ambiguity::Error,
                skip: 0,
                times: Some(1),
            },
        ));
        let blind = b
            .faulty
            .add_rule(Rule::new(OpSet::GET, path(), Action::Fail { times: None }));
        let r = acquire_shared(&b.stores, &a.wal, &mut k, 0).await;
        b.faulty.remove_rule(blind);
        b.faulty.remove_rule(amb);
        ensure!(r.is_err(), "the range claim must fail");
        let rec = owner(&b, 0).await?;
        ensure!(
            lists(&rec, &live) && !k.wal().segments.iter().any(|s| s.log == live),
            "bad setup: the segment must reach the bucket without the keeper knowing: bucket {rec:?}, keeper {:?}",
            k.wal()
        );
        // 3.
        k.release().await;
        let rec = owner(&b, 0).await?;
        ensure!(rec.released, "the release must reach the bucket: {rec:?}");
        ensure!(
            !lists(&rec, &live),
            "the released record lists a's live log: {rec:?}"
        );
        another_node_takes_range_0(&b, &a, &mut w2).await
    }

    /// **The next takeover meets exactly our own segment again.**
    ///
    /// 1. Node `a` holds range 2 (stripe 0), one durable row.
    /// 2. `a` claims range 0, the replay flush stalls (`flush_stall`) past the Db open cap:
    ///    segment `S = (a's stripe-0 log, f)` was CASed (we read it while stalled), the
    ///    claim fails, the keeper releases — the unserved segment is dropped (correct
    ///    today).
    /// 3. The released owner record still lists `S` — the shape step one left behind on
    ///    the code before the fix, or a self-takeover applying a pending release on a
    ///    record diverging from the keeper (keeping its segments): written directly on the
    ///    bucket.
    /// 4. `a` claims range 0 again: `take_over` skips `S` (the stripe has no frame of the
    ///    range yet), `add_wal_segment(S)` sees the last segment is already `S` ⇒ returns
    ///    early; the flush stalls past the cap again ⇒ the keeper releases.
    ///
    /// Must see: the released record does not list `a`'s live log, and node `b` claiming
    /// range 0 does not bring `a` down. Before the fix: the early-return branch did not
    /// mark "unserved", the keeper had no `shared` so it did not rotate ⇒ a dirty release
    /// kept `S` ⇒ `b` sealed `a`'s live log, `a` fail-stopped.
    #[tokio::test(flavor = "multi_thread")]
    async fn reclaim_meeting_its_own_unserved_segment_does_not_list_the_live_log() -> Result<()> {
        let ctl = StepController::new();
        let b = bucket();
        let a = node(&b, "f4-a").await?;
        let (mut w2, _k2) = acquire(&b, &a, 2).await?;
        put(&mut w2, 1).await?;
        let live = a.wal.log_of(0);
        ensure!(live == a.wal.log_of(2), "0 and 2 must share a stripe");
        let stall = ctl.fail("nodewal::acquire::flush_stall", "return(0)");
        // 2.
        let (_g, mut k) = claim(&a, 0).await?;
        let stores = b.stores.clone();
        let wal = a.wal.clone();
        let attempt = tokio::spawn(async move {
            let r = acquire_shared(&stores, &wal, &mut k, 0).await;
            (r.map(|_| ()), k)
        });
        let mut seg = None;
        for _ in 0..200 {
            let rec = owner(&b, 0).await?;
            seg = rec.wal_segments.iter().find(|s| s.log == live).cloned();
            if seg.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let seg = seg.ok_or_else(|| anyhow!("a's segment did not reach the owner record"))?;
        let (r, k) = attempt.await?;
        ensure!(r.is_err(), "the stalled range claim must fail past the cap");
        k.release().await;
        let rec = owner(&b, 0).await?;
        ensure!(
            rec.released && !lists(&rec, &live),
            "the release after a failed claim must drop the unserved segment: {rec:?}"
        );
        // 3.
        let mut stale = rec.clone();
        stale.wal_segments = vec![seg];
        b.faulty
            .inner()
            .put(
                &Path::from("cairn/owners/range-0"),
                PutPayload::from(stale.encode()),
            )
            .await?;
        // 4.
        let (_g, mut k) = claim(&a, 0).await?;
        ensure!(
            lists(&owner(&b, 0).await?, &live),
            "bad setup: the record must list S"
        );
        let r = acquire_shared(&b.stores, &a.wal, &mut k, 0).await;
        ensure!(r.is_err(), "the stalled range claim must fail past the cap");
        ensure!(
            a.wal.fail_stopped().borrow().is_none(),
            "the retaken claim brought a down: {:?}",
            a.wal.fail_stopped().borrow()
        );
        k.release().await;
        drop(stall);
        let rec = owner(&b, 0).await?;
        ensure!(rec.released, "the release must reach the bucket: {rec:?}");
        ensure!(
            !lists(&rec, &live),
            "the released record lists a's live log: {rec:?}"
        );
        another_node_takes_range_0(&b, &a, &mut w2).await
    }
}
