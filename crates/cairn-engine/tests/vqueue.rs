// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Virtual queues: the place in the queue, and stage changes.
//!
//! Three properties, and the first is the easiest to break because it only goes
//! wrong **after a leader change**:
//!
//! 1. **`seq` is never reissued for a live entry.** The counter lives in RAM
//!    (it is not pending work — it can be rebuilt), so a new leader must seed
//!    it from the store. Without seeding it reissues from 1 and overwrites the
//!    entries of the invocations just recovered.
//! 2. An entry and its **reverse index row** always go together.
//! 3. `Running` → `Inbox` on becoming leader, and it does not touch entries at
//!    other stages.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, Target},
    cairn_runtime::StartParams,
    cairn_store::{LeadershipGrant, PartitionId, PartitionWriter, QueueName, Stage},
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

/// Opens an engine on `root`. Calling it again on the same `root` is a **leader
/// change**: the store is intact, RAM is not.
async fn open_engine(root: &PathBuf) -> Result<Engine> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    let db = Db::builder(DB, store)
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?)
}

fn queue() -> QueueName {
    QueueName {
        tenant: cairn_store::TenantId::default(),
        scope: Some(cairn_store::Scope::new("acme").unwrap()),
        limit: None,
        exclusive: false,
        service: "Work".into(),
        object_key: String::new(),
    }
}

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: Some(cairn_store::Scope::new("acme").unwrap()),
        service: "Work".into(),
        handler: "run".into(),
        object_key: String::new(),
    }
}

fn id(n: u8) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[15] = n;
    b
}

/// Creates `n` invocations in the same queue, returns the `seq` of each.
async fn create_many(engine: &mut Engine, ids: &[[u8; 24]]) -> Result<Vec<u64>> {
    let q = queue();
    let mut seqs = Vec::new();
    for i in ids {
        let seq = engine.next_queue_seq(&q).await?;
        engine
            .create(
                *i,
                &target(),
                CreateOptions {
                    queue: Some((&q, seq)),
                    ..Default::default()
                },
                StartParams::default(),
                Bytes::from_static(b"x"),
            )
            .await?;
        seqs.push(seq);
    }
    Ok(seqs)
}

/// **A leader change must not reissue the `seq` of a live entry.**
///
/// This is the case where an in-RAM counter breaks, and it breaks silently: a
/// new entry overwrites an old entry at the same key `(queue, stage, seq)`, so
/// the queue loses a place. The worst consequence does not show at once but at
/// **the next leader change**: an overwritten `Suspended` entry is an EXCLUSIVE
/// lock with no evidence left to rebuild it, and two invocations run in parallel
/// on one virtual object.
#[tokio::test]
async fn leader_change_does_not_reissue_live_seq() -> Result<()> {
    let root = fresh_root("vq-seq")?;

    let old_ids: Vec<[u8; 24]> = (1..=3u8).map(id).collect();
    {
        let mut engine = open_engine(&root).await?;
        let seqs = create_many(&mut engine, &old_ids).await?;
        assert_eq!(
            seqs,
            vec![1, 2, 3],
            "seq must increase steadily within one leader"
        );
        engine.seal().await?;
    }

    // ── A new leader. RAM is lost; the store still has three entries at `Inbox`.
    let mut engine = open_engine(&root).await?;
    let new_ids: Vec<[u8; 24]> = (10..=12u8).map(id).collect();
    let seqs = create_many(&mut engine, &new_ids).await?;
    assert!(
        seqs.iter().all(|s| *s > 3),
        "the new leader reissued the seq of a live entry: {seqs:?}"
    );

    // And all six entries are still there — none was overwritten.
    let rows = engine.all_vqueues().await?;
    assert_eq!(
        rows.len(),
        6,
        "entries lost after the leader change: {}",
        rows.len()
    );
    let mut ids: Vec<[u8; 24]> = rows.iter().map(|r| r.id).collect();
    ids.sort();
    let mut want: Vec<[u8; 24]> = old_ids.iter().chain(new_ids.iter()).copied().collect();
    want.sort();
    assert_eq!(ids, want);

    // Each one still maps back to exactly its own place.
    for i in old_ids.iter().chain(new_ids.iter()) {
        let slot = engine.vqueue_of(i).await?.expect("there must be an entry");
        assert_eq!(slot.stage, Stage::Inbox);
        assert_eq!(slot.name, queue());
    }
    engine.seal().await?;
    Ok(())
}

/// A stage change keeps the `seq`, and the index follows.
///
/// `seq` is the place in the queue; suspending and waking up **must not** push
/// the invocation to the back of the queue.
#[tokio::test]
async fn stage_change_keeps_queue_position() -> Result<()> {
    let root = fresh_root("vq-stage")?;
    let mut engine = open_engine(&root).await?;
    let ids: Vec<[u8; 24]> = (1..=2u8).map(id).collect();
    create_many(&mut engine, &ids).await?;

    engine.set_stage(&ids[0], Stage::Running).await?;
    let slot = engine.vqueue_of(&ids[0]).await?.unwrap();
    assert_eq!((slot.stage, slot.seq), (Stage::Running, 1));

    engine.set_stage(&ids[0], Stage::Suspended).await?;
    let slot = engine.vqueue_of(&ids[0]).await?.unwrap();
    assert_eq!(
        (slot.stage, slot.seq),
        (Stage::Suspended, 1),
        "suspending and waking up pushed it to the back of the queue"
    );
    // And there is only **one** entry for it — a stage change is delete old + write new.
    let rows = engine.all_vqueues().await?;
    assert_eq!(rows.iter().filter(|r| r.id == ids[0]).count(), 1);
    assert_eq!(rows.len(), 2);

    // Dequeued for good: the entry and the index disappear together.
    engine.dequeue(&ids[0]).await?;
    assert!(engine.vqueue_of(&ids[0]).await?.is_none());
    assert_eq!(engine.all_vqueues().await?.len(), 1);
    engine.seal().await?;
    Ok(())
}

/// **Becoming leader:** `Running` → `Inbox`, and **only** `Running`.
///
/// `Suspended`/`Paused` must stay where they are: they still hold the VO lock,
/// and pushing them back to `Inbox` would declare the lock released.
#[tokio::test]
async fn becoming_leader_moves_running_to_inbox_and_nothing_else() -> Result<()> {
    let root = fresh_root("vq-requeue")?;
    let mut engine = open_engine(&root).await?;
    let ids: Vec<[u8; 24]> = (1..=4u8).map(id).collect();
    create_many(&mut engine, &ids).await?;
    engine.set_stage(&ids[0], Stage::Running).await?;
    engine.set_stage(&ids[1], Stage::Suspended).await?;
    engine.set_stage(&ids[2], Stage::Paused).await?;
    // ids[3] stays at `Inbox`.

    let moved = engine.requeue_running().await?;
    assert_eq!(moved, 1, "only the Running entry may be pushed back");

    let stage_of = |rows: &[cairn_engine::VQueueRow], i: [u8; 24]| {
        rows.iter().find(|r| r.id == i).map(|r| r.slot.stage)
    };
    let rows = engine.all_vqueues().await?;
    assert_eq!(rows.len(), 4, "requeue lost entries");
    assert_eq!(stage_of(&rows, ids[0]), Some(Stage::Inbox));
    assert_eq!(
        stage_of(&rows, ids[1]),
        Some(Stage::Suspended),
        "a Suspended entry pushed back to Inbox — its VO lock was just declared released"
    );
    assert_eq!(stage_of(&rows, ids[2]), Some(Stage::Paused));
    assert_eq!(stage_of(&rows, ids[3]), Some(Stage::Inbox));

    // The place in the queue does not change: pushing back to `Inbox` is not requeueing.
    assert_eq!(engine.vqueue_of(&ids[0]).await?.unwrap().seq, 1);
    engine.seal().await?;
    Ok(())
}
