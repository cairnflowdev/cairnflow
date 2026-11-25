// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `Schedules` table (`0x1A`) and the `ScheduleDue` index.
//!
//! The infrastructure behind the gate: writing/reading schedule rows, the next
//! fire deadline in the index (a paused schedule sits at `u64::MAX` — still
//! listable, never due), moving the deadline deletes the old index row, the
//! derived-zone rebuild restores the index, a creation carrying a schedule mark
//! writes the `cairnflow-schedule` header and the schedule row **in the same
//! commit**, a cross-range fire writes the `Fire` outbox row and the schedule
//! row in the same commit.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{
        schedule::{header_value, LastFire, LastKind, Overlap, ScheduleRow, SpecRow, HDR_SCHEDULE},
        CreateOptions, Engine, OutboundFire, OutboxEntry, ScheduleStamp, ScheduleWrite, Target,
    },
    cairn_runtime::StartParams,
    cairn_store::{
        schedule_id, schedule_run_id, LeadershipGrant, PartitionId, PartitionWriter, TenantId,
    },
    slatedb::{
        config::{FlushOptions, FlushType, Settings},
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "cairnflow-schedtable-{name}-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

fn settings() -> Settings {
    Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    }
}

async fn open_engine(store: &Arc<dyn ObjectStore>) -> Result<Engine> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?.with_schedules(true))
}

fn row(name: &str, next_at: u64) -> ScheduleRow {
    let mut r = ScheduleRow {
        name: name.into(),
        tenant: None,
        scope: None,
        spec: SpecRow::Every(60_000),
        service: "Reports".into(),
        handler: "build".into(),
        key: String::new(),
        limit_key: None,
        input: Bytes::from_static(b"{}"),
        paused: false,
        overlap: Overlap::Skip,
        jitter_ms: 0,
        next_at,
        fire_seq: 0,
        last: vec![],
        fired: 0,
        missed: 0,
        skipped: 0,
        hash: [0; 16],
        created_ms: 1,
        updated_ms: 1,
    };
    r.hash = r.definition_hash();
    r
}

fn sid(name: &str) -> [u8; 24] {
    schedule_id(&TenantId::default(), None, name)
}

/// Write, read, due, move the deadline, pause, delete.
#[tokio::test]
async fn write_read_due_reschedule_delete() -> Result<()> {
    let root = fresh_root("rw")?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let mut engine = open_engine(&store).await?;
    let (a, b, c) = (sid("a"), sid("b"), sid("c"));
    let (ra, rb) = (row("a", 1_000), row("b", 5_000));
    let mut rc = row("c", 0);
    rc.paused = true;
    for (id, due, r) in [(a, 1_000, &ra), (b, 5_000, &rb), (c, u64::MAX, &rc)] {
        engine
            .put_schedule(ScheduleWrite {
                id,
                old_due: None,
                due,
                row: r,
            })
            .await?;
    }
    assert_eq!(engine.schedule(&a).await?, Some((1_000, ra.clone())));
    let due: Vec<_> = engine
        .due_schedules(1_000, 16)
        .await?
        .into_iter()
        .map(|r| r.1)
        .collect();
    assert_eq!(due, vec![a], "only the due schedule");
    assert_eq!(engine.due_schedules(999, 16).await?.len(), 0);
    let all = engine.list_schedules(100).await?;
    assert_eq!(all.len(), 3, "a paused schedule is still listed");
    assert!(engine
        .due_schedules(u64::MAX - 1, 16)
        .await?
        .iter()
        .all(|r| r.1 != c));

    // Moving the deadline: the old index row goes along.
    let mut moved = ra.clone();
    moved.next_at = 61_000;
    engine
        .put_schedule(ScheduleWrite {
            id: a,
            old_due: Some(1_000),
            due: 61_000,
            row: &moved,
        })
        .await?;
    assert_eq!(
        engine
            .due_schedules(60_000, 16)
            .await?
            .iter()
            .map(|r| r.1)
            .collect::<Vec<_>>(),
        vec![b]
    );
    assert_eq!(
        engine.list_schedules(100).await?.len(),
        3,
        "no extra index rows"
    );

    engine.delete_schedule(&b, 5_000).await?;
    assert_eq!(engine.schedule(&b).await?, None);
    assert_eq!(engine.list_schedules(100).await?.len(), 2);
    engine.seal().await?;
    Ok(())
}

async fn wipe_derived(store: &Arc<dyn ObjectStore>) -> Result<usize> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let mut it = db.scan(vec![0x01u8]..vec![0x02u8]).await?;
    let mut keys = vec![];
    while let Some(kv) = it.next().await? {
        keys.push(kv.key);
    }
    for k in &keys {
        db.delete(k).await?;
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;
    Ok(keys.len())
}

/// The derived zone is deleted (split/merge) ⇒ the rebuild restores
/// `ScheduleDue` from the schedule rows, paused schedules included (the index
/// part of CG-14).
#[tokio::test]
async fn derived_rebuild_restores_schedule_index() -> Result<()> {
    let root = fresh_root("rebuild")?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let (a, c) = (sid("a"), sid("c"));
    {
        let mut engine = open_engine(&store).await?;
        let mut rc = row("c", 0);
        rc.paused = true;
        engine
            .put_schedule(ScheduleWrite {
                id: a,
                old_due: None,
                due: 1_000,
                row: &row("a", 1_000),
            })
            .await?;
        engine
            .put_schedule(ScheduleWrite {
                id: c,
                old_due: None,
                due: u64::MAX,
                row: &rc,
            })
            .await?;
        engine.seal().await?;
    }
    assert!(wipe_derived(&store).await? > 0);
    let mut engine = open_engine(&store).await?;
    assert_eq!(engine.list_schedules(100).await?.len(), 2);
    let due: Vec<_> = engine
        .due_schedules(1_000, 16)
        .await?
        .into_iter()
        .map(|r| r.1)
        .collect();
    assert_eq!(due, vec![a]);
    engine.seal().await?;
    Ok(())
}

/// A creation carrying a schedule mark: the `cairnflow-schedule` header in entry
/// 0 and the schedule row moving its deadline **in the same commit** (CG-12) —
/// reading back sees both.
#[tokio::test]
async fn create_with_schedule_mark_writes_header_and_row_in_one_commit() -> Result<()> {
    let root = fresh_root("stamp")?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let mut engine = open_engine(&store).await?;
    let a = sid("nightly");
    let r0 = row("nightly", 60_000);
    engine
        .put_schedule(ScheduleWrite {
            id: a,
            old_due: None,
            due: 60_000,
            row: &r0,
        })
        .await?;
    let run = schedule_run_id(&a, 60_000);
    let mut r1 = r0.clone();
    r1.next_at = 120_000;
    r1.fire_seq = 1;
    r1.fired = 1;
    r1.push_last(LastFire {
        at: 60_000,
        id: run,
        kind: LastKind::Fired,
    });
    let header = header_value("nightly", 60_000);
    let target = Target {
        tenant: Default::default(),
        scope: None,
        service: "Reports".into(),
        handler: "build".into(),
        object_key: String::new(),
    };
    engine
        .create(
            run,
            &target,
            CreateOptions {
                schedule: Some(ScheduleStamp {
                    header: &header,
                    row: Some(ScheduleWrite {
                        id: a,
                        old_due: Some(60_000),
                        due: 120_000,
                        row: &r1,
                    }),
                }),
                ..Default::default()
            },
            StartParams::default(),
            Bytes::from_static(b"{}"),
        )
        .await?;
    engine.seal().await?;

    let engine = open_engine(&store).await?;
    assert_eq!(engine.schedule(&a).await?, Some((120_000, r1)));
    let rows = engine.journal_entries(&run).await?;
    let raw = &rows[0].2;
    let needle = HDR_SCHEDULE;
    assert!(
        raw.windows(needle.len()).any(|w| w == needle.as_bytes())
            && raw.windows(header.len()).any(|w| w == header.as_bytes()),
        "entry 0 lacks the schedule header"
    );
    engine.seal().await?;
    Ok(())
}

/// A cross-range fire: the `Fire` outbox row keyed `(schedule id, fire_seq)` and
/// the schedule row moving its deadline in the same commit.
#[tokio::test]
async fn cross_range_fire_writes_fire_and_schedule_row_in_one_commit() -> Result<()> {
    let root = fresh_root("fire")?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let mut engine = open_engine(&store).await?;
    let a = sid("sweep");
    let mut r0 = row("sweep", 60_000);
    r0.key = "u1".into();
    r0.overlap = Overlap::Allow;
    engine
        .put_schedule(ScheduleWrite {
            id: a,
            old_due: None,
            due: 60_000,
            row: &r0,
        })
        .await?;
    let mut r1 = r0.clone();
    r1.next_at = 120_000;
    r1.fire_seq = 1;
    let fire = OutboundFire {
        schedule: a,
        fire_seq: 1,
        name: "sweep".into(),
        at: 60_000,
        tenant: None,
        scope: None,
        service: "Reports".into(),
        handler: "build".into(),
        object_key: "u1".into(),
        parameter: Bytes::from_static(b"{}"),
        limit_key: None,
        idempotency_key: "schedule/sweep/60000".into(),
    };
    engine
        .fire_schedule_remote(
            ScheduleWrite {
                id: a,
                old_due: Some(60_000),
                due: 120_000,
                row: &r1,
            },
            fire.clone(),
        )
        .await?;
    engine.seal().await?;
    let mut engine = open_engine(&store).await?;
    assert_eq!(engine.schedule(&a).await?.map(|(d, _)| d), Some(120_000));
    let rows = engine.pending_outbox(16).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, OutboxEntry::Fire(fire));
    assert_eq!(rows[0].0.key, (a, 1));
    engine.seal().await?;
    Ok(())
}
