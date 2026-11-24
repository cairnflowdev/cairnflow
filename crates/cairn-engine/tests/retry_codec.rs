// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The lifecycle core's retry-row encoding (`cairn_lifecycle::codec`) matches
//! the engine **byte for byte**, in both directions:
//!
//! 1. every engine write path (`schedule_retry`, `reschedule_retry`,
//!    `defer_retries`, `begin_turn` watchdog/scheduled, `end_turn`,
//!    `undefer_retry`) leaves exactly the payload the codec encodes for the
//!    same row, at the width the engine uses;
//! 2. rows the codec writes (every width, including old 4-byte rows and
//!    watchdog rows carrying an attempt) are read by the engine with the right
//!    `attempt`/`journal_len`/`fires`/watchdog flag
//!    (`due_retries_fired`, `pending_retry`).
//!
//! This test was the fence for switching the engine over to the codec; it
//! keeps the two byte-identical.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    cairn_engine::Engine,
    cairn_lifecycle::codec::{RetryRow, Width},
    cairn_store::{
        keys, make_invocation_id, LeadershipGrant, Mutations, PartitionId, PartitionKey,
        PartitionWriter, Stage,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

async fn open_writer(root: &PathBuf) -> Result<PartitionWriter> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    let db = Db::builder("partition-0", store)
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build()
        .await?;
    Ok(PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?)
}

fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

/// The retry entity row of `id`, raw: `(due, payload)`.
async fn raw(engine: &Engine, id: &[u8; 24]) -> Result<(u64, Vec<u8>)> {
    let v = engine
        .writer()
        .get(&keys::retry(id))
        .await?
        .expect("retry row present");
    let (due, payload) = keys::decode_due(&v).expect("due ‖ payload");
    // The full value is also that row's `RetryRow::encode_value`.
    let row = RetryRow::decode_value(&v).expect("codec decodes the engine's value");
    assert_eq!(row.due, due);
    Ok((due, payload.to_vec()))
}

/// The payload the engine wrote = the codec's encoding of that row at width `w`.
async fn expect(engine: &Engine, id: &[u8; 24], row: RetryRow, w: Width, path: &str) -> Result<()> {
    let (due, payload) = raw(engine, id).await?;
    assert_eq!(due, row.due, "{path}: deadline");
    assert_eq!(
        payload,
        row.encode_as(w).expect("width fits the row").as_slice(),
        "{path}: payload"
    );
    assert_eq!(RetryRow::decode(due, &payload), Ok(row), "{path}: decode");
    // Rows the engine writes are always at the codec's canonical width — except
    // `defer_retries` (13 bytes even when `fires` = 0).
    if path != "defer_retries" {
        assert_eq!(row.canonical_width(), w, "{path}: canonical width");
    }
    Ok(())
}

#[tokio::test]
async fn engine_write_path_matches_codec_byte_for_byte() -> Result<()> {
    let root = fresh_root("retry_codec_write")?;
    let mut engine = Engine::open(open_writer(&root).await?).await?;
    let id = inv_id(1);

    engine.schedule_retry(&id, 100, 3, 10).await?;
    expect(
        &engine,
        &id,
        RetryRow::retry(100, 3, 10, 0),
        Width::W8,
        "schedule_retry",
    )
    .await?;

    engine.reschedule_retry(&id, 100, 200, 3, 10, 2).await?;
    expect(
        &engine,
        &id,
        RetryRow::retry(200, 3, 10, 2),
        Width::W13,
        "reschedule_retry",
    )
    .await?;

    engine.defer_retries(&[(id, 200, 300, 3, 10, 0)]).await?;
    expect(
        &engine,
        &id,
        RetryRow::retry(300, 3, 10, 0),
        Width::W13,
        "defer_retries",
    )
    .await?;

    // `undefer_retry` keeps the payload, only changes the deadline.
    engine.reschedule_retry(&id, 300, 900, 4, 11, 5).await?;
    assert!(engine.undefer_retry(&id, 700, 0).await?);
    expect(
        &engine,
        &id,
        RetryRow::retry(700, 4, 11, 5),
        Width::W13,
        "undefer_retry",
    )
    .await?;

    // A `Running` commit without a queue entry: only the row.
    engine.begin_turn(&id, Some(400), None).await?;
    expect(
        &engine,
        &id,
        RetryRow::legacy_watch(400),
        Width::W9,
        "begin_turn watch",
    )
    .await?;

    engine.begin_turn(&id, None, Some((500, 4, 11))).await?;
    expect(
        &engine,
        &id,
        RetryRow::retry(500, 4, 11, 0),
        Width::W8,
        "begin_turn scheduled",
    )
    .await?;

    engine
        .end_turn(&id, false, Stage::Suspended, None, Some((600, 5, 12)))
        .await?;
    expect(
        &engine,
        &id,
        RetryRow::retry(600, 5, 12, 0),
        Width::W8,
        "end_turn",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn engine_reads_rows_written_by_codec() -> Result<()> {
    let root = fresh_root("retry_codec_doc")?;
    let mut writer = open_writer(&root).await?;
    #[rustfmt::skip]
    let rows: &[(u8, RetryRow, Width)] = &[
        // a 4-byte row from an old binary: journal_len unknown
        (1, RetryRow::retry(10, 7, u32::MAX, 0), Width::W4),
        (2, RetryRow::retry(11, 3, 10, 0),       Width::W8),
        (3, RetryRow::legacy_watch(12),          Width::W9),
        (4, RetryRow::retry(13, 3, 10, 2),       Width::W13),
        // a watchdog row carrying an attempt (the core, the reverse direction):
        // the same 9-byte layout, readable by today's binary
        (5, RetryRow::watch(14, 2, 6),           Width::W9),
        // a watchdog with `fires` (13 bytes, flag 1)
        (6, RetryRow { fires: 4, ..RetryRow::watch(15, 1, 6) }, Width::W13),
    ];
    let mut muts = Mutations::new();
    for (n, row, w) in rows {
        muts.add_retry(row.due, &inv_id(*n), row.encode_as(*w).unwrap().as_slice());
    }
    writer.commit(muts).await?;
    let mut engine = Engine::open(writer).await?;

    let mut due = engine.due_retries_fired(u64::MAX, 64).await?;
    due.sort_by_key(|(d, ..)| *d);
    assert_eq!(due.len(), rows.len());
    for ((d, id, attempt, len, fires), (n, row, w)) in due.iter().zip(rows) {
        assert_eq!(*id, inv_id(*n), "{w:?}");
        assert_eq!(
            (*d, *attempt, *len, *fires),
            (row.due, row.attempt, row.journal_len, row.fires),
            "engine reads row {w:?} {row:?}"
        );
        // `pending_retry` skips watchdog rows (FL-17) — the same question as the codec.
        assert_eq!(
            engine.pending_retry(id).await?,
            (!row.is_watch()).then_some(row.due),
            "watchdog {row:?}"
        );
    }
    Ok(())
}
