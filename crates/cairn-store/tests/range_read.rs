// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The query layer's read handle and the three library assumptions it relies
//! on:
//!
//! 1. snapshot + `durability_filter = Remote` does not see rows not yet durable;
//! 2. a snapshot keeps its image across a memtable flush;
//! 3. reading through a clone of a closed Db returns an error (not a stale
//!    image passed off as real).

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{ensure, Result},
    cairn_store::{
        query_index::{QueryCounts, QUERY_INDEXES},
        range_read::RangeReadHandle,
        LeadershipGrant, PartitionId, RangeEntry,
    },
    slatedb::{
        config::Settings,
        object_store::{memory::InMemory, ObjectStore},
        Db,
    },
    std::{sync::Arc, time::Duration},
};

/// A Db that does not flush on a timer: rows become durable only when the test flushes.
async fn manual_db() -> Result<Db> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Ok(Db::builder("range", store)
        .with_settings(Settings {
            flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?)
}

fn handle(db: &Db) -> RangeReadHandle {
    RangeReadHandle::new(
        db.clone(),
        LeadershipGrant::single_node(PartitionId(0)).validity_handle(),
        RangeEntry::new(0, u64::MAX, PartitionId(0)),
        Arc::new(QueryCounts::default()),
        QUERY_INDEXES,
    )
}

/// The fork's `put` does not wait for durability (the returned handle is what to wait on).
async fn put_unflushed(db: &Db, k: &[u8], v: &[u8]) -> Result<()> {
    db.put(k, v).await?;
    Ok(())
}

async fn keys(h: &RangeReadHandle) -> Result<Vec<Vec<u8>>> {
    let snap = h.snapshot()?;
    Ok(snap
        .scan(vec![0x00], vec![0xFF], 1000, false, false)
        .await?
        .into_iter()
        .map(|(k, _)| k.to_vec())
        .collect())
}

/// **The query layer sees only durable rows** (assumption 1). Write `a` without
/// waiting for durability; scan through the handle; flush the WAL; scan again.
///
/// Must see: the first time no `a` (and `get` returns `None`); after the flush,
/// `a`.
#[tokio::test]
async fn query_sees_only_durable_rows() -> Result<()> {
    let db = manual_db().await?;
    let h = handle(&db);
    put_unflushed(&db, b"\x00a", b"1").await?;
    ensure!(keys(&h).await?.is_empty(), "saw a row not yet durable");
    ensure!(h.snapshot()?.get(b"\x00a").await?.is_none());
    db.flush().await?;
    ensure!(
        keys(&h).await? == vec![b"\x00a".to_vec()],
        "did not see a durable row"
    );
    db.close().await?;
    Ok(())
}

/// **A snapshot is one image** (assumption 2), even across a memtable flush.
/// Write + flush `a`; snapshot; write + flush `b`, flush the memtable; scan the
/// old image and a new one.
///
/// Must see: the old image has only `a`; the new image has `a`, `b`.
#[tokio::test]
async fn snapshot_holds_across_memtable_flush() -> Result<()> {
    let db = manual_db().await?;
    let h = handle(&db);
    put_unflushed(&db, b"\x00a", b"1").await?;
    db.flush().await?;
    let old = h.snapshot()?;
    put_unflushed(&db, b"\x00b", b"2").await?;
    db.flush().await?;
    db.flush_with_options(slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    })
    .await?;
    let seen: Vec<_> = old
        .scan(vec![0x00], vec![0xFF], 10, false, false)
        .await?
        .into_iter()
        .map(|(k, _)| k.to_vec())
        .collect();
    ensure!(seen == vec![b"\x00a".to_vec()], "image changed: {seen:?}");
    ensure!(keys(&h).await?.len() == 2);
    // Scan backwards (the API's `ASC` order on `!t` keys).
    let back: Vec<_> = h
        .snapshot()?
        .scan(vec![0x00], vec![0xFF], 10, true, false)
        .await?
        .into_iter()
        .map(|(k, _)| k.to_vec())
        .collect();
    ensure!(
        back == vec![b"\x00b".to_vec(), b"\x00a".to_vec()],
        "{back:?}"
    );
    db.close().await?;
    Ok(())
}

/// **The handle can no longer read after it is detached or after the Db
/// closes** (detach before closing; assumption 3).
///
/// Must see: detached ⇒ `alive` is false, `snapshot` fails; Db closed ⇒ a read
/// through the clone (handle not detached) returns an error, not an image.
#[tokio::test]
async fn read_handle_gone_after_seal() -> Result<()> {
    let db = manual_db().await?;
    put_unflushed(&db, b"\x00a", b"1").await?;
    db.flush().await?;
    let h = handle(&db);
    ensure!(h.alive() && keys(&h).await?.len() == 1);
    let closed = handle(&db);
    closed.close();
    ensure!(
        !closed.alive() && closed.snapshot().is_err(),
        "detached yet still reading"
    );

    let snap = h.snapshot()?;
    db.close().await?;
    let after = tokio::time::timeout(
        Duration::from_secs(5),
        snap.scan(vec![0x00], vec![0xFF], 10, false, false),
    )
    .await?;
    ensure!(
        after.is_err(),
        "a read through an image of a closed Db must fail: {after:?}"
    );
    ensure!(h.snapshot().is_err(), "a snapshot of a closed Db must fail");
    Ok(())
}
