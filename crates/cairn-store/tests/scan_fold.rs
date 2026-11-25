// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `PartitionReader::scan_fold` — the **whole-Db** scan of `cairn-ctl verify`
//! ((j) cross-range messages, (l) tenant residue, (m)/(n) segmented values) —
//! reads in batches, not one block at a time.
//!
//! In a chaos run with large values, two ranges carrying 3 MiB histories (tails
//! up to 256 KiB in the journal per turn) weighed ~520 MB; the final `verify`
//! scanned each range twice with SlateDB's default `ScanOptions` (one ~4 KiB
//! block per GET, one task) and did not finish in 1800 s. The same load without
//! large values took 308 s.
//!
//! A single test in this file: `MeteredStore`'s counters are per process.

use {
    cairn_store::{
        metered::{requests, MeteredStore, StoreOp},
        PartitionReader,
    },
    slatedb::{
        config::{FlushOptions, FlushType},
        object_store::{memory::InMemory, ObjectStore},
        Db, DbReader,
    },
    std::sync::Arc,
};

const DB: &str = "scan-fold";
/// 4096 rows × 1 KiB ≈ 4 MiB in one L0 SST — ~1000 blocks of 4 KiB.
const ROWS: usize = 4096;
const VALUE: usize = 1024;

#[tokio::test]
async fn scan_fold_fetches_blocks_in_bulk() -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder(DB, Arc::clone(&store)).build().await?;
    for i in 0..ROWS {
        db.put(format!("k{i:06}").as_bytes(), vec![b'v'; VALUE])
            .await?;
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;

    let metered: Arc<dyn ObjectStore> = MeteredStore::wrap(store);
    let reader = PartitionReader::new(DbReader::builder(DB, metered).build().await?);
    let before = requests(StoreOp::Get);
    let mut seen = 0usize;
    reader
        .scan_fold(&[], |_, v| {
            assert_eq!(v.len(), VALUE);
            seen += 1;
            true
        })
        .await?;
    let gets = requests(StoreOp::Get) - before;
    assert_eq!(seen, ROWS);
    // A 4 MiB read-ahead: the index + a few batch reads. One block at a time is ~1000 GETs.
    assert!(
        gets <= 32,
        "scan_fold sent {gets} GETs for ~{} KiB — reading one block at a time",
        (ROWS * VALUE) >> 10
    );
    Ok(())
}
