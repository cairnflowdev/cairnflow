// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The store primitives of split/merge: really closing a Db, clone/union with a
//! projection by partition-key range, and discarding orphan Dbs on roll-back.
//!
//! The safety net under all of them: cloning a Db that is **still open**
//! silently misses data that lives only in the WAL. Since the fork's patch F3,
//! SlateDB refuses such a source itself; the tests here pin that through
//! cairn's path.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::Result,
    cairn_store::{
        split_keeps_l0_headroom,
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        Durability, LeadershipGrant, Mutations, ObjectStores, PartitionId, PartitionKey,
        PartitionWriter, StoreLocation,
    },
    futures::TryStreamExt,
    slatedb::{
        admin::AdminBuilder,
        config::{FlushOptions, FlushType},
        object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
        CloneSourceSpec,
    },
    std::sync::Arc,
};

const INV: [u8; 24] = [0x42; 24];

fn memory_stores() -> ObjectStores {
    ObjectStores::single(StoreLocation::parse("memory://").expect("memory:// always parses"))
}

async fn writer(stores: &ObjectStores, range: u32) -> Result<PartitionWriter> {
    Ok(PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(range)),
        stores.open_db(range).await?,
    )
    .await?)
}

/// Write one journal row and wait until it is **durable** (in the WAL on the store).
async fn write_durable(w: &mut PartitionWriter, payload: &[u8]) -> Result<()> {
    let mut m = Mutations::new();
    m.append_journal(&INV, 0, payload);
    w.commit(m).await?;
    match w.durability()? {
        Durability::Ready(_) => {}
        Durability::Wait(p) => {
            p.durable().await?;
        }
    }
    Ok(())
}

/// **An open source through cairn's path:** the parent is still open, acked
/// data lives only in the WAL ⇒ a clone **with a projection** must be refused,
/// never creating a child that misses data. Cairn's split always clones with a
/// projection (two ranges per child).
///
/// Green from the start thanks to F3 (SlateDB probes the real WAL tail before
/// accepting a projected source); this test is the regression fence for the
/// next fork rebase.
#[tokio::test]
async fn clone_refuses_unclosed_parent() -> Result<()> {
    let stores = memory_stores();
    let mut parent = writer(&stores, 0).await?;
    write_durable(&mut parent, b"acked").await?;

    let cloned = stores
        .clone_range(
            PartitionId(0),
            PartitionId(1),
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await;
    assert!(
        cloned.is_err(),
        "a projected clone of a Db still open (acked data only in the WAL) must be refused"
    );
    Ok(())
}

/// **Without a projection:** a clone **without** a projection, one source,
/// takes the WAL-copy path. It used to copy only the WAL range that the stored
/// manifest knew — with the parent still open the clone was accepted while the
/// child missed acked rows (measured: 0/1). The fork copies up to the real WAL
/// tail when cloning the latest state; this test keeps that hole closed.
///
/// Cairn does not take this path (a split always projects, and always closes
/// the parent first — [`cairn_store::PartitionWriter::seal`]), but acked rows
/// must not be lost on any path.
#[tokio::test]
async fn unprojected_clone_of_open_parent_keeps_acked_rows() -> Result<()> {
    let stores = memory_stores();
    let mut parent = writer(&stores, 0).await?;
    write_durable(&mut parent, b"acked").await?;

    let cloned = AdminBuilder::new(stores.range_path(1), stores.main().store().clone())
        .build()
        .create_clone_builder_from_source(CloneSourceSpec::new(stores.range_path(0)))
        .build()
        .await;
    if cloned.is_ok() {
        let child = writer(&stores, 1).await?;
        assert_eq!(
            child.journal_unbounded(&INV).await?.len(),
            1,
            "the clone was accepted while the child misses acked rows"
        );
    }
    Ok(())
}

/// **`seal` really closes the Db:** after `seal`, the parent can be cloned with
/// a projection — no WAL carries data the manifest does not cover yet (F3
/// refuses such a source) — and the child sees every acked row.
///
/// The procedure: flush the WAL → flush the memtable → close. A bare `close()`
/// after writing leaves the newest part in the WAL, and the clone is refused.
#[tokio::test]
async fn sealed_parent_clones_with_every_acked_row() -> Result<()> {
    let stores = memory_stores();
    let mut parent = writer(&stores, 0).await?;
    write_durable(&mut parent, b"acked").await?;
    parent.seal().await?;

    // Journal rows live in the bulk region (`0x02`) — inside the projection of
    // the whole key space.
    stores
        .clone_range(
            PartitionId(0),
            PartitionId(1),
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    let child = writer(&stores, 1).await?;
    assert_eq!(
        child.journal_unbounded(&INV).await?.len(),
        1,
        "the child misses acked rows"
    );
    Ok(())
}

// ─────────────────────────────────────────── projection by range

/// A key in the three-region layout: `region ‖ pk(u64 BE) ‖ tail`.
fn region_key(region: u8, pk: u64, tail: &str) -> Vec<u8> {
    let mut k = vec![region];
    k.extend_from_slice(&pk.to_be_bytes());
    k.extend_from_slice(tail.as_bytes());
    k
}

const LO: u64 = 10;
const HI: u64 = u64::MAX - 10;
const MID: u64 = 1 << 63;

/// Build range 0's Db by hand in the three-region layout — raw bytes, so the
/// test is about the projection, not the engine's tables: two pks in the two
/// halves of the key space, each with one hot row and one bulk row, plus one row
/// in the derived region. Closed with exactly `seal`'s procedure.
async fn three_region_parent(stores: &ObjectStores) -> Result<()> {
    let db = stores.open_db(0).await?;
    for pk in [LO, HI] {
        db.put(region_key(0x00, pk, "hot"), b"n").await?;
        db.put(region_key(0x02, pk, "bulk"), b"k").await?;
    }
    db.put([0x01, 0x07, 0x42], b"index").await?;
    for flush_type in [FlushType::Wal, FlushType::MemTable] {
        db.flush_with_options(FlushOptions { flush_type }).await?;
    }
    db.close().await?;
    Ok(())
}

/// Every key in a range's Db, read and then closed cleanly (so it can serve as a source).
async fn keys_of(stores: &ObjectStores, range: u32) -> Result<Vec<Vec<u8>>> {
    let db = stores.open_db(range).await?;
    let mut out = Vec::new();
    {
        let mut it = db.scan::<std::ops::RangeFull>(..).await?;
        while let Some(kv) = it.next().await? {
            out.push(kv.key.to_vec());
        }
    }
    for flush_type in [FlushType::Wal, FlushType::MemTable] {
        db.flush_with_options(FlushOptions { flush_type }).await?;
    }
    db.close().await?;
    Ok(out)
}

/// `clone_range` keeps exactly the two regions of the pk interval, dropping the
/// derived region and pks outside the interval; `union_ranges` merges two
/// children into every entity row, the derived region still clean;
/// `discard_range_db` deletes an orphan child together with the checkpoint it
/// left on the parent.
#[tokio::test]
async fn clone_union_and_discard_follow_the_pk_span() -> Result<()> {
    let stores = memory_stores();
    three_region_parent(&stores).await?;
    let (l, r) = (PartitionId(1), PartitionId(2));
    stores
        .clone_range(PartitionId(0), l, (PartitionKey(0), PartitionKey(MID - 1)))
        .await?;
    stores
        .clone_range(
            PartitionId(0),
            r,
            (PartitionKey(MID), PartitionKey(u64::MAX)),
        )
        .await?;
    assert_eq!(
        keys_of(&stores, l.0).await?,
        [region_key(0x00, LO, "hot"), region_key(0x02, LO, "bulk")],
        "left child"
    );
    assert_eq!(
        keys_of(&stores, r.0).await?,
        [region_key(0x00, HI, "hot"), region_key(0x02, HI, "bulk")],
        "right child — the last range covers up to u64::MAX"
    );

    let merged = PartitionId(3);
    stores
        .union_ranges(
            &[
                (l, (PartitionKey(0), PartitionKey(MID - 1))),
                (r, (PartitionKey(MID), PartitionKey(u64::MAX))),
            ],
            merged,
        )
        .await?;
    assert_eq!(
        keys_of(&stores, merged.0).await?,
        [
            region_key(0x00, LO, "hot"),
            region_key(0x00, HI, "hot"),
            region_key(0x02, LO, "bulk"),
            region_key(0x02, HI, "bulk"),
        ],
        "union: every entity row, no derived region"
    );

    // The left child becomes an orphan (as if the split died before the commit step): delete it.
    let parent = AdminBuilder::new(stores.range_path(0), stores.main().store().clone()).build();
    let before = parent.list_checkpoints(None).await?.len();
    let finals: Vec<_> = AdminBuilder::new(stores.range_path(l.0), stores.main().store().clone())
        .build()
        .read_manifest(None)
        .await?
        .expect("the child has a manifest")
        .external_dbs()
        .iter()
        .filter_map(|e| e.final_checkpoint_id)
        .collect();
    assert!(!finals.is_empty(), "child must leave a parent checkpoint");
    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(l),
            &[PartitionId(0)],
            None,
        )
        .await?;
    let after = parent.list_checkpoints(None).await?;
    assert!(
        after.len() < before,
        "the orphan child's checkpoint is still on the parent"
    );
    assert!(after.iter().all(|c| !finals.contains(&c.id)));
    let left: Vec<_> = stores
        .main()
        .store()
        .list(Some(&stores.range_path(l.0)))
        .try_collect()
        .await?;
    assert!(
        left.is_empty(),
        "the orphan child's Db still has {} objects",
        left.len()
    );
    // Idempotent.
    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(l),
            &[PartitionId(0)],
            None,
        )
        .await?;
    // The right child is untouched.
    assert_eq!(keys_of(&stores, r.0).await?.len(), 2);
    Ok(())
}

/// **Visible size** (fork patch F6): a projected child keeps views onto the
/// parent's physical SSTs, so by `estimate_size` the child looks as large as the
/// parent. `range_size` counts the visible part — the left child of a split in
/// two over evenly spread data must be about half the parent, and its pieces lie
/// only in its half.
#[tokio::test]
async fn range_size_counts_only_the_visible_part_of_projected_views() -> Result<()> {
    let stores = memory_stores();
    assert_eq!(
        stores.range_size(PartitionId(0)).await?,
        cairn_store::RangeSize::default(),
        "no Db yet ⇒ empty"
    );
    let db = stores.open_db(0).await?;
    let value = vec![7u8; 1024];
    for i in 0..256u64 {
        let pk = i * (u64::MAX / 256);
        db.put(region_key(0x00, pk, "hot"), &value).await?;
        db.put(region_key(0x02, pk, "bulk"), &value).await?;
    }
    for flush_type in [FlushType::Wal, FlushType::MemTable] {
        db.flush_with_options(FlushOptions { flush_type }).await?;
    }
    db.close().await?;

    let parent = stores.range_size(PartitionId(0)).await?;
    assert!(parent.visible_bytes > 256 * 1024, "cha: {parent:?}");
    stores
        .clone_range(
            PartitionId(0),
            PartitionId(1),
            (PartitionKey(0), PartitionKey(MID - 1)),
        )
        .await?;
    let child = stores.range_size(PartitionId(1)).await?;
    let ratio = child.visible_bytes as f64 / parent.visible_bytes as f64;
    assert!(
        (0.3..=0.7).contains(&ratio),
        "the left child must be about half the parent by visible size: child {} / parent {} = {ratio:.2}",
        child.visible_bytes,
        parent.visible_bytes
    );
    assert!(!child.pieces.is_empty(), "the child has pieces");
    assert!(
        child.pieces.iter().all(|(owner, _)| *owner < MID),
        "the left child's pieces must start in the left half: {:?}",
        child.pieces
    );
    Ok(())
}

/// The L0 precondition counts **views**: each L0 view of the source becomes up
/// to two views in the new Db, and the new Db must leave room for two flushes
/// below `l0_max_ssts`.
#[tokio::test]
async fn l0_headroom_counts_projected_views() -> Result<()> {
    let stores = memory_stores();
    three_region_parent(&stores).await?;
    let views = stores.range_l0_views(PartitionId(0)).await?;
    assert!(views >= 1, "freshly flushed parent must have L0");
    assert_eq!(
        stores.range_l0_views(PartitionId(9)).await?,
        0,
        "no Db yet ⇒ 0"
    );
    let max = stores.l0_max_ssts();
    assert!(split_keeps_l0_headroom(&[views], max));
    assert!(split_keeps_l0_headroom(&[3], 8));
    assert!(
        !split_keeps_l0_headroom(&[4], 8),
        "4 views ⇒ 8 views in the children: at the cap"
    );
    assert!(
        !split_keeps_l0_headroom(&[2, 2], 8),
        "merge adds the views of both sources"
    );
    Ok(())
}

/// Discarding an orphan child must **not** touch the checkpoints the child
/// inherited from its ancestors.
///
/// A one-hour chaos run: range 76 (a grandchild of an earlier split) failed to
/// split midway, the next claimer rolled back and discarded the orphan child.
/// The orphan's manifest lists 76's **ancestors** too (SlateDB copies the
/// source's `external_dbs` into the child), and the old code deleted the
/// checkpoint of every entry — including checkpoints that 76 itself, a **live**
/// range, stood on. From then on every split of 76 returned `500 checkpoint
/// missing`, forever; and the ancestors' GC was allowed to remove SSTs 76 still
/// read.
#[tokio::test]
async fn discarding_an_orphan_grandchild_keeps_the_live_parents_ancestry() -> Result<()> {
    let stores = memory_stores();
    three_region_parent(&stores).await?;
    let (l, r) = (PartitionId(1), PartitionId(2));
    stores
        .clone_range(PartitionId(0), l, (PartitionKey(0), PartitionKey(MID - 1)))
        .await?;
    stores
        .clone_range(
            PartitionId(0),
            r,
            (PartitionKey(MID), PartitionKey(u64::MAX)),
        )
        .await?;
    // `l` becomes a live range: open (initialising the child), then close cleanly to serve as a source.
    let l_keys = keys_of(&stores, l.0).await?;
    let needed: Vec<_> = AdminBuilder::new(stores.range_path(l.0), stores.main().store().clone())
        .build()
        .read_manifest(None)
        .await?
        .expect("l has a manifest")
        .external_dbs()
        .iter()
        .flat_map(|e| [e.final_checkpoint_id, Some(e.source_checkpoint_id)])
        .flatten()
        .collect();
    assert!(!needed.is_empty(), "l stands on range 0's checkpoint");

    // Splitting `l` dies midway: child 3 is cloned and never enters the table ⇒ discard.
    let orphan = PartitionId(3);
    stores
        .clone_range(l, orphan, (PartitionKey(0), PartitionKey(LO)))
        .await?;
    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(orphan),
            &[l],
            None,
        )
        .await?;

    let grand = AdminBuilder::new(stores.range_path(0), stores.main().store().clone()).build();
    let alive: Vec<_> = grand
        .list_checkpoints(None)
        .await?
        .into_iter()
        .map(|c| c.id)
        .collect();
    for id in &needed {
        assert!(
            alive.contains(id),
            "checkpoint {id} that the live range `l` stands on was deleted"
        );
    }
    // The next split of `l` must succeed.
    stores
        .clone_range(l, PartitionId(4), (PartitionKey(0), PartitionKey(LO)))
        .await?;
    assert_eq!(
        keys_of(&stores, 4).await?.len(),
        2,
        "l's new child has LO's rows"
    );
    assert_eq!(keys_of(&stores, l.0).await?, l_keys);
    Ok(())
}

/// Every `final_checkpoint_id` in the manifest of range `range`'s Db: the
/// checkpoints that range stands on at each ancestor (what `verify` checks, and
/// what the range's next clone uses as its source).
async fn finals_of(stores: &ObjectStores, range: u32) -> Result<Vec<(String, String)>> {
    Ok(
        AdminBuilder::new(stores.range_path(range), stores.main().store().clone())
            .build()
            .read_manifest(None)
            .await?
            .expect("the range has a manifest")
            .external_dbs()
            .iter()
            .filter_map(|e| {
                e.final_checkpoint_id
                    .map(|id| (e.path.clone(), id.to_string()))
            })
            .collect(),
    )
}

/// Whether checkpoint `id` is still on the Db at `path`.
async fn checkpoint_alive(stores: &ObjectStores, path: &str, id: &str) -> Result<bool> {
    Ok(
        AdminBuilder::new(Path::from(path), stores.main().store().clone())
            .build()
            .list_checkpoints(None)
            .await?
            .iter()
            .any(|c| c.id.to_string() == id),
    )
}

/// **SM-12 — rolling back a merge intent does not delete checkpoints that the
/// two live sources stand on** (a 20-minute chaos run: merge 40+41 rolled back,
/// after which `500 checkpoint missing` forever and `verify` reported
/// `external_db@40`).
///
/// Interleaving, following the chaos run's lineage (15 → 38 → 40, 41):
/// 1. Range 0 has rows at three pks (`LO`, `Q + 5`, `HI`); split into 1 and 2.
/// 2. Range 1 lives (open, close cleanly), then splits again into 3 and 4. Both
///    live but **have written nothing**: after the projection they own no SST
///    (the seal's `seal_barrier` key lies outside every range).
/// 3. The coordinator merges 3+4 → 5: the union completes (Db 5 has a
///    manifest), then it dies before the commit. Since 3 and 4 own no SST, 5's
///    manifest has **no** entry for 3 or 4 — only one entry for ancestor 0,
///    merged from the two entries inherited from 3 and 4, with
///    `source_checkpoint_id` = the final checkpoint of 3 (or 4) on 0.
/// 4. The range claimer rolls the intent back: `discard_range_db(5)`.
///
/// Must see: every final checkpoint 3 and 4 stand on remains; 5's own final
/// checkpoint is deleted (no leak); the next merge of 3+4 works and the merged
/// Db has every row of both.
#[tokio::test]
async fn discarding_an_orphan_merge_keeps_the_sources_ancestry() -> Result<()> {
    const Q: u64 = 1 << 62;
    let stores = memory_stores();
    let db = stores.open_db(0).await?;
    for pk in [LO, Q + 5, HI] {
        db.put(region_key(0x00, pk, "hot"), b"n").await?;
        db.put(region_key(0x02, pk, "bulk"), b"k").await?;
    }
    for flush_type in [FlushType::Wal, FlushType::MemTable] {
        db.flush_with_options(FlushOptions { flush_type }).await?;
    }
    db.close().await?;
    let (l, r) = (PartitionId(1), PartitionId(2));
    stores
        .clone_range(PartitionId(0), l, (PartitionKey(0), PartitionKey(MID - 1)))
        .await?;
    stores
        .clone_range(
            PartitionId(0),
            r,
            (PartitionKey(MID), PartitionKey(u64::MAX)),
        )
        .await?;
    keys_of(&stores, l.0).await?;
    let (a, b) = (PartitionId(3), PartitionId(4));
    let (a_span, b_span) = (
        (PartitionKey(0), PartitionKey(Q - 1)),
        (PartitionKey(Q), PartitionKey(MID - 1)),
    );
    stores.clone_range(l, a, a_span).await?;
    stores.clone_range(l, b, b_span).await?;
    assert_eq!(keys_of(&stores, a.0).await?.len(), 2, "3 has LO's rows");
    assert_eq!(keys_of(&stores, b.0).await?.len(), 2, "4 has Q + 5's rows");
    let mut needed = finals_of(&stores, a.0).await?;
    needed.extend(finals_of(&stores, b.0).await?);
    assert!(
        !needed.is_empty(),
        "3 and 4 stand on the ancestors' checkpoints"
    );

    // Merging 3+4 dies after the union, before the commit ⇒ 5 is an orphan ⇒ roll back and discard.
    let orphan = PartitionId(5);
    stores
        .union_ranges(&[(a, a_span), (b, b_span)], orphan)
        .await?;
    let own = finals_of(&stores, orphan.0).await?;
    assert!(
        !own.is_empty(),
        "5 must leave a final checkpoint on the ancestors"
    );
    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(orphan),
            &[a, b],
            None,
        )
        .await?;

    for (path, id) in &needed {
        assert!(
            checkpoint_alive(&stores, path, id).await?,
            "checkpoint {id} on {path} that the live ranges 3/4 stand on was deleted"
        );
    }
    for (path, id) in &own {
        assert!(
            !checkpoint_alive(&stores, path, id).await?,
            "the orphan child's final checkpoint {id} is still on {path}"
        );
    }
    for live in [a, b] {
        for e in stores.external_db_refs(live).await?.unwrap_or_default() {
            assert_eq!(
                e.status,
                cairn_store::inspect::ExternalStatus::Ok,
                "range {} → {}",
                live.0,
                e.path
            );
        }
    }
    // The next merge must succeed and have every row.
    stores
        .union_ranges(&[(a, a_span), (b, b_span)], PartitionId(6))
        .await?;
    assert_eq!(
        keys_of(&stores, 6).await?.len(),
        4,
        "the merged Db has the rows of LO and Q + 5"
    );
    Ok(())
}

/// The `external_dbs` entry of range `range` that points at the Db at `parent`.
async fn entry_on(
    stores: &ObjectStores,
    range: u32,
    parent: u32,
) -> Result<slatedb::manifest::ExternalDb> {
    let path = stores.range_path(parent).to_string();
    AdminBuilder::new(stores.range_path(range), stores.main().store().clone())
        .build()
        .read_manifest(None)
        .await?
        .expect("the range has a manifest")
        .external_dbs()
        .iter()
        .find(|e| e.path == path)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("range {range} has no entry pointing at {path}"))
}

/// **`external_db_refs` (what `verify` uses) does not report a missing
/// checkpoint in SlateDB's detach gap** (`gc.rs::single_process_split_
/// and_merge_leave_tombstones_that_gc_reaps` was red 3–9 times in 60 with "the
/// source Db lost a checkpoint … this range stands on" right after a split).
///
/// SlateDB's GC `detach` (`garbage_collector/detach_gc.rs`) cuts the link
/// **parent first**: (1) delete the final checkpoint on the parent, (2) drop
/// the entry from the child's manifest. It applies only to entries with no SST
/// of the parent left (`sst_ids` empty, both in the current manifest and in
/// every manifest a live checkpoint of the child pins).
///
/// Interleaving:
/// 1. Range 0 has only one row, in the left half (`LO`); split into 1 (left
///    half) and 2 (right half). After the projection, 2's entry pointing at 0
///    stands on no SST.
/// 2. 2's detach does step (1): removes 2's final checkpoint on 0.
/// 3. The test cuts into the gap between (1) and (2) — the test does (1) by
///    hand, so the gap stays open forever — and reads `external_db_refs(2)`.
///
/// Must see: 2's entry is **not** `CheckpointMissing` (it stands on no SST:
/// nothing is lost). The mutation — deleting 1's final checkpoint, whose entry
/// stands on real SSTs of 0 — still yields `CheckpointMissing`: the check still
/// catches a real loss.
#[tokio::test]
async fn missing_checkpoint_of_an_entry_without_parent_ssts_is_a_pending_detach() -> Result<()> {
    use cairn_store::inspect::ExternalStatus;
    let stores = memory_stores();
    let db = stores.open_db(0).await?;
    db.put(region_key(0x00, LO, "hot"), b"n").await?;
    for flush_type in [FlushType::Wal, FlushType::MemTable] {
        db.flush_with_options(FlushOptions { flush_type }).await?;
    }
    db.close().await?;
    let (l, r) = (PartitionId(1), PartitionId(2));
    stores
        .clone_range(PartitionId(0), l, (PartitionKey(0), PartitionKey(MID - 1)))
        .await?;
    stores
        .clone_range(
            PartitionId(0),
            r,
            (PartitionKey(MID), PartitionKey(u64::MAX)),
        )
        .await?;
    let (el, er) = (
        entry_on(&stores, l.0, 0).await?,
        entry_on(&stores, r.0, 0).await?,
    );
    assert!(!el.sst_ids.is_empty(), "1 must stand on SSTs of 0: {el:?}");
    assert!(
        er.sst_ids.is_empty(),
        "2 must not stand on any SST of 0: {er:?}"
    );
    let parent = AdminBuilder::new(stores.range_path(0), stores.main().store().clone()).build();

    // Step (1) of the detach for 2.
    parent
        .delete_checkpoint(er.final_checkpoint_id.expect("clone places a checkpoint"))
        .await?;
    let refs = stores.external_db_refs(r).await?.unwrap_or_default();
    assert!(
        refs.iter().any(|e| e.status == ExternalStatus::Detaching),
        "2's entry must be Detaching: {refs:?}"
    );
    for e in refs {
        assert!(
            !matches!(e.status, ExternalStatus::CheckpointMissing(_)),
            "2's entry → {} stands on no SST yet was reported as a missing checkpoint: {:?}",
            e.path,
            e.status
        );
    }
    for e in stores.external_db_refs(l).await?.unwrap_or_default() {
        assert_eq!(e.status, ExternalStatus::Ok, "1 → {}", e.path);
    }

    // Mutation: a real loss.
    parent
        .delete_checkpoint(el.final_checkpoint_id.expect("clone places a checkpoint"))
        .await?;
    let refs = stores.external_db_refs(l).await?.unwrap_or_default();
    assert!(
        refs.iter()
            .any(|e| matches!(e.status, ExternalStatus::CheckpointMissing(_))),
        "deleting the checkpoint 1 stands on, over real SSTs, was not reported: {refs:?}"
    );

    // A manifest pinned by a live checkpoint of the child that still stands on
    // SSTs of the parent (the child has compacted away from the parent since
    // that checkpoint) is not a half-done detach: SlateDB does not remove the
    // checkpoint on the parent then.
    use cairn_store::inspect::detach_pending;
    let emptied = slatedb::manifest::ExternalDb {
        sst_ids: Vec::new(),
        ..el.clone()
    };
    let current = std::slice::from_ref(&emptied);
    assert!(detach_pending(&emptied, [current]));
    assert!(!detach_pending(
        &emptied,
        [current, std::slice::from_ref(&el)]
    ));
    assert!(!detach_pending(&el, [std::slice::from_ref(&el)]));
    let unpinned = slatedb::manifest::ExternalDb {
        final_checkpoint_id: None,
        ..emptied.clone()
    };
    assert!(!detach_pending(
        &unpinned,
        [std::slice::from_ref(&unpinned)]
    ));
    Ok(())
}

// ─────────────────────────────── D-d: discarding a retired parent Db

/// The parent has SSTs ⇒ both children after the split still list the parent in
/// `external_dbs` ⇒ the parent Db must **not** be deleted. This is the
/// data-loss direction: deleting here makes the children lose every row not yet
/// compacted into their own Db.
#[tokio::test]
async fn retired_parent_db_kept_while_children_reference_it() -> Result<()> {
    let stores = memory_stores();
    let mut parent = writer(&stores, 0).await?;
    write_durable(&mut parent, b"acked").await?;
    parent.seal().await?;
    const MID: u64 = 1 << 63;
    let (l, r) = (PartitionId(1), PartitionId(2));
    stores
        .clone_range(PartitionId(0), l, (PartitionKey(0), PartitionKey(MID - 1)))
        .await?;
    stores
        .clone_range(
            PartitionId(0),
            r,
            (PartitionKey(MID), PartitionKey(u64::MAX)),
        )
        .await?;
    assert!(
        !stores
            .retired_db_unreferenced(PartitionId(0), &[l, r])
            .await?,
        "a child still references the parent's SSTs yet D-d considers it unused"
    );
    // A live range is never a candidate, even when listed by mistake.
    assert!(!stores.retired_db_unreferenced(l, &[l, r]).await?);
    Ok(())
}

/// The real D-d path: the child writes and flushes its memtable enough times
/// for its compactor to merge L0 — views inherited from the parent included —
/// into **its own** sorted run; SlateDB's GC `detach` cuts the link (drops the
/// `external_dbs` entry, removes the checkpoint pinned on the parent) ⇒ the
/// parent has no referrers left ⇒ deletable, and the child still reads every
/// row — including rows that only ever lived in the parent's SSTs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_parent_db_deleted_after_children_compact_away_from_it() -> Result<()> {
    use std::time::Duration;
    let polls = cairn_store::PollIntervals {
        manifest: Duration::from_millis(100),
        compactor: Duration::from_millis(100),
        compaction_worker: Duration::from_millis(100),
        gc: Duration::from_millis(200),
        gc_idle_skip_max: Duration::from_millis(200),
        gc_min_age: Duration::from_millis(0),
    };
    // The compactor's checkpoint pins old SSTs (the parent's too) for 10 minutes
    // in real life; the test cuts it to 1 s.
    let stores = memory_stores()
        .with_polls(polls)
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    let mut parent = writer(&stores, 0).await?;
    for i in 0..20u32 {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"parents-own");
        parent.commit(m).await?;
    }
    if let Durability::Wait(p) = parent.durability()? {
        p.durable().await?;
    }
    parent.seal().await?;
    let child = PartitionId(1);
    stores
        .clone_range(
            PartitionId(0),
            child,
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    assert!(
        !stores
            .retired_db_unreferenced(PartitionId(0), &[child])
            .await?
    );

    let mut c = writer(&stores, 1).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut i = 20u32;
    while !stores
        .retired_db_unreferenced(PartitionId(0), &[child])
        .await?
    {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the child did not cut its link to the parent within 90 s (compaction + GC detach)"
        );
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"child-data");
        c.commit(m).await?;
        if let Durability::Wait(p) = c.durability()? {
            p.durable().await?;
        }
        c.flush_memtable().await?;
        i += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
            None,
        )
        .await?;
    assert!(!stores.range_db_exists(PartitionId(0)).await?);
    let rows = c.journal_unbounded(&INV).await?;
    assert_eq!(rows.len() as u32, i, "child lost rows after parent delete");
    assert_eq!(
        &rows[0].1[..],
        b"parents-own",
        "a row that only ever lived in the parent must remain"
    );
    Ok(())
}

/// **The `LiveRefs` suspicion** (`docs/reliability/irreversible.md` X6): for a
/// **fixed** set of live ranges (one table version), a live range's
/// `external_dbs` only **shrinks** — its Db is created once before it enters
/// the table, compaction collects SSTs, GC `detach` drops entries; nothing adds
/// an entry. So a reference set read **early** is a superset of every later
/// read at the same version: D-d reusing it for every candidate of a round
/// (`gc::run_once`, `proof::RoundRefs`) only **keeps** more than necessary, and
/// never deletes a Db a live range still stands on.
///
/// Order: parent 0 is closed; child 1 is cloned from it (the only live range —
/// a fixed set); read set `R0`. The child writes, flushes, compaction and GC
/// `detach` run; the set is read again after each pass. Must see: each read is
/// a subset of the previous one (never an extra range, never `unknown`); once
/// the child cuts its link to the parent, the fresh set says "nobody" about 0
/// while the old `R0` still says "someone" — keeping more than necessary.
#[tokio::test]
async fn n4_live_refs_only_shrink_at_a_fixed_version() -> Result<()> {
    use std::time::Duration;
    let polls = cairn_store::PollIntervals {
        manifest: Duration::from_millis(100),
        compactor: Duration::from_millis(100),
        compaction_worker: Duration::from_millis(100),
        gc: Duration::from_millis(200),
        gc_idle_skip_max: Duration::from_millis(200),
        gc_min_age: Duration::from_millis(0),
    };
    let stores = memory_stores()
        .with_polls(polls)
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    let mut parent = writer(&stores, 0).await?;
    for i in 0..20u32 {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"parents-own");
        parent.commit(m).await?;
    }
    if let Durability::Wait(p) = parent.durability()? {
        p.durable().await?;
    }
    parent.seal().await?;
    let child = PartitionId(1);
    stores
        .clone_range(
            PartitionId(0),
            child,
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    let live = [child];
    let first = stores.referenced_ranges(&live).await?;
    assert!(first.unknown.is_none(), "{first:?}");
    assert_eq!(
        first.refs_of(PartitionId(0)),
        cairn_split::Refs::Referenced,
        "a just-cloned child must stand on the parent: {first:?}"
    );
    let mut c = writer(&stores, 1).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut last = first.clone();
    let mut i = 20u32;
    loop {
        let now = stores.referenced_ranges(&live).await?;
        assert!(now.unknown.is_none(), "{now:?}");
        let grew: Vec<u32> = now
            .by
            .keys()
            .filter(|r| !last.by.contains_key(r))
            .copied()
            .collect();
        assert!(
            grew.is_empty(),
            "the LiveRefs argument breaks: with a fixed set of live ranges, the reference set gained {grew:?} ({last:?} → {now:?})"
        );
        if now.refs_of(PartitionId(0)) == cairn_split::Refs::Clear {
            break;
        }
        last = now;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the child did not cut its link to the parent within 90 s (compaction + GC detach)"
        );
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"child-data");
        c.commit(m).await?;
        if let Durability::Wait(p) = c.durability()? {
            p.durable().await?;
        }
        c.flush_memtable().await?;
        i += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The old set only keeps more: it still says the parent has someone standing on it.
    assert_eq!(
        first.refs_of(PartitionId(0)),
        cairn_split::Refs::Referenced,
        "the early set must still keep the parent"
    );
    c.seal().await?;
    Ok(())
}

// ─────────────────────────────── Three GC bugs

/// Copy every object under `from/` to `to/` — an operator moving the bucket to
/// another prefix (changing `--prefix`).
async fn copy_prefix(store: &Arc<dyn ObjectStore>, from: &str, to: &str) -> Result<()> {
    let listed: Vec<_> = store.list(Some(&Path::from(from))).try_collect().await?;
    for m in listed {
        let rest = m
            .location
            .as_ref()
            .strip_prefix(from)
            .expect("under the source prefix");
        let body = store.get(&m.location).await?.bytes().await?;
        store
            .put(&Path::from(format!("{to}{rest}")), body.into())
            .await?;
    }
    Ok(())
}

/// **G-a:** the parent Db's identity in a child's `external_dbs` is the path
/// string at clone time (`a/ranges/0`). Once the bucket moves to prefix `b`, GC
/// computes `b/ranges/0`, no line matches, and the parent Db is considered
/// unreferenced — the child loses every row not yet compacted into its own Db.
/// A prefix mismatch must yield "still referenced" (conservative), never
/// "unused".
#[tokio::test]
async fn retired_db_kept_when_reference_was_written_under_another_prefix() -> Result<()> {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = ObjectStores::single(StoreLocation::from_store(mem.clone(), "a", "memory://a"));
    let mut parent = writer(&old, 0).await?;
    write_durable(&mut parent, b"acked").await?;
    parent.seal().await?;
    old.clone_range(
        PartitionId(0),
        PartitionId(1),
        (PartitionKey(0), PartitionKey(u64::MAX)),
    )
    .await?;
    copy_prefix(&mem, "a", "b").await?;
    let moved = ObjectStores::single(StoreLocation::from_store(mem.clone(), "b", "memory://b"));
    assert!(
        moved.range_db_exists(PartitionId(1)).await?,
        "the child Db was copied to the new prefix"
    );
    assert!(
        !moved
            .retired_db_unreferenced(PartitionId(0), &[PartitionId(1)])
            .await?,
        "a child lists the parent under the old prefix yet GC considers the parent unreferenced"
    );
    Ok(())
}

/// Names of the objects under `ranges/<range>` (empty prefix).
async fn range_objects(store: &Arc<dyn ObjectStore>, range: u32) -> Result<Vec<String>> {
    let listed: Vec<_> = store
        .list(Some(&Path::from(format!("ranges/{range}"))))
        .try_collect()
        .await?;
    Ok(listed.into_iter().map(|m| m.location.to_string()).collect())
}

/// **G-b:** deleting a retired Db that fails midway must be continued by the
/// next round. The old code deleted in LIST order (`compacted/` < `manifest/` <
/// `wal/`): failing after the manifest was gone made `range_db_exists` return
/// `false`, the next GC round skipped that range, and the remaining `wal/` part
/// was orphaned forever. The manifest must go **last**.
#[tokio::test]
async fn partial_retired_db_delete_is_finished_next_round() -> Result<()> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let mut parent = writer(&stores, 0).await?;
    write_durable(&mut parent, b"acked").await?;
    parent.seal().await?;
    let before = range_objects(&store, 0).await?;
    assert!(
        before.iter().any(|p| p.contains("/wal/")),
        "the Db has WAL objects: {before:?}"
    );
    assert!(
        before.iter().any(|p| p.contains("/manifest/")),
        "the Db has a manifest: {before:?}"
    );

    faulty.add_rule(Rule::new(
        OpSet::DELETE,
        PathMatch::Contains("/wal/".into()),
        Action::Fail { times: Some(1) },
    ));
    assert!(
        stores
            .delete_retired_db(
                cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
                None
            )
            .await
            .is_err(),
        "the first deletion must fail"
    );
    assert!(
        stores.range_db_exists(PartitionId(0)).await?,
        "half deleted with the manifest gone — the next GC round would skip this range forever"
    );
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
            None,
        )
        .await?;
    assert_eq!(
        range_objects(&store, 0).await?,
        Vec::<String>::new(),
        "the second round deletes everything"
    );
    Ok(())
}

/// **Roll-back keeps the evidence until the cleanup is done** (RD4). The orphan
/// child's manifest is the only record of the final checkpoints it placed on
/// its ancestors — when rolling back a **split**, one ancestor is the **live**
/// parent range, and a checkpoint without expiry pins its SSTs forever.
///
/// Interleaving:
/// 1. 0 → clone 1 (a live range); splitting 1 dies midway: the orphan child 3
///    is cloned (final checkpoints on 0 and on 1).
/// 2. `discard_range_db(3, [1])` cannot read 0's manifest in the lineage (a
///    non-retryable error — SlateDB retries transient errors forever itself).
/// 3. The store heals; discard again.
///
/// Must see: at step 2, `discard_range_db` returns an error and **keeps** 3's
/// Db; at step 3, the cleanup completes — 3 has no Db, 1 has no checkpoint
/// without expiry left. Before the fix: step 2 kept the checkpoint (right) but
/// still deleted 3's Db and returned `Ok` — the checkpoint on the live range 1
/// stayed with nothing pointing at it any more.
#[tokio::test]
async fn discard_keeps_the_orphan_until_its_checkpoints_are_gone() -> Result<()> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    three_region_parent(&stores).await?;
    let (live, orphan) = (PartitionId(1), PartitionId(3));
    stores
        .clone_range(
            PartitionId(0),
            live,
            (PartitionKey(0), PartitionKey(MID - 1)),
        )
        .await?;
    keys_of(&stores, live.0).await?;
    stores
        .clone_range(live, orphan, (PartitionKey(0), PartitionKey(LO)))
        .await?;
    let pinned_on_live = || async {
        Ok::<_, anyhow::Error>(
            stores
                .range_checkpoints(live)
                .await?
                .into_iter()
                .filter(|c| !c.expires)
                .count(),
        )
    };
    assert_eq!(
        pinned_on_live().await?,
        1,
        "the clone places one final checkpoint on the live range"
    );

    faulty.add_rule(Rule::new(
        OpSet::GET | OpSet::LIST | OpSet::HEAD,
        PathMatch::Contains("ranges/0/manifest".into()),
        Action::Precondition,
    ));
    let first = stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(orphan),
            &[live],
            None,
        )
        .await;
    faulty.clear_rules();
    assert!(first.is_err(), "the cleanup is not done yet it reported Ok");
    assert!(
        stores.range_db_exists(orphan).await?,
        "the orphan child's Db was deleted before its checkpoint on the live range was removed"
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
        pinned_on_live().await?,
        0,
        "the live range still has the orphan child's final checkpoint"
    );
    Ok(())
}

// ──────────────────── The epoch-conditional closed mark (M1)

/// **Close a source only at exactly the epoch of the writer just quiesced.**
/// Writer A seals (epoch `a`); writer B reopens the Db and seals (epoch `b >
/// a`) — the shape of "the source engine reopened between the quiesce and the
/// close command".
///
/// Must see: `close_range_db_at(a)` refuses (`Fenced`) and sets no flag;
/// `close_range_db_at(b)` closes; retrying still succeeds; a new writer cannot
/// open.
#[tokio::test]
async fn close_at_epoch_refuses_after_a_reopen() -> Result<()> {
    let stores = memory_stores();
    let a = writer(&stores, 0).await?;
    let epoch_a = a.writer_epoch();
    a.seal().await?;
    let b = writer(&stores, 0).await?;
    let epoch_b = b.writer_epoch();
    b.seal().await?;
    anyhow::ensure!(epoch_b > epoch_a, "{epoch_a} → {epoch_b}");

    let err = stores.close_range_db_at(PartitionId(0), epoch_a).await;
    anyhow::ensure!(
        matches!(err, Err(cairn_store::StoreError::Fenced)),
        "closing at the old epoch must be fenced: {err:?}"
    );
    anyhow::ensure!(stores.range_db_closed(PartitionId(0)).await? == Some(false));
    stores.close_range_db_at(PartitionId(0), epoch_b).await?;
    stores.close_range_db_at(PartitionId(0), epoch_b).await?;
    anyhow::ensure!(stores.range_db_closed(PartitionId(0)).await? == Some(true));
    anyhow::ensure!(
        stores.open_db(0).await.is_err(),
        "the Db is closed yet still opens"
    );
    Ok(())
}

/// `adopt_unowned_db`: an existing Db with no owner record yet ⇒ one
/// **released** record carrying the Db's WAL kind; no Db yet, or a record
/// already there ⇒ nothing is written; an ambiguous `Create` (object_store
/// retries by itself, rule 7) is still our own write.
///
/// 1. Range 0: no Db yet ⇒ `false`, no record.
/// 2. Open then close range 0's Db (own WAL) ⇒ `true`, a `released`, `slatedb`
///    record.
/// 3. Call again ⇒ `false`, the record is unchanged.
/// 4. Range 1 has a Db, the owner-record `Create` gets an ambiguous answer
///    (both forms) ⇒ `true`.
#[tokio::test]
async fn adopt_unowned_db_records_the_db_wal_kind() -> Result<()> {
    use cairn_store::{testkit::Ambiguity, WalKind};
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let dyn_store: Arc<dyn ObjectStore> = faulty.clone();
        let stores =
            ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
        let root = Path::default();
        assert!(!stores.adopt_unowned_db(PartitionId(0), "solo").await?);
        assert!(cairn_store::read_owner(&dyn_store, &root, PartitionId(0))
            .await?
            .is_none());

        for r in [0, 1] {
            let mut w = writer(&stores, r).await?;
            write_durable(&mut w, b"before-adopt").await?;
            w.seal().await?;
        }
        assert!(stores.adopt_unowned_db(PartitionId(0), "solo").await?);
        let rec = cairn_store::read_owner(&dyn_store, &root, PartitionId(0))
            .await?
            .expect("just written");
        assert!(rec.released && !rec.retired, "{rec:?}");
        assert_eq!(rec.wal_kind, WalKind::SlateDb);
        assert_eq!((rec.term, rec.session.inc, rec.data), (0, 0, None));
        assert!(!stores.adopt_unowned_db(PartitionId(0), "other").await?);
        assert_eq!(
            cairn_store::read_owner(&dyn_store, &root, PartitionId(0))
                .await?
                .expect("still there")
                .session
                .node,
            "solo"
        );

        faulty.add_rule(Rule::ambiguous_put("owners/", reply));
        assert!(
            stores.adopt_unowned_db(PartitionId(1), "solo").await?,
            "{reply:?}: our own ambiguous Create was taken as a loss"
        );
        assert_eq!(
            cairn_store::read_owner(&dyn_store, &root, PartitionId(1))
                .await?
                .expect("just written")
                .wal_kind,
            WalKind::SlateDb
        );
    }
    Ok(())
}
