// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The typed proof gate: a proof constructor reads, durably, exactly the facts
//! the deletion needs, and the deletion checks the proof.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{ensure, Result},
    cairn_store::{
        gc_barrier::{self, GcBarrier, Protects},
        proof::DeleteProof,
        ClusterConfig, LeadershipGrant, ObjectStores, PartitionId, PartitionKey, PartitionTable,
        PartitionWriter, RangeEntry, StoreLocation, TableUpdate,
    },
    slatedb::config::{FlushOptions, FlushType},
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
    std::sync::Arc,
};

const PREFIX: &str = "cairn";

/// Bucket: the table has only range 0, `next_range_id` = 6. Range 5 is retired
/// (Db + tombstone), range 3 is an abandoned child (Db, no owner record).
async fn setup() -> Result<(Arc<dyn ObjectStore>, ObjectStores, ClusterConfig)> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&store),
        PREFIX,
        "memory://cairn",
    ));
    let cluster = ClusterConfig::open(&store, &Path::from(PREFIX), 1).await?;
    let v = cluster.version();
    let moved = cluster
        .update(v, |t| {
            PartitionTable::from_entries(t.version() + 1, 6, t.ranges().to_vec())
        })
        .await?;
    ensure!(matches!(moved, TableUpdate::Applied(_)));
    for r in [0u32, 5, 3] {
        let w = PartitionWriter::open(
            LeadershipGrant::single_node(PartitionId(r)),
            stores.open_db(r).await?,
        )
        .await?;
        w.seal().await?;
    }
    store
        .put(
            &Path::from(format!("{PREFIX}/owners/range-5")),
            bytes::Bytes::from_static(
                b"cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nretired=1\nwal_kind=slatedb\n",
            )
            .into(),
        )
        .await?;
    Ok((store, stores, cluster))
}

/// D-d: a proof exists only when the range is out of the table, carries a
/// tombstone and no barrier covers it; if the table changes version after the
/// proof, the deletion refuses and deletes nothing.
#[tokio::test]
async fn retired_db_proof_reads_the_facts_it_needs() -> Result<()> {
    let (store, stores, cluster) = setup().await?;
    let now = 1_000;
    let refused = |p: cairn_store::proof::Proven| p.err().unwrap_or_default();
    ensure!(
        refused(DeleteProof::retired_db(&stores, &cluster, PartitionId(0), now).await?)
            .contains("in the table")
    );
    ensure!(
        refused(DeleteProof::retired_db(&stores, &cluster, PartitionId(3), now).await?)
            .contains("tombstone")
    );

    gc_barrier::put_barrier(
        &store,
        &Path::from(PREFIX),
        "backup",
        &GcBarrier {
            holder: "test".into(),
            protects: Protects::All,
            expires_ms: now + 60_000,
            reason: "backup".into(),
        },
    )
    .await?;
    ensure!(
        refused(DeleteProof::retired_db(&stores, &cluster, PartitionId(5), now).await?)
            .contains("GC barrier")
    );
    // The barrier expired ⇒ provable.
    let proof = DeleteProof::retired_db(&stores, &cluster, PartitionId(5), now + 120_000)
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(proof.table_version() == Some(cluster.version()));

    // The table changes after the proof ⇒ the deletion refuses.
    let v = cluster.version();
    cluster
        .update(v, |t| {
            PartitionTable::from_entries(t.version() + 1, 7, t.ranges().to_vec())
        })
        .await?;
    ensure!(
        stores
            .delete_retired_db(proof, Some(&cluster))
            .await
            .is_err(),
        "a proof at an old version still deleted"
    );
    ensure!(stores.range_db_exists(PartitionId(5)).await?);
    // A fresh proof ⇒ deletable.
    let proof = DeleteProof::retired_db(&stores, &cluster, PartitionId(5), now + 120_000)
        .await?
        .map_err(anyhow::Error::msg)?;
    stores.delete_retired_db(proof, Some(&cluster)).await?;
    ensure!(!stores.range_db_exists(PartitionId(5)).await?);
    Ok(())
}

/// Abandoned child: a proof for the Db when the range is out of the table, not
/// promised, its id already allocated; a proof for the owner record only after
/// the Db has no manifest left (RD4).
#[tokio::test]
async fn abandoned_child_owner_waits_for_its_db() -> Result<()> {
    let (store, stores, cluster) = setup().await?;
    ensure!(
        DeleteProof::abandoned_child_db(&stores, &cluster, PartitionId(0), 1_000)
            .await?
            .is_err()
    );
    ensure!(
        DeleteProof::abandoned_child_db(&stores, &cluster, PartitionId(9), 1_000)
            .await?
            .is_err()
    );
    let early = DeleteProof::abandoned_child_owner(&stores, &cluster, PartitionId(3)).await?;
    ensure!(
        early.as_ref().err().is_some_and(|w| w.contains("RD4")),
        "the owner record must not go before the Db: {early:?}"
    );
    let proof = DeleteProof::abandoned_child_db(&stores, &cluster, PartitionId(3), 1_000)
        .await?
        .map_err(anyhow::Error::msg)?;
    stores.discard_range_db(proof, &[], Some(&cluster)).await?;
    let proof = DeleteProof::abandoned_child_owner(&stores, &cluster, PartitionId(3))
        .await?
        .map_err(anyhow::Error::msg)?;
    cairn_store::delete_owner(&store, &Path::from(PREFIX), proof).await?;
    Ok(())
}

/// Abandoned child, against a lax proof gate: "out of the table, not promised,
/// id allocated" also holds for a **retired parent range** that a child still
/// stands on — before, only the caller (SM-14) blocked that. The constructor
/// must check SM-14's (1)–(3) itself:
///
/// 1. Range 4 (id allocated, out of the table, no owner record) has data; range
///    6 is cloned from it and then replaces range 0 in the table — 6 lists 4 in
///    `external_dbs`.
/// 2. An abandoned-child proof for 4 ⇒ must refuse (a live range stands on it).
/// 3. A GC barrier covers range 3 ⇒ a proof for 3 must refuse; with the barrier
///    removed there is one.
/// 4. The table changes version after the proof for 3 ⇒ `discard_range_db` must
///    refuse and 3's Db stays intact.
///
/// Must see: three refusals at 2, 3, 4; the Dbs of 4 and 3 remain.
#[tokio::test]
async fn abandoned_child_proof_checks_refs_barriers_and_version() -> Result<()> {
    let (store, stores, cluster) = setup().await?;
    let parent = PartitionId(4);
    let db = stores.open_db(parent.0).await?;
    let mut key = vec![0x00];
    key.extend_from_slice(&10u64.to_be_bytes());
    key.extend_from_slice(b"hot");
    db.put(&key, b"n").await?;
    for flush_type in [FlushType::Wal, FlushType::MemTable] {
        db.flush_with_options(FlushOptions { flush_type }).await?;
    }
    db.close().await?;
    stores
        .clone_range(
            parent,
            PartitionId(6),
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    let v = cluster.version();
    let moved = cluster
        .update(v, |t| {
            let mut ranges = t.ranges().to_vec();
            ranges[0].id = PartitionId(6);
            PartitionTable::from_entries(t.version() + 1, 7, ranges)
        })
        .await?;
    ensure!(matches!(moved, TableUpdate::Applied(_)));
    ensure!(
        stores
            .referenced_ranges(&[PartitionId(6)])
            .await?
            .by
            .contains_key(&parent.0),
        "bad setup: the child does not list the parent in external_dbs"
    );

    // (1) A live range stands on it.
    let p = DeleteProof::abandoned_child_db(&stores, &cluster, parent, 1_000).await?;
    ensure!(
        p.as_ref().is_err_and(|w| w.contains("external_dbs")),
        "a retired parent a child still stands on was taken for an abandoned child: {p:?}"
    );

    // (2) GC barrier.
    let prefix = Path::from(PREFIX);
    gc_barrier::put_barrier(
        &store,
        &prefix,
        "fork-x",
        &GcBarrier {
            holder: "test".into(),
            protects: Protects::Ranges([3u32].into_iter().collect()),
            expires_ms: u64::MAX,
            reason: "copy".into(),
        },
    )
    .await?;
    let p = DeleteProof::abandoned_child_db(&stores, &cluster, PartitionId(3), 1_000).await?;
    ensure!(
        p.as_ref().is_err_and(|w| w.contains("GC barrier")),
        "a GC barrier covers range 3 yet there is a proof: {p:?}"
    );
    gc_barrier::delete_barrier(&store, &prefix, "fork-x").await?;

    // (3) The table changes after the proof ⇒ the deletion refuses.
    let proof = DeleteProof::abandoned_child_db(&stores, &cluster, PartitionId(3), 1_000)
        .await?
        .map_err(anyhow::Error::msg)?;
    let v = cluster.version();
    cluster
        .update(v, |t| {
            PartitionTable::from_entries(t.version() + 1, 8, t.ranges().to_vec())
        })
        .await?;
    ensure!(
        stores
            .discard_range_db(proof, &[], Some(&cluster))
            .await
            .is_err(),
        "a proof at an old version still discarded"
    );
    ensure!(stores.range_db_exists(PartitionId(3)).await?);
    ensure!(stores.range_db_exists(parent).await?);
    Ok(())
}

/// **D-d's checkpoint fence does not count retired ranges.** Order:
/// 1. Range 5 (retired, tombstone) is cloned to 4: 4 stands on 5 and puts on 5
///    a final checkpoint named `ranges/4`.
/// 2. 4 carries no tombstone yet (a child's Db, nobody has proved it retired)
///    ⇒ the D-d proof for 5 refuses because of the named checkpoint.
/// 3. 4 retires (tombstone). No live range lists 5 or 4.
///
/// Must see: at (3) D-d proves 5 although 4's checkpoint is still on it —
/// nobody opens or reads 4 as a Db, and 4's live descendants (if any) carry
/// their own checkpoints. Before, the fence kept 5 as long as 4's Db existed,
/// and a retired Db that a live range stands on exists forever (a chaos run
/// found four Dbs kept that way).
#[tokio::test]
async fn retired_db_proof_ignores_checkpoints_of_retired_ranges() -> Result<()> {
    let (store, stores, cluster) = setup().await?;
    stores
        .clone_range(
            PartitionId(5),
            PartitionId(4),
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    let refused = |p: cairn_store::proof::Proven| p.err().unwrap_or_default();
    ensure!(
        refused(DeleteProof::retired_db(&stores, &cluster, PartitionId(5), 1_000).await?)
            .contains("named final checkpoint"),
        "4 carries no tombstone yet its checkpoint does not keep 5"
    );
    store
        .put(
            &Path::from(format!("{PREFIX}/owners/range-4")),
            bytes::Bytes::from_static(
                b"cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nretired=1\nwal_kind=slatedb\n",
            )
            .into(),
        )
        .await?;
    let proof = DeleteProof::retired_db(&stores, &cluster, PartitionId(5), 1_000)
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(proof.table_version() == Some(cluster.version()));
    Ok(())
}

/// Write the ledger `ops/<key>` of `op` in phase `phase`.
async fn put_ledger(
    store: &Arc<dyn ObjectStore>,
    op: cairn_split::Op,
    phase: cairn_split::Phase,
) -> Result<()> {
    let record = cairn_store::ledger::OpRecord {
        op,
        phase,
        coordinator: "x/1".into(),
        sealed: Vec::new(),
        by: "x/1#0".into(),
        extra: Vec::new(),
    };
    store
        .put(
            &cairn_store::ledger::op_path(&Path::from(PREFIX), op.key()),
            record.encode().into(),
        )
        .await?;
    Ok(())
}

/// **A settled op ledger**: a proof exists only when the ledger is in a final
/// state, old enough, no intent mentions the op, and — `Committed` — every
/// source carries a tombstone, — `Cleaned` — every child has lost its Db and
/// owner record. The deletion moves the ledger into the trash; a proof for
/// something else is refused.
#[tokio::test]
async fn settled_op_proof_waits_for_every_range_to_settle() -> Result<()> {
    use cairn_split::{Op, Phase};
    let (store, stores, cluster) = setup().await?;
    let prefix = Path::from(PREFIX);
    let refused = |p: cairn_store::proof::Proven| p.err().unwrap_or_default();
    // Split 5 (retired, tombstone) into 1 + 2: Committed.
    let committed = Op::Split {
        source: 5,
        left: 1,
        right: 2,
    };
    put_ledger(&store, committed, Phase::Committed).await?;
    ensure!(
        refused(DeleteProof::settled_op(&stores, &cluster, 1, 60_000, 0).await?).contains("Young"),
        "a ledger just written must wait for its age"
    );
    let proof = DeleteProof::settled_op(&stores, &cluster, 1, 0, u64::MAX / 2)
        .await?
        .map_err(anyhow::Error::msg)?;
    // Split 0 (in the table, no intent) into 3 + 4, rolled back: Cleaned — child
    // 3 still has a Db ⇒ not settled yet.
    let cleaned = Op::Split {
        source: 0,
        left: 3,
        right: 4,
    };
    put_ledger(&store, cleaned, Phase::Cleaned).await?;
    ensure!(
        refused(DeleteProof::settled_op(&stores, &cluster, 3, 0, u64::MAX / 2).await?)
            .contains("NotSettled")
    );
    stores
        .discard_range_db(DeleteProof::assume_db(PartitionId(3)), &[], None)
        .await?;
    let cleaned_proof = DeleteProof::settled_op(&stores, &cluster, 3, 0, u64::MAX / 2)
        .await?
        .map_err(anyhow::Error::msg)?;
    // Ledger not in a final state ⇒ keep; a source without a tombstone ⇒ keep.
    let prepared = Op::Merge {
        lo: 0,
        hi: 5,
        into: 7,
    };
    put_ledger(&store, prepared, Phase::Prepared).await?;
    ensure!(
        refused(DeleteProof::settled_op(&stores, &cluster, 7, 0, u64::MAX / 2).await?)
            .contains("NotSettled")
    );
    put_ledger(&store, prepared, Phase::Committed).await?;
    ensure!(
        refused(DeleteProof::settled_op(&stores, &cluster, 7, 0, u64::MAX / 2).await?)
            .contains("NotSettled"),
        "source 0 carries no tombstone (still in the table) — a Committed ledger disagrees with the table"
    );

    // A proof for another ledger, or for a Db, makes the deletion refuse.
    ensure!(cairn_store::ledger::delete_op(
        &store,
        &prefix,
        DeleteProof::assume_db(PartitionId(1))
    )
    .await
    .is_err());
    cairn_store::ledger::delete_op(&store, &prefix, proof).await?;
    cairn_store::ledger::delete_op(&store, &prefix, cleaned_proof).await?;
    for key in [1, 3] {
        ensure!(
            cairn_store::ledger::read_op(&store, &prefix, key)
                .await?
                .is_none(),
            "ledger ops/{key} still there"
        );
    }
    ensure!(cairn_store::ledger::read_op(&store, &prefix, 7)
        .await?
        .is_some());
    Ok(())
}

/// A proof for something else makes the deletion refuse.
#[tokio::test]
async fn proof_for_another_subject_is_refused() -> Result<()> {
    let (store, stores, cluster) = setup().await?;
    let db_proof = DeleteProof::abandoned_child_db(&stores, &cluster, PartitionId(3), 1_000)
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(
        cairn_store::delete_owner(&store, &Path::from(PREFIX), db_proof)
            .await
            .is_err(),
        "a proof for the Db deleted the owner record"
    );
    ensure!(stores
        .discard_range_db(DeleteProof::assume_db(PartitionId(5)), &[], None)
        .await
        .is_ok());
    ensure!(stores
        .discard_range_db(DeleteProof::assume_owner(PartitionId(3)), &[], None)
        .await
        .is_err());
    let _ = RangeEntry::new(0, 1, PartitionId(0));
    Ok(())
}

/// **Artifact (AP-8)**: a proof for the directory `(tenant, id, epoch)` exists
/// only when (1) the writer's range holds the id by the table, (2) no row of the
/// id carries that epoch, (3) every object is older than `min_age`.
///
/// Must see: the id has rows with epoch 7 ⇒ no proof for epoch 7, a proof for
/// epoch 8 (another incarnation of the same id) once old enough, none while
/// young; a writer of another range ⇒ no proof.
#[tokio::test]
async fn purged_invocation_proof_needs_the_owning_range_no_row_and_age() -> Result<()> {
    use cairn_store::{
        artifact::{ArtifactStore, Owner},
        keys::ArtifactRow,
        proof::artifacts_unreferenced,
        Mutations,
    };
    let (store, stores, cluster) = setup().await?;
    let mut w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    let id = [4u8; 24];
    let mut muts = Mutations::new();
    muts.put_artifact(
        &id,
        &[1; 32],
        &ArtifactRow {
            epoch: 7,
            size: 1,
            content_type: String::new(),
        },
    );
    w.commit(muts).await?;
    let a = ArtifactStore::new(Arc::clone(&store), &Path::from(PREFIX));
    let owner = |epoch| Owner {
        tenant: "t".into(),
        id,
        epoch,
    };
    for epoch in [7, 8] {
        a.put(&owner(epoch), &[1; 32], bytes::Bytes::from_static(b"x"))
            .await?;
    }
    let live = artifacts_unreferenced(&w, &cluster, &owner(7)).await?;
    ensure!(live.is_err(), "the epoch still has rows: {live:?}");
    let now = cairn_store::entropy::now_ms();
    let young = DeleteProof::purged_invocation(
        artifacts_unreferenced(&w, &cluster, &owner(8))
            .await?
            .map_err(anyhow::Error::msg)?,
        &a,
        3_600_000,
        now,
    )
    .await?;
    ensure!(young.is_err(), "just-PUT object still young: {young:?}");
    let old = DeleteProof::purged_invocation(
        artifacts_unreferenced(&w, &cluster, &owner(8))
            .await?
            .map_err(anyhow::Error::msg)?,
        &a,
        3_600_000,
        now + 3_600_001,
    )
    .await?
    .map_err(anyhow::Error::msg)?;
    ensure!(
        old.subject() == &cairn_store::trash::Subject::Artifacts(owner(8)),
        "{old:?}"
    );
    ensure!(old.table_version().is_none(), "permanent");
    // The writer of a range that does not hold the id (range 5, retired).
    let other = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(5)),
        stores.open_db(5).await?,
    )
    .await;
    if let Ok(other) = other {
        let r = artifacts_unreferenced(&other, &cluster, &owner(8)).await?;
        ensure!(r.is_err(), "another range cannot prove: {r:?}");
    }
    Ok(())
}

/// **Segment (large values)**: a proof for an owner's `(sha, gen)` exists only
/// when (1) the writer's range holds `pk` by the table, (2) no holder row of the
/// owner carries that `(sha, gen)`, (3) the object exists and is older than
/// `min_age`. A broken holder row ⇒ nothing can be proved for the whole owner.
///
/// Must see: `(sha, gen=1)` has a holder ⇒ no proof; `(sha, gen=2)` (another
/// seal of the same bytes) is provable once old enough, not while young, not
/// when the object is absent; another owner's token cannot be used; a broken
/// holder row ⇒ no token.
#[tokio::test]
async fn value_proof_needs_the_owning_range_no_holder_and_age() -> Result<()> {
    use cairn_store::{
        keys,
        proof::values_referenced,
        value::{Holder, Owner, Segment, ValueStore},
        Mutations, PartitionKey, TenantId,
    };
    let (store, stores, cluster) = setup().await?;
    let mut w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    // pk belongs to range 0 by `setup`'s table (range 0 covers from the smallest key).
    let pk = PartitionKey(1);
    let owner = Owner([3; 32]);
    let t = TenantId::new("t")?;
    let mut muts = Mutations::new();
    muts.put_value_ref(pk, &t, &owner, &Holder::State("k".into()), &[1; 32], 1, 1);
    w.commit(muts).await?;
    let v = ValueStore::new(Arc::clone(&store), &Path::from(PREFIX));
    let seg = |gen| Segment {
        tenant: "t".into(),
        pk,
        owner,
        sha: [1; 32],
        gen,
    };
    for gen in [1, 2] {
        v.put(&seg(gen), bytes::Bytes::from_static(b"x")).await?;
    }
    let token = values_referenced(&w, &cluster, "t", pk, &owner)
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(token.held().contains(&([1; 32], 1)), "holders readable");
    let now = cairn_store::entropy::now_ms();
    let held =
        DeleteProof::value_unreferenced(&token, &v, &seg(1), 3_600_000, now + 3_600_001).await?;
    ensure!(held.is_err(), "gen 1 still has a holder: {held:?}");
    let young = DeleteProof::value_unreferenced(&token, &v, &seg(2), 3_600_000, now).await?;
    ensure!(young.is_err(), "just-PUT segment still young: {young:?}");
    let missing = Segment { gen: 3, ..seg(3) };
    let gone =
        DeleteProof::value_unreferenced(&token, &v, &missing, 3_600_000, now + 3_600_001).await?;
    ensure!(gone.is_err(), "object absent: {gone:?}");
    let other = Segment {
        owner: Owner([9; 32]),
        ..seg(2)
    };
    let foreign =
        DeleteProof::value_unreferenced(&token, &v, &other, 3_600_000, now + 3_600_001).await?;
    ensure!(foreign.is_err(), "another owner's token: {foreign:?}");
    let old = DeleteProof::value_unreferenced(&token, &v, &seg(2), 3_600_000, now + 3_600_001)
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(
        old.subject() == &cairn_store::trash::Subject::Value(seg(2)),
        "{old:?}"
    );
    ensure!(old.table_version().is_none(), "permanent");

    // A broken holder row (strange value) ⇒ nothing can be proved.
    let mut muts = Mutations::new();
    muts.put_value_ref(pk, &t, &owner, &Holder::State("j".into()), &[2; 32], 1, 1);
    w.commit(muts).await?;
    let bad_key = keys::value_ref(pk, &t, &owner, &Holder::State("j".into()), &[2; 32], 1);
    let mut muts = Mutations::new();
    muts.raw_put(bad_key, b"\x09");
    w.commit(muts).await?;
    let r = values_referenced(&w, &cluster, "t", pk, &owner).await?;
    ensure!(r.is_err(), "broken row ⇒ no token: {r:?}");
    Ok(())
}

/// Write `cluster-version` (purge gate on or off) and the `tenants` book under
/// [`PREFIX`].
async fn purge_book(store: &Arc<dyn ObjectStore>, gate: bool, book: &str) -> Result<()> {
    let cv = if gate {
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=11\ntenant_purge=on\n"
    } else {
        "cairnflow-cluster-version/1\nversion=2\nmin_binary=10\n"
    };
    store
        .put(
            &Path::from(format!("{PREFIX}/cluster-version")),
            bytes::Bytes::from(cv).into(),
        )
        .await?;
    store
        .put(
            &Path::from(format!("{PREFIX}/tenants")),
            bytes::Bytes::from(format!("cairnflow-tenants/1\n{book}")).into(),
        )
        .await?;
    Ok(())
}

/// **Tenant purge**: both proofs and the row-deletion grant read
/// `cluster-version` and the `tenants` book **fresh** themselves.
///
/// Must see:
/// 1. gate off ⇒ nothing is proved;
/// 2. book `active`/`suspended`, tenant `default` ⇒ no;
/// 3. `deleting` ⇒ `tenant_deleting` yes (only to keep: `may_purge` is false),
///    `tenant_purged` and the row-deletion grant not yet — `purging-ms=` is
///    missing;
/// 4. `purging-ms=` present but less than 7 days ⇒ not yet; a broken mark ⇒ no;
/// 5. 7 days passed ⇒ `tenant_purged` yes (permanent, deletable), the
///    row-deletion grant yes;
/// 6. the book fails to read ⇒ `Err` (keep).
#[tokio::test]
async fn tenant_purge_proofs_read_the_book_and_the_gate() -> Result<()> {
    use cairn_store::{
        proof::tenant_purge_grant,
        tenant_purge::{PrefixKind, KEEP},
        TenantId,
    };
    let (store, stores, _cluster) = setup().await?;
    let acme = TenantId::new("acme")?;
    let day = 24 * 3_600_000u64;
    let now = 100 * day;
    let k = PrefixKind::Archive;
    let proofs = |t: TenantId| {
        let stores = stores.clone();
        async move {
            let held = DeleteProof::tenant_deleting(&stores, &t, k).await?;
            let purged = DeleteProof::tenant_purged(&stores, &t, k, KEEP, now).await?;
            let grant = tenant_purge_grant(&stores, &t, KEEP, now).await?;
            Ok::<_, anyhow::Error>((held, purged, grant))
        }
    };
    // 1.
    purge_book(
        &store,
        false,
        &format!(
            "acme state=deleting deleting-ms={} purging-ms={}\n",
            now - 8 * day,
            now - day
        ),
    )
    .await?;
    let (h, p, g) = proofs(acme.clone()).await?;
    ensure!(
        h.is_err() && p.is_err() && g.is_err(),
        "gate off: {h:?} {p:?} {g:?}"
    );
    // 2.
    for book in ["acme state=suspended\n", "other hash-chain\n"] {
        purge_book(&store, true, book).await?;
        let (h, p, g) = proofs(acme.clone()).await?;
        ensure!(
            h.is_err() && p.is_err() && g.is_err(),
            "{book:?}: {h:?} {p:?} {g:?}"
        );
    }
    purge_book(
        &store,
        true,
        "default state=deleting deleting-ms=1 purging-ms=2\n",
    )
    .await?;
    let (h, p, g) = proofs(TenantId::default()).await?;
    ensure!(
        h.is_err() && p.is_err() && g.is_err(),
        "default: {h:?} {p:?} {g:?}"
    );
    // 3.
    purge_book(
        &store,
        true,
        &format!("acme state=deleting deleting-ms={}\n", now - 8 * day),
    )
    .await?;
    let (h, p, g) = proofs(acme.clone()).await?;
    let h = h.map_err(anyhow::Error::msg)?;
    ensure!(
        !h.may_purge()
            && h.subject()
                == &cairn_store::trash::Subject::TenantPrefix {
                    tenant: acme.clone(),
                    kind: k
                },
        "{h:?}"
    );
    ensure!(p.is_err() && g.is_err(), "purging-ms missing: {p:?} {g:?}");
    // 4.
    for book in [
        format!(
            "acme state=deleting deleting-ms={} purging-ms={now}\n",
            now - 6 * day
        ),
        format!("acme state=deleting deleting-ms=x purging-ms={now}\n"),
        format!(
            "acme state=deleting deleting-ms={} purging-ms=y\n",
            now - 8 * day
        ),
    ] {
        purge_book(&store, true, &book).await?;
        let (_, p, g) = proofs(acme.clone()).await?;
        ensure!(p.is_err() && g.is_err(), "{book:?}: {p:?} {g:?}");
    }
    // 5.
    purge_book(
        &store,
        true,
        &format!(
            "acme state=deleting deleting-ms={} purging-ms={}\n",
            now - 7 * day,
            now - day
        ),
    )
    .await?;
    let (_, p, g) = proofs(acme.clone()).await?;
    let p = p.map_err(anyhow::Error::msg)?;
    ensure!(p.may_purge() && p.table_version().is_none(), "{p:?}");
    let g = g.map_err(anyhow::Error::msg)?;
    ensure!(g.tenant() == &acme);
    // 6.
    store
        .put(
            &Path::from(format!("{PREFIX}/tenants")),
            bytes::Bytes::from_static(b"cairnflow-tenants/1\n/broken state=deleting\n").into(),
        )
        .await?;
    ensure!(
        DeleteProof::tenant_purged(&stores, &acme, k, KEEP, now)
            .await
            .is_err(),
        "a broken book must be Err"
    );
    Ok(())
}
