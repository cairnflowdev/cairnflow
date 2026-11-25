// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Library contracts: each test pins **one** assumption about SlateDB or object_store
//! that cairn relies on, so that a version bump (or a fork change) altering the semantics
//! goes red here — not in a data loss later on. The table of assumptions and who relies on
//! them: `docs/reliability/irreversible.md` §0.3. The contracts of the real S3 client
//! (OS1) live in `cairn-server/tests/s3_client_contract.rs` (they need a fake HTTP
//! server); the contracts of the S3 store itself (S3-1..S3-4) in
//! `tests/s3_store_contract.rs` (`#[ignore]`, they need a real store).

// Tests run on real time; only `sim_cluster` must be deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{ensure, Context, Result},
    cairn_store::{
        proof::{DeleteProof, DurableReplayMark},
        testkit::{Action, FaultyStore, Op, OpSet, PathClass, PathMatch, Rule, TripAt, Tripwire},
        Durability, LeadershipGrant, Mutations, ObjectStores, PartitionId, PartitionKey,
        PartitionWriter, PollIntervals, StoreLocation,
    },
    futures::TryStreamExt,
    slatedb::{
        admin::AdminBuilder,
        object_store::{
            local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore, ObjectStoreExt,
        },
    },
    std::{collections::BTreeSet, sync::Arc, time::Duration},
};

const INV: [u8; 24] = [0x17; 24];

fn stores_on(store: Arc<dyn ObjectStore>) -> ObjectStores {
    ObjectStores::single(StoreLocation::from_store(store, "cairn", "memory://cairn"))
}

async fn writer(stores: &ObjectStores, range: u32) -> Result<PartitionWriter> {
    Ok(PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(range)),
        stores.open_db(range).await?,
    )
    .await?)
}

/// Writes `n` journal rows, each waiting for durability (native WAL: one WAL SST per row).
async fn write_rows(w: &mut PartitionWriter, n: u32) -> Result<()> {
    for i in 0..n {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"hang");
        w.commit(m).await?;
        if let Durability::Wait(p) = w.durability()? {
            p.durable().await?;
        }
    }
    Ok(())
}

/// **G2.** `flush(MemTable)` only returns **after** the manifest has been written to the
/// store (`resolve_pending_flushes` runs after `write_manifest_update`). The shared WAL's
/// `flush_through`, `PartitionWriter::seal` and the cutting of segments at the stored mark
/// rely on it.
///
/// Every manifest PUT is delayed by 300 ms: if the flush returned before the manifest was
/// written, the read of the stored mark right after it would see the old mark.
#[tokio::test]
async fn g2_memtable_flush_returns_after_the_manifest_is_stored() -> Result<()> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = stores_on(faulty.clone());
    let mut w = writer(&stores, 0).await?;
    write_rows(&mut w, 3).await?;
    let before = DurableReplayMark::read(&stores, 0)
        .await?
        .map_or(0, |m| m.value());
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Class(PathClass::Manifest),
        Action::Delay(Duration::from_millis(300)),
    ));
    w.flush_memtable().await?;
    let after = DurableReplayMark::read(&stores, 0)
        .await?
        .map_or(0, |m| m.value());
    ensure!(
        after > before,
        "the memtable flush returned but the stored manifest's mark did not advance: {before} → {after}"
    );
    w.seal().await?;
    Ok(())
}

/// **G12.** `set_wal_name` (changing a Db's WAL kind, `adopt_shared_wal`) **refuses** a Db
/// whose WAL still has rows the manifest does not cover — the refusing direction, while
/// only the positive direction used to be pinned. Renaming over a WAL that still has rows
/// would drop acked rows (S7, NW-9).
#[tokio::test]
async fn g12_wal_kind_change_refuses_a_db_whose_wal_has_rows() -> Result<()> {
    let stores = stores_on(Arc::new(InMemory::new()));
    let mut w = writer(&stores, 0).await?;
    write_rows(&mut w, 3).await?;
    // Die without sealing: the WAL keeps three rows, the manifest does not cover them.
    drop(w);
    let refused = stores.adopt_shared_wal(0).await;
    ensure!(
        refused.is_err(),
        "changing the WAL kind of a Db with rows still in its WAL must be refused, got {refused:?}"
    );
    // The rows are still readable with the native WAL.
    let w = writer(&stores, 0).await?;
    let rows = w.journal_unbounded(&INV).await?;
    ensure!(rows.len() == 3, "{} / 3 rows left", rows.len());
    w.seal().await?;
    Ok(())
}

/// A temporary directory of its own for one run (cleaned up afterwards).
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let p = std::env::temp_dir().join(format!(
            "cairn-contract-{tag}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).expect("create temp dir");
        TempDir(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// **OS5.** `list(prefix)` matches **by path segment**: `ranges/1` is not a prefix of
/// `ranges/10/…`. D-d and orphan cleanup delete every object under `ranges/<id>` from one
/// LIST (`delete_range_objects`), removing a dead log for good deletes from a LIST of
/// `nodewal/<node>/<inc>/<stripe>` (`LogStore::delete_all`): a store matching bare strings
/// would delete ranges 10–19 when deleting range 1 (suspicion N5). Pinned for every store
/// cairn builds (`memory://`, `file://`) and for the test wrapper (`FaultyStore`).
#[tokio::test]
async fn o5_list_matches_whole_path_segments() -> Result<()> {
    let dir = TempDir::new("o5");
    let stores: Vec<(&str, Arc<dyn ObjectStore>)> = vec![
        ("InMemory", Arc::new(InMemory::new())),
        (
            "LocalFileSystem",
            Arc::new(LocalFileSystem::new_with_prefix(&dir.0)?),
        ),
        ("FaultyStore", FaultyStore::wrap(Arc::new(InMemory::new()))),
        (
            "StoreLocation's memory://",
            Arc::clone(StoreLocation::parse("memory://x")?.store()),
        ),
    ];
    for (name, store) in stores {
        for p in [
            "ranges/1/manifest/1",
            "ranges/10/manifest/1",
            "ranges/12/wal/1",
            "nodewal/n/1/0/0",
            "nodewal/n/10/0/0",
        ] {
            store
                .put(&Path::from(p), bytes::Bytes::from_static(b"x").into())
                .await?;
        }
        for (prefix, want) in [
            ("ranges/1", vec!["ranges/1/manifest/1"]),
            ("nodewal/n/1", vec!["nodewal/n/1/0/0"]),
        ] {
            let mut got: Vec<String> = store
                .list(Some(&Path::from(prefix)))
                .map_ok(|m| m.location.to_string())
                .try_collect()
                .await?;
            got.sort();
            ensure!(
                got == want,
                "{name}: list({prefix}) = {got:?}, expected {want:?}"
            );
        }
    }
    Ok(())
}

/// **G14.** Removing the close mark conditionally on the epoch (the fork patch
/// `Admin::reopen_for_writes_at_epoch`): a removal using a mark read before a **later**
/// close is fenced (`Fenced`) and removes nothing; a removal using the current mark
/// removes it, and retrying still succeeds. The rollback (`split::reopen_sources`) and the
/// self-heal (`heal_stale_close`) rely on it.
#[tokio::test]
async fn g14_reopen_at_epoch_keeps_a_newer_close() -> Result<()> {
    let stores = stores_on(Arc::new(InMemory::new()));
    let p = PartitionId(0);
    writer(&stores, 0).await?.seal().await?;
    stores.close_range_db(p).await?;
    let first = stores
        .range_db_close_mark(p)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the Db has no manifest yet"))?;
    ensure!(first.closed);
    stores.reopen_range_db_at(p, first.writer_epoch).await?;
    writer(&stores, 0).await?.seal().await?;
    stores.close_range_db(p).await?;

    let late = stores.reopen_range_db_at(p, first.writer_epoch).await;
    ensure!(
        matches!(late, Err(cairn_store::StoreError::Fenced)),
        "a removal using the old mark must be fenced: {late:?}"
    );
    ensure!(
        stores.range_db_closed(p).await? == Some(true),
        "the new mark was removed"
    );
    let now = stores
        .range_db_close_mark(p)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the Db has no manifest yet"))?;
    stores.reopen_range_db_at(p, now.writer_epoch).await?;
    stores.reopen_range_db_at(p, now.writer_epoch).await?;
    ensure!(stores.range_db_closed(p).await? == Some(false));
    Ok(())
}

// ───────────────────────── clone, compaction, GC

/// The whole partition key space: a clone without projection drops no journal row.
const FULL: (PartitionKey, PartitionKey) = (PartitionKey(0), PartitionKey(u64::MAX));

/// A bucket without prefix on `store`: paths are `ranges/<id>/…`.
fn bare_stores(store: Arc<dyn ObjectStore>) -> ObjectStores {
    ObjectStores::single(StoreLocation::from_store(store, "", "memory://"))
}

/// Fast background ticks: SlateDB's compactor, workers and GC every 50 ms, GC without
/// waiting for age — so "N cycles" within a few seconds is real.
fn fast_polls() -> PollIntervals {
    PollIntervals {
        manifest: Duration::from_millis(50),
        compactor: Duration::from_millis(50),
        compaction_worker: Duration::from_millis(50),
        gc: Duration::from_millis(50),
        gc_idle_skip_max: Duration::from_millis(50),
        gc_min_age: Duration::from_millis(0),
    }
}

/// Writes `rows` journal rows (from `from`), each waiting for durability **and flushing
/// the memtable**: one L0 SST per row, so the compactor has work.
async fn write_l0s(w: &mut PartitionWriter, from: u32, rows: u32) -> Result<()> {
    for i in from..from + rows {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, b"hang");
        w.commit(m).await?;
        if let Durability::Wait(p) = w.durability()? {
            p.durable().await?;
        }
        w.flush_memtable().await?;
    }
    Ok(())
}

/// Lineage 0 → 1 for G4: range 0 has three rows (sealed), cloned in full to 1, 1 writes
/// one more row (sealed). A later clone 1 → 2 has two `external_dbs` entries: 1 (the direct
/// source) and 0 (inherited) — a final checkpoint on both.
async fn g4_lineage(stores: &ObjectStores) -> Result<()> {
    let mut w = writer(stores, 0).await?;
    write_rows(&mut w, 3).await?;
    w.seal().await?;
    stores
        .clone_range(PartitionId(0), PartitionId(1), FULL)
        .await?;
    let mut w = writer(stores, 1).await?;
    let mut m = Mutations::new();
    m.append_journal(&INV, 3, b"ours-1");
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.seal().await?;
    Ok(())
}

/// `(ancestor Db path, final_checkpoint_id)` of every entry in the manifest of range
/// `range`.
async fn finals_of(stores: &ObjectStores, range: u32) -> Result<Vec<(String, String)>> {
    Ok(
        AdminBuilder::new(stores.range_path(range), stores.main().store().clone())
            .build()
            .read_manifest(None)
            .await?
            .context("the range has a manifest")?
            .external_dbs()
            .iter()
            .filter_map(|e| {
                e.final_checkpoint_id
                    .map(|id| (e.path.clone(), id.to_string()))
            })
            .collect(),
    )
}

/// The final (unbounded) checkpoint on range `on` created by range `by`.
async fn finals_by(stores: &ObjectStores, on: u32, by: u32) -> Result<usize> {
    Ok(stores
        .range_checkpoints(PartitionId(on))
        .await?
        .into_iter()
        .filter(|c| !c.expires && c.created_by == Some(PartitionId(by)))
        .count())
}

fn manifest_of(range: u32) -> String {
    format!("ranges/{range}/manifest/")
}

/// **G4 (order).** A clone writes the child manifest — already carrying the new
/// `final_checkpoint_id` of every entry — **before** creating the final checkpoint on each
/// ancestor (`create_clone_manifest`: the child manifest, then the `write_checkpoint` loop
/// over each `external_db`). SM-12 relies on that: `discard_range_db` takes the child
/// manifest as the prior record of every checkpoint the clone places on ancestors — if
/// the checkpoint came first, a death between the two steps would leave a checkpoint on a
/// live range with nothing left saying whose it is.
///
/// The wrapping store records the PUT order around clone 1 → 2. Must see: the first PUT
/// under `ranges/2/manifest/` comes before every PUT under `ranges/0/manifest/` (the final
/// checkpoint on the inherited ancestor) and before the last PUT under
/// `ranges/1/manifest/` (the final checkpoint on the direct source; the PUT before it on 1
/// is the source's temporary checkpoint).
#[tokio::test]
async fn g4_clone_writes_the_child_manifest_before_ancestor_checkpoints() -> Result<()> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = bare_stores(faulty.clone());
    g4_lineage(&stores).await?;
    faulty.record_writes();
    stores
        .clone_range(PartitionId(1), PartitionId(2), FULL)
        .await?;
    let puts: Vec<String> = faulty
        .take_writes()
        .into_iter()
        .filter(|(op, _)| *op == Op::Put)
        .map(|(_, p)| p.to_string())
        .collect();
    let first = |dir: &str| puts.iter().position(|p| p.starts_with(dir));
    let last = |dir: &str| puts.iter().rposition(|p| p.starts_with(dir));
    let child = first(&manifest_of(2)).context("the clone wrote no child manifest")?;
    let on_ancestor = first(&manifest_of(0)).context("the clone wrote nothing on ancestor 0")?;
    let on_source = last(&manifest_of(1)).context("the clone wrote nothing on source 1")?;
    ensure!(
        child < on_ancestor && child < on_source,
        "the child manifest (#{child}) must come before the checkpoint on 0 (#{on_ancestor}) \
         and on 1 (#{on_source}): {puts:#?}"
    );
    // Both final checkpoints exist, they belong to 2, and the child manifest names them.
    ensure!(finals_by(&stores, 0, 2).await? == 1 && finals_by(&stores, 1, 2).await? == 1);
    ensure!(finals_of(&stores, 2).await?.len() == 2);
    Ok(())
}

/// **G4 (death between the two steps).** The interleaving:
/// 1. Lineage 0 → 1 (as above); clone 1 → 2 starts.
/// 2. The child manifest of 2 is written; the manifest PUT on ancestor 0 (the final
///    checkpoint) — the process dies right before that PUT leaves (`Tripwire` `Before`
///    freezes the whole store, like `kill -9` seen from the bucket).
/// 3. A new process on the same bucket: the rollback (or the orphan cleanup loop) calls
///    `discard_range_db(2, [1])`.
///
/// Must see: at step 3, the Db of 2 has a manifest listing a final checkpoint on 0 **that
/// does not exist**; `discard_range_db` treats the absent checkpoint as removed
/// (`delete_checkpoint` reports no error — SM-12 in the `split-merge` guide), removes 2's
/// checkpoint on 1 if it had come into being, deletes the Db of 2, and leaves no final
/// checkpoint of 2 on 0 or 1. Range 1 still reads every row.
#[tokio::test]
async fn g4_clone_dying_between_child_manifest_and_ancestor_checkpoint_is_discarded() -> Result<()>
{
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulty = FaultyStore::wrap(Arc::clone(&inner));
    let stores = bare_stores(faulty.clone());
    g4_lineage(&stores).await?;
    let wire = Tripwire::new(0, TripAt::Before);
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Prefix(manifest_of(0)),
        Action::Trip(wire.clone()),
    ));
    let died = tokio::time::timeout(
        Duration::from_secs(5),
        stores.clone_range(PartitionId(1), PartitionId(2), FULL),
    )
    .await;
    ensure!(
        died.is_err() && wire.fired(),
        "the clone must hang at the ancestor's manifest PUT: {died:?}, wire fired = {}",
        wire.fired()
    );
    drop(stores);

    // A new process: no rules, the same bucket.
    let stores = bare_stores(FaultyStore::wrap(inner));
    ensure!(
        stores.range_db_exists(PartitionId(2)).await?,
        "the child manifest must exist before the checkpoint on the ancestor (G4)"
    );
    let ancestor = stores.range_path(0).to_string();
    let finals = finals_of(&stores, 2).await?;
    let (_, missing) = finals
        .iter()
        .find(|(p, _)| p.trim_matches('/') == ancestor.trim_matches('/'))
        .with_context(|| format!("the child manifest has no entry for ancestor 0: {finals:?}"))?;
    ensure!(
        !stores
            .range_checkpoints(PartitionId(0))
            .await?
            .iter()
            .any(|c| &c.id == missing),
        "checkpoint {missing} on 0 must not exist (died before the PUT)"
    );
    ensure!(finals_by(&stores, 0, 2).await? == 0);

    stores
        .discard_range_db(
            DeleteProof::assume_db(PartitionId(2)),
            &[PartitionId(1)],
            None,
        )
        .await?;
    ensure!(!stores.range_db_exists(PartitionId(2)).await?);
    for on in [0, 1] {
        ensure!(
            finals_by(&stores, on, 2).await? == 0,
            "range {on} still has a final checkpoint of the cleaned-up child"
        );
    }
    let w = writer(&stores, 1).await?;
    ensure!(
        w.journal_unbounded(&INV).await?.len() == 4,
        "range 1 lost rows"
    );
    w.seal().await?;
    Ok(())
}

/// **G15 (names).** Every clone **and** union places on **every** `external_dbs` entry of
/// the child Db — the direct source and the inherited ancestors alike — exactly one final
/// (unbounded) checkpoint **named after the child Db's path** (a fork patch). The second
/// fence of D-d (`DeleteProof::retired_db`) reads, from the ancestor's side, who still
/// stands on it by that very name.
///
/// Lineage 0 → 1 (sealed), then 1 → 2 (lower half), 1 → 3 (upper half), then union
/// {2, 3} → 4. Must see: for each child 2, 3, 4 and each of its `external_dbs` entries, the
/// entry's ancestor has exactly one final checkpoint whose name points back to that child;
/// and no final checkpoint on 0..=3 lacks a name.
#[tokio::test]
async fn g15_clone_and_union_name_their_final_checkpoint_on_every_ancestor() -> Result<()> {
    let stores = bare_stores(Arc::new(InMemory::new()));
    g4_lineage(&stores).await?;
    const MID: u64 = u64::MAX / 2;
    let low = (PartitionKey(0), PartitionKey(MID));
    let high = (PartitionKey(MID + 1), PartitionKey(u64::MAX));
    stores
        .clone_range(PartitionId(1), PartitionId(2), low)
        .await?;
    stores
        .clone_range(PartitionId(1), PartitionId(3), high)
        .await?;
    stores
        .union_ranges(
            &[(PartitionId(2), low), (PartitionId(3), high)],
            PartitionId(4),
        )
        .await?;
    for child in [2, 3, 4] {
        let entries = finals_of(&stores, child).await?;
        ensure!(!entries.is_empty(), "child {child} stands on no ancestor");
        for (path, _) in entries {
            let ancestor: u32 = path
                .rsplit('/')
                .next()
                .and_then(|s| s.parse().ok())
                .with_context(|| format!("unknown ancestor path {path:?}"))?;
            ensure!(
                finals_by(&stores, ancestor, child).await? == 1,
                "ancestor {ancestor} must have exactly one final checkpoint named after child {child}"
            );
        }
    }
    for ancestor in 0..=3 {
        for c in stores.range_checkpoints(PartitionId(ancestor)).await? {
            ensure!(
                c.expires || c.created_by.is_some(),
                "final checkpoint {} on {ancestor} has no name (or an unknown name {:?})",
                c.id,
                c.name
            );
        }
    }
    Ok(())
}

/// Every object under `ranges/<range>/`.
async fn objects_under(store: &Arc<dyn ObjectStore>, range: u32) -> Result<BTreeSet<String>> {
    Ok(store
        .list(Some(&Path::from(format!("ranges/{range}"))))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await?)
}

/// **G5.** A Db's compaction and GC only run in the process that has it **open** for
/// writing; a quiesced source does not change its manifest. D-d not reading children in
/// flight is still correct partly thanks to this (N2: a quiesced source does not detach an
/// ancestor a child stands on), and the clone's 300 s temporary checkpoint relies on it.
///
/// Order: Db 0 has six L0s (enough for the compactor to have work) — sealed (quiesced),
/// then the close mark is set (`close_range_db`), clone 0 → 1 (a child in flight); open a
/// `DbReader` (the follower path, `open_reader`) following 0 with every tick at 50 ms,
/// `gc_min_age` 0, reading through it many times over ~3 s (≈ 60 poll cycles).
///
/// Must see: no write reaches `ranges/0/` (PUT, DELETE, copy) meanwhile, and the set of
/// objects under `ranges/0/` does not change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g5_quiesced_closed_db_gains_no_objects_under_compactor_and_gc() -> Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulty = FaultyStore::wrap(Arc::clone(&inner));
    let stores = bare_stores(faulty.clone()).with_polls(fast_polls());
    let mut w = writer(&stores, 0).await?;
    write_l0s(&mut w, 0, 6).await?;
    w.seal().await?;
    stores.close_range_db(PartitionId(0)).await?;
    stores
        .clone_range(PartitionId(0), PartitionId(1), FULL)
        .await?;
    let before = objects_under(&inner, 0).await?;
    faulty.record_writes();

    let reader = stores.open_reader(0, Duration::from_millis(50)).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut reads = 0;
    while tokio::time::Instant::now() < deadline {
        ensure!(
            reader.journal(&INV).await?.len() == 6,
            "the reader is missing rows"
        );
        reads += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let writes: Vec<String> = faulty
        .take_writes()
        .into_iter()
        .filter(|(_, p)| p.as_ref().starts_with("ranges/0/"))
        .map(|(op, p)| format!("{op:?} {p}"))
        .collect();
    ensure!(
        writes.is_empty(),
        "a quiesced + closed Db was written while only a reader followed it ({reads} reads): {writes:#?}"
    );
    let after = objects_under(&inner, 0).await?;
    ensure!(
        before == after,
        "objects under ranges/0 changed: added {:?}, lost {:?}",
        after.difference(&before).collect::<Vec<_>>(),
        before.difference(&after).collect::<Vec<_>>()
    );
    drop(reader);
    Ok(())
}

/// **G8 (cairn's side).** SlateDB's SST GC cuts by the stored manifest **and** the
/// manifests of every live checkpoint: compaction + GC on the parent (`gc_min_age` 0) does
/// not delete SSTs a child's final checkpoint still pins (S1 of the inventory).
///
/// Order: parent 0 has three L0s (sealed); clone 0 → 1; **delete the clone's temporary
/// checkpoint** on 0 (so only the final checkpoint pins — otherwise the 300 s temporary
/// checkpoint would hide what needs checking); reopen the parent, write more L0s until the
/// compactor has merged L0 and the parent's GC has deleted at least one SST produced
/// **after** the clone (proving the GC runs, without waiting for age).
///
/// Must see: every SST referenced by the manifest **pinned by the final checkpoint** (L0
/// and sorted runs — exactly what the child stands on) is still there, and the child
/// (through a reader, not opening a writer — so the child's compactor does not cut the
/// wire) reads all three rows. SSTs already on the store at clone time that this manifest
/// no longer references (the parent's compactor merged them before the seal) may be
/// deleted by GC by the rules — the first version of this test compared them too and went
/// red under nextest's load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g8_parent_compaction_and_gc_keep_what_the_child_reads() -> Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner))
        .with_polls(fast_polls())
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    let mut w = writer(&stores, 0).await?;
    write_l0s(&mut w, 0, 3).await?;
    w.seal().await?;
    stores
        .clone_range(PartitionId(0), PartitionId(1), FULL)
        .await?;
    let ssts = |all: &BTreeSet<String>| -> BTreeSet<String> {
        all.iter()
            .filter(|p| p.ends_with(".sst") && !p.contains("/wal/"))
            .cloned()
            .collect()
    };
    let at_clone = ssts(&objects_under(&inner, 0).await?);

    let admin = AdminBuilder::new(stores.range_path(0), Arc::clone(&inner)).build();
    let checkpoints = admin.list_checkpoints(None).await?;
    // SSTs pinned by the child's final checkpoint: the L0s and sorted runs of that manifest.
    let final_cp = checkpoints
        .iter()
        .find(|c| c.expire_time.is_none())
        .context("the child must have a final checkpoint on the parent")?;
    let pinned_manifest = admin
        .read_manifest(Some(final_cp.manifest_id))
        .await?
        .context("the final checkpoint's manifest")?;
    let pinned: BTreeSet<String> = pinned_manifest
        .l0()
        .iter()
        .chain(
            pinned_manifest
                .compacted()
                .iter()
                .flat_map(|r| r.sst_views()),
        )
        .map(|v| format!("ranges/0/compacted/{}.sst", v.sst.id.value()))
        .collect();
    ensure!(!pinned.is_empty(), "the final checkpoint pins no SST");
    ensure!(
        pinned.is_subset(&at_clone),
        "pinned SSTs missing from the store: {:?}",
        pinned.difference(&at_clone).collect::<Vec<_>>()
    );

    // Drop the clone's temporary (bounded) checkpoint on the parent.
    let temps: Vec<_> = checkpoints
        .into_iter()
        .filter(|c| c.expire_time.is_some())
        .collect();
    ensure!(
        !temps.is_empty(),
        "the clone must leave a temporary checkpoint on the parent"
    );
    for c in temps {
        admin.delete_checkpoint(c.id).await?;
    }
    ensure!(
        finals_by(&stores, 0, 1).await? == 1,
        "the child must have a final checkpoint on the parent"
    );

    let mut w = writer(&stores, 0).await?;
    let mut seen = at_clone.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut next = 3;
    loop {
        write_l0s(&mut w, next, 1).await?;
        next += 1;
        let now = ssts(&objects_under(&inner, 0).await?);
        seen.extend(now.iter().cloned());
        let reaped: Vec<_> = seen
            .difference(&now)
            .filter(|p| !at_clone.contains(*p))
            .collect();
        if !reaped.is_empty() {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the parent's GC deleted no SST produced after the clone within 60 s ({next} rows)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    w.seal().await?;

    let now = ssts(&objects_under(&inner, 0).await?);
    let lost: Vec<_> = pinned.difference(&now).collect();
    ensure!(
        lost.is_empty(),
        "the parent's GC deleted SSTs pinned by the child's final checkpoint: {lost:?}"
    );
    let reader = stores.open_reader(1, Duration::from_millis(50)).await?;
    let rows = reader.journal(&INV).await?;
    ensure!(rows.len() == 3, "the child reads {} / 3 rows", rows.len());
    Ok(())
}

/// One hand-written round of the G18 contract (readable without opening `store_probe`):
/// a winning CAS on `X` → GET If-None-Match old etag returns 200 with the new body (not
/// 304); the new etag returns 304; another client reads the new body.
async fn g18_one_round(
    name: &str,
    writer: &Arc<dyn ObjectStore>,
    reader: &Arc<dyn ObjectStore>,
) -> Result<()> {
    use slatedb::object_store::{Error as OsError, GetOptions, PutMode};
    let x = Path::from("g18/x");
    let cas = |body: &'static [u8], mode: PutMode| {
        cairn_store::cas::put_resolving_or_overwrite(
            writer,
            &x,
            bytes::Bytes::from_static(body),
            mode,
        )
    };
    let cairn_store::cas::Cas::Written(v1) = cas(b"v1", PutMode::Create).await? else {
        anyhow::bail!("{name}: Create v1 lost");
    };
    let cairn_store::cas::Cas::Written(v2) = cas(b"v2", PutMode::Update(v1.clone())).await? else {
        anyhow::bail!("{name}: CAS v1 → v2 lost");
    };
    let (old, new) = (v1.e_tag.context("etag v1")?, v2.e_tag.context("etag v2")?);
    ensure!(old != new, "{name}: etag unchanged after the write: {old}");
    for (who, store) in [("writing client", writer), ("other client", reader)] {
        let cond = |etag: &str| GetOptions {
            if_none_match: Some(etag.to_string()),
            ..Default::default()
        };
        match store.get_opts(&x, cond(&old)).await {
            Err(OsError::NotModified { .. }) => {
                anyhow::bail!(
                    "{name}/{who}: GET If-None-Match old etag returned 304 after a winning CAS"
                )
            }
            Err(e) => return Err(e.into()),
            Ok(got) => {
                let b = got.bytes().await?;
                ensure!(
                    &b[..] == b"v2",
                    "{name}/{who}: old etag ⇒ read {b:?}, expected v2"
                );
            }
        }
        match store.get_opts(&x, cond(&new)).await {
            Err(OsError::NotModified { .. }) => {}
            other => {
                anyhow::bail!("{name}/{who}: GET If-None-Match new etag must be 304, got {other:?}")
            }
        }
        let b = store.get(&x).await?.bytes().await?;
        ensure!(
            &b[..] == b"v2",
            "{name}/{who}: read right after the write saw {b:?}"
        );
    }
    Ok(())
}

/// One store client in G18.
type Client = Arc<dyn ObjectStore>;

/// **G18.** A conditional GET never returns a stale `304`. Solo mode (LS-16: a self-check
/// GET returning `304` ⇒ nobody has challenged ⇒ renew), the read fence (LS-5) and the
/// coordinator fence rely on it: after a winning CAS on `X`, a GET of `X` If-None-Match the
/// **old** etag returns `200` with the new body; the new etag ⇒ `304`; a read right after
/// a write (same client or another) sees the new body; every write changes the etag. One
/// hand-written round, then `store_probe::conditional_get` for 50 rounds (bounded rounds —
/// they catch weak consistency) — exactly the probe `cairn-ctl cluster enable-quiet-lease`
/// runs on the real bucket. Pinned for `memory://`, `file://` (two separate
/// `LocalFileSystem` clients on the same directory, overwriting since there is no CAS —
/// `put_resolving_or_overwrite`) and the `FaultyStore` wrapper. The real S3 row:
/// `s3_store_contract.rs::s3_5_conditional_get_never_serves_a_stale_304`.
#[tokio::test]
async fn g18_conditional_get_never_serves_a_stale_304() -> Result<()> {
    let dir = TempDir::new("g18");
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulty: Arc<dyn ObjectStore> = FaultyStore::wrap(Arc::new(InMemory::new()));
    let loc = StoreLocation::parse("memory://g18")?;
    let pairs: Vec<(&str, Client, Client)> = vec![
        ("InMemory", Arc::clone(&mem), mem),
        ("FaultyStore", Arc::clone(&faulty), faulty),
        (
            "StoreLocation's memory://",
            Arc::clone(loc.store()),
            Arc::clone(loc.store()),
        ),
        (
            "LocalFileSystem",
            Arc::new(LocalFileSystem::new_with_prefix(&dir.0)?),
            Arc::new(LocalFileSystem::new_with_prefix(&dir.0)?),
        ),
    ];
    for (name, writer, reader) in pairs {
        g18_one_round(name, &writer, &reader).await?;
        let nonce = cairn_store::store_probe::nonce();
        let got = cairn_store::store_probe::conditional_get(
            &writer,
            &reader,
            &Path::from("p"),
            &nonce,
            50,
        )
        .await?;
        let passed = got.map_err(|why| anyhow::anyhow!("{name}: {why}"))?;
        ensure!(passed.rounds == 50, "{name}: {passed:?}");
        let left = writer
            .head(&cairn_store::store_probe::path(&Path::from("p"), &nonce))
            .await;
        ensure!(
            left.is_err(),
            "{name}: the probe left a probe object behind"
        );
    }
    Ok(())
}

// ─────────────────────────────── create_ts (CG-7)
//
// Invocation timestamps read the `create_ts` SlateDB attaches to every row at write time
// (`batch_write.rs`: `mono_clock.now()` per batch, epoch ms, monotonic per Db) — not one
// byte of cairn's own. Each test below pins one clause: the mark is kept through reopen
// (native WAL replay), compaction, clone, union, `DbReader` and shared-WAL replay after a
// node death; an overwrite ⇒ a new mark. A clause going red on a SlateDB bump is a stop
// point (the fallback: a mark table of our own behind a gate).

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

fn journal_key(id: &[u8; 24], i: u32) -> Vec<u8> {
    cairn_store::keys::journal_entry(id, i)
}

async fn ts_of(db: &slatedb::Db, key: &[u8]) -> Result<i64> {
    Ok(db.get_key_value(key).await?.context("no row")?.create_ts)
}

/// Writes journal row `i` of `id` (waiting for durability), returns the `create_ts` read
/// back right away — also checking it lies within the real time around the write.
async fn stamp(w: &mut PartitionWriter, id: &[u8; 24], i: u32) -> Result<i64> {
    let before = now_ms();
    let mut m = Mutations::new();
    m.append_journal(id, i, b"stamp");
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    let ts = ts_of(w.store(), &journal_key(id, i)).await?;
    let after = now_ms();
    ensure!(
        before - 5 <= ts && ts <= after + 5,
        "create_ts {ts} outside the real time of the write [{before}, {after}]"
    );
    Ok(ts)
}

/// **CG-7 — reopen and overwrite.** The row only lives in the native WAL (no memtable
/// flush); a second Db opening the same range (fencing the first) replays the WAL and
/// reads the same mark. Overwriting the same key afterwards ⇒ a new, larger mark.
#[tokio::test]
async fn create_ts_survives_reopen_from_wal_and_moves_on_overwrite() -> Result<()> {
    let stores = bare_stores(Arc::new(InMemory::new()));
    let mut w = writer(&stores, 0).await?;
    let t0 = stamp(&mut w, &INV, 0).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let db = stores.open_db(0).await?;
    ensure!(
        ts_of(&db, &journal_key(&INV, 0)).await? == t0,
        "WAL replay changed create_ts"
    );
    drop(w);
    let mut w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let t1 = stamp(&mut w, &INV, 0).await?;
    ensure!(t1 > t0, "the overwrite kept the old mark: {t0} → {t1}");
    Ok(())
}

/// **CG-7 — compaction.** Five rows, one L0 each; the compactor (50 ms tick) merges until
/// there is a sorted run and at most one L0 left (the compactor's merge threshold may
/// leave the last L0). Every mark reads back unchanged — at least four rows went through
/// compaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_ts_survives_compaction() -> Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner)).with_polls(fast_polls());
    let mut w = writer(&stores, 0).await?;
    let mut ts = Vec::new();
    for i in 0..5 {
        ts.push(stamp(&mut w, &INV, i).await?);
        w.flush_memtable().await?;
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let admin = AdminBuilder::new(stores.range_path(0), Arc::clone(&inner)).build();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let m = admin.read_manifest(None).await?.context("manifest")?;
        if m.l0().len() <= 1 && !m.compacted().is_empty() {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the compactor did not merge L0 within 30 s (L0 {}, sorted runs {})",
            m.l0().len(),
            m.compacted().len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (i, t) in ts.iter().enumerate() {
        let got = ts_of(w.store(), &journal_key(&INV, i as u32)).await?;
        ensure!(
            got == *t,
            "row {i}: create_ts {t} → {got} through compaction"
        );
    }
    Ok(())
}

/// **CG-7 — clone, union, `DbReader`.** Range 0 has one row in each half of the key
/// space; sealed; each half cloned to 1 and 2; union 1 + 2 into 3. The marks read at 1, 2,
/// 3 (Dbs open for writing) and at 3 through a `DbReader` equal the marks at write time on 0.
#[tokio::test]
async fn create_ts_survives_clone_union_and_reader() -> Result<()> {
    const HI: [u8; 24] = [0x97; 24];
    const MID: u64 = u64::MAX / 2;
    let low = (PartitionKey(0), PartitionKey(MID));
    let high = (PartitionKey(MID + 1), PartitionKey(u64::MAX));
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner));
    let mut w = writer(&stores, 0).await?;
    let t_lo = stamp(&mut w, &INV, 0).await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let t_hi = stamp(&mut w, &HI, 0).await?;
    w.seal().await?;
    stores
        .clone_range(PartitionId(0), PartitionId(1), low)
        .await?;
    stores
        .clone_range(PartitionId(0), PartitionId(2), high)
        .await?;
    stores
        .union_ranges(
            &[(PartitionId(1), low), (PartitionId(2), high)],
            PartitionId(3),
        )
        .await?;
    for (range, want) in [
        (1, vec![(INV, t_lo)]),
        (2, vec![(HI, t_hi)]),
        (3, vec![(INV, t_lo), (HI, t_hi)]),
    ] {
        let db = stores.open_db(range).await?;
        for (id, t) in want {
            let got = ts_of(&db, &journal_key(&id, 0)).await?;
            ensure!(got == t, "range {range}: create_ts {t} → {got}");
        }
        db.close().await?;
    }
    let reader = slatedb::DbReader::builder(stores.range_path(3), Arc::clone(&inner))
        .with_reader_mode(slatedb::DbReaderMode::FollowLatest)
        .build()
        .await?;
    for (id, t) in [(INV, t_lo), (HI, t_hi)] {
        let got = reader
            .get_key_value(journal_key(&id, 0))
            .await?
            .context("the DbReader does not see the row")?
            .create_ts;
        ensure!(got == t, "DbReader: create_ts {t} → {got}");
    }
    reader.close().await?;
    Ok(())
}

/// **CG-7 — shared-WAL replay after a node death.** Node A claims range 0 over the shared
/// WAL, writes one row (durable: only in A's log, not yet in L0), then dies (stops
/// renewing). Node B claims the range: seals A's log, replays into the Db. Must see: the
/// mark B reads equals the mark A read at write time — `nodewal/format.rs` encodes
/// `create_ts` per row, the replay attaches no new mark.
#[tokio::test]
async fn create_ts_survives_shared_wal_replay_after_crash() -> Result<()> {
    use cairn_store::{acquire_shared, Claim, LeaseConfig, NodeWal, NodeWalConfig, Session};
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&inner),
        "cairn",
        "memory://cairn",
    ));
    let cfg = || NodeWalConfig {
        window: Duration::from_millis(5),
        stripes: 2,
        inflight: 1,
        object_cap: 1 << 20,
        linger: NodeWalConfig::default().linger,
        frame_age_flush: Duration::from_secs(3600),
        put_timeout: Duration::from_millis(500),
        put_attempts: 2,
        gc_interval: Duration::from_millis(50),
        meter: None,
        rotation_net: None,
    };
    let start = |name: &'static str| {
        let inner = Arc::clone(&inner);
        let stores = stores.clone();
        async move {
            let session = Session::establish(
                inner,
                &Path::from("cairn"),
                LeaseConfig::new()
                    .with_node_id(name)
                    .with_ttl(Duration::from_millis(400))
                    .with_renew_interval(Duration::from_millis(50)),
            )
            .await?;
            let id = session.current().context("session just opened")?;
            let wal = NodeWal::start(&stores, id, cfg());
            for _ in 0..200 {
                match session.claim(PartitionId(0)).await? {
                    Claim::Won(grant, mut keeper) => {
                        let db = acquire_shared(&stores, &wal, &mut keeper, 0).await?;
                        let w = PartitionWriter::open(grant, db).await?;
                        return Ok::<_, anyhow::Error>((session, wal, w, keeper));
                    }
                    Claim::Held(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
            anyhow::bail!("{name} did not claim range 0")
        }
    };
    let (_sa, _wa, mut wa, ka) = start("a").await?;
    let t = stamp(&mut wa, &INV, 0).await?;
    ka.crash_handle().crash();
    let (_sb, _wb, wb, _kb) = start("b").await?;
    let got = ts_of(wb.store(), &journal_key(&INV, 0)).await?;
    ensure!(got == t, "replay shared WAL: create_ts {t} → {got}");
    std::mem::forget((wa, ka));
    Ok(())
}

// ---------------------------------------------------------------------------
// The SST disk cache: the fork's `CachedObjectStore`, one per node
// (`ObjectStores::with_disk_cache`). Rows D1–D5 of `docs/reliability/irreversible.md` §0.3.

/// Every file in the disk cache, as a path relative to the cache root.
fn cached_files(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(rel) = p.strip_prefix(root) {
                out.push(rel.display().to_string());
            }
        }
    }
    out.sort();
    out
}

async fn cached_stores(store: Arc<dyn ObjectStore>, dir: &std::path::Path) -> Result<ObjectStores> {
    Ok(bare_stores(store)
        .with_disk_cache(cairn_store::disk_cache::DiskCacheConfig::new(dir))
        .await?)
}

/// Writes `rows` journal rows carrying `payload`, each waiting for durability and flushing
/// the memtable (one L0 SST per row), then seals.
async fn sealed_with(stores: &ObjectStores, range: u32, rows: u32, payload: &[u8]) -> Result<()> {
    let mut w = writer(stores, range).await?;
    for i in 0..rows {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, payload);
        w.commit(m).await?;
        if let Durability::Wait(p) = w.durability()? {
            p.durable().await?;
        }
        w.flush_memtable().await?;
    }
    w.seal().await?;
    Ok(())
}

/// The journal of `INV` in range `range`, read with a reader opened through `stores` (a new
/// RAM cache per `ObjectStores`: this read goes down to the disk cache / object store).
async fn journal_via(stores: &ObjectStores, range: u32) -> Result<Vec<(u32, Vec<u8>)>> {
    let r = stores.open_reader(range, Duration::from_secs(60)).await?;
    let rows = r.journal(&INV).await?;
    r.close().await?;
    Ok(rows.into_iter().map(|(i, b)| (i, b.to_vec())).collect())
}

/// **D1. Only compacted SSTs go into the disk cache.** The WAL, manifests and every
/// unlabelled read (cairn's own I/O) go straight to the object store (`policy.rs`
/// `DefaultGetPolicy`/`DefaultPutPolicy`): nothing that decides durability or fencing is
/// read from the node's disk.
///
/// Must see: after writing (cache_on_flush) and reading back, every cache file lies under
/// `ranges/0/compacted/`, none under `wal/`, `manifest/`, or cairn's own objects
/// (`owner`, `format`…).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d1_disk_cache_holds_only_compacted_ssts() -> Result<()> {
    let dir = TempDir::new("disk-d1");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = cached_stores(store.clone(), &dir.0).await?;
    sealed_with(&stores, 0, 3, b"hang").await?;
    let again = cached_stores(store, &dir.0).await?;
    ensure!(journal_via(&again, 0).await?.len() == 3);
    let snap = again.disk_cache().context("cache enabled")?.snapshot();
    ensure!(
        snap.hits > 0,
        "reading back did not hit the disk cache: {snap:?}"
    );
    let files = cached_files(&dir.0);
    ensure!(
        !files.is_empty(),
        "the cache is empty: cache_on_flush did not run"
    );
    for f in &files {
        ensure!(
            f.starts_with("ranges/0/compacted/") && f.contains(".sst/"),
            "a file other than a compacted SST in the cache: {f}"
        );
    }
    Ok(())
}

/// **D2. A corrupt part ⇒ read again correctly.** Parts are written to a temporary file
/// then `rename`d, without fsync: after a crash a part may carry garbage bytes. A block
/// checksum mismatch ⇒ SlateDB reads again **once** with `Refetch` (`sst_io.rs`), drops the
/// corrupt part and overwrites it.
///
/// 1. Write three rows (SSTs into the disk cache).
/// 2. Overwrite every part in the cache with garbage bytes, keeping the length.
/// 3. Read back through that cache (a new RAM cache).
///
/// Must see: the journal exactly as read without a cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d2_corrupt_part_is_refetched() -> Result<()> {
    let dir = TempDir::new("disk-d2");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    sealed_with(&cached_stores(store.clone(), &dir.0).await?, 0, 3, b"hang").await?;
    let mut hit = 0;
    for f in cached_files(&dir.0) {
        if !f.contains("_part") {
            continue;
        }
        let p = dir.0.join(&f);
        let len = std::fs::metadata(&p)?.len() as usize;
        std::fs::write(&p, vec![0xA5; len])?;
        hit += 1;
    }
    ensure!(hit > 0, "no part to corrupt");
    let want = journal_via(&bare_stores(store.clone()), 0).await?;
    ensure!(want.len() == 3);
    let cached = cached_stores(store, &dir.0).await?;
    let got = journal_via(&cached, 0).await?;
    ensure!(
        got == want,
        "read through corrupt parts: {got:?} ≠ {want:?}"
    );
    let snap = cached.disk_cache().context("cache enabled")?.snapshot();
    ensure!(
        snap.accesses > 0,
        "the read did not go through the disk cache: {snap:?}"
    );
    Ok(())
}

/// **D3. Two ranges sharing one cache never read each other's data.** The cache key is the
/// object path (SSTs are named by ULID under `ranges/<id>/compacted/`), not the user key:
/// the same journal key in two ranges is two different SSTs.
///
/// Must see: range 0 reads `a`, range 1 reads `b`, through the same cache directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d3_two_ranges_share_one_cache() -> Result<()> {
    let dir = TempDir::new("disk-d3");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = cached_stores(store.clone(), &dir.0).await?;
    sealed_with(&stores, 0, 2, b"a").await?;
    sealed_with(&stores, 1, 2, b"b").await?;
    let again = cached_stores(store, &dir.0).await?;
    for (range, v) in [(0, b"a"), (1, b"b")] {
        let rows = journal_via(&again, range).await?;
        ensure!(
            rows.len() == 2 && rows.iter().all(|(_, b)| b.as_slice() == v),
            "range {range}: {rows:?}"
        );
    }
    ensure!(
        cached_files(&dir.0)
            .iter()
            .any(|f| f.starts_with("ranges/1/")),
        "range 1 did not get into the cache"
    );
    Ok(())
}

/// **D4. An unwritable disk ⇒ fall back to the object store.** I/O errors of the cache
/// (disk full, permissions) only lose the cache (`object_store.rs`: part-store errors are
/// ignored, part-read errors ⇒ read upstream).
///
/// Must see: a read-only cache directory, writes and reads back are still right, the
/// cache stays empty.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d4_unwritable_cache_falls_back_to_the_store() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("disk-d4");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = cached_stores(store.clone(), &dir.0).await?;
    std::fs::set_permissions(&dir.0, std::fs::Permissions::from_mode(0o500))?;
    // A process ignoring permissions (root) makes the test meaningless.
    if std::fs::write(dir.0.join("probe"), b"x").is_ok() {
        std::fs::set_permissions(&dir.0, std::fs::Permissions::from_mode(0o700))?;
        eprintln!("skipping D4: the process can write into a read-only directory (root?)");
        return Ok(());
    }
    let r = async {
        sealed_with(&stores, 0, 2, b"hang").await?;
        let rows = journal_via(&cached_stores(store, &dir.0).await?, 0).await?;
        ensure!(rows.len() == 2, "{rows:?}");
        ensure!(
            cached_files(&dir.0).is_empty(),
            "{:?}",
            cached_files(&dir.0)
        );
        Ok(())
    }
    .await;
    std::fs::set_permissions(&dir.0, std::fs::Permissions::from_mode(0o700))?;
    r
}

/// **D5. A GCed SST never makes a read wrong.** The cache never invalidates: correct
/// because ULID-named SSTs are never reused, and a Db only reads the SSTs in its manifest.
/// Worst case: the whole Db is deleted from the store (D-d) while a copy stays in the
/// cache, then a new Db at **the same path** (cairn never reuses range ids, but the ULID is
/// what keeps it right).
///
/// Must see: the new Db reads its own data, not the old copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d5_gc_deleted_db_is_never_served_from_cache() -> Result<()> {
    let dir = TempDir::new("disk-d5");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = cached_stores(store.clone(), &dir.0).await?;
    sealed_with(&stores, 0, 2, b"old").await?;
    ensure!(!cached_files(&dir.0).is_empty());
    let old: Vec<Path> = store
        .list(Some(&Path::from("ranges/0")))
        .map_ok(|m| m.location)
        .try_collect()
        .await?;
    for p in old {
        store.delete(&p).await?;
    }
    let fresh = cached_stores(store, &dir.0).await?;
    sealed_with(&fresh, 0, 2, b"new").await?;
    let rows = journal_via(&fresh, 0).await?;
    ensure!(
        rows.len() == 2 && rows.iter().all(|(_, b)| b.as_slice() == b"new"),
        "{rows:?}"
    );
    Ok(())
}

/// **D1–D3 on a real S3 store** (store coverage): the same three contracts as on
/// `InMemory`, on the store of `CAIRNFLOW_STORE_CONTRACT_URI` (like `s3_store_contract.rs`)
/// — byte-range reads and SST names go through the real S3 client.
///
/// ```sh
/// CAIRNFLOW_STORE_CONTRACT_URI=s3://bucket/prefix AWS_ENDPOINT=… AWS_ALLOW_HTTP=true \
/// AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-east-1 \
///   cargo test -p cairn-store --test library_contracts d_on_real_s3 -- --ignored --nocapture
/// ```
///
/// Must see: (D1) after writing and reading back, the cache holds only SSTs under
/// `…/ranges/0/compacted/` and reading back hits the cache; (D2) every part overwritten
/// with garbage, reading back through the cache yields exactly the journal read without a
/// cache; (D3) two ranges sharing one cache read their own data. Without the URI it prints
/// "skipping" and passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a real S3 store — CAIRNFLOW_STORE_CONTRACT_URI"]
async fn d_on_real_s3() -> Result<()> {
    let Ok(uri) = std::env::var("CAIRNFLOW_STORE_CONTRACT_URI") else {
        eprintln!("d_on_real_s3: skipping — CAIRNFLOW_STORE_CONTRACT_URI not set");
        return Ok(());
    };
    let run = format!(
        "{}/disk-cache-{}-{}",
        uri.trim_end_matches('/'),
        std::process::id(),
        now_ms()
    );
    eprintln!("d_on_real_s3: {run}");
    // A new `ObjectStores` each time: a new RAM block cache, reads go down to the disk cache.
    let plain =
        || -> Result<ObjectStores> { Ok(ObjectStores::single(StoreLocation::parse(&run)?)) };
    let cached = |dir: &std::path::Path| {
        let dir = dir.to_path_buf();
        let s = plain();
        async move {
            Ok::<_, anyhow::Error>(
                s?.with_disk_cache(cairn_store::disk_cache::DiskCacheConfig::new(dir))
                    .await?,
            )
        }
    };
    let result = async {
        // D1.
        let dir = TempDir::new("disk-s3-d1");
        sealed_with(&cached(&dir.0).await?, 0, 3, b"hang").await?;
        let again = cached(&dir.0).await?;
        ensure!(journal_via(&again, 0).await?.len() == 3);
        let snap = again.disk_cache().context("cache enabled")?.snapshot();
        ensure!(
            snap.hits > 0,
            "D1: reading back did not hit the disk cache: {snap:?}"
        );
        let files = cached_files(&dir.0);
        ensure!(!files.is_empty(), "D1: the cache is empty");
        for f in &files {
            ensure!(
                f.contains("ranges/0/compacted/") && f.contains(".sst/"),
                "D1: a file other than a compacted SST in the cache: {f}"
            );
        }
        // D2: corrupt every part, then read back through that cache.
        let mut hit = 0;
        for f in cached_files(&dir.0) {
            if f.contains("_part") {
                let p = dir.0.join(&f);
                let len = std::fs::metadata(&p)?.len() as usize;
                std::fs::write(&p, vec![0xA5; len])?;
                hit += 1;
            }
        }
        ensure!(hit > 0, "D2: no part");
        let want = journal_via(&plain()?, 0).await?;
        let got = journal_via(&cached(&dir.0).await?, 0).await?;
        ensure!(got == want && want.len() == 3, "D2: {got:?} ≠ {want:?}");
        // D3: ranges 1 and 2 share journal keys with different values, and one cache.
        let dir = TempDir::new("disk-s3-d3");
        let s = cached(&dir.0).await?;
        sealed_with(&s, 1, 2, b"a").await?;
        sealed_with(&s, 2, 2, b"b").await?;
        let again = cached(&dir.0).await?;
        for (range, v) in [(1, b"a"), (2, b"b")] {
            let rows = journal_via(&again, range).await?;
            ensure!(
                rows.len() == 2 && rows.iter().all(|(_, b)| b.as_slice() == v),
                "D3 range {range}: {rows:?}"
            );
        }
        eprintln!(
            "d_on_real_s3: D1, D2, D3 green ({} parts corrupted in D2)",
            hit
        );
        Ok(())
    }
    .await;
    // Clean up every object of the run.
    let loc = StoreLocation::parse(&run)?;
    let all: Vec<Path> = loc
        .store()
        .list(Some(loc.prefix()))
        .map_ok(|m| m.location)
        .try_collect()
        .await?;
    for p in all {
        let _ = loc.store().delete(&p).await;
    }
    result
}

/// Whether any object under `ranges/<range>/` contains `needle` (reading the bytes directly
/// — the test's Db does not compress blocks).
async fn bytes_contain(
    store: &Arc<dyn ObjectStore>,
    range: u32,
    needle: &[u8],
) -> Result<Vec<String>> {
    let mut hit = Vec::new();
    for p in objects_under(store, range).await? {
        let body = match store.get(&Path::from(p.as_str())).await {
            Ok(g) => g.bytes().await?,
            Err(slatedb::object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        if body.windows(needle.len()).any(|w| w == needle) {
            hit.push(p);
        }
    }
    Ok(hit)
}

/// **G23**: one **full** compaction of the base tree into the last sorted run
/// (`ObjectStores::submit_full_compaction`, through `Admin`) drops the tail tombstones,
/// and SlateDB's GC deletes the old SSTs — the bytes of logically deleted rows **leave
/// every object** of the Db. The tenant purge (phase B) relies on this so that "deleted"
/// means deleted, not merely "unreadable".
///
/// Order: a row carrying its own marker into L0 (memtable flush), a few other L0s so the
/// compactor has runs; the marker shows in an SST. Delete the row (a tombstone, memtable
/// flush), request a full compaction, wait for it. Must see: within 60 s (GC `min_age` 0)
/// no object under `ranges/0/` still contains the marker, and the Db still reads every
/// other row.
#[tokio::test]
async fn g23_full_compaction_into_the_last_run_drops_deleted_bytes() -> Result<()> {
    const MARK: &[u8] = b"g23-data-of-deleted-tenant-0123456789";
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // Every compaction makes the compactor set a checkpoint (15 minutes by default) pinning
    // the source SSTs — old bytes leave the store after that period plus `gc_min_age`. The
    // test shortens both.
    let stores = bare_stores(Arc::clone(&inner))
        .with_polls(fast_polls())
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    let mut w = writer(&stores, 0).await?;
    let mut m = Mutations::new();
    m.append_journal(&INV, 100, MARK);
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.flush_memtable().await?;
    write_l0s(&mut w, 0, 4).await?;
    ensure!(
        !bytes_contain(&inner, 0, MARK).await?.is_empty(),
        "the marker must be in an SST before the delete (a test seeing nothing is meaningless)"
    );
    let mut m = Mutations::new();
    m.purge_journal_entry(&INV, 100);
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.flush_memtable().await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut id = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has L0s but nothing to compact")?;
    loop {
        match stores.full_compaction_done(0, &mut id).await? {
            Some(true) => {
                let hit = bytes_contain(&inner, 0, MARK).await?;
                if hit.is_empty() {
                    break;
                }
                // Compacted yet still there: a new L0 (a late WAL flush) or GC not yet run —
                // request once more if the manifest still has sources outside the last run.
                if tokio::time::Instant::now() >= deadline {
                    let admin = AdminBuilder::new(stores.range_path(0), Arc::clone(&inner)).build();
                    let m = admin.read_manifest(None).await?.context("manifest")?;
                    let live: Vec<String> = m
                        .l0()
                        .iter()
                        .chain(m.compacted().iter().flat_map(|r| r.sst_views().iter()))
                        .map(|v| v.sst.id.value().to_string())
                        .collect();
                    anyhow::bail!(
                        "60 s after the full compaction the marker is still in {hit:?} (live SSTs: {live:?})"
                    );
                }
                if let Some(again) = stores.submit_full_compaction(0).await?.requested() {
                    id = again;
                }
            }
            Some(false) => {
                id = stores
                    .submit_full_compaction(0)
                    .await?
                    .requested()
                    .context("re-request after a failed compaction")?;
            }
            None => {}
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the full compaction did not finish within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rows = w.journal_unbounded(&INV).await?;
    ensure!(rows.len() == 4, "{} / 4 other rows left", rows.len());
    w.seal().await?;
    Ok(())
}

/// **G28**: a clone of a Db made **after** its full compaction (G23) finished takes the
/// compacted manifest — the child references no SST still carrying the deleted rows'
/// bytes, even though the source's old SSTs are still on the store waiting for GC. The
/// tenant purge relies on this so that the children of a split made **after** the source
/// range's compaction mark inherit "clean" (`tenant_purge::inherits`) without being
/// compacted again.
///
/// Order: a row carrying its own marker into L0, a few other L0s; delete, flush, request a
/// full compaction, wait for it (GC left at its default — the old SSTs **remain** on the
/// store); seal the source; clone 0 → 1. Must see: no SST referenced by the child's
/// manifest (L0s and sorted runs) contains the marker, there is at least one such SST, the
/// marker is still in some old object of the source (the test is not vacuous), and the
/// child reads every other row.
#[tokio::test]
async fn g28_clone_after_full_compaction_references_no_deleted_bytes() -> Result<()> {
    const MARK: &[u8] = b"g28-data-of-deleted-tenant-9876543210";
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner)).with_polls(fast_polls());
    let mut w = writer(&stores, 0).await?;
    let mut m = Mutations::new();
    m.append_journal(&INV, 100, MARK);
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.flush_memtable().await?;
    write_l0s(&mut w, 0, 4).await?;
    let mut m = Mutations::new();
    m.purge_journal_entry(&INV, 100);
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.flush_memtable().await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut id = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has L0s but nothing to compact")?;
    loop {
        match stores.full_compaction_done(0, &mut id).await? {
            Some(true) => break,
            Some(false) => {
                id = stores
                    .submit_full_compaction(0)
                    .await?
                    .requested()
                    .context("re-request after a failed compaction")?;
            }
            None => {}
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the full compaction did not finish within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    w.seal().await?;
    ensure!(
        !bytes_contain(&inner, 0, MARK).await?.is_empty(),
        "old SSTs carrying the marker must remain on the store (default GC) — otherwise the test distinguishes nothing"
    );
    stores
        .clone_range(PartitionId(0), PartitionId(1), FULL)
        .await?;
    let admin = AdminBuilder::new(stores.range_path(1), Arc::clone(&inner)).build();
    let m = admin
        .read_manifest(None)
        .await?
        .context("the child's manifest")?;
    let referenced: BTreeSet<String> = m
        .l0()
        .iter()
        .chain(m.compacted().iter().flat_map(|r| r.sst_views().iter()))
        .map(|v| v.sst.id.value().to_string())
        .collect();
    ensure!(!referenced.is_empty(), "the child references no SST");
    let mut seen = 0;
    for range in [0, 1] {
        for p in objects_under(&inner, range).await? {
            if !referenced.iter().any(|id| p.contains(id.as_str())) {
                continue;
            }
            seen += 1;
            let body = inner.get(&Path::from(p.as_str())).await?.bytes().await?;
            ensure!(
                !body.windows(MARK.len()).any(|w| w == MARK),
                "the child references {p}, an SST still holding the deleted row's bytes"
            );
        }
    }
    ensure!(
        seen > 0,
        "found no object for the SSTs the child references: {referenced:?}"
    );
    let child = writer(&stores, 1).await?;
    let rows = child.journal_unbounded(&INV).await?;
    ensure!(
        rows.len() == 4,
        "the child has {} / 4 other rows left",
        rows.len()
    );
    child.seal().await?;
    Ok(())
}

/// **G24**: a compaction requested through `Admin::submit_compaction`
/// (`ObjectStores::submit_full_compaction`) is **not** signalled to anything in the
/// process — it sits in `Submitted` until the coordinator's next poll
/// (`DEFAULT_POLLS.compactor` = 1 h). `Db::wake_compactor` (fork) raises the `l0_flushed`
/// signal: the coordinator re-reads the compaction state as on a poll and runs the
/// compaction at once. The tenant purge (phase B) calls it right after requesting a full
/// compaction.
///
/// Order: the default poll tick; one L0 (below the auto-compaction threshold); request a
/// full compaction. Must see: after 1 s the compaction has not finished (nothing else
/// wakes it); after `wake_compactor` it finishes within 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g24_wake_compactor_runs_a_submitted_compaction_without_a_poll() -> Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner));
    let mut w = writer(&stores, 0).await?;
    write_l0s(&mut w, 0, 1).await?;
    // Let the coordinator handle the signal of the flush above before requesting a compaction.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut id = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has L0s but nothing to compact")?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    ensure!(
        stores.full_compaction_done(0, &mut id).await?.is_none(),
        "the compaction requested through Admin ran with nobody waking it — G24 is no longer needed"
    );
    w.wake_compactor();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match stores.full_compaction_done(0, &mut id).await? {
            Some(true) => break,
            Some(false) => anyhow::bail!("the compaction failed"),
            None => {}
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "10 s after wake_compactor the compaction still has not finished"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(
        w.journal_unbounded(&INV).await?.len() == 1,
        "every row remains after the compaction"
    );
    w.seal().await?;
    Ok(())
}

/// **G30**: two full compactions requested back to back (`submit_full_compaction` twice:
/// two tenant purge rounds, or two nodes, ask for the same range) name the same
/// destination run. The coordinator of the fork runs one job per destination: the second
/// request waits until the first ends, then ends too (done or failed). The Db stays open.
///
/// Before the fork fix, an `Admin` request skipped the destination check of the
/// coordinator. With L0 sources, the rule "one L0 job per segment" still failed the second
/// request; with sorted runs only, nothing did. The coordinator promoted both jobs, the
/// compaction worker panicked on the second one (`compactor_executor.rs`: one task per
/// destination), and the Db closed. The two jobs stayed in the compaction ledger, so every
/// later open of the range failed the same way: the range never served again.
///
/// Order: two sorted runs and no L0 (two L0s compacted into run 0, two more into run 1);
/// request a full compaction twice (sources: both runs, destination: run 0) and wake the
/// compactor (its coordinator sees both requests at once). Must see: both requests end
/// within 30 s, and the Db then still commits and reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g30_two_full_compactions_with_one_destination_keep_the_db_open() -> Result<()> {
    use slatedb::compactor::{CompactionSpec, SourceId};
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner));
    let admin = AdminBuilder::new(stores.range_path(0), Arc::clone(&inner)).build();
    let mut w = writer(&stores, 0).await?;
    for (from, run) in [(0u32, 0u32), (2, 1)] {
        write_l0s(&mut w, from, 2).await?;
        let m = admin.read_manifest(None).await?.context("manifest")?;
        let sources = m.l0().iter().map(|v| SourceId::SstView(v.id)).collect();
        let mut id = stores
            .watch_compaction(
                0,
                admin
                    .submit_compaction(CompactionSpec::new(sources, run))
                    .await?
                    .id(),
            )
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            w.wake_compactor();
            match stores.full_compaction_done(0, &mut id).await? {
                Some(true) => break,
                Some(false) => anyhow::bail!("the compaction of the L0s into run {run} failed"),
                None => {}
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "the compaction of the L0s into run {run} did not end within 30 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let m = admin.read_manifest(None).await?.context("manifest")?;
    ensure!(
        m.l0().is_empty() && m.compacted().len() == 2,
        "the setup must leave two sorted runs and no L0: {} L0s, {} runs",
        m.l0().len(),
        m.compacted().len()
    );
    let mut first = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has runs but nothing to compact")?;
    let mut second = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has runs but nothing to compact")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        w.wake_compactor();
        let a = stores.full_compaction_done(0, &mut first).await?;
        let b = stores.full_compaction_done(0, &mut second).await?;
        if a.is_some() && b.is_some() {
            ensure!(
                a == Some(true) || b == Some(true),
                "neither full compaction finished: {a:?} {b:?}"
            );
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "30 s after the requests the two full compactions have not both ended: {a:?} {b:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    write_l0s(&mut w, 4, 1).await?;
    let rows = w.journal_unbounded(&INV).await?;
    ensure!(
        rows.len() == 5,
        "{} / 5 rows after the compactions",
        rows.len()
    );
    w.seal().await?;
    Ok(())
}

/// The status of compaction `id` in the newest `.compactions` version; `None` = the ledger
/// no longer holds it.
async fn ledger_status(
    admin: &slatedb::admin::Admin,
    id: ulid::Ulid,
) -> Result<Option<slatedb::compactor::CompactionStatus>> {
    Ok(admin.read_compaction(id, None).await?.map(|c| c.status()))
}

/// Wakes the compactor until compaction `id` has status `want` (30 s at most).
async fn wait_ledger_status(
    w: &PartitionWriter,
    admin: &slatedb::admin::Admin,
    id: ulid::Ulid,
    want: slatedb::compactor::CompactionStatus,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        w.wake_compactor();
        let now = ledger_status(admin, id).await?;
        if now == Some(want) {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "compaction {id} is {now:?} after 30 s, expected {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **G31**: the compaction ledger (`.compactions`) keeps the active jobs and the finished job
/// with the highest id. A finished job with a lower id leaves the ledger at a later write. So a
/// job that the newest version no longer holds proves nothing about how it ended. A tenant
/// purge that reads "absent" as "done" writes a clean proof over a compaction that failed: the
/// deleted rows stay in the SSTs. The fork stores every finish once with its final status, and
/// `full_compaction_done` looks a job that left up in the stored versions after the last one
/// it saw.
///
/// Order: two L0s. A request that the coordinator must refuse (its only source, run 77, does
/// not exist), so it ends `Failed`. A full compaction with a higher id ends `Completed`, and
/// the ledger drops the failed job. One more L0 and one more full compaction end `Completed`,
/// and the ledger drops the first full compaction. Must see: the failed job reads "failed",
/// the first full compaction reads "done" although the ledger no longer holds either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g31_a_job_that_left_the_ledger_is_not_done() -> Result<()> {
    use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner));
    let admin = AdminBuilder::new(stores.range_path(0), Arc::clone(&inner)).build();
    let mut w = writer(&stores, 0).await?;
    write_l0s(&mut w, 0, 2).await?;
    let failed = admin
        .submit_compaction(CompactionSpec::new(vec![SourceId::SortedRun(77)], 77))
        .await?
        .id();
    let mut failed_watch = stores.watch_compaction(0, failed).await?;
    wait_ledger_status(&w, &admin, failed, CompactionStatus::Failed).await?;
    let mut done = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has L0s but nothing to compact")?;
    wait_ledger_status(&w, &admin, done.id.parse()?, CompactionStatus::Completed).await?;
    ensure!(
        ledger_status(&admin, failed).await?.is_none(),
        "the ledger still holds the failed job: the test no longer covers the trim"
    );
    let seen = stores.full_compaction_done(0, &mut failed_watch).await?;
    ensure!(
        seen == Some(false),
        "a failed compaction that left the ledger reads {seen:?}, not failed: a tenant purge \
         would write a false proof"
    );

    write_l0s(&mut w, 2, 1).await?;
    let later = stores
        .submit_full_compaction(0)
        .await?
        .requested()
        .context("the Db has an L0 but nothing to compact")?;
    wait_ledger_status(&w, &admin, later.id.parse()?, CompactionStatus::Completed).await?;
    ensure!(
        ledger_status(&admin, done.id.parse()?).await?.is_none(),
        "the ledger still holds the first full compaction: the test no longer covers the trim"
    );
    let seen = stores.full_compaction_done(0, &mut done).await?;
    ensure!(
        seen == Some(true),
        "a completed compaction that left the ledger reads {seen:?}: a tenant purge would \
         request it again for ever on a busy range"
    );
    w.seal().await?;
    Ok(())
}

/// Whether an SST that the newest manifest of range 0 references contains `needle`.
async fn live_ssts_contain(
    admin: &slatedb::admin::Admin,
    store: &Arc<dyn ObjectStore>,
    needle: &[u8],
) -> Result<bool> {
    let m = admin.read_manifest(None).await?.context("manifest")?;
    let live: Vec<String> = m
        .l0()
        .iter()
        .chain(m.compacted().iter().flat_map(|r| r.sst_views().iter()))
        .map(|v| v.sst.id.value().to_string())
        .collect();
    let hits = bytes_contain(store, 0, needle).await?;
    Ok(hits
        .iter()
        .any(|p| live.iter().any(|id| p.contains(id.as_str()))))
}

/// **G32**: a compaction keeps every version that an open snapshot needs, also a compaction
/// into the last run. The job's `retention_min_seq` is the `recent_snapshot_min_seq` of the
/// manifest that the worker reads after it claims the job. The flusher writes that value: the
/// oldest open snapshot, or `last_l0_seq` when none is open. So a full compaction that runs
/// while a snapshot older than a delete is open keeps the deleted bytes in a live SST. A
/// tenant purge must not request its compaction then: it waits until, right after its flush,
/// `recent_snapshot_min_seq >= last_l0_seq`.
///
/// Order: a row with a marker into L0, two more L0s. Open a snapshot, delete the row, flush.
/// Must see: (1) `submit_full_compaction` requests nothing while the snapshot is open; (2) the
/// same full compaction requested directly through `Admin` ends `Completed` and a live SST
/// still holds the marker (the gate is needed); (3) once the snapshot is dropped, a flush and
/// a request do start a full compaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g32_a_full_compaction_keeps_what_an_open_snapshot_needs() -> Result<()> {
    use slatedb::compactor::{CompactionSpec, SourceId};
    const MARK: &[u8] = b"g32-data-a-snapshot-still-reads-0123456789";
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner));
    let admin = AdminBuilder::new(stores.range_path(0), Arc::clone(&inner)).build();
    let mut w = writer(&stores, 0).await?;
    let mut m = Mutations::new();
    m.append_journal(&INV, 100, MARK);
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.flush_memtable().await?;
    write_l0s(&mut w, 0, 2).await?;
    let db = w.read_store();
    let snapshot = db.snapshot()?;
    let mut m = Mutations::new();
    m.purge_journal_entry(&INV, 100);
    w.commit(m).await?;
    if let Durability::Wait(p) = w.durability()? {
        p.durable().await?;
    }
    w.flush_memtable().await?;

    let asked = stores.submit_full_compaction(0).await?;
    ensure!(
        asked == cairn_store::FullCompaction::SnapshotOpen,
        "a full compaction was requested ({asked:?}) while a snapshot older than the delete is \
         open: it keeps the deleted bytes under a clean proof"
    );

    // The scheduler can compact some L0s first; then the request fails validation and goes
    // again.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let manifest = admin.read_manifest(None).await?.context("manifest")?;
        let mut sources: Vec<SourceId> = manifest
            .l0()
            .iter()
            .map(|v| SourceId::SstView(v.id))
            .collect();
        sources.extend(
            manifest
                .compacted()
                .iter()
                .map(|r| SourceId::SortedRun(r.id)),
        );
        let destination = manifest.compacted().last().map_or(0, |r| r.id);
        let direct = admin
            .submit_compaction(CompactionSpec::new(sources, destination))
            .await?
            .id();
        let mut watch = stores.watch_compaction(0, direct).await?;
        let ended = loop {
            w.wake_compactor();
            if let Some(ended) = stores.full_compaction_done(0, &mut watch).await? {
                break ended;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "the direct full compaction did not end within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        if ended {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the direct full compaction kept failing for 60 s"
        );
    }
    ensure!(
        live_ssts_contain(&admin, &inner, MARK).await?,
        "the full compaction dropped bytes that an open snapshot needs: the snapshot gate is \
         no longer needed (or the test lost its marker)"
    );

    drop(snapshot);
    w.flush_memtable_fresh().await?;
    ensure!(
        stores
            .submit_full_compaction(0)
            .await?
            .requested()
            .is_some(),
        "no full compaction after the snapshot was dropped: the purge would wait forever"
    );
    w.seal().await?;
    Ok(())
}

/// `(last_l0_seq, first byte of the sequence_tracker)` of a SlateDB manifest object. The
/// object is 2 bytes of format version (big-endian; 2 = `ManifestV2`) then a flatbuffer.
/// SlateDB does not expose the tracker's bytes, so the test decodes by hand along the
/// vtable slots of `schemas/manifest.fbs` (`VT_LAST_L0_SEQ` = 30, `VT_SEQUENCE_TRACKER` =
/// 34, as in `root_generated.rs`). `last_l0_seq` lets the caller compare with
/// `Admin::read_manifest`: a mismatch ⇒ wrong slot decoded, the test goes red rather than
/// green by mistake.
fn manifest_tracker(bytes: &[u8]) -> Result<(u64, Option<u8>)> {
    const VT_LAST_L0_SEQ: usize = 30;
    const VT_SEQUENCE_TRACKER: usize = 34;
    ensure!(bytes.len() > 2, "empty manifest");
    let format = u16::from_be_bytes([bytes[0], bytes[1]]);
    ensure!(
        format == 2,
        "manifest format {format}, the test only decodes ManifestV2"
    );
    let buf = &bytes[2..];
    let slice = |at: usize, n: usize| buf.get(at..at + n).context("read outside the flatbuffer");
    let u16_at =
        |at: usize| -> Result<usize> { Ok(u16::from_le_bytes(slice(at, 2)?.try_into()?) as usize) };
    let u32_at =
        |at: usize| -> Result<usize> { Ok(u32::from_le_bytes(slice(at, 4)?.try_into()?) as usize) };
    let table = u32_at(0)?;
    let back = i32::from_le_bytes(slice(table, 4)?.try_into()?);
    let vtable = usize::try_from(table as i64 - back as i64)?;
    let vt_len = u16_at(vtable)?;
    let field = |slot: usize| -> Result<Option<usize>> {
        if slot + 2 > vt_len {
            return Ok(None);
        }
        let off = u16_at(vtable + slot)?;
        Ok((off != 0).then_some(table + off))
    };
    let last_l0_seq = match field(VT_LAST_L0_SEQ)? {
        Some(at) => u64::from_le_bytes(slice(at, 8)?.try_into()?),
        None => 0,
    };
    let version = match field(VT_SEQUENCE_TRACKER)? {
        None => None,
        Some(at) => {
            let vector = at + u32_at(at)?;
            match u32_at(vector)? {
                0 => None,
                _ => Some(*slice(vector + 4, 1)?.first().context("empty tracker")?),
            }
        }
    };
    Ok((last_l0_seq, version))
}

/// **G26 — manifests cairn writes always carry a format-v1 sequence tracker.** SlateDB
/// #2181 (0.18) writes a v2 tracker when a delta-of-delta of a seq or a timestamp falls
/// outside `i32`; a binary before 0.18 meeting v2 **panics** when decoding the manifest
/// (`flatbuffer_types.rs`, `expect`). Downgrading is only safe while this holds: cairn
/// never sets seqs by hand, so the seq of a Db never jumps by more than 2³¹ between two
/// 60 s samples.
///
/// Load: lineage 0 → 1 (write, seal, clone, write more — `g4_lineage`), then range 1
/// reopened, three L0s, a full compaction. Scans every `*.manifest` on the bucket.
///
/// Must see: every tracker present starts with byte 1, and at least one tracker is
/// present (the test is not vacuous); `last_l0_seq` decoded by hand matches
/// `Admin::read_manifest`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g26_every_manifest_keeps_sequence_tracker_format_v1() -> Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = bare_stores(Arc::clone(&inner));
    g4_lineage(&stores).await?;
    let mut w = writer(&stores, 1).await?;
    write_l0s(&mut w, 10, 3).await?;
    if let Some(mut id) = stores.submit_full_compaction(1).await?.requested() {
        w.wake_compactor();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while stores.full_compaction_done(1, &mut id).await?.is_none() {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "the full compaction did not finish"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    w.seal().await?;
    let manifests: Vec<Path> = inner
        .list(None)
        .map_ok(|m| m.location)
        .try_filter(|p| futures::future::ready(p.as_ref().ends_with(".manifest")))
        .try_collect()
        .await?;
    let mut trackers = 0;
    for path in &manifests {
        let bytes = inner.get(path).await?.bytes().await?;
        let (last_l0_seq, version) = manifest_tracker(&bytes)?;
        let (db, file) = path
            .as_ref()
            .rsplit_once("/manifest/")
            .context("a manifest outside the manifest/ directory")?;
        let id: u64 = file.trim_end_matches(".manifest").parse()?;
        let stored = AdminBuilder::new(db, Arc::clone(&inner))
            .build()
            .read_manifest(Some(id))
            .await?
            .context("Admin cannot read a manifest just listed")?;
        ensure!(
            stored.last_l0_seq() == last_l0_seq,
            "{path}: last_l0_seq decoded by hand {last_l0_seq}, Admin {} — vtable slot mismatch",
            stored.last_l0_seq()
        );
        if let Some(version) = version {
            trackers += 1;
            ensure!(version == 1, "{path}: sequence tracker format {version}");
        }
    }
    ensure!(trackers > 0, "no manifest carries a tracker: {manifests:?}");
    Ok(())
}
