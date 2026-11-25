// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Deleted by mistake, then restored: every path by which cairn deletes
//! customer data leaves a copy in the trash, and restoring puts exactly the
//! acked data back where it was. The fourth path — a dead shared-WAL log — is in
//! `tests/shared_wal.rs` (`dead_log_forgotten_by_mistake_is_restored`); the
//! fifth is the op ledger of a settled op (`op_ledger_deleted_by_mistake_is_restored`).

// Tests run on real time; only `sim_cluster` must be deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{ensure, Context, Result},
    cairn_store::{
        trash::{Subject, Trash},
        Durability, LeadershipGrant, Mutations, ObjectStores, PartitionId, PartitionKey,
        PartitionWriter, StoreLocation,
    },
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
    std::sync::Arc,
};

const INV: [u8; 24] = [0x42; 24];

fn memory_stores() -> ObjectStores {
    ObjectStores::single(StoreLocation::from_store(
        Arc::new(InMemory::new()),
        "cairn",
        "memory://cairn",
    ))
}

fn bin(stores: &ObjectStores) -> Trash {
    Trash::new(Arc::clone(stores.main().store()), stores.main().prefix())
}

async fn writer(stores: &ObjectStores, range: u32) -> Result<PartitionWriter> {
    Ok(PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(range)),
        stores.open_db(range).await?,
    )
    .await?)
}

/// Write `n` durable journal rows, starting at index `from`.
async fn write_rows(w: &mut PartitionWriter, from: u32, n: u32, payload: &[u8]) -> Result<()> {
    for i in from..from + n {
        let mut m = Mutations::new();
        m.append_journal(&INV, i, payload);
        w.commit(m).await?;
        if let Durability::Wait(p) = w.durability()? {
            p.durable().await?;
        }
    }
    Ok(())
}

/// Restore the only entry for `subject` in the trash.
async fn restore_only(t: &Trash, subject: &Subject) -> Result<()> {
    let entries = t.entries().await?;
    let mine: Vec<_> = entries
        .iter()
        .filter(|e| e.subject.as_ref() == Some(subject))
        .collect();
    ensure!(
        mine.len() == 1,
        "the trash must have exactly one entry for {subject}, has {}: {entries:?}",
        mine.len()
    );
    let r = t.restore(&mine[0].id).await?;
    ensure!(
        r.conflicts.is_empty(),
        "conflicts while restoring: {:?}",
        r.conflicts
    );
    ensure!(r.restored > 0, "the restore copied nothing back");
    Ok(())
}

/// **D-d deletes a range's Db by mistake** (bug class RD6: a Db still needed
/// that GC believes is not). Range 0 has 10 acked rows and is closed for good;
/// `delete_retired_db` deletes it.
///
/// Must see: the Db disappears from its place, the trash has one `range-db 0`
/// entry, and after the restore it reopens with all 10 rows.
#[tokio::test]
async fn retired_db_deleted_by_mistake_is_restored() -> Result<()> {
    let stores = memory_stores();
    let mut w = writer(&stores, 0).await?;
    write_rows(&mut w, 0, 10, b"acked").await?;
    w.seal().await?;

    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
            None,
        )
        .await?;
    ensure!(
        !stores.range_db_exists(PartitionId(0)).await?,
        "the Db is still in its place"
    );

    restore_only(&bin(&stores), &Subject::RangeDb(0)).await?;
    ensure!(stores.range_db_exists(PartitionId(0)).await?, "Db missing");
    let w = writer(&stores, 0).await?;
    let rows = w.journal_unbounded(&INV).await?;
    ensure!(rows.len() == 10, "restored {} / 10 rows", rows.len());
    Ok(())
}

/// **A split roll-back deletes a child's Db by mistake** (the bug class of a
/// child the coordinator still uses). Parent 0 is closed and cloned into child
/// 1, the child writes 5 rows of its own; `discard_range_db` discards the
/// child.
///
/// Must see: the trash has a `range-db 1` entry; after the restore the child
/// opens with the parent's rows **and** the 5 rows only it has — read at once,
/// before the parent's GC gets to delete SSTs the child's checkpoint pinned
/// (that part relies on bucket versioning).
#[tokio::test]
async fn discarded_child_db_is_restored() -> Result<()> {
    let stores = memory_stores();
    let mut parent = writer(&stores, 0).await?;
    write_rows(&mut parent, 0, 3, b"parents-own").await?;
    parent.seal().await?;
    let child = PartitionId(1);
    stores
        .clone_range(
            PartitionId(0),
            child,
            (PartitionKey(0), PartitionKey(u64::MAX)),
        )
        .await?;
    let mut c = writer(&stores, 1).await?;
    write_rows(&mut c, 3, 5, b"child-data").await?;
    c.seal().await?;

    stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(child),
            &[PartitionId(0)],
            None,
        )
        .await?;
    ensure!(
        !stores.range_db_exists(child).await?,
        "the child's Db is still in its place"
    );

    restore_only(&bin(&stores), &Subject::RangeDb(1)).await?;
    let c = writer(&stores, 1).await?;
    let rows = c.journal_unbounded(&INV).await?;
    ensure!(
        rows.len() == 8,
        "the child restored {} / 8 rows",
        rows.len()
    );
    ensure!(
        rows.iter().filter(|(_, v)| &v[..] == b"child-data").count() == 5,
        "rows only the child had are missing"
    );
    Ok(())
}

/// **A split roll-back deletes a child's owner record by mistake**
/// (`delete_owner`, a bare DELETE).
///
/// Must see: the record leaves `owners/`, and the restore brings it back byte
/// for byte.
#[tokio::test]
async fn deleted_owner_record_is_restored() -> Result<()> {
    let stores = memory_stores();
    let store = stores.main().store();
    let path = Path::from("cairn/owners/range-7");
    store
        .put(&path, bytes::Bytes::from_static(b"owner-record").into())
        .await?;

    cairn_store::delete_owner(
        store,
        stores.main().prefix(),
        cairn_store::proof::DeleteProof::assume_owner(PartitionId(7)),
    )
    .await?;
    ensure!(
        store.head(&path).await.is_err(),
        "the owner record is still in its place"
    );

    restore_only(&bin(&stores), &Subject::Owner(7)).await?;
    let got = store.get(&path).await.context("owner record not back")?;
    ensure!(&got.bytes().await?[..] == b"owner-record", "wrong content");
    Ok(())
}

/// **The op ledger**: the ledger sweep moves a settled op's `ops/<key>` into
/// the trash; deleted by mistake, the restore brings back exactly the ledger
/// (phase, op, `sealed`).
#[tokio::test]
async fn op_ledger_deleted_by_mistake_is_restored() -> Result<()> {
    use cairn_split::{Op, Phase};
    let stores = memory_stores();
    let (store, prefix) = (stores.main().store(), stores.main().prefix());
    let record = cairn_store::ledger::OpRecord {
        op: Op::Split {
            source: 4,
            left: 9,
            right: 10,
        },
        phase: Phase::Committed,
        coordinator: "node-a/3".into(),
        sealed: vec![(4, 7)],
        by: "node-a/3#00ff".into(),
        extra: Vec::new(),
    };
    store
        .put(
            &cairn_store::ledger::op_path(prefix, 9),
            record.encode().into(),
        )
        .await?;

    cairn_store::ledger::delete_op(
        store,
        prefix,
        cairn_store::proof::DeleteProof::assume(Subject::Op(9)),
    )
    .await?;
    ensure!(
        cairn_store::ledger::read_op(store, prefix, 9)
            .await?
            .is_none(),
        "the ledger is still in its place"
    );

    restore_only(&bin(&stores), &Subject::Op(9)).await?;
    let back = cairn_store::ledger::read_op(store, prefix, 9)
        .await?
        .context("the ledger is not back")?;
    ensure!(
        back.record == record,
        "the restored ledger differs: {:?}",
        back.record
    );
    Ok(())
}

/// The trash does not break the deletion semantics of those paths: a second
/// delete is a no-op, and the bucket outside `trash/` is as clean as after a
/// real delete.
#[tokio::test]
async fn deletion_paths_stay_idempotent() -> Result<()> {
    let stores = memory_stores();
    let mut w = writer(&stores, 0).await?;
    write_rows(&mut w, 0, 2, b"x").await?;
    w.seal().await?;
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
            None,
        )
        .await?;
    stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
            None,
        )
        .await?;
    cairn_store::delete_owner(
        stores.main().store(),
        stores.main().prefix(),
        cairn_store::proof::DeleteProof::assume_owner(PartitionId(3)),
    )
    .await?;
    use futures::TryStreamExt;
    let left: Vec<_> = stores
        .main()
        .store()
        .list(Some(&Path::from("cairn/ranges")))
        .try_collect()
        .await?;
    ensure!(left.is_empty(), "{} objects left under ranges/", left.len());
    Ok(())
}

/// **Hold in place**: the entry writes one PUT, the object stays where it is;
/// the general sweep skips the entry even when expired; restoring only removes
/// the entry; `purge_in_place` requires a proof for exactly that subject,
/// deletes the object, then the entry.
#[tokio::test]
async fn hold_in_place_keeps_objects_until_purged_with_proof() {
    use bytes::Bytes;
    use cairn_store::{
        artifact::Owner,
        proof::DeleteProof,
        trash::{purge_expired, EntryId, Facts},
    };
    use std::time::Duration;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bin = Trash::new(Arc::clone(&store), &Path::from("p"));
    let owner = Owner {
        tenant: "t".into(),
        id: [1; 24],
        epoch: 9,
    };
    let obj = Path::from("p/artifacts/t/x/0000000000000009/aa");
    store
        .put(&obj, Bytes::from_static(b"x").into())
        .await
        .unwrap();
    let subject = Subject::Artifacts(owner.clone());
    let id = EntryId::new(1_000);
    bin.hold_in_place(&id, &DeleteProof::assume(subject.clone()), "purge")
        .await
        .unwrap();
    // A subject that cannot be held in place is refused.
    assert!(bin
        .hold_in_place(
            &EntryId::new(1_001),
            &DeleteProof::assume(Subject::Op(1)),
            "x"
        )
        .await
        .is_err());
    let e = bin.entries().await.unwrap();
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].subject, Some(subject.clone()));
    assert_eq!(e[0].objects, 0);
    assert!(store.head(&obj).await.is_ok(), "the object stays in place");
    // The general sweep: expired, still untouched.
    let round = purge_expired(
        std::slice::from_ref(&bin),
        Duration::ZERO,
        u64::MAX / 2,
        || async { Ok(Facts::default()) },
    )
    .await
    .unwrap();
    assert!(
        round.purged.is_empty() && round.kept.is_empty(),
        "{round:?}"
    );
    // A proof for the wrong subject.
    let wrong = Subject::Artifacts(Owner {
        epoch: 10,
        ..owner.clone()
    });
    assert!(bin
        .purge_in_place(&id, DeleteProof::assume(wrong), std::slice::from_ref(&obj))
        .await
        .is_err());
    assert!(store.head(&obj).await.is_ok());
    // Restore = remove the entry.
    let r = bin.restore(&id).await.unwrap();
    assert_eq!((r.restored, r.conflicts.len()), (0, 0));
    assert!(bin.entries().await.unwrap().is_empty());
    assert!(store.head(&obj).await.is_ok());
    // Hold, then delete for real.
    let id = EntryId::new(2_000);
    bin.hold_in_place(&id, &DeleteProof::assume(subject.clone()), "purge")
        .await
        .unwrap();
    let n = bin
        .purge_in_place(
            &id,
            DeleteProof::assume(subject),
            std::slice::from_ref(&obj),
        )
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert!(store.head(&obj).await.is_err());
    assert!(bin.entries().await.unwrap().is_empty());
}

/// **SM-16 for a tenant prefix**: tenant `acme` was moved to `deleting` by
/// mistake.
///
/// 1. Phase A holds `archive/acme` in place with `tenant_deleting` (keep
///    only): the objects stay, the general sweep skips the entry even when
///    expired.
/// 2. `purge_in_place` with a keep-only proof ⇒ refused, nothing deleted; with
///    a proof for another subject (`acme/stats`) ⇒ refused.
/// 3. Reversed (the book back to `active`) ⇒ `tenant_purged` cannot be built;
///    restore = remove the entry ⇒ every object intact.
/// 4. All the way (`deleting` again, `purging-ms=`, 7 days passed):
///    `tenant_purged` deletes exactly `archive/acme/…` — `archive/acme-2/…`,
///    `archive/acme.x/…` and `stats/acme/…` stay intact (prefixes by path
///    segment).
#[tokio::test]
async fn tenant_prefix_held_by_mistake_is_restored_whole() -> Result<()> {
    use bytes::Bytes;
    use cairn_store::{
        proof::DeleteProof,
        tenant_purge::{objects, PrefixKind, KEEP},
        trash::{purge_expired, EntryId, Facts},
        TenantId,
    };
    let stores = memory_stores();
    let store = Arc::clone(stores.main().store());
    let root = stores.main().prefix().clone();
    let t = bin(&stores);
    let acme = TenantId::new("acme")?;
    let day = 24 * 3_600_000u64;
    let now = 100 * day;
    let put = |p: &'static str| {
        let store = Arc::clone(&store);
        async move {
            store
                .put(
                    &Path::from(format!("cairn/{p}")),
                    Bytes::from_static(b"x").into(),
                )
                .await
        }
    };
    let mine = [
        "archive/acme/2026-11-01/aa",
        "archive/acme/2026-11-02/bb.artifacts/cc",
    ];
    let others = [
        "archive/acme-2/2026-11-01/aa",
        "archive/acme.x/2026-11-01/aa",
        "stats/acme/2026-11-01/n1",
    ];
    for p in mine.iter().chain(&others) {
        put(p).await?;
    }
    let book = |line: String| {
        let store = Arc::clone(&store);
        async move {
            store
                .put(
                    &Path::from("cairn/cluster-version"),
                    Bytes::from_static(
                        b"cairnflow-cluster-version/1\nversion=2\nmin_binary=11\ntenant_purge=on\n",
                    )
                    .into(),
                )
                .await?;
            store
                .put(
                    &Path::from("cairn/tenants"),
                    Bytes::from(format!("cairnflow-tenants/1\n{line}")).into(),
                )
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    };
    let subject = Subject::TenantPrefix {
        tenant: acme.clone(),
        kind: PrefixKind::Archive,
    };
    let all_there = || async {
        for p in mine.iter().chain(&others) {
            ensure!(
                store.head(&Path::from(format!("cairn/{p}"))).await.is_ok(),
                "{p} lost"
            );
        }
        Ok(())
    };

    // 1.
    book(format!(
        "acme state=deleting deleting-ms={}\n",
        now - 8 * day
    ))
    .await?;
    let hold = DeleteProof::tenant_deleting(&stores, &acme, PrefixKind::Archive)
        .await?
        .map_err(anyhow::Error::msg)?;
    let id = EntryId::new(now - 8 * day);
    t.hold_in_place(&id, &hold, "tenant deleting").await?;
    ensure!(
        t.entries()
            .await?
            .iter()
            .any(|e| e.subject.as_ref() == Some(&subject)),
        "hold entry for {subject} not found"
    );
    let round = purge_expired(std::slice::from_ref(&t), KEEP, now, || async {
        Ok(Facts::default())
    })
    .await?;
    ensure!(
        round.purged.is_empty(),
        "the general sweep touched a hold entry: {round:?}"
    );
    all_there().await?;

    // 2.
    let listed = objects(&store, &root, &acme, PrefixKind::Archive, 1000).await?;
    ensure!(
        t.purge_in_place(&id, hold, &listed).await.is_err(),
        "a keep-only proof could delete"
    );
    let wrong = DeleteProof::tenant_deleting(&stores, &acme, PrefixKind::Stats)
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(t.purge_in_place(&id, wrong, &listed).await.is_err());
    all_there().await?;

    // 3.
    book("acme slow-ms=5\n".into()).await?;
    let p = DeleteProof::tenant_purged(&stores, &acme, PrefixKind::Archive, KEEP, now).await?;
    ensure!(p.is_err(), "reversed, yet still provable: {p:?}");
    let r = t.restore(&id).await?;
    ensure!(
        r.conflicts.is_empty() && t.entries().await?.is_empty(),
        "{r:?}"
    );
    all_there().await?;

    // 4.
    book(format!(
        "acme state=deleting deleting-ms={} purging-ms={}\n",
        now - 8 * day,
        now - day
    ))
    .await?;
    let id = EntryId::new(now - 8 * day);
    let hold = DeleteProof::tenant_deleting(&stores, &acme, PrefixKind::Archive)
        .await?
        .map_err(anyhow::Error::msg)?;
    t.hold_in_place(&id, &hold, "tenant deleting").await?;
    let listed = objects(&store, &root, &acme, PrefixKind::Archive, 1000).await?;
    ensure!(
        listed.len() == mine.len(),
        "the LIST strayed into another tenant: {listed:?}"
    );
    let proof = DeleteProof::tenant_purged(&stores, &acme, PrefixKind::Archive, KEEP, now)
        .await?
        .map_err(anyhow::Error::msg)?;
    let n = t.purge_in_place(&id, proof, &listed).await?;
    ensure!(n == mine.len(), "deleted {n}");
    for p in mine {
        ensure!(
            store.head(&Path::from(format!("cairn/{p}"))).await.is_err(),
            "{p} still there"
        );
    }
    for p in others {
        ensure!(
            store.head(&Path::from(format!("cairn/{p}"))).await.is_ok(),
            "{p} lost"
        );
    }
    ensure!(t.entries().await?.is_empty());
    Ok(())
}

/// A store wrapping `InMemory` that records the prefix of every recursive LIST
/// and the path of every GET — to measure what the sweep reads.
#[derive(Debug)]
struct Spy {
    inner: Arc<dyn ObjectStore>,
    lists: std::sync::Mutex<Vec<String>>,
    gets: std::sync::Mutex<Vec<String>>,
    /// Number of `delete_stream` calls (each single-object `delete` goes through here too).
    delete_calls: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Display for Spy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Spy({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Spy {
    async fn put_opts(
        &self,
        location: &Path,
        payload: slatedb::object_store::PutPayload,
        opts: slatedb::object_store::PutOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: slatedb::object_store::PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn slatedb::object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: slatedb::object_store::GetOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::GetResult> {
        self.gets.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, slatedb::object_store::Result<Path>> {
        self.delete_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::ObjectMeta>,
    > {
        self.lists
            .lock()
            .unwrap()
            .push(prefix.map(|p| p.to_string()).unwrap_or_default());
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<slatedb::object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: slatedb::object_store::CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// **The sweep does not read the part of the trash that cannot be expired
/// yet**: a day directory `trash/<day>/` whose earliest possible entry (start of
/// the day) plus `keep` is still after `now` holds no expired entry — the sweep
/// does not LIST it and does not GET any of its `entry` objects.
///
/// Before the fix, each round recursively LISTed **all** of `trash/` and then
/// GOT the `entry` of **every** entry: the cost of a round grew with the whole
/// retention history (7 days), and the round runs every time a node takes the
/// runner role. Under chaos (`kill -9` every 10 s, every dead WAL log moved into
/// the trash) the trash grows by tens of thousands of objects per minute, and
/// that LIST loaded the store until the whole cluster slowed down.
///
/// Must see: the old (expired) entry is really deleted; the three entries of
/// today remain; no recursive LIST at the `trash/` root or under today; no GET
/// of today's `entry` objects.
#[tokio::test]
async fn purge_does_not_read_days_that_cannot_be_due() -> Result<()> {
    use cairn_store::trash::{purge_expired, EntryId, Facts};
    use std::time::Duration;
    let spy = Arc::new(Spy {
        inner: Arc::new(InMemory::new()),
        lists: Default::default(),
        gets: Default::default(),
        delete_calls: Default::default(),
    });
    let store: Arc<dyn ObjectStore> = spy.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("cairn"));
    let day = 86_400_000u64;
    // 2026-11-08 12:00 UTC.
    let now = 1_794_139_200_000u64;
    let keep = Duration::from_secs(7 * 24 * 3600);
    let today = cairn_store::archive::utc_date(now);
    let mut ids = Vec::new();
    for (k, at) in [now - 10 * day, now - 3_600_000, now - 60_000, now - 1_000]
        .into_iter()
        .enumerate()
    {
        let obj = Path::from(format!("cairn/owners/range-{k}"));
        store
            .put(&obj, bytes::Bytes::from_static(b"o").into())
            .await?;
        let id = EntryId::new(at);
        t.move_into(&id, &Subject::Owner(k as u32), "test", &[obj])
            .await?;
        ids.push(id);
    }
    spy.lists.lock().unwrap().clear();
    spy.gets.lock().unwrap().clear();
    let round = purge_expired(std::slice::from_ref(&t), keep, now, || async {
        Ok(Facts::default())
    })
    .await?;
    ensure!(
        round.purged == vec![ids[0].clone()] && round.kept.is_empty(),
        "only the 10-day-old entry is expired: {round:?}"
    );
    let lists = spy.lists.lock().unwrap().clone();
    let gets = spy.gets.lock().unwrap().clone();
    let today_dir = format!("cairn/trash/{today}");
    ensure!(
        !lists
            .iter()
            .any(|p| p == "cairn/trash" || p.starts_with(&today_dir)),
        "the sweep LISTed the whole trash or a day that cannot be expired yet: {lists:?}"
    );
    ensure!(
        !gets.iter().any(|p| p.starts_with(&today_dir)),
        "the sweep GOT the `entry` of a day that cannot be expired yet: {gets:?}"
    );
    let left: Vec<_> = t.entries().await?.into_iter().map(|e| e.id).collect();
    ensure!(left == ids[1..], "today's entries must remain: {left:?}");
    Ok(())
}

/// **The trash sweep is paced and capped per pass**: after an hour of chaos,
/// 46 k expired entries (~16 objects each) were deleted in one go at ~700
/// DELETE/s, a local SeaweedFS was saturated for 17 minutes, and every range
/// slowed down with it. Virtual clock (`start_paused`): measures the time the
/// round yields, not wall time.
///
/// 1. 60 expired entries, 500 objects each (30 000 objects).
/// 2. One `purge_expired` pass.
///
/// Must see: the pass stops at the cap (expired entries left, `capped`), not
/// deleting everything; and the pace is no more than 1 000 objects per virtual
/// second. Old code: all 60 entries deleted in 0 s.
#[tokio::test(start_paused = true)]
async fn purge_is_paced_and_bounded_per_round() -> Result<()> {
    use cairn_store::trash::{purge_expired, EntryId, Facts};
    use std::time::Duration;
    const ENTRIES: usize = 60;
    const OBJECTS: usize = 500;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let t = Trash::new(Arc::clone(&store), &Path::from("cairn"));
    let day = 86_400_000u64;
    let now = 1_794_139_200_000u64;
    let keep = Duration::from_secs(3600);
    for k in 0..ENTRIES {
        let mut objs = Vec::with_capacity(OBJECTS);
        for j in 0..OBJECTS {
            let obj = Path::from(format!("cairn/nodewal/log-{k}/{j:05}"));
            store
                .put(&obj, bytes::Bytes::from_static(b"o").into())
                .await?;
            objs.push(obj);
        }
        let id = EntryId::new(now - 2 * day + k as u64);
        t.move_into(&id, &Subject::Owner(1000 + k as u32), "test", &objs)
            .await?;
    }
    let t0 = tokio::time::Instant::now();
    let round = purge_expired(std::slice::from_ref(&t), keep, now, || async {
        Ok(Facts::default())
    })
    .await?;
    let took = t0.elapsed();
    let deleted = round.purged.len() * OBJECTS;
    ensure!(
        !round.purged.is_empty(),
        "the pass must delete part of the backlog: {round:?}"
    );
    ensure!(
        round.purged.len() < ENTRIES && round.capped,
        "one pass deleted all {} entries ({deleted} objects) — no per-pass cap",
        round.purged.len()
    );
    ensure!(
        took >= Duration::from_secs_f64(deleted as f64 / 1000.0),
        "deleted {deleted} objects in {took:?} — faster than 1 000 objects/s, no pacing"
    );
    let left = t.entries().await?.len();
    ensure!(
        left == ENTRIES - round.purged.len(),
        "entries not yet reached must stay intact: {left} left"
    );
    Ok(())
}

/// **The sweep does not recursively LIST a whole day directory**: under chaos
/// every dead WAL log goes into **today's** trash, ~940 k objects after an
/// hour; each round recursively LISTed all of `trash/<today>/` (every copy of
/// every entry) before deleting anything — several minutes, longer than a
/// runner process lives (`kill -9` every 10 s), so the trash only grew (one
/// run: 2 979 entries purged in an hour) and SeaweedFS slowed down with it.
///
/// 1. A day that can already be expired: an old entry (past `keep`, 3 objects)
///    and a new entry (not past `keep`, 200 objects) in the same day directory.
/// 2. One `purge_expired` pass.
///
/// Must see: the old entry is really deleted, the new one stays intact, and the
/// new entry's `entry` is not read. (The day directory is now LISTed
/// recursively as a stream and deleted from during the LIST —
/// `purge_makes_progress_within_a_term_shorter_than_one_listing`; the earlier
/// property "no recursive LIST of the day" became "no read of an entry not yet
/// expired".)
#[tokio::test(start_paused = true)]
async fn purge_lists_only_due_entries_of_a_day() -> Result<()> {
    use cairn_store::trash::{purge_expired, EntryId, Facts};
    use std::time::Duration;
    let spy = Arc::new(Spy {
        inner: Arc::new(InMemory::new()),
        lists: Default::default(),
        gets: Default::default(),
        delete_calls: Default::default(),
    });
    let store: Arc<dyn ObjectStore> = spy.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("cairn"));
    // 2026-11-08 00:00 UTC; keep 1 h; now 23:00 the same day.
    let day_start = 1_794_096_000_000u64;
    let keep = Duration::from_secs(3600);
    let now = day_start + 23 * 3_600_000;
    let day = cairn_store::archive::utc_date(day_start);
    let mut ids = Vec::new();
    for (k, (at, objects)) in [(day_start + 3_600_000, 3usize), (now - 60_000, 200)]
        .into_iter()
        .enumerate()
    {
        let mut objs = Vec::new();
        for j in 0..objects {
            let obj = Path::from(format!("cairn/nodewal/log-{k}/{j:04}"));
            store
                .put(&obj, bytes::Bytes::from_static(b"o").into())
                .await?;
            objs.push(obj);
        }
        let id = EntryId::new(at);
        t.move_into(&id, &Subject::Owner(2000 + k as u32), "test", &objs)
            .await?;
        ids.push(id);
    }
    spy.lists.lock().unwrap().clear();
    let round = purge_expired(std::slice::from_ref(&t), keep, now, || async {
        Ok(Facts::default())
    })
    .await?;
    ensure!(
        round.purged == vec![ids[0].clone()],
        "only the old entry is expired: {round:?}"
    );
    // The day directory is now LISTed recursively as a stream (deleting during
    // the LIST); the property that remains: an entry not yet expired gets no GET
    // of its `entry`.
    let fresh_entry = format!("cairn/trash/{day}/{}/entry", ids[1]);
    let gets = spy.gets.lock().unwrap().clone();
    ensure!(
        !gets.iter().any(|p| p == &fresh_entry),
        "the sweep read the `entry` of an entry not yet expired: {gets:?}"
    );
    let left: Vec<_> = t.entries().await?.into_iter().map(|e| e.id).collect();
    ensure!(left == ids[1..], "unexpired entries must remain: {left:?}");
    Ok(())
}

/// **Really deleting an entry in one batch**: the sweep deleted objects one
/// sequential DELETE at a time (~16 objects per dead WAL log, a few ms each) —
/// under chaos ~60 objects/s, below the production rate (~300 objects/s), so
/// the trash kept growing (one run: 293 k objects at minute 20). S3 has
/// DeleteObjects (up to 1 000 keys per request); object_store's `delete_stream`
/// uses it.
///
/// 1. One expired entry, 50 objects.
/// 2. One `purge_expired` pass.
///
/// Must see: the entry is really deleted (objects and `entry`), with ≤ 2
/// `delete_stream` calls (one batch of objects, one `entry`). Old code: 51.
#[tokio::test(start_paused = true)]
async fn purge_deletes_an_entry_in_one_batch() -> Result<()> {
    use cairn_store::trash::{purge_expired, EntryId, Facts};
    use std::time::Duration;
    const OBJECTS: usize = 50;
    let spy = Arc::new(Spy {
        inner: Arc::new(InMemory::new()),
        lists: Default::default(),
        gets: Default::default(),
        delete_calls: Default::default(),
    });
    let store: Arc<dyn ObjectStore> = spy.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("cairn"));
    let now = 1_794_139_200_000u64;
    let mut objs = Vec::new();
    for j in 0..OBJECTS {
        let obj = Path::from(format!("cairn/nodewal/log-x/{j:04}"));
        store
            .put(&obj, bytes::Bytes::from_static(b"o").into())
            .await?;
        objs.push(obj);
    }
    let id = EntryId::new(now - 2 * 86_400_000);
    t.move_into(&id, &Subject::Owner(3000), "test", &objs)
        .await?;
    spy.delete_calls
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let round = purge_expired(
        std::slice::from_ref(&t),
        Duration::from_secs(3600),
        now,
        || async { Ok(Facts::default()) },
    )
    .await?;
    ensure!(
        round.purged == vec![id],
        "the expired entry must be deleted: {round:?}"
    );
    ensure!(t.entries().await?.is_empty(), "the trash must be empty");
    let calls = spy.delete_calls.load(std::sync::atomic::Ordering::SeqCst);
    ensure!(
        calls <= 2,
        "{calls} delete calls for one entry of {OBJECTS} objects — deleting one object at a time"
    );
    Ok(())
}

/// A store where each object a LIST returns costs `delay` (virtual time) — a
/// large trash on a slow store: a full LIST takes long.
struct SlowList {
    inner: Arc<dyn ObjectStore>,
    delay: std::time::Duration,
}

impl std::fmt::Display for SlowList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowList({})", self.inner)
    }
}

impl std::fmt::Debug for SlowList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowList")
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowList {
    async fn put_opts(
        &self,
        location: &Path,
        payload: slatedb::object_store::PutPayload,
        opts: slatedb::object_store::PutOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: slatedb::object_store::PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn slatedb::object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: slatedb::object_store::GetOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, slatedb::object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::ObjectMeta>,
    > {
        use futures::StreamExt as _;
        let d = self.delay;
        self.inner
            .list(prefix)
            .then(move |r| async move {
                tokio::time::sleep(d).await;
                r
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<slatedb::object_store::ListResult> {
        let r = self.inner.list_with_delimiter(prefix).await?;
        let n = (r.common_prefixes.len() + r.objects.len()) as u32;
        tokio::time::sleep(self.delay * n).await;
        Ok(r)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: slatedb::object_store::CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// **A term shorter than one LIST pass still deletes**: under `kill -9` every
/// 10 s the sweep runner (range 0's leader) changes every 10–30 s; each pass
/// started by LISTing **every** entry of the day before deleting, so with ~50 k
/// entries in the day that preamble outlived the runner and the sweep stopped
/// altogether (one run: no entry purged from minute 20 to the end of the load,
/// trash at 795 k objects).
///
/// 1. 200 expired entries, 5 objects each, on a store where each object a LIST
///    returns costs 10 ms (virtual time) — a full LIST of the day is ~12 s.
/// 2. One `purge_expired` pass, cut after 1 s (the term ends).
///
/// Must see: within that 1 s some entries were really deleted (fewer than 200
/// entries left in the trash), and the remaining entries are intact (no entry
/// half-done with its `entry` gone but objects left). Old code: 200 — the pass
/// was cut while still LISTing.
#[tokio::test(start_paused = true)]
async fn purge_makes_progress_within_a_term_shorter_than_one_listing() -> Result<()> {
    use cairn_store::trash::{purge_expired, EntryId, Facts};
    use std::time::Duration;
    const ENTRIES: usize = 200;
    const OBJECTS: usize = 5;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let slow: Arc<dyn ObjectStore> = Arc::new(SlowList {
        inner: Arc::clone(&inner),
        delay: Duration::from_millis(10),
    });
    let fast = Trash::new(Arc::clone(&inner), &Path::from("cairn"));
    let t = Trash::new(Arc::clone(&slow), &Path::from("cairn"));
    let now = 1_794_139_200_000u64;
    for k in 0..ENTRIES {
        let mut objs = Vec::new();
        for j in 0..OBJECTS {
            let obj = Path::from(format!("cairn/nodewal/log-{k}/{j}"));
            inner
                .put(&obj, bytes::Bytes::from_static(b"o").into())
                .await?;
            objs.push(obj);
        }
        let id = EntryId::new(now - 2 * 3_600_000 + k as u64);
        fast.move_into(&id, &Subject::Owner(4000 + k as u32), "test", &objs)
            .await?;
    }
    let cut = tokio::time::timeout(
        Duration::from_secs(1),
        purge_expired(
            std::slice::from_ref(&t),
            Duration::from_secs(3600),
            now,
            || async { Ok(Facts::default()) },
        ),
    )
    .await;
    ensure!(cut.is_err(), "the pass must be cut midway (slow store)");
    let left = fast.entries().await?;
    ensure!(
        left.len() < ENTRIES,
        "{} entries intact after 1 s — the pass was cut before deleting anything",
        left.len()
    );
    for e in &left {
        ensure!(
            e.unreadable.is_none() || e.objects == 0,
            "a half-done entry lost its `entry` but kept objects: {e:?}"
        );
    }
    Ok(())
}

/// **Durable facts do not serve a long pass**: since deleting during the LIST,
/// a pass can run for very long (LIST on a slow store, many held or skipped
/// entries not counted against the object cap) while still using facts read
/// once at the start of the pass — contrary to the reason for the per-pass cap.
///
/// 1. 100 expired entries, 5 objects each, a store whose LIST costs 1 s per
///    object (virtual time): each entry takes a few seconds, the whole pass
///    hundreds of seconds.
/// 2. One `purge_expired` pass; count the fact reads.
///
/// Must see: the pass stops (`capped`) once the facts are older than 60 s —
/// entries left undeleted, facts read exactly once. Old code: the pass ran
/// through all 100 entries on one read.
#[tokio::test(start_paused = true)]
async fn purge_round_ends_when_its_facts_get_old() -> Result<()> {
    use cairn_store::trash::{purge_expired, EntryId, Facts};
    use std::time::Duration;
    const ENTRIES: usize = 100;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let slow: Arc<dyn ObjectStore> = Arc::new(SlowList {
        inner: Arc::clone(&inner),
        delay: Duration::from_secs(1),
    });
    let fast = Trash::new(Arc::clone(&inner), &Path::from("cairn"));
    let t = Trash::new(Arc::clone(&slow), &Path::from("cairn"));
    let now = 1_794_139_200_000u64;
    for k in 0..ENTRIES {
        let mut objs = Vec::new();
        for j in 0..5 {
            let obj = Path::from(format!("cairn/nodewal/log-{k}/{j}"));
            inner
                .put(&obj, bytes::Bytes::from_static(b"o").into())
                .await?;
            objs.push(obj);
        }
        let id = EntryId::new(now - 2 * 3_600_000 + k as u64);
        fast.move_into(&id, &Subject::Owner(5000 + k as u32), "test", &objs)
            .await?;
    }
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let round = {
        let reads = Arc::clone(&reads);
        purge_expired(
            std::slice::from_ref(&t),
            Duration::from_secs(3600),
            now,
            || async move {
                reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Facts::default())
            },
        )
        .await?
    };
    ensure!(
        reads.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "facts are read exactly once per pass"
    );
    ensure!(
        round.capped && round.purged.len() < ENTRIES,
        "the pass used the facts read at its start for all {} entries (capped {})",
        round.purged.len(),
        round.capped
    );
    Ok(())
}

/// **A store that keeps empty directories (SeaweedFS) — hold-in-place entries
/// that were removed do not stay in `held_entries`.**
///
/// 1. A store that keeps empty directories; two hold-in-place entries
///    (artifacts).
/// 2. The first entry is really deleted (`purge_in_place`), the second is
///    restored (`restore`): neither `entry` exists any more, the entry
///    directories remain.
///
/// Must see: `held_entries` is empty. Before the fix: the classification memory
/// (`Seen`) dropped only ids the LIST no longer listed — on this store the LIST
/// lists them forever, so both stayed: every artifact/value/tenant-purge sweep
/// asked the engine once, built a proof and did a GET per stale entry, and
/// `round.purged` counted phantoms, until the process restarted.
#[tokio::test]
async fn held_entries_forgets_removed_entries_on_a_store_that_keeps_empty_dirs() -> Result<()> {
    use cairn_store::{proof::DeleteProof, testkit::FaultyStore, trash::EntryId};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    faulty.keep_empty_dirs();
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("p"));
    let now = cairn_store::entropy::now_ms();
    let held = |n: u8| {
        Subject::Artifacts(cairn_store::artifact::Owner {
            tenant: "acme".into(),
            id: [n; 24],
            epoch: 1,
        })
    };
    let (purged, restored) = (EntryId::new(now - 20), EntryId::new(now - 10));
    for (id, n) in [(&purged, 1), (&restored, 2)] {
        t.hold_in_place(id, &DeleteProof::assume(held(n)), "test")
            .await?;
    }
    ensure!(t.held_entries().await?.len() == 2, "bad setup: two holds");
    t.purge_in_place(&purged, DeleteProof::assume(held(1)), &[])
        .await?;
    t.restore(&restored).await?;
    let mut dirs = 0;
    for day in store
        .list_with_delimiter(Some(&Path::from("p/trash")))
        .await?
        .common_prefixes
    {
        dirs += store
            .list_with_delimiter(Some(&day))
            .await?
            .common_prefixes
            .len();
    }
    ensure!(
        dirs == 2,
        "bad setup: the store must still list two entry directories, has {dirs}"
    );
    ensure!(
        t.held_entries().await?.is_empty(),
        "a removed entry is still in held_entries"
    );
    Ok(())
}

/// **Classifying hold-in-place entries keeps its progress when a call stops
/// midway** (a chaos run). `GET /admin/tenants/{t}/purge` on a cold node
/// classifies every trash entry (`held_entries`); the client gives up after
/// 15 s and the future is cancelled — if the `Seen` memory took results only at
/// the end of a pass, the next visit would start over and never catch up.
///
/// 1. A trash of 10 `op` entries (not held in place).
/// 2. The first call fails at the 7th `entry` GET (6 entries read).
/// 3. Call twice more; count requests.
///
/// Must see: the second call GETs only the `entry` of entries not yet read
/// (exactly 1 request more than the third — every entry known). Before the fix:
/// 10. Since reads went parallel, the first call also remembers the in-flight
/// entries that finished reading when a GET failed (it stops sending new
/// requests), so only the failed entry is left; before that (sequential) 4 were
/// left.
#[tokio::test]
async fn held_entries_keeps_progress_when_a_call_stops_midway() -> Result<()> {
    use cairn_store::{
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        trash::EntryId,
    };
    let f = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = f.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("cairn"));
    let now = cairn_store::entropy::now_ms();
    let mut ids = Vec::new();
    for k in 0..10u32 {
        let p = Path::from(format!("cairn/ops/{}", 1000 + k));
        store.put(&p, b"x".to_vec().into()).await?;
        ids.push(EntryId::new(now + u64::from(k)));
        t.move_into(
            ids.last().expect("just pushed"),
            &Subject::Op(1000 + k),
            "test",
            &[p],
        )
        .await?;
    }
    // The 7th entry's GET keeps failing (a single failure is retried within the call).
    f.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains(format!("{}/entry", ids[6].as_str())),
        Action::Fail { times: None },
    ));
    ensure!(
        t.held_entries().await.is_err(),
        "the first call must fail at the 7th entry GET"
    );
    f.clear_rules();
    let a = f.counts();
    t.held_entries().await?;
    let b = f.counts();
    t.held_entries().await?;
    let c = f.counts();
    let (second, third) = (b.since(&a).total(), c.since(&b).total());
    ensure!(
        second == third + 1,
        "the retry re-read classified entries: second {second}, third {third} requests"
    );
    Ok(())
}

/// **One failed `entry` GET failed the whole tenant purge round.** Under chaos the
/// round reads many trash entries; a single injected GET fault returned `Err` from
/// `held_entries`, so the whole round was dropped and started again on the next tick —
/// 25 of 34 failed rounds in a 40-minute run. Reading an entry is read-only: a transient
/// error is retried a few times within the call.
///
/// Must see: with the third entry GET failing once, `held_entries` answers.
#[tokio::test]
async fn held_entries_rides_out_one_failed_get() -> Result<()> {
    use cairn_store::{
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        trash::EntryId,
    };
    let f = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = f.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("cairn"));
    let now = cairn_store::entropy::now_ms();
    for k in 0..5u32 {
        let p = Path::from(format!("cairn/ops/{}", 1000 + k));
        store.put(&p, b"x".to_vec().into()).await?;
        t.move_into(
            &EntryId::new(now + u64::from(k)),
            &Subject::Op(1000 + k),
            "test",
            &[p],
        )
        .await?;
    }
    f.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("/entry".into()),
        Action::FailNth { skip: 2 },
    ));
    t.held_entries().await?;
    Ok(())
}

/// A store that keeps empty directories, two nodes (two `Trash`, each with its own `Seen` memory).
fn two_nodes() -> (Arc<cairn_store::testkit::FaultyStore>, Trash, Trash) {
    let faulty = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
    faulty.keep_empty_dirs();
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let a = Trash::new(Arc::clone(&store), &Path::from("p"));
    let b = Trash::new(store, &Path::from("p"));
    (faulty, a, b)
}

fn tenant_hold(kind: cairn_store::tenant_purge::PrefixKind) -> Subject {
    Subject::TenantPrefix {
        tenant: cairn_store::TenantId::new("acme").unwrap(),
        kind,
    }
}

/// **A held entry another node has removed is no longer in
/// `held_entries_checked`** (a chaos run: node 0's API listed 17 held entries
/// that node 2 had deleted right after `purged`).
///
/// 1. A store that keeps empty directories (SeaweedFS: 2–3 minutes after the
///    last DELETE); two nodes A, B; two prefix holds of `acme`, one artifact
///    hold.
/// 2. A classifies all three (`held_entries`).
/// 3. B really deletes one prefix hold (`purge_in_place`) and removes the
///    artifact entry (`restore`).
///
/// Must see: at A, `held_entries_checked` (prefix filter) has only the entry
/// not yet deleted and is complete (`unchecked == 0`), and HEADs again only the
/// holds the filter accepts (one HEAD per known prefix hold, no `entry` GET);
/// `held_entries` (not re-checked) may still remember the artifact entry —
/// exactly as its docs say.
#[tokio::test]
async fn held_entries_checked_forgets_holds_another_node_removed() -> Result<()> {
    use cairn_store::{
        proof::DeleteProof,
        tenant_purge::PrefixKind,
        testkit::{Op, PathClass},
        trash::EntryId,
    };
    let (faulty, a, b) = two_nodes();
    let now = cairn_store::entropy::now_ms();
    let (gone, kept, art) = (
        EntryId::new(now - 30),
        EntryId::new(now - 20),
        EntryId::new(now - 10),
    );
    let artifact = Subject::Artifacts(cairn_store::artifact::Owner {
        tenant: "acme".into(),
        id: [1; 24],
        epoch: 1,
    });
    for (id, s) in [
        (&gone, tenant_hold(PrefixKind::Metering)),
        (&kept, tenant_hold(PrefixKind::Audit)),
        (&art, artifact.clone()),
    ] {
        a.hold_in_place(id, &DeleteProof::assume(s), "test").await?;
    }
    ensure!(
        a.held_entries().await?.len() == 3,
        "bad setup: A classifies three entries"
    );
    b.purge_in_place(
        &gone,
        DeleteProof::assume(tenant_hold(PrefixKind::Metering)),
        &[],
    )
    .await?;
    b.restore(&art).await?;
    let prefix = |s: &Subject| matches!(s, Subject::TenantPrefix { .. });
    let before = faulty.counts();
    let scan = a.held_entries_checked(prefix, None).await?;
    let used = faulty.counts().since(&before);
    ensure!(
        scan.held == vec![(kept.clone(), tenant_hold(PrefixKind::Audit))] && scan.unchecked == 0,
        "A still reports the entry B deleted: {scan:?}"
    );
    ensure!(
        used.get(Op::Head, PathClass::Other) == 2 && used.get(Op::Get, PathClass::Other) == 0,
        "re-check used the wrong request count: {used:?}"
    );
    // Next time: a removed entry is remembered as removed, only the remaining hold is HEADed.
    let before = faulty.counts();
    let scan = a.held_entries_checked(prefix, None).await?;
    ensure!(scan.held.len() == 1, "{scan:?}");
    ensure!(
        faulty
            .counts()
            .since(&before)
            .get(Op::Head, PathClass::Other)
            == 1
    );
    Ok(())
}

/// **`held_entries_checked` stops at its cap, reports the unclassified part,
/// and continues next time** (a chaos run: 10/10 visits of `GET …/purge` took
/// over 15 s on a trash of 8 000–9 000 entries, cold cache).
///
/// 1. A trash of 600 ordinary (`op`) entries and one prefix hold; each `entry`
///    GET is delayed 40 ms (sequentially: 24 s per pass).
/// 2. Call with a 300 ms cap, repeatedly.
///
/// Must see: every call returns within 1 s; the first is incomplete
/// (`unchecked > 0`) and does not wrongly claim there is or is not a hold
/// outside the part read; the unclassified count decreases; within 20 calls one
/// is complete with exactly one hold — the size of the trash does not inflate a
/// single call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_entries_checked_stops_at_its_budget_and_resumes() -> Result<()> {
    use {
        cairn_store::{
            proof::DeleteProof,
            tenant_purge::PrefixKind,
            testkit::{Action, FaultyStore, LatencyDist, OpSet, PathMatch, Rule},
            trash::EntryId,
        },
        std::time::Duration,
    };
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let t = Trash::new(Arc::clone(&store), &Path::from("p"));
    let now = cairn_store::entropy::now_ms();
    for k in 0..600u32 {
        let p = Path::from(format!("p/ops/{k}"));
        store.put(&p, b"x".to_vec().into()).await?;
        t.move_into(
            &EntryId::new(now - 1_000 + u64::from(k)),
            &Subject::Op(k),
            "test",
            &[p],
        )
        .await?;
    }
    let hold = EntryId::new(now);
    t.hold_in_place(
        &hold,
        &DeleteProof::assume(tenant_hold(PrefixKind::Stats)),
        "test",
    )
    .await?;
    faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("/entry".into()),
        Action::Latency {
            dist: LatencyDist::fixed(Duration::from_millis(40)),
            seed: 1,
        },
    ));
    // Cold memory: a new `Trash` on the same store.
    let cold = Trash::new(store, &Path::from("p"));
    let mut seen = Vec::new();
    for _ in 0..20 {
        let t0 = tokio::time::Instant::now();
        let scan = cold
            .held_entries_checked(|_| true, Some(Duration::from_millis(300)))
            .await?;
        let took = t0.elapsed();
        seen.push((scan.unchecked, scan.held.len(), took));
        ensure!(
            took < Duration::from_secs(1),
            "the call took {took:?}: {seen:?}"
        );
        if scan.unchecked == 0 {
            ensure!(
                scan.held == vec![(hold.clone(), tenant_hold(PrefixKind::Stats))],
                "{scan:?}"
            );
            ensure!(
                seen[0].0 > 0,
                "the first call did not hit the cap — the test measures nothing: {seen:?}"
            );
            ensure!(
                seen.windows(2).all(|w| w[1].0 <= w[0].0),
                "the unclassified part does not decrease: {seen:?}"
            );
            return Ok(());
        }
    }
    anyhow::bail!("none of 20 calls was complete: {seen:?}")
}
