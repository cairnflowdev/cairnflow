// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

use {
    super::open_io,
    cairn_store::{
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        ClusterConfig, ObjectStores, PartitionId, StoreError, StoreLocation,
    },
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
    std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

/// A store failing the first `times` GETs to paths containing `path`.
fn failing_gets(store: &FaultyStore, path: &str, times: u32) {
    store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains(path.into()),
        Action::Fail { times: Some(times) },
    ));
}

fn faulty() -> (Arc<FaultyStore>, Arc<dyn ObjectStore>) {
    let store = FaultyStore::wrap(Arc::new(InMemory::new()));
    let dyn_store: Arc<dyn ObjectStore> = store.clone();
    (store, dyn_store)
}

// LS-12: every read of the engine open (table reload, `cluster-version`,
// the owner record of an existing Db, the WAL kind mark) retries I/O errors in
// place — whether the layer below wraps them as `Io`, `LeaseIo` or a SlateDB error.

/// Reloading `cluster.conf`: `ClusterConfig::reload` wraps store errors as
/// `LeaseIo`.
#[tokio::test]
async fn open_io_retries_table_reload() {
    let (store, dyn_store) = faulty();
    let cluster = ClusterConfig::open(&dyn_store, &Path::default(), 1)
        .await
        .expect("create the table");
    failing_gets(&store, "cluster.conf", 2);
    open_io("range table", None, || cluster.reload())
        .await
        .expect("two failed GETs must be retried");
}

/// `cluster-version` (`migrate::ensure_known`, an `Io` error).
#[tokio::test]
async fn open_io_retries_cluster_version() {
    let (store, dyn_store) = faulty();
    failing_gets(&store, "cluster-version", 2);
    let root = Path::default();
    open_io("cluster-version", None, || {
        cairn_store::migrate::ensure_known(&dyn_store, &root, 0)
    })
    .await
    .expect("two failed GETs must be retried");
}

/// The owner record of an existing Db (`ObjectStores::adopt_unowned_db`, a
/// `LeaseIo` error from `owner.rs`) — before the take, no keeper yet.
#[tokio::test]
async fn open_io_retries_owner_record() {
    let (store, _) = faulty();
    failing_gets(&store, "owners/", 2);
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    open_io("owner record of an existing Db", None, || {
        stores.adopt_unowned_db(PartitionId(0), "n")
    })
    .await
    .expect("two failed GETs must be retried");
}

/// The WAL kind mark in the manifest (`adopt_shared_wal`): SlateDB's `Admin` retries
/// object store errors itself, what still leaks out is `Unavailable`.
#[tokio::test]
async fn open_io_retries_wal_kind_mark() {
    let (store, _) = faulty();
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    // A sealed native-WAL Db (empty WAL — the shape of the parent of an old split): the
    // mark can be switched.
    cairn_store::PartitionWriter::open(
        cairn_store::LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await.expect("open the Db"),
    )
    .await
    .expect("writer")
    .seal()
    .await
    .expect("seal");
    failing_gets(&store, "manifest", 2);
    open_io("WAL kind mark", None, || stores.adopt_shared_wal(0))
        .await
        .expect("two failed GETs must be retried");
}

/// Reading the WAL kind mark goes through SlateDB's `Admin`, which in production retries
/// object store errors **forever** (`object_store_max_retries: None`). A long store
/// failure would make the open hang here while holding the lease: the range has no leader
/// and nobody can take it, commands only get `NotLeader`.
///
/// 1. Range 0's Db has a manifest.
/// 2. Every `manifest` GET fails, forever.
/// 3. `open_io` reads the WAL kind mark.
///
/// Must see: `open_io` returns an error within its limit (the open fails, the leadership
/// loop releases the lease and takes it again with backoff), no hang.
#[tokio::test(start_paused = true)]
async fn open_io_bounds_slatedb_retrying_forever() {
    let (store, _) = faulty();
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    stores
        .open_db(0)
        .await
        .expect("open the Db")
        .close()
        .await
        .expect("close the Db");
    store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("manifest".into()),
        Action::Fail { times: None },
    ));
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(600),
        open_io("WAL kind mark", None, || stores.adopt_shared_wal(0)),
    )
    .await
    .expect("open_io must be bounded, even when SlateDB retries forever");
    assert!(r.is_err(), "{r:?}");
}

/// A non-I/O error (corrupt data, bad configuration) goes up at once, no retry.
#[tokio::test]
async fn open_io_does_not_retry_non_io_errors() {
    let calls = AtomicU32::new(0);
    let r: Result<(), StoreError> = open_io("broken", None, || {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Err(StoreError::Corrupt("broken".into())) }
    })
    .await;
    assert!(matches!(r, Err(StoreError::Corrupt(_))), "{r:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// With the lease lost, stop retrying (LS-12): carrying on with the open is pointless — the
/// range may already be in someone else's hands — so the error goes up at once and the
/// leadership loop takes again from scratch. The same failed read, with the lease still held, is retried.
#[tokio::test]
async fn open_io_stops_retrying_once_the_lease_is_lost() {
    use cairn_store::{Claim, LeaseConfig, Session};
    let (_, dyn_store) = faulty();
    let session = Session::establish(
        dyn_store,
        &Path::default(),
        LeaseConfig::new().with_node_id("open-io"),
    )
    .await
    .expect("open the session");
    let Claim::Won(_, keeper) = session.claim(PartitionId(0)).await.expect("claim") else {
        panic!("empty range but could not take it");
    };
    let calls = AtomicU32::new(0);
    let flaky = || {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if n < 2 {
                Err(StoreError::Io("503".into()))
            } else {
                Ok(())
            }
        }
    };
    open_io("lease held", Some(&keeper), flaky)
        .await
        .expect("lease held ⇒ retried");
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    session.close().await;
    assert!(
        !keeper.is_valid(),
        "closing the session must revoke the grant"
    );
    calls.store(0, Ordering::SeqCst);
    let r: Result<(), StoreError> = open_io("lease lost", Some(&keeper), || {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Err(StoreError::Io("503".into())) }
    })
    .await;
    assert!(matches!(r, Err(StoreError::Io(_))), "{r:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "lease lost but still retried"
    );
}

/// **The disk cache warm-up is bounded and skips the split window**:
///
/// 1. Range 0 has three L0 SSTs (one flush per row), written without the cache; the writer
///    stays open. **Three, not four:** each L0 flush wakes the compactor
///    (`L0Flushed`), and from `min_compaction_sources` (4) SSTs it merges L0 into one
///    sorted run — racing step 3's manifest read (empty L0 ⇒ warms
///    0 SSTs). Below the threshold, by default there is no fallback consolidation
///    (`sorted_run_consolidation_threshold` = 0), so L0 stays put.
/// 2. A new `ObjectStores` with an empty disk cache warms with a limit just big enough for one SST.
/// 3. Then warms with the default limit.
///
/// Must see: step 2 loads exactly one SST (newest first: the first SST of the list);
/// step 3 loads all three, and no GET of the WAL or the manifest goes through the cache; and
/// the `warm_allowed` gate refuses a range that left the table, carries an intent, or is
/// being split/merged on this node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warmup_is_bounded_and_skips_split_window() {
    use super::spawner::warm_allowed;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let plain = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let mut w = cairn_store::PartitionWriter::open(
        cairn_store::LeadershipGrant::single_node(PartitionId(0)),
        plain.open_db(0).await.expect("open the Db"),
    )
    .await
    .expect("writer");
    for i in 0..3u32 {
        let mut m = cairn_store::Mutations::new();
        m.append_journal(&[7; 24], i, &[0x5A; 4096]);
        w.commit(m).await.expect("put");
        if let cairn_store::Durability::Wait(p) = w.durability().expect("durable") {
            p.durable().await.expect("durable");
        }
        w.flush_memtable().await.expect("flush");
    }
    // The writer stays open until the end: `seal` merges L0 into one sorted run.
    let paths = plain.l0_sst_paths(0).await.expect("manifest");
    assert_eq!(paths.len(), 3, "{paths:?}");

    let dir = std::env::temp_dir().join(format!(
        "cairn-warm-{}-{}",
        cairn_store::entropy::process_id(),
        cairn_store::entropy::now_ms()
    ));
    let ssts_in = |dir: &std::path::Path| {
        let mut n = 0;
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                let p = e.path();
                if p.is_dir() {
                    if p.extension().is_some_and(|x| x == "sst") {
                        n += 1;
                    } else {
                        stack.push(p);
                    }
                }
            }
        }
        n
    };
    let cached = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"))
        .with_disk_cache(cairn_store::disk_cache::DiskCacheConfig::new(&dir))
        .await
        .expect("cache");
    let cache = cached.disk_cache().expect("enabled").clone();
    let first = slatedb::object_store::ObjectStoreExt::head(&store, &paths[0])
        .await
        .expect("head")
        .size as usize;
    assert!(cache.warm(paths.clone(), first).await, "bounded warm-up");
    assert_eq!(ssts_in(&dir), 1, "byte limit fits one SST ⇒ one SST");
    assert_eq!(cached.warm_disk_cache(0).await, Some(paths.len()));
    assert_eq!(ssts_in(&dir), paths.len());
    let _ = std::fs::remove_dir_all(&dir);

    assert!(warm_allowed(true, false, false));
    assert!(!warm_allowed(false, false, false), "range left the table");
    assert!(!warm_allowed(true, true, false), "range carries an intent");
    assert!(
        !warm_allowed(true, false, true),
        "being split/merged on this node"
    );
    w.seal().await.expect("seal");
}

/// `--advertise-host`: advertise the given host with the listener's **real** port
/// (port `0` ⇒ system-picked); IPv6 with `[]`; absent ⇒ exactly the bind address.
#[tokio::test]
async fn advertise_host_replaces_bound_host_but_keeps_real_ports() {
    let l = super::setup::bind_listeners("127.0.0.1", Some("10.1.2.3"), 0, 0)
        .await
        .expect("bind");
    assert_eq!(
        l.advertised,
        format!("http://10.1.2.3:{}", l.internal_addr.port())
    );
    assert_eq!(l.ingress_url, format!("http://10.1.2.3:{}", l.addr.port()));
    let l = super::setup::bind_listeners("127.0.0.1", Some("fdaa::3"), 0, 0)
        .await
        .expect("bind");
    assert_eq!(
        l.advertised,
        format!("http://[fdaa::3]:{}", l.internal_addr.port())
    );
    let l = super::setup::bind_listeners("127.0.0.1", None, 0, 0)
        .await
        .expect("bind");
    assert_eq!(l.advertised, format!("http://{}", l.internal_addr));
    assert_eq!(l.ingress_url, format!("http://{}", l.addr));
}

#[test]
fn advertise_host_rejects_what_peers_cannot_reach() {
    use super::check_advertise_host as check;
    assert_eq!(check("fdaa:0:1::3").as_deref(), Ok("fdaa:0:1::3"));
    assert_eq!(check("[fdaa::3]").as_deref(), Ok("fdaa::3"));
    assert_eq!(check("10.0.0.7").as_deref(), Ok("10.0.0.7"));
    assert_eq!(
        check("node-a.vm.cell.internal").as_deref(),
        Ok("node-a.vm.cell.internal")
    );
    for bad in [
        "", "0.0.0.0", "::", "[::]", "http://h", "h:8080", "h/x", "a b", "[h]",
    ] {
        assert!(check(bad).is_err(), "{bad:?} must be refused");
    }
}
