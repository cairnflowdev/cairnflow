// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The data format marker.
//!
//! Refactor steps were allowed to **break the format** on purpose (the project was then
//! in its research phase, with no users whose compatibility had to be kept). The one cost
//! that cannot be paid is breaking it *silently*: a new binary misreading old data reports
//! no error, it just misunderstands every byte. So the bucket must carry a format marker,
//! and data without a marker is refused with instructions for fixing it.

// Tests run on real time, network and processes; only `sim_cluster` must be deterministic
// (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    cairn_store::{ObjectStores, StoreLocation},
    slatedb::{
        object_store::{memory::InMemory, ObjectStore},
        Db,
    },
    std::sync::Arc,
};

/// Data written by an old binary — with `partition-0` but no format marker — must be
/// refused, not opened as if it had the same format.
#[tokio::test]
async fn old_bucket_refused_with_clear_error() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Old binary: open `partition-0` directly, write something, close.
    let db = Db::builder("partition-0", Arc::clone(&store))
        .build()
        .await
        .expect("open an old-style db");
    db.put(b"k", b"v").await.expect("put");
    db.close().await.expect("close");

    let stores = ObjectStores::single(StoreLocation::from_store(store, "", "memory://"));
    let err = match stores.open_db(0).await {
        Ok(_) => panic!(
            "a bucket without a format marker still opened — old data read as the new format"
        ),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("format"),
        "the error must say this is about the data format, got: {msg}"
    );
}

/// An empty bucket: the first open writes the marker, later opens are normal.
#[tokio::test]
async fn fresh_bucket_gets_format_marker() {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&store),
        "",
        "memory://",
    ));

    let db = stores.open_db(0).await.expect("an empty bucket opens");
    db.close().await.expect("close");
    let marker = store.get(&Path::from("format")).await.expect("no marker");
    let text = String::from_utf8(marker.bytes().await.unwrap().to_vec()).unwrap();
    assert!(
        text.contains(&format!("version={}", cairn_store::DATA_FORMAT)),
        "the marker must record the current version, got: {text}"
    );

    // Reopen (and open another partition) on a bucket that already has a marker.
    stores
        .open_db(0)
        .await
        .expect("reopen")
        .close()
        .await
        .unwrap();
    stores
        .open_db(1)
        .await
        .expect("another partition")
        .close()
        .await
        .unwrap();
}

/// A marker of another version ⇒ refused, naming both numbers.
#[tokio::test]
async fn other_format_version_refused() {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("format"),
            PutPayload::from("cairnflow-format\nversion=999\n"),
        )
        .await
        .unwrap();
    let stores = ObjectStores::single(StoreLocation::from_store(store, "", "memory://"));
    let err = match stores.open_db(0).await {
        Ok(_) => panic!("version 999 still opened"),
        Err(e) => e,
    };
    assert!(
        matches!(
            err,
            cairn_store::StoreError::DataFormat {
                found: Some(999),
                ..
            }
        ),
        "got: {err}"
    );
}

/// The read side checks the marker but **does not write** — a reader writes nothing to the bucket.
#[tokio::test]
async fn reader_checks_but_never_writes_marker() {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&store),
        "",
        "memory://",
    ));
    let _ = stores
        .open_reader(0, std::time::Duration::from_millis(50))
        .await;
    assert!(
        store.get(&Path::from("format")).await.is_err(),
        "the reader wrote a format marker into the bucket"
    );
}

// ───────────────────────── per-object compatibility policy

/// The error of a read must say "this header is newer than the binary — upgrade the
/// binary", naming exactly the header met.
fn assert_newer(what: &str, header: &str, msg: &str) {
    assert!(
        msg.contains("upgrade this binary") && msg.contains(header),
        "{what}: a newer header must stop loudly, naming header {header:?} and asking for a binary upgrade — got: {msg}"
    );
}

/// **An old binary meeting a newer header stops loudly.** Every object with a header —
/// read strictly or leniently — changes its header when its layout changes; a binary
/// meeting a header of the same family with a larger number must say plainly "the bucket
/// is newer than me", not "the object is corrupt" (which suggests fixing it by hand,
/// overwriting) and even less carry on silently.
#[tokio::test]
async fn newer_headers_stop_loudly() {
    use {
        cairn_store::{
            archive::ArchiveRecord, CatalogDoc, ClusterConfig, OwnerRecord, RuleBook, SessionRecord,
        },
        slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload},
    };
    let e = RuleBook::decode(b"cairnflow-rules/3\nversion=1\n").unwrap_err();
    assert_newer("rules", "cairnflow-rules/3", &e.to_string());
    let e = CatalogDoc::decode(b"cairnflow-catalog v2\nversion=1\n").unwrap_err();
    assert_newer("catalog", "cairnflow-catalog v2", &e.to_string());
    let e = OwnerRecord::decode(b"cairnflow-owner/2\nterm=1\nsession=a/1\nwal_kind=slatedb\n")
        .unwrap_err();
    assert_newer("owners", "cairnflow-owner/2", &e.to_string());
    let e =
        SessionRecord::decode(b"cairnflow-session/2\nnode=a\nbeat=1\nstate=live\n").unwrap_err();
    assert_newer("sessions", "cairnflow-session/2", &e.to_string());
    let raw = zstd::encode_all(&b"cairnflow-archive/2\n"[..], 1).unwrap();
    let e = ArchiveRecord::decode(&raw).unwrap_err();
    assert_newer("archive", "cairnflow-archive/2", &e.to_string());

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("cluster.conf"),
            PutPayload::from("cairnflow-cluster/4\ntable_version=1\nnext_range_id=1\n"),
        )
        .await
        .unwrap();
    let e = ClusterConfig::open(&store, &Path::from(""), 1)
        .await
        .unwrap_err();
    assert_newer("cluster.conf", "cairnflow-cluster/4", &e.to_string());

    // A newer format marker: also "upgrade the binary", not "delete the bucket".
    let err = cairn_store::StoreError::DataFormat {
        found: Some(cairn_store::DATA_FORMAT + 1),
        expected: cairn_store::DATA_FORMAT,
    };
    assert!(err.to_string().contains("upgrade this binary"), "{err}");
}

/// **No more advice to delete the bucket** (D27). Since format 9 a bucket has a migration
/// path; an error telling the operator to delete the bucket tells them to delete customer
/// data. Formats before 9 have no migration path — but the right advice is "use the old
/// binary / a new prefix", not "delete".
#[tokio::test]
async fn format_errors_never_say_delete_the_bucket() {
    use {
        cairn_store::ClusterConfig,
        slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload},
    };
    let says_delete = |m: &str| {
        let m = m.to_lowercase();
        m.contains("delete the bucket") || m.contains("delete bucket")
    };
    for found in [None, Some(8), Some(cairn_store::DATA_FORMAT + 1)] {
        let err = cairn_store::StoreError::DataFormat {
            found,
            expected: cairn_store::DATA_FORMAT,
        };
        assert!(!says_delete(&err.to_string()), "{found:?}: {err}");
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("cluster.conf"),
            PutPayload::from("cairnflow-cluster/2\npartitions=1\n"),
        )
        .await
        .unwrap();
    let e = ClusterConfig::open(&store, &Path::from(""), 1)
        .await
        .unwrap_err();
    assert!(!says_delete(&e.to_string()), "{e}");
}
