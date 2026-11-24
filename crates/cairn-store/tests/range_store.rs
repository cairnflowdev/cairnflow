// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The contract of [`RangeStore`], the KV layer — **every test runs on both
//! implementations**: `slatedb::Db` on an in-memory object store, and the
//! BTreeMap model `testkit::RangeModel`. The two disagreeing on a test is a bug
//! in one of them: the model is trustworthy only while it gives the same answers
//! as the real Db.
//!
//! The key semantics here are what the writer and the engine rely on: byte
//! order of keys, atomic in-order batches, reads that see what is not yet
//! durable, "written ≠ durable", a crash loses an undurable tail, and the
//! `writer_epoch` fence.

// Tests run on real time; only `sim_cluster` must be deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    bytes::Bytes,
    cairn_store::{
        testkit::{RangeBucket, RangeModel},
        Batch, Flush, RangeIter, RangeStore, ScanHow, StoreError,
    },
    slatedb::{config::Settings, object_store::memory::InMemory, CloseReason, Db},
    std::sync::Arc,
};

/// How one implementation opens / kills a range Db.
trait Harness {
    type S: RangeStore;
    /// Open the Db (a new open fences the previous one).
    async fn open(&mut self) -> Self::S;
    /// The process holding `s` dies: nothing more is flushed.
    fn crash(&mut self, s: Self::S);
}

/// SlateDB on an in-memory object store, with **no** automatic WAL flush: data
/// becomes durable only on a flush (`flush`) or a close — so a "crash" loses
/// exactly the unflushed part, deterministically.
struct Slate(Arc<InMemory>);

impl Harness for Slate {
    type S = Db;
    async fn open(&mut self) -> Db {
        Db::builder("range", self.0.clone())
            .with_settings(Settings {
                flush_interval: None,
                ..Default::default()
            })
            .build()
            .await
            .expect("open db")
    }
    fn crash(&mut self, s: Db) {
        // No `close`: no more flushes. The Db's background tasks stop with it.
        drop(s);
    }
}

struct Model(RangeBucket);

impl Harness for Model {
    type S = RangeModel;
    async fn open(&mut self) -> RangeModel {
        self.0.open()
    }
    fn crash(&mut self, s: RangeModel) {
        self.0.crash(0);
        drop(s);
    }
}

fn b(s: &[u8]) -> Bytes {
    Bytes::copy_from_slice(s)
}

fn batch(ops: &[(&[u8], Option<&[u8]>)]) -> Batch {
    let mut out = Batch::default();
    for (k, v) in ops {
        match v {
            Some(v) => out.put(b(k), b(v)),
            None => out.delete(b(k)),
        }
    }
    out
}

async fn put<S: RangeStore>(s: &S, k: &[u8], v: &[u8]) -> u64 {
    s.write(batch(&[(k, Some(v))])).await.expect("put").seqnum()
}

async fn collect<I: RangeIter>(mut it: I) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    while let Some(r) = it.next().await.expect("scan") {
        out.push((r.key.to_vec(), r.value.to_vec()));
    }
    out
}

async fn keys<S: RangeStore>(s: &S, lo: &[u8], hi: &[u8]) -> Vec<Vec<u8>> {
    let it = s
        .scan(lo.to_vec(), hi.to_vec(), ScanHow::Small)
        .await
        .expect("scan");
    collect(it).await.into_iter().map(|(k, _)| k).collect()
}

async fn prefix_keys<S: RangeStore>(s: &S, p: &[u8]) -> Vec<Vec<u8>> {
    let it = s.scan_prefix(p, ScanHow::Small).await.expect("scan");
    collect(it).await.into_iter().map(|(k, _)| k).collect()
}

// ── the contracts ───────────────────────────────────────────────────────────

/// A read sees a batch just written even though it is not durable yet (SlateDB reads at `Memory` level).
async fn reads_see_unflushed_writes<H: Harness>(mut h: H) {
    let s = h.open().await;
    let seq = put(&s, b"k", b"v1").await;
    assert!(s.status().durable_seq() < seq, "durable right after write");
    assert_eq!(s.get(b"k").await.unwrap(), Some(b(b"v1")));
    put(&s, b"k", b"v2").await;
    assert_eq!(s.get(b"k").await.unwrap(), Some(b(b"v2")));
    s.write(batch(&[(b"k", None)])).await.unwrap();
    assert_eq!(s.get(b"k").await.unwrap(), None);
    assert_eq!(s.get(b"missing").await.unwrap(), None);
}

/// A batch applies atomically, in batch order: write then delete the same key
/// and the key is dead, delete then write and it lives. Seq increases per batch.
async fn batch_applies_in_order<H: Harness>(mut h: H) {
    let s = h.open().await;
    let a = s
        .write(batch(&[
            (b"a", Some(b"1")),
            (b"b", Some(b"2")),
            (b"a", None),
            (b"c", None),
            (b"c", Some(b"3")),
        ]))
        .await
        .unwrap()
        .seqnum();
    assert_eq!(s.get(b"a").await.unwrap(), None);
    assert_eq!(s.get(b"b").await.unwrap(), Some(b(b"2")));
    assert_eq!(s.get(b"c").await.unwrap(), Some(b(b"3")));
    let next = put(&s, b"d", b"4").await;
    assert!(next > a, "seq did not increase: {a} then {next}");
}

/// Scans follow the byte order of keys, `[lo, hi)`; a prefix scan takes exactly
/// the keys with that prefix (including the prefix itself), even a prefix
/// ending in `0xFF`.
async fn scans_follow_byte_order<H: Harness>(mut h: H) {
    let s = h.open().await;
    let all: [&[u8]; 9] = [
        &[2],
        &[1, 0xFF],
        &[1],
        &[0, 0xFF, 0xFF],
        &[1, 0],
        &[1, 0xFF, 0],
        &[1, 0xFF, 0xFF],
        &[0xFF],
        &[0xFF, 0xFF, 1],
    ];
    // Two batches, written in an order different from the key order.
    s.write(batch(
        &all[..4]
            .iter()
            .map(|k| (*k, Some(&b"x"[..])))
            .collect::<Vec<_>>(),
    ))
    .await
    .unwrap();
    s.write(batch(
        &all[4..]
            .iter()
            .map(|k| (*k, Some(&b"y"[..])))
            .collect::<Vec<_>>(),
    ))
    .await
    .unwrap();
    let mut sorted: Vec<Vec<u8>> = all.iter().map(|k| k.to_vec()).collect();
    sorted.sort();
    assert_eq!(keys(&s, &[0], &[0xFF, 0xFF, 0xFF]).await, sorted);
    assert_eq!(
        keys(&s, &[1], &[1, 0xFF]).await,
        vec![vec![1], vec![1, 0]],
        "hi must be exclusive"
    );
    assert_eq!(
        prefix_keys(&s, &[1]).await,
        vec![
            vec![1],
            vec![1, 0],
            vec![1, 0xFF],
            vec![1, 0xFF, 0],
            vec![1, 0xFF, 0xFF]
        ]
    );
    assert_eq!(
        prefix_keys(&s, &[1, 0xFF]).await,
        vec![vec![1, 0xFF], vec![1, 0xFF, 0], vec![1, 0xFF, 0xFF]]
    );
    assert_eq!(
        prefix_keys(&s, &[0xFF]).await,
        vec![vec![0xFF], vec![0xFF, 0xFF, 1]]
    );
    // Values travel with their keys.
    let it = s
        .scan_prefix(&[2], ScanHow::Bulk { cache_blocks: true })
        .await
        .unwrap();
    assert_eq!(collect(it).await, vec![(vec![2], b"x".to_vec())]);
}

/// A scan is a snapshot taken when it opens: a batch written after the scan
/// opened does not show up in it. (The writer does not write during a scan —
/// a scan borrows `&self`, a commit borrows `&mut self` — but the model must
/// match the Db exactly here.)
async fn scan_is_a_snapshot<H: Harness>(mut h: H) {
    let s = h.open().await;
    s.write(batch(&[(b"a", Some(b"1")), (b"c", Some(b"1"))]))
        .await
        .unwrap();
    let it = s
        .scan(b"a".to_vec(), b"z".to_vec(), ScanHow::Small)
        .await
        .unwrap();
    s.write(batch(&[(b"b", Some(b"2")), (b"c", None)]))
        .await
        .unwrap();
    assert_eq!(
        collect(it).await,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"1".to_vec())
        ]
    );
}

/// Written ≠ durable: `durable_seq` passes a batch's seq only after a flush;
/// waiting for durability on the handle returns `Ok` and afterwards
/// `durable_seq` is not below that seq.
async fn durability_follows_flush<H: Harness>(mut h: H) {
    let s = h.open().await;
    let status = s.status();
    let h1 = s.write(batch(&[(b"a", Some(b"1"))])).await.unwrap();
    assert!(status.durable_seq() < h1.seqnum());
    s.flush(Flush::Wal).await.unwrap();
    assert!(status.durable_seq() >= h1.seqnum(), "flushed, not durable");
    h1.await_durable().await.expect("durable: wait is instant");
    let h2 = s.write(batch(&[(b"b", Some(b"2"))])).await.unwrap();
    assert!(status.durable_seq() < h2.seqnum());
    assert!(status.close_reason().is_none());
}

/// A crash loses exactly the undurable part — a **tail**: flushed batches stay
/// intact, batches after the last flush are all lost. The next open has a
/// larger `writer_epoch`.
async fn crash_loses_only_unflushed_tail<H: Harness>(mut h: H) {
    let s = h.open().await;
    let e1 = s.writer_epoch();
    put(&s, b"a", b"1").await;
    put(&s, b"b", b"1").await;
    s.flush(Flush::Wal).await.unwrap();
    put(&s, b"b", b"2").await;
    put(&s, b"c", b"2").await;
    s.write(batch(&[(b"a", None)])).await.unwrap();
    h.crash(s);

    let s = h.open().await;
    assert!(s.writer_epoch() > e1, "next open did not bump the epoch");
    assert_eq!(s.get(b"a").await.unwrap(), Some(b(b"1")));
    assert_eq!(s.get(b"b").await.unwrap(), Some(b(b"1")));
    assert_eq!(s.get(b"c").await.unwrap(), None);
    // Seq continues, never goes back.
    let seq = put(&s, b"d", b"3").await;
    assert!(seq > s.status().durable_seq());
}

/// The epoch fence: the second open fences the first. The zombie can still
/// write into its memtable, but cannot make it durable: waiting for durability
/// returns `Fenced`, and the new open never sees that batch.
async fn second_open_fences_the_first<H: Harness>(mut h: H) {
    let old = h.open().await;
    put(&old, b"a", b"old").await;
    old.flush(Flush::Wal).await.unwrap();
    let new = h.open().await;
    assert!(new.writer_epoch() > old.writer_epoch());
    assert_eq!(new.get(b"a").await.unwrap(), Some(b(b"old")));

    let zombie = old.write(batch(&[(b"z", Some(b"zombie"))])).await;
    if let Ok(handle) = zombie {
        let r = handle.await_durable().await;
        assert!(r.is_err(), "zombie made a batch durable after its fence");
    }
    let r = old.flush(Flush::Wal).await;
    assert!(
        matches!(r, Err(StoreError::Fenced) | Err(StoreError::Closed)),
        "the zombie flushed the WAL: {r:?}"
    );
    assert_eq!(old.status().close_reason(), Some(CloseReason::Fenced));

    // The new open sees nothing of the zombie, and still writes durably as usual.
    assert_eq!(new.get(b"z").await.unwrap(), None);
    let h2 = new.write(batch(&[(b"n", Some(b"new"))])).await.unwrap();
    new.flush(Flush::Wal).await.unwrap();
    h2.await_durable().await.unwrap();
    drop(old);
    let s = h.open().await;
    assert_eq!(s.get(b"z").await.unwrap(), None);
    assert_eq!(s.get(b"n").await.unwrap(), Some(b(b"new")));
}

/// Close: what was written becomes durable, the store reports `Clean`, later calls fail.
async fn close_flushes_and_ends<H: Harness>(mut h: H) {
    let s = h.open().await;
    put(&s, b"a", b"1").await;
    let mut status = s.status();
    s.close().await.unwrap();
    assert_eq!(status.close_reason(), Some(CloseReason::Clean));
    assert_eq!(status.closed().await, Some(CloseReason::Clean));
    assert!(s.get(b"a").await.is_err(), "read after close");
    drop(s);
    let s = h.open().await;
    assert_eq!(s.get(b"a").await.unwrap(), Some(b(b"1")));
}

/// A **durable**-level read (`get_durable`, tripwire IR-1) sees exactly what
/// was flushed: an unflushed batch is seen by `get` but not by `get_durable`;
/// after a flush both see it.
async fn durable_reads_see_only_flushed_writes<H: Harness>(mut h: H) {
    let s = h.open().await;
    put(&s, b"a", b"1").await;
    s.flush(Flush::Wal).await.unwrap();
    put(&s, b"a", b"2").await;
    put(&s, b"b", b"2").await;
    assert_eq!(s.get(b"a").await.unwrap(), Some(b(b"2")));
    assert_eq!(s.get_durable(b"a").await.unwrap(), Some(b(b"1")));
    assert_eq!(s.get_durable(b"b").await.unwrap(), None);
    s.flush(Flush::Wal).await.unwrap();
    assert_eq!(s.get_durable(b"a").await.unwrap(), Some(b(b"2")));
    assert_eq!(s.get_durable(b"b").await.unwrap(), Some(b(b"2")));
}

/// **A send ticket covers exactly the commits with send rows** (LF-1), via
/// `PartitionWriter`: a commit with a durable outbox row + a commit without
/// send rows not yet durable ⇒ the send ticket is granted at once, the full
/// ticket must wait; one more commit with a send row not yet durable ⇒ the send
/// ticket must wait, and its mark is that commit. A crash keeps a prefix: send
/// rows that already had a ticket survive the reopen.
async fn send_ticket_covers_exactly_the_commits_with_sends<H: Harness>(mut h: H) {
    use cairn_store::{
        keys, Durability, LeadershipGrant, Mutations, PartitionId, PartitionWriter, SendDurability,
    };
    let s = h.open().await;
    let status = s.status();
    let mut w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), s)
        .await
        .unwrap();
    let id = [7u8; 24];
    let pk = cairn_store::partition_key_of(&id);
    let mut m = Mutations::new();
    m.push_outbox(pk, 1, &id, 1, b"call");
    w.commit(m).await.unwrap();
    let sent = w
        .latest_send_seq()
        .expect("a commit with an outbox row sets the send-ticket mark");
    assert!(matches!(
        w.send_durability().unwrap(),
        SendDurability::Wait(_)
    ));
    // The harness's SlateDB does not flush automatically: flush by hand.
    w.store().flush(Flush::Wal).await.unwrap();
    w.wait_durable().await.unwrap();
    let mut m = Mutations::new();
    m.set_invocation_status(&id, b"x");
    w.commit(m).await.unwrap();
    assert_eq!(
        w.latest_send_seq(),
        Some(sent),
        "a commit without send rows does not move the mark"
    );
    assert!(
        matches!(w.send_durability().unwrap(), SendDurability::Ready(_)),
        "the send ticket must be granted at once: every send row is durable"
    );
    assert!(matches!(w.durability().unwrap(), Durability::Wait(_)));
    let mut m = Mutations::new();
    m.push_signal_outbox(2, &id, 4, b"tin");
    w.commit(m).await.unwrap();
    assert!(w.latest_send_seq() > Some(sent));
    assert!(
        matches!(w.send_durability().unwrap(), SendDurability::Wait(_)),
        "a signal row not yet durable, yet the send ticket was granted at once"
    );
    // Deleting an outbox row (after the ack) is not a send row.
    let mut m = Mutations::new();
    m.pop_outbox(pk, 1, &id, 1);
    let before = w.latest_send_seq();
    w.commit(m).await.unwrap();
    assert_eq!(
        w.latest_send_seq(),
        before,
        "deleting a send row does not move the send-ticket mark"
    );
    drop(status);
    // Crash: the Db is dropped without `close` (no further flush); the new open fences the old one.
    drop(w);
    let s = h.open().await;
    assert!(
        s.get(&keys::outbox(pk, &id, 1)).await.unwrap().is_some(),
        "a send row that had a ticket was lost after the crash"
    );
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_millis() as i64
}

/// **`get_meta` (CG-8):** the value comes with the `create_ts` (ms) of the
/// batch that wrote the row — within the real time around the write; a later
/// overwrite ⇒ a larger mark; another row in the same batch has the same mark;
/// the mark travels with the row through a flush, a crash, a reopen; delete ⇒
/// `None`.
async fn get_meta_carries_create_ts<H: Harness>(mut h: H) {
    let s = h.open().await;
    assert_eq!(s.get_meta(b"a").await.unwrap(), None);
    let before = now_ms();
    s.write(batch(&[(b"a", Some(b"1")), (b"b", Some(b"2"))]))
        .await
        .unwrap();
    let after = now_ms();
    let (v, ta) = s.get_meta(b"a").await.unwrap().expect("a is present");
    assert_eq!(v, b(b"1"));
    assert!(
        before - 5 <= ta && ta <= after + 5,
        "create_ts {ta} outside [{before}, {after}]"
    );
    let (_, tb) = s.get_meta(b"b").await.unwrap().expect("b is present");
    assert_eq!(ta, tb, "one batch, one mark");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    put(&s, b"a", b"3").await;
    let (v, ta2) = s.get_meta(b"a").await.unwrap().expect("a is present");
    assert_eq!(v, b(b"3"));
    assert!(ta2 > ta, "an overwrite kept the old mark: {ta} → {ta2}");
    s.flush(Flush::Wal).await.unwrap();
    h.crash(s);
    let s = h.open().await;
    assert_eq!(s.get_meta(b"a").await.unwrap(), Some((b(b"3"), ta2)));
    assert_eq!(s.get_meta(b"b").await.unwrap(), Some((b(b"2"), tb)));
    s.write(batch(&[(b"a", None)])).await.unwrap();
    assert_eq!(s.get_meta(b"a").await.unwrap(), None);
}

macro_rules! contract {
    ($($name:ident),* $(,)?) => {
        mod slate {
            $(
                #[tokio::test]
                async fn $name() {
                    super::$name(super::Slate(std::sync::Arc::new(
                        slatedb::object_store::memory::InMemory::new(),
                    )))
                    .await
                }
            )*
        }
        mod model {
            $(
                #[tokio::test]
                async fn $name() {
                    super::$name(super::Model(cairn_store::testkit::RangeBucket::new())).await
                }
            )*
        }
    };
}

contract!(
    get_meta_carries_create_ts,
    durable_reads_see_only_flushed_writes,
    send_ticket_covers_exactly_the_commits_with_sends,
    reads_see_unflushed_writes,
    batch_applies_in_order,
    scans_follow_byte_order,
    scan_is_a_snapshot,
    durability_follows_flush,
    crash_loses_only_unflushed_tail,
    second_open_fences_the_first,
    close_flushes_and_ends,
);
