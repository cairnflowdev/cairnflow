// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`RangeStore`] — the store surface of **one range Db** that the writer and the engine
//! use: point reads, scans in key order, atomic batch writes, durability queries, close.
//!
//! Async but not network in the sense of D16: this is exactly the I/O that the turn path
//! does **while holding the engine** (store reads do not release the engine). Network,
//! waiting for durability, slots and locks stay above — `Pending::durable` is the only
//! place that waits for durability.
//!
//! Two implementations: [`slatedb::Db`] (production) and the BTreeMap model of
//! `testkit::range_model` (tests, the core simulation). [`PartitionWriter`] and
//! `cairn_engine::Engine` are generic over this trait with `Db` as the default, so every
//! call site writing `PartitionWriter`/`Engine` still gets the SlateDB version and
//! monomorphization produces exactly the old code — no `dyn`, no extra allocation on the
//! turn path.
//!
//! **Contract** (test: `cairn-store/tests/range_store.rs`, run on both implementations):
//! - reads see every batch whose `write` has finished, even before it is durable (SlateDB
//!   reads at the `Memory` level — `tests/durability.rs`);
//! - scans return keys in increasing byte order, within `[lo, hi)` (or a prefix), a
//!   snapshot as of when the scan was opened;
//! - a batch applies atomically and in its internal order (a write then a delete of the
//!   same key ⇒ the key is dead); a later batch's seq is greater than an earlier one's;
//! - **written ≠ durable**: the `durable_seq` of [`StoreStatus`] only passes a batch's seq
//!   once the WAL containing it has been flushed; a process death loses a **tail** of the
//!   non-durable batches (the WAL is sequential: it never loses an earlier batch while
//!   keeping a later one);
//! - **epoch fence**: opening the Db again bumps `writer_epoch`; the old writer can no
//!   longer make anything durable (its durability wait returns `Fenced`), and none of its
//!   batches that were not durable when it was fenced shows up in the new writer.

use {
    super::read::{bulk_scan, small_scan},
    crate::error::{Result, StoreError},
    bytes::Bytes,
    slatedb::{
        config::{FlushOptions, FlushType},
        CloseReason, Db, WriteBatch, WriteHandle,
    },
    tokio::sync::watch,
};

/// How to scan — the store picks the matching way to read blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanHow {
    /// A small scan over hot blocks: the block cache (`small_scan`).
    Small,
    /// A whole-table scan: large read-ahead; `cache_blocks` only on at open time
    /// (`PartitionWriter::scan_open`).
    Bulk { cache_blocks: bool },
}

/// A flush of [`RangeStore::flush`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flush {
    /// Flushes the WAL: every written batch becomes durable.
    Wal,
    /// Flushes the memtable into an L0 SST and writes the manifest.
    MemTable,
}

/// One write batch — the operations in order, `None` being a delete. Applied atomically.
///
/// Cairn's own type rather than `slatedb::WriteBatch`: the operations of a `WriteBatch`
/// cannot be read back, so the model could not apply it. The SlateDB implementation
/// converts to a `WriteBatch` at write time — the same number of copies as before (the
/// key travels with the value, the value is copied once in `Mutations::put`).
#[derive(Default, Debug)]
pub struct Batch {
    ops: Vec<(Bytes, Option<Bytes>)>,
}

impl Batch {
    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.ops.push((key, Some(value)));
    }

    pub fn delete(&mut self, key: Bytes) {
        self.ops.push((key, None));
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// The operations, in the order written into the batch.
    pub fn into_ops(self) -> Vec<(Bytes, Option<Bytes>)> {
        self.ops
    }
}

/// The state of the store under one writer — readable from outside the engine lock
/// ([`super::WriterHealth`]) and carried into [`super::Pending`].
///
/// An enum rather than an associated type of the trait: `Pending`, `Durability` and
/// `WriterHealth` travel all over cairn-server, and a type parameter there would be
/// signature churn for no gain. A release build (no `testkit`) has only one variant.
#[derive(Clone)]
pub enum StoreStatus {
    Slate(watch::Receiver<slatedb::DbStatus>),
    #[cfg(any(test, feature = "testkit"))]
    Model(watch::Receiver<ModelStatus>),
}

/// The state of the model store (`testkit::range_model`).
#[cfg(any(test, feature = "testkit"))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelStatus {
    pub durable_seq: u64,
    pub close_reason: Option<CloseReason>,
}

impl StoreStatus {
    /// Every batch with a seq not above this number is durable.
    pub fn durable_seq(&self) -> u64 {
        match self {
            Self::Slate(s) => s.borrow().durable_seq,
            #[cfg(any(test, feature = "testkit"))]
            Self::Model(s) => s.borrow().durable_seq,
        }
    }

    /// Why the store closed, if it has closed.
    pub fn close_reason(&self) -> Option<CloseReason> {
        match self {
            Self::Slate(s) => s.borrow().close_reason,
            #[cfg(any(test, feature = "testkit"))]
            Self::Model(s) => s.borrow().close_reason,
        }
    }

    /// Number of L0 views in the in-RAM manifest — the model has no L0 (0).
    pub fn l0_views(&self) -> usize {
        match self {
            Self::Slate(s) => s.borrow().current_manifest.l0().len(),
            #[cfg(any(test, feature = "testkit"))]
            Self::Model(_) => 0,
        }
    }

    /// Number of **distinct** ancestor Dbs (by path) in the `external_dbs` of the in-RAM
    /// manifest — the depth of the range's lineage. SlateDB copies the source's
    /// `external_dbs` into the child (every ancestor, not only the parent) and the GC's
    /// `detach` removes an entry once no SST points to it any more, so this number is the
    /// number of ancestors the range still stands on. The model has no lineage (`None`).
    pub fn external_depth(&self) -> Option<usize> {
        self.external_paths().map(|p| p.len())
    }

    /// The **distinct** (sorted) paths of the ancestor Dbs in the `external_dbs` of the
    /// in-RAM manifest — [`Self::external_depth`] is the number of elements. Whoever
    /// forces a child to stand on its own after a split reads it to know which ancestors
    /// it still stands on. The model has no lineage (`None`).
    pub fn external_paths(&self) -> Option<Vec<String>> {
        match self {
            Self::Slate(s) => {
                let st = s.borrow();
                let mut paths: Vec<String> = st
                    .current_manifest
                    .external_dbs()
                    .iter()
                    .map(|e| e.path.clone())
                    .collect();
                paths.sort_unstable();
                paths.dedup();
                Some(paths)
            }
            #[cfg(any(test, feature = "testkit"))]
            Self::Model(_) => None,
        }
    }

    /// Waits until the store closes, returns the reason. `None` = nobody holds the store.
    pub async fn closed(&mut self) -> Option<CloseReason> {
        match self {
            Self::Slate(s) => match s.wait_for(|s| s.close_reason.is_some()).await {
                Ok(s) => s.close_reason,
                Err(_) => None,
            },
            #[cfg(any(test, feature = "testkit"))]
            Self::Model(s) => match s.wait_for(|s| s.close_reason.is_some()).await {
                Ok(s) => s.close_reason,
                Err(_) => None,
            },
        }
    }
}

/// One row of a scan. Not `slatedb::KeyValue`: that type is `non_exhaustive`, the model
/// cannot build one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub key: Bytes,
    pub value: Bytes,
}

/// A scan in progress.
#[allow(
    async_fn_in_trait,
    reason = "writer `!Send` (INV-4): its futures need not be `Send`"
)]
pub trait RangeIter {
    async fn next(&mut self) -> Result<Option<Row>>;
}

/// The store of one range Db — see the module comment for the contract.
#[allow(
    async_fn_in_trait,
    reason = "writer `!Send` (INV-4): its futures need not be `Send`"
)]
pub trait RangeStore: 'static {
    type Iter: RangeIter;

    /// `writer_epoch` of this open — the mark of the durability floor (`floors.rs`).
    fn writer_epoch(&self) -> u64;

    /// Durability / close state, observable.
    fn status(&self) -> StoreStatus;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Like [`Self::get`], plus the `create_ts` (epoch ms) the store attached to the row
    /// at write time — one mark per batch, never going back within a Db, kept through
    /// flushes, compaction, clone, union and replay (library contract G20). The
    /// invocation timestamps are read here: 0 bytes of cairn's own.
    async fn get_meta(&self, key: &[u8]) -> Result<Option<(Bytes, i64)>>;

    /// Like [`Self::get`], but sees only **durable** data — the flusher's IR-1 tripwire
    /// (LF-1): asks the store itself, not the writer's bookkeeping.
    async fn get_durable(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scans `[lo, hi)` in key order. **`lo < hi` is mandatory** — SlateDB panics on an
    /// empty range; the call sites guard it themselves (e.g. `due_purges` when the upper
    /// mark is above `now`).
    async fn scan(&self, lo: Vec<u8>, hi: Vec<u8>, how: ScanHow) -> Result<Self::Iter>;

    /// Scans every key with prefix `prefix`, in key order.
    async fn scan_prefix(&self, prefix: &[u8], how: ScanHow) -> Result<Self::Iter>;

    /// Writes one batch. Does **not** wait for durability: the returned handle is what to wait on.
    async fn write(&self, batch: Batch) -> Result<WriteHandle>;

    async fn flush(&self, what: Flush) -> Result<()>;

    /// Wakes this process's compaction coordinator so it re-reads the compaction state at
    /// once — a compaction just requested through `Admin` (`submit_full_compaction`) need
    /// not wait for the poll tick (1 h, `DEFAULT_POLLS.compactor`). No I/O, no wait; a
    /// store without a compactor does nothing (`irreversible.md` 0.3 G24).
    fn wake_compactor(&self) {}

    /// Closes the store. Every later call of any other operation returns an error.
    async fn close(&self) -> Result<()>;
}

/// SlateDB's iterator, errors converted into [`StoreError`].
pub struct SlateIter(slatedb::DbIterator);

impl RangeIter for SlateIter {
    async fn next(&mut self) -> Result<Option<Row>> {
        let kv = self.0.next().await.map_err(StoreError::from_slate)?;
        Ok(kv.map(|kv| Row {
            key: kv.key,
            value: kv.value,
        }))
    }
}

fn scan_options(how: ScanHow) -> slatedb::config::ScanOptions {
    match how {
        ScanHow::Small => small_scan(),
        ScanHow::Bulk { cache_blocks } => bulk_scan(cache_blocks),
    }
}

impl RangeStore for Db {
    type Iter = SlateIter;

    fn writer_epoch(&self) -> u64 {
        self.subscribe().borrow().current_manifest.writer_epoch()
    }

    fn status(&self) -> StoreStatus {
        StoreStatus::Slate(self.subscribe())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Db::get(self, key).await.map_err(StoreError::from_slate)
    }

    async fn get_meta(&self, key: &[u8]) -> Result<Option<(Bytes, i64)>> {
        Ok(Db::get_key_value(self, key)
            .await
            .map_err(StoreError::from_slate)?
            .map(|kv| (kv.value, kv.create_ts)))
    }

    /// SlateDB's `Remote` filter: `seq ≤ last_remote_persisted_seq`. With the shared WAL,
    /// that mark is the `durable_seq` pushed up by `SharedWriterInit`
    /// (`tests/shared_wal.rs::remote_read_sees_exactly_the_durable_prefix_with_a_shared_wal`, G19).
    async fn get_durable(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let opts = slatedb::config::ReadOptions::new()
            .with_durability_filter(slatedb::config::DurabilityLevel::Remote);
        Db::get_with_options(self, key, &opts)
            .await
            .map_err(StoreError::from_slate)
    }

    async fn scan(&self, lo: Vec<u8>, hi: Vec<u8>, how: ScanHow) -> Result<SlateIter> {
        self.scan_with_options(lo..hi, &scan_options(how))
            .await
            .map(SlateIter)
            .map_err(StoreError::from_slate)
    }

    async fn scan_prefix(&self, prefix: &[u8], how: ScanHow) -> Result<SlateIter> {
        self.scan_prefix_with_options(prefix, .., &scan_options(how))
            .await
            .map(SlateIter)
            .map_err(StoreError::from_slate)
    }

    async fn write(&self, batch: Batch) -> Result<WriteHandle> {
        let mut b = WriteBatch::new();
        for (key, value) in batch.ops {
            match value {
                Some(value) => b.put_bytes(key, value),
                None => b.delete(key),
            }
        }
        Db::write(self, b).await.map_err(StoreError::from_slate)
    }

    async fn flush(&self, what: Flush) -> Result<()> {
        let flush_type = match what {
            Flush::Wal => FlushType::Wal,
            Flush::MemTable => FlushType::MemTable,
        };
        self.flush_with_options(FlushOptions { flush_type })
            .await
            .map_err(StoreError::from_slate)
    }

    fn wake_compactor(&self) {
        Db::wake_compactor(self)
    }

    async fn close(&self) -> Result<()> {
        Db::close(self).await.map_err(StoreError::from_slate)
    }
}
