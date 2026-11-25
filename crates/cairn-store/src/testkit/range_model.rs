// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A BTreeMap store model of **one range Db** — implements [`RangeStore`] so the writer,
//! the engine and the recovery path run in RAM, deterministically, without SlateDB and
//! without an object store.
//!
//! Two tiers, like a real Db:
//! - [`RangeBucket`] is the **durable** part (object storage): the key → value image of
//!   every durable batch, `durable_seq`, the latest `writer_epoch`. It survives "deaths".
//! - [`RangeModel`] is **one open** (one process): its own memtable (the durable image at
//!   open plus the batches it wrote itself) and the non-durable WAL tail.
//!
//! Semantics per the [`RangeStore`] contract:
//! - reads see every written batch, even before it is durable; scans go in the byte order
//!   of keys, as a snapshot taken when the scan opens;
//! - a batch applies atomically, in its internal order;
//! - **durable when someone waits**: waiting for one batch to be durable (or
//!   [`RangeBucket::flush`], i.e. the WAL flush tick) flushes **the whole tail** — like a
//!   real WAL flushing the whole `current_wal`. With nobody waiting the batch stays in the
//!   tail, and [`RangeBucket::crash`] keeps a chosen **prefix** of the tail and drops the
//!   rest (the WAL is sequential);
//! - **write marks**: each batch carries a `create_ts` (ms) read from `entropy` at write
//!   time, never going back within a bucket — like the `mono_clock` SlateDB attaches to
//!   each batch. [`RangeStore::get_meta`] returns it; the mark travels with the row
//!   through flushes, deaths and reopens (CG-8);
//! - **epoch fence**: [`RangeBucket::open`] bumps the epoch and drops the previous open's
//!   non-durable tail. The old open (a zombie) can still write into its memtable — as
//!   measured on SlateDB — but cannot make anything durable: a durability wait returns
//!   `Fenced`, and from then on every operation of it returns `Fenced`.
//!
//! The contract shared with SlateDB: `cairn-store/tests/range_store.rs` (the KV layer) and
//! `cairn-engine/tests/range_model.rs` (the engine layer).

use {
    crate::{
        error::{Result, StoreError},
        writer::{Batch, Flush, ModelStatus, RangeIter, RangeStore, Row, ScanHow, StoreStatus},
    },
    bytes::Bytes,
    slatedb::{CloseReason, WriteHandle},
    std::{
        collections::{BTreeMap, VecDeque},
        ops::Bound,
        sync::{Arc, Mutex, MutexGuard},
    },
    tokio::sync::watch,
};

/// Key → (value, `create_ts` ms of the batch that wrote it).
type Map = BTreeMap<Bytes, (Bytes, i64)>;
type Ops = Vec<(Bytes, Option<Bytes>)>;

/// The durable part of a range Db. `Clone` is the same bucket.
#[derive(Clone, Default)]
pub struct RangeBucket {
    inner: Arc<Mutex<Bucket>>,
}

#[derive(Default)]
struct Bucket {
    durable: Map,
    durable_seq: u64,
    /// The latest seq issued (across every open — seqs continue across opens, like SlateDB).
    last_seq: u64,
    /// The latest `create_ts` attached — a mark that never goes back within a Db.
    last_ts: i64,
    /// `writer_epoch` of the latest open.
    epoch: u64,
    /// The latest open, still alive.
    current: Option<Arc<Mutex<Process>>>,
}

/// One open.
struct Process {
    view: Map,
    /// Batches written but not yet durable, by increasing seq, with their `create_ts`.
    tail: VecDeque<(u64, i64, Ops)>,
    status: watch::Sender<ModelStatus>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic midway in a test means the state is discarded anyway.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn apply(map: &mut Map, ts: i64, ops: &Ops) {
    for (k, v) in ops {
        match v {
            Some(v) => {
                map.insert(k.clone(), (v.clone(), ts));
            }
            None => {
                map.remove(k);
            }
        }
    }
}

fn closed_error(reason: CloseReason) -> StoreError {
    match reason {
        CloseReason::Fenced => StoreError::Fenced,
        _ => StoreError::Closed,
    }
}

impl RangeBucket {
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens the Db: epoch + 1, the previous open is fenced and its non-durable tail is
    /// lost (not flushed before the fence). The new open's read image = the durable part.
    pub fn open(&self) -> RangeModel {
        let mut b = lock(&self.inner);
        if let Some(old) = b.current.take() {
            lock(&old).tail.clear();
        }
        b.epoch += 1;
        let (status, _) = watch::channel(ModelStatus {
            durable_seq: b.durable_seq,
            close_reason: None,
        });
        let p = Arc::new(Mutex::new(Process {
            view: b.durable.clone(),
            tail: VecDeque::new(),
            status,
        }));
        b.current = Some(Arc::clone(&p));
        RangeModel {
            bucket: Arc::clone(&self.inner),
            epoch: b.epoch,
            p,
        }
    }

    /// The process of the current open **dies**: the first `keep` batches of the
    /// non-durable tail made it to the flush (a prefix — the WAL is sequential), the rest is
    /// lost. That open closes (`Closed`); the next open sees exactly the durable part.
    pub fn crash(&self, keep: usize) {
        let mut b = lock(&self.inner);
        let Some(p) = b.current.take() else {
            return;
        };
        let mut p = lock(&p);
        let tail = std::mem::take(&mut p.tail);
        for (seq, ts, ops) in tail.into_iter().take(keep) {
            apply(&mut b.durable, ts, &ops);
            b.durable_seq = seq;
        }
        let durable_seq = b.durable_seq;
        p.status.send_modify(|s| {
            s.durable_seq = durable_seq;
            s.close_reason = Some(CloseReason::Clean);
        });
    }

    /// The WAL flush tick: every batch of the current open becomes durable.
    pub fn flush(&self) {
        let mut b = lock(&self.inner);
        if let Some(p) = b.current.clone() {
            flush_all(&mut b, &mut lock(&p));
        }
    }

    /// Number of non-durable batches of the current open.
    pub fn pending(&self) -> usize {
        let b = lock(&self.inner);
        b.current.as_ref().map_or(0, |p| lock(p).tail.len())
    }

    /// The bucket's `durable_seq`.
    pub fn durable_seq(&self) -> u64 {
        lock(&self.inner).durable_seq
    }

    /// The durable image: key → value, in key order. For test invariants — reads "object
    /// storage" directly, through no open.
    pub fn durable_entries(&self) -> Vec<(Bytes, Bytes)> {
        lock(&self.inner)
            .durable
            .iter()
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect()
    }
}

/// Flushes the whole tail of `p` into the durable part. Called when `p` is the current open.
fn flush_all(b: &mut Bucket, p: &mut Process) {
    while let Some((seq, ts, ops)) = p.tail.pop_front() {
        apply(&mut b.durable, ts, &ops);
        b.durable_seq = seq;
    }
    let durable_seq = b.durable_seq;
    p.status.send_if_modified(|s| {
        let changed = s.durable_seq != durable_seq;
        s.durable_seq = durable_seq;
        changed
    });
}

/// One open of a Db on a [`RangeBucket`] — implements [`RangeStore`].
pub struct RangeModel {
    bucket: Arc<Mutex<Bucket>>,
    epoch: u64,
    p: Arc<Mutex<Process>>,
}

impl RangeModel {
    /// An error if the open has closed. A zombie that does not know it was fenced can still
    /// read/write its memtable — like SlateDB.
    fn check_open(p: &Process) -> Result<()> {
        match p.status.borrow().close_reason {
            Some(r) => Err(closed_error(r)),
            None => Ok(()),
        }
    }

    /// Waits for durability up to `seq`: flushes the whole tail if this is still the
    /// current open; a fenced open learns it here (`Fenced`) — unless `seq` was already
    /// durable.
    fn make_durable(
        bucket: &Mutex<Bucket>,
        p: &Mutex<Process>,
        epoch: u64,
        seq: u64,
    ) -> Result<()> {
        let mut b = lock(bucket);
        let mut p = lock(p);
        if b.epoch != epoch {
            if p.status.borrow().close_reason.is_none() {
                p.tail.clear();
                p.status
                    .send_modify(|s| s.close_reason = Some(CloseReason::Fenced));
            }
            return if seq <= p.status.borrow().durable_seq {
                Ok(())
            } else {
                Err(StoreError::Fenced)
            };
        }
        // Dead (`crash`) or closed: only what this open managed to make durable is durable
        // — the bucket's seq may have moved on thanks to a later open.
        if let Some(r) = p.status.borrow().close_reason {
            return if seq <= p.status.borrow().durable_seq {
                Ok(())
            } else {
                Err(closed_error(r))
            };
        }
        flush_all(&mut b, &mut p);
        Ok(())
    }
}

/// The model's scan: a snapshot taken when it opens.
pub struct ModelIter(std::vec::IntoIter<Row>);

impl RangeIter for ModelIter {
    async fn next(&mut self) -> Result<Option<Row>> {
        Ok(self.0.next())
    }
}

fn row((k, (v, _)): (&Bytes, &(Bytes, i64))) -> Row {
    Row {
        key: k.clone(),
        value: v.clone(),
    }
}

/// The smallest key greater than every key with prefix `prefix`; `None` = none.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

impl RangeStore for RangeModel {
    type Iter = ModelIter;

    fn writer_epoch(&self) -> u64 {
        self.epoch
    }

    fn status(&self) -> StoreStatus {
        StoreStatus::Model(lock(&self.p).status.subscribe())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let p = lock(&self.p);
        Self::check_open(&p)?;
        Ok(p.view.get(key).map(|(v, _)| v.clone()))
    }

    async fn get_meta(&self, key: &[u8]) -> Result<Option<(Bytes, i64)>> {
        let p = lock(&self.p);
        Self::check_open(&p)?;
        Ok(p.view.get(key).cloned())
    }

    async fn get_durable(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Self::check_open(&lock(&self.p))?;
        Ok(lock(&self.bucket).durable.get(key).map(|(v, _)| v.clone()))
    }

    async fn scan(&self, lo: Vec<u8>, hi: Vec<u8>, _how: ScanHow) -> Result<ModelIter> {
        let p = lock(&self.p);
        Self::check_open(&p)?;
        // Like SlateDB (`BytesRange::new`): an empty range is the caller's error.
        assert!(lo < hi, "Range must be non-empty");
        let rows: Vec<_> = p
            .view
            .range::<[u8], _>((Bound::Included(&lo[..]), Bound::Excluded(&hi[..])))
            .map(row)
            .collect();
        Ok(ModelIter(rows.into_iter()))
    }

    async fn scan_prefix(&self, prefix: &[u8], _how: ScanHow) -> Result<ModelIter> {
        let p = lock(&self.p);
        Self::check_open(&p)?;
        let end = prefix_end(prefix);
        let hi = match &end {
            Some(e) => Bound::Excluded(&e[..]),
            None => Bound::Unbounded,
        };
        let rows: Vec<_> = p
            .view
            .range::<[u8], _>((Bound::Included(prefix), hi))
            .map(row)
            .collect();
        Ok(ModelIter(rows.into_iter()))
    }

    async fn write(&self, batch: Batch) -> Result<WriteHandle> {
        let seq = {
            let mut b = lock(&self.bucket);
            let mut p = lock(&self.p);
            Self::check_open(&p)?;
            b.last_seq += 1;
            let seq = b.last_seq;
            // The batch's mark: the source's wall clock (virtual time in the sim), never
            // going back — like SlateDB's `mono_clock`.
            let ts = (crate::entropy::now_ms() as i64).max(b.last_ts);
            b.last_ts = ts;
            let ops = batch.into_ops();
            apply(&mut p.view, ts, &ops);
            p.tail.push_back((seq, ts, ops));
            seq
        };
        let (bucket, p, epoch) = (Arc::clone(&self.bucket), Arc::clone(&self.p), self.epoch);
        Ok(WriteHandle::new(seq, 0, move || {
            let r = Self::make_durable(&bucket, &p, epoch, seq);
            async move {
                r.map_err(|e| match e {
                    StoreError::Fenced => {
                        slatedb::Error::closed("model: fenced".into(), CloseReason::Fenced)
                    }
                    _ => slatedb::Error::closed("model: closed".into(), CloseReason::Clean),
                })
            }
        }))
    }

    async fn flush(&self, _what: Flush) -> Result<()> {
        let seq = lock(&self.bucket).last_seq;
        Self::make_durable(&self.bucket, &self.p, self.epoch, seq)
    }

    async fn close(&self) -> Result<()> {
        let mut b = lock(&self.bucket);
        let mut p = lock(&self.p);
        if b.epoch == self.epoch && p.status.borrow().close_reason.is_none() {
            flush_all(&mut b, &mut p);
            b.current = None;
        }
        p.status.send_if_modified(|s| {
            let open = s.close_reason.is_none();
            s.close_reason.get_or_insert(CloseReason::Clean);
            open
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_prefix() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(prefix_end(&[0xFF, 0xFF]), None);
    }
}
