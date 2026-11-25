// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The read side. The very opposite of the writer: `Clone` freely, works on any node.
//!
//! At first it was only used for introspection on the same node. Now it is the follower
//! read — and the hard part (overlay ↔ manifest) already exists in SlateDB, as an early
//! reader spike confirmed, so we do not have to write it.

use {
    crate::{
        error::{Result, StoreError},
        keyspace::{self, InvocationId},
        partition_table::PartitionKey,
    },
    slatedb::DbReader,
    std::sync::Arc,
};

/// Reads a partition's state. `Clone` is safe — a reader writes nothing.
#[derive(Clone)]
pub struct PartitionReader {
    db: Arc<DbReader>,
    /// Shared WAL: the WAL source of `db`, to ask about the unexposed tail
    /// ([`Self::unexposed_tail_age`]). `None` = a native-WAL reader.
    tail: Option<crate::nodewal::SharedWalReader>,
}

/// The two marks of a follower reader, read from SlateDB (the reader spike confirmed that
/// both are exposed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Freshness {
    /// `D'ᵢ` — how far the manifest this node holds covers.
    pub manifest_seq: u64,
    /// `T` — how far the reader has replayed the WAL. This is the real "freshness" mark.
    pub durable_seq: u64,
}

impl Freshness {
    /// The uncompacted WAL span the reader has to keep in memory.
    pub fn lag(&self) -> u64 {
        self.durable_seq.saturating_sub(self.manifest_seq)
    }
}

/// Watch for a reader that has **stalled without knowing it**.
///
/// `DbReader` treats a deleted WAL as the *end of the stream*, not as an error, so a
/// reader that has fallen too far behind silently stops advancing and keeps returning old
/// data. There is no error signal to catch — the only thing observable is the *shape* of
/// freshness over time.
///
/// Invariant when healthy: `durable_seq ≥ manifest_seq` — the reader must replay the WAL
/// *further* than what the manifest covers, since the manifest only covers the compacted
/// part. So `durable_seq < manifest_seq` means the manifest advances (the leader keeps
/// writing and compacting) while the reader does not keep up with the WAL — exactly that
/// signature. A single sample may be transient (in the middle of a refresh round), so the
/// conclusion is only drawn after [`StallDetector::GRACE`] samples in a row.
///
/// Once detected, **refuse to serve** rather than keep serving: better a 503 than a wrong
/// answer.
#[derive(Debug, Default)]
pub struct StallDetector {
    behind_streak: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderHealth {
    Healthy,
    /// The reader has lagged behind the manifest for more than [`StallDetector::GRACE`]
    /// samples in a row.
    Wedged {
        behind_for_samples: u32,
    },
}

impl StallDetector {
    /// Number of consecutive `durable < manifest` samples before concluding.
    ///
    /// 3 rather than 1: right after the reader picks up a new manifest, `durable_seq` is
    /// updated in a separate step of the refresh round, so one off sample is measurement
    /// noise, not a disease.
    pub const GRACE: u32 = 3;

    pub fn observe(&mut self, f: Freshness) -> ReaderHealth {
        if f.durable_seq < f.manifest_seq {
            self.behind_streak += 1;
        } else {
            self.behind_streak = 0;
        }
        if self.behind_streak >= Self::GRACE {
            ReaderHealth::Wedged {
                behind_for_samples: self.behind_streak,
            }
        } else {
            ReaderHealth::Healthy
        }
    }
}

impl PartitionReader {
    pub fn new(db: DbReader) -> Self {
        Self {
            db: Arc::new(db),
            tail: None,
        }
    }

    /// A reader reading the shared WAL through `tail` (the node's tailers).
    pub(crate) fn with_tail(db: DbReader, tail: crate::nodewal::SharedWalReader) -> Self {
        Self {
            db: Arc::new(db),
            tail: Some(tail),
        }
    }

    /// How long the range has had a frame in a shared WAL object the tailer **has read but
    /// not exposed** (C5: no `acked_upto` header covers it yet); `None` = none, or a
    /// native-WAL reader. Longer than a few poll ticks means the owner's log has gone quiet
    /// after its last object: the state the reader sees **may be stale** until the forced
    /// flush by frame age (I4) — see [`crate::nodewal::SharedWalReader::unexposed_since`].
    pub fn unexposed_tail_age(&self) -> Option<std::time::Duration> {
        self.tail.as_ref()?.unexposed_since().map(|at| at.elapsed())
    }

    /// Current freshness.
    ///
    /// **`durable_seq` standing still must be watched**: `DbReader` treats a deleted WAL as
    /// the *end of the stream*, not as an error, so a reader that has fallen too far behind
    /// silently stands still and keeps returning old data without any report. If nobody
    /// calls this function periodically, that failure never shows.
    pub fn freshness(&self) -> Freshness {
        Freshness {
            manifest_seq: self.db.manifest().last_l0_seq(),
            durable_seq: self.db.status().durable_seq,
        }
    }

    /// Follows freshness by events, not by polling.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<slatedb::DbStatus> {
        self.db.subscribe()
    }

    pub async fn invocation_status(&self, id: &InvocationId) -> Result<Option<bytes::Bytes>> {
        self.get(&keyspace::invocation_status(id)).await
    }

    pub async fn state(
        &self,
        owner_pk: PartitionKey,
        tenant: &crate::scope::TenantId,
        scope: Option<&crate::scope::Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
    ) -> Result<Option<bytes::Bytes>> {
        // See `PartitionWriter::state`: a rolled-over key points into another object's slot.
        let key = keyspace::state(owner_pk, tenant, scope, service, object_key, user_key);
        StoreError::key("state", &key)?;
        self.get(&key).await
    }

    /// The journal of an invocation, **in command index order** — exactly the order the
    /// protocol's REPLAY phase needs.
    pub async fn journal(&self, id: &InvocationId) -> Result<Vec<(u32, bytes::Bytes)>> {
        let prefix = keyspace::journal_prefix(id);
        let mut iter = self
            .db
            .scan_prefix(&prefix, ..)
            .await
            .map_err(StoreError::from_slate)?;

        let mut out = Vec::new();
        while let Some(kv) = iter.next().await.map_err(StoreError::from_slate)? {
            let Some(index) = keyspace::parse_journal_index(&kv.key) else {
                continue;
            };
            out.push((index, kv.value));
        }
        Ok(out)
    }

    /// One journal page — see [`crate::PartitionWriter::journal_page`].
    pub async fn journal_page(
        &self,
        id: &InvocationId,
        from: u32,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(Vec<(u32, bytes::Bytes)>, Option<u32>)> {
        let lo = keyspace::journal_entry(id, from);
        let hi = crate::query_index::prefix_end(&keyspace::journal_prefix(id));
        let mut iter = self.db.scan(lo..hi).await.map_err(StoreError::from_slate)?;
        let mut page = keyspace::JournalCut::new(max_entries, max_bytes);
        while let Some(kv) = iter.next().await.map_err(StoreError::from_slate)? {
            if let Some(index) = keyspace::parse_journal_index(&kv.key) {
                if page.push(index, kv.value) {
                    break;
                }
            }
        }
        Ok(page.finish())
    }

    /// Timers due at `now_ms`, in due order: `(due, id, completion)`.
    ///
    /// Reads the index without checking entity rows — a follower only *looks*, it fires no
    /// timer; the leader checks at use.
    pub async fn due_timers(&self, now_ms: u64) -> Result<Vec<(u64, InvocationId, u32)>> {
        let lo = keyspace::index_prefix(keyspace::Index::Timer);
        let hi = keyspace::ix_timer_due_upper_bound(now_ms);
        let mut iter = self.db.scan(lo..hi).await.map_err(StoreError::from_slate)?;

        let mut out = Vec::new();
        while let Some(kv) = iter.next().await.map_err(StoreError::from_slate)? {
            if let Some(t) = keyspace::parse_ix_timer(&kv.key) {
                out.push(t);
            }
        }
        Ok(out)
    }

    /// Work still pending in the outbox, in **creation order**: `((invocation,
    /// completion), payload)`.
    ///
    /// The follower's read path: "what is still pending". Reads the [`Index::Outbox`] index
    /// and then the main rows — an index row that is off (the main row is gone or carries a
    /// different `seq`) is skipped.
    ///
    /// This is the function a new leader calls after failover: every outgoing call not yet
    /// fully sent lives here, not in the RAM of the dead leader.
    /// [`Index::Outbox`]: keyspace::Index::Outbox
    pub async fn pending_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<((InvocationId, u32), bytes::Bytes)>> {
        let ix = keyspace::Index::Outbox;
        let prefix = keyspace::index_prefix(ix);
        let mut iter = self
            .db
            .scan_prefix(&prefix, ..)
            .await
            .map_err(StoreError::from_slate)?;

        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = iter.next().await.map_err(StoreError::from_slate)? else {
                break;
            };
            let Some((seq, ekey)) = keyspace::parse_ix_seq(&kv.key, ix) else {
                continue;
            };
            let (Some(k), Some(v)) = (keyspace::parse_outbox(ekey), self.get(ekey).await?) else {
                continue;
            };
            if let Some((s, payload)) = keyspace::decode_seq(&v) {
                if s == seq {
                    out.push((k, v.slice_ref(payload)));
                }
            }
        }
        Ok(out)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        self.db.get(key).await.map_err(StoreError::from_slate)
    }

    /// Like [`crate::RangeStore::get_meta`] on a follower: the value with the row's
    /// `create_ts` — the same mark the leader reads (G20).
    pub async fn get_meta(&self, key: &[u8]) -> Result<Option<(bytes::Bytes, i64)>> {
        Ok(self
            .db
            .get_key_value(key)
            .await
            .map_err(StoreError::from_slate)?
            .map(|kv| (kv.value, kv.create_ts)))
    }

    /// Visits every row whose key starts with `prefix` (empty = every row) in key order,
    /// calling `f(key, value)` for each row **without keeping anything** — `f` returning
    /// `false` stops. For checks that must walk the whole Db while keeping only a small part
    /// (keys, a flag) — check (j) of `verify`.
    ///
    /// Reads in batches like the writer's full-table scans (`bulk_scan`: 4 MiB read-ahead ×
    /// 4 tasks, no block cache): SlateDB's default is one ~4 KiB block per GET, and with a
    /// Db carrying large values (~520 MB in one chaos run) `verify` did not finish in 30
    /// minutes (`tests/scan_fold.rs`).
    pub async fn scan_fold(
        &self,
        prefix: &[u8],
        mut f: impl FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<()> {
        let opts = slatedb::config::ScanOptions {
            read_ahead_bytes: 4 << 20,
            max_fetch_tasks: 4,
            cache_blocks: false,
            ..Default::default()
        };
        let mut iter = self
            .db
            .scan_prefix_with_options(prefix, .., &opts)
            .await
            .map_err(StoreError::from_slate)?;
        while let Some(kv) = iter.next().await.map_err(StoreError::from_slate)? {
            if !f(&kv.key, &kv.value) {
                break;
            }
        }
        Ok(())
    }

    /// At most `limit` raw rows whose key starts with `prefix` (empty = every row) and
    /// passes `keep`, in key order — for `cairn-ctl range dump`. Decodes nothing.
    pub async fn scan_raw(
        &self,
        prefix: &[u8],
        limit: usize,
        keep: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let mut iter = self
            .db
            .scan_prefix(prefix, ..)
            .await
            .map_err(StoreError::from_slate)?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = iter.next().await.map_err(StoreError::from_slate)? else {
                break;
            };
            if keep(&kv.key) {
                out.push((kv.key, kv.value));
            }
        }
        Ok(out)
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await.map_err(StoreError::from_slate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(manifest: u64, durable: u64) -> Freshness {
        Freshness {
            manifest_seq: manifest,
            durable_seq: durable,
        }
    }

    /// The healthy path: durable always ≥ manifest, even when both stand still (no writes —
    /// standing still is *correct*).
    #[test]
    fn idle_without_writes_is_healthy() {
        let mut d = StallDetector::default();
        for _ in 0..20 {
            assert_eq!(d.observe(f(5, 5)), ReaderHealth::Healthy);
            assert_eq!(d.observe(f(5, 9)), ReaderHealth::Healthy);
        }
    }

    /// The stall signature: the manifest advances while durable does not follow — wedged
    /// after GRACE samples.
    #[test]
    fn falling_behind_successive_manifests_is_wedged() {
        let mut d = StallDetector::default();
        assert_eq!(d.observe(f(10, 7)), ReaderHealth::Healthy);
        assert_eq!(d.observe(f(11, 7)), ReaderHealth::Healthy);
        assert!(matches!(
            d.observe(f(12, 7)),
            ReaderHealth::Wedged {
                behind_for_samples: 3
            }
        ));
        // And it stays wedged as long as it keeps lagging.
        assert!(matches!(d.observe(f(13, 7)), ReaderHealth::Wedged { .. }));
    }

    /// A single off sample (in the middle of a refresh round) must not lead to a hasty
    /// conclusion — and catching up clears the count.
    #[test]
    fn transient_lag_then_catch_up_is_healthy_again() {
        let mut d = StallDetector::default();
        assert_eq!(d.observe(f(10, 7)), ReaderHealth::Healthy);
        assert_eq!(d.observe(f(10, 12)), ReaderHealth::Healthy);
        assert_eq!(d.observe(f(11, 7)), ReaderHealth::Healthy);
        assert_eq!(d.observe(f(12, 7)), ReaderHealth::Healthy);
        // A new run starts counting from scratch.
        assert_eq!(d.observe(f(13, 13)), ReaderHealth::Healthy);
    }
}
