// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **A range's read handle for the query layer**: reads that bypass the engine, durable
//! data only, one consistent snapshot per scan.
//!
//! The read contract:
//! 1. **Durable data only** — `durability_filter = Remote`.
//! 2. **Each range is one consistent snapshot** — one [`RangeSnapshot`] (`Db::snapshot`)
//!    for the whole scan of that range.
//! 3. **May be stale** compared with engine reads by at most one durability tick; by at
//!    most one lease TTL if the leader has just been deposed (the handle is removed when
//!    the writer loses validity).
//!
//! No common snapshot across ranges is promised, and no strict order for rows created
//! while paging.
//!
//! A handle lives by its writer's term: [`RangeReadHandle::alive`] = not removed **and**
//! the writer's `ValidityHandle` still valid. Each batch of a scan checks `alive` first — a
//! range deposed in the middle of a query reports an error for the rest, and the caller
//! reports `partial` + the range's name. Remove the handle **before** closing the Db.

use {
    crate::{
        error::{Result, StoreError},
        partition_table::RangeEntry,
        query_index::{self, QueryCounts, QueryIndex},
        writer::ValidityHandle,
    },
    bytes::Bytes,
    slatedb::{
        config::{DurabilityLevel, ReadOptions, ScanOptions},
        Db, DbSnapshot, IterationOrder,
    },
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// Read handle of a range that is leading on this node. Cheap to clone.
#[derive(Clone)]
pub struct RangeReadHandle {
    db: Db,
    validity: ValidityHandle,
    range: RangeEntry,
    counts: Arc<QueryCounts>,
    /// The range's counter of live artifact bytes.
    artifact_bytes: Arc<crate::artifact_bytes::ArtifactBytes>,
    indexes: &'static [QueryIndex],
    closed: Arc<AtomicBool>,
    /// Live snapshots (taken, not yet released) — cancellation tests assert it returns to 0.
    live: Arc<std::sync::atomic::AtomicUsize>,
}

impl std::fmt::Debug for RangeReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeReadHandle")
            .field("range", &self.range.id.0)
            .field("gen", &self.range.gen)
            .field("alive", &self.alive())
            .finish()
    }
}

/// The "range can no longer be read through this handle" error — the caller reports
/// `partial`.
pub fn gone(range: &RangeEntry) -> StoreError {
    StoreError::Io(format!(
        "range {} (gen {}) can no longer be read through the handle (removed or lease lost)",
        range.id.0, range.gen
    ))
}

impl RangeReadHandle {
    pub fn new(
        db: Db,
        validity: ValidityHandle,
        range: RangeEntry,
        counts: Arc<QueryCounts>,
        indexes: &'static [QueryIndex],
    ) -> Self {
        Self {
            db,
            validity,
            range,
            counts,
            artifact_bytes: Arc::default(),
            indexes,
            live: Arc::default(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn range(&self) -> &RangeEntry {
        &self.range
    }

    /// The range's in-RAM query counters — reads without touching the store.
    pub fn counts(&self) -> &Arc<QueryCounts> {
        &self.counts
    }

    /// The range's counter of live artifact bytes — reads without touching the store. A
    /// handle that is not attached ([`Self::with_artifact_bytes`]) has a counter that is not
    /// built yet: readers see the range as `indexing`.
    pub fn artifact_bytes(&self) -> &Arc<crate::artifact_bytes::ArtifactBytes> {
        &self.artifact_bytes
    }

    /// Attaches the engine's counter of artifact bytes.
    pub fn with_artifact_bytes(mut self, bytes: Arc<crate::artifact_bytes::ArtifactBytes>) -> Self {
        self.artifact_bytes = bytes;
        self
    }

    /// The table of descriptors the range's writer maintains.
    pub fn indexes(&self) -> &'static [QueryIndex] {
        self.indexes
    }

    /// Still readable: not removed and the writer still valid.
    pub fn alive(&self) -> bool {
        !self.closed.load(Ordering::Acquire) && self.validity.is_valid()
    }

    /// Removes the handle: every later read returns an error. Call **before** closing the Db.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// A consistent snapshot for one scan of the range.
    pub fn snapshot(&self) -> Result<RangeSnapshot> {
        if !self.alive() {
            return Err(gone(&self.range));
        }
        let snap = self.db.snapshot().map_err(StoreError::from_slate)?;
        self.live.fetch_add(1, Ordering::Relaxed);
        Ok(RangeSnapshot {
            snap,
            handle: self.clone(),
        })
    }

    /// Shares the live snapshot counter with the other handles of the same range (the
    /// engine hands out several handles for one range).
    pub fn sharing_live(mut self, live: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        self.live = live;
        self
    }

    /// Number of this handle's live snapshots. A query that has finished or been cancelled
    /// holds no snapshot (`query_cancel_releases_snapshot`).
    pub fn live_snapshots(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }
}

impl Drop for RangeSnapshot {
    fn drop(&mut self) {
        self.handle.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A range's snapshot for one scan. Dropping it releases the snapshot.
pub struct RangeSnapshot {
    snap: Arc<DbSnapshot>,
    handle: RangeReadHandle,
}

fn read_options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    }
}

/// Query index scans: small, hot blocks (index heads, the "latest" page) — cached. Scans of
/// hot tables (the main path, fallback) are not.
fn scan_options(cache_blocks: bool, order: IterationOrder) -> ScanOptions {
    ScanOptions {
        durability_filter: DurabilityLevel::Remote,
        cache_blocks,
        order,
        ..Default::default()
    }
}

impl RangeSnapshot {
    pub fn handle(&self) -> &RangeReadHandle {
        &self.handle
    }

    fn check(&self) -> Result<()> {
        if self.handle.alive() {
            Ok(())
        } else {
            Err(gone(&self.handle.range))
        }
    }

    /// One value, at the `Remote` level.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check()?;
        self.snap
            .get_with_options(key, &read_options())
            .await
            .map_err(StoreError::from_slate)
    }

    /// At most `limit` rows in `[lo, hi)` in key order (`descending` = reversed), at the
    /// `Remote` level. `alive` is checked before opening and every 256 rows.
    pub async fn scan(
        &self,
        lo: Vec<u8>,
        hi: Vec<u8>,
        limit: usize,
        descending: bool,
        cache_blocks: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.check()?;
        let order = if descending {
            IterationOrder::Descending
        } else {
            IterationOrder::Ascending
        };
        let mut it = self
            .snap
            .scan_with_options(lo..hi, &scan_options(cache_blocks, order))
            .await
            .map_err(StoreError::from_slate)?;
        let mut out = Vec::new();
        while out.len() < limit {
            if out.len() % 256 == 255 {
                self.check()?;
            }
            let Some(kv) = it.next().await.map_err(StoreError::from_slate)? else {
                break;
            };
            out.push((kv.key, kv.value));
        }
        Ok(out)
    }

    /// The `readable` descriptors according to this snapshot's marks.
    pub async fn readable_indexes(&self) -> Result<Vec<&'static QueryIndex>> {
        let marker = self
            .get(&query_index::ready_key())
            .await?
            .and_then(|raw| query_index::parse_ready_marker(&raw))
            .filter(|m| {
                m.range == u64::from(self.handle.range.id.0) && m.gen == self.handle.range.gen
            });
        Ok(self
            .handle
            .indexes
            .iter()
            .filter(|ix| {
                marker
                    .as_ref()
                    .and_then(|m| m.version_of(ix.name))
                    .is_some_and(|v| v == ix.version)
            })
            .collect())
    }
}
