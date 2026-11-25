// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Measuring a range's size and L0 for split (`range_size`, `range_l0_views`, …).

use super::*;

impl ObjectStores {
    /// Total bytes of every object under `ranges/<id>` on both stores — what D-d would
    /// give back if it deleted that Db.
    pub async fn range_bytes(&self, range: RangeId) -> Result<u64> {
        let prefix = self.range_path(range.0);
        let mut stores = vec![self.main.store()];
        if let Some(wal) = &self.wal {
            stores.push(wal.store());
        }
        let mut n = 0;
        for store in stores {
            let listed: Vec<_> = store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|e| StoreError::Io(e.to_string()))?;
            n += listed.iter().map(|m| m.size).sum::<u64>();
        }
        Ok(n)
    }

    /// The number of L0 **views** in the stored manifest of a range's Db (0 if the Db
    /// does not exist yet).
    ///
    /// Counts views, not SSTs, because SlateDB's `l0_max_ssts` counts views — and after
    /// a projection, each parent L0 SST spanning both regions becomes **two** views in
    /// the child (F4). The split precondition uses this number through
    /// [`split_keeps_l0_headroom`].
    pub async fn range_l0_views(&self, range: RangeId) -> Result<usize> {
        Ok(admin_io(self.admin(range.0).read_manifest(None))
            .await?
            .map(|m| m.l0().len())
            .unwrap_or(0))
    }

    /// The number of L0 views a new Db **will inherit** from the Db of range `range` if
    /// it takes the pk span `span` — exact, not an upper bound: each L0 view of the
    /// source becomes one view for each byte range of `span` it intersects (F4), and an
    /// already projected view (the child of an earlier split) intersects only one region.
    ///
    /// The split precondition checks each child; the merge precondition sums the two
    /// sources. [`split_keeps_l0_headroom`] is the upper bound when the views cannot be
    /// read.
    pub async fn projected_l0_views(
        &self,
        range: RangeId,
        span: (PartitionKey, PartitionKey),
    ) -> Result<usize> {
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(0);
        };
        let targets = crate::keyspace::owned_ranges(span.0, span.1);
        Ok(m.l0()
            .iter()
            .map(|v| {
                targets
                    .iter()
                    .filter(|(lo, hi)| {
                        view_meets(
                            v.sst.info.first_entry.as_deref(),
                            v.sst.info.last_entry.as_deref(),
                            v.visible_range(),
                            lo,
                            hi,
                        )
                    })
                    .count()
            })
            .sum())
    }

    /// **Approximate size** of the Db of range `range` — one manifest GET, no SST is
    /// read.
    ///
    /// Counts the **visible part** of each view (`estimate_visible_size`, fork patch
    /// F6): a projected child range keeps views onto the parent's physical SSTs, and
    /// `estimate_size` counts those whole SSTs — without F6 the child looks as large as
    /// the parent. Memtable and unflushed WAL are not counted: the size is for deciding
    /// a split, on the order of GiB, so a few dozen unflushed MiB change nothing.
    ///
    /// It comes with the **pieces** `(owner_pk at the start of the view, bytes)` — enough
    /// to pick a split point near the size median when splitting for size without a load
    /// sample. Views starting in the derived region (no owner) have no piece.
    pub async fn range_size(&self, range: RangeId) -> Result<RangeSize> {
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(RangeSize::default());
        };
        let mut out = RangeSize::default();
        let mut add = |v: &slatedb::manifest::SsTableView, l0: bool| {
            let bytes = v.estimate_visible_size();
            out.visible_bytes += bytes;
            if l0 {
                out.l0_views += 1;
            }
            let start = match v.visible_range() {
                Some(r) => match std::ops::RangeBounds::start_bound(&r) {
                    Bound::Included(k) | Bound::Excluded(k) => Some(k.clone()),
                    Bound::Unbounded => None,
                },
                None => v.sst.info.first_entry.clone(),
            };
            if let Some(owner) = start.as_deref().and_then(owner_prefix) {
                out.pieces.push((owner, bytes));
            }
        };
        for v in m.l0() {
            add(v, true);
        }
        for sr in m.compacted() {
            for v in sr.sst_views() {
                add(v, false);
            }
        }
        Ok(out)
    }

    /// The L0 cap the range's Db runs with — SlateDB's `l0_max_ssts`, counted in views.
    /// Cairn does not tune it, so this is SlateDB's default.
    pub fn l0_max_ssts(&self) -> usize {
        slatedb::Settings::default().l0_max_ssts
    }
}

/// The L0 precondition of split/merge, **counted in views**.
///
/// Each L0 view of a source can become two views in the new Db — one for the hot
/// region, one for the bulk region (F4) — and SlateDB blocks writes when the L0 view
/// count reaches `l0_max_ssts` (measured: writes stall for up to one poll period). So
/// the new Db must keep room for at least two flushes:
/// `2 × Σ L0 views of the sources + 2 ≤ l0_max_ssts`. If it fails, the coordinator
/// waits for compaction (or forces a flush + compact) and tries again.
pub fn split_keeps_l0_headroom(source_l0_views: &[usize], l0_max_ssts: usize) -> bool {
    2 * source_l0_views.iter().sum::<usize>() + 2 <= l0_max_ssts
}

/// `owner_pk` at the start of an entity-region key (`region ‖ owner_pk ‖ …`) — including
/// the nine-byte truncated key of a projection boundary. Derived region ⇒ `None`.
pub(super) fn owner_prefix(key: &[u8]) -> Option<u64> {
    let region = crate::keyspace::Region::of_key(key)?;
    if region == crate::keyspace::Region::Derived {
        return None;
    }
    Some(u64::from_be_bytes(key.get(1..9)?.try_into().ok()?))
}

/// The result of [`ObjectStores::range_size`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeSize {
    /// Total visible part of every view (L0 + sorted runs), in bytes.
    pub visible_bytes: u64,
    /// The number of L0 views.
    pub l0_views: usize,
    /// `(owner_pk at the start of the view, visible bytes)` — in no particular order.
    pub pieces: Vec<(u64, u64)>,
}

/// Whether an L0 view (SST `[first, last]`, visible range `visible`) has any visible key
/// in `[lo, hi)` — the intersection of the SST's physical range `[first, last]` with the
/// view's visible range. Missing information returns `true`: overcounting only makes the
/// precondition stricter.
pub(super) fn view_meets(
    first: Option<&[u8]>,
    last: Option<&[u8]>,
    visible: Option<impl std::ops::RangeBounds<Bytes>>,
    lo: &[u8],
    hi: &[u8],
) -> bool {
    if first.is_some_and(|f| f >= hi) {
        return false;
    }
    if last.is_some_and(|l| l < lo) {
        return false;
    }
    if let Some(r) = visible {
        let starts_before_hi = match r.start_bound() {
            Bound::Included(s) | Bound::Excluded(s) => s.as_ref() < hi,
            Bound::Unbounded => true,
        };
        let ends_after_lo = match r.end_bound() {
            Bound::Included(e) => e.as_ref() >= lo,
            Bound::Excluded(e) => e.as_ref() > lo,
            Bound::Unbounded => true,
        };
        if !(starts_before_hi && ends_after_lo) {
            return false;
        }
    }
    true
}
