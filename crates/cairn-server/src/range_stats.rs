// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Lifecycle measurements of each range — open, rebuilding the derived region, recovering
//! the queues, drain and seal at quiesce.
//!
//! The split/merge coordinator puts them into the `report` of the admin response: its own
//! part (CAS, clone, commit) it measures itself, while what happens **inside** the engine
//! thread (seal, opening the children's Dbs, rebuilds) only the engine thread can measure.
//!
//! **One copy per node**, carried on [`crate::engine_pool::EnginePool`] (one pool per node,
//! like the turn limit D-c): the engine factory and `leadership_loop` both reach the pool
//! without a path to ingress state. Keyed by range id — ids are never reused (D22), so a
//! row is never overwritten by another range. Not a process-wide static: many nodes in one
//! process (tests) have the same range ids.

use {
    serde::Serialize,
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::Duration,
    },
};

/// Range lifecycle measurements of **one node**. `Clone` is cheap, shared.
#[derive(Clone, Default)]
pub struct RangeStats(Arc<Mutex<BTreeMap<u32, RangeTimings>>>);

impl RangeStats {
    /// Change the row of range `range` (created if missing).
    pub fn update(&self, range: u32, f: impl FnOnce(&mut RangeTimings)) {
        if let Ok(mut m) = self.0.lock() {
            f(m.entry(range).or_default());
        }
    }

    /// The current row of range `range`.
    pub fn get(&self, range: u32) -> RangeTimings {
        self.0
            .lock()
            .ok()
            .and_then(|m| m.get(&range).cloned())
            .unwrap_or_default()
    }
}

/// The latest measurements of one range. A field that has not happened yet is `None`.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeTimings {
    /// Taking the lease (through the balancer) — the latest leadership take.
    pub claim_ms: Option<f64>,
    /// Table reload + opening the SlateDB Db + opening the writer.
    pub open_db_ms: Option<f64>,
    /// The derived region rebuild at open, if the mark was missing or off.
    pub rebuild: Option<RebuildReport>,
    /// Flow control recovery (VO locks, queues) + loading rules + `recover`.
    pub restore_ms: Option<f64>,
    /// Quiesce: wait for running turns, then cancel the rest.
    pub drain_ms: Option<f64>,
    /// Seal: fence, flush the WAL, flush the memtable, close the Db.
    pub seal_ms: Option<f64>,
}

/// One rebuild of the derived region, after [`cairn_store::RebuildStats`].
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebuildReport {
    pub ms: f64,
    /// Keys read by every scan of the rebuild, per region.
    pub hot_rows: u64,
    pub derived_rows: u64,
    pub bulk_rows: u64,
    /// Bytes (key + value) of the hot region's scan.
    pub bytes: u64,
    pub written: usize,
    pub removed: usize,
}

impl From<&cairn_store::RebuildStats> for RebuildReport {
    fn from(s: &cairn_store::RebuildStats) -> Self {
        Self {
            ms: ms(s.elapsed),
            hot_rows: s.regions.hot,
            derived_rows: s.regions.derived,
            bulk_rows: s.regions.bulk,
            bytes: s.bytes,
            written: s.written,
            removed: s.removed,
        }
    }
}

/// Milliseconds, one decimal digit — enough for phases of a few ms.
pub fn ms(d: Duration) -> f64 {
    (d.as_secs_f64() * 10_000.0).round() / 10.0
}
