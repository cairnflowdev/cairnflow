// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The `QueryRanges` registry**: every range that finishes opening on this
//! node registers a [`RangeReadHandle`] — a `Db` clone, the writer's
//! `ValidityHandle`, the range + generation, query counters. The query layer
//! reads through here, **not through the engine**: however long a turn holds
//! the engine, it never blocks a SQL statement.
//!
//! The handle is removed **before** the Db closes: [`QueryRangeGuard`] lives
//! as long as the term's serving part and is dropped before stepping down
//! (seal). Removal marks the handle closed, so a scan running on an old clone
//! stops at the next batch (`partial`).

use {
    cairn_store::range_read::RangeReadHandle,
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    },
};

/// The node's read-handle registry. Cheap to clone (shared). The lock is only
/// touched at register, removal, and when a query snapshots the list — never on
/// the turn path.
#[derive(Clone, Default)]
pub struct QueryRanges {
    inner: Arc<Mutex<BTreeMap<u32, RangeReadHandle>>>,
}

impl std::fmt::Debug for QueryRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.handles()).finish()
    }
}

impl QueryRanges {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, RangeReadHandle>> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Registers the handle of a range that just finished opening. An older
    /// handle of the same range (a previous term not yet removed) is closed and
    /// replaced.
    pub fn register(&self, handle: RangeReadHandle) -> QueryRangeGuard {
        let id = handle.range().id.0;
        if let Some(old) = self.lock().insert(id, handle.clone()) {
            old.close();
        }
        QueryRangeGuard {
            ranges: self.clone(),
            handle,
        }
    }

    /// Every live handle, by range id.
    pub fn handles(&self) -> Vec<RangeReadHandle> {
        self.lock()
            .values()
            .filter(|h| h.alive())
            .cloned()
            .collect()
    }

    /// The handle of range `id`, if this node is serving it.
    pub fn get(&self, id: u32) -> Option<RangeReadHandle> {
        self.lock().get(&id).filter(|h| h.alive()).cloned()
    }
}

/// Holds one registration; dropping it removes (and closes) the handle.
pub struct QueryRangeGuard {
    ranges: QueryRanges,
    handle: RangeReadHandle,
}

impl Drop for QueryRangeGuard {
    fn drop(&mut self) {
        self.handle.close();
        let id = self.handle.range().id.0;
        let mut map = self.ranges.lock();
        // Only remove our own handle — a later term may already have replaced it.
        if map.get(&id).is_some_and(|h| !h.alive()) {
            map.remove(&id);
        }
    }
}
