// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The nearest deadline of timed work — timers, retries, deferred one-way
//! calls.
//!
//! The range's background loop used to scan the timed tables only once per
//! tick, so a `ctx.sleep` woke up to one tick late (200 ms by default). The
//! engine, however, **knows** the nearest deadline without an extra scan: it
//! just wrote the timed row, and the previous scan saw the first row not yet
//! due ([`cairn_store::Hinted::next_due`]). Keeping that number in RAM is
//! enough for the background loop to sleep until exactly the deadline.
//!
//! It is only a **hint**, not the source of truth — the timed tables in the
//! store are (INV-3). A hint that is too early (the row was cancelled, the
//! commit failed) costs one scan; a missing hint (lost on a leader change)
//! still has the tick as a safety net, and the new term's first scan rebuilds
//! it.

use {
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    tokio::sync::Notify,
};

/// "No known deadline" — no timed row has this deadline.
const NONE: u64 = u64::MAX;

/// The nearest deadline (ms, wall clock — the same clock as the timed rows),
/// shared between the engine (writer) and the background loop (sleeper)
/// without taking the engine lock.
#[derive(Clone)]
pub struct NextDue(Arc<Inner>);

struct Inner {
    at: AtomicU64,
    /// The deadline was just **lowered**: whoever sleeps until the old one must recompute.
    lowered: Notify,
}

impl Default for NextDue {
    fn default() -> Self {
        Self(Arc::new(Inner {
            at: AtomicU64::new(NONE),
            lowered: Notify::new(),
        }))
    }
}

impl NextDue {
    /// There is timed work at `due_ms`. Earlier than the held deadline ⇒ lower
    /// it, and wake the sleeper.
    pub fn offer(&self, due_ms: u64) {
        if due_ms < self.0.at.fetch_min(due_ms, Ordering::AcqRel) {
            // `notify_one` keeps one permit when nobody waits yet: a lowering
            // between the sleeper reading the deadline and starting to wait
            // is not missed.
            self.0.lowered.notify_one();
        }
    }

    /// Like [`Self::offer`], for the result of a scan.
    pub fn offer_scanned(&self, due_ms: Option<u64>) {
        if let Some(d) = due_ms {
            self.offer(d);
        }
    }

    /// The held deadline.
    pub fn get(&self) -> Option<u64> {
        match self.0.at.load(Ordering::Acquire) {
            NONE => None,
            d => Some(d),
        }
    }

    /// Drops the held deadline — right before a scan, which rebuilds it.
    ///
    /// This is what stops the background loop from spinning: when the deadline
    /// has passed but the scan picked up no row (the row was cancelled, the
    /// invocation is mid-turn), after that scan the deadline is the next row
    /// **not yet** due, not the old deadline.
    pub fn clear(&self) {
        self.0.at.store(NONE, Ordering::Release);
    }

    /// Waits until the next time the deadline is lowered.
    pub async fn lowered(&self) {
        self.0.lowered.notified().await;
    }
}
