// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `ActiveIntents`: ranges this node's coordinator is splitting/merging, and the quiesced set (SM-13).

use super::*;
/// Ranges **this node's** coordinator is splitting/merging. The engine factory
/// that meets an intent on a range in this set does not roll back — the
/// coordinator is alive, right here.
///
/// The second set, **quiesced** (SM-13): sources the coordinator is about to
/// quiesce or has quiesced for clone/union. From then until the coordinator
/// finishes (commit or roll-back), nobody may open the source's Db for writing: a
/// write after the clone is not in the children, and the commit CAS still wins
/// because the table did not change. A source engine that loses its lease
/// midway takes it back and then **waits** in the engine factory
/// ([`Self::wait_unquiesced`]) instead of opening. Marked right **before** the
/// quiesce command, not earlier: the other range of a merge is gathered and
/// opened on this node (`gather_here`) before that.
#[derive(Clone, Default)]
pub struct ActiveIntents(Arc<ActiveInner>);

#[derive(Default)]
struct ActiveInner {
    sets: Mutex<ActiveSets>,
    /// Wakes the waiters in [`ActiveIntents::wait_unquiesced`] when a mark is removed.
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct ActiveSets {
    active: BTreeSet<u32>,
    quiesced: BTreeSet<u32>,
}

impl ActiveIntents {
    pub fn contains(&self, p: PartitionId) -> bool {
        self.0.sets.lock().is_ok_and(|s| s.active.contains(&p.0))
    }

    /// This node's coordinator has quiesced (or is about to quiesce) `p` for
    /// clone/union, and is not done yet (SM-13).
    pub fn is_quiesced(&self, p: PartitionId) -> bool {
        self.0.sets.lock().is_ok_and(|s| s.quiesced.contains(&p.0))
    }

    /// Wait until `p` no longer carries the quiesce mark — the coordinator has
    /// committed or rolled back. Holds the lease while waiting (the holder of
    /// `keeper`): lease lost ⇒ `LeaseLost`, the caller takes it again from scratch.
    pub async fn wait_unquiesced(
        &self,
        p: PartitionId,
        keeper: Option<&LeaseKeeper>,
    ) -> cairn_store::Result<()> {
        if self.is_quiesced(p) {
            tracing::info!(range = p.0, "range is the quiesced source of a split/merge running on this node — waiting, not opening the Db");
        }
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.is_quiesced(p) {
                return Ok(());
            }
            let lost = async {
                match keeper {
                    Some(k) => k.lost().await,
                    None => std::future::pending().await,
                }
            };
            // `biased`: lease loss before every re-check.
            tokio::select! {
                biased;
                () = lost => return Err(StoreError::LeaseLost),
                _ = changed => {}
                // Net for a missed wake-up: the mark only changes under the mutex.
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    }

    /// Hold `ranges` for one coordinator. `None` if a range among them already has
    /// another coordinator **of this node**: two splits of the same range from the
    /// same table snapshot write out the **same** table (child ids allocated from
    /// `next_range_id`), and resolving an ambiguous CAS by comparing content
    /// (rule 7) would declare both of them the winner.
    ///
    /// Because of that refusal, each range has **at most one** guard on a node: the
    /// set is already a counter with limit 1, and the guard of a failed `hold` does
    /// not exist, so it cannot remove the id of the current holder.
    pub(super) fn hold(&self, ranges: &[PartitionId]) -> Option<ActiveGuard> {
        let mut s = self.0.sets.lock().ok()?;
        let taken = ranges.iter().any(|p| s.active.contains(&p.0));
        match cairn_split::step(
            &cairn_split::View::default(),
            &cairn_split::Event::Hold { taken },
        ) {
            cairn_split::Plan::Hold => {}
            // The guard of a failed hold does not exist to remove the current
            // holder's mark (`release: false`).
            cairn_split::Plan::Refuse { .. } => return None,
            p => unreachable!("core returned {p:?} for Hold"),
        }
        s.active.extend(ranges.iter().map(|p| p.0));
        Some(ActiveGuard {
            set: self.clone(),
            ranges: ranges.iter().map(|p| p.0).collect(),
            quiesced: std::sync::atomic::AtomicBool::new(false),
            holders: Mutex::new(Vec::new()),
        })
    }
}

pub(super) struct ActiveGuard {
    set: ActiveIntents,
    ranges: Vec<u32>,
    /// This guard has marked `ranges` as quiesced. Only the winner of the intent
    /// CAS marks, so a range's mark has exactly one owner.
    quiesced: std::sync::atomic::AtomicBool,
    /// Sending ends of the quiesce hold wire (SM-15), one per engine asked to quiesce.
    /// Dropped with the guard: any engine still in `quiesced()` exits as on `Resign`.
    holders: Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
}

impl ActiveGuard {
    /// Mark every source of the guard as quiesced (SM-13) — call right **before**
    /// the first quiesce command. Removed when the guard is dropped: after commit
    /// (and retirement), or after a roll-back has CASed the intent away.
    pub(super) fn mark_quiesced(&self) {
        if let Ok(mut s) = self.set.0.sets.lock() {
            s.quiesced.extend(self.ranges.iter().copied());
        }
        self.quiesced
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// A new quiesce hold wire (SM-15) — the receiving end goes with the quiesce
    /// command, the sending end lives until the guard drops.
    pub(super) fn quiesce_hold(&self) -> crate::actor::QuiesceHold {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if let Ok(mut h) = self.holders.lock() {
            h.push(tx);
        }
        rx
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let quiesced = self.quiesced.load(std::sync::atomic::Ordering::Acquire);
        if let Ok(mut s) = self.set.0.sets.lock() {
            for r in &self.ranges {
                s.active.remove(r);
                if quiesced {
                    s.quiesced.remove(r);
                }
            }
        }
        if quiesced {
            self.set.0.changed.notify_waiters();
        }
    }
}
