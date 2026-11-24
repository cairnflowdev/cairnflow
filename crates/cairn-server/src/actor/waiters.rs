// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Registry of the waiters for the outcome of a range's invocations.
//!
//! `/invoke` waits for an invocation to stop for good (`ingress/invoke.rs::wait_for_settled`).
//! This registry wakes it **right when** the commit that takes the invocation to
//! `completed`/`failed`/`paused` is submitted. The poll is only a safety net: sparse (1 s)
//! while the registry **is fed** — the range's leader has attached its notify hook to the
//! writer ([`Waiters::feed`]) — and 5 → 50 ms when it is not (the range has no leader yet).
//!
//! **The registry is only a hint (LF-7).** It carries no state: a notification only makes
//! the poll come earlier, and the answer still goes through `on_settled` → `finish` (durable
//! ticket + read fence). A spurious notification ⇒ one poll answering "not yet"; a missing
//! one (the registry is lost on a leader change or a range split, the waiter is on another
//! node) ⇒ the poll catches it, slower but not wrong. Hence the registry is not *pending
//! work* in the sense of INV-3: each waiter is an HTTP request that already lives in the
//! ingress's RAM.
//!
//! **No leaks.** An id's entry lives exactly as long as its [`WaitGuard`]s — the guard lives
//! in the request's future, so the request finishing, hitting its deadline or the client
//! disconnecting (the future is dropped) all remove the entry. No path adds an entry
//! without a guard.
//!
//! **Lock per range, not per node.** One registry per [`super::EngineHandle`]; the lock is
//! held only for one map lookup, never across an `.await` — the notifier is the commit on
//! the engine thread (`PartitionWriter::set_status_hook`).

use {
    cairn_engine::InvocationId,
    cairn_store::entropy::HashMap,
    std::{
        pin::Pin,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, MutexGuard,
        },
    },
    tokio::sync::{futures::OwnedNotified, Notify},
};

/// Waiter registry of one range. Cheap to clone, all copies share one registry.
#[derive(Clone, Default)]
pub struct Waiters(Arc<Book>);

#[derive(Default)]
struct Book {
    map: Mutex<HashMap<InvocationId, Slot>>,
    /// Notify hook of the attached leader ([`Waiters::feed`]).
    fed: AtomicBool,
}

struct Slot {
    notify: Arc<Notify>,
    /// Number of live [`WaitGuard`]s of this id.
    guards: usize,
}

impl Waiters {
    fn map(&self) -> MutexGuard<'_, HashMap<InvocationId, Slot>> {
        self.0.map.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Register to wait for `id`. The entry lives until the last guard of `id` is dropped.
    pub fn watch(&self, id: InvocationId) -> WaitGuard {
        let notify = {
            let mut map = self.map();
            let slot = map.entry(id).or_insert_with(|| Slot {
                notify: Arc::new(Notify::new()),
                guards: 0,
            });
            slot.guards += 1;
            Arc::clone(&slot.notify)
        };
        crate::metrics::floor::waiters_add(1);
        WaitGuard {
            waiters: self.clone(),
            id,
            notify,
        }
    }

    /// Like [`Self::watch`], plus an **already armed** notification future: build it
    /// **before** the first question about `id` (the `invoke`, `status` command), so an
    /// outcome submitted between that answer and entering the wait loop does not slip past.
    pub fn watch_armed(&self, id: InvocationId) -> Watch {
        let guard = self.watch(id);
        let next = guard.arm();
        Watch { guard, next }
    }

    /// The leader has just attached its notify hook to the range's writer: until the
    /// returned guard is dropped (end of serving), every submitted outcome goes through
    /// [`Self::notify`]. Dropping the guard wakes every waiter of the registry.
    pub fn feed(&self) -> Feeding {
        self.0.fed.store(true, Ordering::Release);
        Feeding(self.clone())
    }

    /// Whether a notify hook is feeding the registry. `false` ⇒ waiters only have the poll.
    pub fn fed(&self) -> bool {
        self.0.fed.load(Ordering::Acquire)
    }

    /// A commit has just taken `id` to an outcome: wake the waiters of **exactly that id**.
    /// Nobody waiting costs one map lookup.
    pub fn notify(&self, id: &InvocationId) {
        let notify = self.map().get(id).map(|s| Arc::clone(&s.notify));
        if let Some(n) = notify {
            n.notify_waiters();
        }
    }

    /// Number of ids that have waiters.
    pub fn len(&self) -> usize {
        self.map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One term feeding the registry ([`Waiters::feed`]). Drop ⇒ the registry is no longer fed.
pub struct Feeding(Waiters);

impl Drop for Feeding {
    fn drop(&mut self) {
        self.0 .0.fed.store(false, Ordering::Release);
        // Collect the `Notify`s outside the lock, then notify: the lock is held only for the scan.
        let all: Vec<_> = self
            .0
            .map()
            .values()
            .map(|s| Arc::clone(&s.notify))
            .collect();
        for n in all {
            n.notify_waiters();
        }
    }
}

/// A registered waiter with its next notification already armed ([`Waiters::watch_armed`]).
pub struct Watch {
    guard: WaitGuard,
    next: Pin<Box<OwnedNotified>>,
}

impl Watch {
    pub fn into_parts(self) -> (WaitGuard, Pin<Box<OwnedNotified>>) {
        (self.guard, self.next)
    }
}

/// A registered waiter. Drop ⇒ removes the entry when it is the id's last guard.
pub struct WaitGuard {
    waiters: Waiters,
    id: InvocationId,
    notify: Arc<Notify>,
}

impl WaitGuard {
    /// Future that wakes on the **next** notification — already armed: a notification that
    /// arrives after this call but before the poll is not lost. Call it **before** the poll.
    pub fn arm(&self) -> Pin<Box<OwnedNotified>> {
        let mut armed = Box::pin(Arc::clone(&self.notify).notified_owned());
        armed.as_mut().enable();
        armed
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        {
            let mut map = self.waiters.map();
            if let Some(slot) = map.get_mut(&self.id) {
                slot.guards -= 1;
                if slot.guards == 0 {
                    map.remove(&self.id);
                }
            }
        }
        crate::metrics::floor::waiters_add(-1);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, futures::FutureExt};

    #[test]
    fn notify_wakes_only_the_armed_id_and_guards_clean_up() {
        let w = Waiters::default();
        let a = w.watch([1; 24]);
        let b = w.watch([2; 24]);
        let a2 = w.watch([1; 24]);
        assert_eq!(w.len(), 2);
        let mut wa = a.arm();
        let mut wb = b.arm();
        // A notification before the poll is still not lost: the future is armed.
        w.notify(&[1; 24]);
        assert!(
            wa.as_mut().now_or_never().is_some(),
            "waiter of id 1 did not wake"
        );
        assert!(
            wb.as_mut().now_or_never().is_none(),
            "notifying id 1 woke id 2"
        );
        drop(a);
        assert_eq!(w.len(), 2, "one guard of id 1 remains");
        drop(a2);
        drop(b);
        drop(wb);
        assert!(
            w.is_empty(),
            "registry still has entries after every guard dropped"
        );
    }

    #[test]
    fn armed_watch_catches_a_notify_before_the_wait_starts() {
        let w = Waiters::default();
        let (_guard, mut next) = w.watch_armed([3; 24]).into_parts();
        w.notify(&[3; 24]);
        assert!(
            next.as_mut().now_or_never().is_some(),
            "notification before the wait was lost"
        );
    }

    #[test]
    fn feeding_lasts_exactly_one_term() {
        let w = Waiters::default();
        assert!(!w.fed());
        let term = w.feed();
        assert!(w.clone().fed(), "every clone shares the flag");
        let (_guard, mut next) = w.watch_armed([4; 24]).into_parts();
        drop(term);
        assert!(!w.fed(), "term ended but the registry still counts as fed");
        assert!(
            next.as_mut().now_or_never().is_some(),
            "term ended but the waiter did not wake"
        );
    }

    #[test]
    fn notify_without_waiters_is_a_no_op() {
        let w = Waiters::default();
        w.notify(&[9; 24]);
        assert!(w.is_empty());
    }
}
