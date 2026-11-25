// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The node-wide replay RAM budget.
//!
//! Each SDK turn holds the invocation's journal in RAM for the whole turn: the replayed
//! state machine, and with `REQUEST_RESPONSE` also the encoded (and compressed) body of the
//! whole journal. `max-journal-bytes=` limits **one** journal; this budget limits the
//! **total** on the node — otherwise 64 slots × 32 MiB is still enough to run a 1 GB machine
//! out of RAM, the platform restarts it, the turns run again, and it crashes again.
//!
//! A turn holds a [`ReplayHold`] for the durable journal bytes of it from loading until the
//! end of the turn. Not enough room ⇒ the turn **does not call the SDK** and is rescheduled
//! after [`REPLAY_RETRY_MS`], the attempt unchanged — the same "refuse before calling" path
//! as a suspended tenant (`View::tenant_refused_ms`, the lifecycle core). While the budget is
//! empty a turn always runs, even when its journal is larger than the whole budget: an
//! invocation is never blocked forever.
//!
//! **Split by tenant** (a security requirement): one tenant holds no more than
//! [`TENANT_SHARE`] (¼, matching the per-tenant turn limit) of the budget — except for its
//! first turn while it holds nothing; and a turn whose journal is ≤ `min(`[`SMALL_JOURNAL`]`, cap /
//! 64)` is always admitted, not counted against the tenant's share (RAM is still bounded: the
//! node's turn slots × that threshold). Otherwise two tenants holding turns open until
//! inactivity would be enough to keep every other tenant's turns rescheduling forever.
//!
//! Atomic counters of the **process** (= the node in production), no lock at all
//! (AGENTS §6 "hot path"): tenant shares are counted in [`TENANT_SLOTS`] slots chosen
//! by a deterministic hash of the name — two tenants in the same slot share one share (a loss
//! of fairness, not starvation: an empty slot always gets one turn). A harness running many
//! nodes in one process shares the budget among them — the default limit is never
//! reached in tests.

use std::sync::atomic::{AtomicU64, Ordering};

/// Default budget (flag `--replay-ram-budget-bytes`): 512 MiB.
pub const DEFAULT_REPLAY_RAM_BUDGET: u64 = 512 << 20;

/// A tenant's maximum share of the budget: `cap / TENANT_SHARE`.
pub const TENANT_SHARE: u64 = 4;

/// A turn whose journal is no larger than `min(SMALL_JOURNAL, cap / SMALL_DIV)` is always
/// admitted: what exceeds the limit is bounded (turn slots × `cap / SMALL_DIV`).
pub const SMALL_JOURNAL: u64 = 1 << 20;

/// Xem [`SMALL_JOURNAL`].
pub const SMALL_DIV: u64 = 64;

/// Number of slots counting tenant shares.
pub const TENANT_SLOTS: usize = 256;

/// A turn without room in the budget is rescheduled after this long.
pub const REPLAY_RETRY_MS: u64 = 1_000;

/// The process's budget.
pub static NODE: ReplayBudget = ReplayBudget::new(DEFAULT_REPLAY_RAM_BUDGET);

/// A byte budget. `cap == 0` = off (every turn runs).
#[derive(Debug)]
pub struct ReplayBudget {
    cap: AtomicU64,
    used: AtomicU64,
    /// Turns rescheduled because the budget ran out (metric).
    throttled: AtomicU64,
    /// Bytes held per tenant slot ([`slot_of`]), small turns excluded.
    tenants: [AtomicU64; TENANT_SLOTS],
}

/// The slot of `tenant`: 64-bit FNV-1a — deterministic (simulation), no collision resistance needed.
fn slot_of(tenant: &str) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in tenant.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % TENANT_SLOTS as u64) as usize
}

/// Add `bytes` to `c` if `c + bytes ≤ limit` or `c == 0`; `limit == 0` = no limit.
fn add_within(c: &AtomicU64, bytes: u64, limit: u64) -> bool {
    let mut cur = c.load(Ordering::Acquire);
    loop {
        if limit > 0 && cur > 0 && cur.saturating_add(bytes) > limit {
            return false;
        }
        match c.compare_exchange_weak(
            cur,
            cur.saturating_add(bytes),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(seen) => cur = seen,
        }
    }
}

impl ReplayBudget {
    pub const fn new(cap: u64) -> Self {
        Self {
            cap: AtomicU64::new(cap),
            used: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            tenants: [const { AtomicU64::new(0) }; TENANT_SLOTS],
        }
    }

    /// Set the limit (node start-up). `0` = off.
    pub fn set_cap(&self, cap: u64) {
        self.cap.store(cap, Ordering::Release);
    }

    pub fn cap(&self) -> u64 {
        self.cap.load(Ordering::Acquire)
    }

    /// Bytes held.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    pub fn throttled(&self) -> u64 {
        self.throttled.load(Ordering::Relaxed)
    }

    /// Hold `bytes` for a turn of `tenant`. `None` = not enough room (already counted in the
    /// metric). The rules are in the module's top doc: small turns always pass; other turns need
    /// room in the tenant's share (or the tenant holds nothing) **and** in the node budget
    /// (or the node holds nothing).
    pub fn try_hold_for(&self, tenant: &str, bytes: u64) -> Option<ReplayHold<'_>> {
        let cap = self.cap();
        if cap == 0 || bytes <= SMALL_JOURNAL.min(cap / SMALL_DIV) {
            self.used.fetch_add(bytes, Ordering::AcqRel);
            return Some(ReplayHold {
                budget: self,
                bytes,
                slot: None,
            });
        }
        let slot = slot_of(tenant);
        if !add_within(&self.tenants[slot], bytes, cap / TENANT_SHARE) {
            self.throttled.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if !add_within(&self.used, bytes, cap) {
            self.tenants[slot].fetch_sub(bytes, Ordering::AcqRel);
            self.throttled.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(ReplayHold {
            budget: self,
            bytes,
            slot: Some(slot),
        })
    }

    /// Hold `bytes` without a tenant (node limit only). `None` = not enough room.
    pub fn try_hold(&self, bytes: u64) -> Option<ReplayHold<'_>> {
        if add_within(&self.used, bytes, self.cap()) {
            Some(ReplayHold {
                budget: self,
                bytes,
                slot: None,
            })
        } else {
            self.throttled.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

/// The room one turn holds in the budget; given back on drop.
#[derive(Debug)]
pub struct ReplayHold<'a> {
    budget: &'a ReplayBudget,
    bytes: u64,
    /// The tenant slot added to (not a small turn).
    slot: Option<usize>,
}

impl Drop for ReplayHold<'_> {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        if let Some(slot) = self.slot {
            self.budget.tenants[slot].fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hold when there is room, refuse and count when there is not; given back on drop; with
    /// the budget empty a journal larger than the whole limit still runs; `0` = off.
    #[test]
    fn holds_and_refuses() {
        let b = ReplayBudget::new(100);
        let a = b.try_hold(60).expect("empty");
        assert!(b.try_hold(50).is_none(), "60 + 50 > 100");
        assert_eq!(b.throttled(), 1);
        let c = b.try_hold(40).expect("60 + 40 = 100");
        assert_eq!(b.used(), 100);
        drop(a);
        drop(c);
        assert_eq!(b.used(), 0);
        let big = b
            .try_hold(500)
            .expect("empty budget: never blocked forever");
        assert!(b.try_hold(1).is_none());
        drop(big);
        b.set_cap(0);
        let _x = b.try_hold(500).unwrap();
        let _y = b.try_hold(500).expect("off");
    }

    const MIB: u64 = 1 << 20;

    /// **Two tenants do not starve every other tenant.**
    /// Order: a 64 MiB limit; tenants `a` and `b` each open 16 MiB journal turns
    /// one after another (held until inactivity) until refused; then `c` comes with
    /// a small journal turn (512 KiB) and a large one (20 MiB).
    ///
    /// Must see: each tenant holds no more than ¼ of the budget (except for its first turn while
    /// it holds nothing); `c`'s small turn always runs; `c`'s large turn still has room.
    #[test]
    fn two_tenants_do_not_starve_a_third() {
        let b = ReplayBudget::new(64 * MIB);
        let mut held = Vec::new();
        for t in ["a", "b"] {
            while let Some(h) = b.try_hold_for(t, 16 * MIB) {
                held.push(h);
                assert!(held.len() < 16, "never refused");
            }
        }
        assert!(
            b.used() <= 64 * MIB / 2,
            "a and b hold {} MiB — more than ¼ each",
            b.used() / MIB
        );
        let small = b.try_hold_for("c", 512 * 1024);
        assert!(small.is_some(), "c's small turn was blocked");
        let big = b.try_hold_for("c", 20 * MIB);
        assert!(big.is_some(), "c's large turn was blocked");
    }

    /// A small journal turn (≤ 1 MiB) is admitted even when the budget is full; a tenant that
    /// holds nothing always gets one turn (a journal larger than its whole share).
    /// The small-turn threshold scales with the limit: an 8 MiB limit ⇒ 128 KiB.
    #[test]
    fn small_turns_and_first_turn_always_run() {
        let b = ReplayBudget::new(64 * MIB);
        let _full = b.try_hold_for("a", 64 * MIB).expect("empty budget");
        let _small = b.try_hold_for("b", MIB).expect("b's 1 MiB turn");
        assert!(
            b.try_hold_for("b", 3 * MIB).is_none(),
            "budget already full"
        );
        let solo = ReplayBudget::new(64 * MIB);
        let _first = solo
            .try_hold_for("a", 40 * MIB)
            .expect("a's first turn larger than its ¼ share still runs");
        assert!(
            solo.try_hold_for("a", 20 * MIB).is_none(),
            "second turn exceeds a's share"
        );
        let tight = ReplayBudget::new(8 * MIB);
        let _t = tight.try_hold_for("a", 8 * MIB).unwrap();
        let _s = tight.try_hold_for("b", 128 * 1024).expect("≤ cap/64");
        assert!(tight.try_hold_for("b", 256 * 1024).is_none(), "> cap/64");
    }
}
