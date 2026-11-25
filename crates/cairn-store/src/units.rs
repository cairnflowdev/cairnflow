// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Payload-weighted billing units, in one range's RAM** (the same shape as the
//! per-"action" billing of hosted durable-execution services).
//!
//! A durable command that became durable counts `max(1, ⌈payload / 64 KiB⌉)` units
//! ([`of_payload`]); each invocation adds 2. Classifying entries happens in the engine
//! (`cairn_engine::units`); here there is only the addition and the "count when durable"
//! rule:
//!
//! - **Counted in one place, at commit.** Whoever builds a batch attaches the unit counts
//!   to the very [`crate::Mutations`] that carries the rows producing them
//!   ([`crate::Mutations::meter_units`]). If the batch is submitted, the writer hands them
//!   to the range's [`UnitMeter`] together with the batch's `seq`; if submission fails,
//!   they fall with the batch. No batch is submitted twice, so no entry is counted twice.
//! - **Charged only when durable.** The counts wait until the store's `durable_seq`
//!   passes the batch's `seq`. A batch that was submitted but did not become durable in
//!   time (node died, fenced) is lost together with its journal — the redo at the new
//!   leader is the only count. Without this rule, a death between submit and durable
//!   would count the entry twice (once in the dead incarnation, once in the redo).
//! - **By the time of the commit** (`entropy`), not the time of the flush.
//!
//! Read out with [`collect`] (the node's metering flusher, `cairn_server::metering`) —
//! it drains the durable part of every live range in the process, plus what closed
//! ranges left behind ([`UnitMeter`] `Drop`). Each range's lock belongs to that range (the
//! engine thread writes, the flusher reads every tick); the global registry is touched
//! only when a range opens/closes and at flush time, not on the turn path.
//!
//! Nothing is written durably: an error here is at worst a skewed count, never data.
//! **Limitation** (like the metering `steps`): a process death loses what was not yet
//! flushed — at most one metering flush tick.

use {
    crate::writer::StoreStatus,
    std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex, MutexGuard, Weak},
    },
};

/// Bytes of one unit: each payload chunk that starts a new 64 KiB block adds 1.
pub const UNIT_BYTES: u64 = 64 * 1024;

/// `max(1, ⌈len / 64 KiB⌉)` — the units of a command carrying `len` payload bytes.
pub const fn of_payload(len: usize) -> u64 {
    let n = (len as u64).div_ceil(UNIT_BYTES);
    if n == 0 {
        1
    } else {
        n
    }
}

/// The part **beyond** one unit: `of_payload(len) − 1`. An invocation's result counts as
/// the payload of its completion — the 1 completion unit is added on the write of the
/// completed status, the surplus on the `Output` entry.
pub const fn surplus(len: usize) -> u64 {
    of_payload(len) - 1
}

/// Hour index (hours since 1970-01-01T00 UTC) of now, by `entropy`.
pub fn hour_now() -> u64 {
    crate::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() / 3_600)
        .unwrap_or(0)
}

/// `(tenant, hour index)` → units.
pub type Tally = BTreeMap<(String, u64), u64>;

/// A submitted, not yet durable batch: `(seq, hour, [(tenant, units)])`.
type Submitted = (u64, u64, Vec<(String, u64)>);

#[derive(Default)]
struct Inner {
    /// Submitted, not yet durable batches, by increasing seq.
    pending: VecDeque<Submitted>,
    /// Durable, not yet drained.
    ready: Tally,
}

impl Inner {
    /// Moves every batch with `seq ≤ durable` to `ready`.
    fn promote(&mut self, durable: u64) {
        while self
            .pending
            .front()
            .is_some_and(|(seq, _, _)| *seq <= durable)
        {
            let Some((_, hour, units)) = self.pending.pop_front() else {
                break;
            };
            for (tenant, n) in units {
                *self.ready.entry((tenant, hour)).or_default() += n;
            }
        }
    }
}

/// One range's units — see the module doc. The writer holds an `Arc` (attached with
/// `PartitionWriter::enable_units`); the process registry holds a `Weak`.
pub struct UnitMeter {
    status: StoreStatus,
    inner: Mutex<Inner>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic midway only leaves a half-updated count — still readable.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Meters of the ranges live in the process.
static LIVE: Mutex<Vec<Weak<UnitMeter>>> = Mutex::new(Vec::new());
/// Durable part of closed ranges, waiting for the next drain.
static RETIRED: Mutex<Tally> = Mutex::new(BTreeMap::new());

impl UnitMeter {
    /// A new meter on a store with status `status`, registered in the process registry.
    pub(crate) fn new(status: StoreStatus) -> Arc<Self> {
        let m = Arc::new(Self {
            status,
            inner: Mutex::default(),
        });
        let mut live = lock(&LIVE);
        live.retain(|w| w.strong_count() > 0);
        live.push(Arc::downgrade(&m));
        m
    }

    /// The batch at `seq` was submitted, carrying `units` — on the engine thread, right
    /// after `db.write`. The range's lock, not a shared lock.
    pub(crate) fn committed(&self, seq: u64, hour: u64, units: Vec<(String, u64)>) {
        let durable = self.status.durable_seq();
        let mut inner = lock(&self.inner);
        inner.pending.push_back((seq, hour, units));
        inner.promote(durable);
    }

    /// Drains this range's durable part.
    pub fn take(&self) -> Tally {
        let durable = self.status.durable_seq();
        let mut inner = lock(&self.inner);
        inner.promote(durable);
        std::mem::take(&mut inner.ready)
    }

    /// Units submitted but not yet durable (for tests).
    pub fn pending(&self) -> u64 {
        lock(&self.inner)
            .pending
            .iter()
            .flat_map(|(_, _, u)| u.iter().map(|(_, n)| *n))
            .sum()
    }
}

impl Drop for UnitMeter {
    /// The range closes (clean shutdown, lease lost, split/merge retiring the parent): the
    /// durable part goes to the process registry; the non-durable part is dropped — its
    /// journal may not survive.
    fn drop(&mut self) {
        let left = self.take();
        if left.is_empty() {
            return;
        }
        let mut retired = lock(&RETIRED);
        for (k, n) in left {
            *retired.entry(k).or_default() += n;
        }
    }
}

fn add_into(into: &mut Tally, from: Tally) {
    for (k, n) in from {
        *into.entry(k).or_default() += n;
    }
}

/// Drains the durable part of **every** range in the process (live and closed).
pub fn collect() -> Tally {
    let live: Vec<Arc<UnitMeter>> = {
        let mut live = lock(&LIVE);
        live.retain(|w| w.strong_count() > 0);
        live.iter().filter_map(Weak::upgrade).collect()
    };
    let mut out = std::mem::take(&mut *lock(&RETIRED));
    for m in live {
        add_into(&mut out, m.take());
    }
    out
}

/// Like [`collect`], but drains only `tenant`'s part — tests running in parallel in one
/// process do not drain each other's counts.
pub fn collect_tenant(tenant: &str) -> Tally {
    let mut out = Tally::new();
    let mut keep = |all: Tally, back: &mut dyn FnMut(Tally)| {
        let (mine, rest): (Tally, Tally) = all.into_iter().partition(|((t, _), _)| t == tenant);
        add_into(&mut out, mine);
        back(rest);
    };
    let live: Vec<Arc<UnitMeter>> = {
        let mut live = lock(&LIVE);
        live.retain(|w| w.strong_count() > 0);
        live.iter().filter_map(Weak::upgrade).collect()
    };
    {
        let mut retired = lock(&RETIRED);
        let all = std::mem::take(&mut *retired);
        keep(all, &mut |rest| add_into(&mut retired, rest));
    }
    for m in live {
        let all = m.take();
        keep(all, &mut |rest| add_into(&mut lock(&m.inner).ready, rest));
    }
    out
}

/// Drops every count of the process — deterministic simulation runs several clusters in
/// one process (like `metering::reset_for_sim`).
#[cfg(any(test, feature = "testkit"))]
pub fn reset() {
    let live: Vec<Arc<UnitMeter>> = lock(&LIVE).iter().filter_map(Weak::upgrade).collect();
    for m in live {
        let mut inner = lock(&m.inner);
        inner.pending.clear();
        inner.ready.clear();
    }
    lock(&RETIRED).clear();
}

#[cfg(test)]
mod tests {
    use {super::*, crate::writer::ModelStatus, tokio::sync::watch};

    #[test]
    fn unit_of_payload_boundaries() {
        assert_eq!(of_payload(0), 1);
        assert_eq!(of_payload(1), 1);
        assert_eq!(of_payload(64 * 1024), 1);
        assert_eq!(of_payload(64 * 1024 + 1), 2);
        assert_eq!(of_payload(100 * 1024), 2);
        assert_eq!(of_payload(1024 * 1024), 16);
        assert_eq!(of_payload(1024 * 1024 + 1), 17);
        assert_eq!(surplus(0), 0);
        assert_eq!(surplus(64 * 1024), 0);
        assert_eq!(surplus(64 * 1024 + 1), 1);
        assert_eq!(surplus(1024 * 1024), 15);
    }

    fn meter() -> (watch::Sender<ModelStatus>, Arc<UnitMeter>) {
        let (tx, rx) = watch::channel(ModelStatus::default());
        (tx, UnitMeter::new(StoreStatus::Model(rx)))
    }

    /// **Charged only when durable**: batches submitted at seq 2 and 3, durable up to 2 ⇒
    /// only batch 2; durable up to 3 ⇒ both; draining twice does not double count.
    #[test]
    fn counts_only_durable_batches_once() {
        let t = "units-durable";
        let (tx, m) = meter();
        m.committed(2, 7, vec![(t.into(), 3)]);
        m.committed(3, 7, vec![(t.into(), 5)]);
        assert!(m.take().is_empty());
        assert_eq!(m.pending(), 8);
        tx.send_modify(|s| s.durable_seq = 2);
        assert_eq!(m.take(), Tally::from([((t.into(), 7), 3)]));
        tx.send_modify(|s| s.durable_seq = 3);
        assert_eq!(m.take(), Tally::from([((t.into(), 7), 5)]));
        assert!(m.take().is_empty());
    }

    /// **A closed range**: the durable part goes to the process registry, the non-durable
    /// part is dropped (the next incarnation redoes it and counts it once).
    #[test]
    fn dropped_meter_retires_durable_part_only() {
        let t = "units-retired";
        let (tx, m) = meter();
        m.committed(1, 9, vec![(t.into(), 4)]);
        m.committed(2, 9, vec![(t.into(), 6)]);
        tx.send_modify(|s| s.durable_seq = 1);
        drop(m);
        assert_eq!(collect_tenant(t), Tally::from([((t.into(), 9), 4)]));
        assert!(collect_tenant(t).is_empty());
    }

    /// `collect_tenant` does not drain other tenants' counts.
    #[test]
    fn collect_tenant_leaves_other_tenants() {
        let (a, b) = ("units-a", "units-b");
        let (tx, m) = meter();
        m.committed(1, 1, vec![(a.into(), 1), (b.into(), 2)]);
        tx.send_modify(|s| s.durable_seq = 1);
        assert_eq!(collect_tenant(a), Tally::from([((a.into(), 1), 1)]));
        assert_eq!(collect_tenant(b), Tally::from([((b.into(), 1), 2)]));
    }
}
