// #[hot-path] — child file of `flow/mod.rs` (AGENTS.md §6); only compiled in tests.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flow sim: invariants FL-3/4/5/6/7/9/13 and liveness.

use super::*;

impl Sim {
    // ───────────────────────────────────────────────────────── invariants

    fn check_all(&mut self) -> Check {
        let wasted: u64 = self.ranges.iter().map(|rs| rs.core.stats.wasted).sum();
        let step_wasted = wasted - self.wasted;
        self.wasted = wasted;
        ensure!(
            step_wasted <= WASTED_PER_STEP,
            "FL-9: {step_wasted} wasted calls in one step (the called one blocked again at the same place)"
        );
        self.check_fl26_tenant_turns()?;
        for r in 0..self.ranges.len() {
            self.check_fl3_accounting(r)?;
            check_fl6_structure(&self.ranges[r].core)?;
            let core = &self.ranges[r].core;
            let rate_waiters = core
                .rm
                .waiters
                .iter()
                .any(|(k, w)| matches!(k, ResourceKind::Rate(_)) && w.live > 0);
            ensure!(
                !rate_waiters || core.rate_due.is_some(),
                "FL-12: someone waits for tokens but nobody set the rate clock"
            );
        }
        self.check_fl7_node()
    }

    /// FL-3 + FL-4: the core's counters, cooldown, turn slots and lock book equal exactly
    /// what the model derives from the live permits and the invocations holding locks.
    fn check_fl3_accounting(&self, r: usize) -> Check {
        let core = &self.ranges[r].core;
        let mut usage: HashMap<Counter, u32> = HashMap::default();
        let mut cool: HashMap<CoolKey, u32> = HashMap::default();
        let mut turns = 0usize;
        let mut locks: HashMap<LockName, InvocationId> = HashMap::default();
        for inv in self.invs.iter().filter(|i| i.range == r) {
            if let St::Granted { permit, .. } = &inv.st {
                turns += 1;
                for c in &permit.counters {
                    *usage.entry(c.clone()).or_insert(0) += 1;
                }
                if let Some(k) = &permit.cool {
                    *cool.entry(k.clone()).or_insert(0) += 1;
                }
            }
            if let Some(l) = &inv.lock {
                ensure!(
                    locks.insert(l.clone(), inv.id).is_none(),
                    "FL-4: two invocations hold lock {l:?}"
                );
            }
        }
        ensure!(
            core.rm.turns_in_use == turns,
            "FL-3: range {r} turns_in_use {} ≠ {turns} live permits",
            core.rm.turns_in_use
        );
        ensure!(
            core.rm.usage == usage,
            "FL-3: range {r} counters differ: core {:?} / model {usage:?}",
            core.rm.usage
        );
        ensure!(
            core.rm.cool_usage == cool,
            "FL-3: range {r} cooldown differs"
        );
        ensure!(
            core.rm.locks == locks,
            "FL-4: range {r} lock book differs: core {} locks / model {}",
            core.rm.locks.len(),
            locks.len()
        );
        Ok(())
    }

    /// FL-26: the per-tenant turns of each range equal exactly that tenant's live permits on
    /// the range; the node's counter is the sum over ranges; no tenant exceeds the cap (the
    /// sim runs sequentially — the cap is exact, not exceeded at all).
    fn check_fl26_tenant_turns(&self) -> Check {
        let mut node: HashMap<TenantId, u32> = HashMap::default();
        for (r, rs) in self.ranges.iter().enumerate() {
            let mut model: HashMap<TenantId, u32> = HashMap::default();
            for inv in self.invs.iter().filter(|i| i.range == r) {
                if let St::Granted { .. } = &inv.st {
                    *model.entry(inv.queue.tenant.clone()).or_insert(0) += 1;
                }
            }
            ensure!(
                rs.core.rm.tenant_turns == model,
                "FL-26: range {r} per-tenant turns differ: core {:?} / model {model:?}",
                rs.core.rm.tenant_turns
            );
            for (t, n) in model {
                *node.entry(t).or_insert(0) += n;
            }
        }
        // A meter already in one range's buffer — do not create a new meter (creating one
        // changes the tenant's starting `vt`, i.e. changes the sim itself).
        for rs in &self.ranges {
            for m in rs.core.rm.meters.borrow().values() {
                let sum = node.get(m.tenant()).copied().unwrap_or(0);
                ensure!(
                    m.turns() == sum,
                    "FL-26: node counts {} turns of {}, sum over ranges {sum}",
                    m.turns(),
                    m.tenant()
                );
            }
        }
        if let Some(cap) = self.k.tenant_cap {
            for (t, n) in &node {
                ensure!(*n as usize <= cap, "FL-26: {t} runs {n} turns, cap {cap}");
            }
        }
        Ok(())
    }

    /// FL-7: the node's counter is the sum of the live cells; the starving flags match; the
    /// total does not exceed the cap except for guaranteed shares granted while the node
    /// leads too many ranges.
    fn check_fl7_node(&self) -> Check {
        if self.budget.is_none() {
            return Ok(());
        }
        let probes: Vec<_> = self
            .ranges
            .iter()
            .map(|rs| rs.core.rm.node.as_ref().expect("attached to node").probe())
            .collect();
        let n = &probes[0];
        let sum: usize = self.ranges.iter().map(|rs| rs.core.rm.turns_in_use).sum();
        ensure!(
            n.in_use == sum,
            "FL-7: node counts {} turns, sum over ranges {sum}",
            n.in_use
        );
        let starving = self.ranges.iter().filter(|rs| rs.core.rm.starving).count();
        ensure!(
            n.starving == starving,
            "FL-7: node counts {} starving ranges, actual {starving}",
            n.starving
        );
        for (r, (rs, p)) in self.ranges.iter().zip(&probes).enumerate() {
            ensure!(
                p.mine == rs.core.rm.turns_in_use,
                "FL-7: the range's cell differs from the scheduler"
            );
            ensure!(
                p.mine_starving == rs.core.rm.starving,
                "FL-7: the cell's starving flag differs"
            );
            // A starving flag blocks every other range from borrowing: it may only stand
            // while the range still has slot waiters, while a "node changed" message — the
            // flag-clearing tick of `node_turns_changed` — is on its way, or while the node
            // is full (nobody can borrow; the next release takes the node from full to having
            // room and notifies every range). `set_rules` leaves such a flag when a slot
            // waiter moves to blocking elsewhere — the node is always full at that moment.
            let waiting = rs
                .core
                .rm
                .waiters
                .get(&ResourceKind::Turns)
                .is_some_and(|w| w.live > 0);
            ensure!(
                !rs.core.rm.starving || waiting || rs.changed || p.in_use >= p.total,
                "FL-7: range {r} starving with no slot waiter and no node message coming — every other range stops borrowing (node {}/{}, {} ranges led, range {}/{})",
                p.in_use,
                p.total,
                p.leading,
                rs.core.rm.turns_in_use,
                rs.core.rm.max_turns
            );
        }
        let oversub = self
            .invs
            .iter()
            .filter(|i| matches!(i.st, St::Granted { oversub: true, .. }))
            .count();
        ensure!(
            n.in_use <= n.total + oversub,
            "FL-7: node runs {} turns > cap {} (+{oversub} granted while over-leading ranges)",
            n.in_use,
            n.total
        );
        Ok(())
    }

    // ───────────────────────────────────────────────────────── liveness

    /// Stop the faults; run every turn until nobody is waiting. Every invocation must
    /// finish within [`DRAIN_ROUNDS`] rounds, then every counter returns to 0 (FL-3).
    fn drain(&mut self) -> Check {
        self.note("— liveness phase —".into());
        // Unsuspend every tenant: the waiters on `Tenant` must be woken.
        self.suspended.clear();
        self.push_suspended()?;
        for _ in 0..DRAIN_ROUNDS {
            for r in 0..self.ranges.len() {
                if self.ranges[r].changed {
                    self.deliver(r)?;
                }
            }
            let mut moved = false;
            for i in 0..self.invs.len() {
                if let St::Granted { permit, begun, .. } = &mut self.invs[i].st {
                    if !*begun {
                        permit.begin();
                        *begun = true;
                    }
                    self.end(i, true, 1)?;
                    moved = true;
                } else if matches!(self.invs[i].st, St::Idle | St::Deferred { .. })
                    && !self.invs[i].done
                {
                    self.request(i)?;
                    moved = true;
                } else {
                    continue;
                }
                self.check_all()?;
            }
            if self.invs.iter().all(|i| i.done) {
                return self.check_quiescent();
            }
            if !moved {
                // Only waiters left: time advances to the earliest deadline of the rate tier
                // (or a sweep tick).
                let now = self.now();
                let due = self
                    .ranges
                    .iter()
                    .filter_map(|rs| rs.core.rate_due())
                    .min()
                    .map_or(100_000, |d| {
                        d.saturating_duration_since(now).as_micros() as u64
                    });
                let dt = due.clamp(1, 1_000_000);
                self.clock_us += dt;
                let now = self.now();
                if self.clock_us - self.last_sweep_us >= crate::rate::SWEEP_EVERY.as_micros() as u64
                {
                    self.last_sweep_us = self.clock_us;
                    self.book.sweep(now, false);
                }
                for r in 0..self.ranges.len() {
                    if self.ranges[r].core.rate_due().is_some_and(|d| d <= now) {
                        let acts = self.ranges[r].core.rate_tick(now);
                        self.absorb(r, acts)?;
                    }
                }
                self.check_all()?;
            }
        }
        let stuck: Vec<String> = self
            .invs
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.done)
            .map(|(n, i)| {
                let blocked = self.ranges[i.range]
                    .core
                    .queues
                    .get(&i.key)
                    .and_then(|q| q.blocked_on.clone());
                format!("inv{n} {} blocked on {blocked:?}", i.queue)
            })
            .collect();
        Err(format!(
            "liveness: still stuck after {DRAIN_ROUNDS} rounds: {stuck:?}"
        ))
    }

    fn check_quiescent(&self) -> Check {
        for (r, rs) in self.ranges.iter().enumerate() {
            ensure!(
                rs.core.usage_is_empty(),
                "FL-3: range {r} still has counters after every turn finished"
            );
            ensure!(
                rs.core.rm.locks.is_empty(),
                "FL-4: range {r} still has locks after every invocation finished"
            );
            // A queue with only dead entries (≤ 32, below the sweep threshold) stays until
            // the resource it blocks on has room — cleared gradually, bounded (FL-6).
            for q in rs.core.queues.values() {
                ensure!(
                    q.live() == 0 && q.dead.len() <= 32 && q.blocked_on.is_some(),
                    "FL-6: range {r} still has queue {} ({} live, {} dead, blocked on {:?}) with nobody waiting",
                    q.name,
                    q.live(),
                    q.dead.len(),
                    q.blocked_on
                );
            }
        }
        if let Some(rs) = self.ranges.first() {
            if let Some(n) = &rs.core.rm.node {
                let p = n.probe();
                ensure!(p.in_use == 0, "FL-7: node still has {} turns", p.in_use);
            }
        }
        Ok(())
    }

    pub(super) fn run(&mut self) -> Check {
        for step in 0..self.k.steps {
            self.step = step;
            self.event()?;
            self.check_all()?;
        }
        self.step = self.k.steps;
        self.drain()
    }

    pub(super) fn report(&self, err: &str) -> String {
        let mut s = format!(
            "seed {} red at step {}: {err}\nknobs: {:?}\nlast trace:\n",
            self.seed, self.step, self.k
        );
        for line in &self.trace {
            let _ = writeln!(s, "  {line}");
        }
        let _ = write!(
            s,
            "reproduce: CAIRN_FLOW_SIM_SEED={} cargo test -p cairn-server --lib flow::sim",
            self.seed
        );
        s
    }
}

/// FL-7 + FL-13 at the moment of decision, from [`Seen`]: the guaranteed share when the
/// node has room (or is over-leading ranges); borrowing when the node has room, no range is
/// starving, and `vt` is not far ahead of `min_vt`; a slot called from the `Turns` wait line
/// goes to the tenant with the smallest `vt`.
pub(super) fn check_grant_turns(seen: &Seen) -> Check {
    match &seen.node {
        None => ensure!(
            seen.turns_before < seen.max_turns,
            "FL-7: turn granted {} with cap {}",
            seen.turns_before + 1,
            seen.max_turns
        ),
        Some(n) if seen.turns_before < seen.max_turns => ensure!(
            n.in_use < n.total || n.leading > n.total,
            "FL-7: guaranteed share granted while the node is full ({}/{})",
            n.in_use,
            n.total
        ),
        Some(n) => {
            ensure!(n.in_use < n.total, "FL-7: borrowed while the node is full");
            ensure!(n.starving == 0, "FL-7: borrowed while a range is starving");
            ensure!(
                seen.vt <= seen.min_vt.saturating_add(BORROW_SLACK_VT),
                "FL-13: borrowed with vt {} > min_vt {} + slack",
                seen.vt,
                seen.min_vt
            );
        }
    }
    if let Some(min) = seen.from_turns {
        ensure!(
            seen.vt <= min,
            "FL-13: free slot went to a tenant with vt {} while a tenant waits with vt {min}",
            seen.vt
        );
    }
    Ok(())
}

/// FL-6: after askers leave, a queue's dead entries do not exceed its live entries by more
/// than 32 (cleared gradually).
pub(super) fn check_fl6_dead(core: &Core<u64>, key: &[u8]) -> Check {
    if let Some(q) = core.queues.get(key) {
        ensure!(
            q.dead.len() <= 32 || 2 * q.dead.len() <= q.waiting.len(),
            "FL-6: {} dead entries / {} entries in the queue",
            q.dead.len(),
            q.waiting.len()
        );
    }
    Ok(())
}

/// FL-6 + FL-9, structure of the core after an event:
/// - the eligibility loop is drained (`dispatch` runs until it can make no progress);
/// - every queue that still has live waiters is blocked on a resource **and** has a live
///   entry in that resource's wait list — otherwise no release would ever call it again;
/// - the number of live waiters of each wait list equals the number of queues blocked
///   there;
/// - no empty queue stays in the table.
fn check_fl6_structure(core: &Core<u64>) -> Check {
    ensure!(
        core.ring.is_empty() && core.in_ring.is_empty(),
        "FL-6: the eligibility loop still has {} queues after the event",
        core.ring.len()
    );
    let mut per_res: HashMap<ResourceKind, usize> = HashMap::default();
    for (key, q) in &core.queues {
        let name = &q.name;
        ensure!(
            !q.waiting.is_empty(),
            "FL-6: empty queue {name} still in the table"
        );
        match &q.blocked_on {
            None => ensure!(
                q.live() == 0,
                "FL-6: queue {name} has {} waiters but is blocked nowhere and not in the loop",
                q.live()
            ),
            Some(res) => {
                *per_res.entry(res.clone()).or_insert(0) += 1;
                let list = core.rm.waiters.get(res);
                let listed = list.is_some_and(|l| {
                    l.entries.iter().any(|(k, t)| k == key && *t == q.ticket)
                        || l.fair
                            .by_tenant
                            .values()
                            .any(|dq| dq.iter().any(|(k, t)| k == key && *t == q.ticket))
                });
                ensure!(
                    listed,
                    "FL-6: queue {name} blocked on {res:?} with no live entry there"
                );
            }
        }
    }
    for (res, list) in &core.rm.waiters {
        let want = per_res.get(res).copied().unwrap_or(0);
        ensure!(
            list.live == want,
            "FL-9: wait list {res:?} counts {} live waiters, actual {want}",
            list.live
        );
    }
    Ok(())
}

pub(super) fn outcome(o: AcquireOutcome) -> String {
    match o {
        AcquireOutcome::Acquired(_) => "acquired".into(),
        AcquireOutcome::BlockedOn(r) => format!("blocked {r:?}"),
    }
}

/// The core's resource state — what `poll_acquire` must not change.
pub(super) fn fingerprint(core: &Core<u64>) -> String {
    let mut usage: Vec<String> = core
        .rm
        .usage
        .iter()
        .map(|(c, n)| format!("{c}={n}"))
        .collect();
    usage.sort();
    let mut cool: Vec<String> = core
        .rm
        .cool_usage
        .iter()
        .map(|(c, n)| format!("{c:?}={n}"))
        .collect();
    cool.sort();
    let mut locks: Vec<String> = core
        .rm
        .locks
        .iter()
        .map(|(l, h)| format!("{l:?}={h:?}"))
        .collect();
    locks.sort();
    format!(
        "{usage:?} {cool:?} {locks:?} {} {}",
        core.rm.turns_in_use, core.rm.starving
    )
}
