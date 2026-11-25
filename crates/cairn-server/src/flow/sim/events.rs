// #[hot-path] — child file of `flow/mod.rs` (AGENTS.md §6); only compiled in tests.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flow sim: data generation, events, and the core's outputs.

use super::*;

impl Sim {
    // ───────────────────────────────────────────────────────── data generation

    pub(super) fn draw_rules(&mut self) -> RuleBook {
        let mut b = RuleBook::default();
        b.version = self.rules.version + 1;
        if self.k.limits {
            for p in ["*", "*/*", "*/*/*", "s0"] {
                if self.chance(500) {
                    let n = 1 + self.below(4) as u32;
                    b.set(p, Some(Limit::Max(n)), None, None, 0).unwrap();
                }
            }
        }
        if self.k.counter_rates && self.chance(700) {
            let p = ["*", "s1", "*/*"][self.below(3) as usize];
            let edit = RuleEdit {
                rate: Some(Limit::Max(1 + self.below(30) as u32)),
                burst: Some(1 + self.below(5) as u32),
                ..Default::default()
            };
            b.apply(p, edit, 0).unwrap();
        }
        if self.k.tenant_rates {
            for t in 0..self.k.tenants {
                if self.chance(500) {
                    let edit = RuleEdit {
                        rate: Some(Limit::Max(2 + self.below(60) as u32)),
                        burst: Some(1 + self.below(8) as u32),
                        ..Default::default()
                    };
                    b.apply_tenant(&format!("t{t}"), edit, 0).unwrap();
                }
            }
        }
        b
    }

    /// A random queue and its range. The scope decides the range (D-F1): every queue of the
    /// same `(tenant, scope)` — same counter, same lock — is on one range; queues without a
    /// scope go by object key.
    fn draw_queue(&mut self) -> (QueueName, usize) {
        let t = self.below(self.k.tenants as u64) as usize;
        let scope = (self.k.scopes > 0 && !self.chance(250))
            .then(|| self.below(self.k.scopes as u64) as usize);
        let exclusive = self.chance(self.k.exclusive);
        let obj = self.below(self.k.objects as u64) as usize;
        let limit = match (scope, self.below(4)) {
            (None, _) | (_, 0) => None,
            (_, 1) => Some("l0"),
            (_, 2) => Some("l0/m0"),
            _ => Some("l1/m1"),
        };
        let q = QueueName {
            tenant: TenantId::new(&format!("t{t}")).unwrap(),
            scope: scope.map(|s| Scope::new(&format!("s{s}")).unwrap()),
            limit: limit.map(|l| LimitKey::parse(l).unwrap()),
            exclusive,
            service: if exclusive { "Obj" } else { "Svc" }.into(),
            object_key: if exclusive {
                format!("k{obj}")
            } else {
                String::new()
            },
        };
        let range = match scope {
            Some(s) => (t * 7 + s) % self.k.ranges,
            None => (t + obj) % self.k.ranges,
        };
        (q, range)
    }

    fn new_inv(&mut self) -> usize {
        let (queue, range) = self.draw_queue();
        let key = queue.encode();
        let seq = {
            let s = self.next_seq.entry(key.clone()).or_insert(0);
            *s += 1;
            *s
        };
        let cool = (self.k.cooldown && self.chance(300)).then(|| CoolKey {
            tenant: queue.tenant.to_string(),
            service: queue.service.clone(),
            handler: "boom".into(),
            deployment: "dp_1".into(),
        });
        let n = self.next_inv;
        self.next_inv += 1;
        let mut id = [0u8; 24];
        id[..8].copy_from_slice(&n.to_be_bytes());
        self.invs.push(Inv {
            id,
            queue,
            key,
            range,
            seq,
            cool,
            st: St::Idle,
            lock: None,
            done: false,
        });
        self.invs.len() - 1
    }

    /// A random index among the invocations satisfying `f`.
    fn pick(&mut self, f: impl Fn(&Inv) -> bool) -> Option<usize> {
        let ids: Vec<usize> = (0..self.invs.len()).filter(|&i| f(&self.invs[i])).collect();
        if ids.is_empty() {
            return None;
        }
        let i = self.below(ids.len() as u64) as usize;
        Some(ids[i])
    }

    // ───────────────────────────────────────────────────────── events

    pub(super) fn event(&mut self) -> Check {
        let total: u64 = self.k.weights.iter().sum();
        let mut x = self.below(total);
        let mut ev = Ev::Time;
        for (i, w) in self.k.weights.iter().enumerate() {
            if x < *w {
                ev = EVENTS[i];
                break;
            }
            x -= w;
        }
        if self.nacks && self.chance(150) {
            // The granted one's turn did not win the invocation lock.
            if let Some(i) = self.pick(|i| matches!(i.st, St::Granted { begun: false, .. })) {
                return self.nack(i);
            }
        }
        match ev {
            Ev::Request => {
                let live = self.invs.iter().filter(|i| !i.done).count();
                let idle = self.pick(|i| !i.done && matches!(i.st, St::Idle | St::Deferred { .. }));
                let i = match idle {
                    Some(i) if live >= self.k.max_live || self.chance(500) => i,
                    _ if live < self.k.max_live => self.new_inv(),
                    _ => return Ok(()),
                };
                self.request(i)
            }
            Ev::Cancel => match self.pick(|i| matches!(i.st, St::Waiting(_))) {
                Some(i) => self.cancel(i),
                None => Ok(()),
            },
            Ev::GiveBack => match self.pick(|i| matches!(i.st, St::Granted { begun: false, .. })) {
                Some(i) => self.give_back(i),
                None => Ok(()),
            },
            Ev::Begin => {
                if let Some(i) = self.pick(|i| matches!(i.st, St::Granted { begun: false, .. })) {
                    if let St::Granted { permit, begun, .. } = &mut self.invs[i].st {
                        permit.begin();
                        *begun = true;
                    }
                    self.note(format!("begin inv{i}"));
                }
                Ok(())
            }
            Ev::End => match self.pick(|i| matches!(i.st, St::Granted { begun: true, .. })) {
                Some(i) => {
                    let terminal = self.chance(500);
                    let cu = self.below(5);
                    self.end(i, terminal, cu)
                }
                None => Ok(()),
            },
            Ev::Time => {
                let dt =
                    [0, 300, 1_000, 10_000, 50_000, 200_000, 1_000_000][self.below(7) as usize];
                let jitter = self.below(dt / 2 + 1);
                self.advance(dt + jitter)
            }
            Ev::Rules => {
                if self.chance(300) {
                    self.rules = self.draw_rules();
                }
                let r = self.below(self.ranges.len() as u64) as usize;
                let now = self.now();
                let book = self.rules.clone();
                self.note(format!("rules v{} → range {r}", book.version));
                let acts = self.ranges[r].core.set_rules(book, now);
                self.absorb(r, acts)
            }
            Ev::Admin => {
                // Admin cancel holds the invocation's lock (`ctx.locks.invocation`),
                // so it only touches invocations with no turn waiting or running.
                match self.pick(|i| {
                    !i.done && matches!(i.st, St::Idle | St::Deferred { .. }) && i.lock.is_some()
                }) {
                    Some(i) => self.admin_cancel(i),
                    None => Ok(()),
                }
            }
            Ev::Reset => {
                let r = self.below(self.ranges.len() as u64) as usize;
                self.reset(r)
            }
            Ev::Lead => {
                let Some(b) = self.budget.clone() else {
                    return Ok(());
                };
                if self.phantoms.len() < 6 && (self.phantoms.is_empty() || self.chance(500)) {
                    self.phantoms.push(b.lead());
                } else {
                    self.phantoms.pop();
                }
                self.note(format!("node: {} other ranges led", self.phantoms.len()));
                self.mark_changed();
                Ok(())
            }
            Ev::Deliver => {
                let ready: Vec<usize> = (0..self.ranges.len())
                    .filter(|&r| self.ranges[r].changed)
                    .collect();
                if ready.is_empty() {
                    return Ok(());
                }
                let r = ready[self.below(ready.len() as u64) as usize];
                self.deliver(r)
            }
            Ev::Probe => match self.pick(|i| !matches!(i.st, St::Granted { .. })) {
                Some(i) => self.probe(i),
                None => Ok(()),
            },
            Ev::Suspend => {
                let t = TenantId::new(&format!("t{}", self.below(self.k.tenants as u64))).unwrap();
                let on = !self.suspended.remove(&t);
                if on {
                    self.suspended.insert(t.clone());
                }
                self.note(format!("{} {t}", if on { "suspend" } else { "unsuspend" }));
                self.push_suspended()
            }
        }
    }

    /// Push the current suspended set into the core of every range (shell:
    /// `Scheduler::sync_tenants`).
    pub(super) fn push_suspended(&mut self) -> Check {
        let now = self.now();
        let mut out = Ok(());
        for r in 0..self.ranges.len() {
            let acts = self.ranges[r].core.set_refused(self.suspended.clone(), now);
            let res = self.absorb(r, acts);
            if out.is_ok() {
                out = res;
            }
        }
        out
    }

    pub(super) fn request(&mut self, i: usize) -> Check {
        let req = self.next_req;
        self.next_req += 1;
        let now = self.now();
        let inv = &self.invs[i];
        let r = inv.range;
        let (asked, acts) =
            self.ranges[r]
                .core
                .request(&inv.queue, inv.id, inv.seq, inv.cool.clone(), req, now);
        let holds = inv.lock.is_some() && inv.queue.exclusive;
        self.reqs.insert(
            req,
            Req {
                inv: i,
                range: r,
                key: inv.key.clone(),
                seq: inv.seq,
                core_req: asked.req,
                held_lock: asked.held_lock,
                live: true,
            },
        );
        self.invs[i].st = St::Waiting(req);
        self.note(format!(
            "request inv{i} seq {} range {r} req {req} {} holds lock {}",
            self.invs[i].seq, self.invs[i].queue, asked.held_lock
        ));
        // FL-4: the core and the model agree on who holds the lock.
        ensure!(
            asked.held_lock == holds,
            "FL-4: the core says inv{i} holds the lock = {}, the model says {holds}",
            asked.held_lock
        );
        self.absorb(r, acts)
    }

    fn cancel(&mut self, i: usize) -> Check {
        let St::Waiting(req) = self.invs[i].st else {
            return Ok(());
        };
        let rq = self.reqs.get_mut(&req).expect("waiting request");
        rq.live = false;
        let (r, key, core_req) = (rq.range, rq.key.clone(), rq.core_req);
        self.invs[i].st = St::Idle;
        // Half the time: withdraw right away like a new ask replacing a parked ticket
        // (`Core::withdraw`) — the asker returned is exactly this request.
        if self.chance(500) {
            let now = self.now();
            let (to, acts) = self.ranges[r].core.withdraw(&key, core_req, now);
            ensure!(
                to == Some(req),
                "withdraw inv{i}: the core returned {to:?}, the model expected request {req}"
            );
            self.note(format!("withdraw inv{i} req {req}"));
            self.absorb(r, acts)?;
            return check_fl6_dead(&self.ranges[r].core, &key);
        }
        self.ranges[r].core.cancel(&key, core_req);
        self.note(format!("cancel inv{i} req {req}"));
        check_fl6_dead(&self.ranges[r].core, &key)
    }

    /// **Nack**: `i`'s unused slot is returned through `Core::nack`; `i` waits for the
    /// invocation lock to be released, then asks again with the same `seq`. Model: the ticket
    /// of an EXCLUSIVE queue **keeps the VO lock** across the nack (FL-5 — the lock holder
    /// is at the head of the re-ask); other queues return it by `held_before`.
    fn nack(&mut self, i: usize) -> Check {
        let St::Granted {
            req, mut permit, ..
        } = std::mem::replace(&mut self.invs[i].st, St::Idle)
        else {
            return Ok(());
        };
        let held = self.reqs.get(&req).is_some_and(|q| q.held_lock);
        self.invs[i].st = St::Deferred { held };
        let r = self.invs[i].range;
        let now = self.now();
        self.note(format!("nack inv{i} held lock before {held}"));
        let acts = self.ranges[r].core.nack(&mut permit, held, now);
        if !held && !self.invs[i].queue.exclusive {
            self.invs[i].lock = None;
        }
        self.absorb(r, acts)
    }

    fn give_back(&mut self, i: usize) -> Check {
        let St::Granted {
            req, mut permit, ..
        } = std::mem::replace(&mut self.invs[i].st, St::Idle)
        else {
            return Ok(());
        };
        let held = self.reqs.get(&req).is_some_and(|q| q.held_lock);
        let r = self.invs[i].range;
        let now = self.now();
        self.note(format!("give_back inv{i} held lock before {held}"));
        let acts = self.ranges[r].core.give_back(&mut permit, held, now);
        if !held {
            // Like `release_all`: the lock leaves the invocation.
            self.invs[i].lock = None;
        }
        self.absorb(r, acts)
    }

    /// End of turn: settle the CU **before** releasing (FL-12), then suspend (keep the
    /// lock) or finish (release everything).
    pub(super) fn end(&mut self, i: usize, terminal: bool, cu: u64) -> Check {
        let St::Granted { mut permit, .. } = std::mem::replace(&mut self.invs[i].st, St::Idle)
        else {
            return Ok(());
        };
        let r = self.invs[i].range;
        let now = self.now();
        self.ranges[r].core.settle(&permit, cu, now);
        self.note(format!("end inv{i} cu {cu} terminal {terminal}"));
        let acts = if terminal {
            let acts = self.ranges[r].core.release_all(&mut permit, now);
            self.invs[i].lock = None;
            self.invs[i].done = true;
            acts
        } else {
            self.ranges[r].core.release_running(&mut permit, now)
        };
        self.absorb(r, acts)
    }

    fn admin_cancel(&mut self, i: usize) -> Check {
        let Some(lock) = self.invs[i].lock.take() else {
            return Ok(());
        };
        self.invs[i].done = true;
        // A nacked ticket of a cancelled invocation is no longer waiting for anything.
        self.invs[i].st = St::Idle;
        let r = self.invs[i].range;
        let now = self.now();
        self.note(format!("admin cancel inv{i}"));
        let acts = self.ranges[r].core.release_lock(lock, self.invs[i].id, now);
        self.absorb(r, acts)
    }

    fn advance(&mut self, dt_us: u64) -> Check {
        self.clock_us += dt_us;
        let now = self.now();
        if self.clock_us - self.last_sweep_us >= crate::rate::SWEEP_EVERY.as_micros() as u64 {
            self.last_sweep_us = self.clock_us;
            self.book.sweep(now, false);
        }
        for r in 0..self.ranges.len() {
            // `rate_loop` wakes on time — sometimes one tick late.
            if self.ranges[r].core.rate_due().is_some_and(|d| d <= now) && !self.chance(100) {
                self.note(format!("rate_tick range {r}"));
                let acts = self.ranges[r].core.rate_tick(now);
                self.absorb(r, acts)?;
            }
        }
        Ok(())
    }

    /// Leader change of range `r`: the old term's tasks are cancelled (`JoinSet::shutdown`
    /// — waiters leave, permits not yet in a turn are returned, half-done turns are cut
    /// while holding the lock), then the new term does `reset`, attaches the node's new
    /// cell, rebuilds the VO locks from the "store" (the model) — like `leadership_loop` +
    /// `restore_flow` + `serve`.
    fn reset(&mut self, r: usize) -> Check {
        self.note(format!("leader change range {r}"));
        while let Some(i) = (0..self.invs.len())
            .find(|&i| self.invs[i].range == r && !matches!(self.invs[i].st, St::Idle))
        {
            match &self.invs[i].st {
                St::Waiting(_) => self.cancel(i)?,
                St::Granted { begun: false, .. } => self.give_back(i)?,
                St::Granted { begun: true, .. } => {
                    let St::Granted { mut permit, .. } =
                        std::mem::replace(&mut self.invs[i].st, St::Idle)
                    else {
                        unreachable!()
                    };
                    let now = self.now();
                    let acts = self.ranges[r].core.release_running(&mut permit, now);
                    self.absorb(r, acts)?;
                }
                // The ticket is lost with the term: not yet in a turn, the lock only belongs
                // to the nacked grant — not rebuilt.
                St::Deferred { held } => {
                    if !*held {
                        self.invs[i].lock = None;
                    }
                    self.invs[i].st = St::Idle;
                }
                St::Idle => unreachable!(),
            }
        }
        let acts = self.ranges[r].core.reset();
        self.absorb(r, acts)?;
        let now = self.now();
        if let Some(b) = self.budget.clone() {
            self.ranges[r].lead = None;
            self.ranges[r].lead = Some(b.lead());
            self.ranges[r].core.attach_node_budget(&b);
            let acts = self.ranges[r].core.set_max_turns(b.share(), now);
            self.absorb(r, acts)?;
            self.mark_changed();
        }
        for i in 0..self.invs.len() {
            if self.invs[i].range == r {
                if let Some(l) = self.invs[i].lock.clone() {
                    self.ranges[r].core.restore_lock(l, self.invs[i].id);
                }
            }
        }
        ensure!(
            self.ranges[r].core.usage_is_empty(),
            "FL-8: counter not 0 after reset"
        );
        Ok(())
    }

    fn mark_changed(&mut self) {
        for rs in &mut self.ranges {
            rs.changed = true;
        }
    }

    pub(super) fn deliver(&mut self, r: usize) -> Check {
        self.ranges[r].changed = false;
        let share = self.share();
        let now = self.now();
        self.note(format!("node changed → range {r} (share {share})"));
        let acts = self.ranges[r].core.node_turns_changed(share, now);
        self.absorb(r, acts)
    }

    /// FL-3: `poll_acquire` has no side effects — asking twice with the same `now` gives
    /// the same answer, and no counter, lock or slot changes.
    fn probe(&mut self, i: usize) -> Check {
        let inv = &self.invs[i];
        let core = &self.ranges[inv.range].core;
        let now = self.now();
        let before = fingerprint(core);
        let a = outcome(
            core.rm
                .poll_acquire(&inv.queue, &inv.id, inv.cool.as_ref(), now),
        );
        let b = outcome(
            core.rm
                .poll_acquire(&inv.queue, &inv.id, inv.cool.as_ref(), now),
        );
        ensure!(
            a == b,
            "FL-3: poll_acquire differs between two calls: {a} / {b}"
        );
        ensure!(
            fingerprint(core) == before,
            "FL-3: poll_acquire changed the state"
        );
        Ok(())
    }

    // ───────────────────────────────────────────────────────── core outputs

    pub(super) fn absorb(&mut self, r: usize, acts: Actions<u64>) -> Check {
        if acts.node_room {
            self.mark_changed();
        }
        let mut grants = acts.grants;
        let mut out = Ok(());
        for g in grants.drain(..) {
            let res = self.on_grant(r, g);
            if out.is_ok() {
                out = res;
            }
        }
        self.ranges[r].core.recycle(grants);
        out
    }

    fn on_grant(&mut self, r: usize, g: Grant<u64>) -> Check {
        let Grant {
            to,
            mut permit,
            seen,
            ..
        } = g;
        // FL-6: only those still waiting are granted.
        let Some(rq) = self.reqs.get(&to).filter(|q| q.live) else {
            permit.live = false;
            return Err(format!(
                "FL-6: grant to request {to} that left or does not exist"
            ));
        };
        let (i, seq, held, key) = (rq.inv, rq.seq, rq.held_lock, rq.key.clone());
        // FL-T: a suspended tenant is granted no slot at all.
        if self.suspended.contains(&self.invs[i].queue.tenant) {
            permit.live = false;
            return Err(format!(
                "FL-T: slot granted to inv{i} of suspended tenant {}",
                self.invs[i].queue.tenant
            ));
        }
        ensure!(
            rq.range == r,
            "the core of range {r} granted a request of range {}",
            rq.range
        );
        // FL-5: nobody with a smaller `seq` is still waiting in the same queue — unless the
        // granted one held the lock when asking (head of the line).
        let mut ahead: Vec<(u64, u64)> = self
            .reqs
            .iter()
            .filter(|(&n, q)| n != to && q.live && q.range == r && q.key == key && q.seq < seq)
            .map(|(&n, q)| (n, q.seq))
            .collect();
        // A nacked waiter of an EXCLUSIVE queue still waits at its `seq` position: nobody
        // with a later `seq` is granted before it.
        ahead.extend(
            self.invs
                .iter()
                .enumerate()
                .filter(|(j, o)| {
                    *j != i
                        && !o.done
                        && matches!(o.st, St::Deferred { .. })
                        && o.queue.exclusive
                        && o.range == r
                        && o.key == key
                        && o.seq < seq
                })
                .map(|(_, o)| (u64::MAX, o.seq)),
        );
        self.reqs.get_mut(&to).expect("just seen").live = false;
        self.invs[i].st = St::Granted {
            req: to,
            permit: Box::new(permit),
            begun: false,
            oversub: seen.node.as_ref().is_some_and(|n| n.leading > n.total),
        };
        self.note(format!("grant inv{i} seq {seq} req {to}"));
        ensure!(
            held || ahead.is_empty(),
            "FL-5: inv{i} (seq {seq}) granted before request {ahead:?} (req, seq) of the same queue"
        );
        let St::Granted { permit, .. } = &self.invs[i].st else {
            unreachable!()
        };
        // FL-4: the VO lock is exclusive.
        if let Some(l) = &permit.lock {
            let other = self
                .invs
                .iter()
                .enumerate()
                .find(|(j, o)| *j != i && o.lock.as_ref() == Some(l))
                .map(|(j, _)| j);
            ensure!(
                other.is_none(),
                "FL-4: inv{i} and inv{other:?} both hold lock {l:?}"
            );
        }
        let lock = permit.lock.clone();
        // FL-3: the effective cap is the tightest of the three tiers.
        let core = &self.ranges[r].core;
        for c in &permit.counters {
            let used = core.rm.usage.get(c).copied().unwrap_or(0);
            if let Some(max) = core.rm.rules.limit_of(&c.path) {
                ensure!(
                    used <= max,
                    "FL-3: counter {c} uses {used} > cap {max} after a grant"
                );
            }
        }
        if let Some(k) = &permit.cool {
            let used = core.rm.cool_usage.get(k).copied().unwrap_or(0);
            ensure!(
                used <= COOLDOWN_CAP,
                "FL-14: cooldown {used} > {COOLDOWN_CAP}"
            );
        }
        check_grant_turns(&seen)?;
        if lock.is_some() {
            self.invs[i].lock = lock;
        }
        Ok(())
    }
}
