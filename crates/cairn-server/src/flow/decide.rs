// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The scheduler's synchronous core: queues, grants, `Core`.

use super::*;

// ──────────────────────────────────────────────────────── scheduler

/// A waiting slot request. `T` is the asker, as the shell calls it: the sending end of a
/// channel in [`Scheduler`], a request number in the sim.
pub(super) struct Pending<T> {
    id: InvocationId,
    /// The invocation's position in the durable queue (`VQueues.seq`).
    ///
    /// **This is what decides the service order, not the arrival order.** Two reasons, and
    /// the first is a hard blocker:
    ///
    /// 1. An invocation that suspended and woke up **still holds the VO lock** and must ask
    ///    for a slot again. Ordered by arrival, it would sit behind a newcomer of the same
    ///    queue — who is blocked by the very lock it holds. The queue deadlocks, and there is
    ///    no error to see.
    /// 2. `seq` is the real order of entering the queue, and it is **durable**. Arrival
    ///    order is not: after a leader change, the order in which tasks ask for slots again
    ///    is the tokio scheduler's random order.
    seq: u64,
    /// Number of the request ([`Asked::req`]) — so the asker can report leaving
    /// ([`Core::cancel`]).
    req: u64,
    /// The turn's `cooldown` identity (FL-14) — `None` almost all the time.
    cool: Option<CoolKey>,
    to: T,
    /// Counted toward the tenant's `demand` on the node until this entry leaves the line —
    /// by any path (`min_vt` only counts waiting tenants). `None` while the tenant is
    /// suspended: it cannot run, so its frozen `vt` is not "the slowest waiting tenant" —
    /// counting it would pin `min_vt` and block every other tenant from borrowing slots for
    /// the whole suspension. [`Core::set_refused`] detaches/reattaches it when the suspended
    /// set changes.
    demand: Option<Demand>,
}

/// The receiving end of a slot request, **safe to cancel mid-way**.
///
/// The window it closes: the scheduler `secure`s a permit and `send`s it into the channel;
/// right after, before the asker gets to poll, the asker's future is cancelled (wait cap
/// hit in [`crate::turn`], or `JoinSet::shutdown` on losing leadership). The `Permit`
/// sitting in the channel is then dropped with **nobody releasing it** — and `Drop` of
/// `Permit` deliberately does not release, so slot and lock stay stuck until the next
/// leader change.
///
/// Releasing here is `release_all` — no turn has started, so there is nothing to keep the
/// lock for — **unless the asker already held the lock before** (suspended, waiting to
/// retry, or a lock just rebuilt on becoming leader): that lock belongs to its half-done
/// journal, not to this ask. Releasing it would hand the object to the next one while the
/// half-done turn has not finished (a lost update: the later turn writes state, the
/// half-done turn reruns and overwrites it with the replayed old value).
///
/// `Claim` only covers until the permit leaves the channel. From there until the turn
/// starts, the permit itself carries this slot return path ([`Permit::begin`]) — that gap
/// has one more `await` point: winning back the engine in [`crate::lease::Lease::wait`].
pub(super) struct Claim {
    pub(super) rx: oneshot::Receiver<Permit>,
    pub(super) sched: Scheduler,
    /// The queue that holds this asker's `Pending` — to report that it just died.
    pub(super) key: QueueKey,
    /// This asker's request number in that queue.
    pub(super) req: u64,
    /// Slot received: the scheduler already took the `Pending` out, nothing to clean up.
    pub(super) done: bool,
    /// The asker already held the queue's VO lock **before** this ask.
    pub(super) held_lock: bool,
}

impl Drop for Claim {
    fn drop(&mut self) {
        // `close()` first so no slot slips in after the check; then sweep up whatever is
        // already sitting in the channel.
        self.rx.close();
        let Ok(mut permit) = self.rx.try_recv() else {
            if !self.done {
                // Leaving before being called: our `Pending` becomes a dead entry in the wait
                // line — report it so it gets cleaned up. An empty channel with nothing
                // received ⇒ the core has not granted us (the core grants and the shell sends
                // right away, within one borrow): exactly the condition of `cancel`.
                self.sched.core.borrow_mut().cancel(&self.key, self.req);
            }
            return;
        };
        self.give_back().give(&mut permit);
    }
}

impl Claim {
    pub(super) fn give_back(&self) -> GiveBack {
        GiveBack {
            sched: self.sched.clone(),
            held_lock: self.held_lock,
        }
    }
}

/// The state of one queue in the scheduler.
pub(super) struct QueueState<T> {
    pub(super) name: QueueName,
    /// FIFO of this queue's requests waiting for a slot.
    pub(super) waiting: VecDeque<Pending<T>>,
    /// Deficit round-robin. With `quantum = 1` for every queue, DRR **degenerates exactly
    /// into round-robin** — and that is the behaviour we want from the start. The field has
    /// existed from day one because adding weights later is changing a constant, while
    /// adding a field to a loop already running in production is changing the design.
    deficit: u32,
    quantum: u32,
    /// The resource blocking this queue, if it is not in the eligibility loop.
    pub(super) blocked_on: Option<ResourceKind>,
    /// Request numbers of the **dead** entries (the asker left) still in `waiting` — see
    /// [`Core::cancel`]. Empty almost all the time.
    pub(super) dead: HashSet<u64>,
    /// Ticket of the latest wait registration — see [`WaitList`].
    pub(super) ticket: u64,
    /// Total wait time and number of times blocked — **not persisted**.
    pub(super) granted: u64,
    pub(super) blocked_times: u64,
    /// Sim: the queue was just called out of this resource's wait list (in the current
    /// `dispatch` round), with the smallest `vt` of the tenants still alive in the `Turns`
    /// wait line at that time — see [`Seen`].
    #[cfg(test)]
    woken: Option<(ResourceKind, Option<u64>)>,
}

impl<T> QueueState<T> {
    fn new(name: QueueName) -> Self {
        Self {
            name,
            waiting: VecDeque::new(),
            deficit: 0,
            quantum: 1,
            blocked_on: None,
            dead: HashSet::default(),
            ticket: 0,
            granted: 0,
            blocked_times: 0,
            #[cfg(test)]
            woken: None,
        }
    }

    /// Number of askers still alive.
    pub(super) fn live(&self) -> usize {
        self.waiting.len().saturating_sub(self.dead.len())
    }
}

#[derive(Default)]
pub(super) struct Stats {
    /// Number of times a request was blocked, by resource label.
    pub(super) blocked: HashMap<&'static str, u64>,
    pub(super) granted: u64,
    /// Sim (FL-9): number of times a queue just called out of the wait list of a
    /// **countable** resource R was blocked again at R itself — a wasted call.
    #[cfg(test)]
    pub(super) wasted: u64,
}

/// A slot just granted — an output of [`Core`]. The permit is already `secure`d; the shell
/// only has to hand it to `to`.
pub(crate) struct Grant<T> {
    pub(crate) to: T,
    pub(crate) permit: Permit,
    /// Whether the invocation held the queue's VO lock **right before** this grant — the
    /// unused-slot return path of a parked ticket (`Scheduler::nack`): the ticket does not
    /// hold the invocation lock while waiting, so the value at ask time may be stale (a
    /// cancel releasing the VO lock while the ticket was parked).
    pub(crate) held_before: bool,
    /// Sim: what the core saw right before the grant — to check FL-7/FL-13 at exactly the
    /// moment of decision.
    #[cfg(test)]
    pub(crate) seen: Seen,
}

/// What the shell must do after an event of [`Core`]. The core does not call `Waker`s,
/// channels or tokio: it only says what needs doing.
pub(crate) struct Actions<T> {
    pub(crate) grants: Vec<Grant<T>>,
    /// The rate tier's deadline ([`Core::rate_due`]) was just brought forward — wake
    /// [`Scheduler::rate_loop`].
    pub(crate) rate_sooner: bool,
    /// The node just went from full to having room — notify every range
    /// ([`crate::engine_pool::NodeTurns::announce`]).
    pub(crate) node_room: bool,
}

impl<T> Default for Actions<T> {
    fn default() -> Self {
        Self {
            grants: Vec::new(),
            rate_sooner: false,
            node_room: false,
        }
    }
}

/// Receipt of [`Core::request`].
pub(crate) struct Asked {
    /// Number of the request — to report leaving ([`Core::cancel`]).
    pub(crate) req: u64,
    /// The asker already held the queue's VO lock before this ask (see [`Claim`]).
    pub(crate) held_lock: bool,
}

/// Sim: the core's state right before a grant (see [`Grant::seen`]).
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub(crate) turns_before: usize,
    pub(crate) max_turns: usize,
    pub(crate) node: Option<crate::engine_pool::NodeProbe>,
    /// The tenant's `vt` at decision time (before `secure` adds to it).
    pub(crate) vt: u64,
    pub(crate) min_vt: u64,
    /// This queue was called out of the `Turns` wait line in this round, and the smallest
    /// `vt` of the tenants still alive in that wait line at that time (itself included).
    pub(crate) from_turns: Option<u64>,
}

/// **The scheduler's core**: synchronous, no tokio, no hidden clock. Each event (ask,
/// leave, release, rule change, rate tick…) takes `now` from the caller and returns
/// [`Actions`] — the slots just granted and what must be notified. [`Scheduler`] is the
/// async shell: channels for askers, a `Notify` for [`Scheduler::rate_loop`], the node's
/// watch; this module's sim applies events directly to the core.
pub(crate) struct Core<T> {
    pub(super) rm: ResourceManager,
    pub(super) queues: HashMap<QueueKey, QueueState<T>>,
    /// The loop of **eligible** queues: they have waiting requests and are not blocked, as
    /// last known.
    pub(super) ring: VecDeque<QueueKey>,
    pub(super) in_ring: HashSet<QueueKey>,
    /// **Countable** resources (counters, turn cap) that just got room but whose waiters
    /// have not been called yet — see [`Core::wake`].
    freed: VecDeque<ResourceKind>,
    /// Next ticket for [`QueueState::ticket`]. Grows forever, never repeats.
    next_ticket: u64,
    /// Next number for [`Pending::req`]. Grows forever, across `reset` too: an asker of the
    /// old term reporting that it left after a `reset` cannot hit anyone of the new term.
    next_req: u64,
    pub(super) stats: Stats,
    /// The earliest moment a bucket blocking someone has tokens again — see
    /// [`Scheduler::rate_loop`]. `None` = nobody waits for tokens.
    pub(super) rate_due: Option<Instant>,
    /// **Slot gate closed** (`Scheduler::close_gate`): grant nothing — askers still enter
    /// the line, resources are still released, but [`Self::dispatch`] does not run. Start of
    /// a term: `ungate` puts every ticket waiting at the gate into the core before opening,
    /// one grant pass by `seq` (not one `dispatch` per ticket). Quiesce: returned slots are
    /// not granted in a cascade to the next ticket.
    pub(super) frozen: bool,
    /// Output being collected for the current event.
    out: Actions<T>,
}

impl<T> Core<T> {
    pub(crate) fn new(max_turns: usize) -> Self {
        Self {
            rm: ResourceManager::new(max_turns),
            queues: HashMap::default(),
            ring: VecDeque::new(),
            in_ring: HashSet::default(),
            freed: VecDeque::new(),
            next_ticket: 0,
            next_req: 0,
            stats: Stats::default(),
            rate_due: None,
            frozen: false,
            out: Actions::default(),
        }
    }

    /// Close the slot gate — see [`Self::frozen`].
    pub(crate) fn freeze(&mut self) {
        self.frozen = true;
    }

    /// Open the slot gate: every queue goes back to the eligibility loop (the knowledge
    /// "waiting on resource X" may be stale after releases that granted nobody), one
    /// `dispatch`.
    pub(crate) fn thaw(&mut self, now: Instant) -> Actions<T> {
        if !self.frozen {
            return self.take();
        }
        self.frozen = false;
        let keys: Vec<QueueKey> = self.queues.keys().cloned().collect();
        self.rm.waiters.clear();
        for k in keys {
            if let Some(q) = self.queues.get_mut(&k) {
                q.blocked_on = None;
            }
            self.rearm(&k);
        }
        self.dispatch(now);
        self.fed_unless_waiting();
        self.take()
    }

    /// Output of the event just finished.
    fn take(&mut self) -> Actions<T> {
        self.out.node_room |= std::mem::take(&mut self.rm.announce);
        std::mem::take(&mut self.out)
    }

    /// Give back the vec of grants already handed out — keeps the capacity, no allocation on
    /// every release.
    pub(crate) fn recycle(&mut self, mut grants: Vec<Grant<T>>) {
        grants.clear();
        if self.out.grants.capacity() == 0 {
            self.out.grants = grants;
        }
    }

    /// A slot request for invocation `id` (position `seq` in the durable queue). The request
    /// stays in the wait line until it is granted (a [`Grant`] carrying `to`) or the asker
    /// leaves ([`Self::cancel`]).
    pub(crate) fn request(
        &mut self,
        queue: &QueueName,
        id: InvocationId,
        seq: u64,
        cool: Option<CoolKey>,
        to: T,
        now: Instant,
    ) -> (Asked, Actions<T>) {
        let key = queue.encode();
        // The one **holding the queue's lock** goes to the head of the line, whatever its
        // `seq`. Someone who joined the line before it (smaller `seq`) but had no task when
        // it was granted may be at the head, waiting for the very lock it holds; the
        // scheduler only looks at the head, so ordering by `seq` here would make the two
        // wait for each other forever. It already holds the lock — serving it first is the
        // right semantics.
        let holder = queue
            .lock_name()
            .and_then(|l| self.rm.locks.get(&l).copied());
        let holds_lock = holder == Some(id);
        let demand = (!self.rm.refused.contains(&queue.tenant))
            .then(|| Demand::new(&self.rm.meter(&queue.tenant)));
        let req = self.next_req;
        self.next_req += 1;
        let waiting = &mut self
            .queues
            .entry(key.clone())
            .or_insert_with(|| QueueState::new(queue.clone()))
            .waiting;
        // Insert by `seq`, not push_back — binary search: this list is **not** short for a
        // hot object (thousands of waiters on one VO lock), and it is on the range's engine
        // thread.
        //
        // And **nobody cuts in front of the lock holder** standing at the head: its
        // position 0 is out of `seq` order, so a binary search purely by `seq` would put a
        // later arrival with a smaller `seq` in front of it — exactly the deadlock just
        // avoided, only arriving one tick later (`recover` hands invocations over by id, not
        // by `seq`). There is only one lock holder and it is always at the head, so the
        // predicate still splits the line in two.
        let at = if holds_lock {
            0
        } else {
            waiting.partition_point(|p| p.seq <= seq || Some(p.id) == holder)
        };
        waiting.insert(
            at,
            Pending {
                id,
                seq,
                req,
                cool,
                to,
                demand,
            },
        );
        // The queue has new work ⇒ it is eligible again, even if it was in a resource's
        // wait list before: the old knowledge may be outdated, and `poll_acquire` is the
        // only place that decides.
        self.rearm(&key);
        self.dispatch(now);
        (
            Asked {
                req,
                held_lock: holds_lock,
            },
            self.take(),
        )
    }

    /// Asker `req` of queue `key` just left before being granted.
    ///
    /// Only for requests **not yet granted** (the shell knows: its channel is empty). Its
    /// entry only leaves the line by itself when it drifts to the head — and when the head
    /// is a **live** waiter (the holder of the smallest seq of a hot object), the dead
    /// entries behind it pile up forever: on a billing workload's cluster 300 000 entries
    /// in one range, each insert a `memmove` over the whole line. Sweep when dead entries
    /// outnumber live ones: O(n) each time, but only after ≥ n/2 departures — amortised
    /// O(1), like [`WaitList`]. Wakes nobody: the place of one who left was never a free
    /// place.
    pub(crate) fn cancel(&mut self, key: &[u8], req: u64) {
        let Some(q) = self.queues.get_mut(key) else {
            return;
        };
        q.dead.insert(req);
        if q.dead.len() > 32 && 2 * q.dead.len() > q.waiting.len() {
            let dead = std::mem::take(&mut q.dead);
            q.waiting.retain(|p| !dead.contains(&p.req));
            if q.waiting.is_empty() {
                // Everyone left: the queue leaves the table, and its entry in the wait list
                // (if blocked) dies with it — no wake-up would ever find it to clean it up.
                let blocked = q.blocked_on.take();
                self.queues.remove(key);
                if let Some(res) = blocked {
                    self.forget_waiter(&res);
                }
            }
        }
    }

    /// Withdraw **right away** the not-yet-granted request `req` of queue `key` and return
    /// its asker (a new ask of the same invocation replaces a parked ticket —
    /// `Scheduler::adopt`). Unlike [`Self::cancel`] (mark dead, sweep gradually): the ticket
    /// carries the invocation's wake-up duty and "turn handed over" mark, and must leave the
    /// line right away. O(line length) — only when an invocation with a parked ticket is
    /// asked for a slot again. A queue with people left goes back to the eligibility loop
    /// (the new head may not be blocked where the old one was).
    pub(crate) fn withdraw(
        &mut self,
        key: &[u8],
        req: u64,
        now: Instant,
    ) -> (Option<T>, Actions<T>) {
        let Some(q) = self.queues.get_mut(key) else {
            return (None, self.take());
        };
        let Some(at) = q.waiting.iter().position(|p| p.req == req) else {
            return (None, self.take());
        };
        let pending = q.waiting.remove(at).expect("just seen above");
        q.dead.remove(&req);
        if q.waiting.len() == q.dead.len() {
            // Nobody alive left: the queue leaves the table as in `cancel`.
            let blocked = q.blocked_on.take();
            self.queues.remove(key);
            if let Some(res) = blocked {
                self.forget_waiter(&res);
            }
        } else {
            self.rearm(key);
            self.dispatch(now);
        }
        (Some(pending.to), self.take())
    }

    /// Release the slot, **keep the VO lock** — see [`Scheduler::release_running`].
    pub(crate) fn release_running(&mut self, permit: &mut Permit, now: Instant) -> Actions<T> {
        let freed = self.rm.release_running(permit);
        self.wake(freed);
        self.dispatch(now);
        self.take()
    }

    /// Release everything — see [`Scheduler::release_all`].
    pub(crate) fn release_all(&mut self, permit: &mut Permit, now: Instant) -> Actions<T> {
        let freed = self.rm.release_all(permit);
        self.wake(freed);
        self.dispatch(now);
        self.take()
    }

    /// Return an **unused** slot (a permit dropped before the turn starts — see
    /// [`Claim`]): `release_all`, unless the asker already held the lock before.
    pub(crate) fn give_back(
        &mut self,
        permit: &mut Permit,
        held_lock: bool,
        now: Instant,
    ) -> Actions<T> {
        if held_lock {
            self.release_running(permit, now)
        } else {
            self.release_all(permit, now)
        }
    }

    /// **Nack** of a granted ticket whose turn did not win the invocation lock
    /// (`Scheduler::nack`): return the unused slot, refund the 1 CU charged up front (the
    /// turn did not run — `settle` 0, FL-12). The ticket of an EXCLUSIVE queue **keeps the
    /// VO lock** (`release_running`) even though the grant just took it (`held_before =
    /// false`): the ticket still waits at its `seq` position and asks again when the
    /// invocation lock is released — at the head of the line because it holds the lock
    /// (FL-5). Giving the lock back would hand the object to a later `seq` before it. A
    /// later cancel releases the lock through [`Self::release_lock`]; a ticket lost with the
    /// term has its RAM lock dropped by `reset`.
    pub(crate) fn nack(
        &mut self,
        permit: &mut Permit,
        held_before: bool,
        now: Instant,
    ) -> Actions<T> {
        self.rm.settle(permit, 0, now);
        if permit.lock.is_some() {
            self.release_running(permit, now)
        } else {
            self.give_back(permit, held_before, now)
        }
    }

    /// Settle the turn's CU — see [`Scheduler::settle`].
    pub(crate) fn settle(&self, permit: &Permit, cu: u64, now: Instant) {
        self.rm.settle(permit, cu, now);
    }

    /// Tells the client how long to wait before coming back when the turn is blocked on
    /// `res` — see [`Scheduler::throttle_after`].
    pub(crate) fn throttle_after(&self, res: &ResourceKind, now: Instant) -> Option<Duration> {
        match res {
            ResourceKind::Rate(k) => Some(self.rm.rate_wait(k, now)),
            ResourceKind::Concurrency(_) => Some(Duration::ZERO),
            ResourceKind::Tenant(_) => Some(self.rm.suspended_retry),
            _ => None,
        }
    }

    /// The earliest moment a bucket blocking someone has tokens again — the deadline of
    /// [`Self::rate_tick`]. `None` = nobody waits for tokens.
    pub(crate) fn rate_due(&self) -> Option<Instant> {
        self.rate_due
    }

    /// The rate tier's tick at `now`: wake the waiters at every `Rate` bucket, then
    /// reschedule for those still waiting.
    pub(crate) fn rate_tick(&mut self, now: Instant) -> Actions<T> {
        self.rate_due = None;
        let keys: Vec<ResourceKind> = self
            .rm
            .waiters
            .keys()
            .filter(|r| matches!(r, ResourceKind::Rate(_)))
            .cloned()
            .collect();
        self.wake(keys);
        self.dispatch(now);
        // Token waiters still left (the bucket ran dry again after a few turns): reschedule.
        let left: Vec<RateKey> = self
            .rm
            .waiters
            .iter()
            .filter(|(r, w)| matches!(r, ResourceKind::Rate(_)) && w.live > 0)
            .filter_map(|(r, _)| match r {
                ResourceKind::Rate(k) => Some(k.clone()),
                _ => None,
            })
            .collect();
        for k in left {
            self.arm_rate(&k, now);
        }
        self.take()
    }

    /// Attach to the **node's** turn cap — see [`Scheduler::attach_node_budget`].
    pub(crate) fn attach_node_budget(&mut self, budget: &crate::engine_pool::TurnBudget) {
        self.rm.node = Some(budget.node_turns());
        // The per-tenant bucket and `vt` belong to the node: drop the private book.
        self.rm.fair = budget.tenants().clone();
        self.rm.meters.borrow_mut().clear();
        let rules = self.rm.rules.clone();
        self.rm.fair.set_rules(&rules);
    }

    /// The node's turn cap just changed, or the node just got room again — see
    /// [`Scheduler::node_turns_changed`].
    pub(crate) fn node_turns_changed(&mut self, share: usize, now: Instant) -> Actions<T> {
        self.set_max_turns_at(share, now);
        // The node's announcement is also the announcement "a tenant just left its turn cap
        // in another range": ask the tenant-cap waiters again.
        let mut freed = vec![ResourceKind::Turns];
        freed.extend(
            self.rm
                .waiters
                .keys()
                .filter(|r| matches!(r, ResourceKind::TenantTurns(_)))
                .cloned(),
        );
        self.wake(freed);
        self.dispatch(now);
        // The starving flag of a range with no slot waiters left (the waiters left) would
        // block every other range from borrowing forever — clear it here, on the tick where
        // the node just got room.
        let waiting_turns = self
            .rm
            .waiters
            .get(&ResourceKind::Turns)
            .is_some_and(|w| w.live > 0);
        if !waiting_turns {
            self.rm.note_fed();
        }
        self.take()
    }

    /// Change the range's turn cap — see [`Scheduler::set_max_turns`].
    pub(crate) fn set_max_turns(&mut self, n: usize, now: Instant) -> Actions<T> {
        self.set_max_turns_at(n, now);
        self.take()
    }

    fn set_max_turns_at(&mut self, n: usize, now: Instant) {
        let n = n.max(1);
        if self.rm.max_turns == n {
            return;
        }
        let grew = n > self.rm.max_turns;
        self.rm.max_turns = n;
        if grew {
            self.wake(vec![ResourceKind::Turns]);
            self.dispatch(now);
        }
    }

    /// The rule book just changed — see [`Scheduler::set_rules`].
    pub(crate) fn set_rules(&mut self, book: RuleBook, now: Instant) -> Actions<T> {
        if !self.rm.set_rules(book) {
            return self.take();
        }
        let keys: Vec<QueueKey> = self.queues.keys().cloned().collect();
        self.rm.waiters.clear();
        for k in keys {
            let Some(tenant) = self.queues.get(&k).map(|q| q.name.tenant.clone()) else {
                continue;
            };
            // Waiters of a suspended tenant do not count toward `demand`.
            let meter = (!self.rm.refused.contains(&tenant)).then(|| self.rm.meter(&tenant));
            if let Some(q) = self.queues.get_mut(&k) {
                q.blocked_on = None;
                for p in q.waiting.iter_mut() {
                    match &meter {
                        None => p.demand = None,
                        Some(m) if p.demand.is_none() => p.demand = Some(Demand::new(m)),
                        Some(_) => {}
                    }
                }
            }
            self.rearm(&k);
        }
        self.dispatch(now);
        self.fed_unless_waiting();
        self.take()
    }

    /// The suspended tenant set just changed — see [`Scheduler::set_refused`]. Like
    /// [`Self::set_rules`]: put **every** waiting queue back into the eligibility loop and
    /// let `poll_acquire` filter — on suspension, those waiting elsewhere (VO lock, turn
    /// cap) move to waiting on `Tenant`; on unsuspension, the `Tenant` waiters are asked
    /// again. The set changes a few times a day.
    pub(crate) fn set_refused(&mut self, refused: HashSet<TenantId>, now: Instant) -> Actions<T> {
        if !self.rm.set_refused(refused) {
            return self.take();
        }
        let keys: Vec<QueueKey> = self.queues.keys().cloned().collect();
        self.rm.waiters.clear();
        for k in keys {
            if let Some(q) = self.queues.get_mut(&k) {
                q.blocked_on = None;
            }
            self.rearm(&k);
        }
        self.dispatch(now);
        self.fed_unless_waiting();
        self.take()
    }

    /// Reschedule turns of suspended tenants after this long (tests lower it).
    pub(crate) fn set_suspended_retry(&mut self, d: Duration) {
        self.rm.suspended_retry = d;
    }

    /// Return `id`'s VO lock when it ends **outside** a turn — see
    /// [`Scheduler::release_lock`].
    pub(crate) fn release_lock(
        &mut self,
        lock: LockName,
        id: InvocationId,
        now: Instant,
    ) -> Actions<T> {
        if self.rm.locks.get(&lock) != Some(&id) {
            return self.take();
        }
        self.rm.locks.remove(&lock);
        self.wake(vec![ResourceKind::Lock(lock)]);
        self.dispatch(now);
        self.take()
    }

    /// Drop all counting state — see [`Scheduler::reset`].
    pub(crate) fn reset(&mut self) -> Actions<T> {
        self.rm.reset();
        self.queues.clear();
        self.ring.clear();
        self.in_ring.clear();
        self.freed.clear();
        self.take()
    }

    /// Rebuild a VO lock that is being held — the recovery path on becoming leader.
    pub(crate) fn restore_lock(&mut self, lock: LockName, id: InvocationId) {
        self.rm.restore_lock(lock, id);
    }

    /// Someone waits for `k`'s tokens: set [`Scheduler::rate_loop`] to wake when that bucket
    /// goes positive again (counted from `now`), if earlier than the current deadline.
    fn arm_rate(&mut self, k: &RateKey, now: Instant) {
        let at = now + self.rm.rate_wait(k, now).max(Duration::from_millis(1));
        if self.rate_due.is_none_or(|d| at < d) {
            self.rate_due = Some(at);
            self.out.rate_sooner = true;
        }
    }

    /// Put a queue back into the eligibility loop.
    fn rearm(&mut self, key: &[u8]) {
        let Some(q) = self.queues.get_mut(key) else {
            return;
        };
        if q.waiting.is_empty() {
            return;
        }
        if let Some(res) = q.blocked_on.take() {
            // `blocked_on` just went back to `None` ⇒ its entry in the wait list is already
            // dead (see `WaitList`); only the count needs redoing.
            self.forget_waiter(&res);
        }
        if self.in_ring.insert(key.to_vec()) {
            self.ring.push_back(key.to_vec());
        }
    }

    /// An entry in `res`'s wait list just died. Sweep the list when dead entries clearly
    /// outnumber live ones — amortised, so O(1) per call on average.
    fn forget_waiter(&mut self, res: &ResourceKind) {
        let Some(list) = self.rm.waiters.get_mut(res) else {
            return;
        };
        list.live = list.live.saturating_sub(1);
        if list.live == 0 {
            self.rm.waiters.remove(res);
            if *res == ResourceKind::Turns {
                self.rm.note_fed();
            }
        } else if list.len() > 2 * list.live + 32 {
            let queues = &self.queues;
            list.retain(|k, t| waits_with(queues, k, t));
        }
    }

    /// **The starving flag only stands while someone waits for a slot** (FL-7; `flow/` sim
    /// seed 1473): after re-queueing every queue (`set_rules`, `set_refused`, `thaw`), the
    /// old `Turns` waiters may have moved to blocking elsewhere — suspended tenant, counter,
    /// lock. Without clearing here the flag stands until the next grant in this range or the
    /// next "node changed" message, and with the node not full nobody sends one: every other
    /// range stops borrowing the node's free slots.
    fn fed_unless_waiting(&mut self) {
        let waiting = self
            .rm
            .waiters
            .get(&ResourceKind::Turns)
            .is_some_and(|w| w.live > 0);
        if !waiting {
            self.rm.note_fed();
        }
    }

    /// Take the first **live** waiter at `res` out of the wait list.
    fn pop_waiter(&mut self, res: &ResourceKind) -> Option<QueueKey> {
        let rm = &mut self.rm;
        let (meters, fair) = (&rm.meters, &rm.fair);
        let list = rm.waiters.get_mut(res)?;
        let mut found = None;
        while let Some((key, ticket)) = list.pop(|t| meter_in(meters, fair, t).vt()) {
            if waits_with(&self.queues, &key, ticket) {
                list.live = list.live.saturating_sub(1);
                found = Some(key);
                break;
            }
        }
        if list.live == 0 {
            self.rm.waiters.remove(res);
            if *res == ResourceKind::Turns {
                // A called waiter blocked again on `Turns` starves again right away
                // (`note_turns_blocked`); granted or blocked elsewhere, it stops.
                self.rm.note_fed();
            }
        }
        let key = found?;
        if let Some(q) = self.queues.get_mut(&key) {
            // Already taken out of the wait list, so don't let `rearm` count it again.
            q.blocked_on = None;
        }
        Some(key)
    }

    /// Wake the waiters at the resources that just got room.
    ///
    /// Two kinds of resources, two ways:
    ///
    /// - **VO lock**: wake everyone. The waiters for a lock are the invocations of *one*
    ///   virtual object — a few, and the old lock holder (suspended, then woken) may stand
    ///   behind a newcomer in the list; let `poll_acquire` filter.
    /// - **Counters and turn cap**: only *record* the free room; [`Core::dispatch`] calls the
    ///   waiters one at a time, **only once the eligibility loop is drained** and only while
    ///   the resource has room. A free slot can be granted to exactly one waiter, so waking
    ///   everyone would re-ask the whole list only to queue nearly all of them again — one
    ///   core at 100% with 100k invocations. Calling one at a time does not abandon free
    ///   room either: if the called one turns out to be blocked elsewhere, or has left, the
    ///   room is still free and the next one is called.
    fn wake(&mut self, freed: Vec<ResourceKind>) {
        for res in freed {
            if let ResourceKind::Lock(_) = res {
                while let Some(key) = self.pop_waiter(&res) {
                    #[cfg(test)]
                    self.mark_woken(&key, &res, None);
                    self.rearm(&key);
                }
            } else if self.rm.waiters.contains_key(&res) {
                self.freed.push_back(res);
            }
        }
    }

    /// Call the next waiter of a countable resource that has room. `false` when there is
    /// nobody left to call.
    fn wake_next(&mut self, now: Instant) -> bool {
        while let Some(res) = self.freed.front() {
            if self.rm.has_room(res, now) {
                let res = res.clone();
                #[cfg(test)]
                let min_vt = (res == ResourceKind::Turns)
                    .then(|| self.turns_min_vt())
                    .flatten();
                if let Some(key) = self.pop_waiter(&res) {
                    #[cfg(test)]
                    self.mark_woken(&key, &res, min_vt);
                    self.rearm(&key);
                    return true;
                }
            }
            self.freed.pop_front();
        }
        false
    }

    /// One scheduling round: deficit round-robin over the eligibility loop.
    ///
    /// Repeats until **no further progress is possible**: a queue granted a slot may clear
    /// the way for another queue (by releasing a lock, say), so one sweep is not enough.
    /// When the eligibility loop is drained while a countable resource still has free room,
    /// call that resource's next waiter (see [`Core::wake`]).
    ///
    /// `now` is the time of the whole round: every token check in one round compares
    /// against the same moment, read by the caller.
    fn dispatch(&mut self, now: Instant) {
        if self.frozen {
            return;
        }
        loop {
            let n = self.ring.len();
            if n == 0 {
                if self.wake_next(now) {
                    continue;
                }
                return;
            }
            let mut progress = false;
            for _ in 0..n {
                let Some(key) = self.ring.pop_front() else {
                    break;
                };
                self.in_ring.remove(&key);
                if self.serve_queue(&key, &mut progress, now) {
                    // More work and not blocked ⇒ back to the end of the loop. This is where
                    // `quantum` turns DRR into round-robin: each queue gets exactly one slot
                    // before the others get their turn.
                    self.in_ring.insert(key.clone());
                    self.ring.push_back(key);
                }
            }
            // No progress means the eligibility loop is drained (every queue served without
            // a grant leaves the loop) — the next waiter's turn.
            if !progress && !self.wake_next(now) {
                return;
            }
        }
    }

    /// Grant slots to one queue until its quantum runs out. Returns `true` if it is still
    /// eligible.
    ///
    /// Every touch of `self.queues` is a short, deliberate borrow: `poll_acquire` and `wake`
    /// both need `&mut self`, and holding a `&mut QueueState` across them is something the
    /// compiler does not allow — and rightly so, because `wake` may delete that very queue.
    fn serve_queue(&mut self, key: &[u8], progress: &mut bool, now: Instant) -> bool {
        let name = {
            let Some(q) = self.queues.get_mut(key) else {
                return false;
            };
            q.deficit = q.deficit.saturating_add(q.quantum);
            q.name.clone()
        };
        #[cfg(test)]
        let woken = self.queues.get_mut(key).and_then(|q| q.woken.take());

        loop {
            // Head of the queue, after dropping askers that are gone (tasks cancelled on
            // losing leadership) — granting a slot nobody receives loses that slot.
            let front = {
                let Some(q) = self.queues.get_mut(key) else {
                    return false;
                };
                if q.deficit == 0 {
                    if !q.waiting.is_empty() {
                        return true;
                    }
                    // Just granted to the last one: the empty queue leaves the table right
                    // away, like the branch below — otherwise it stays until the end of the term.
                    self.queues.remove(key);
                    return false;
                }
                while !q.dead.is_empty()
                    && q.waiting.front().is_some_and(|p| q.dead.contains(&p.req))
                {
                    if let Some(p) = q.waiting.pop_front() {
                        q.dead.remove(&p.req);
                    }
                }
                q.waiting.front().map(|p| (p.id, p.cool.clone()))
            };
            let Some((id, cool)) = front else {
                // An empty queue is removed from the table entirely: keeping every queue
                // that ever ran is a slow memory leak no test would see.
                self.queues.remove(key);
                return false;
            };

            match self.rm.poll_acquire(&name, &id, cool.as_ref(), now) {
                AcquireOutcome::Acquired(prov) => {
                    #[cfg(test)]
                    let seen = self.seen(&name.tenant, woken.as_ref());
                    let held_before = name
                        .lock_name()
                        .is_some_and(|l| self.rm.locks.get(&l) == Some(&id));
                    let permit = prov.secure(&mut self.rm, now);
                    let pending = {
                        let q = self.queues.get_mut(key).expect("just seen above");
                        q.deficit -= 1;
                        q.granted += 1;
                        q.waiting.pop_front().expect("just seen above")
                    };
                    self.stats.granted += 1;
                    *progress = true;
                    self.out.grants.push(Grant {
                        to: pending.to,
                        permit,
                        held_before,
                        #[cfg(test)]
                        seen,
                    });
                }
                AcquireOutcome::BlockedOn(res) => {
                    *self.stats.blocked.entry(res.label()).or_insert(0) += 1;
                    // A lock wakes every waiter for it (FL-9) — only countable resources call
                    // waiters one at a time.
                    #[cfg(test)]
                    if woken.as_ref().is_some_and(|(from, _)| *from == res)
                        && !matches!(res, ResourceKind::Lock(_))
                    {
                        self.stats.wasted += 1;
                    }
                    crate::metrics::flow::bump(match res {
                        ResourceKind::Lock(_) => &crate::metrics::flow::BLOCKED_LOCK,
                        ResourceKind::Concurrency(_) => &crate::metrics::flow::BLOCKED_CONCURRENCY,
                        ResourceKind::Turns => &crate::metrics::flow::BLOCKED_TURNS,
                        ResourceKind::Rate(_) => &crate::rate::BLOCKED_RATE,
                        ResourceKind::Cooldown(_) => &crate::quarantine::COOLED,
                        ResourceKind::Tenant(_) => &crate::metrics::flow::BLOCKED_TENANT,
                        ResourceKind::TenantTurns(_) => &crate::metrics::flow::BLOCKED_TENANT_TURNS,
                    });
                    // The turn-cap wait line is ordered by the tenant's `vt`.
                    let order = match &res {
                        ResourceKind::Turns => {
                            self.rm.note_turns_blocked();
                            Some((name.tenant.clone(), self.rm.meter(&name.tenant).vt()))
                        }
                        ResourceKind::Rate(k) => {
                            self.arm_rate(k, now);
                            None
                        }
                        _ => None,
                    };
                    let ticket = self.next_ticket;
                    self.next_ticket += 1;
                    if let Some(q) = self.queues.get_mut(key) {
                        q.blocked_times += 1;
                        q.deficit = 0;
                        q.blocked_on = Some(res.clone());
                        q.ticket = ticket;
                    }
                    self.rm.add_waiter(res, key.to_vec(), ticket, order);
                    return false;
                }
            }
        }
    }

    /// Sim: queue `key` was just called out of `res`'s wait list.
    #[cfg(test)]
    fn mark_woken(&mut self, key: &[u8], res: &ResourceKind, min_vt: Option<u64>) {
        if let Some(q) = self.queues.get_mut(key) {
            q.woken = Some((res.clone(), min_vt));
        }
    }

    /// Sim: the smallest `vt` of the tenants with **live** waiters at `Turns`.
    #[cfg(test)]
    fn turns_min_vt(&self) -> Option<u64> {
        let list = self.rm.waiters.get(&ResourceKind::Turns)?;
        list.fair
            .by_tenant
            .iter()
            .filter(|(_, dq)| dq.iter().any(|(k, t)| waits_with(&self.queues, k, *t)))
            .map(|(t, _)| self.rm.meter(t).vt())
            .min()
    }

    #[cfg(test)]
    fn seen(&self, tenant: &TenantId, woken: Option<&(ResourceKind, Option<u64>)>) -> Seen {
        Seen {
            turns_before: self.rm.turns_in_use,
            max_turns: self.rm.max_turns,
            node: self.rm.node.as_ref().map(|n| n.probe()),
            vt: self.rm.meter(tenant).vt(),
            min_vt: self.rm.fair.min_vt(),
            from_turns: woken
                .and_then(|(res, min)| (*res == ResourceKind::Turns).then_some(*min).flatten()),
        }
    }
}
