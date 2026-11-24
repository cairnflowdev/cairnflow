// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The async shell `Scheduler` around `Core`, and the snapshots for `/flow`.

use super::*;

/// An asker in the async shell: a task waiting on a channel, or a parked ticket.
type Tx = Waiter;

/// The scheduler of one partition: the async shell around [`Core`].
///
/// `Rc<RefCell<..>>` because it lives on the engine thread and is shared by many tasks of
/// that same thread — the same reason and the same constraint as [`crate::lease`]. Not
/// `Send`, and the compiler enforces it. Each public call reads the clock once, applies one
/// event to the core, then acts on its [`Actions`] ([`Self::deliver`]).
#[derive(Clone)]
pub struct Scheduler {
    pub(super) core: Rc<RefCell<Core<Tx>>>,
    /// Tells `rate_loop` that the rate tier's deadline has just been brought forward.
    rate_armed: Rc<Notify>,
    /// The node's per-tenant limit source and the generation already pushed into the core:
    /// each slot ask compares generations — one atomic read; only on a change does it take
    /// the source's lock. A change the node has acknowledged (ACK) takes effect at the next
    /// slot ask, without waiting for the background tick.
    tenants: Rc<RefCell<Option<(cairn_engine::TenantLimitsSource, u64)>>>,
    /// Tenants that just left the refused set (unsuspended) and the background loop has not
    /// handled yet ([`Self::take_lifted`]).
    lifted: Rc<RefCell<Vec<Lifted>>>,
    /// `lifted-ms=` marks of the tenant registry already put into [`Self::lifted`] in this
    /// term: one unsuspend, one pull, even when the registry changes for another field.
    marks_seen: Rc<RefCell<HashMap<TenantId, u64>>>,
    /// **Slot gate** (FL-8): closed from the start of the term until rebuilding the VO locks
    /// (`actor::restore_flow`) has gone all the way through. Askers wait at the gate
    /// **before** entering the core ([`Self::acquire_cooled`]), so every decision of the core
    /// — position in the line (the lock holder at the head), "already held the lock before
    /// asking" ([`Claim`]), the grant — sees a complete lock book. Open by default: a
    /// scheduler built outside the leadership loop (tests) has no lock rebuild to wait for.
    gate: Rc<tokio::sync::watch::Sender<bool>>,
    /// Live tasks running the range's background turns ([`TurnTask`]): from when the serve
    /// loop spawns them until the task is done — through the gate, waiting for a slot, the
    /// turn.
    turn_tasks: Rc<std::cell::Cell<usize>>,
    /// Parked tickets of background turns — see [`super::ticket`].
    pub(super) tickets: Rc<RefCell<Tickets>>,
    /// Notifies the serve loop: a ticket was just granted, or the invocation lock of a
    /// nacked ticket was just released.
    pub(super) ticket_wake: Rc<Notify>,
    /// Ids whose invocation lock was just released after a missed claim (`Locks`).
    pub(super) freed: Rc<RefCell<Vec<InvocationId>>>,
}

/// A live task running a background turn ([`Scheduler::turn_task`]): counts toward the
/// range's [`SchedulerRow::turn_tasks`] and the node's gauge `cairnflow_flow_turn_tasks`
/// until dropped — on every exit path, cancellation included.
pub struct TurnTask {
    count: Rc<std::cell::Cell<usize>>,
}

impl Drop for TurnTask {
    fn drop(&mut self) {
        self.count.set(self.count.get().saturating_sub(1));
        crate::metrics::flow::TURN_TASKS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Scheduler {
    pub fn new(max_turns: usize) -> Self {
        Self {
            core: Rc::new(RefCell::new(Core::new(max_turns))),
            rate_armed: Rc::new(Notify::new()),
            tenants: Rc::new(RefCell::new(None)),
            lifted: Rc::new(RefCell::new(Vec::new())),
            marks_seen: Rc::new(RefCell::new(HashMap::default())),
            gate: Rc::new(tokio::sync::watch::Sender::new(true)),
            turn_tasks: Rc::default(),
            tickets: Rc::default(),
            ticket_wake: Rc::new(Notify::new()),
            freed: Rc::default(),
        }
    }

    /// Count one task running a background turn from now until the guard is dropped.
    pub fn turn_task(&self) -> TurnTask {
        self.turn_tasks.set(self.turn_tasks.get() + 1);
        crate::metrics::flow::TURN_TASKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        TurnTask {
            count: Rc::clone(&self.turn_tasks),
        }
    }

    /// Close the slot gate: the term's VO lock rebuild begins.
    pub fn close_gate(&self) {
        self.gate.send_replace(false);
        self.core.borrow_mut().freeze();
    }

    /// Open the slot gate: every VO lock has been rebuilt. Those waiting at the gate enter
    /// the core — tasks waiting on a channel, and tickets parked while the gate was closed.
    pub fn open_gate(&self) {
        // Tickets waiting at the gate enter the core while the core is still closed (not
        // granting one ticket at a time), then one grant pass by `seq`.
        self.ungate();
        self.gate.send_replace(true);
        let mut st = self.core.borrow_mut();
        let acts = st.thaw(Instant::now());
        self.deliver(&mut st, acts);
    }

    /// The slot gate is open — the term's VO lock rebuild has gone all the way through.
    pub fn gate_open(&self) -> bool {
        *self.gate.borrow()
    }

    /// Attach the node's tenant state source and the suspended-tenant reschedule tick; push
    /// the current suspended set into the core right away.
    pub fn attach_tenants(&self, src: cairn_engine::TenantLimitsSource, suspended_retry: Duration) {
        self.core.borrow_mut().set_suspended_retry(suspended_retry);
        // An "unseen" generation so the first sync always pushes.
        *self.tenants.borrow_mut() = Some((src, u64::MAX));
        self.sync_tenants();
    }

    /// The source changed generation ⇒ push the new set of `suspended` tenants into the core
    /// ([`Self::set_refused`]). Called on every slot ask and every background tick.
    ///
    /// In the same pass, a tenant that is **not** refused whose registry entry carries a
    /// `lifted-ms=` mark not yet seen in this term ⇒ [`Lifted`] with the marks: a leader that
    /// came up after the unsuspend — the term's first sync, from an empty set — does not see
    /// the suspended set change, so only the registry's marks tell it which rescheduled rows
    /// of the suspension are still waiting. The background loop drops marks that are past
    /// their window (`turn::LiftSweep::add`).
    pub fn sync_tenants(&self) {
        let (refused, marked) = {
            let mut t = self.tenants.borrow_mut();
            let Some((src, seen)) = t.as_mut() else {
                return;
            };
            let now = src.generation();
            if now == *seen {
                return;
            }
            *seen = now;
            let snap = src.snapshot();
            let refused = snap.turns_refused().cloned().collect::<HashSet<_>>();
            let mut marks_seen = self.marks_seen.borrow_mut();
            let mut marked = Vec::new();
            for (tenant, l) in snap.limited() {
                let Some(lifted) = l.lifted_ms else { continue };
                if l.state.refuses_turns() || marks_seen.get(tenant) == Some(&lifted) {
                    continue;
                }
                marks_seen.insert(tenant.clone(), lifted);
                marked.push(Lifted {
                    tenant: tenant.clone(),
                    marks: Some((l.refused_ms, lifted)),
                });
            }
            (refused, marked)
        };
        self.set_refused(refused);
        if !marked.is_empty() {
            self.lifted.borrow_mut().extend(marked);
        }
    }

    /// `tenant` is refused slots according to the latest sync.
    pub fn refuses(&self, tenant: &TenantId) -> bool {
        self.sync_tenants();
        self.core.borrow().rm.refused.contains(tenant)
    }

    /// Act on the core's output: hand each slot into the asker's channel, wake `rate_loop`,
    /// notify the node. Called while the core is still borrowed — sending into a oneshot
    /// only wakes, it does not run the receiver.
    pub(super) fn deliver(&self, st: &mut Core<Tx>, mut acts: Actions<Tx>) {
        loop {
            if acts.rate_sooner {
                self.rate_armed.notify_one();
            }
            if acts.node_room {
                if let Some(n) = &st.rm.node {
                    n.announce();
                }
            }
            let mut grants = std::mem::take(&mut acts.grants);
            let mut orphans = Vec::new();
            for g in grants.drain(..) {
                match g.to {
                    Waiter::Task(tx) => {
                        if let Err(orphan) = tx.send(g.permit) {
                            orphans.push(orphan);
                        }
                    }
                    // Parked ticket: into the ready line, the serve loop spawns the turn.
                    Waiter::Ticket(t) => self.granted_ticket(t, g.permit, g.held_before),
                }
            }
            st.recycle(grants);
            if orphans.is_empty() {
                return;
            }
            // The asker vanished between the grant and the send. The core does not grant
            // to one that has reported leaving (`Claim::drop` reports right away, on the same
            // thread), so this is only a net: release right away, do not let the slot get
            // stuck — the only path where a permit is released without going through its
            // holder.
            let now = Instant::now();
            acts = Actions::default();
            for mut orphan in orphans {
                let more = st.release_all(&mut orphan, now);
                acts.grants.extend(more.grants);
                acts.rate_sooner |= more.rate_sooner;
                acts.node_room |= more.node_room;
            }
        }
    }

    /// Ask for a slot for an invocation. Returns once there is a permit.
    ///
    /// **The caller must have released the engine** ([`crate::lease::Lease::released`]) —
    /// waiting here can take as long as a turn of whoever is ahead, and holding the engine
    /// through that locks the whole partition.
    ///
    /// There is no "ask now, don't wait" path: a blocked request *must* sit in a wait list,
    /// otherwise releasing a slot wakes nobody and fairness turns into
    /// whoever-asks-again-faster.
    pub async fn acquire(&self, queue: &QueueName, id: InvocationId, seq: u64) -> Permit {
        self.acquire_cooled(queue, id, seq, None).await
    }

    /// Like [`Self::acquire`], for a turn of an identity quarantined in `cooldown`
    /// (`cool = Some`): also subject to the [`COOLDOWN_CAP`] cap tier of `poll_acquire`
    /// (FL-14).
    pub async fn acquire_cooled(
        &self,
        queue: &QueueName,
        id: InvocationId,
        seq: u64,
        cool: Option<CoolKey>,
    ) -> Permit {
        // The gate (FL-8): wait for the lock rebuild to go all the way through **before**
        // entering the core. Cancelled here, there is nothing to release yet — no request in
        // the line.
        if !self.gate_open() {
            let mut open = self.gate.subscribe();
            // The sender lives as long as `self`: `wait_for` returns no error here.
            let _ = open.wait_for(|o| *o).await;
        }
        self.sync_tenants();
        // One entry per invocation in the line: this turn replaces its parked ticket — an
        // external signal, a `resume` to an invocation with a ticket. If the ticket was
        // granted but its turn has not won the lock: this turn runs on that very slot and
        // asks for no other — the slot already carries the unused-slot return path by
        // `held_before` at grant time.
        if let Some(permit) = self.adopt(&id) {
            return permit;
        }
        let (rx, asked) = {
            // `Claim` wraps the receiving end, and it exists for **a single reason**: the
            // future calling this function may be cancelled *after* the slot has been sent
            // into the channel (the `timeout` path in `turn::acquire_turn`, and on stopping
            // leadership). Without it the `Permit` sitting in the channel is dropped with
            // nobody releasing it — slot and lock stuck until the next leader change.
            let mut st = self.core.borrow_mut();
            let (tx, rx) = oneshot::channel();
            let (asked, acts) = st.request(queue, id, seq, cool, Waiter::Task(tx), Instant::now());
            self.deliver(&mut st, acts);
            (rx, asked)
        };
        let mut claim = Claim {
            rx,
            sched: self.clone(),
            key: queue.encode(),
            req: asked.req,
            done: false,
            held_lock: asked.held_lock,
        };
        // The granter does `secure` before `send`, so this channel only closes when the
        // scheduler is dropped — by then we have no slot and no engine either.
        let started = tokio::time::Instant::now();
        let mut permit = (&mut claim.rx)
            .await
            .expect("the scheduler never abandons a request");
        claim.done = true;
        // Off the channel but not yet in a turn: `Claim` no longer covers it, the permit
        // covers itself until `Permit::begin` (the asker still has to win back the engine).
        permit.unstarted = Some(claim.give_back());
        crate::metrics::flow::add(
            &crate::metrics::flow::WAIT_MS_TOTAL,
            started.elapsed().as_millis() as u64,
        );
        crate::metrics::flow::bump(&crate::metrics::flow::GRANTED);
        permit
    }

    /// Release the slot, **keep the VO lock**: the turn ended while the invocation is still
    /// suspended, or the turn failed in a retryable way.
    pub fn release_running(&self, mut permit: Permit) {
        let mut st = self.core.borrow_mut();
        let acts = st.release_running(&mut permit, Instant::now());
        self.deliver(&mut st, acts);
    }

    /// Release everything: the invocation is done, failed for good, or cancelled.
    pub fn release_all(&self, mut permit: Permit) {
        let mut st = self.core.borrow_mut();
        let acts = st.release_all(&mut permit, Instant::now());
        self.deliver(&mut st, acts);
    }

    /// Attach the range's scheduler to the **node's** turn cap: beyond its guaranteed share,
    /// the range may borrow slots while the whole node is below the total cap.
    pub fn attach_node_budget(&self, budget: &crate::engine_pool::TurnBudget) {
        self.core.borrow_mut().attach_node_budget(budget);
    }

    /// Settle the CU of the turn just finished: 1 was charged up front, now charge the
    /// remaining `cu − 1` to the bucket and add it to `vt`. Called **before** releasing the
    /// permit.
    pub fn settle(&self, permit: &Permit, cu: u64) {
        self.core.borrow().settle(permit, cu, Instant::now());
    }

    /// Tells the client how long to wait before coming back when the turn is blocked on
    /// `res`. `None` = not "out of budget".
    pub fn throttle_after(&self, res: &ResourceKind) -> Option<Duration> {
        self.core.borrow().throttle_after(res, Instant::now())
    }

    /// **The rate tier's clock.** Tokens are not "released" — they come with time — so no
    /// release event wakes token waiters. This loop sleeps until `rate_due` (the earliest
    /// moment a blocking bucket has tokens again), wakes the waiters at every `Rate` bucket,
    /// then sleeps again. Lives as long as the leader term (the actor runs it in `serve`'s
    /// `JoinSet`).
    pub async fn rate_loop(&self) {
        loop {
            let due = self.core.borrow().rate_due();
            let notified = self.rate_armed.notified();
            match due {
                None => notified.await,
                Some(at) => {
                    // `biased`: on the deadline, wake right away. The branch below only
                    // reports that the deadline was brought forward — missing it for one
                    // round loses nothing: the next round rereads `rate_due`, and
                    // `notify_one` keeps the permit for the next `notified()`.
                    tokio::select! {
                        biased;
                        () = tokio::time::sleep_until(at) => self.rate_tick(),
                        () = notified => {}
                    }
                }
            }
        }
    }

    /// One tick of [`Self::rate_loop`] — `pub(crate)` so tests can call it directly.
    pub(crate) fn rate_tick(&self) {
        let mut st = self.core.borrow_mut();
        let acts = st.rate_tick(Instant::now());
        self.deliver(&mut st, acts);
    }

    /// The node's turn cap just changed, or the node just got room again (another range
    /// released a turn while the node was full): this range's slot waiters try again.
    pub fn node_turns_changed(&self, share: usize) {
        let mut st = self.core.borrow_mut();
        let acts = st.node_turns_changed(share, Instant::now());
        self.deliver(&mut st, acts);
    }

    /// Change the range's turn cap — its share of the node's cap changed (D-c). Raised ⇒
    /// wake the slot waiters; lowered ⇒ running turns run to the end, only new turns are
    /// blocked.
    pub fn set_max_turns(&self, n: usize) {
        let mut st = self.core.borrow_mut();
        let acts = st.set_max_turns(n, Instant::now());
        self.deliver(&mut st, acts);
    }

    /// The rule book just changed.
    ///
    /// A rule change ⇒ a counter may match a **different** rule ⇒ the queues' eligibility
    /// changes. The cheap and correct path: **put every waiting queue back into the
    /// eligibility loop** and let `poll_acquire` filter. Rules change a few times a day;
    /// optimising here is optimising a place that is not hot.
    pub fn set_rules(&self, book: RuleBook) {
        let mut st = self.core.borrow_mut();
        let acts = st.set_rules(book, Instant::now());
        self.deliver(&mut st, acts);
    }

    /// The suspended tenant set just changed — see [`Core::set_refused`].
    pub fn set_refused(&self, refused: HashSet<TenantId>) {
        let mut st = self.core.borrow_mut();
        let lifted: Vec<TenantId> = st.rm.refused.difference(&refused).cloned().collect();
        let acts = st.set_refused(refused, Instant::now());
        self.deliver(&mut st, acts);
        drop(st);
        if !lifted.is_empty() {
            self.lifted
                .borrow_mut()
                .extend(lifted.into_iter().map(|tenant| Lifted {
                    tenant,
                    marks: None,
                }));
        }
    }

    /// Tenants unsuspended since the last call: the background loop pulls their rescheduled
    /// rows back ([`crate::turn::wake_lifted`]). Unsuspending only wakes the queues in RAM;
    /// background turns refused a slot during the suspension sit in durable rescheduled rows,
    /// with `SUSPENDED_RETRY` ahead of them.
    pub fn take_lifted(&self) -> Vec<Lifted> {
        std::mem::take(&mut *self.lifted.borrow_mut())
    }

    /// Reschedule tick for turns of suspended tenants ([`SUSPENDED_RETRY`], tests lower it).
    pub fn suspended_retry(&self) -> Duration {
        self.core.borrow().rm.suspended_retry
    }

    /// The resource currently blocking a queue, as last known.
    ///
    /// It exists to **tell two kinds of waiting apart at the HTTP surface**, and the
    /// difference is real: waiting for an `EXCLUSIVE` lock is the normal serialisation of a
    /// virtual object (`202`, "received, ask again later"), while waiting for a counter means
    /// *the tenant is out of budget* (`429`, "too many, back off"). Returning the same code
    /// for both tells the client to back off for something it does not control, or — worse —
    /// silently swallows the only signal saying the bill is hitting its cap.
    pub fn blocked_on(&self, queue: &QueueName) -> Option<ResourceKind> {
        self.core
            .borrow()
            .queues
            .get(&queue.encode())
            .and_then(|q| q.blocked_on.clone())
    }

    pub fn rules_version(&self) -> u64 {
        self.core.borrow().rm.rules.version
    }

    /// Drop all counting state. Called on losing leadership — and right after
    /// [`Self::attach_tenants`] at the start of every term.
    ///
    /// Unsuspended tenants waiting for the background loop are dropped too, so the seen marks
    /// are forgotten as well and the next sync rereads the registry: otherwise the
    /// `lifted-ms=` mark that `attach_tenants`'s sync just put into the line would be cleared
    /// here and never come back — the new leader would keep the rows rescheduled during the
    /// suspension until their reschedule tick.
    pub fn reset(&self) {
        self.lifted.borrow_mut().clear();
        self.marks_seen.borrow_mut().clear();
        if let Some((_, seen)) = self.tenants.borrow_mut().as_mut() {
            *seen = u64::MAX;
        }
        self.drop_tickets();
        let mut st = self.core.borrow_mut();
        let acts = st.reset();
        self.deliver(&mut st, acts);
    }

    /// Rebuild a VO lock that is being held — the recovery path on becoming leader.
    pub fn restore_lock(&self, lock: LockName, id: InvocationId) {
        self.core.borrow_mut().restore_lock(lock, id);
    }

    /// Release the VO lock of an invocation that ended **outside** a turn (cancelled through
    /// the admin API), so there is no permit to release.
    ///
    /// Only release when we really hold it: clearing someone else's lock lets two
    /// invocations run on one virtual object at the same time.
    /// Like [`Self::release_lock`] when the lock name cannot be read (`id`'s queue read is
    /// corrupt): release every VO lock `id` holds. Scans the lock book — only for the error
    /// path of an admin cancel, not the slot release path (FL-9).
    pub fn release_locks_of(&self, id: InvocationId) {
        let held: Vec<LockName> = {
            let st = self.core.borrow();
            st.rm
                .locks
                .iter()
                .filter(|(_, h)| **h == id)
                .map(|(l, _)| l.clone())
                .collect()
        };
        for lock in held {
            self.release_lock(lock, id);
        }
    }

    pub fn release_lock(&self, lock: LockName, id: InvocationId) {
        let mut st = self.core.borrow_mut();
        let acts = st.release_lock(lock, id, Instant::now());
        self.deliver(&mut st, acts);
    }

    /// Number of turns holding a slot — running, not waiting.
    pub fn turns_in_use(&self) -> usize {
        self.core.borrow().rm.turns_in_use
    }

    /// **Recovery invariant:** after becoming leader, no counter is non-zero.
    pub fn usage_is_empty(&self) -> bool {
        self.core.borrow().usage_is_empty()
    }

    /// Snapshot of usage/limit/available per tier — `GET /limits`.
    pub fn limits_view(&self) -> Vec<LimitRow> {
        let st = self.core.borrow();
        let mut out: Vec<LimitRow> = st
            .rm
            .usage
            .iter()
            .map(|(c, used)| {
                let limit = st.rm.rules.limit_of(&c.path);
                LimitRow {
                    counter: c.to_string(),
                    level: c.path.depth() as u8,
                    used: *used,
                    limit,
                    available: limit.map(|m| m.saturating_sub(*used)),
                    rule: st.rm.rules.winner(&c.path).map(|r| r.pattern.to_string()),
                }
            })
            .collect();
        out.sort_by(|a, b| a.counter.cmp(&b.counter));
        out
    }

    /// Snapshot of the queues — `GET /vqueues`.
    ///
    /// The queue **names** can be read out without a reverse lookup table: that is the
    /// payoff of using composite keys instead of hashes.
    pub fn queues_view(&self) -> Vec<QueueRow> {
        let st = self.core.borrow();
        let mut out: Vec<QueueRow> = st
            .queues
            .values()
            .map(|q| QueueRow {
                queue: q.name.to_string(),
                scope: q.name.scope.as_ref().map(|s| s.to_string()),
                limit_key: q.name.limit.as_ref().map(|l| l.to_string()),
                service: q.name.service.clone(),
                object_key: (!q.name.object_key.is_empty()).then(|| q.name.object_key.clone()),
                exclusive: q.name.exclusive,
                waiting: q.live(),
                granted: q.granted,
                blocked_times: q.blocked_times,
                blocked_on: q.blocked_on.as_ref().map(|r| r.label().to_string()),
                lock_holder: q
                    .name
                    .lock_name()
                    .and_then(|l| st.rm.locks.get(&l))
                    .map(hex),
            })
            .collect();
        out.sort_by(|a, b| a.queue.cmp(&b.queue));
        out
    }

    /// Snapshot of the scheduler — `GET /scheduler`.
    pub fn scheduler_view(&self) -> SchedulerRow {
        let st = self.core.borrow();
        SchedulerRow {
            restoring: !self.gate_open(),
            turn_tasks: self.turn_tasks.get(),
            parked: self.parked(),
            eligible: st.ring.len(),
            queues: st.queues.len(),
            waiting: st.queues.values().map(QueueState::live).sum(),
            turns_in_use: st.rm.turns_in_use,
            max_turns: st.rm.max_turns,
            locks_held: st.rm.locks.len(),
            rules_version: st.rm.rules.version,
            tenants: st.rm.fair.view(),
            granted: st.stats.granted,
            blocked: {
                let mut v: Vec<(String, u64)> = st
                    .stats
                    .blocked
                    .iter()
                    .map(|(k, n)| (k.to_string(), *n))
                    .collect();
                v.sort();
                v
            },
        }
    }
}

impl<T> Core<T> {
    /// No counter, cooldown lock, turn slot or per-tenant turn is held.
    pub(crate) fn usage_is_empty(&self) -> bool {
        self.rm.usage.is_empty()
            && self.rm.cool_usage.is_empty()
            && self.rm.turns_in_use == 0
            && self.rm.tenant_turns.is_empty()
    }
}

/// Snapshot of a partition's whole flow control.
///
/// Gathers three reads into one command because all three must run **on the engine
/// thread**, and three trips through the actor channel to build one diagnostic screen are
/// three times queueing behind everything else — exactly when the operator needs the
/// answer most.
pub struct FlowSnapshot {
    pub limits: Vec<LimitRow>,
    pub queues: Vec<QueueRow>,
    pub scheduler: SchedulerRow,
}

impl Scheduler {
    pub fn snapshot(&self) -> FlowSnapshot {
        FlowSnapshot {
            limits: self.limits_view(),
            queues: self.queues_view(),
            scheduler: self.scheduler_view(),
        }
    }
}

/// One row of `GET /limits`.
pub struct LimitRow {
    pub counter: String,
    pub level: u8,
    pub used: u32,
    pub limit: Option<u32>,
    pub available: Option<u32>,
    pub rule: Option<String>,
}

/// One row of `GET /vqueues`.
pub struct QueueRow {
    pub queue: String,
    pub scope: Option<String>,
    pub limit_key: Option<String>,
    pub service: String,
    pub object_key: Option<String>,
    pub exclusive: bool,
    pub waiting: usize,
    pub granted: u64,
    pub blocked_times: u64,
    pub blocked_on: Option<String>,
    /// The invocation holding the object's VO lock (`EXCLUSIVE` queue) — the answer to "who
    /// is blocking this object?".
    pub lock_holder: Option<String>,
}

/// Body of `GET /scheduler`.
pub struct SchedulerRow {
    pub eligible: usize,
    pub queues: usize,
    pub waiting: usize,
    pub turns_in_use: usize,
    pub max_turns: usize,
    pub locks_held: usize,
    pub rules_version: u64,
    /// Bucket + `vt` of each tenant on the node.
    pub tenants: Vec<crate::rate::TenantRateRow>,
    pub granted: u64,
    pub blocked: Vec<(String, u64)>,
    /// The term's VO lock rebuild is still running — the slot gate is closed (FL-8). Not in
    /// the `GET /scheduler` body: for tests and `EngineHandle`.
    pub restoring: bool,
    /// Live tasks running the range's background turns ([`TurnTask`]). Not in the
    /// `GET /scheduler` body: for tests and `EngineHandle`; the node has the gauge
    /// `cairnflow_flow_turn_tasks`.
    pub turn_tasks: usize,
    /// Live parked tickets of the range — in the line, waiting at the gate, granted but not
    /// yet holding the invocation lock, or waiting for the lock. Not in the
    /// `GET /scheduler` body; the node has the gauge `cairnflow_flow_parked`.
    pub parked: usize,
}
