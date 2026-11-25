// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Resources: the two-phase permit, `ResourceManager` (VO locks, counters, turn cap).

use super::*;

// ─────────────────────────────────────────────────────────── resources

/// Result of phase one. See the module head for why there are two phases.
pub enum AcquireOutcome {
    Acquired(Provisional),
    BlockedOn(ResourceKind),
}

/// Resources *checked* but **not yet taken**. Dropping it is a no-op.
#[must_use = "a provisional must be secured or dropped — it does nothing on its own"]
pub struct Provisional {
    id: InvocationId,
    /// `Some` = we will hold this lock (including when we already held it before).
    lock: Option<LockName>,
    counters: Vec<Counter>,
    /// The tenant's bucket + `vt`.
    meter: Arc<TenantMeter>,
    /// Counters with a `rate` rule — their buckets are charged too.
    rated: Vec<Counter>,
    /// The identity of this turn that is in `cooldown` (FL-14).
    cool: Option<CoolKey>,
}

impl Provisional {
    /// **The only place** a counter goes up and a lock is taken. `now` is when tokens are charged.
    pub fn secure(self, rm: &mut ResourceManager, now: Instant) -> Permit {
        // Tripwire (AGENTS §6.4): an invocation has at most one live slot — every ask goes
        // under the invocation lock, and a granted ticket hands its slot to the new ask
        // (`Scheduler::adopt`).
        let live = rm.live_permits.entry(self.id).or_insert(0);
        if *live > 0 {
            cairn_store::failpoint!("flow::second_live_permit");
        }
        *live += 1;
        if let Some(l) = &self.lock {
            // `insert`, not `entry`: holding and then holding again overwrites the same
            // value. An invocation that suspended and woke up goes through exactly this branch.
            rm.locks.insert(l.clone(), self.id);
        }
        for c in &self.counters {
            *rm.usage.entry(c.clone()).or_insert(0) += 1;
        }
        if let Some(k) = &self.cool {
            *rm.cool_usage.entry(k.clone()).or_insert(0) += 1;
        }
        rm.turns_in_use += 1;
        *rm.tenant_turns
            .entry(self.meter.tenant().clone())
            .or_insert(0) += 1;
        self.meter.take_turn();
        if let Some(n) = &rm.node {
            n.take();
        }
        rm.note_fed();
        // Charge up front by estimate: 1 CU, add `1 × w` to `vt` — and penalise the k-th
        // turn > 8 of the same invocation.
        let k = {
            let k = rm.turn_index.entry(self.id).or_insert(0);
            *k = k.saturating_add(1);
            *k
        };
        self.meter.charge(1, self.meter.penalty_for_turn(k), now);
        for c in &self.rated {
            if let Some(b) = rm.buckets.borrow().get(c) {
                b.charge(1, &rm.fair, now);
            }
        }
        Permit {
            id: self.id,
            lock: self.lock,
            counters: self.counters,
            meter: self.meter,
            rated: self.rated,
            cool: self.cool,
            live: true,
            unstarted: None,
        }
    }
}

/// Proof that an invocation holds a slot.
///
/// **Not `Clone`, and `Drop` of a permit that has entered a turn does not release.** A
/// silent release is right on three exit paths (done, failed, cancelled) and **wrong** on
/// the fourth (suspended — must keep the lock), so `Drop` does not have enough information
/// to do the right thing. Every exit from a turn must consume the permit explicitly via
/// [`Scheduler::release_running`] or [`Scheduler::release_all`]; forgetting is a red
/// `debug_assert` and an `error!` line in production, not a silent leak.
///
/// **Except for the gap before the turn starts.** From the moment the asker receives the
/// permit until [`Self::begin`], nothing has happened under this slot yet — exactly the
/// situation of [`Claim`], only the permit has left the channel. That gap has one `await`
/// point: the asker waits for a slot in [`crate::lease::Lease::wait`], and `wait` receives
/// the permit **and then** waits to win back the engine. A task cancelled there
/// (`JoinSet::shutdown` on losing leadership) is a permit dropped while still alive. So in
/// this gap `Drop` releases by exactly the rule of `Claim`.
#[must_use = "a permit must be released explicitly — see the Permit docs"]
pub struct Permit {
    id: InvocationId,
    pub(super) lock: Option<LockName>,
    pub(super) counters: Vec<Counter>,
    /// The turn's tenant — to settle the CU after the turn.
    meter: Arc<TenantMeter>,
    rated: Vec<Counter>,
    pub(super) cool: Option<CoolKey>,
    pub(super) live: bool,
    /// `Some` from the moment the asker receives the permit until [`Self::begin`] — the path
    /// that returns the slot if the permit is dropped before the turn starts.
    pub(super) unstarted: Option<GiveBack>,
}

/// The path that returns an **unused** slot to the scheduler: `release_all`, unless the
/// asker already held the VO lock before asking (see [`Claim`]).
pub(super) struct GiveBack {
    pub(super) sched: Scheduler,
    pub(super) held_lock: bool,
}

impl GiveBack {
    pub(super) fn give(self, permit: &mut Permit) {
        let mut st = self.sched.core.borrow_mut();
        let acts = st.give_back(permit, self.held_lock, Instant::now());
        self.sched.deliver(&mut st, acts);
    }
}

impl Permit {
    pub fn invocation(&self) -> InvocationId {
        self.id
    }

    pub fn tenant(&self) -> &TenantId {
        self.meter.tenant()
    }

    /// The turn starts under this slot: from here on the permit must be released
    /// explicitly, because only the place running the turn knows where it stops (keeping
    /// the lock or not).
    pub(crate) fn begin(&mut self) {
        self.unstarted = None;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        if let Some(back) = self.unstarted.take() {
            back.give(self);
            return;
        }
        debug_assert!(
            false,
            "permit dropped instead of released explicitly — the counters and lock of \
             this invocation stay stuck until the next leader change"
        );
        tracing::error!(
            invocation = %hex(&self.id),
            "permit dropped: slot and lock stuck until the next leader change"
        );
    }
}

pub(super) fn hex(id: &InvocationId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Counters, locks, the turn cap, and the wait list of each.
///
/// The counters are **one** table keyed by [`CounterPath`], not three tables by tier. The
/// two are equivalent — `CounterPath` already carries its depth in its own type — but with
/// one table add/subtract/read have a single code path, and with three tables they have
/// three, i.e. three places to forget one.
pub struct ResourceManager {
    pub(super) rules: RuleBook,
    pub(super) usage: HashMap<Counter, u32>,
    /// Running turns of each `cooldown` identity (FL-14). Empty almost all the time: only a
    /// slot ask **with** a `CoolKey` touches it.
    pub(super) cool_usage: HashMap<CoolKey, u32>,
    pub(super) locks: HashMap<LockName, InvocationId>,
    pub(super) turns_in_use: usize,
    /// The range's **guaranteed** share of the node's turn cap (D-c).
    pub(super) max_turns: usize,
    /// The whole node's turn cap — beyond the guaranteed share, borrow while the node has
    /// room. `None` ⇒ `max_turns` is a hard cap (standalone engine, tests).
    pub(super) node: Option<crate::engine_pool::NodeTurns>,
    /// This range is waiting for its guaranteed share because the node is full — the node
    /// has been told.
    pub(super) starving: bool,
    /// The release just took the node from full to having room: the shell notifies every
    /// range ([`crate::engine_pool::NodeTurns::announce`]) — the core does not touch channels.
    pub(super) announce: bool,
    /// Waiting queues, keyed by **concrete resource** — see [`ResourceKind`].
    pub(super) waiters: HashMap<ResourceKind, WaitList>,
    /// The tenant book (bucket + `vt`) — the node's when attached to a node, otherwise this
    /// scheduler's own.
    pub(super) fair: TenantBook,
    /// `Arc` copies of each tenant's meter: the node book (locked) is only looked up the
    /// first time. `RefCell` because `poll_acquire` is `&self` — this is a cache, not
    /// resource state (FL-3 still holds).
    pub(super) meters: RefCell<HashMap<TenantId, Arc<TenantMeter>>>,
    /// Buckets of `(tenant, counter)` with a `rate` rule (per scope).
    buckets: RefCell<HashMap<Counter, CounterBucket>>,
    /// Number of turns granted to each invocation in this term — for the k-th turn
    /// penalty. Dropped when the invocation finishes.
    turn_index: HashMap<InvocationId, u32>,
    /// `suspended` tenants: granted no slot at all. The shell pushes them in when the
    /// per-tenant limit source changes generation ([`Scheduler::sync_tenants`]).
    pub(super) refused: HashSet<TenantId>,
    /// Reschedule a suspended tenant's turns after this long ([`SUSPENDED_RETRY`]).
    pub(super) suspended_retry: Duration,
    /// Running turns **of this range** by tenant: the range's part already added to the
    /// node's `TenantMeter::turns` — give back exactly that much on release or on losing
    /// leadership ([`Self::reset`]).
    pub(super) tenant_turns: HashMap<TenantId, u32>,
    /// Live slot of each invocation (from `secure` to release) — only for the two tripwires
    /// `flow::second_live_permit`, `flow::lock_released_by_non_holder`. One entry per
    /// running turn.
    pub(super) live_permits: HashMap<InvocationId, u32>,
}

/// The meter of `tenant` in a scheduler's `meters` cache; the first time it looks up the
/// `fair` book (locked), then keeps a copy.
pub(super) fn meter_in(
    meters: &RefCell<HashMap<TenantId, Arc<TenantMeter>>>,
    fair: &TenantBook,
    tenant: &TenantId,
) -> Arc<TenantMeter> {
    if let Some(m) = meters.borrow().get(tenant) {
        return Arc::clone(m);
    }
    let m = fair.meter(tenant);
    meters.borrow_mut().insert(tenant.clone(), Arc::clone(&m));
    m
}

/// The wait list of a resource, **lazily deleted**.
///
/// A queue leaves a wait list by two paths: being woken (taken from the head), or having
/// new work / a rule change and so being put back into the eligibility loop from anywhere
/// in the list. The second path used to be a `retain` — O(number of waiters) for each new
/// request to a blocked queue. Now it only **invalidates** the old entry: each wait
/// registration carries a new ticket ([`QueueState::ticket`]), and an entry is only alive
/// while its queue is still waiting with exactly that ticket. Dead entries are skipped
/// when taken, and the whole list is swept when dead entries outnumber live ones two to
/// one — so memory stays proportional to the real number of waiters.
#[derive(Default)]
pub(super) struct WaitList {
    pub(super) entries: VecDeque<(QueueKey, u64)>,
    /// Only for [`ResourceKind::Turns`]: WFQ instead of FIFO — when saturated, this is where
    /// the service order is really decided.
    pub(super) fair: FairQueue,
    pub(super) live: usize,
}

/// Turn-cap wait line by virtual time: one FIFO per tenant (by ticket), and taking from it
/// picks the tenant with the smallest key `(priority, **current** vt)`.
///
/// The key is the `vt` at take time, not at registration: a noisy tenant's queue that
/// registered a second ago carries the `vt` of a second ago, and ordering by that snapshot
/// would put it ahead of a quiet tenant (a test measured this). The heap keeps **one**
/// entry per tenant; `vt` only grows, so old entries are fixed lazily: taking one and
/// finding `vt` larger than the key pushes it back with the new key.
///
/// `priority` is always 0 today — a slot for priority classes. (The `cooldown`
/// quarantine does not use it: it is a cap tier of `poll_acquire`,
/// [`ResourceKind::Cooldown`].)
#[derive(Default)]
pub(super) struct FairQueue {
    pub(super) by_tenant: HashMap<TenantId, VecDeque<(QueueKey, u64)>>,
    order: BinaryHeap<Reverse<(u8, u64, u64, TenantId)>>,
    len: usize,
    next: u64,
}

impl FairQueue {
    fn push(&mut self, tenant: &TenantId, vt: u64, key: QueueKey, ticket: u64) {
        let dq = self.by_tenant.entry(tenant.clone()).or_default();
        if dq.is_empty() {
            self.next += 1;
            self.order.push(Reverse((0, vt, self.next, tenant.clone())));
        }
        dq.push_back((key, ticket));
        self.len += 1;
    }

    fn pop(&mut self, vt_of: impl Fn(&TenantId) -> u64) -> Option<(QueueKey, u64)> {
        while let Some(Reverse((prio, key, n, t))) = self.order.pop() {
            let now = vt_of(&t);
            if now > key {
                self.order.push(Reverse((prio, now, n, t)));
                continue;
            }
            let Some(dq) = self.by_tenant.get_mut(&t) else {
                continue;
            };
            let item = dq.pop_front();
            if dq.is_empty() {
                self.by_tenant.remove(&t);
            } else {
                self.order.push(Reverse((prio, now, n, t)));
            }
            if item.is_some() {
                self.len -= 1;
                return item;
            }
        }
        None
    }

    fn retain(&mut self, mut keep: impl FnMut(&[u8], u64) -> bool) {
        for dq in self.by_tenant.values_mut() {
            dq.retain(|(k, t)| keep(k, *t));
        }
        self.by_tenant.retain(|_, dq| !dq.is_empty());
        self.len = self.by_tenant.values().map(VecDeque::len).sum();
        let live: HashSet<&TenantId> = self.by_tenant.keys().collect();
        self.order.retain(|Reverse((.., t))| live.contains(t));
    }

    /// Tenant at the head of the line by its stored key (may be stale — estimate only).
    fn head(&self) -> Option<&TenantId> {
        self.order.peek().map(|Reverse((.., t))| t)
    }
}

impl WaitList {
    pub(super) fn len(&self) -> usize {
        self.entries.len() + self.fair.len
    }

    pub(super) fn pop(&mut self, vt_of: impl Fn(&TenantId) -> u64) -> Option<(QueueKey, u64)> {
        if self.fair.len > 0 {
            return self.fair.pop(vt_of);
        }
        self.entries.pop_front()
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&[u8], u64) -> bool) {
        self.entries.retain(|(k, t)| keep(k, *t));
        self.fair.retain(keep);
    }
}

/// Whether entry `(key, ticket)` of a wait list is alive. Tickets never repeat, so there is
/// no need to compare the resource too.
pub(super) fn waits_with<T>(
    queues: &HashMap<QueueKey, QueueState<T>>,
    key: &[u8],
    ticket: u64,
) -> bool {
    queues
        .get(key)
        .is_some_and(|q| q.blocked_on.is_some() && q.ticket == ticket)
}

impl ResourceManager {
    pub fn new(max_turns: usize) -> Self {
        Self {
            rules: RuleBook::default(),
            usage: HashMap::default(),
            cool_usage: HashMap::default(),
            locks: HashMap::default(),
            turns_in_use: 0,
            max_turns: max_turns.max(1),
            node: None,
            starving: false,
            announce: false,
            waiters: HashMap::default(),
            fair: TenantBook::new(),
            meters: RefCell::new(HashMap::default()),
            buckets: RefCell::new(HashMap::default()),
            turn_index: HashMap::default(),
            refused: HashSet::default(),
            suspended_retry: SUSPENDED_RETRY,
            tenant_turns: HashMap::default(),
            live_permits: HashMap::default(),
        }
    }

    /// Meter of `tenant` — the cached copy, looking up the node book the first time.
    pub(super) fn meter(&self, tenant: &TenantId) -> Arc<TenantMeter> {
        meter_in(&self.meters, &self.fair, tenant)
    }

    /// Whether the bucket of counter `c` has tokens at `now`, with the current `rate` cap.
    fn counter_rate_room(&self, c: &Counter, now: Instant) -> Option<bool> {
        let (rate, _) = self.rules.rate_of(&c.path)?;
        let mut buckets = self.buckets.borrow_mut();
        let b = buckets.entry(c.clone()).or_default();
        b.configure(Some(rate));
        Some(b.positive(&self.fair, now))
    }

    /// How long (from `now`) until bucket `k` has tokens again.
    pub(super) fn rate_wait(&self, k: &RateKey, now: Instant) -> Duration {
        match k {
            RateKey::Tenant(t) => self.meter(t).until_positive(now),
            RateKey::Counter(c) => self
                .buckets
                .borrow()
                .get(c)
                .map_or(Duration::ZERO, |b| b.until_positive(&self.fair, now)),
        }
    }

    /// Settlement after the turn: the real turn cost `cu`, 1 was charged up front. `cu = 0`
    /// (the turn never reached the SDK) ⇒ refund 1.
    pub(super) fn settle(&self, permit: &Permit, cu: u64, now: Instant) {
        let extra = cu as i64 - 1;
        permit.meter.charge(extra, 0, now);
        for c in &permit.rated {
            if let Some(b) = self.buckets.borrow().get(c) {
                b.charge(extra, &self.fair, now);
            }
        }
    }

    pub fn rules(&self) -> &RuleBook {
        &self.rules
    }

    /// Phase one: **no side effects**.
    ///
    /// Wherever it is blocked it returns exactly that place, and the caller registers in
    /// only **one** wait list — the *first* blocking resource. When woken it checks again
    /// from the start and finds the next blocking place by itself, so there is no need to
    /// track several blocking places at once.
    ///
    /// The check order — lock, then counters from **shallow to deep**, then the turn cap —
    /// is not arbitrary: it goes from *scarce and long-held* resources (the VO lock, held
    /// even while suspended) to *plentiful and short* ones (a turn slot). Blocking at the
    /// scarcest place is blocking at the right place.
    pub fn poll_acquire(
        &self,
        queue: &QueueName,
        id: &InvocationId,
        cool: Option<&CoolKey>,
        now: Instant,
    ) -> AcquireOutcome {
        // The very first tier: suspended tenant. Before the VO lock — a suspended
        // invocation holding the lock keeps it, but has no turn.
        if self.refused.contains(&queue.tenant) {
            return AcquireOutcome::BlockedOn(ResourceKind::Tenant(queue.tenant.clone()));
        }
        let lock = queue.lock_name();
        if let Some(l) = &lock {
            match self.locks.get(l) {
                // Already ours: an invocation that suspended and woke up still holds the
                // lock, and it must ask again for a **slot**, not for the lock.
                Some(holder) if holder == id => {}
                Some(_) => return AcquireOutcome::BlockedOn(ResourceKind::Lock(l.clone())),
                None => {}
            }
        }

        // No scope ⇒ no counter. The unscoped path is **subject to no limit** except the
        // partition's turn cap — that is the contract of unscoped calls, and it must show
        // here rather than hide in a default rule.
        let counters: Vec<Counter> = match &queue.scope {
            None => Vec::new(),
            Some(s) => CounterPath::all_for(s, queue.limit.as_ref())
                .into_iter()
                .map(|path| Counter {
                    tenant: queue.tenant.clone(),
                    path,
                })
                .collect(),
        };
        for c in &counters {
            if let Some(max) = self.rules.limit_of(&c.path) {
                if self.usage.get(c).copied().unwrap_or(0) >= max {
                    return AcquireOutcome::BlockedOn(ResourceKind::Concurrency(c.clone()));
                }
            }
        }

        // The turn cap of the **tenant on the whole node**: like a concurrency counter of
        // the whole tenant — after the counters, before the rate (blocking earlier spends
        // no tokens).
        let meter = self.meter(&queue.tenant);
        if self.fair.turn_cap().is_some_and(|cap| meter.turns() >= cap) {
            return AcquireOutcome::BlockedOn(ResourceKind::TenantTurns(queue.tenant.clone()));
        }

        // The fourth tier: rate. After the counters — a concurrency cap that blocks first
        // spends no tokens — and before the turn cap.
        if !meter.positive(now) {
            return AcquireOutcome::BlockedOn(ResourceKind::Rate(RateKey::Tenant(
                queue.tenant.clone(),
            )));
        }
        let mut rated = Vec::new();
        if self.rules.has_counter_rates() {
            for c in &counters {
                match self.counter_rate_room(c, now) {
                    Some(false) => {
                        return AcquireOutcome::BlockedOn(ResourceKind::Rate(RateKey::Counter(
                            c.clone(),
                        )))
                    }
                    Some(true) => rated.push(c.clone()),
                    None => {}
                }
            }
        }

        // The fifth tier: the `cooldown` quarantine (FL-14). After the rate — a throttled
        // turn does not hold a place of the identity — and before the turn cap: a turn
        // waiting here takes no slot of the range.
        if let Some(k) = cool {
            if self.cool_usage.get(k).copied().unwrap_or(0) >= COOLDOWN_CAP {
                return AcquireOutcome::BlockedOn(ResourceKind::Cooldown(k.clone()));
            }
        }

        if !self.turn_room(Some(meter.vt())) {
            return AcquireOutcome::BlockedOn(ResourceKind::Turns);
        }

        AcquireOutcome::Acquired(Provisional {
            id: *id,
            lock,
            counters,
            meter,
            rated,
            cool: cool.cloned(),
        })
    }

    /// There is room for one more turn:
    ///
    /// - **Guaranteed share** (below `max_turns`): when the node has room — or when the
    ///   node leads more ranges than the total cap (floor of 1 per range).
    /// - **Borrowing** (from `max_turns` up): when the node has room **and** no range is
    ///   waiting for its guaranteed share.
    ///
    /// Not attached to a node ⇒ `max_turns` is a hard cap as before.
    ///
    /// **Borrowing by `vt`**: the tenant at the head of the line (its `vt`) may only borrow
    /// when it is not far ahead of the slowest tenant waiting on the node (`min_vt` +
    /// [`crate::rate::BORROW_SLACK_VT`]) — a noisy tenant does not take the node's free
    /// slots before other tenants. The guaranteed share is unchanged.
    fn turn_room(&self, vt: Option<u64>) -> bool {
        match &self.node {
            None => self.turns_in_use < self.max_turns,
            Some(n) if self.turns_in_use < self.max_turns => n.has_room() || n.oversubscribed(),
            Some(n) => {
                n.has_room()
                    && !n.anyone_starving()
                    && vt.is_none_or(|v| {
                        v <= self
                            .fair
                            .min_vt()
                            .saturating_add(crate::rate::BORROW_SLACK_VT)
                    })
            }
        }
    }

    /// Blocked for lack of slots while below the guaranteed share ⇒ tell the node this range
    /// is starving, so other ranges stop borrowing.
    pub(super) fn note_turns_blocked(&mut self) {
        if self.starving || self.turns_in_use >= self.max_turns {
            return;
        }
        if let Some(n) = &self.node {
            self.starving = true;
            n.starve();
        }
    }

    /// No longer starving: just granted a turn, or nobody is waiting for a slot any more.
    pub(super) fn note_fed(&mut self) {
        if self.starving {
            self.starving = false;
            if let Some(n) = &self.node {
                n.fed();
            }
        }
    }

    /// Release counters + turn cap, **keep the VO lock**. The path "turn ended, invocation
    /// suspended" and the path "turn failed on an infrastructure error, retry scheduled".
    ///
    /// Returns the resources that just got room — the caller wakes the waiters at exactly
    /// those places.
    pub(super) fn release_running(&mut self, permit: &mut Permit) -> Vec<ResourceKind> {
        permit.live = false;
        if let Some(n) = self.live_permits.get_mut(&permit.id) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.live_permits.remove(&permit.id);
            }
        }
        let mut freed = Vec::with_capacity(permit.counters.len() + 1);
        for c in &permit.counters {
            if let Some(v) = self.usage.get_mut(c) {
                *v = v.saturating_sub(1);
                if *v == 0 {
                    self.usage.remove(c);
                }
            }
            freed.push(ResourceKind::Concurrency(c.clone()));
        }
        if let Some(k) = permit.cool.take() {
            if let Some(v) = self.cool_usage.get_mut(&k) {
                *v = v.saturating_sub(1);
                if *v == 0 {
                    self.cool_usage.remove(&k);
                }
            }
            freed.push(ResourceKind::Cooldown(k));
        }
        if self.turns_in_use > 0 {
            self.turns_in_use -= 1;
            if let Some(n) = &self.node {
                self.announce |= n.give(1);
            }
        }
        freed.push(ResourceKind::Turns);
        let tenant = permit.meter.tenant();
        if let Some(n) = self.tenant_turns.get_mut(tenant) {
            *n -= 1;
            if *n == 0 {
                self.tenant_turns.remove(tenant);
            }
            self.give_tenant_turns(&permit.meter, 1);
            freed.push(ResourceKind::TenantTurns(tenant.clone()));
        }
        freed
    }

    /// Give `n` of the tenant's turns back to the node's counter; the tenant just left the
    /// cap ⇒ notify every range (its waiters in other ranges do not see this release).
    fn give_tenant_turns(&mut self, meter: &TenantMeter, n: u32) {
        let before = meter.give_turns(n);
        if self.fair.turn_cap().is_some_and(|cap| before >= cap) {
            self.announce = true;
        }
    }

    /// Release everything. The path "invocation done / failed / cancelled".
    pub(super) fn release_all(&mut self, permit: &mut Permit) -> Vec<ResourceKind> {
        self.turn_index.remove(&permit.id);
        let mut freed = self.release_running(permit);
        if let Some(l) = permit.lock.take() {
            // Only release when we **really** hold it. Clearing someone else's lock lets two
            // invocations run on one virtual object at the same time.
            if self.locks.get(&l) == Some(&permit.id) {
                // Tripwire: the VO lock is only released by exactly the slot holding it —
                // the comparison by id cannot tell two slots of one invocation apart.
                if self.live_permits.contains_key(&permit.id) {
                    cairn_store::failpoint!("flow::lock_released_by_non_holder");
                }
                self.locks.remove(&l);
            }
            freed.push(ResourceKind::Lock(l));
        }
        freed
    }

    /// `fair` = tenant plus current `vt` for the turn cap's WFQ wait line; `None` = FIFO.
    pub(super) fn add_waiter(
        &mut self,
        res: ResourceKind,
        queue: QueueKey,
        ticket: u64,
        fair: Option<(TenantId, u64)>,
    ) {
        let list = self.waiters.entry(res).or_default();
        match fair {
            Some((tenant, vt)) => list.fair.push(&tenant, vt, queue, ticket),
            None => list.entries.push_back((queue, ticket)),
        }
        list.live += 1;
    }

    /// Whether the resource has room for **one** more.
    ///
    /// For a lock "room" means nobody holds it; but locks do not go through here — see
    /// [`Core::wake`].
    pub(super) fn has_room(&self, res: &ResourceKind, now: Instant) -> bool {
        match res {
            ResourceKind::Lock(l) => !self.locks.contains_key(l),
            ResourceKind::Concurrency(c) => self
                .rules
                .limit_of(&c.path)
                .is_none_or(|max| self.usage.get(c).copied().unwrap_or(0) < max),
            ResourceKind::Turns => self.turn_room(
                self.waiters
                    .get(res)
                    .and_then(|w| w.fair.head())
                    .map(|t| self.meter(t).vt()),
            ),
            ResourceKind::Rate(RateKey::Tenant(t)) => self.meter(t).positive(now),
            ResourceKind::Rate(RateKey::Counter(c)) => {
                self.counter_rate_room(c, now).unwrap_or(true)
            }
            ResourceKind::Cooldown(k) => {
                self.cool_usage.get(k).copied().unwrap_or(0) < COOLDOWN_CAP
            }
            ResourceKind::Tenant(t) => !self.refused.contains(t),
            ResourceKind::TenantTurns(t) => self
                .fair
                .turn_cap()
                .is_none_or(|cap| self.meter(t).turns() < cap),
        }
    }

    /// Set a new rule book. Returns `true` if anything changed.
    pub(super) fn set_rules(&mut self, book: RuleBook) -> bool {
        // The per-tenant rate cap lives in the (node's) book: load it there — the book
        // skips versions it has already seen.
        self.fair.set_rules(&book);
        if book.version == self.rules.version && !self.rules.is_empty() {
            return false;
        }
        let changed = book != self.rules;
        self.rules = book;
        changed
    }

    /// Set the suspended tenant set. Returns `true` if anything changed.
    pub(super) fn set_refused(&mut self, refused: HashSet<TenantId>) -> bool {
        if refused == self.refused {
            return false;
        }
        self.refused = refused;
        true
    }

    /// Drop all state. Called on losing leadership — the counters are rebuilt from the
    /// store on becoming leader, nothing old is carried over.
    pub(super) fn reset(&mut self) {
        self.usage.clear();
        self.cool_usage.clear();
        self.live_permits.clear();
        self.locks.clear();
        // The old term's turns — including abandoned permits — go back to the node's
        // counter, otherwise the whole node's borrowable share shrinks with every owner change.
        if let Some(n) = &self.node {
            self.announce |= n.give(self.turns_in_use);
        }
        self.turns_in_use = 0;
        // The range's per-tenant turns go back to the node's counter — permits abandoned on
        // losing leadership can no longer be released.
        let held: Vec<(TenantId, u32)> = self.tenant_turns.drain().collect();
        for (t, n) in held {
            let meter = self.meter(&t);
            self.give_tenant_turns(&meter, n);
        }
        self.note_fed();
        self.waiters.clear();
        self.buckets.borrow_mut().clear();
        self.turn_index.clear();
    }

    /// Rebuild a VO lock that is being held — the recovery path on becoming leader.
    pub(super) fn restore_lock(&mut self, lock: LockName, id: InvocationId) {
        self.locks.insert(lock, id);
    }
}

impl Drop for ResourceManager {
    /// The range's scheduler is dropped (retired, owner changed) while it still has
    /// half-done turns: its per-tenant turns leave the node's counter right away — like the
    /// cell of the node turn cap (`NodeTurns::drop`). Otherwise the tenant would stay stuck
    /// at the cap forever. The announcement goes out before the range's `node` cell drops
    /// itself.
    fn drop(&mut self) {
        let held: Vec<(TenantId, u32)> = self.tenant_turns.drain().collect();
        let mut left_cap = false;
        for (t, n) in held {
            let before = self.meter(&t).give_turns(n);
            left_cap |= self.fair.turn_cap().is_some_and(|cap| before >= cap);
        }
        if left_cap {
            if let Some(node) = &self.node {
                node.announce();
            }
        }
    }
}
