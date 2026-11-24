// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Rate throttling and fairness between tenants, at the **node** level.
//!
//! One [`TenantBook`] per node (carried by [`crate::engine_pool::TurnBudget`],
//! shared by every range of the node). Each tenant has one [`TenantMeter`] holding two numbers:
//!
//! - **Token bucket** (CU, the unit of [`crate::metering::run_cu`]) — the rate limit of the
//!   `tenant` line in the rule book, **split evenly** across the number of nodes leading at
//!   least one range: the total never exceeds `rate`. Before a turn 1 CU is taken (an
//!   estimate), after the turn `CU − 1` is settled (inspired by TiDB's RU v2) — the bucket
//!   may go negative: when negative the next turn waits (debt).
//! - **Virtual time** (`vt`) for WFQ: each CU adds `w`
//!   (`w = max_rate / rate`, a tenant without `rate` has `w = 1`). Each range's scheduler
//!   orders the waiters for the turn limit by `vt` ([`crate::flow`]).
//!
//! # No lock on the turn path
//!
//! The bucket is GCRA on **one** `AtomicU64` (the theoretical arrival time — `tat`):
//! taking `c` CU is `tat = max(tat, now) + c·I`, still positive while `tat − now < τ`
//! (`I` = ns per CU, `τ = burst·I`). No refill loop, no lock. `vt` is an atomic too. The
//! tenant → meter table has a `Mutex`, but it is only touched when a range meets a tenant
//! **for the first time** (each scheduler keeps an `Arc` copy of the meter), when the rule
//! book or the node count changes, and on the background sweep tick — never on the slot
//! acquire/release path (AGENTS.md §6).
//!
//! **No implicit clock**: every bucket operation takes `now`
//! (`tokio::time::Instant`) from the caller — the scheduler reads it once for the whole
//! scheduling round. The book only keeps a mark (`epoch`) to turn `now` into ns; the core
//! [`crate::flow::ResourceManager`] is therefore a function of `now`, runnable without a
//! runtime.
//!
//! # The sweep tick
//!
//! Every [`SWEEP_EVERY`]: `min_vt` = the smallest `vt` among the tenants **with waiters**
//! on the node (`demand > 0`) — the mark for the cross-range slot borrowing rule. Every
//! [`CATCH_UP_EVERY`]: pull lagging tenants up by `vt += (max − vt) / 2` — a tenant asleep
//! for long cannot accumulate credit to wake up and take over the whole queue. A new tenant
//! starts at `min_vt` (or at the lowest current level if nobody is waiting).

use cairn_store::entropy::HashMap;
use {
    cairn_store::{Rate, RuleBook, TenantId},
    std::{
        sync::{
            atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::Instant,
};

/// One CU adds this many `vt` units when the weight is 1.
pub const VT_PER_CU: u64 = 1_000;

/// The k-th turn, k > [`PENALTY_AFTER_TURNS`], of the same invocation gets an extra penalty
/// of `0.2 × w`: a long-running invocation cannot keep its place at the head of the queue
/// thanks to its tenant's `vt`.
pub const PENALTY_AFTER_TURNS: u32 = 8;
pub const PENALTY_VT: u64 = VT_PER_CU / 5;

/// Slack of the cross-range slot borrowing rule: the tenant at the head of a range's queue
/// may only borrow when its `vt` ≤ the node's `min_vt` + this much
/// (32 CU at weight 1).
pub const BORROW_SLACK_VT: u64 = 32 * VT_PER_CU;

/// Interval for recomputing `min_vt`.
pub const SWEEP_EVERY: Duration = Duration::from_millis(100);
/// Interval for pulling lagging tenants up ("every second").
pub const CATCH_UP_EVERY: Duration = Duration::from_secs(1);

/// Turns blocked for lack of tokens (tenant and counter), every node of the process.
pub static BLOCKED_RATE: AtomicU64 = AtomicU64::new(0);

/// Lock-free GCRA bucket. `interval_ns = 0` ⇔ unlimited.
#[derive(Debug, Default)]
struct Gcra {
    tat: AtomicU64,
    interval_ns: AtomicU64,
    tau_ns: AtomicU64,
}

impl Gcra {
    fn configure(&self, rate: Option<Rate>, share: u32) {
        match rate {
            None => {
                self.interval_ns.store(0, Ordering::Release);
                self.tau_ns.store(0, Ordering::Release);
            }
            Some(r) => {
                // The node's share: `per_sec / share` CU/s, `burst / share` CU
                // (at least 1 — a bucket that is never positive would hang the tenant).
                let share = u64::from(share.max(1));
                let interval = (1_000_000_000 * share / u64::from(r.per_sec.max(1))).max(1);
                let burst = (u64::from(r.burst) / share).max(1);
                self.interval_ns.store(interval, Ordering::Release);
                self.tau_ns.store(burst * interval, Ordering::Release);
            }
        }
    }

    fn limited(&self) -> bool {
        self.interval_ns.load(Ordering::Acquire) > 0
    }

    /// Tokens left (balance > 0).
    fn positive(&self, now: u64) -> bool {
        let i = self.interval_ns.load(Ordering::Acquire);
        i == 0 || self.tat.load(Ordering::Acquire) < now + self.tau_ns.load(Ordering::Acquire)
    }

    /// How long until the balance becomes positive. 0 = positive now.
    fn until_positive(&self, now: u64) -> Duration {
        let i = self.interval_ns.load(Ordering::Acquire);
        if i == 0 {
            return Duration::ZERO;
        }
        let ready = self
            .tat
            .load(Ordering::Acquire)
            .saturating_sub(self.tau_ns.load(Ordering::Acquire));
        if ready < now {
            Duration::ZERO
        } else {
            // +1 ns: at exactly `ready` the balance is 0, not yet positive.
            Duration::from_nanos(ready - now + 1)
        }
    }

    /// Take `cu` CU (negative = refund). A refund does not push the balance above `burst`.
    fn charge(&self, cu: i64, now: u64) {
        let i = self.interval_ns.load(Ordering::Acquire);
        if i == 0 || cu == 0 {
            return;
        }
        let _ = self
            .tat
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |tat| {
                Some(if cu > 0 {
                    tat.max(now).saturating_add(cu as u64 * i)
                } else if tat > now {
                    tat.saturating_sub(cu.unsigned_abs() * i).max(now)
                } else {
                    tat
                })
            });
    }

    /// Current balance, CU (negative = debt) — for display.
    fn balance(&self, now: u64) -> Option<f64> {
        let i = self.interval_ns.load(Ordering::Acquire);
        if i == 0 {
            return None;
        }
        let tat = self.tat.load(Ordering::Acquire).max(now);
        let tau = self.tau_ns.load(Ordering::Acquire);
        Some((now as f64 + tau as f64 - tat as f64) / i as f64)
    }
}

/// Bucket + `vt` of one tenant on the node.
#[derive(Debug)]
pub struct TenantMeter {
    tenant: TenantId,
    bucket: Gcra,
    vt: AtomicU64,
    /// `w × VT_PER_CU`.
    weight: AtomicU64,
    /// Number of the tenant's slot requests waiting on the node — see [`Demand`].
    demand: AtomicI64,
    /// The tenant's **running** turns on the node, summed over every range — compared with
    /// [`TenantBook::turn_cap`]. Each range adds/subtracts exactly what it granted
    /// (`flow::ResourceManager::tenant_turns`), including when it loses leadership.
    turns: AtomicU32,
    /// The limit in effect (cluster-wide, before splitting), `per_sec`/`burst`; 0 = none.
    per_sec: AtomicU32,
    burst: AtomicU32,
    /// The book's clock mark — `now` of every bucket operation is turned into ns counted
    /// from here.
    epoch: Instant,
}

impl TenantMeter {
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    fn ns(&self, now: Instant) -> u64 {
        ns_since(self.epoch, now)
    }

    /// Whether the tenant has tokens to start a turn, at `now`.
    pub fn positive(&self, now: Instant) -> bool {
        !self.bucket.limited() || self.bucket.positive(self.ns(now))
    }

    /// How long (from `now`) until the tenant has tokens again (`Retry-After`).
    pub fn until_positive(&self, now: Instant) -> Duration {
        self.bucket.until_positive(self.ns(now))
    }

    pub fn limited(&self) -> bool {
        self.bucket.limited()
    }

    /// The tenant's running turns on the node.
    pub fn turns(&self) -> u32 {
        self.turns.load(Ordering::Acquire)
    }

    /// One of the tenant's turns was just granted on a range.
    pub(crate) fn take_turn(&self) {
        self.turns.fetch_add(1, Ordering::AcqRel);
    }

    /// `n` of the tenant's turns were just released (or just left a range that lost
    /// leadership). Returns the number of turns **before** the release — the caller compares
    /// it with the limit to know whether the tenant just went below it (its waiters on other
    /// ranges need the node's notice).
    pub(crate) fn give_turns(&self, n: u32) -> u32 {
        self.turns
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(n))
            })
            .unwrap_or(0)
    }

    pub fn vt(&self) -> u64 {
        self.vt.load(Ordering::Acquire)
    }

    /// Take `cu` CU from the bucket at `now` and add `cu × w` (+ `penalty`) to
    /// `vt`. A negative `cu` is a refund (the turn never reached the SDK) — only the bucket is
    /// refunded; `vt` does not go back.
    pub fn charge(&self, cu: i64, penalty: u64, now: Instant) {
        self.bucket.charge(cu, self.ns(now));
        let add = (cu.max(0) as u64)
            .saturating_mul(self.weight.load(Ordering::Acquire))
            .saturating_add(penalty);
        if add > 0 {
            self.vt.fetch_add(add, Ordering::AcqRel);
        }
    }

    /// Penalty for the k-th turn (already multiplied by the weight).
    pub fn penalty_for_turn(&self, k: u32) -> u64 {
        if k > PENALTY_AFTER_TURNS {
            PENALTY_VT.saturating_mul(self.weight.load(Ordering::Acquire)) / VT_PER_CU
        } else {
            0
        }
    }

    /// Balance (CU) at `now` — `None` when unlimited.
    pub fn balance(&self, now: Instant) -> Option<f64> {
        self.bucket.balance(self.ns(now))
    }

    fn configure(&self, rate: Option<Rate>, share: u32, max_rate: u32) {
        self.bucket.configure(rate, share);
        self.per_sec
            .store(rate.map_or(0, |r| r.per_sec), Ordering::Release);
        self.burst
            .store(rate.map_or(0, |r| r.burst), Ordering::Release);
        let w = match rate {
            Some(r) if max_rate > r.per_sec => {
                VT_PER_CU * u64::from(max_rate) / u64::from(r.per_sec.max(1))
            }
            _ => VT_PER_CU,
        };
        self.weight.store(w, Ordering::Release);
    }
}

/// One of the tenant's slot requests waiting on the node: `demand += 1` on creation, `−1`
/// on drop — whatever the path (granted, abandoned, queue cleared, owner change), so there
/// is no counter to leak.
#[derive(Debug)]
pub struct Demand(Arc<TenantMeter>);

impl Demand {
    pub fn new(m: &Arc<TenantMeter>) -> Self {
        m.demand.fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(m))
    }
}

impl Drop for Demand {
    fn drop(&mut self) {
        self.0.demand.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct Inner {
    epoch: Instant,
    meters: Mutex<Meters>,
    /// Number of nodes sharing a tenant's limit. ≥ 1.
    peers: AtomicU32,
    /// Smallest `vt` among tenants with waiters; `u64::MAX` = nobody.
    min_vt: AtomicU64,
    /// The `vt` level for a new tenant when nobody is waiting.
    floor_vt: AtomicU64,
    /// Limit on running turns of **one** tenant on the node
    /// (`--max-turns-per-tenant`). `0` = no limit.
    turn_cap: AtomicU32,
}

#[derive(Debug, Default)]
struct Meters {
    by_tenant: HashMap<TenantId, Arc<TenantMeter>>,
    rates: HashMap<TenantId, Rate>,
    max_rate: u32,
    rules_version: Option<u64>,
    last_catch_up: Option<Instant>,
}

/// A node's tenant book — see the module's top. `Clone` is cheap.
#[derive(Clone, Debug)]
pub struct TenantBook(Arc<Inner>);

impl Default for TenantBook {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantBook {
    pub fn new() -> Self {
        Self::starting_at(Instant::now())
    }

    /// A book with the clock mark `epoch` given — the sim sets the mark to its virtual time,
    /// so every bucket operation is a function of the seed.
    pub(crate) fn starting_at(epoch: Instant) -> Self {
        Self(Arc::new(Inner {
            epoch,
            meters: Mutex::new(Meters::default()),
            peers: AtomicU32::new(1),
            min_vt: AtomicU64::new(u64::MAX),
            floor_vt: AtomicU64::new(0),
            turn_cap: AtomicU32::new(0),
        }))
    }

    /// Set the limit on running turns of one tenant on the node.
    /// `None` = no limit.
    pub fn set_turn_cap(&self, cap: Option<usize>) {
        let cap = cap.map_or(0, |c| u32::try_from(c.max(1)).unwrap_or(u32::MAX));
        self.0.turn_cap.store(cap, Ordering::Release);
    }

    /// The limit on running turns of one tenant on the node, if any.
    pub fn turn_cap(&self) -> Option<u32> {
        match self.0.turn_cap.load(Ordering::Acquire) {
            0 => None,
            n => Some(n),
        }
    }

    /// The meter of `tenant`, created if missing (a new tenant starts at `min_vt`).
    /// Locks the table — the caller keeps an `Arc` copy, not calling again on every turn.
    pub fn meter(&self, tenant: &TenantId) -> Arc<TenantMeter> {
        let mut m = self.0.meters.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(x) = m.by_tenant.get(tenant) {
            return Arc::clone(x);
        }
        let start = match self.0.min_vt.load(Ordering::Acquire) {
            u64::MAX => self.0.floor_vt.load(Ordering::Acquire),
            v => v,
        };
        let meter = Arc::new(TenantMeter {
            tenant: tenant.clone(),
            bucket: Gcra::default(),
            vt: AtomicU64::new(start),
            weight: AtomicU64::new(VT_PER_CU),
            demand: AtomicI64::new(0),
            turns: AtomicU32::new(0),
            per_sec: AtomicU32::new(0),
            burst: AtomicU32::new(0),
            epoch: self.0.epoch,
        });
        meter.configure(m.rates.get(tenant).copied(), self.peers(), m.max_rate);
        m.by_tenant.insert(tenant.clone(), Arc::clone(&meter));
        meter
    }

    pub fn peers(&self) -> u32 {
        self.0.peers.load(Ordering::Acquire).max(1)
    }

    /// Load the per-tenant rate limits from the rule book (skipped if `version` was already seen).
    pub fn set_rules(&self, book: &RuleBook) {
        let mut m = self.0.meters.lock().unwrap_or_else(|e| e.into_inner());
        if m.rules_version == Some(book.version) {
            return;
        }
        m.rules_version = Some(book.version);
        m.rates = book.tenant_rates().map(|(t, r)| (t.clone(), r)).collect();
        m.max_rate = m.rates.values().map(|r| r.per_sec).max().unwrap_or(0);
        self.reconfigure(&m);
    }

    /// The number of nodes leading at least one range changed.
    pub fn set_peers(&self, n: u32) {
        let n = n.max(1);
        if self.0.peers.swap(n, Ordering::AcqRel) == n {
            return;
        }
        let m = self.0.meters.lock().unwrap_or_else(|e| e.into_inner());
        self.reconfigure(&m);
    }

    fn reconfigure(&self, m: &Meters) {
        let peers = self.peers();
        for (t, meter) in &m.by_tenant {
            meter.configure(m.rates.get(t).copied(), peers, m.max_rate);
        }
    }

    /// `now` turned into ns counted from the book's mark — the unit of every bucket.
    fn ns(&self, now: Instant) -> u64 {
        ns_since(self.0.epoch, now)
    }

    /// Smallest `vt` among the tenants waiting for slots on the node, as of the latest
    /// sweep. `u64::MAX` = nobody waiting.
    pub fn min_vt(&self) -> u64 {
        self.0.min_vt.load(Ordering::Acquire)
    }

    /// One sweep tick at `now` — see the module's top.
    /// `catch_up` forces pulling lagging tenants up right away (tests); normally every
    /// [`CATCH_UP_EVERY`].
    pub fn sweep(&self, now: Instant, catch_up: bool) {
        let mut m = self.0.meters.lock().unwrap_or_else(|e| e.into_inner());
        let due = catch_up
            || m.last_catch_up
                .is_none_or(|t| now.duration_since(t) >= CATCH_UP_EVERY);
        if due {
            m.last_catch_up = Some(now);
            let max = m.by_tenant.values().map(|x| x.vt()).max().unwrap_or(0);
            for x in m.by_tenant.values() {
                let _ = x.vt.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    (v < max).then(|| v + (max - v) / 2)
                });
            }
        }
        let waiting = m
            .by_tenant
            .values()
            .filter(|x| x.demand.load(Ordering::Acquire) > 0);
        let min = waiting.map(|x| x.vt()).min().unwrap_or(u64::MAX);
        self.0.min_vt.store(min, Ordering::Release);
        let floor = m.by_tenant.values().map(|x| x.vt()).min().unwrap_or(0);
        self.0.floor_vt.store(floor, Ordering::Release);
    }

    /// Run [`Self::sweep`] every [`SWEEP_EVERY`] on the current runtime, until every copy of
    /// the book has been dropped. No runtime ⇒ does nothing (tests that build the book
    /// outside a runtime call `sweep` themselves).
    pub fn spawn_sweeper(&self) {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let weak = Arc::downgrade(&self.0);
        rt.spawn(async move {
            loop {
                cairn_store::pace::beat(SWEEP_EVERY).await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                TenantBook(inner).sweep(Instant::now(), false);
            }
        });
    }

    /// Snapshot for `GET /scheduler` — tenants with a limit or with waiters.
    pub fn view(&self) -> Vec<TenantRateRow> {
        let now = Instant::now();
        let m = self.0.meters.lock().unwrap_or_else(|e| e.into_inner());
        let peers = self.peers();
        let mut out: Vec<TenantRateRow> = m
            .by_tenant
            .values()
            .filter(|x| x.limited() || x.demand.load(Ordering::Acquire) > 0)
            .map(|x| TenantRateRow {
                tenant: x.tenant.to_string(),
                rate: (x.per_sec.load(Ordering::Acquire) > 0)
                    .then(|| x.per_sec.load(Ordering::Acquire)),
                burst: (x.burst.load(Ordering::Acquire) > 0)
                    .then(|| x.burst.load(Ordering::Acquire)),
                peers,
                balance: x.balance(now),
                vt: x.vt(),
                waiting: x.demand.load(Ordering::Acquire).max(0) as u64,
            })
            .collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }
}

/// Interval for recounting the nodes sharing a tenant's limit.
pub const PEERS_EVERY: Duration = Duration::from_secs(2);

/// Count the nodes **leading at least one range** by the routing table, every
/// [`PEERS_EVERY`], and set it in `book`: each node keeps
/// `rate / that count`. Conservative: the total does not exceed `rate` — except in the gap
/// where a node has just taken a range that this node's table does not see yet (at most one
/// refresh round of the routing table). This node is always counted (it runs the tenant's turns).
pub fn spawn_peer_count(
    book: TenantBook,
    routing: crate::routing::RoutingTable,
    me: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(cairn_store::io_source::tagged("rate", async move {
        loop {
            let mut leaders: cairn_store::entropy::HashSet<String> = routing
                .snapshot()
                .into_values()
                .map(|l| l.node_id)
                .collect();
            leaders.insert(me.clone());
            book.set_peers(leaders.len() as u32);
            cairn_store::pace::beat(PEERS_EVERY).await;
        }
    }))
}

/// `now` in ns from `epoch`. Before the mark (impossible with a monotonic
/// clock) ⇒ 0.
fn ns_since(epoch: Instant, now: Instant) -> u64 {
    now.saturating_duration_since(epoch).as_nanos() as u64
}

/// One tenant row of `GET /scheduler`.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantRateRow {
    pub tenant: String,
    /// The cluster-wide limit (CU/s); this node keeps `rate / peers`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst: Option<u32>,
    pub peers: u32,
    /// CU balance of the bucket on this node (negative = debt).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<f64>,
    pub vt: u64,
    pub waiting: u64,
}

/// The bucket of **one** `(tenant, counter)` with a `rate` rule — it lives in the range's
/// scheduler (the scope decides the range, D-F1), so the limit is not split across nodes
/// and needs no atomic. The clock mark is the [`TenantBook`]'s so that `until_positive`
/// values compare with each other; `now` is given by the caller.
#[derive(Debug, Default)]
pub struct CounterBucket(Gcra);

impl CounterBucket {
    pub fn configure(&self, rate: Option<Rate>) {
        self.0.configure(rate, 1);
    }
    pub fn positive(&self, book: &TenantBook, now: Instant) -> bool {
        self.0.positive(book.ns(now))
    }
    pub fn until_positive(&self, book: &TenantBook, now: Instant) -> Duration {
        self.0.until_positive(book.ns(now))
    }
    pub fn charge(&self, cu: i64, book: &TenantBook, now: Instant) {
        self.0.charge(cu, book.ns(now));
    }
}

// ─────────────────────────────────────────── per-key limits (login)

impl Gcra {
    /// A fixed bucket: `burst` at once, then one every `every`.
    fn fixed(every: Duration, burst: u32) -> Self {
        let i = u64::try_from(every.as_nanos()).unwrap_or(u64::MAX).max(1);
        Self {
            tat: AtomicU64::new(0),
            interval_ns: AtomicU64::new(i),
            tau_ns: AtomicU64::new(u64::from(burst.max(1)).saturating_mul(i)),
        }
    }

    /// Whether there is room to take **one** slot without debt at `now`; otherwise `Err(wait)`.
    /// Unlike [`Self::positive`] (for turns that run while the balance is positive and settle
    /// later, debt allowed): here `burst` times is exactly `burst` times.
    fn room(&self, now: u64) -> Result<(), Duration> {
        let i = self.interval_ns.load(Ordering::Acquire);
        let tau = self.tau_ns.load(Ordering::Acquire);
        let next = self.tat.load(Ordering::Acquire).max(now).saturating_add(i);
        if next <= now.saturating_add(tau) {
            Ok(())
        } else {
            Err(Duration::from_nanos(next - now.saturating_add(tau)))
        }
    }

    /// Fully refilled at `now` — dropping it loses nothing.
    fn full(&self, now: u64) -> bool {
        self.tat.load(Ordering::Acquire) <= now
    }
}

/// The key of a client in the per-IP limiter: IPv4 (including IPv4 embedded in IPv6) is
/// kept as is; IPv6 is cut down to /64 — an IPv6 host is usually given a whole /64 and
/// changes address within it at will, so one bucket per /128 would mean no limit.
pub fn client_key(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip.to_canonical() {
        std::net::IpAddr::V6(v6) => {
            let bits = u128::from(v6) & !((1u128 << 64) - 1);
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(bits))
        }
        v4 => v4,
    }
}

/// A per-key GCRA bucket for **non-hot** paths — console login per IP and per key
/// prefix. The core is the tenant [`Gcra`]; the table has a `Mutex` because login is not
/// on the turn path.
///
/// The table is limited to `max_keys`: at the limit, fully refilled keys are cleaned out;
/// if still full, a **new** key counts as out of slots — fail closed, so someone with many
/// IPs cannot push a blocked key out of the table.
pub struct KeyedLimiter<K> {
    every: Duration,
    burst: u32,
    max_keys: usize,
    epoch: Instant,
    buckets: Mutex<HashMap<K, Gcra>>,
    /// Mark (ns from `epoch`) of the latest full-table cleanup, and the number of cleanups:
    /// a full table is cleaned at most once every `every` — not scanning `max_keys`
    /// entries under the lock on **every** request of a new key.
    swept_at: std::sync::atomic::AtomicU64,
    sweeps: std::sync::atomic::AtomicU64,
}

impl<K: std::hash::Hash + Eq + Clone> KeyedLimiter<K> {
    pub fn new(burst: u32, every: Duration, max_keys: usize) -> Self {
        Self {
            every,
            burst,
            max_keys: max_keys.max(1),
            epoch: Instant::now(),
            buckets: Mutex::new(HashMap::default()),
            swept_at: std::sync::atomic::AtomicU64::new(0),
            sweeps: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The table is full and `key` is new: clean out fully refilled keys — at most once every
    /// `every`. `true` = there is room.
    fn room_for_new(&self, map: &mut HashMap<K, Gcra>, now: u64) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if map.len() < self.max_keys {
            return true;
        }
        let last = self.swept_at.load(Relaxed);
        let every = u64::try_from(self.every.as_nanos()).unwrap_or(u64::MAX);
        if self.sweeps.load(Relaxed) > 0 && now.saturating_sub(last) < every {
            return false;
        }
        self.swept_at.store(now, Relaxed);
        self.sweeps.fetch_add(1, Relaxed);
        map.retain(|_, b| !b.full(now));
        map.len() < self.max_keys
    }

    /// Number of full-table cleanups so far (tests).
    pub fn sweeps(&self) -> u64 {
        self.sweeps.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `Err(wait)` when `key` is out of slots at `now`. Takes nothing.
    pub fn check(&self, key: &K, now: Instant) -> Result<(), Duration> {
        let now = ns_since(self.epoch, now);
        let mut map = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(b) = map.get(key) {
            return b.room(now);
        }
        if self.room_for_new(&mut map, now) {
            Ok(())
        } else {
            Err(self.every)
        }
    }

    /// Check **and** take one slot of `key` at `now` while holding the lock once —
    /// two concurrent requests do not both pass the check and then both take.
    /// `Err(wait)` when out of slots (takes nothing).
    pub fn take(&self, key: &K, now: Instant) -> Result<(), Duration> {
        let now = ns_since(self.epoch, now);
        let mut map = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        if !map.contains_key(key) && !self.room_for_new(&mut map, now) {
            return Err(self.every);
        }
        let b = map
            .entry(key.clone())
            .or_insert_with(|| Gcra::fixed(self.every, self.burst));
        b.room(now)?;
        b.charge(1, now);
        Ok(())
    }

    /// Give back one slot of `key` taken earlier (never above `burst`).
    pub fn refund(&self, key: &K, now: Instant) {
        let now = ns_since(self.epoch, now);
        let map = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(b) = map.get(key) {
            b.charge(-1, now);
        }
    }

    /// Take one slot of `key` at `now`.
    pub fn charge(&self, key: &K, now: Instant) {
        let now = ns_since(self.epoch, now);
        let mut map = self.buckets.lock().unwrap_or_else(|p| p.into_inner());
        if !map.contains_key(key) && !self.room_for_new(&mut map, now) {
            return;
        }
        map.entry(key.clone())
            .or_insert_with(|| Gcra::fixed(self.every, self.burst))
            .charge(1, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A full table is not scanned on every new key.**
    /// At `max_keys`, every new key used to scan the whole table under the shared lock.
    /// Must see: 1 000 new keys within one tick ⇒ ≤ 1 cleanup, the new keys are
    /// refused (fail closed); after one tick ⇒ cleaning works again.
    #[test]
    fn full_keyed_limiter_sweeps_at_most_once_per_period() {
        let l = KeyedLimiter::<u32>::new(5, Duration::from_secs(1), 100);
        let t0 = Instant::now();
        for k in 0..100 {
            l.take(&k, t0).unwrap();
        }
        for k in 100..1_100 {
            assert!(
                l.take(&k, t0).is_err(),
                "table full but new key {k} got through"
            );
        }
        assert!(l.sweeps() <= 1, "{} cleanups within one tick", l.sweeps());
        let later = t0 + Duration::from_secs(10);
        l.take(&5_000, later)
            .expect("after a tick, fully refilled keys are cleaned out");
        assert_eq!(l.sweeps(), 2);
    }

    /// `refund` gives back one slot taken by `take`, never more than `burst`.
    #[test]
    fn refund_gives_back_one_slot() {
        let l = KeyedLimiter::<u32>::new(2, Duration::from_secs(60), 100);
        let t0 = Instant::now();
        l.take(&1, t0).unwrap();
        l.take(&1, t0).unwrap();
        assert!(l.take(&1, t0).is_err(), "burst 2");
        l.refund(&1, t0);
        l.take(&1, t0).expect("a refund gives one slot back");
        assert!(l.take(&1, t0).is_err());
        l.refund(&2, t0);
        l.refund(&1, t0);
        l.refund(&1, t0);
        l.refund(&1, t0);
        l.take(&1, t0).unwrap();
        l.take(&1, t0).unwrap();
        assert!(l.take(&1, t0).is_err(), "refunds never go above burst");
    }

    #[test]
    fn client_key_groups_v6_by_64_and_keeps_v4() {
        let k = |s: &str| client_key(s.parse().unwrap()).to_string();
        assert_eq!(k("2001:db8:1:2::1"), "2001:db8:1:2::");
        assert_eq!(k("2001:db8:1:2:ffff:ffff:ffff:ffff"), "2001:db8:1:2::");
        assert_ne!(k("2001:db8:1:3::1"), k("2001:db8:1:2::1"));
        assert_eq!(k("198.51.100.7"), "198.51.100.7");
        assert_eq!(k("::ffff:198.51.100.7"), "198.51.100.7");
    }

    #[test]
    fn take_checks_and_charges_at_once() {
        let l = KeyedLimiter::new(2, Duration::from_secs(10), 4);
        let t = Instant::now();
        assert!(l.take(&1u8, t).is_ok());
        assert!(l.take(&1u8, t).is_ok());
        assert!(l.take(&1u8, t).is_err(), "out of slots");
        assert!(
            l.take(&2u8, t).is_ok(),
            "another key does not share the bucket"
        );
    }

    fn tenant(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }

    fn book_with(rate: u32, burst: Option<u32>) -> RuleBook {
        let mut b = RuleBook::default();
        b.apply_tenant(
            "a",
            cairn_store::RuleEdit {
                rate: Some(cairn_store::Limit::Max(rate)),
                burst,
                ..Default::default()
            },
            0,
        )
        .unwrap();
        b.version = 1;
        b
    }

    /// The bucket is full at `burst`, may go negative, refills at `rate` — and splits by node count.
    #[tokio::test(start_paused = true)]
    async fn bucket_starts_full_goes_negative_refills_by_rate_split_by_node() {
        let book = TenantBook::new();
        book.set_rules(&book_with(10, Some(3)));
        let a = book.meter(&tenant("a"));
        assert!(a.limited());
        for _ in 0..3 {
            assert!(a.positive(Instant::now()));
            a.charge(1, 0, Instant::now());
        }
        assert!(!a.positive(Instant::now()), "burst of 3 used up");
        a.charge(5, 0, Instant::now());
        // A debt of 5 CU at 10 CU/s ⇒ positive again after a bit more than 0.5 s.
        let wait = a.until_positive(Instant::now());
        assert!(
            wait > Duration::from_millis(500) && wait <= Duration::from_millis(501),
            "{wait:?}"
        );
        tokio::time::advance(wait).await;
        assert!(a.positive(Instant::now()));
        // A refund does not exceed the burst.
        a.charge(-100, 0, Instant::now());
        assert!(a.balance(Instant::now()).unwrap() <= 3.0 + 1e-9);
        // Two nodes: 5 CU/s each, burst ⌊3/2⌋ = 1.
        book.set_peers(2);
        tokio::time::advance(Duration::from_secs(10)).await;
        a.charge(2, 0, Instant::now());
        assert!(!a.positive(Instant::now()));
        let wait = a.until_positive(Instant::now());
        assert!(
            wait > Duration::from_millis(200) && wait <= Duration::from_millis(201),
            "{wait:?}"
        );
        // A tenant without a rate: never blocked.
        let b = book.meter(&tenant("b"));
        b.charge(1_000_000, 0, Instant::now());
        assert!(b.positive(Instant::now()) && !b.limited());
    }

    /// `vt` grows by the weight `max_rate / rate`; a new tenant starts at
    /// `min_vt`; the sweep tick pulls lagging tenants up by half the gap.
    #[tokio::test]
    async fn vt_by_weight_new_tenant_at_min_vt_and_pulled_back() {
        let book = TenantBook::new();
        let mut rules = book_with(10, None);
        rules
            .apply_tenant(
                "fast",
                cairn_store::RuleEdit {
                    rate: Some(cairn_store::Limit::Max(100)),
                    ..Default::default()
                },
                0,
            )
            .unwrap();
        book.set_rules(&rules);
        let slow = book.meter(&tenant("a"));
        let fast = book.meter(&tenant("fast"));
        let free = book.meter(&tenant("free"));
        slow.charge(1, 0, Instant::now());
        fast.charge(1, 0, Instant::now());
        free.charge(1, 0, Instant::now());
        assert_eq!(
            (slow.vt(), fast.vt(), free.vt()),
            (10 * VT_PER_CU, VT_PER_CU, VT_PER_CU)
        );
        // Nobody waiting ⇒ min_vt = MAX. The first tick pulls lagging tenants up by half the
        // gap to the leading tenant: fast 1000 → 1000 + (10000 − 1000)/2.
        book.sweep(Instant::now(), false);
        assert_eq!(book.min_vt(), u64::MAX);
        assert_eq!(fast.vt(), VT_PER_CU + 9 * VT_PER_CU / 2);
        // The next pull tick not due yet: no further pull.
        book.sweep(Instant::now(), false);
        assert_eq!(fast.vt(), VT_PER_CU + 9 * VT_PER_CU / 2);
        let _d = Demand::new(&slow);
        book.sweep(Instant::now(), true);
        assert_eq!(fast.vt(), 7_750);
        assert_eq!(book.min_vt(), 10 * VT_PER_CU, "only waiting tenants");
        let late = book.meter(&tenant("late"));
        assert_eq!(late.vt(), 10 * VT_PER_CU, "a new tenant starts at min_vt");
        assert_eq!(slow.penalty_for_turn(9), PENALTY_VT * 10);
        assert_eq!(free.penalty_for_turn(8), 0);
    }

    /// The number of nodes sharing the limit = the number of distinct nodes leading at least
    /// one range by the routing table (plus this node); buckets are split by that.
    #[tokio::test(start_paused = true)]
    async fn node_count_splits_cap_by_routing_table() {
        let book = TenantBook::new();
        book.set_rules(&book_with(60, None));
        let routing = crate::routing::RoutingTable::new();
        let leader = |n: &str| crate::routing::Leader {
            node_id: n.into(),
            address: None,
            ingress: None,
            term: 1,
        };
        routing.insert(cairn_store::PartitionId(0), leader("a"));
        routing.insert(cairn_store::PartitionId(1), leader("b"));
        routing.insert(cairn_store::PartitionId(2), leader("b"));
        let task = spawn_peer_count(book.clone(), routing.clone(), "a".into());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(book.peers(), 2);
        routing.insert(cairn_store::PartitionId(3), leader("c"));
        tokio::time::sleep(PEERS_EVERY).await;
        assert_eq!(book.peers(), 3);
        // 60 CU/s / 3 nodes = 20 CU/s per node: burst 20, then 50 ms per CU.
        let a = book.meter(&tenant("a"));
        a.charge(21, 0, Instant::now());
        let wait = a.until_positive(Instant::now());
        assert!(
            wait > Duration::from_millis(50) && wait <= Duration::from_millis(51),
            "{wait:?}"
        );
        task.abort();
    }

    /// Login limiting: `burst` times then blocked, one slot refilled
    /// every `every`; other keys are not affected; with the table full new keys are
    /// blocked until an old key is fully refilled.
    #[test]
    fn keyed_limiter_burst_refill_and_cap() {
        let l = KeyedLimiter::new(3, Duration::from_secs(6), 2);
        let t0 = Instant::now() + Duration::from_secs(1);
        for _ in 0..3 {
            assert!(l.check(&"a", t0).is_ok());
            l.charge(&"a", t0);
        }
        let wait = l.check(&"a", t0).expect_err("out of slots");
        assert!(
            wait > Duration::ZERO && wait <= Duration::from_secs(6),
            "{wait:?}"
        );
        assert!(
            l.check(&"a", t0 + Duration::from_secs(6)).is_ok(),
            "one slot refilled"
        );
        l.charge(&"b", t0);
        assert!(l.check(&"b", t0).is_ok());
        // Table full (a, b): a new key is blocked until a key is fully refilled.
        assert!(l.check(&"c", t0).is_err());
        l.charge(&"c", t0);
        assert!(
            l.check(&"c", t0).is_err(),
            "a new key cannot enter a full table"
        );
        assert!(l.check(&"c", t0 + Duration::from_secs(19)).is_ok());
    }
}
