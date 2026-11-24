// #[hot-path] — child file of `flow/mod.rs` (AGENTS.md §6); only compiled in tests.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flow sim **without tokio** (in the style of TigerBeetle's simulator): apply a sequence
//! of events drawn from a seed directly to the [`Core`] and check the invariants after
//! **every** step.
//!
//! One seed decides the knobs too (swarm mode): number of ranges, the node's cap (or no
//! node attached), number of tenants, scopes, objects, the VO ratio, concurrency rules,
//! per-tenant and per-counter rates, cooldown, and the weight of each event kind — weight 0
//! turns a feature off entirely. Events: ask for a slot, the asker leaves, a permit dropped
//! before its turn (`give_back`), entering a turn, ending a turn (suspend or finish,
//! settling CU before releasing), time passing (rate tick, the node's `vt` sweep tick), a
//! rule change, an admin cancel, a leader change of a range (`reset` + rebuilding the
//! locks), another range gaining/losing leadership on the node, delivering a late "node
//! changed" message to each range, a `poll_acquire` probe, and suspending/unsuspending a
//! tenant.
//!
//! After every step: FL-3 (counter = sum of live permits, cap not exceeded, `poll_acquire`
//! without side effects), FL-4 (VO lock exclusive, matches the model), FL-5 (service order
//! by `seq`, the lock holder at the head), FL-6 (no grant to one who left, dead entries
//! bounded, no queue waiting without being in a wait list), FL-7 (the node's counter = sum
//! of the cells, guaranteed share and borrowing rules), FL-9 (no wasted calls to waiters),
//! FL-13 (a free slot goes to the tenant with the smallest `vt`, borrowing by `vt`), FL-T
//! (no slot for a suspended tenant). Each seed ends with a **liveness** phase: unsuspend
//! every tenant, stop the faults, run every turn until nobody is waiting — every request
//! must be granted within a bounded number of rounds, and every counter returns to 0
//! (FL-3).
//!
//! Run: `cargo test -p cairn-server --lib flow::sim`. Long run:
//! `CAIRN_FLOW_SIM_SEEDS=10000`. One seed: `CAIRN_FLOW_SIM_SEED=<s>`. The core's `HashMap`
//! iteration order is deterministic in tests (the dev-dependency enables
//! `cairn-store/testkit`, fixed hash keys), so seeds reproduce.

use {
    super::*,
    crate::{
        engine_pool::{Leading, TurnBudget},
        rate::BORROW_SLACK_VT,
    },
    cairn_store::{testkit::SimRng, Limit, LimitKey, RuleEdit},
    std::fmt::Write as _,
};

/// Default number of seeds — fast enough for `check.sh` (debug build).
const DEFAULT_SEEDS: u64 = 400;

/// Round cap of the liveness phase.
const DRAIN_ROUNDS: usize = 20_000;

/// FL-9: maximum number of wasted calls (a queue just called out of R's wait list blocked
/// again at R itself) in one step.
const WASTED_PER_STEP: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Request,
    Cancel,
    GiveBack,
    Begin,
    End,
    Time,
    Rules,
    Admin,
    Reset,
    Lead,
    Deliver,
    Probe,
    Suspend,
}

const EVENTS: [Ev; 13] = [
    Ev::Request,
    Ev::Cancel,
    Ev::GiveBack,
    Ev::Begin,
    Ev::End,
    Ev::Time,
    Ev::Rules,
    Ev::Admin,
    Ev::Reset,
    Ev::Lead,
    Ev::Deliver,
    Ev::Probe,
    Ev::Suspend,
];

/// Knobs of one seed (swarm).
#[derive(Clone, Debug)]
struct Knobs {
    ranges: usize,
    /// The node's turn cap; `None` ⇒ one range standing alone with `solo_turns`.
    node_total: Option<usize>,
    solo_turns: usize,
    tenants: usize,
    scopes: usize,
    objects: usize,
    /// ‰ of EXCLUSIVE queues.
    exclusive: u64,
    limits: bool,
    tenant_rates: bool,
    counter_rates: bool,
    cooldown: bool,
    weights: [u64; EVENTS.len()],
    steps: usize,
    max_live: usize,
    /// Cap on a tenant's running turns on the node (FL-26) — drawn **last**: the earlier
    /// knobs keep the seed's draw sequence unchanged.
    tenant_cap: Option<usize>,
}

impl Knobs {
    fn draw(rng: &mut SimRng) -> Self {
        let mut below = |n: u64| rng.next_u64() % n.max(1);
        let node = below(4) != 0;
        let ranges = if node { 1 + below(3) as usize } else { 1 };
        let mut weights = [0u64; EVENTS.len()];
        for (i, w) in weights.iter_mut().enumerate() {
            // A one-in-three chance of turning an event kind off entirely — except the four
            // kinds without which nothing runs.
            let core = matches!(EVENTS[i], Ev::Request | Ev::Begin | Ev::End | Ev::Time);
            *w = if !core && below(3) == 0 {
                0
            } else {
                1 + below(10)
            };
        }
        // Leader changes and rule changes are rarer than for real — they wipe the state.
        weights[8] = weights[8].min(2);
        weights[6] = weights[6].min(3);
        // Suspending/unsuspending a tenant also puts every queue back in the eligibility
        // loop, like a rule change.
        weights[12] = weights[12].min(3);
        Self {
            ranges,
            node_total: node.then(|| 1 + below(6) as usize),
            solo_turns: 1 + below(4) as usize,
            tenants: 1 + below(4) as usize,
            scopes: below(4) as usize,
            objects: 1 + below(5) as usize,
            exclusive: [0, 300, 700, 1000][below(4) as usize],
            limits: below(3) != 0,
            tenant_rates: below(2) == 0,
            counter_rates: below(3) == 0,
            cooldown: below(3) == 0,
            weights,
            steps: 200 + below(600) as usize,
            max_live: 4 + below(36) as usize,
            tenant_cap: (below(3) == 0).then(|| 1 + below(3) as usize),
        }
    }
}

enum St {
    Idle,
    Waiting(u64),
    /// Nacked tickets ([`Sim::nacks`]): the slot is returned, the ticket waits for the
    /// invocation lock to be released, then asks again **with the same `seq`** — still a
    /// waiter of the queue (FL-5). `held`: held the VO lock before the nacked grant.
    Deferred {
        held: bool,
    },
    Granted {
        req: u64,
        permit: Box<Permit>,
        begun: bool,
        /// Granted while the node leads more ranges than the total cap (FL-7).
        oversub: bool,
    },
}

struct Inv {
    id: InvocationId,
    queue: QueueName,
    key: QueueKey,
    range: usize,
    seq: u64,
    cool: Option<CoolKey>,
    st: St,
    /// VO locks the invocation holds according to the model — from the grant until the end
    /// (across suspensions and leader changes).
    lock: Option<LockName>,
    done: bool,
}

struct Req {
    inv: usize,
    range: usize,
    key: QueueKey,
    seq: u64,
    core_req: u64,
    /// The asker held the lock when asking ⇒ at the head of the line (FL-5).
    held_lock: bool,
    live: bool,
}

struct RangeSim {
    core: Core<u64>,
    lead: Option<Leading>,
    /// "Node changed" messages not yet delivered (the node's watch — coalesced, late).
    changed: bool,
}

struct Sim {
    seed: u64,
    rng: SimRng,
    k: Knobs,
    t0: Instant,
    clock_us: u64,
    last_sweep_us: u64,
    book: crate::rate::TenantBook,
    budget: Option<TurnBudget>,
    phantoms: Vec<Leading>,
    ranges: Vec<RangeSim>,
    rules: RuleBook,
    invs: Vec<Inv>,
    reqs: HashMap<u64, Req>,
    next_req: u64,
    next_inv: u64,
    next_seq: HashMap<QueueKey, u64>,
    trace: VecDeque<String>,
    step: usize,
    wasted: u64,
    /// Tenants currently `suspended` — pushed into the core of every range.
    suspended: HashSet<TenantId>,
    /// Enables the **nack** event: the unused slot of a granted one is returned through
    /// `Core::nack`, and that one waits, then asks again with the same `seq`. Off in the
    /// default swarm (keeps the draw sequence of the old seeds).
    nacks: bool,
}

impl Drop for Sim {
    /// A seed red mid-way: permits still alive in the model must not panic a second time
    /// during teardown (`Drop` of `Permit`).
    fn drop(&mut self) {
        for inv in &mut self.invs {
            if let St::Granted { permit, .. } = &mut inv.st {
                permit.live = false;
            }
        }
    }
}

type Check = Result<(), String>;

macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

mod events;
mod invariants;
use invariants::*;
impl Sim {
    fn new(seed: u64) -> Self {
        let mut rng = SimRng::new(seed);
        let k = Knobs::draw(&mut rng);
        let t0 = Instant::now();
        let book = crate::rate::TenantBook::starting_at(t0);
        book.set_turn_cap(k.tenant_cap);
        let budget = k
            .node_total
            .map(|n| TurnBudget::with_tenants(n, book.clone()));
        let mut ranges = Vec::with_capacity(k.ranges);
        for _ in 0..k.ranges {
            let mut core = Core::new(k.solo_turns);
            core.rm.fair = book.clone();
            ranges.push(RangeSim {
                core,
                lead: budget.as_ref().map(TurnBudget::lead),
                changed: false,
            });
        }
        let mut sim = Self {
            seed,
            rng,
            k,
            t0,
            clock_us: 0,
            last_sweep_us: 0,
            book,
            budget,
            phantoms: Vec::new(),
            ranges,
            rules: RuleBook::default(),
            invs: Vec::new(),
            reqs: HashMap::default(),
            next_req: 0,
            next_inv: 0,
            next_seq: HashMap::default(),
            trace: VecDeque::new(),
            step: 0,
            wasted: 0,
            suspended: HashSet::default(),
            nacks: false,
        };
        // Becoming leader: attach the node, the range's share, the current rules (like `serve`).
        sim.rules = sim.draw_rules();
        let now = sim.now();
        for r in 0..sim.ranges.len() {
            if let Some(b) = sim.budget.clone() {
                sim.ranges[r].core.attach_node_budget(&b);
                let acts = sim.ranges[r].core.set_max_turns(b.share(), now);
                sim.ranges[r].core.recycle(acts.grants);
            }
            let acts = sim.ranges[r].core.set_rules(sim.rules.clone(), now);
            sim.ranges[r].core.recycle(acts.grants);
        }
        sim
    }

    fn below(&mut self, n: u64) -> u64 {
        self.rng.next_u64() % n.max(1)
    }

    fn chance(&mut self, permille: u64) -> bool {
        self.below(1000) < permille
    }

    fn now(&self) -> Instant {
        self.t0 + Duration::from_micros(self.clock_us)
    }

    fn note(&mut self, s: String) {
        if self.trace.len() == 60 {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("#{} {s}", self.step));
    }

    fn share(&self) -> usize {
        self.budget
            .as_ref()
            .map_or(self.k.solo_turns, TurnBudget::share)
    }
}

fn run_seed(seed: u64) -> Result<(), String> {
    let mut sim = Sim::new(seed);
    sim.run().map_err(|e| sim.report(&e))
}

fn run_seed_with_nacks(seed: u64) -> Result<(), String> {
    let mut sim = Sim::new(seed);
    sim.nacks = true;
    sim.run().map_err(|e| sim.report(&e))
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// Swarm: each seed one set of knobs and one event sequence; invariants after every step,
/// liveness at the end. `CAIRN_FLOW_SIM_SEED` runs exactly one seed, `CAIRN_FLOW_SIM_SEEDS`
/// changes the number of seeds (default [`DEFAULT_SEEDS`]).
#[test]
fn swarm_holds_flow_invariants_after_every_step() {
    let seeds: Vec<u64> = match env_u64("CAIRN_FLOW_SIM_SEED") {
        Some(s) => vec![s],
        None => (1..=env_u64("CAIRN_FLOW_SIM_SEEDS").unwrap_or(DEFAULT_SEEDS)).collect(),
    };
    let started = Instant::now();
    let mut red = Vec::new();
    for &s in &seeds {
        if let Err(e) = run_seed(s) {
            if red.is_empty() {
                eprintln!("{e}");
            }
            red.push(s);
        }
    }
    eprintln!(
        "flow sim: {} seeds, {} red, {:?}",
        seeds.len(),
        red.len(),
        started.elapsed()
    );
    assert!(red.is_empty(), "red seeds: {red:?}");
}

/// **Nack in the FL-5 check**: the swarm gets an extra nack event — a granted one whose
/// turn did not win the invocation lock returns the unused slot (`Core::nack`), waits, then
/// asks again with the same `seq`. The nacked one is still a waiter of the queue: the FL-5
/// check counts it, and the lock model keeps the EXCLUSIVE queue's VO lock for it (the
/// re-ask is at the head of the line). Before the fix a nack returned the slot by
/// `held_before` at grant time ⇒ the VO lock left the nacked one, a later `seq` was granted
/// first (FIFO on one object key broken).
#[test]
fn swarm_with_nacks_keeps_seq_order() {
    let seeds = env_u64("CAIRN_FLOW_SIM_SEEDS").unwrap_or(DEFAULT_SEEDS);
    let red: Vec<u64> = (1..=seeds)
        .filter(|&s| match run_seed_with_nacks(s) {
            Ok(()) => false,
            Err(e) => {
                eprintln!("{e}");
                true
            }
        })
        .take(3)
        .collect();
    assert!(red.is_empty(), "red seeds (≤ 3 first): {red:?}");
}

/// **Seed 1473** (FL-7, a bug older than the sim's finding of it): suspending a tenant moves
/// a range's only slot waiter to blocking on `Tenant`, and that range's starving flag stays
/// up while the node is not full — every other range stops borrowing. Lower-level proof:
/// `flow::tests::rate::starvation_flag_does_not_outlive_its_permit_waiter`.
#[test]
fn seed_1473_starving_flag_outlives_its_waiter() {
    if let Err(e) = run_seed(1473) {
        panic!("{e}");
    }
}

/// A seed run twice gives the same trace — the sim is deterministic, so a red seed it prints
/// is reproducible.
#[test]
fn same_seed_same_trace() {
    for seed in [3, 17, 101] {
        let mut a = Sim::new(seed);
        let mut b = Sim::new(seed);
        let ra = a.run();
        let rb = b.run();
        assert_eq!(ra, rb);
        assert_eq!(a.trace, b.trace, "seed {seed}");
        assert_eq!(a.clock_us, b.clock_us);
    }
}

/// Every seed that has ever been red. Seeds are tied to the current generator — changing
/// `Knobs::draw` or the draw order turns them into different seeds; the durable proof is
/// the unit test of each bug in `flow::tests`.
/// - 1 (and every seed of the first run): a queue with no waiters left stays in the
///   table — `queue_emptied_by_a_grant_leaves_the_table`.
/// - 10: the VO lock wakes every waiter — the FL-9 check only counts countable
///   resources.
/// - 4127, 7661: a queue with only dead entries (≤ 32) sits waiting for its resource.
/// - 1402, 1771, 1959, 2735, 2963, 3080, 3467, 7408, 8183, 8209, 8495: the starving flag
///   stays up after `set_rules` while the node is full.
#[test]
fn seeds_that_were_red() {
    for seed in [
        1, 10, 4127, 7661, 1402, 1771, 1959, 2735, 2963, 3080, 3467, 7408, 8183, 8209, 8495,
    ] {
        if let Err(e) = run_seed(seed) {
            panic!("{e}");
        }
    }
}
