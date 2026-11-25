// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flow control: the counters, locks and scheduler of one partition.
//!
//! All of it lives **on the engine thread** ([`crate::actor`]), and that is what makes the
//! whole design cheap: the scope decides the partition
//! ([`cairn_store::ClusterConfig::partition_of_scope`]), a partition is a single-threaded
//! single writer, so a scope's concurrency counter is a `u32` variable in the RAM of exactly
//! one process. No consensus, no CRDT, no distributed counter, **no network step on the
//! path that asks for a slot**.
//!
//! # Three resources, three lifecycles
//!
//! | Resource | Held by | Released while suspended? |
//! |---|---|---|
//! | EXCLUSIVE lock of a virtual object | invocation | **no** |
//! | Concurrency counter (3 tiers) | running invocation | yes |
//! | Partition turn cap | running invocation | yes |
//!
//! The first row is the easiest one to get wrong. On a 10-minute `ctx.sleep` the
//! invocation releases its *slot* — money is only spent while the handler runs. But it does
//! **not** release the VO lock: releasing the lock while suspended reopens exactly the race
//! the queue exists to close (a read-modify-write that spans a suspension point would lose
//! updates).
//!
//! # Why a two-phase permit
//!
//! Phase one ([`ResourceManager::poll_acquire`]) **has no side effects**: check the lock,
//! check the three counter tiers, check the turn cap. Phase two ([`Provisional::secure`]) is
//! **the only place** a counter goes up and a lock is taken.
//!
//! Both run on one thread and nobody can step in between — so why split them? Because
//! phase one can **fail at the third tier after passing the first two**. With a single phase
//! that would need an undo, and undoing counters is where bugs live longest: it is not wrong
//! right away, it leaks, and the leak only shows after a few days as "the system slows down
//! on its own and then stops".
//!
//! # Core and shell
//!
//! Every decision lives in [`Core`]: synchronous, no tokio, no hidden clock. Each event
//! (ask, leave, release, rule change, rate tick, node change) takes `now` and returns
//! [`Actions`] — the [`Grant`]s just issued, the rate deadline just brought forward, the
//! node that just got room. [`Scheduler`] is the async shell: it reads the clock once per
//! call, hands grants into the asker's `oneshot` channel, wakes `rate_loop`, notifies the
//! node. The module's sim (`flow/sim/`) applies event sequences drawn from a seed directly
//! to the core and checks FL-3/4/5/6/7/9/13 after every step.

use cairn_store::entropy::{HashMap, HashSet};
use {
    crate::rate::{CounterBucket, Demand, TenantBook, TenantMeter},
    cairn_engine::InvocationId,
    cairn_store::{CounterPath, QueueName, RuleBook, Scope, TenantId},
    std::{
        cell::RefCell,
        cmp::Reverse,
        collections::{BinaryHeap, VecDeque},
        rc::Rc,
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{oneshot, Notify},
        time::Instant,
    },
};

mod resource;
pub use resource::*;
mod decide;
pub(crate) use decide::*;
mod scheduler;
pub use scheduler::*;
mod ticket;
pub use ticket::*;

/// Identity of a queue as bytes — exactly the keyspace's `queue_key`.
pub type QueueKey = Vec<u8>;

/// EXCLUSIVE lock of a virtual object: `(scope, service, object_key)`.
///
/// **The limit key is not here.** Two calls to the same object with different limit keys
/// sit in two different queues, yet they are still *the same object* and must still lock
/// each other out. That is why the lock is a resource of its own rather than the queue
/// itself.
pub type LockName = (TenantId, Option<Scope>, String, String);

/// A concurrency counter **of one tenant**.
///
/// The rule book belongs to the whole cluster and matches by [`CounterPath`] (scope, limit
/// key) — but each tenant counts **separately**: a rule `acme` with a cap of 10 gives tenant
/// A's scope `acme` 10 and tenant B's scope `acme` 10 as well, they do not share 10.
/// Otherwise one tenant taking every slot could block another tenant just because both
/// picked the same scope name.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Counter {
    pub tenant: TenantId,
    pub path: CounterPath,
}

impl std::fmt::Display for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.tenant.is_default() {
            write!(f, "{}:", self.tenant)?;
        }
        write!(f, "{}", self.path)
    }
}

/// What can block a queue.
///
/// **One cannot say "this queue is blocked by rule R"** — it is blocked by the *pair* (rule,
/// concrete counter). Releasing a slot at `acme/foo/tenant1` wakes the queue waiting on
/// exactly that counter, not the queue waiting on `acme/bar/tenant1`, even though both
/// match the same rule `acme/*/tenant1`. That is why the variant below carries a
/// [`CounterPath`], not a pattern.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ResourceKind {
    Lock(LockName),
    Concurrency(Counter),
    /// Turn cap of the partition — the resource "connections to the SDK from this node".
    ///
    /// It used to be a separate `Semaphore` outside the scheduler. Here it is a resource on
    /// a par with counters and locks, and that is not about tidiness: a brake that sits
    /// outside the eligibility loop leaves the scheduler unaware of whom it is blocking, so
    /// it does not wake the right one on release.
    Turns,
    /// Out of tokens: the **rate** cap (CU/s) of the whole tenant or of one counter. Like a
    /// counter, this is "the tenant is out of budget" — `429`, not `202`. Nobody "releases"
    /// a token: room comes with time, so the scheduler sets its own timer
    /// ([`Scheduler::rate_loop`]).
    Rate(RateKey),
    /// The identity `(tenant, service, handler, deployment)` is quarantined in `cooldown`
    /// (FL-14): a cap of [`COOLDOWN_CAP`] concurrent turns on the range. **Not** "out of
    /// budget" — keep waiting like the VO lock (`202`, a background turn does not give up).
    Cooldown(CoolKey),
    /// The tenant is `suspended`: the scheduler grants it no slot at all — the engine does
    /// not call out to the tenant's endpoint. Checked **first** in `poll_acquire`. Counts as
    /// budget: a background turn gives up right away and is rescheduled after
    /// [`SUSPENDED_RETRY`]; lifting the suspension wakes the waiters.
    Tenant(TenantId),
    /// The tenant's running turns on the **whole node** have hit the cap
    /// (`--max-turns-per-tenant`): a tenant with a hung endpoint or long model steps does not
    /// hold every slot of the node. Like the turn cap — **not** budget: the tenant's own work
    /// queues behind its running turns (`202`, a background turn keeps waiting). A release in
    /// another range wakes through the node's announcement
    /// ([`crate::engine_pool::NodeTurns::announce`]).
    TenantTurns(TenantId),
}

/// Reschedule a turn of a suspended tenant after this long: ≤ 4 commits/hour for each
/// invocation with work due. On unsuspend the range's leader fires those rows right away
/// (`turn::wake_lifted`) — including a leader that came up after the unsuspend, going by the
/// `refused-ms=`/`lifted-ms=` marks of the tenant registry ([`Lifted`]).
pub const SUSPENDED_RETRY: Duration = Duration::from_secs(15 * 60);

/// Extra width at both ends of the due window of rows rescheduled during a suspension, when
/// the leader infers it from the tenant registry's marks ([`Lifted`]): the marks are written
/// by the clock of the node that took the command, the rescheduled rows by the leader's
/// clock, and a node that has not reread the registry still reschedules for a while after
/// the unsuspend command. The same clock-skew cap as the signatures between nodes
/// (`auth::MAX_SKEW_S`). Too wide only makes a few of the tenant's ordinary retries run
/// early once; too narrow leaves rows outside waiting for their reschedule tick as before.
pub const LIFT_SLACK: Duration = Duration::from_secs(60);

/// A tenant just unsuspended whose rescheduled rows the range's leader must pull back
/// (`turn::LiftSweep::add`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lifted {
    pub tenant: TenantId,
    /// `None`: the leader **saw** the suspended set change — pull every due row within one
    /// reschedule tick. `Some((refused, lifted))`: the registry's marks (`refused-ms=`,
    /// `lifted-ms=`) — the leader did not see the suspension (it came up after the
    /// unsuspend, or the registry changed before it read it), so it only pulls due rows in
    /// the window the suspension could have rescheduled into.
    pub marks: Option<(Option<u64>, u64)>,
}

/// Identity of the `cooldown` tier — same key as the per-handler summary.
pub type CoolKey = crate::stats::HandlerKey;

/// Concurrency cap of an identity in `cooldown`, on one range.
pub const COOLDOWN_CAP: u32 = 1;

/// Which bucket blocks: the whole tenant's (the rule book's `tenant` line, shared by every
/// range of the node) or one `(tenant, counter)` with a `rate` rule (the range's).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum RateKey {
    Tenant(TenantId),
    Counter(Counter),
}

impl ResourceKind {
    /// Short label for metrics. Carries no concrete value — the cardinality of scopes is
    /// something users set, and a metric label set by outsiders is a way to blow up
    /// Prometheus.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Lock(_) => "lock",
            Self::Concurrency(c) => match c.path.depth() {
                1 => "concurrency_scope",
                2 => "concurrency_l1",
                _ => "concurrency_l2",
            },
            Self::Turns => "turns",
            Self::Rate(_) => "rate",
            Self::Cooldown(_) => "cooldown",
            Self::Tenant(_) => "tenant",
            Self::TenantTurns(_) => "tenant_turns",
        }
    }

    /// "Tenant out of budget" (counter or rate) — `429` to the client, and a background turn
    /// gives up after its wait cap. The VO lock and the turn cap do not: keep waiting.
    pub fn is_budget(&self) -> bool {
        matches!(self, Self::Concurrency(_) | Self::Rate(_) | Self::Tenant(_))
    }
}

#[cfg(test)]
mod sim;

#[cfg(test)]
mod tests;
