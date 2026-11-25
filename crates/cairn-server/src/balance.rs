// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Leadership balancing — dynamic rebalancing.
//!
//! "Whoever takes it keeps it" is right for one node and wrong for a cluster:
//! the node that starts first grabs every partition, nodes joining later sit
//! idle, and no force pulls things back into balance. Changing the **number**
//! of partitions is not the way; the way is moving **leadership** — which is
//! cheap precisely by the diskless thesis: state lives in the bucket, moving a
//! partition means moving a lease, not a single byte.
//!
//! The policy fits in one sentence: **hold no more than the fair share,
//! release when over it; if a node stands below the floor, someone at the cap
//! gives way.**
//!
//! - *Fair share* = ⌈ranges / live nodes⌉, ranges counted from **the current
//!   table** (the table changes at runtime), live nodes counted from sessions
//!   ([`cairn_store::Membership`]) — every node has a session even before it
//!   holds anything, and precisely the node holding nothing is the one that
//!   needs to be given ranges. A node that **comes up midway** only counts
//!   once it has been alive steadily ([`settle_for`]): otherwise every time a
//!   node dies and comes back means two owner changes.
//! - *Hold no more*: before contending for a lease one must take a
//!   [`ContendPermit`]; permits count contests **in progress** too, because
//!   eight engine threads all seeing "one slot left" and all winning is the
//!   classic over-grab.
//! - *Release when over*: the background loop recounts every period; over the
//!   fair share long enough (two periods — a deliberate delay, a presence
//!   flicker of one tick must not cause a wave of failovers) ⇒ `resign`
//!   exactly **one** partition, then wait for the next period. Releasing one
//!   at a time is regulation: releasing en masse is a self-inflicted failover
//!   storm.
//! - *Floor* = ⌊ranges / live nodes⌋. The cap alone is not enough: four ranges
//!   on three nodes as {2, 2, 0} has nobody *over* ⌈4/3⌉ = 2, and the third
//!   node is alive yet leads nothing, forever. So when a steady node stands
//!   **below the floor** (per the `owns` hints, [`Membership::loads`]):
//!   - every node contends only up to the floor, not the cap ("fill the floor
//!     first") — a range just given away can only be taken by a node below
//!     the floor, and not even the giver takes it back;
//!   - **one** node at the cap — the smallest id among the nodes at the cap,
//!     deterministically — gives one range away, also after two periods like
//!     release-when-over. When a donation reaches nobody (a node below the
//!     floor with a heartbeat that takes no work) the next one waits twice as
//!     long, up to [`DONATE_BACKOFF_MAX`]: no tug of war every few TTLs.
//! - *Draining* ([`Balancer::set_draining`]; and while the shared WAL has a
//!   rotated slot with no stream yet — [`Balancer::with_wal_gate`]): a slow
//!   node shedding its ranges refuses every range, on every path — including a
//!   child range the split coordinator just took for it (taken, then given
//!   away at once) and the rescue path. Other nodes see `draining=1` in its
//!   session and leave it out of the floor, the cap and the nodes that need
//!   ranges.
//!
//! Safety does not change one bit: everything here only decides *who should
//! try to hold a lease*. Who may **write** is still decided by `writer_epoch` +
//! conditional PUT, as always. If this policy is wrong (miscounts live nodes,
//! releases by mistake) the distribution is temporarily skewed — exactly the
//! kind of error that is allowed.

// The runtime's clock: outside simulation it is `std::time::Instant`, in
// deterministic simulation the virtual clock.
use tokio::time::Instant;
use {
    crate::partitions::Partitions,
    cairn_store::{
        health::SlowMeter, Claim, ClusterConfig, LeadershipGrant, LeaseKeeper, Membership,
        PartitionId, Session,
    },
    std::{
        collections::BTreeSet,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

/// A node that came up midway must be alive continuously for this long before
/// it counts in other nodes' fair share: 10 TTLs, clamped to [5 s, 30 s].
///
/// Long enough that a node dying and coming back repeatedly does not drag
/// ranges back and forth (each drag hands over every unfinished invocation of
/// the range); short enough that a genuinely new node gets work within half a
/// minute.
pub fn settle_for(ttl: Duration) -> Duration {
    (ttl * 10).clamp(Duration::from_secs(5), Duration::from_secs(30))
}

/// Cap on the wait between two donations to the same below-floor node when
/// the previous one did not reach it — see the module head.
pub const DONATE_BACKOFF_MAX: Duration = Duration::from_secs(600);

/// The current split, as seen from one node.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Shares {
    /// ⌊ranges / nodes⌋.
    floor: usize,
    /// ⌈ranges / nodes⌉ — the fair share.
    ceil: usize,
    /// A steady **other** node holding below the floor, smallest id. `Some` ⇒
    /// every node contends only up to the floor.
    needy: Option<(String, usize)>,
    /// Other (live) nodes holding at or above the cap — to choose who gives way.
    at_ceiling: BTreeSet<String>,
}

impl Shares {
    /// The lease contention cap right now: the floor when a node is below it, otherwise the cap.
    fn cap(&self) -> usize {
        if self.needy.is_some() {
            self.floor
        } else {
            self.ceil
        }
    }
}

struct Inner {
    /// Partitions this node currently leads — the balancer's own books, updated
    /// synchronously on win/loss, so without the routing table's lag.
    held: BTreeSet<u32>,
    /// Number of lease contests in progress. Added to `held` when checking the
    /// fair share, to prevent over-grab.
    contending: u32,
    /// The most recent release decisions of the loop (capped at [`RELEASE_LOG_MAX`]).
    releases: std::collections::VecDeque<Release>,
}

/// Number of recent releases [`Balancer::releases`] keeps.
pub const RELEASE_LOG_MAX: usize = 64;

/// One decision of the release loop to release a range — at the moment of
/// **deciding**, before `resign`. Lets tests measure the loop's rhythm
/// (LS-10) without mixing in `resign` time, or ranges lost for reasons other
/// than the balancer (a failed lease).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Release {
    pub at: Instant,
    pub partition: u32,
    /// `true` = over the fair share; `false` = giving way to a node below the floor.
    pub over: bool,
}

/// The balancing policy of **one node**. `Clone` is cheap, the clones share the books.
#[derive(Clone)]
pub struct Balancer {
    /// The current range table — the fair share's denominator is read from it every time.
    cluster: ClusterConfig,
    node_id: String,
    members: Membership,
    inner: Arc<Mutex<Inner>>,
    /// The node is shedding itself because it is slow — refuses every range.
    draining: Arc<AtomicBool>,
    /// The node's meter: the shared WAL has a rotated slot with no stream yet
    /// ([`SlowMeter::wal_draining`]), or the clock rate is skewed
    /// ([`SlowMeter::clock_draining`]) ⇒ drain as well.
    meter: Option<SlowMeter>,
    /// The node's registry freshness: a new incarnation takes no range before a full
    /// refresh round after it opened ([`crate::registry_gen::RegistryGen::session_ready`]).
    registry: Option<crate::registry_gen::RegistryGen>,
}

impl Balancer {
    pub fn new(cluster: ClusterConfig, node_id: String, members: Membership) -> Self {
        Self {
            cluster,
            node_id,
            members,
            inner: Arc::new(Mutex::new(Inner {
                held: BTreeSet::new(),
                contending: 0,
                releases: Default::default(),
            })),
            draining: Arc::new(AtomicBool::new(false)),
            meter: None,
            registry: None,
        }
    }

    /// Take no range while the node's current incarnation has not fully re-read the
    /// registry since it opened, or while its grant is not valid. A node that lost its
    /// session and opened a new one in-process does not rerun the boot sequence: a
    /// synchronous registry write (tenant state, deployment `DELETE`) may already have
    /// been acked without it, and its range engines would admit work and pin deployments
    /// from the old copy.
    pub fn with_registry_gate(mut self, gen: crate::registry_gen::RegistryGen) -> Self {
        self.registry = Some(gen);
        self
    }

    /// Also drain while the node's shared WAL has a rotated slot with no new
    /// stream: ranges just dirtily handed off from that slot go to another
    /// node, not back to a slot nobody serves.
    pub fn with_wal_gate(mut self, meter: SlowMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Turns draining on/off: on ⇒ [`Self::acquire`] refuses every range and
    /// the background loop releases nothing (releasing is the drainer's job).
    pub fn set_draining(&self, on: bool) {
        self.draining.store(on, Ordering::Release);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
            || self
                .meter
                .as_ref()
                .is_some_and(|m| m.wal_draining() || m.clock_draining())
    }

    /// The current fair share: ⌈ranges in the current table / **steady** live
    /// nodes⌉ ([`settle_for`]).
    ///
    /// Counts itself among the live nodes even before presence has seen it —
    /// right after startup "0 live nodes" would divide by zero, while "just
    /// us" is the right answer at worst.
    pub fn fair_share(&self) -> usize {
        self.shares().ceil
    }

    /// Floor, cap and who stands below the floor — see the module head.
    fn shares(&self) -> Shares {
        let mut nodes = self.members.settled_nodes(settle_for(self.members.ttl()));
        nodes.insert(self.node_id.clone());
        // A draining node holds nothing and takes nothing: not in the
        // denominator, not a node that needs ranges.
        let draining: BTreeSet<String> = self
            .members
            .peer_health()
            .into_iter()
            .filter(|(_, h)| h.draining)
            .map(|(node, _)| node)
            .collect();
        nodes.retain(|n| !draining.contains(n));
        let n = nodes.len().max(1);
        let ranges = self.cluster.partition_count() as usize;
        let (floor, ceil) = (ranges / n, ranges.div_ceil(n));
        let loads = self.members.loads();
        let needy = nodes
            .iter()
            .filter(|node| **node != self.node_id)
            .map(|node| (node.clone(), loads.get(node).copied().unwrap_or(0)))
            .find(|(_, held)| *held < floor);
        let at_ceiling = loads
            .into_iter()
            .filter(|(node, held)| *held >= ceil && !draining.contains(node))
            .map(|(node, _)| node)
            .collect();
        Shares {
            floor,
            ceil,
            needy,
            at_ceiling,
        }
    }

    /// Partitions this node holds, per the balancer's books.
    pub fn held(&self) -> BTreeSet<u32> {
        self.inner
            .lock()
            .map(|i| i.held.clone())
            .unwrap_or_default()
    }

    fn note_lost(&self, p: PartitionId) {
        if let Ok(mut i) = self.inner.lock() {
            i.held.remove(&p.0);
        }
    }

    /// A range has left the table: strike it from the books. Its engine exits
    /// for good, so there will be no `acquire` left to strike it.
    pub fn forget(&self, p: PartitionId) {
        self.note_lost(p);
    }

    /// Requests a lease contention slot. `None` = share already full, do not contend.
    fn try_contend(&self) -> Option<ContendPermit> {
        if self.is_draining() {
            return None;
        }
        let cap = self.shares().cap();
        let mut i = self.inner.lock().ok()?;
        if i.held.len() + i.contending as usize >= cap {
            return None;
        }
        i.contending += 1;
        Some(ContendPermit {
            inner: Arc::clone(&self.inner),
            won: None,
        })
    }

    /// A contention slot **above the fair share** — the rescue path, see [`Self::acquire`].
    fn contend_anyway(&self) -> Option<ContendPermit> {
        let mut i = self.inner.lock().ok()?;
        i.contending += 1;
        Some(ContendPermit {
            inner: Arc::clone(&self.inner),
            won: None,
        })
    }

    /// Takes `partition`, **respecting the fair share**.
    ///
    /// Replaces a bare `claim` in the engine factory. Three points, each the
    /// answer to a lesson:
    ///
    /// 1. **Look before spending a slot.** Contention slots are scarce (at most
    ///    `fair_share` contests at once); burning one on a partition whose
    ///    current owner is **alive** takes the slot from a thread aiming at an
    ///    empty partition — starvation, measured for real.
    /// 2. **Wait for events, do not poll** (D20). The `owns` hints in other
    ///    nodes' sessions say who holds what; a follower waits for the tracker
    ///    to report a change instead of GETting each partition's owner record
    ///    every TTL/5 — that used to be the largest read cost of an idle
    ///    cluster, multiplied by the number of partitions.
    /// 3. **Rescue grace** `rescue_grace`: an empty partition that nobody below
    ///    the fair share has come for in too long may be taken above the share
    ///    — without it, a wedged-but-heartbeating node keeps everyone's
    ///    denominator unchanged and the partitions it released stay orphaned
    ///    forever.
    pub async fn acquire(
        &self,
        session: &Session,
        partition: PartitionId,
        rescue_grace: Duration,
        pace: Duration,
    ) -> cairn_store::Result<(LeadershipGrant, LeaseKeeper)> {
        self.note_lost(partition);
        let members = session.membership();
        let mut changes = members.changed();
        // Wait for the tracker to report a change, but no longer than `limit`
        // — a safety net for a missed event (a hint not written yet, an owner
        // record changed while the session has not).
        let wait_change = |changes: &mut tokio::sync::watch::Receiver<u64>, limit: Duration| {
            let mut changes = changes.clone();
            async move {
                let _ = tokio::time::timeout(limit, changes.changed()).await;
            }
        };
        // Since when this partition has been "empty" — the mark for the rescue grace.
        let mut free_since: Option<Instant> = None;
        // Since when the node's incarnation has not been ready to take ranges.
        let mut not_ready_since: Option<Instant> = None;
        let ready = || self.registry.as_ref().is_none_or(|g| g.session_ready());
        loop {
            changes.borrow_and_update();
            // **Draining**: take nothing, on every path. A child range the
            // split coordinator just took for us is accepted and given away
            // at once — left alone, its owner record would point at our live
            // session, and nobody else would take it.
            if self.is_draining() {
                if session.owned().contains_key(&partition.0) {
                    if let Claim::Won(_, keeper) = session.claim(partition).await? {
                        tracing::info!(
                            partition = partition.0,
                            "draining — giving away a range just handed over"
                        );
                        keeper.release().await;
                    }
                }
                free_since = None;
                wait_change(&mut changes, pace).await;
                continue;
            }
            if !ready() {
                // A range handed to this incarnation (a split child claimed by the
                // coordinator) while it cannot take it: give it away after the rescue
                // grace, as draining does — peers see a live owner and would never
                // rescue it.
                let since = *not_ready_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= rescue_grace && session.owned().contains_key(&partition.0) {
                    if let Claim::Won(_, keeper) = session.claim(partition).await? {
                        tracing::info!(
                            partition = partition.0,
                            "registry not re-read under this incarnation — giving away a range handed over"
                        );
                        keeper.release().await;
                    }
                }
                free_since = None;
                wait_change(&mut changes, pace).await;
                continue;
            }
            not_ready_since = None;
            // **Already ours** (the split/merge coordinator just took this
            // range within the node's session): accept it at once, without
            // spending a fair-share slot — there is nothing to win, and waiting
            // for a slot would leave the child range leaderless during the
            // split window.
            if session.owned().contains_key(&partition.0) {
                if let Some(mut permit) = self.contend_anyway() {
                    if let Claim::Won(grant, keeper) = session.claim(partition).await? {
                        // The claim re-reads the live incarnation: a new one may have
                        // opened since `ready()` was checked.
                        if !ready() {
                            drop(grant);
                            keeper.release().await;
                            continue;
                        }
                        permit.won = Some(partition.0);
                        return Ok((grant, keeper));
                    }
                }
            }
            // A live owner per the hints, and not us ⇒ nothing to win.
            let mine = session.current();
            if let Some(h) = members.holders().get(&partition.0) {
                if Some(&h.session) != mine.as_ref() {
                    free_since = None;
                    wait_change(&mut changes, rescue_grace).await;
                    continue;
                }
            }
            let force = match free_since {
                Some(t) => t.elapsed() >= rescue_grace,
                None => {
                    free_since = Some(Instant::now());
                    false
                }
            };
            // **A range with work in flight** (a split/merge intent, D-e): the
            // coordinator is gathering it, do not contend. Empty past the
            // rescue grace ⇒ take it anyway — the coordinator has died, and
            // whoever takes it will roll the intent back.
            if !force
                && self
                    .cluster
                    .range(partition)
                    .is_some_and(|r| r.intent.is_some())
            {
                wait_change(&mut changes, pace).await;
                continue;
            }
            let permit = if force {
                self.contend_anyway().inspect(|_| {
                    tracing::info!(
                        partition = partition.0,
                        "partition empty too long — taking it above the fair share"
                    )
                })
            } else {
                self.try_contend()
            };
            let Some(mut permit) = permit else {
                wait_change(&mut changes, pace).await;
                continue;
            };
            match session.claim(partition).await? {
                // The claim re-reads the live incarnation: a new one may have opened
                // since `ready()` was checked.
                Claim::Won(grant, keeper) if !ready() => {
                    drop(permit);
                    drop(grant);
                    keeper.release().await;
                }
                Claim::Won(grant, keeper) => {
                    permit.won = Some(partition.0);
                    return Ok((grant, keeper));
                }
                // A live owner the hints have not reported yet — give the slot back, wait for a change.
                Claim::Held(Some(_)) => {
                    drop(permit);
                    free_since = None;
                    wait_change(&mut changes, rescue_grace).await;
                }
                // Someone just changed the record, or our session was just
                // lost. Sleep one tick before reconsidering: without sleeping,
                // this very thread would grab the slot back before anyone
                // else woke up.
                Claim::Held(None) => {
                    drop(permit);
                    cairn_store::pace::beat(pace).await;
                }
            }
        }
    }

    /// The release-when-over and give-way-to-a-node-below-the-floor loop. Runs
    /// once every `interval`; the same reason must hold for **two**
    /// consecutive periods before releasing, and only one partition is
    /// released each time.
    ///
    /// Releases the **highest-numbered** partition held: deterministic, so two
    /// consecutive periods do not change their mind midway, and the logs of
    /// two nodes tell the same story.
    ///
    /// One pathological state worth knowing in advance: a
    /// **wedged-but-heartbeating** node (heart beating, engine not taking its
    /// share) makes this loop and the rescue path of [`Self::acquire`] play
    /// tug of war — releasing for being over the share (or giving way because
    /// it is below the floor), rescuing because orphaned, on exactly one
    /// partition. Release-when-over tugs every few TTLs; donations back off by
    /// doubling each time they do not land (up to [`DONATE_BACKOFF_MAX`]).
    /// Accepted: that state is itself an incident needing a human, the tug is
    /// slow and clearly logged on both sides, and the alternative (counting
    /// each node's share through the routing table to know who *really* takes
    /// work) is a layer of complexity for a rare case.
    pub fn spawn_release_loop(
        self,
        handles: Partitions,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(cairn_store::io_source::tagged("balance", async move {
            // The release reason waiting for two full periods, and since when.
            let mut pending: Option<(Reason, Instant)> = None;
            let mut donations = Donations::new(settle_for(self.members.ttl()));
            loop {
                cairn_store::pace::beat(interval).await;
                if self.is_draining() {
                    pending = None;
                    continue;
                }
                let shares = self.shares();
                let held = self.held();
                let reason = self.release_reason(&shares, held.len(), &donations);
                let Some(reason) = reason else {
                    pending = None;
                    continue;
                };
                let long_enough = match &pending {
                    Some((r, t)) if *r == reason => t.elapsed() >= interval,
                    _ => {
                        pending = Some((reason.clone(), Instant::now()));
                        false
                    }
                };
                if !long_enough {
                    continue;
                }
                pending = None;
                let Some(p) = held.iter().next_back().copied() else {
                    continue;
                };
                match &reason {
                    Reason::Over => tracing::info!(
                        partition = p,
                        held = held.len(),
                        fair_share = shares.ceil,
                        "over the fair share — yielding a partition"
                    ),
                    Reason::Donate { to, held: theirs } => {
                        donations.donated(to, *theirs);
                        tracing::info!(
                            partition = p,
                            held = held.len(),
                            floor = shares.floor,
                            to = %to,
                            they_hold = theirs,
                            "a node is below the floor — a node at the cap yields a partition"
                        )
                    }
                }
                self.note_release(p, &reason);
                if let Some(h) = handles.get(PartitionId(p)) {
                    // Resign goes through the actor so the writer is sealed in
                    // the right order; the `held` books are decremented when
                    // that partition's factory runs again and calls
                    // `note_lost`. Bounded: an engine that has not entered its
                    // serving loop reads no commands, and the balancing loop
                    // must not stand behind it.
                    match tokio::time::timeout(crate::actor::RESIGN_WAIT, h.resign()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::warn!(partition = p, error = %e, "resign failed, next period retries")
                        }
                        Err(_) => tracing::warn!(
                            partition = p,
                            "resign not done after {:?}, next period checks again",
                            crate::actor::RESIGN_WAIT
                        ),
                    }
                }
            }
        }))
    }

    /// The most recent release decisions of the loop, oldest first.
    pub fn releases(&self) -> Vec<Release> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .releases
            .iter()
            .cloned()
            .collect()
    }

    fn note_release(&self, partition: u32, reason: &Reason) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.releases.len() >= RELEASE_LOG_MAX {
            inner.releases.pop_front();
        }
        inner.releases.push_back(Release {
            at: Instant::now(),
            partition,
            over: matches!(reason, Reason::Over),
        });
    }

    /// Why this node should release a range right now, if at all.
    fn release_reason(
        &self,
        shares: &Shares,
        held: usize,
        donations: &Donations,
    ) -> Option<Reason> {
        if held > shares.ceil {
            return Some(Reason::Over);
        }
        let (to, theirs) = shares.needy.clone()?;
        // Only nodes at the cap, and only **one** of them: the smallest id.
        // Stale hints making two nodes both think they are the giver cost one
        // extra owner change, not an error — the extra range returns to the
        // node below the cap.
        let donor = shares.ceil > shares.floor
            && held >= shares.ceil
            && shares.at_ceiling.iter().all(|n| *n > self.node_id);
        (donor && donations.may_donate(&to, theirs)).then_some(Reason::Donate { to, held: theirs })
    }
}

/// Why the background loop releases a range.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Reason {
    /// Holding more than the fair share.
    Over,
    /// At the cap, and node `to` (holding `held`) is below the floor.
    Donate { to: String, held: usize },
}

/// Backoff of donations: the previous donation did not reach the node below
/// the floor (it still holds the same) ⇒ the next one waits twice as long.
struct Donations {
    base: Duration,
    last: Option<(Instant, String, usize, Duration)>,
}

impl Donations {
    fn new(base: Duration) -> Self {
        Self { base, last: None }
    }

    /// The wait if giving way to `to` (holding `held`) right now.
    fn wait_for(&self, to: &str, held: usize) -> Duration {
        match &self.last {
            Some((_, t, h, wait)) if t == to && held <= *h => (*wait * 2).min(DONATE_BACKOFF_MAX),
            _ => Duration::ZERO,
        }
    }

    fn may_donate(&self, to: &str, held: usize) -> bool {
        match &self.last {
            Some((at, ..)) => at.elapsed() >= self.wait_for(to, held),
            None => true,
        }
    }

    fn donated(&mut self, to: &str, held: usize) {
        let wait = self.wait_for(to, held).max(self.base);
        self.last = Some((Instant::now(), to.to_string(), held, wait));
    }
}

/// A lease contention slot being held. Drop ⇒ the slot is returned; a win ⇒
/// the slot becomes a row in the `held` books, under the **same** lock hold —
/// there is no gap between "one contest fewer" and "one more partition held"
/// for another thread to slip through and get an extra slot.
struct ContendPermit {
    inner: Arc<Mutex<Inner>>,
    won: Option<u32>,
}

impl Drop for ContendPermit {
    fn drop(&mut self) {
        if let Ok(mut i) = self.inner.lock() {
            i.contending = i.contending.saturating_sub(1);
            if let Some(p) = self.won {
                i.held.insert(p);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        cairn_store::LeaseConfig,
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
    };

    async fn session(store: &Arc<dyn ObjectStore>, id: &str) -> Session {
        Session::establish(
            Arc::clone(store),
            &Path::default(),
            LeaseConfig::new()
                .with_node_id(id)
                .with_ttl(Duration::from_millis(300))
                .with_renew_interval(Duration::from_millis(50)),
        )
        .await
        .unwrap()
    }

    // Virtual clock (`start_paused`): sessions, the tracker and `settle_for`
    // all measure with `tokio::time`, so the `sleep`s below are deterministic
    // time steps, not waits on the real clock.
    #[tokio::test(start_paused = true)]
    async fn fair_share_rounds_up_and_counts_self() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session(&store, "a").await;
        let tracker = a.tracker();
        // Just us ⇒ taking everything is right.
        let b = Balancer::new(ClusterConfig::equal(8), "a".into(), a.membership());
        assert_eq!(b.fair_share(), 8);

        // Two live nodes ⇒ ⌈8/2⌉ = 4; three nodes ⇒ ⌈8/3⌉ = 3. LIST only runs
        // every TTL, and `a`'s background loop has just LISTed at establish —
        // wait for the next turn.
        let s2 = session(&store, "b").await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        tracker.refresh().await.unwrap();
        assert_eq!(b.fair_share(), 4);
        let s3 = session(&store, "c").await;
        // LIST only runs every TTL — wait until its turn comes.
        tokio::time::sleep(Duration::from_millis(350)).await;
        tracker.refresh().await.unwrap();
        assert_eq!(b.fair_share(), 3);
        for s in [a, s2, s3] {
            s.close().await;
        }
    }

    /// A node that **comes up midway** does not count in the fair share until
    /// it is alive steadily — otherwise every time a node dies and comes back
    /// (a chaos run: every 10 s, back after 4 s) means two owner changes
    /// instead of one: another node takes its ranges, and 1.5–3 s after it
    /// comes back they are given back to it. A billing workload's 10-minute
    /// chaos run saw 326 leader promotions, 138 of them self-yields; each one
    /// hands over every unfinished invocation of the range (61 105 in 10
    /// minutes).
    ///
    /// Nodes present **when we start** count at once (a whole cluster coming
    /// up together must split evenly at once) — the test above pins that.
    #[tokio::test(start_paused = true)]
    async fn node_joining_midway_not_counted_in_fair_share_until_stable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session(&store, "a").await;
        let tracker = a.tracker();
        let b = Balancer::new(ClusterConfig::equal(8), "a".into(), a.membership());
        // Past `a`'s startup window (floor 5 s).
        tokio::time::sleep(Duration::from_millis(5_500)).await;
        let s2 = session(&store, "b").await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        tracker.refresh().await.unwrap();
        assert!(
            tracker.live_nodes().contains("b"),
            "b must be seen as alive"
        );
        assert_eq!(
            b.fair_share(),
            8,
            "a node that just came up midway and is not steady yet got ranges pulled to it"
        );
        // Alive long enough ⇒ counted.
        tokio::time::sleep(Duration::from_millis(5_000)).await;
        tracker.refresh().await.unwrap();
        assert_eq!(b.fair_share(), 4);
        for s in [a, s2] {
            s.close().await;
        }
    }

    /// A donation that did not reach the node below the floor (it still holds
    /// the same) ⇒ the next one waits twice as long, capped; one that did
    /// (it holds more) ⇒ no extra wait.
    #[test]
    fn failed_yield_doubles_backoff() {
        let base = Duration::from_secs(10);
        let mut d = Donations::new(base);
        assert!(d.may_donate("c", 0));
        d.donated("c", 0);
        assert_eq!(d.wait_for("c", 0), base * 2);
        assert_eq!(d.wait_for("c", 1), Duration::ZERO, "reached: no backoff");
        assert_eq!(d.wait_for("d", 0), Duration::ZERO, "other node: no backoff");
        for _ in 0..10 {
            d.donated("c", 0);
        }
        assert_eq!(d.wait_for("c", 0), DONATE_BACKOFF_MAX);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn permit_blocks_over_grab_under_concurrent_contention() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session(&store, "a").await;
        let b = Balancer::new(ClusterConfig::equal(4), "a".into(), a.membership());
        // Just us ⇒ fair = 4. Ask for 6 slots: only 4 are granted.
        let permits: Vec<_> = (0..6).filter_map(|_| b.try_contend()).collect();
        assert_eq!(permits.len(), 4);
        // Give all back ⇒ can ask again.
        drop(permits);
        assert!(b.try_contend().is_some());
        a.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn win_turns_permit_into_held_loss_returns_permit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session(&store, "a").await;
        let b = Balancer::new(ClusterConfig::equal(2), "a".into(), a.membership());
        let mut p1 = b.try_contend().unwrap();
        p1.won = Some(0);
        drop(p1);
        assert_eq!(b.held().into_iter().collect::<Vec<_>>(), vec![0]);
        // fair = 2, held = 1 ⇒ exactly one slot left.
        let p2 = b.try_contend().unwrap();
        assert!(b.try_contend().is_none(), "over the fair share");
        drop(p2);
        a.close().await;
    }
}
