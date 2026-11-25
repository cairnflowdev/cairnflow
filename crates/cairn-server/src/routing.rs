// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Who holds which partition, and at which address.
//!
//! **No channel of its own.** This table is built from the lease layer in the bucket
//! itself — something that has to exist anyway to elect leaders. So "where is the leader
//! of partition p" does not need a gossip protocol to write, run and debug when it is wrong.
//!
//! **Built from hints, not by reading each partition** (D20). Each node's session
//! carries an `owns` hint (partition → term), and the session tracker already GETs them
//! every TTL/3 to measure liveness. The table only re-reads what the tracker already has,
//! through [`cairn_store::Membership`] — its cost does not grow with the number of
//! partitions. Only a partition with **no** hint (changing owner, or its owner just died)
//! needs its owner record read.
//!
//! In exchange the table is **always slightly stale**: up to one observation tick. That
//! is acceptable because it is only used to *route*, not to decide who may write. A wrong
//! route only means the client gets `NotLeader` from the other node and asks again —
//! annoying, not dangerous. Whoever sees the table is wrong first (a failed send, answering
//! `NotLeader`) calls [`RoutingTable::request_refresh`] to observe again right away.
//!
//! **The owner that just released.** If a range has no leader and its owner record was
//! released by another session that is **still alive**, the table remembers that session
//! separately ([`RoutingTable::releaser_of`]) — not as leader: `/cluster`, the hints for
//! clients and every user of [`RoutingTable::leader_of`] still see "nobody". Only the
//! inter-range transport uses it, as a place to try asking: if the range has left that
//! owner's table (split/merge) it answers `409 wrong_range` with the new table — the only
//! way a node holding an old table that cannot load `cluster.conf` learns the table.

use {
    cairn_store::{read_owner, ClusterConfig, PartitionId, Session},
    slatedb::object_store::{path::Path, ObjectStore},
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
    tokio::sync::Notify,
};

/// Where a partition is being led.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leader {
    pub node_id: String,
    /// The node's **internal port** — where inter-range messages and range hand-over
    /// requests go. `None` if the lease owner advertises no address — the partition still
    /// runs, it just cannot be routed to.
    pub address: Option<String>,
    /// The node's **ingress port** — what is shown to *clients* (the `leader` hint of a
    /// `503`, `/cluster`). Never used for node-to-node sends.
    pub ingress: Option<String>,
    pub term: u64,
}

/// Minimum gap between two refresh rounds, even when asked for continuously.
pub const MIN_REFRESH_GAP: Duration = Duration::from_millis(200);

#[derive(Clone, Default)]
pub struct RoutingTable {
    inner: Arc<RwLock<BTreeMap<u32, Leader>>>,
    /// "The table may be stale — refresh now." The refresh round listens to it.
    stale: Arc<Notify>,
    /// The table's generation: bumped each time an entry **really** changes (added, removed,
    /// a different leader, term or address). Anything remembered per table (the transport
    /// remembering nodes without a batch route) compares with it to know when to forget.
    generation: Arc<AtomicU64>,
    /// Range without a leader → another live session that just released it (see the
    /// module's top). Not counted in `generation`: nobody sends to it as they send
    /// to a leader.
    releasers: Arc<RwLock<BTreeMap<u32, Leader>>>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a partition's leader by hand — for the node's own takes (the engine
    /// factory) and for tests.
    pub fn insert(&self, p: PartitionId, leader: Leader) {
        self.set(p, Some(leader));
    }

    /// Remove a range — the range has left the table.
    pub fn remove(&self, p: PartitionId) {
        self.set(p, None);
        self.set_releaser(p, None);
    }

    /// Ask the refresh round to run now, without waiting for its period. Returns at once;
    /// several requests while a round is running merge into one round after it.
    ///
    /// Call it when there is evidence the table is wrong: a failed send, or this node
    /// itself having to answer `NotLeader`.
    pub fn request_refresh(&self) {
        self.stale.notify_one();
    }

    pub fn leader_of(&self, p: PartitionId) -> Option<Leader> {
        self.inner.read().ok()?.get(&p.0).cloned()
    }

    /// Another live session that just released `p`, when `p` has no leader — only for the
    /// inter-range transport to **try asking** (see the module's top), not a leader.
    pub fn releaser_of(&self, p: PartitionId) -> Option<Leader> {
        if self.leader_of(p).is_some() {
            return None;
        }
        self.releasers.read().ok()?.get(&p.0).cloned()
    }

    fn set_releaser(&self, p: PartitionId, releaser: Option<Leader>) {
        if let Ok(mut m) = self.releasers.write() {
            match releaser {
                Some(l) => m.insert(p.0, l),
                None => m.remove(&p.0),
            };
        }
    }

    pub fn snapshot(&self) -> BTreeMap<u32, Leader> {
        self.inner.read().map(|m| m.clone()).unwrap_or_default()
    }

    /// The current generation — see the `generation` field.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn set(&self, p: PartitionId, leader: Option<Leader>) {
        if let Ok(mut m) = self.inner.write() {
            self.put(&mut m, p, leader);
        }
    }

    /// Write one entry under the write lock already held; a real change bumps the generation.
    fn put(&self, m: &mut BTreeMap<u32, Leader>, p: PartitionId, leader: Option<Leader>) {
        let changed = match leader {
            Some(l) => m.insert(p.0, l.clone()).as_ref() != Some(&l),
            None => m.remove(&p.0).is_some(),
        };
        if changed {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Like [`Self::set`] — unless **we** hold `p`: then write us.
    ///
    /// `rebuild` snapshots `owned()` once at the start of the round and only then reads each
    /// range's owner record (with `await`s). If we take a range between those two moments,
    /// its owner record already points at us but the snapshot does not — and this round's
    /// `None` overwrites the "us" entry the engine factory just wrote, until the next round
    /// (up to one TTL). The very node leading the range would have a `/cluster` saying
    /// "nobody", and inter-range messages to that range would wait. Re-checking `owned()`
    /// **under the table's write lock** lets no ordering slip through: a claim updates
    /// `owned()` before the engine factory writes the table, so either we see it here or the
    /// factory's write comes later.
    fn set_unless_ours(&self, session: &Session, p: PartitionId, leader: Option<Leader>) {
        if let Ok(mut m) = self.inner.write() {
            let leader = match session.owned().get(&p.0) {
                Some(term) => Some(Leader {
                    node_id: session.node_id().to_string(),
                    address: session.address().map(str::to_string),
                    ingress: session.ingress().map(str::to_string),
                    term: *term,
                }),
                None => leader,
            };
            self.put(&mut m, p, leader);
        }
    }

    /// Rebuild the table once from the sessions' hints.
    ///
    /// Walks the **current range table**: ranges that left it are removed from the routing
    /// table. Priority for each range: we hold it ⇒ us; an `owns` hint of another live
    /// session ⇒ that session; no hint at all ⇒ read the owner record, and only trust it if
    /// the session it points to is not dead. An owner record **released** by another
    /// session that is not dead ⇒ the range has no leader, but that session becomes the
    /// range's releaser ([`Self::releaser_of`]).
    pub async fn rebuild(
        &self,
        session: &Session,
        store: &Arc<dyn ObjectStore>,
        prefix: &Path,
        cluster: &ClusterConfig,
    ) {
        let members = session.membership();
        let hints = members.holders();
        let mine = session.owned();
        let me = session.current();
        let table = cluster.table();
        if let Ok(mut m) = self.inner.write() {
            let before = m.len();
            m.retain(|p, _| table.contains(PartitionId(*p)));
            if m.len() != before {
                self.generation.fetch_add(1, Ordering::AcqRel);
            }
        }
        if let Ok(mut m) = self.releasers.write() {
            m.retain(|p, _| table.contains(PartitionId(*p)));
        }
        for p in table.partitions() {
            if let Some(term) = mine.get(&p.0) {
                self.set_releaser(p, None);
                self.set(
                    p,
                    Some(Leader {
                        node_id: session.node_id().to_string(),
                        address: session.address().map(str::to_string),
                        ingress: session.ingress().map(str::to_string),
                        term: *term,
                    }),
                );
                continue;
            }
            if let Some(h) = hints.get(&p.0) {
                self.set_releaser(p, None);
                self.set_unless_ours(
                    session,
                    p,
                    Some(Leader {
                        node_id: h.session.node.clone(),
                        address: h.address.clone(),
                        ingress: h.ingress.clone(),
                        term: h.term,
                    }),
                );
                continue;
            }
            match read_owner(store, prefix, p).await {
                Ok(Some(r))
                    if !r.released
                        && Some(&r.session) != me.as_ref()
                        && !members.looks_dead(&r.session) =>
                {
                    // The owner record only carries the internal port; the ingress port comes
                    // from the owner's session (the tracker has read it).
                    let ingress = members.ingress_of(&r.session);
                    self.set_releaser(p, None);
                    self.set_unless_ours(
                        session,
                        p,
                        Some(Leader {
                            node_id: r.session.node,
                            address: r.address,
                            ingress,
                            term: r.term,
                        }),
                    )
                }
                // Released by another session that is not dead (a hand-over, or a tombstone
                // after split/merge): nobody leads, but the releaser is a place to try
                // asking — if the range has left **its** table it answers with the new table.
                // Our table may be the only old table we have (`cluster.conf`
                // unreadable): without this path it is `NoLeader` forever, and no refusal
                // ever arrives to learn the table from.
                Ok(Some(r))
                    if r.released
                        && Some(&r.session) != me.as_ref()
                        && !members.looks_dead(&r.session) =>
                {
                    // A released record carries no address: take the internal port that
                    // session advertises (the membership hint).
                    let address = members
                        .peers()
                        .into_iter()
                        .find(|(id, _)| id == &r.session)
                        .and_then(|(_, a)| a)
                        .or(r.address);
                    let ingress = members.ingress_of(&r.session);
                    self.set_releaser(
                        p,
                        Some(Leader {
                            node_id: r.session.node,
                            address,
                            ingress,
                            term: r.term,
                        }),
                    );
                    self.set_unless_ours(session, p, None)
                }
                Ok(_) => {
                    self.set_releaser(p, None);
                    self.set_unless_ours(session, p, None)
                }
                // Keep the old entry on a read error. One failed GET is no evidence the
                // partition lost its leader — and removing the entry would turn a passing
                // network hiccup into "no idea where to send".
                Err(e) => {
                    tracing::debug!(partition = p.0, error = %e, "cannot read the owner record")
                }
            }
        }
    }

    /// Watch sessions in the background: rebuild the table whenever the tracker reports a
    /// change, every `every` (the safety net), or when someone calls [`Self::request_refresh`]
    /// — then also ask the tracker to observe again right away. Never more often than [`MIN_REFRESH_GAP`].
    pub fn follow(
        self,
        session: Session,
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        cluster: ClusterConfig,
        every: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(cairn_store::io_source::tagged("routing", async move {
            let members = session.membership();
            let mut changes = members.changed();
            loop {
                changes.borrow_and_update();
                self.rebuild(&session, &store, &prefix, &cluster).await;
                // Floor between two rounds: a burst of failed sends at once (every row to
                // a node that just died) must not become a burst of observations.
                cairn_store::pace::beat(MIN_REFRESH_GAP.min(every)).await;
                // `biased`: the "stale table" message first (it also asks the tracker to
                // refresh), then session changes, then the tick; whichever branch fires, the
                // next round rebuilds the whole table.
                tokio::select! {
                    biased;
                    _ = self.stale.notified() => members.request_refresh(),
                    _ = changes.changed() => {}
                    _ = tokio::time::sleep(every.saturating_sub(MIN_REFRESH_GAP)) => {}
                }
            }
        }))
    }
}
