// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`Membership`] — the cluster as this node sees it: who is alive, where, holding what,
//! how healthy.
//!
//! One interface for every user of membership **hints**: routing (`routing.rs`), balancing
//! (`balance.rs`), shedding when slow (`slow.rs`), the shared WAL stream rotation net
//! (`node/start.rs`), book change reports (`registry_sync.rs`) and graceful shutdown reports
//! (`node/running.rs`). There are two sources: the session's bucket observation loop
//! ([`SessionTracker`]), and the `view` the coordinator returns over the heartbeat — used
//! while fresh, falling back to the tracker when not. Users do not change anything when the
//! source changes: every place in `cairn-server` reads membership through here, not through
//! [`Session::tracker`].
//!
//! # No "dead" decision here (LS-3)
//!
//! Everything [`Membership`] returns is a **hint**: it may be up to one observation tick
//! stale (or the `view`'s age), it may be missing a node that just joined. It is only used
//! to *point the way* and *choose who should try*. When wrong, the distribution is
//! temporarily skewed, and a misdirected message gets `NotLeader`/`409` and is sent again.
//!
//! Irreversible actions based on "session X is dead" — taking a range ([`Session::claim`]),
//! a tombstone on its behalf (`gc.rs:OrphanRetirer`), the migration lock
//! (`migrate.rs:holder_is_dead`) — do **not** go through here. They call
//! [`Session::claim`] / [`Session::is_session_dead`], i.e. the internal tracker (an ended
//! state that was read, or our own winning `expired` CAS) plus a direct GET of the session
//! record: "dead" only comes from the bucket (LS-1, LS-3). The coordinator's `view` also
//! carries "ended according to the bucket" states, but over the network: they may only be a
//! hint in [`Self::looks_dead`], never wired into `is_session_dead`.
//!
//! # The contract for the second source
//!
//! - Synchronous functions without I/O, reading tables in RAM; only
//!   [`Self::discover_peers`] may read the store.
//! - "Other" means not counting our own current incarnation; [`Self::live_nodes`] and
//!   [`Self::settled_nodes`] always include ourselves.
//! - [`Self::changed`] must change whenever a result of the functions above may change —
//!   including when the source switches between `view` and the tracker.
//! - [`Self::request_refresh`] means "the table may be wrong, refresh now": one full
//!   observation round of the tracker, plus an early heartbeat in listening mode.
//!
//! # The `view` source
//!
//! The control loop (`cairn-server/src/control.rs`) calls [`Membership::hear`] with the
//! `view` of each ack, and [`Membership::forget_view`] when it stops listening. A `view` is
//! **fresh** when the heartbeat carrying it was sent within the last TTL/3 (our clock); once
//! it is no longer fresh every function returns to the tracker by itself at read time,
//! without waiting for anyone to tell it — the fallback is the protocol without a
//! coordinator. Sessions in the `view` are added to the tracker ([`SessionTracker::adopt`]),
//! so the sparse tracker ([`Observing::Sparse`]) and the suspicion marks also cover nodes we
//! have never seen in a LIST.

use super::*;

/// A session in the coordinator's `view`, as this node receives it. "Ended" follows the
/// bucket — the coordinator's tracker read or CASed it — but it arrives here over the
/// network: only a hint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewMember {
    pub id: SessionId,
    pub ended: bool,
    pub address: Option<String>,
    pub ingress: Option<String>,
    pub owns: BTreeMap<u32, u64>,
    pub health: crate::health::Published,
    /// The coordinator has seen it silent for more than 2·TTL without being able to expire
    /// it — the receiving node observes it itself ([`Observing::Sparse`]).
    pub suspect: bool,
}

/// The part of a [`ViewMember`] whose changes [`Membership`] users see — like the tracker:
/// the slowness score and p99 change every round without waking anyone.
type Shape<'a> = (
    &'a SessionId,
    bool,
    &'a Option<String>,
    &'a Option<String>,
    &'a BTreeMap<u32, u64>,
    bool,
);

impl ViewMember {
    fn shape(&self) -> Shape<'_> {
        (
            &self.id,
            self.ended,
            &self.address,
            &self.ingress,
            &self.owns,
            self.health.draining,
        )
    }
}

/// The last `view` received, and when the heartbeat whose ack carried it was **sent**.
struct Heard {
    members: Vec<ViewMember>,
    sent: Instant,
}

/// The second source of [`Membership`] — shared by every `Clone`.
#[derive(Default)]
struct ViewSlot {
    heard: Mutex<Option<Heard>>,
    /// [`Membership::request_refresh`] in listening mode: ask for an early heartbeat.
    early: Notify,
}

/// This node's cluster membership hints — see the module header. Cheap `Clone`, every copy
/// shares one source.
#[derive(Clone)]
pub struct Membership {
    tracker: SessionTracker,
    view: Arc<ViewSlot>,
}

impl Membership {
    pub(super) fn new(tracker: SessionTracker) -> Self {
        Self {
            tracker,
            view: Arc::default(),
        }
    }

    // ───────────────────────────────────────── the `view` source

    /// A `view` just arrived in the ack of a heartbeat sent at `sent` (our clock). New
    /// sessions in the `view` are added to the tracker (as if a LIST had seen them — so the
    /// sparse tracker and the suspicion marks cover them); a session the `view` reports as
    /// just ended wakes [`Self::ended`].
    pub fn hear(&self, members: Vec<ViewMember>, sent: Instant) {
        let (changed, ended) = {
            let mut heard = self.view.heard.lock().expect("mutex");
            let (changed, ended) = match heard.as_ref() {
                // The source switches from the tracker to the `view`.
                None => (true, false),
                Some(old) => {
                    let was = |m: &ViewMember| old.members.iter().find(|o| o.id == m.id);
                    let changed = old.members.len() != members.len()
                        || members
                            .iter()
                            .any(|m| was(m).is_none_or(|o| o.shape() != m.shape()));
                    let ended = members
                        .iter()
                        .any(|m| m.ended && was(m).is_none_or(|o| !o.ended));
                    (changed, ended)
                }
            };
            *heard = Some(Heard {
                members: members.clone(),
                sent,
            });
            (changed, ended)
        };
        for m in members.iter().filter(|m| !m.ended) {
            self.tracker.adopt(&m.id);
        }
        if changed {
            self.tracker.bump();
        }
        if ended {
            self.tracker.bump_ended();
        }
    }

    /// Stops listening to the `view` (stale, coordinator lost, or we became the
    /// coordinator): every function returns to the tracker.
    pub fn forget_view(&self) {
        let had = self.view.heard.lock().expect("mutex").take().is_some();
        if had {
            self.tracker.bump();
        }
    }

    /// When the current `view` stops being fresh (TTL/3 after the heartbeat carrying it was
    /// sent); `None` = no `view`.
    pub fn view_fresh_until(&self) -> Option<Instant> {
        let heard = self.view.heard.lock().expect("mutex");
        heard.as_ref().map(|h| h.sent + self.tracker.ttl() / 3)
    }

    /// Taking membership from the `view` (there is a `view` and it is still fresh).
    pub fn listening(&self) -> bool {
        self.view_fresh_until()
            .is_some_and(|until| Instant::now() < until)
    }

    /// Waits for a refresh request ([`Self::request_refresh`]) — the heartbeat loop waits
    /// on this to send early.
    pub async fn refresh_requested(&self) {
        self.view.early.notified().await
    }

    /// `f` over the members of the fresh `view` **other** than us; `None` = not listening.
    fn with_view<R>(&self, f: impl FnOnce(std::vec::IntoIter<&ViewMember>) -> R) -> Option<R> {
        let heard = self.view.heard.lock().expect("mutex");
        let h = heard.as_ref()?;
        if h.sent + self.tracker.ttl() / 3 <= Instant::now() {
            return None;
        }
        let me = self.tracker.me();
        let others: Vec<&ViewMember> = h
            .members
            .iter()
            .filter(|m| Some(&m.id) != me.as_ref())
            .collect();
        Some(f(others.into_iter()))
    }

    // ───────────────────────────────────────────────── interface

    /// The lease TTL the cluster uses to judge life and death — the unit of every
    /// "settling" interval (`balance.rs:settle_for`).
    pub fn ttl(&self) -> Duration {
        self.tracker.ttl()
    }

    /// Live nodes, counted by `node`. Always includes ourselves.
    pub fn live_nodes(&self) -> BTreeSet<String> {
        let me = self.tracker.me();
        self.with_view(|others| {
            let mut out: BTreeSet<String> = others
                .filter(|m| !m.ended)
                .map(|m| m.id.node.clone())
                .collect();
            if let Some(me) = &me {
                out.insert(me.node.clone());
            }
            out
        })
        .unwrap_or_else(|| self.tracker.live_nodes())
    }

    /// Live nodes **that have settled** (present since we started, or alive continuously
    /// for at least `settle`) — the denominator of the fair share. Always includes
    /// ourselves. See [`SessionTracker::settled_nodes`]; in listening mode, the "first seen"
    /// mark is still our tracker's (sessions in the `view` are added to it).
    pub fn settled_nodes(&self, settle: Duration) -> BTreeSet<String> {
        let me = self.tracker.me();
        self.with_view(|others| {
            let mut out: BTreeSet<String> = others
                .filter(|m| !m.ended && self.tracker.settled(&m.id, settle))
                .map(|m| m.id.node.clone())
                .collect();
            if let Some(me) = &me {
                out.insert(me.node.clone());
            }
            out
        })
        .unwrap_or_else(|| self.tracker.settled_nodes(settle))
    }

    /// HTTP addresses (internal ports) of the **other** live sessions, each address once,
    /// from the current table — reads nothing. A node that has just joined may be missing;
    /// when completeness matters use [`Self::discover_peers`].
    pub fn peer_addresses(&self) -> Vec<String> {
        self.with_view(|others| {
            let mut out: Vec<String> = others
                .filter(|m| !m.ended)
                .filter_map(|m| m.address.clone())
                .collect();
            out.sort();
            out.dedup();
            out
        })
        .unwrap_or_else(|| self.tracker.peer_addresses())
    }

    /// Like [`Self::peer_addresses`], after looking for nodes **right now** — for the rare
    /// reports that must reach even a node that has just joined (book writes, graceful
    /// shutdown). With a fresh `view`: the `view` itself (a new node reports itself to the
    /// coordinator over the heartbeat), no I/O. Otherwise one LIST of `sessions/` + one GET
    /// per session (`session.tracker`).
    pub async fn discover_peers(&self) -> Vec<String> {
        if self.listening() {
            return self.peer_addresses();
        }
        self.tracker.discover_peers().await
    }

    /// The **other** live sessions (not ended according to a fresh `view`, otherwise
    /// according to the tracker's table), one entry per session with its internal port
    /// address — for reports that **each** session must confirm (synchronous key
    /// revocation). Reads nothing; when completeness matters call [`Self::discover_peers`]
    /// first.
    pub fn peers(&self) -> Vec<(SessionId, Option<String>)> {
        self.with_view(|others| {
            others
                .filter(|m| !m.ended)
                .map(|m| (m.id.clone(), m.address.clone()))
                .collect()
        })
        .unwrap_or_else(|| {
            self.tracker
                .snapshot()
                .into_iter()
                .filter(|o| !o.state.is_terminal())
                .map(|o| (o.id, o.address))
                .collect()
        })
    }

    /// Like [`Self::peers`], but from the tracker's LIST of `sessions/` + GETs **right
    /// now** — not from the coordinator's `view`, not from the existing table — and `Err`
    /// when the LIST fails. For "every node has applied it" answers (key revocation): a fresh
    /// `view` may still miss a node that has just written its session but not yet sent a
    /// heartbeat, and the existing table misses a node that joined after the previous LIST.
    /// A node writes its session **before** it reads the key book at startup
    /// (`node/start.rs`), so every node that read the book before a write is present in a
    /// LIST after that write. A session the LIST sees whose GET has not been read yet counts
    /// as alive — "unknown" is never "ended".
    pub async fn list_peers_now(&self) -> Result<Vec<(SessionId, Option<String>)>> {
        self.tracker.list_now().await?;
        Ok(self
            .tracker
            .snapshot()
            .into_iter()
            .filter(|o| !o.state.is_terminal())
            .map(|o| (o.id, o.address))
            .collect())
    }

    /// The peers of [`Self::list_peers_now`] whose record **on the bucket** has not ended:
    /// one GET per listed peer, right now, read only (no tracker table update, no
    /// challenge, no `expired` CAS — the observation schedule is unchanged). A sparse
    /// observer reads a known session again only every `SPARSE_AFTER_TTLS` TTLs, so its
    /// table can count a session live long after the coordinator wrote `expired`; for
    /// "every live node has applied it" answers a node whose record says `expired`/`closed`
    /// (or is gone) is not a live node. A failed GET or an undecodable record ⇒ the peer
    /// stays: "unknown" is never "ended".
    pub async fn live_peers_now(&self) -> Result<Vec<(SessionId, Option<String>)>> {
        let peers = self.list_peers_now().await?;
        let ended =
            futures::future::join_all(peers.iter().map(|(id, _)| self.tracker.ended_on_bucket(id)))
                .await;
        Ok(peers
            .into_iter()
            .zip(ended)
            .filter(|(_, ended)| !ended)
            .map(|(peer, _)| peer)
            .collect())
    }

    /// Whether `id`'s record **on the bucket** has ended (`expired`/`closed`, or gone): one
    /// GET right now, read only. A failed GET or an undecodable record ⇒ `false` —
    /// "unknown" is never "ended".
    pub async fn ended_on_bucket(&self, id: &SessionId) -> bool {
        self.tracker.ended_on_bucket(id).await
    }

    /// Partition → owner, according to the `owns` hints of the **other** live sessions; with
    /// two hints for one partition the higher term wins.
    pub fn holders(&self) -> BTreeMap<u32, Holder> {
        self.with_view(|others| {
            let mut out: BTreeMap<u32, Holder> = BTreeMap::new();
            for m in others.filter(|m| !m.ended) {
                for (p, term) in &m.owns {
                    if out.get(p).is_none_or(|h| h.term < *term) {
                        out.insert(
                            *p,
                            Holder {
                                session: m.id.clone(),
                                address: m.address.clone(),
                                ingress: m.ingress.clone(),
                                term: *term,
                            },
                        );
                    }
                }
            }
            out
        })
        .unwrap_or_else(|| self.tracker.holders())
    }

    /// The ingress address session `id` declares, as last seen.
    pub fn ingress_of(&self, id: &SessionId) -> Option<String> {
        self.with_view(|mut others| others.find(|m| &m.id == id).and_then(|m| m.ingress.clone()))
            .flatten()
            .or_else(|| self.tracker.ingress_of(id))
    }

    /// Number of ranges each **other** node holds, according to the `owns` hints; a live
    /// node holding nothing is present with 0.
    pub fn loads(&self) -> BTreeMap<String, usize> {
        let me = self.tracker.me().map(|m| m.node);
        self.with_view(|others| {
            let mut out: BTreeMap<String, usize> = BTreeMap::new();
            for m in others.filter(|m| !m.ended && Some(&m.id.node) != me.as_ref()) {
                *out.entry(m.id.node.clone()).or_default() += m.owns.len();
            }
            out
        })
        .unwrap_or_else(|| self.tracker.loads())
    }

    /// The health the **other** live nodes publish (`slow=`, `p99_put_ms=`, `draining=`),
    /// merged by node.
    pub fn peer_health(&self) -> BTreeMap<String, crate::health::Published> {
        let me = self.tracker.me().map(|m| m.node);
        self.with_view(|others| {
            let mut out: BTreeMap<String, crate::health::Published> = BTreeMap::new();
            for m in others.filter(|m| !m.ended && Some(&m.id.node) != me.as_ref()) {
                let e = out.entry(m.id.node.clone()).or_default();
                e.slow = e.slow.max(m.health.slow);
                e.p99_put_ms = e.p99_put_ms.max(m.health.p99_put_ms);
                e.draining |= m.health.draining;
            }
            out
        })
        .unwrap_or_else(|| self.tracker.peer_health())
    }

    /// Session `id` looks ended — **only a hint**, so routing skips owner records pointing
    /// at it. Not for deciding to take or delete anything: that path is
    /// [`Session::is_session_dead`] (see the module header). In listening mode: the `view`
    /// says ended, **or** our tracker has read it.
    pub fn looks_dead(&self, id: &SessionId) -> bool {
        self.tracker.is_dead(id)
            || self
                .with_view(|mut others| others.any(|m| &m.id == id && m.ended))
                .unwrap_or(false)
    }

    /// Changes whenever the set of live sessions, the `owns` hints, or a dead session
    /// changes — including when the source switches between `view` and the tracker.
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.tracker.changed()
    }

    /// Changes whenever an **other** session has just ended according to the bucket: our
    /// tracker read or CASed the ended state, or the `view` (the coordinator's tracker)
    /// reports it for the first time. For the tombstone-on-behalf loop (SM-11): a nudge only
    /// opens a round — that round decides "dead" by itself with [`Session::is_session_dead`],
    /// so a report over the network decides nothing.
    pub fn ended(&self) -> watch::Receiver<u64> {
        self.tracker.ended()
    }

    /// Asks for a refresh now — there is evidence the table is wrong (a failed send,
    /// `NotLeader`, a peer reporting shutdown): one full observation round of the tracker (in
    /// every mode), plus an early heartbeat in listening mode.
    pub fn request_refresh(&self) {
        self.tracker.request_refresh();
        self.view.early.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use {super::*, slatedb::object_store::memory::InMemory};

    const TTL: Duration = Duration::from_millis(300);

    async fn session_on(store: &Arc<dyn ObjectStore>, node: &str) -> Session {
        let cfg = LeaseConfig::new()
            .with_node_id(node)
            .with_ttl(TTL)
            .with_renew_interval(Duration::from_millis(50))
            .with_observe_interval(TTL / 3)
            .with_address(format!("http://{node}"));
        Session::establish(Arc::clone(store), &Path::from(""), cfg)
            .await
            .unwrap()
    }

    fn member(id: &SessionId, owns: &[(u32, u64)], ended: bool) -> ViewMember {
        ViewMember {
            id: id.clone(),
            ended,
            address: Some(format!("http://{}", id.node)),
            ingress: None,
            owns: owns.iter().copied().collect(),
            health: Default::default(),
            suspect: false,
        }
    }

    /// A fresh `view` is the source of every function; once stale (TTL/3 after sending) they
    /// return to the tracker by themselves at read time; changing the source and changing
    /// the content wake `changed()`; a session the `view` reports as ended wakes `ended()` and
    /// is only a `looks_dead` hint — never `is_session_dead`.
    #[tokio::test]
    async fn fresh_view_is_the_source_and_a_stale_one_falls_back_to_the_tracker() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        let m = a.membership();
        // A real session, `live` on the bucket — the `view` will lie about it.
        let z = session_on(&store, "z").await;
        let ghost = z.current().unwrap();
        let me = a.current().unwrap();
        let mut changed = m.changed();
        changed.borrow_and_update();
        let mut ended = m.ended();
        ended.borrow_and_update();
        m.hear(
            vec![
                member(&me, &[(0, 1)], false),
                member(&ghost, &[(3, 2)], false),
            ],
            Instant::now(),
        );
        assert!(m.listening());
        assert!(changed.has_changed().unwrap(), "source switched ⇒ changed");
        changed.borrow_and_update();
        ended.borrow_and_update();
        assert_eq!(
            m.holders().get(&3).map(|h| h.session.clone()),
            Some(ghost.clone())
        );
        assert!(!m.holders().contains_key(&0), "ourselves not counted");
        assert_eq!(m.peer_addresses(), vec!["http://z".to_string()]);
        assert!(m.live_nodes().contains("z") && m.live_nodes().contains("a"));
        assert_eq!(m.loads().get("z"), Some(&1));
        assert!(
            a.tracker().state_of(&ghost).is_some(),
            "the view's session is added to the tracker"
        );

        m.hear(
            vec![
                member(&me, &[(0, 1)], false),
                member(&ghost, &[(3, 2)], true),
            ],
            Instant::now(),
        );
        assert!(changed.has_changed().unwrap());
        assert!(ended.has_changed().unwrap(), "view reports ended ⇒ ended");
        assert!(m.looks_dead(&ghost));
        assert!(m.holders().is_empty());
        assert!(
            !a.is_session_dead(&ghost).await.unwrap(),
            "\"dead\" only from the bucket: the view saying ended does not make the session dead"
        );

        tokio::time::sleep(TTL / 3 + Duration::from_millis(20)).await;
        assert!(!m.listening(), "view older than TTL/3");
        assert_eq!(m.holders(), a.tracker().holders(), "source is the tracker");
        m.forget_view();
        assert!(changed.has_changed().unwrap(), "not listening ⇒ changed");
        a.close().await;
        z.close().await;
    }

    /// **A killed node stayed in `pending` of every confirmed admin write.** Under chaos (a
    /// node killed every 10 s) `PUT /admin/tenants/{t}/state` answered `202` for 60 s: the
    /// node serving it observes sessions sparsely (delegation on), so it read a dead
    /// incarnation's record again only every `SPARSE_AFTER_TTLS` TTLs, and kept asking it
    /// to confirm long after the coordinator had written `expired`.
    ///
    /// 1. `a` observes sparsely and has read `b` live.
    /// 2. `b` ends: its record on the bucket says `closed`.
    /// 3. `a`'s table still lists `b` (no GET due yet).
    ///
    /// Must see: `live_peers_now` reads the record and leaves `b` out.
    #[tokio::test]
    async fn live_peers_now_leaves_out_a_session_ended_on_the_bucket() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        let b = session_on(&store, "b").await;
        let b_id = b.current().unwrap();
        let t = a.tracker();
        t.refresh().await.unwrap();
        t.set_observing(Observing::Sparse {
            suspects: BTreeSet::new(),
        });
        t.refresh().await.unwrap();
        b.close().await;
        let m = a.membership();
        assert!(
            m.list_peers_now()
                .await
                .unwrap()
                .iter()
                .any(|(id, _)| id == &b_id),
            "precondition: the sparse table still counts b live"
        );
        let live = m.live_peers_now().await.unwrap();
        assert!(
            !live.iter().any(|(id, _)| id == &b_id),
            "b's record says closed, yet it is a live peer: {live:?}"
        );
        a.close().await;
    }

    /// The sparse tracker still marks a silent session `expired` (LS-1, just later); a
    /// suspected session is observed every tick.
    #[tokio::test]
    async fn sparse_tracker_watches_suspects_every_round() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        let b = session_on(&store, "b").await;
        let b_id = b.current().unwrap();
        let t = a.tracker();
        t.refresh().await.unwrap();
        assert!(t.state_of(&b_id).is_some());
        t.set_observing(Observing::Sparse {
            suspects: BTreeSet::new(),
        });
        // The last full round (if any) is done; b dies.
        t.refresh().await.unwrap();
        b.abort();
        let t0 = Instant::now();
        // Not suspected: close to `SPARSE_AFTER_TTLS` TTLs it is still not expired (each
        // session every that many TTLs).
        let quiet = TTL * crate::session::SPARSE_AFTER_TTLS - TTL / 2;
        while t0.elapsed() < quiet {
            t.refresh().await.unwrap();
            tokio::time::sleep(TTL / 3).await;
        }
        assert!(
            !t.is_dead(&b_id),
            "the sparse tracker does not GET b before SPARSE_AFTER_TTLS·TTL"
        );
        t.set_observing(Observing::Sparse {
            suspects: BTreeSet::from([b_id.clone()]),
        });
        let t1 = Instant::now();
        while !t.is_dead(&b_id) {
            assert!(t1.elapsed() < TTL * 3, "b suspected but not yet expired");
            t.refresh().await.unwrap();
            tokio::time::sleep(TTL / 3).await;
        }
        a.close().await;
    }
}
