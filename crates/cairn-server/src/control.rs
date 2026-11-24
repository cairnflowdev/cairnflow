// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The node's control plane**: who is the cluster's coordinator, and
//! heartbeats to it.
//!
//! The role and its fence live in the session layer
//! (`cairn_store::Session::try_coordinate`, `coordinator_fence`, the module
//! head of `cairn-store/src/session/coordinator.rs`); this module is the
//! **schedule** and the **wiring**: one background loop per node, at interval
//! `renew_interval` (TTL/5), plus the coordinator's heartbeat batching loop.
//!
//! # Who holds the role
//!
//! - **The coordinator** (holding the role under the current incarnation):
//!   fences every heartbeat batch; when idle (nobody heartbeating) it fences
//!   every [`IDLE_FENCE_TTLS`] TTLs — to notice a lost role even when nobody
//!   asks.
//! - **Other nodes**: read `control/coordinator` (`If-None-Match`, almost
//!   always `304`) every TTL **while they have no fresh ack**, or when kicked
//!   (a peer's graceful-shutdown notice). An empty record (absent, garbage,
//!   released, pointing at a session already ended on the bucket) is taken at
//!   once by **the node running GC** ([`crate::gc::is_runner`]); other nodes
//!   take it when the situation is unchanged after TTL + jitter. A node that
//!   just released or lost the role does not take it back for
//!   [`cairn_store::RETAKE_AFTER_TTLS`] TTLs.
//! - The incarnation changes: the old incarnation's role is released — the
//!   role does not carry over to the new incarnation.
//!
//! # Heartbeats
//!
//! Every node other than the coordinator sends `POST /internal/heartbeat` to
//! the record's `address=` every `renew_interval`, through
//! [`crate::auth::ClusterSecret::send`] (the test network filter, the sim's
//! in-process network), with a TTL/5 limit. The body ([`Heartbeat`]): the
//! session, the term the node believes current, the in-memory `beat`, the
//! etag of the last renewal, an `owns` hint, health, `knows`, the registry
//! generation and the range-table version applied. The reply
//! ([`HeartbeatReply`]): term, `ack`, `view` (every session the coordinator
//! knows: alive or ended **per the bucket**, address, `owns`, health, suspect
//! mark), and the registry generation and table version the coordinator
//! sees.
//!
//! The coordinator gathers heartbeats into batches ([`gather`]): a batch opens
//! when the first heartbeat arrives and closes after [`BATCH_WINDOW_DIV`]-th of
//! an interval; only **after** it closes does it fence (two GETs), then it
//! answers the whole batch. `ack` = the fence passed **and** the heartbeat's
//! term equals the current term; a failed fence ⇒ `409` for the whole batch.
//! The reply carries a hint for when to send next (`next_in_ms`), putting
//! every node on the coordinator's single interval grid — so one batch per
//! interval, two fence GETs, whatever the number of nodes.
//!
//! A new node **announces itself** with its first heartbeat: the coordinator
//! adds that session to its tracker ([`cairn_store::SessionTracker::adopt`] —
//! equivalent to a LIST seeing it), so it is seen within one interval instead
//! of up to a TTL. The coordinator's tracker still LISTs `sessions/` every
//! TTL: a session that does not heartbeat (an old binary, a node cut off from
//! the coordinator) only reaches `view` through the LIST — one node LISTing
//! instead of every node. "Ended" in `view` only comes from the tracker — that
//! is, from the bucket; heartbeats only refresh hints (`owns`, health) and
//! suspect marks.
//!
//! # Watching owners through `view`
//!
//! A node receiving `view` (a heartbeat ack; at the coordinator: the `view` it
//! just built) where `view` records **another**, not-yet-ended session holding
//! a range we hold with a term no smaller than ours ⇒
//! [`Session::owner_changed`] for that range — exactly the check path of a
//! taker's notice: the watch loop GETs the owner record, and only a bucket
//! read showing the record is no longer ours revokes the grant (LS-3: `view`
//! is only a hint). A notice that got lost while we can still reach the
//! coordinator: the old owner sees it within ~`renew_interval` + one GET
//! instead of at the safety-net interval. Each (range, holder) pair kicks at
//! most once per TTL, so a stale `owns` in `view` costs only one GET (`304`)
//! per TTL.
//!
//! Every I/O of the loop is bounded (fence TTL/3, heartbeat TTL/5); waits go
//! through `tokio::time` (virtual time in `sim_cluster`), jitter through
//! `cairn_store::pace::jittered`.

use {
    crate::{
        auth::ClusterSecret,
        partitions::Partitions,
        registry_gen::{GenHint, RegistryGen},
    },
    cairn_store::{
        ClusterConfig, CoordinatorSeen, LeaseView, Observing, PartitionId, Session, SessionId,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{
        sync::{mpsc, oneshot, Notify},
        time::Instant,
    },
};

/// An idle coordinator (nobody heartbeating) fences every this many TTLs.
pub const IDLE_FENCE_TTLS: u32 = 6;

/// Batch window = heartbeat interval / this.
pub const BATCH_WINDOW_DIV: u32 = 10;

/// How long (in TTLs) a received heartbeat is kept to refresh hints in `view`.
const BEAT_FRESH_TTLS: u32 = 1;

/// A session silent (no heartbeat, beat on the bucket not increasing) for
/// more than this many TTLs is marked suspect in `view`.
pub const SUSPECT_AFTER_TTLS: u32 = 2;

/// Body of `POST /internal/heartbeat`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    /// The sending incarnation (`<node>/<inc>`).
    pub session: String,
    /// The term of `control/coordinator` the sender believes current.
    pub term: u64,
    /// The in-memory `beat` (the last renewal that won).
    pub beat: u64,
    /// Etag of the session record after the last renewal.
    pub etag: Option<String>,
    pub address: Option<String>,
    pub ingress: Option<String>,
    #[serde(default)]
    pub owns: BTreeMap<u32, u64>,
    pub slow: Option<u32>,
    pub p99_put_ms: Option<u64>,
    #[serde(default)]
    pub draining: bool,
    pub knows: u32,
    /// The registry generation the sender last saw (`control/registry-gen`).
    pub registry_gen: Option<GenHint>,
    /// The range-table version the sender is using.
    pub table_version: u64,
    /// Age (ms) of the sender's most recent successful bucket access: the
    /// coordinator only acks when it is < TTL/3. Absent (older binaries) ⇒ no
    /// ack.
    #[serde(default)]
    pub touched_ms: Option<u64>,
    /// Whom the sender's record currently delegates to (confirmed) —
    /// `<node>/<inc>:<term>`; absent ⇒ not delegating.
    #[serde(default)]
    pub delegated: Option<String>,
}

/// What a heartbeat reply says about the sender's lease (**permitting** and
/// **renewing** are separate).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lease {
    /// Permission to enter delegated mode (the sender is `direct`).
    Grant,
    /// Renews the grant up to `t_sent + ⅔·TTL`.
    Ack,
    /// The coordinator has issued the `expired` CAS, or read that the session ended.
    Expired,
    /// Back to `direct`.
    Fallback,
}

impl From<Lease> for cairn_store::LeaseAnswer {
    fn from(l: Lease) -> Self {
        match l {
            Lease::Grant => Self::Grant,
            Lease::Ack => Self::Ack,
            Lease::Expired => Self::Expired,
            Lease::Fallback => Self::Fallback,
        }
    }
}

/// One session in the coordinator's `view`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub session: String,
    /// Ended **per the bucket** (the coordinator's tracker read it, or its CAS
    /// to `expired`/`closed` succeeded). Only a hint at the receiver (LS-3).
    pub ended: bool,
    pub address: Option<String>,
    pub ingress: Option<String>,
    #[serde(default)]
    pub owns: BTreeMap<u32, u64>,
    pub slow: Option<u32>,
    pub p99_put_ms: Option<u64>,
    #[serde(default)]
    pub draining: bool,
    /// The coordinator saw it silent for more than [`SUSPECT_AFTER_TTLS`] TTLs
    /// without being able to expire it yet — the receiver observes it itself.
    #[serde(default)]
    pub suspect: bool,
}

/// Reply to `POST /internal/heartbeat`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeartbeatReply {
    /// The coordinator's current term.
    pub term: u64,
    /// The batch's fence passed and the heartbeat's term equals [`Self::term`].
    /// `false` ⇒ the sender re-reads `control/coordinator` and does not use
    /// `view`.
    pub ack: bool,
    pub view: Vec<Member>,
    /// The registry generation the coordinator last read; `None` = not read yet.
    pub registry_gen: Option<GenHint>,
    /// The coordinator's range-table version.
    pub table_version: u64,
    /// Send the next heartbeat after this many ms — the coordinator's interval grid.
    pub next_in_ms: u64,
    /// What the reply says about the sender's lease — only with `ack`. Absent
    /// (an older coordinator, or no opinion) ⇒ the sender changes nothing.
    #[serde(default)]
    pub lease: Option<Lease>,
}

/// The gate of one batch: on, and every session knows `lease/quiet`; off, or
/// an old binary present ⇒ the whole cluster goes back to `direct`; a session
/// not yet read ⇒ no new delegation (its `knows` is unknown) but nobody is
/// forced back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Gate {
    Open,
    Unsure,
    Closed,
}

/// A heartbeat waiting for its batch.
struct Pending {
    hb: Heartbeat,
    reply: oneshot::Sender<Option<HeartbeatReply>>,
}

/// The latest ack this node received (on a node that is not the coordinator).
#[derive(Clone, Debug)]
pub struct Acked {
    /// When the acked heartbeat was **sent** (our clock, before the first attempt).
    pub sent: Instant,
    pub reply: HeartbeatReply,
}

/// The control loop's connector for the rest of the node. `Clone` is cheap.
#[derive(Clone)]
pub struct Control(Arc<Inner>);

struct Inner {
    session: Session,
    table: ClusterConfig,
    ttl: Duration,
    secret: Option<ClusterSecret>,
    client: reqwest::Client,
    gen: Option<RegistryGen>,
    poke: Notify,
    queue: mpsc::UnboundedSender<Pending>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<Pending>>>,
    /// The latest heartbeat of each session, at receipt (at the coordinator).
    beats: Mutex<BTreeMap<SessionId, (Instant, Heartbeat)>>,
    /// The latest fence (at the coordinator).
    last_fence: Mutex<Option<Instant>>,
    /// Origin of the coordinator's heartbeat interval grid.
    origin: Instant,
    acked: Mutex<Option<Acked>>,
    /// (range, holding session per `view`) pairs that kicked an owner watch, with the kick time.
    owner_hints: Mutex<BTreeMap<(u32, String), Instant>>,
    /// Suspect marks in `view` ([`SUSPECT_AFTER_TTLS`]); only tests turn them off.
    suspects: std::sync::atomic::AtomicBool,
    /// The node's clock is trusted ([`Control::set_clock_trusted`]).
    clock_trusted: std::sync::atomic::AtomicBool,
}

impl Control {
    pub fn new(
        session: Session,
        table: ClusterConfig,
        ttl: Duration,
        secret: Option<ClusterSecret>,
        gen: Option<RegistryGen>,
    ) -> Self {
        let (queue, rx) = mpsc::unbounded_channel();
        Control(Arc::new(Inner {
            session,
            table,
            ttl,
            secret,
            client: reqwest::Client::new(),
            gen,
            poke: Notify::new(),
            queue,
            rx: Mutex::new(Some(rx)),
            beats: Mutex::new(BTreeMap::new()),
            last_fence: Mutex::new(None),
            origin: Instant::now(),
            acked: Mutex::new(None),
            owner_hints: Mutex::new(BTreeMap::new()),
            suspects: std::sync::atomic::AtomicBool::new(true),
            clock_trusted: std::sync::atomic::AtomicBool::new(true),
        }))
    }

    /// The node's monotonic clock rate is off against the store (`false`) or
    /// back within bounds (`true`) — see [`crate::clock_rate`]. Untrusted ⇒
    /// the loop releases the coordinator role and does not take it.
    pub(crate) fn set_clock_trusted(&self, on: bool) {
        self.0
            .clock_trusted
            .store(on, std::sync::atomic::Ordering::Release);
    }

    fn clock_trusted(&self) -> bool {
        self.0
            .clock_trusted
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Turns off suspect marks in the coordinator's `view`
    /// (`NodeConfig::view_suspects`) — only to test the sparse-tracker safety
    /// net.
    #[cfg(feature = "testkit")]
    pub fn set_suspects(&self, on: bool) {
        self.0
            .suspects
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// Heartbeat interval: `renew_interval` (TTL/5).
    fn pace(&self) -> Duration {
        (self.0.ttl / 5).max(Duration::from_millis(10))
    }

    /// There is news that the coordinator record may have just changed (a peer
    /// shutting down gracefully): re-read it at once on the next tick.
    pub fn poke(&self) {
        self.0.poke.notify_one();
    }

    /// The latest ack this node received — `None` at the coordinator or when
    /// there is none yet.
    pub fn acked(&self) -> Option<Acked> {
        self.0.acked.lock().expect("mutex").clone()
    }

    /// The node's control loop and batching loop — see the module head.
    pub fn spawn(&self, handles: Partitions) -> Vec<tokio::task::JoinHandle<()>> {
        let node = self.0.session.node_id().to_string();
        let main = {
            let (me, node) = (self.clone(), node.clone());
            tokio::spawn(cairn_store::io_source::tagged(
                "control.coordinator",
                async move { cairn_store::failpoint::in_node(&node, me.run(handles)).await },
            ))
        };
        let rx = self.0.rx.lock().expect("mutex").take();
        let mut out = vec![main];
        if let Some(rx) = rx {
            let me = self.clone();
            out.push(tokio::spawn(cairn_store::io_source::tagged(
                "control.heartbeat",
                async move { cairn_store::failpoint::in_node(&node, me.gather(rx)).await },
            )));
        }
        out
    }

    // ───────────────────────────────────────────── coordinator side

    /// One incoming heartbeat (the handler of `/internal/heartbeat`). `None` ⇒
    /// we are not the coordinator, or the batch's fence failed (`409`).
    pub async fn on_heartbeat(&self, hb: Heartbeat) -> Option<HeartbeatReply> {
        self.0.session.coordinating()?;
        let (reply, rx) = oneshot::channel();
        self.0.queue.send(Pending { hb, reply }).ok()?;
        // The batch closes after one window, the fence is bounded by TTL/3:
        // waiting longer than one interval + TTL/3 means the sender has given
        // up too.
        tokio::time::timeout(self.pace() + self.0.ttl / 3, rx)
            .await
            .ok()?
            .ok()?
    }

    /// The batching loop: the first heartbeat opens a batch, which closes
    /// after the window; fence **after** it closes; answer the whole batch.
    async fn gather(self, mut rx: mpsc::UnboundedReceiver<Pending>) {
        let window = self.pace() / BATCH_WINDOW_DIV;
        while let Some(first) = rx.recv().await {
            let close = Instant::now() + window;
            let mut batch = vec![first];
            loop {
                // `biased`: the order only matters for a deterministic sim.
                tokio::select! {
                    biased;
                    more = rx.recv() => match more {
                        Some(p) => batch.push(p),
                        None => break,
                    },
                    () = tokio::time::sleep_until(close) => break,
                }
            }
            self.answer(batch).await;
        }
    }

    async fn answer(&self, batch: Vec<Pending>) {
        let session = &self.0.session;
        let tracker = session.tracker();
        let received = Instant::now();
        let ids: Vec<Option<SessionId>> = batch
            .iter()
            .map(|p| SessionId::parse(&p.hb.session))
            .collect();
        for id in ids.iter().flatten() {
            // A new node announces itself: as if a LIST saw it.
            tracker.adopt(id);
        }
        // Ack rule 4: a heartbeat reports a record delegating to us at a
        // version the tracker does not have yet (a node that just delegated,
        // just wrote `owns`) ⇒ GET it, in parallel with the fence (both issued
        // after the batch is received).
        let claim = session
            .current()
            .zip(session.coordinating())
            .map(|(me, t)| format!("{me}:{t}"));
        let to_confirm: Vec<SessionId> = batch
            .iter()
            .zip(&ids)
            .filter_map(|(p, id)| {
                let id = id.as_ref()?;
                let ours = claim.is_some() && p.hb.delegated == claim;
                let held = matches!(tracker.lease_view(id),
                    LeaseView::DelegatedToMe { beat, etag } if beat == p.hb.beat && etag == p.hb.etag);
                (ours && !held).then(|| id.clone())
            })
            .collect();
        let (fenced, ()) = tokio::join!(session.coordinator_fence(), async {
            futures::future::join_all(to_confirm.iter().map(|id| tracker.confirm(id))).await;
        });
        *self.0.last_fence.lock().expect("mutex") = Some(Instant::now());
        let term = session.coordinating().filter(|_| fenced);
        let Some(term) = term else {
            for p in batch {
                let _ = p.reply.send(None);
            }
            return;
        };
        let now = Instant::now();
        {
            let mut beats = self.0.beats.lock().expect("mutex");
            for (p, id) in batch.iter().zip(&ids) {
                if let Some(id) = id {
                    beats.insert(id.clone(), (now, p.hb.clone()));
                }
            }
            let ttl = self.0.ttl;
            beats.retain(|_, (at, _)| at.elapsed() < ttl * IDLE_FENCE_TTLS);
        }
        let gate = self.gate(&batch).await;
        let view = self.view();
        self.check_owners(&view);
        let registry_gen = self.0.gen.as_ref().and_then(|g| g.latest());
        let table_version = self.0.table.table().version();
        let next_in_ms = self.next_in().as_millis() as u64;
        let leases: Vec<Option<Lease>> = batch
            .iter()
            .zip(&ids)
            .map(|(p, id)| {
                let id = id.as_ref()?;
                (p.hb.term == term).then(|| self.lease_for(&p.hb, id, gate))
            })
            .collect();
        // Pause point between the fence and the reply (zombie test: the
        // coordinator loses the role in this gap).
        cairn_store::failpoint!("coordinator::before_ack");
        for ((p, id), lease) in batch.into_iter().zip(ids).zip(leases) {
            let mut lease = lease;
            if let (Some(Lease::Ack), Some(id)) = (lease, id.as_ref()) {
                if tracker.expire_issued(id) {
                    // Tripwire: ack rule 5 — must never be reached.
                    cairn_store::failpoint!("coordinator::ack_after_expire");
                    tracing::error!(session = %id, "nearly acked a session we already issued the expired CAS for");
                    lease = Some(Lease::Expired);
                } else {
                    // τ(N) of LS-15: when the acked heartbeat was **received**
                    // — every grant from our acks comes from a heartbeat
                    // received before τ (M1 `HeartbeatAck`). A heartbeat that
                    // is not acked renews nothing.
                    tracker.heard(id, received, p.hb.beat);
                }
            }
            let reply = HeartbeatReply {
                term,
                ack: p.hb.term == term,
                view: view.clone(),
                registry_gen: registry_gen.clone(),
                table_version,
                next_in_ms,
                lease,
            };
            let _ = p.reply.send(Some(reply));
        }
    }

    /// The gate's runtime rule for one batch: `control/quiet-lease` on **with
    /// the delegation flag** (`delegation=on`) **and** every session (the
    /// tracker, plus the batch's heartbeats) has `knows ≥ K`.
    async fn gate(&self, batch: &[Pending]) -> Gate {
        let session = &self.0.session;
        // Gate on but no delegation flag: solo mode only — nobody delegates.
        if !session.quiet_lease_on().await || !session.delegation_on() {
            return Gate::Closed;
        }
        let k = cairn_store::migrate::QUIET_LEASE;
        if batch.iter().any(|p| p.hb.knows < k) {
            return Gate::Closed;
        }
        match session.tracker().all_know(k) {
            Some(true) => Gate::Open,
            Some(false) => Gate::Closed,
            None => Gate::Unsure,
        }
    }

    /// What to say about the lease of session `id`'s heartbeat `hb` (fence
    /// passed, same term) — ack rules 4–6 (rules 1–3 are the fence).
    fn lease_for(&self, hb: &Heartbeat, id: &SessionId, gate: Gate) -> Lease {
        let touched = hb
            .touched_ms
            .is_some_and(|ms| Duration::from_millis(ms) < self.0.ttl / 3);
        match self.0.session.tracker().lease_view(id) {
            // Ack rule 5: ended on the bucket, or we issued the `expired` CAS.
            LeaseView::Ended => Lease::Expired,
            _ if gate == Gate::Closed => Lease::Fallback,
            LeaseView::DelegatedToMe { beat, etag } => {
                // Ack rules 4 (exactly the version the sender reports) and 6.
                if beat == hb.beat && etag == hb.etag && touched {
                    Lease::Ack
                } else {
                    Lease::Grant
                }
            }
            // `direct`, or delegating to another role (a coordinator change:
            // delegate to us again). A sender that cannot reach the bucket
            // gets no permission.
            LeaseView::Direct | LeaseView::DelegatedElsewhere => {
                if gate == Gate::Open && touched {
                    Lease::Grant
                } else {
                    Lease::Fallback
                }
            }
            // `solo`, unreadable, unknown mode.
            LeaseView::Other => Lease::Fallback,
        }
    }

    /// How long until the receiver should send next: the next grid point (one
    /// interval apart, origin `origin`) minus half a window, no earlier than
    /// half an interval.
    fn next_in(&self) -> Duration {
        let pace = self.pace();
        let half_window = pace / BATCH_WINDOW_DIV / 2;
        let since = self.0.origin.elapsed();
        let pace_ns = pace.as_nanos().max(1);
        let mut k = since.as_nanos() / pace_ns + 1;
        loop {
            let target = Duration::from_nanos((k * pace_ns) as u64).saturating_sub(half_window);
            let wait = target.saturating_sub(since);
            if wait >= pace / 2 {
                return wait;
            }
            k += 1;
        }
    }

    /// The coordinator's `view`: itself, plus every session its tracker knows,
    /// hints refreshed from the latest heartbeat.
    fn view(&self) -> Vec<Member> {
        let session = &self.0.session;
        let ttl = self.0.ttl;
        let suspects = self.0.suspects.load(std::sync::atomic::Ordering::Relaxed);
        let mut out = Vec::new();
        if let Some(me) = session.self_report() {
            out.push(Member {
                session: me.id.to_string(),
                ended: false,
                address: me.address,
                ingress: me.ingress,
                owns: me.owns,
                slow: me.health.slow,
                p99_put_ms: me.health.p99_put_ms,
                draining: me.health.draining,
                suspect: false,
            });
        }
        let beats = self.0.beats.lock().expect("mutex");
        for o in session.tracker().snapshot() {
            let hb = beats
                .get(&o.id)
                .filter(|(at, _)| at.elapsed() < ttl * BEAT_FRESH_TTLS);
            let quiet = beats
                .get(&o.id)
                .map_or(o.quiet, |(at, _)| o.quiet.min(at.elapsed()));
            let ended = o.state.is_terminal();
            let mut m = Member {
                session: o.id.to_string(),
                ended,
                address: o.address,
                ingress: o.ingress,
                owns: o.owns,
                slow: o.health.slow,
                p99_put_ms: o.health.p99_put_ms,
                draining: o.health.draining,
                suspect: suspects && !ended && quiet > ttl * SUSPECT_AFTER_TTLS,
            };
            if let (Some((_, hb)), false) = (hb, ended) {
                m.owns = hb.owns.clone();
                m.slow = hb.slow;
                m.p99_put_ms = hb.p99_put_ms;
                m.draining = hb.draining;
                m.address = m.address.or_else(|| hb.address.clone());
                m.ingress = m.ingress.or_else(|| hb.ingress.clone());
            }
            out.push(m);
        }
        out
    }

    /// `view` records another session holding a range we hold ⇒ kick the owner
    /// watch loop for that range (see the module head). No I/O here: the watch
    /// loop does the GET.
    fn check_owners(&self, view: &[Member]) {
        let session = &self.0.session;
        let Some(me) = session.current() else { return };
        let mine = session.owned();
        let mut hints = self.0.owner_hints.lock().expect("mutex");
        if mine.is_empty() {
            hints.clear();
            return;
        }
        let me = me.to_string();
        let mut live = std::collections::BTreeSet::new();
        for m in view.iter().filter(|m| !m.ended && m.session != me) {
            for (p, term) in &m.owns {
                if mine.get(p).is_none_or(|ours| term < ours) {
                    continue;
                }
                let key = (*p, m.session.clone());
                if hints.get(&key).is_none_or(|at| at.elapsed() >= self.0.ttl) {
                    tracing::debug!(range = p, holder = %m.session, "view shows our range in another session — checking the owner record");
                    hints.insert(key.clone(), Instant::now());
                    session.owner_changed(PartitionId(*p));
                }
                live.insert(key);
            }
        }
        hints.retain(|k, _| live.contains(k));
    }

    // ─────────────────────────────────────────────── other-node side

    /// One heartbeat to the coordinator at `to`, term `term`. `None` = no
    /// answer, `409`, or failure. Also returns the beat the heartbeat reported
    /// (for the ack).
    async fn beat(&self, to: &str, term: u64) -> Option<(HeartbeatReply, u64)> {
        let session = &self.0.session;
        let me = session.self_report()?;
        let secret = self.0.secret.as_ref()?;
        let beat = me.beat;
        let hb = Heartbeat {
            touched_ms: session.touched_age().map(|a| a.as_millis() as u64),
            delegated: session
                .delegated_to()
                .map(|(coord, term)| format!("{coord}:{term}")),
            session: me.id.to_string(),
            term,
            beat: me.beat,
            etag: me.etag,
            address: me.address,
            ingress: me.ingress,
            owns: me.owns,
            slow: me.health.slow,
            p99_put_ms: me.health.p99_put_ms,
            draining: me.health.draining,
            knows: me.knows,
            registry_gen: self.0.gen.as_ref().and_then(|g| g.latest()),
            table_version: self.0.table.table().version(),
        };
        let req = self
            .0
            .client
            .post(format!("{}/internal/heartbeat", to.trim_end_matches('/')))
            .json(&hb);
        let sent = tokio::time::timeout(self.pace(), async {
            let r = secret.send(&self.0.client, req).await.ok()?;
            if !r.status().is_success() {
                return None;
            }
            r.json::<HeartbeatReply>().await.ok()
        })
        .await;
        sent.ok().flatten().map(|r| (r, beat))
    }

    /// An ack just came back (heartbeat sent at `sent`): `Membership` listens
    /// to its `view`, the tracker goes sparse (suspected sessions every
    /// interval), the registry re-read loop compares with the generation in
    /// `view`.
    fn listen(&self, reply: &HeartbeatReply, sent: Instant) {
        let session = &self.0.session;
        self.check_owners(&reply.view);
        let members: Vec<cairn_store::ViewMember> = reply
            .view
            .iter()
            .filter_map(|m| {
                Some(cairn_store::ViewMember {
                    id: SessionId::parse(&m.session)?,
                    ended: m.ended,
                    address: m.address.clone(),
                    ingress: m.ingress.clone(),
                    owns: m.owns.clone(),
                    health: cairn_store::health::Published {
                        slow: m.slow,
                        p99_put_ms: m.p99_put_ms,
                        draining: m.draining,
                    },
                    suspect: m.suspect,
                })
            })
            .collect();
        let suspects = members
            .iter()
            .filter(|m| m.suspect && !m.ended)
            .map(|m| m.id.clone())
            .collect();
        session.membership().hear(members, sent);
        session
            .tracker()
            .set_observing(Observing::Sparse { suspects });
        if let Some(g) = &self.0.gen {
            g.hear(reply.registry_gen.clone(), sent + self.0.ttl / 3);
        }
    }

    /// Stop listening to `view` (stale, no coordinator, or we are the
    /// coordinator): everything goes back to reading the bucket itself — the
    /// protocol from before the coordinator existed.
    fn stop_listening(&self) {
        let session = &self.0.session;
        session.membership().forget_view();
        if session.coordinating().is_none() {
            session.tracker().set_observing(Observing::Full);
        }
        if let Some(g) = &self.0.gen {
            g.hear(None, Instant::now());
        }
    }

    /// A range-table version hint for `ClusterConfig::follow_with`: the version
    /// in a fresh `view`, `None` when not listening.
    pub fn table_hint(&self) -> Arc<dyn Fn() -> Option<u64> + Send + Sync> {
        let me = self.clone();
        Arc::new(move || {
            if !me.0.session.membership().listening() {
                return None;
            }
            me.acked().map(|a| a.reply.table_version)
        })
    }

    /// A fresh ack: an acked heartbeat sent within the last TTL/3.
    fn fresh_ack(&self) -> bool {
        let fresh = self.0.ttl / 3;
        self.0
            .acked
            .lock()
            .expect("mutex")
            .as_ref()
            .is_some_and(|a| a.sent.elapsed() < fresh)
    }

    // ──────────────────────────────────────────────────────── the loop

    async fn run(self, handles: Partitions) {
        let session = self.0.session.clone();
        let ttl = self.0.ttl;
        let pace = self.pace();
        let retake = ttl * cairn_store::RETAKE_AFTER_TTLS;
        let mut last_read: Option<Instant> = None;
        // When we first saw an empty record, and the (jittered) wait of a node
        // that does not run GC.
        let mut vacancy: Option<(Instant, Duration)> = None;
        let mut next = pace;
        let mut last_sent: Option<Instant> = None;
        let members = session.membership();
        loop {
            // `biased`: the order only matters for a deterministic sim.
            let (poked, early) = tokio::select! {
                biased;
                _ = self.0.poke.notified() => (true, false),
                _ = members.refresh_requested() => (false, true),
                _ = tokio::time::sleep(next) => (false, false),
            };
            next = pace;
            let Some(me) = session.current() else {
                self.stop_listening();
                continue;
            };
            session.drop_stale_coordinator().await;
            if session.coordinating().is_some() {
                vacancy = None;
                self.stop_listening();
                session.tracker().set_observing(Observing::Coordinator);
                // An odd registry generation standing past the threshold ⇒ remove the token.
                if let Some(g) = &self.0.gen {
                    g.seal_stuck().await;
                }
                // Expire rule 3 (Lifeguard): when weak, delay expiring, up to
                // one TTL; weak beyond that ⇒ release the role instead of
                // delaying forever. A clock running at the wrong rate: the
                // coordinator's expire rule counts TTLs on its own clock —
                // release the role.
                if !self.clock_trusted() {
                    tracing::warn!("node clock rate is skewed — releasing the coordinator role");
                    session.release_coordinator().await;
                    continue;
                }
                session.tracker().set_weak(session.weak());
                if session.tracker().weak_for().is_some_and(|w| w >= ttl) {
                    tracing::warn!("coordinator weak for more than one TTL — releasing the role");
                    session.release_coordinator().await;
                    session.tracker().set_weak(false);
                    continue;
                }
                let idle = self
                    .0
                    .last_fence
                    .lock()
                    .expect("mutex")
                    .is_none_or(|t| t.elapsed() >= ttl * IDLE_FENCE_TTLS);
                if idle {
                    *self.0.last_fence.lock().expect("mutex") = Some(Instant::now());
                    if !session.coordinator_fence().await {
                        tracing::debug!("coordinator fence failed on an idle check");
                    }
                }
                continue;
            }
            let fresh = self.fresh_ack();
            if !fresh && (poked || last_read.is_none_or(|t| t.elapsed() >= ttl)) {
                last_read = Some(Instant::now());
                if let Err(e) = session.read_coordinator().await {
                    tracing::debug!(error = %e, "cannot read control/coordinator");
                }
            }
            // Heartbeat to the coordinator the record points at. A refresh
            // request (`Membership::request_refresh`) sends early, but no more
            // often than half an interval.
            let too_soon = early && last_sent.is_some_and(|t| t.elapsed() < pace / 2);
            if let (Some(CoordinatorSeen::Held(rec)), false) =
                (session.coordinator_seen(), too_soon)
            {
                if !rec.released && rec.session != me {
                    if let Some(addr) = rec.address.as_deref() {
                        let sent = Instant::now();
                        last_sent = Some(sent);
                        match self.beat(addr, rec.term).await {
                            Some((reply, beat)) if reply.ack => {
                                let hint = Duration::from_millis(reply.next_in_ms);
                                next = hint.clamp(pace / 4, pace * 3 / 2);
                                self.listen(&reply, sent);
                                // What the reply says about the lease — an ack
                                // renews up to `sent + ⅔·TTL` (send time,
                                // before the first attempt).
                                session.heard_coordinator(&rec.session, reply.term);
                                if let Some(lease) = reply.lease {
                                    session.on_lease_answer(
                                        &rec.session,
                                        reply.term,
                                        lease.into(),
                                        sent,
                                        beat,
                                    );
                                }
                                *self.0.acked.lock().expect("mutex") = Some(Acked { sent, reply });
                            }
                            // Another term: our copy of the record is stale — re-read it now.
                            Some(_) => last_read = None,
                            None => {}
                        }
                    }
                }
            }
            // Listening to a fresh `view`: the tracker goes sparse (plus
            // suspected sessions), and wakes exactly when `view` goes stale so
            // it returns to the tracker at once.
            if let Some(until) = members.view_fresh_until().filter(|_| members.listening()) {
                vacancy = None;
                next = next.min(
                    until.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                );
                continue;
            }
            self.stop_listening();
            match session.coordinator_vacant().await {
                Ok(true) => {
                    let runner = crate::gc::is_runner(&self.0.table, &handles).await;
                    let (since, wait) = *vacancy.get_or_insert_with(|| {
                        (Instant::now(), ttl + cairn_store::pace::jittered(ttl / 2))
                    });
                    let due = runner || since.elapsed() >= wait;
                    // A node delegating to the old role does not take the role:
                    // it falls back to `direct` first (losing its ack after
                    // TTL/3), and only then takes it.
                    let delegated = session.delegated_to().is_some();
                    if due
                        && !delegated
                        && self.clock_trusted()
                        && !session.released_coordinator_within(retake)
                    {
                        match session.try_coordinate().await {
                            Ok(Some(term)) => {
                                tracing::debug!(
                                    term,
                                    gc_runner = runner,
                                    "took the coordinator role"
                                );
                                vacancy = None;
                                *self.0.last_fence.lock().expect("mutex") = Some(Instant::now());
                            }
                            Ok(None) => last_read = None,
                            Err(e) => {
                                tracing::debug!(error = %e, "could not take the coordinator role")
                            }
                        }
                    }
                }
                Ok(false) => vacancy = None,
                Err(e) => tracing::debug!(error = %e, "could not examine the coordinator record"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Heartbeat bodies and replies survive JSON intact (`owns` keys are numbers).
    #[test]
    fn wire_round_trips() {
        let m = Member {
            session: "a/1".into(),
            ended: false,
            address: Some("http://a".into()),
            ingress: None,
            owns: BTreeMap::from([(3, 7)]),
            slow: Some(1),
            p99_put_ms: None,
            draining: true,
            suspect: true,
        };
        let r = HeartbeatReply {
            term: 2,
            ack: true,
            view: vec![m.clone()],
            registry_gen: Some(GenHint::Present("x".into())),
            table_version: 4,
            next_in_ms: 150,
            lease: Some(Lease::Ack),
        };
        let back: HeartbeatReply =
            serde_json::from_slice(&serde_json::to_vec(&r).unwrap()).unwrap();
        assert_eq!(back.view, vec![m]);
        assert_eq!(back.registry_gen, r.registry_gen);
        assert_eq!(back.lease, Some(Lease::Ack));
        // A reply from an older coordinator: nothing about the lease.
        let old: HeartbeatReply = serde_json::from_str(
            r#"{"term":1,"ack":true,"view":[],"registry_gen":null,"table_version":0,"next_in_ms":1}"#,
        )
        .unwrap();
        assert_eq!(old.lease, None);
        assert_eq!(
            serde_json::to_string(&Lease::Fallback).unwrap(),
            r#""fallback""#
        );
        // Unknown fields from a newer binary are ignored; missing fields take defaults.
        let lax: Member =
            serde_json::from_str(r#"{"session":"b/2","ended":true,"address":null,"ingress":null,"slow":null,"p99_put_ms":null,"moi":1}"#)
                .unwrap();
        assert!(lax.ended && lax.owns.is_empty() && !lax.suspect);
    }
}
