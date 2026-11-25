// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`SessionTracker`] — peer discovery, holders, load, peer health.

use super::*;

// ───────────────────────────────────────────────────────────── tracker

/// A session of another node, as we observe it.
pub(super) struct Seen {
    node: String,
    /// The largest beat ever seen, and **when we received** the first response carrying it.
    best_beat: u64,
    since: Instant,
    /// When we **first** saw this session — the incarnation's age as we see it
    /// (see [`SessionTracker::settled_nodes`]).
    first_seen: Instant,
    state: SessionState,
    address: Option<String>,
    ingress: Option<String>,
    owns: BTreeMap<u32, u64>,
    /// What the session publishes about the node's health.
    health: crate::health::Published,
    /// Number of `expired` CASes (or challenge CASes) lost while the beat did not increase.
    expire_failures: u32,
    terminal_since: Option<Instant>,
    /// When we **received** the latest GET answer for this session — the schedule of the
    /// sparse mode ([`Observing::Sparse`]). `None` = never GET yet.
    observed_at: Option<Instant>,
    /// Mode and `challenged` line of the record at the latest successful read. A `solo`
    /// record whose `challenged` line changed while the beat did not increase is a **new
    /// version** of the record: `since` is reset to when we received it — the τ of LS-16.
    mode: SessionMode,
    challenged: Option<String>,
    /// The latest read could not be decoded, after a readable `solo` record.
    unreadable: bool,
    /// Etag of the latest successful read — with `best_beat`, the "version" the
    /// coordinator compares with the heartbeat (ack rule 4).
    etag: Option<String>,
    /// The full version of that read — the `expired` CAS on the version held when it
    /// cannot be read again ([`SessionTracker::expire_held`]).
    version: Option<UpdateVersion>,
    /// `knows=` of the latest read (`None` = a binary from before the migration
    /// framework, or not read yet) — the gate's run-time rule: the coordinator grants
    /// delegation only when every session knows the `lease/quiet` step.
    knows: Option<u32>,
    /// When the coordinator **received** this session's latest heartbeat — the τ(N) of
    /// LS-15 is `max(since, heard)` —, and the beat that heartbeat reported.
    heard: Option<Instant>,
    heard_beat: Option<u64>,
    /// We have issued an `expired` CAS for this session (even lost or ambiguous: a late
    /// conditional PUT may still apply — rule 7). Ack rule 5: the coordinator never acks
    /// it again.
    expire_issued: bool,
}

impl Seen {
    /// A session whose name we just learned but have not read — treated as live until we
    /// know otherwise. "Unknown" is never taken to mean "dead".
    fn unread(id: &SessionId) -> Seen {
        Seen {
            node: id.node.clone(),
            best_beat: 0,
            since: Instant::now(),
            first_seen: Instant::now(),
            state: SessionState::Live,
            address: None,
            ingress: None,
            owns: BTreeMap::new(),
            health: Default::default(),
            expire_failures: 0,
            terminal_since: None,
            observed_at: None,
            mode: SessionMode::Direct,
            challenged: None,
            unreadable: false,
            etag: None,
            version: None,
            knows: None,
            heard: None,
            heard_beat: None,
            expire_issued: false,
        }
    }

    /// No terminal state seen on the bucket yet. "Dead" only comes from the bucket
    /// (`expired`/`closed` read, or our winning `expired` CAS) — never from our own clock
    /// alone.
    fn alive(&self) -> bool {
        self.state == SessionState::Live
    }
}

/// Body of the challenge CAS: the owner's record **verbatim** (lines this binary does not
/// know are kept too) plus a `challenged=<us>` line.
fn challenge_body(raw: &[u8], me: &SessionId) -> Vec<u8> {
    let mut body = raw.to_vec();
    if !body.ends_with(b"\n") {
        body.push(b'\n');
    }
    body.extend_from_slice(format!("challenged={me}\n").as_bytes());
    body
}

/// Counters of **overwrites** of session records that could not be marked — metric
/// `cairnflow_session_rearm_total{outcome}`.
pub mod rearm_stats {
    use std::sync::atomic::AtomicU64;

    /// The overwrite reached the store: observers wait one TTL, then CAS `expired`.
    pub static WRITTEN: AtomicU64 = AtomicU64::new(0);
    /// The overwrite failed (or was ambiguous): the session is **not** considered dead.
    pub static FAILED: AtomicU64 = AtomicU64::new(0);
}

/// Who holds a partition, according to the `owns` hints of live sessions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Holder {
    pub session: SessionId,
    pub address: Option<String>,
    pub ingress: Option<String>,
    pub term: u64,
}

/// One session in the tracker's table, as it sees it — for the `view` the coordinator
/// returns over the heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observed {
    pub id: SessionId,
    pub state: SessionState,
    pub address: Option<String>,
    pub ingress: Option<String>,
    pub owns: BTreeMap<u32, u64>,
    pub health: crate::health::Published,
    /// How long the tracker has not seen the beat increase (since **receiving** the first
    /// answer carrying the largest beat; a session that just entered the table: since it
    /// entered).
    pub quiet: Duration,
}

/// Which sessions the tracker GETs, and how often it LISTs `sessions/`. The mode only
/// changes the observation **cadence**; the rule for marking `expired` (LS-1: one silent
/// TTL counted from receiving the largest beat) stays the same in every mode — a sparse
/// cadence only makes `expired` later.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Observing {
    /// Every unended session every tick, LIST every TTL — the original behaviour, and the
    /// fallback.
    #[default]
    Full,
    /// Coordinator: every session every tick, LIST every TTL — like [`Self::Full`]; new
    /// nodes also announce themselves over the heartbeat ([`SessionTracker::adopt`]), so
    /// it sees them sooner than a LIST would. It still LISTs every TTL because sessions
    /// that **do not heartbeat** (old binaries in a mixed cluster, a node cut off from the
    /// coordinator, a session without a node) reach the table only through it: with only
    /// the 10-minute net nobody would observe them — if they died their ranges would be
    /// stuck for up to 10 minutes. One node LISTs instead of N nodes.
    Coordinator,
    /// A node following the coordinator's fresh `view`: each session every
    /// [`SPARSE_AFTER_TTLS`] TTLs (a net — the coordinator is not a single point of
    /// failure for detection), sessions in `suspects` (marked suspect by `view`) every
    /// tick; LIST on the [`LIST_NET`] net. Asking for a refresh
    /// ([`SessionTracker::request_refresh`]) forces a full round.
    Sparse { suspects: BTreeSet<SessionId> },
}

/// Cadence of the sparse tracker ([`Observing::Sparse`]), in TTLs. This is the only net
/// that catches a dead node when the coordinator does not mark it suspect (it can neither
/// read it nor know it went quiet): the first GET after the owner dies usually only sees a
/// newer beat, the second one CASes `expired`, so worst case
/// ~2·`SPARSE_AFTER_TTLS`·TTL + a tick. 2 ⇒ ~4·TTL (it used to be 6 ⇒ ~12·TTL). Cost:
/// Class B GETs, each peer every 2·TTL per node following `view` — still ~6 times fewer
/// than the full tracker (TTL/3).
pub const SPARSE_AFTER_TTLS: u32 = 2;

/// The `sessions/` LIST net of a node following `view` ([`Observing::Sparse`]): the
/// coordinator LISTs every TTL and `view` carries every session it sees.
pub const LIST_NET: Duration = Duration::from_secs(10 * 60);

/// What the tracker knows about `control/coordinator` (LS-15): enough to apply the
/// **yield rule** to a `delegated coord=X:t` record — an observer that is not that term's
/// coordinator expires it only one TTL after
/// `max(when it saw role (X, t) lost, when it saw this version of the record)`. Every mark
/// is on our clock, at the time we **saw** it.
#[derive(Default)]
pub(super) struct CoordKnowledge {
    /// When we first saw each term, and the session holding it.
    terms: BTreeMap<u64, (Instant, SessionId)>,
    /// When we first saw that term `released` (or gave up that term's role ourselves).
    released: BTreeMap<u64, Instant>,
    /// When we first saw the session of a role holder ended on the bucket.
    ended: BTreeMap<SessionId, Instant>,
    /// The role our current incarnation holds: term, since when.
    mine: Option<(u64, Instant)>,
    /// Since when we (the coordinator) have been "weak" (expire rule 3, as in Lifeguard):
    /// our grant is paused or our slowness score is at the cap.
    weak_since: Option<Instant>,
    /// The latest coordinator fence that **passed** (when it was issued).
    fenced_at: Option<Instant>,
}

/// Under which rule a record is due for expiry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Due {
    No,
    /// The coordinator's rule: τ(N) = `max(since, heard)` is one TTL old.
    ByCoordinator,
    /// The yield rule: one TTL since seeing the role lost **and** since this version.
    ByYield,
}

/// The cluster has had a coordinator (as far as we know): a record may be delegating.
fn delegation_possible(k: &CoordKnowledge) -> bool {
    !k.terms.is_empty() || k.mine.is_some()
}

/// We (the coordinator) have been weak for less than the one-TTL deferral cap.
fn deferring(k: &CoordKnowledge, ttl: Duration) -> bool {
    k.weak_since.is_some_and(|w| w.elapsed() < ttl)
}

/// Is the `delegated coord=<coord>:<term>` record (session `s`) due yet — LS-15.
fn delegated_due(
    k: &CoordKnowledge,
    me: Option<&SessionId>,
    s: &Seen,
    coord: &SessionId,
    term: u64,
    holder_ended: Option<Instant>,
    ttl: Duration,
) -> Due {
    if me == Some(coord) && k.mine.is_some_and(|(t, _)| t == term) {
        // Every ack we returned answers a heartbeat received before τ; every write of the
        // owner was readable before τ — `t_sent + ⅔·TTL < τ + TTL`.
        let tau = s.heard.map_or(s.since, |h| h.max(s.since));
        return if tau.elapsed() >= ttl && !deferring(k, ttl) {
            Due::ByCoordinator
        } else {
            Due::No
        };
    }
    let later = k.terms.range(term + 1..).map(|(_, (at, _))| *at).min();
    let lost = [
        later,
        k.released.get(&term).copied(),
        holder_ended,
        k.ended.get(coord).copied(),
    ]
    .into_iter()
    .flatten()
    .min();
    // The yield rule: the mark is `max(when the role was seen lost, when this version was
    // seen)` — `since` resets whenever the beat increases (a delegating CAS is a renewal).
    match lost {
        Some(at) if at.elapsed() >= ttl && s.since.elapsed() >= ttl => Due::ByYield,
        _ => Due::No,
    }
}

/// Is an unreadable record in a cluster with a coordinator due yet: we are the
/// coordinator, a fence passed **after** we saw it corrupt, and τ (≥ when we took the
/// role) is one TTL old.
fn unknown_due(k: &CoordKnowledge, s: &Seen, ttl: Duration) -> bool {
    let (Some((_, held_since)), Some(fenced)) = (k.mine, k.fenced_at) else {
        return false;
    };
    if fenced <= s.since {
        return false;
    }
    let tau = [Some(s.since), s.heard, Some(held_since)]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(s.since);
    tau.elapsed() >= ttl && !deferring(k, ttl)
}

/// The coordinator role each node of the process holds — for the gauge
/// `cairnflow_coordinator_term` (chaos `--kill-coordinator-every-s` kills exactly the node
/// holding the role). Only a measurement, nobody decides anything by it.
pub mod roles {
    use std::{collections::BTreeMap, sync::Mutex};

    static ROLES: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());

    pub(crate) fn set(node: &str, term: Option<u64>) {
        let mut r = ROLES.lock().expect("mutex");
        match term {
            Some(t) => r.insert(node.to_string(), t),
            None => r.remove(node),
        };
    }

    /// Term of the coordinator role node `node` holds (current incarnation).
    pub fn term_of(node: &str) -> Option<u64> {
        ROLES.lock().expect("mutex").get(node).copied()
    }
}

/// Number of recent terms [`CoordKnowledge`] keeps — older terms were lost long ago.
const KEEP_TERMS: usize = 16;

/// A session whose heartbeat the coordinator answers, as the tracker sees it
/// ([`SessionTracker::lease_view`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseView {
    /// The record has ended on the bucket, or we have issued an `expired` CAS for it.
    Ended,
    /// The record delegates to exactly the role we hold, at this (beat, etag) version.
    DelegatedToMe { beat: u64, etag: Option<String> },
    /// A `direct` record (or not read yet) — delegation may be granted.
    Direct,
    /// The record delegates to another role (old coordinator, old term).
    DelegatedElsewhere,
    /// Another mode (`solo`, delegated to another role, unreadable, unknown).
    Other,
}

pub(super) struct TrackerInner {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    ttl: Duration,
    interval: Duration,
    /// Our own current incarnation — never marked by us.
    me: Mutex<Option<SessionId>>,
    // BTreeMap: a fixed order for observing/cleaning sessions (deterministic simulation)
    // — a HashMap iterates by hash keys that are random per process.
    table: Mutex<BTreeMap<SessionId, Seen>>,
    last_list: Mutex<Option<Instant>>,
    changed: watch::Sender<u64>,
    /// Counts **other** sessions that just ended according to the bucket — see
    /// [`SessionTracker::ended`].
    ended: watch::Sender<u64>,
    poke: Notify,
    /// When this tracker was born — the mark for "present from the start".
    started: Instant,
    /// Observation mode — see [`Observing`].
    observing: Mutex<Observing>,
    /// The next round GETs every session whatever the mode (someone asked for a refresh).
    forced: std::sync::atomic::AtomicBool,
    /// Our session is in `mode=solo`: the `sessions/` LIST is only a net every
    /// `quiet_list_every` — a newcomer always challenges, and the challenge changes the
    /// etag, so our self-check GET sees it at once.
    quiet: std::sync::atomic::AtomicBool,
    quiet_list_every: Duration,
    /// The process's node — the node condition of failpoints in the observation loop.
    node: String,
    /// What we know about `control/coordinator` — see [`CoordKnowledge`].
    coord: Mutex<CoordKnowledge>,
    /// The coordinator our record currently delegates to (confirmed —
    /// `Current::delegated`), reported by the renewal loop ([`SessionTracker::lean_on`]).
    /// The `solo` record of **that very** session is not challenged by us ("deferred
    /// challenge"): we are living on its acks. Falling back to `direct` ends the deferral.
    leaning_on: Mutex<Option<SessionId>>,
}

/// Observes the sessions of other nodes: alive, dead, what they hold.
///
/// Cost per round (`interval` = [`LeaseConfig::observe_interval`], TTL/3 on a cluster
/// node): one GET for each other **unended** session; one LIST per TTL to discover new
/// nodes. Nothing per partition.
#[derive(Clone)]
pub struct SessionTracker(Arc<TrackerInner>);

impl SessionTracker {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &Path,
        ttl: Duration,
        interval: Duration,
        quiet_list_every: Duration,
        node: &str,
    ) -> Self {
        Self(Arc::new(TrackerInner {
            store,
            prefix: prefix.clone(),
            ttl,
            interval,
            me: Mutex::new(None),
            table: Mutex::new(BTreeMap::new()),
            last_list: Mutex::new(None),
            changed: watch::Sender::new(0),
            ended: watch::Sender::new(0),
            poke: Notify::new(),
            started: Instant::now(),
            observing: Mutex::new(Observing::Full),
            forced: std::sync::atomic::AtomicBool::new(false),
            quiet: std::sync::atomic::AtomicBool::new(false),
            quiet_list_every,
            node: node.to_string(),
            coord: Mutex::new(CoordKnowledge::default()),
            leaning_on: Mutex::new(None),
        }))
    }

    /// The lease TTL this tracker uses to judge alive or dead.
    pub fn ttl(&self) -> Duration {
        self.0.ttl
    }

    /// Live nodes that **have settled**: present since we started (seen in the first
    /// `settle`), or whose incarnation has been alive continuously for at least `settle`.
    /// Always includes ourselves.
    ///
    /// For load balancing: a node that just restarted midway is not yet counted in the
    /// denominator of the fair share, so nobody hands ranges back to it right away. A node
    /// that dies and restarts continuously (crash loop, chaos) does not drag ranges back
    /// and forth; when the whole cluster starts together it still splits evenly at once,
    /// because everyone sees everyone within the first `settle`.
    pub fn settled_nodes(&self, settle: Duration) -> BTreeSet<String> {
        let started = self.0.started;
        let mut out: BTreeSet<String> = self
            .0
            .table
            .lock()
            .expect("mutex")
            .values()
            .filter(|s| s.alive())
            .filter(|s| {
                s.first_seen.saturating_duration_since(started) <= settle
                    || s.first_seen.elapsed() >= settle
            })
            .map(|s| s.node.clone())
            .collect();
        if let Some(me) = self.0.me.lock().expect("mutex").as_ref() {
            out.insert(me.node.clone());
        }
        out
    }

    pub(super) fn set_me(&self, id: Option<SessionId>) {
        let changed = *self.0.me.lock().expect("mutex") != id;
        if changed {
            // The role does not carry over to a new incarnation: the old incarnation's
            // role is from now on a lost role for the yield rule.
            self.coordinating(None);
            // A new incarnation opens in `direct`: delegating to nobody.
            self.lean_on(None);
        }
        *self.0.me.lock().expect("mutex") = id;
        // New incarnation: "alone" must rest on a LIST **after** opening, not on the
        // previous incarnation's table — especially when that table only LISTed on the
        // net because we used to be `solo`.
        *self.0.last_list.lock().expect("mutex") = None;
        self.set_quiet(false);
        self.bump();
        self.request_refresh();
    }

    /// Are we the **only unended** session on the bucket, according to the table: we have
    /// LISTed at least once since opening the current incarnation, and every other session
    /// was read as ended. A session just LISTed but not yet read counts as alive —
    /// "unknown" is never "alone".
    pub(super) fn alone(&self) -> bool {
        let t = &self.0;
        let Some(me) = t.me.lock().expect("mutex").clone() else {
            return false;
        };
        if t.last_list.lock().expect("mutex").is_none() {
            return false;
        }
        t.table
            .lock()
            .expect("mutex")
            .iter()
            .all(|(id, s)| *id == me || s.state.is_terminal())
    }

    /// Coordinator `coord` with role `term` may enter `solo`: it has LISTed since opening
    /// the current incarnation, and every other unended session read is
    /// `delegated coord=<coord>:<term>`. Not read yet, unreadable, `direct`, delegated to
    /// another role or `solo` ⇒ no — like [`Self::alone`], "unknown" is never "yes".
    pub(super) fn all_delegated_to(&self, coord: &SessionId, term: u64) -> bool {
        let t = &self.0;
        if t.last_list.lock().expect("mutex").is_none() {
            return false;
        }
        t.table.lock().expect("mutex").iter().all(|(id, s)| {
            id == coord
                || s.state.is_terminal()
                || (!s.unreadable && s.mode.delegated_to(coord, term))
        })
    }

    /// Our record just became (or stopped being) a confirmed delegation to `coord` — see
    /// [`TrackerInner::leaning_on`].
    pub(super) fn lean_on(&self, coord: Option<SessionId>) {
        *self.0.leaning_on.lock().expect("mutex") = coord;
    }

    /// We enter or leave `mode=solo` — see [`TrackerInner::quiet`]. Leaving LISTs on the
    /// next round.
    pub(super) fn set_quiet(&self, quiet: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        if self.0.quiet.swap(quiet, Relaxed) && !quiet {
            *self.0.last_list.lock().expect("mutex") = None;
            self.request_refresh();
        }
    }

    /// A session we know exists but the table does not have yet — a challenger names
    /// itself in the `challenged=` line: from now on we are not "alone" until we read it
    /// ended, so we do not re-enter `solo` and get challenged again (back-and-forth).
    pub(super) fn learn(&self, id: &SessionId) {
        let me = self.0.me.lock().expect("mutex").clone();
        if me.as_ref() == Some(id) {
            return;
        }
        let mut table = self.0.table.lock().expect("mutex");
        if !table.contains_key(id) {
            table.insert(id.clone(), Seen::unread(id));
            drop(table);
            self.bump();
        }
        self.request_refresh();
    }

    pub(super) fn bump(&self) {
        self.0.changed.send_modify(|n| *n += 1);
    }

    /// Our own current incarnation.
    pub(super) fn me(&self) -> Option<SessionId> {
        self.0.me.lock().expect("mutex").clone()
    }

    /// Session `id` has "settled" by the rule of [`Self::settled_nodes`] (present since we
    /// started, or in the table for at least `settle`). Not in the table ⇒ not yet.
    pub(super) fn settled(&self, id: &SessionId, settle: Duration) -> bool {
        let started = self.0.started;
        self.0
            .table
            .lock()
            .expect("mutex")
            .get(id)
            .is_some_and(|s| {
                s.first_seen.saturating_duration_since(started) <= settle
                    || s.first_seen.elapsed() >= settle
            })
    }

    /// Changes whenever the set of live sessions, the `owns` hints, or a dead session changes.
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.0.changed.subscribe()
    }

    /// Changes whenever the tracker sees another session **just end**: the record carries
    /// `expired`/`closed` (written by itself, or by our winning `expired` CAS), or the
    /// record of an unended session disappeared — exactly the ways
    /// [`crate::Session::is_session_dead`] considers dead. Narrower than [`Self::changed`]
    /// (does not change with `owns` hints or addresses): for the loop writing tombstones on
    /// another's behalf (SM-11) — the owner's death is when the range it left unfinished
    /// can be tombstoned.
    pub fn ended(&self) -> watch::Receiver<u64> {
        self.0.ended.subscribe()
    }

    pub(super) fn bump_ended(&self) {
        self.0.ended.send_modify(|n| *n += 1);
    }

    /// Asks for an observation round now (floor [`MIN_REFRESH_GAP`]) — in every mode it is
    /// a round that GETs every unended session.
    pub fn request_refresh(&self) {
        self.0
            .forced
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.0.poke.notify_one();
    }

    /// Live nodes, counted by `node` — two incarnations of the same node (during the
    /// switch) are one node. Always includes ourselves.
    pub fn live_nodes(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = self
            .0
            .table
            .lock()
            .expect("mutex")
            .values()
            .filter(|s| s.alive())
            .map(|s| s.node.clone())
            .collect();
        if let Some(me) = self.0.me.lock().expect("mutex").as_ref() {
            out.insert(me.node.clone());
        }
        out
    }

    /// HTTP addresses of the **other** live sessions, each address once — after an
    /// observation round **with a LIST**.
    ///
    /// For a node about to shut down cleanly to tell peers to observe again at once: a
    /// peer only sees our `closed` at its next observation round, up to TTL/3 later. LIST
    /// now rather than use the existing table: the table only LISTs every TTL, so a node
    /// that just joined the cluster (exactly the one that will take our ranges) may not be
    /// in it yet. One LIST + one GET per session, once per shutdown.
    pub async fn discover_peers(&self) -> Vec<String> {
        if let Err(e) = self.list_now().await {
            tracing::debug!(error = %e, "could not observe sessions before shutting down");
        }
        self.peer_addresses()
    }

    /// One observation round **with a LIST** right now; `Err` = the LIST failed (known
    /// sessions are still GET as in every round).
    pub async fn list_now(&self) -> Result<()> {
        *self.0.last_list.lock().expect("mutex") = None;
        self.refresh().await
    }

    /// The ingress address session `id` declares, as of the latest observation.
    pub fn ingress_of(&self, id: &SessionId) -> Option<String> {
        self.0
            .table
            .lock()
            .expect("mutex")
            .get(id)
            .and_then(|s| s.ingress.clone())
    }

    /// HTTP addresses (internal port) of the **other** live sessions, from the current
    /// table — reads nothing. A node that just joined and that no LIST round has seen yet
    /// is absent.
    pub fn peer_addresses(&self) -> Vec<String> {
        let me = self.0.me.lock().expect("mutex").clone();
        let mut out: Vec<String> = self
            .0
            .table
            .lock()
            .expect("mutex")
            .iter()
            .filter(|(id, s)| s.alive() && Some(*id) != me.as_ref())
            .filter_map(|(_, s)| s.address.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// State of session `id` according to the table (our read or CAS), no I/O; `None` =
    /// not in the table.
    pub fn state_of(&self, id: &SessionId) -> Option<SessionState> {
        self.0.table.lock().expect("mutex").get(id).map(|s| s.state)
    }

    /// Changes the observation mode. Back to [`Observing::Full`] from another mode ⇒ a
    /// round at once, always with a LIST: the table may be up to one net stale.
    pub fn set_observing(&self, mode: Observing) {
        let back_to_full = {
            let mut m = self.0.observing.lock().expect("mutex");
            let was = std::mem::replace(&mut *m, mode.clone());
            was != Observing::Full && mode == Observing::Full
        };
        if back_to_full {
            *self.0.last_list.lock().expect("mutex") = None;
            self.request_refresh();
        }
    }

    /// The current observation mode.
    pub fn observing(&self) -> Observing {
        self.0.observing.lock().expect("mutex").clone()
    }

    /// Adds session `id` to the table as a LIST would see it (not read — treated as alive
    /// until a GET knows otherwise), if the table does not have it yet. For a session that
    /// another node reported over the network (heartbeat): equivalent to a LIST seeing it,
    /// so no rule changes — "dead" still only comes from the bucket. `true` = just added.
    pub fn adopt(&self, id: &SessionId) -> bool {
        if self.0.me.lock().expect("mutex").as_ref() == Some(id) {
            return false;
        }
        let added = {
            let mut table = self.0.table.lock().expect("mutex");
            if table.contains_key(id) {
                false
            } else {
                table.insert(id.clone(), Seen::unread(id));
                true
            }
        };
        if added {
            self.bump();
            self.request_refresh();
        }
        added
    }

    /// Every **other** session in the table — see [`Observed`].
    pub fn snapshot(&self) -> Vec<Observed> {
        let me = self.0.me.lock().expect("mutex").clone();
        self.0
            .table
            .lock()
            .expect("mutex")
            .iter()
            .filter(|(id, _)| Some(*id) != me.as_ref())
            .map(|(id, s)| Observed {
                id: id.clone(),
                state: s.state,
                address: s.address.clone(),
                ingress: s.ingress.clone(),
                owns: s.owns.clone(),
                health: s.health,
                quiet: s.since.elapsed(),
            })
            .collect()
    }

    /// The session has ended (`expired`/`closed`) or is considered dead.
    pub fn is_dead(&self, id: &SessionId) -> bool {
        self.0
            .table
            .lock()
            .expect("mutex")
            .get(id)
            .is_some_and(|s| !s.alive())
    }

    /// Partition → owner, according to the hints of the **other** live sessions. With two
    /// hints for one partition the higher term wins.
    pub fn holders(&self) -> BTreeMap<u32, Holder> {
        let mut out: BTreeMap<u32, Holder> = BTreeMap::new();
        for (id, s) in self.0.table.lock().expect("mutex").iter() {
            if !s.alive() {
                continue;
            }
            for (p, term) in &s.owns {
                if out.get(p).is_none_or(|h| h.term < *term) {
                    out.insert(
                        *p,
                        Holder {
                            session: id.clone(),
                            address: s.address.clone(),
                            ingress: s.ingress.clone(),
                            term: *term,
                        },
                    );
                }
            }
        }
        out
    }

    /// Number of ranges each **other** node holds, according to the `owns` hints of live
    /// sessions, grouped by node. A live node holding nothing is present with 0.
    ///
    /// For the balancer: knowing which node is below the floor ⌊ranges / nodes⌋ lets a node
    /// at the ceiling hand ranges to it. The hints lag by at most one renewal tick plus one
    /// observation tick — enough for a decision that needs two TTL cycles before acting.
    pub fn loads(&self) -> BTreeMap<String, usize> {
        let me = self
            .0
            .me
            .lock()
            .expect("mutex")
            .as_ref()
            .map(|m| m.node.clone());
        let mut out: BTreeMap<String, usize> = BTreeMap::new();
        for s in self.0.table.lock().expect("mutex").values() {
            if !s.alive() || Some(&s.node) == me.as_ref() {
                continue;
            }
            *out.entry(s.node.clone()).or_default() += s.owns.len();
        }
        out
    }

    /// Health published by the **other** live nodes (`slow=`, `p99_put_ms=`,
    /// `draining=`), grouped by node: two incarnations alive at once (during the switch)
    /// take the larger score and p99, `draining` if either is draining.
    pub fn peer_health(&self) -> BTreeMap<String, crate::health::Published> {
        let me = self
            .0
            .me
            .lock()
            .expect("mutex")
            .as_ref()
            .map(|m| m.node.clone());
        let mut out: BTreeMap<String, crate::health::Published> = BTreeMap::new();
        for s in self.0.table.lock().expect("mutex").values() {
            if !s.alive() || Some(&s.node) == me.as_ref() {
                continue;
            }
            let e = out.entry(s.node.clone()).or_default();
            e.slow = e.slow.max(s.health.slow);
            e.p99_put_ms = e.p99_put_ms.max(s.health.p99_put_ms);
            e.draining |= s.health.draining;
        }
        out
    }

    // ─────────────────────────────── delegation

    /// One read of `control/coordinator` (ours: the control loop, the fence, a takeover)
    /// — the source of the yield rule ([`CoordKnowledge`]).
    pub(super) fn coordinator_observed(&self, seen: &CoordinatorSeen) {
        let now = Instant::now();
        let mut k = self.0.coord.lock().expect("mutex");
        if let CoordinatorSeen::Held(r) = seen {
            k.terms
                .entry(r.term)
                .or_insert_with(|| (now, r.session.clone()));
            if r.released {
                k.released.entry(r.term).or_insert(now);
            }
        }
        while k.terms.len() > KEEP_TERMS {
            let Some((&oldest, _)) = k.terms.iter().next() else {
                break;
            };
            k.terms.remove(&oldest);
            k.released.remove(&oldest);
        }
        let holders: BTreeSet<SessionId> = k.terms.values().map(|(_, h)| h.clone()).collect();
        k.ended.retain(|id, _| holders.contains(id));
    }

    /// The current incarnation's role: holding term `Some(t)`, or none. Dropping a term we
    /// held makes it, from now on, a **lost** role for the yield rule.
    pub(super) fn coordinating(&self, term: Option<u64>) {
        let now = Instant::now();
        roles::set(&self.0.node, term);
        let mut k = self.0.coord.lock().expect("mutex");
        match (k.mine, term) {
            (Some((had, _)), now_term) if now_term != Some(had) => {
                k.released.entry(had).or_insert(now);
                k.mine = now_term.map(|t| (t, now));
            }
            (None, Some(t)) => k.mine = Some((t, now)),
            _ => {}
        }
    }

    /// A coordinator fence just passed (issued at `at`).
    pub(super) fn fenced(&self, at: Instant) {
        let mut k = self.0.coord.lock().expect("mutex");
        k.fenced_at = Some(k.fenced_at.map_or(at, |f| f.max(at)));
    }

    /// Session `id` just ended on the bucket (read, our CAS, vanished): if it ever held
    /// the coordinator role, that role is lost from now on (yield rule).
    fn note_ended(&self, id: &SessionId) {
        self.0
            .coord
            .lock()
            .expect("mutex")
            .ended
            .entry(id.clone())
            .or_insert_with(Instant::now);
    }

    /// We (the coordinator) become weak or healthy again (expire rule 3).
    pub fn set_weak(&self, weak: bool) {
        let mut k = self.0.coord.lock().expect("mutex");
        match (weak, k.weak_since) {
            (true, None) => k.weak_since = Some(Instant::now()),
            (false, Some(_)) => k.weak_since = None,
            _ => {}
        }
    }

    /// Since when we have been continuously weak — `None` = healthy.
    pub fn weak_for(&self) -> Option<Duration> {
        self.0
            .coord
            .lock()
            .expect("mutex")
            .weak_since
            .map(|at| at.elapsed())
    }

    /// At the coordinator: the heartbeat of `id` **received** at `at`, reporting beat
    /// `beat`, was just **acked** — the τ(N) of LS-15. A heartbeat that is not acked does
    /// not count (M1 `HeartbeatAck`): it renews no grant, and counting it would mean a
    /// node cut off from the bucket but still heartbeating is never expired.
    pub fn heard(&self, id: &SessionId, at: Instant, beat: u64) {
        self.adopt(id);
        if let Some(s) = self.0.table.lock().expect("mutex").get_mut(id) {
            if s.heard.is_none_or(|h| h <= at) {
                s.heard = Some(at);
                s.heard_beat = Some(beat);
            }
        }
    }

    /// At the coordinator: GET `id`'s record now (the heartbeat reports a version the
    /// tracker does not have yet — the node just delegated, or just wrote `owns`). Like an
    /// observation round of its own (M1 `CoordinatorRead`).
    pub async fn confirm(&self, id: &SessionId) {
        if self.me().as_ref() == Some(id) {
            return;
        }
        if let Err(e) = self.observe(id).await {
            tracing::debug!(session = %id, error = %e, "could not confirm the delegated record");
        }
    }

    /// Whether session `id`'s record on the bucket has ended **right now**: one GET, read
    /// only — the table is not updated ([`Membership::live_peers_now`]). Gone, `expired` or
    /// `closed` ⇒ `true`; a failed GET or an undecodable record ⇒ `false`.
    pub async fn ended_on_bucket(&self, id: &SessionId) -> bool {
        match read_session(&self.0.store, &self.0.prefix, id).await {
            Ok(None) => true,
            Ok(Some((Ok(rec), _))) => rec.state.is_terminal(),
            Ok(Some((Err(_), _))) | Err(_) => false,
        }
    }

    /// We have issued an `expired` CAS for `id` (ack rule 5).
    pub fn expire_issued(&self, id: &SessionId) -> bool {
        self.0
            .table
            .lock()
            .expect("mutex")
            .get(id)
            .is_some_and(|s| s.expire_issued)
    }

    /// Session `id` as the coordinator needs it to answer its heartbeat (ack rules 4–5) —
    /// see [`LeaseView`].
    pub fn lease_view(&self, id: &SessionId) -> LeaseView {
        let me = self.me();
        let mine = self.0.coord.lock().expect("mutex").mine;
        let table = self.0.table.lock().expect("mutex");
        let Some(s) = table.get(id) else {
            return LeaseView::Direct;
        };
        if s.state.is_terminal() || s.expire_issued {
            return LeaseView::Ended;
        }
        if s.unreadable {
            return LeaseView::Other;
        }
        match &s.mode {
            SessionMode::Delegated { coord, term }
                if Some(coord) == me.as_ref() && mine.is_some_and(|(t, _)| t == *term) =>
            {
                LeaseView::DelegatedToMe {
                    beat: s.best_beat,
                    etag: s.etag.clone(),
                }
            }
            SessionMode::Direct => LeaseView::Direct,
            SessionMode::Delegated { .. } => LeaseView::DelegatedElsewhere,
            _ => LeaseView::Other,
        }
    }

    /// Every unended session we have read has `knows ≥ k` — the gate's run-time rule. A
    /// session read without `knows` (a binary from before the migration framework) or
    /// with a lower one ⇒ `Some(false)`; a session never read yet ⇒ `None` (unknown: no
    /// new delegation, but the whole cluster is not forced to fall back).
    pub fn all_know(&self, k: u32) -> Option<bool> {
        let me = self.me();
        let table = self.0.table.lock().expect("mutex");
        let mut unknown = false;
        for (id, s) in table.iter() {
            if Some(id) == me.as_ref() || s.state.is_terminal() {
                continue;
            }
            if s.observed_at.is_none() || s.unreadable {
                unknown = true;
                continue;
            }
            if s.knows.is_none_or(|v| v < k) {
                return Some(false);
            }
        }
        (!unknown).then_some(true)
    }

    /// One observation round. `pub` for tests; normally runs in the background.
    pub async fn refresh(&self) -> Result<()> {
        let t = &self.0;
        let me = t.me.lock().expect("mutex").clone();
        let mode = t.observing.lock().expect("mutex").clone();
        let forced = t.forced.swap(false, std::sync::atomic::Ordering::Relaxed);
        // Alone (`solo`): the LIST is only a net.
        let list_every = if t.quiet.load(std::sync::atomic::Ordering::Relaxed) {
            t.quiet_list_every
        } else {
            match mode {
                Observing::Full | Observing::Coordinator => t.ttl,
                Observing::Sparse { .. } => LIST_NET,
            }
        };
        let list_due = t
            .last_list
            .lock()
            .expect("mutex")
            .is_none_or(|at| at.elapsed() >= list_every);
        // A failed LIST does not skip the whole round: known sessions are still GET and
        // marked as usual — otherwise a dead node would never be declared dead as long as
        // the LIST keeps failing (missing LIST permission, per-prefix throttling), and
        // nobody would take its ranges. `last_list` unchanged ⇒ the next round LISTs again.
        let listed = if list_due {
            let r = self.discover().await;
            if r.is_ok() {
                *t.last_list.lock().expect("mutex") = Some(Instant::now());
            }
            r
        } else {
            Ok(())
        };
        // An ended session is final (`expired`/`closed` never returns to `live`): do not
        // GET it again. It leaves the table when the LIST no longer sees it, or when we
        // clean it up after `GC_AFTER_TTLS` TTLs (under a burst of kills, the dead
        // incarnations used to take most of this round's GETs).
        // Sparse tracker: only sessions that are due (never GET yet, or last GET more than
        // `SPARSE_AFTER_TTLS` TTLs ago) and suspect sessions.
        let sparse_every = t.ttl * SPARSE_AFTER_TTLS;
        // Coordinator: a session delegating to our role whose heartbeats keep arriving
        // needs no GET — τ is when the heartbeat was received, and when a heartbeat reports
        // a new version the responder GETs it at once ([`Self::confirm`]). Heartbeats stop
        // arriving ⇒ GET every tick as before, and CAS `expired` after one TTL.
        let mine = t.coord.lock().expect("mutex").mine.map(|(term, _)| term);
        let heard_fresh = t.ttl / 3;
        let due = |id: &SessionId, s: &Seen| match &mode {
            Observing::Sparse { suspects } if !forced => {
                suspects.contains(id) || s.observed_at.is_none_or(|at| at.elapsed() >= sparse_every)
            }
            Observing::Coordinator if !forced => {
                let delegated_here = matches!(&s.mode, SessionMode::Delegated { coord, term }
                    if me.as_ref() == Some(coord) && mine == Some(*term));
                // The heartbeat reports exactly the beat we read: the owner wrote nothing new.
                !(delegated_here
                    && !s.unreadable
                    && s.heard_beat == Some(s.best_beat)
                    && s.heard.is_some_and(|h| h.elapsed() < heard_fresh))
            }
            _ => true,
        };
        let ids: Vec<SessionId> = t
            .table
            .lock()
            .expect("mutex")
            .iter()
            .filter(|(id, s)| !s.state.is_terminal() && due(id, s))
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if me.as_ref() == Some(&id) {
                continue;
            }
            if let Err(e) = self.observe(&id).await {
                tracing::debug!(session = %id, error = %e, "could not observe the session");
                self.expire_held(&id).await;
            }
        }
        listed
    }

    /// The coordinator cannot read a record delegated to its role (the GET fails — the
    /// store throttles that prefix alone): expire rule 4 does not need a re-read — CAS
    /// `expired` If-Match **the version held** (etag and content of the last read) once τ
    /// is one TTL old. If the owner wrote anything since, the CAS loses.
    async fn expire_held(&self, id: &SessionId) {
        let t = &self.0;
        let me = self.me();
        let rec = {
            let k = t.coord.lock().expect("mutex");
            let mut table = t.table.lock().expect("mutex");
            let Some(s) = table.get_mut(id) else { return };
            let SessionMode::Delegated { coord, term } = s.mode.clone() else {
                return;
            };
            if s.unreadable || !s.alive() || s.expire_issued || s.version.is_none() {
                return;
            }
            if delegated_due(&k, me.as_ref(), s, &coord, term, None, t.ttl) != Due::ByCoordinator {
                return;
            }
            s.expire_issued = true;
            SessionRecord {
                node: s.node.clone(),
                beat: s.best_beat,
                address: s.address.clone(),
                ingress: s.ingress.clone(),
                knows: s.knows,
                slow: s.health.slow,
                p99_put_ms: s.health.p99_put_ms,
                draining: s.health.draining,
                owns: s.owns.clone(),
                state: SessionState::Expired,
                mode: s.mode.clone(),
                challenged: None,
            }
        };
        let Some(version) = t
            .table
            .lock()
            .expect("mutex")
            .get(id)
            .and_then(|s| s.version.clone())
        else {
            return;
        };
        tracing::info!(session = %id, "coordinator: delegated record unreadable and silent for one TTL — CAS expired on the version held");
        crate::failpoint!("coordinator::before_expire", |_| ());
        let put = t
            .store
            .put_opts(
                &session_path(&t.prefix, id),
                PutPayload::from(rec.encode()),
                PutOptions::from(PutMode::Update(version)),
            )
            .await;
        if put.is_ok() {
            if let Some(s) = t.table.lock().expect("mutex").get_mut(id) {
                s.state = SessionState::Expired;
                s.terminal_since = Some(Instant::now());
            }
            self.note_ended(id);
            self.bump();
            self.bump_ended();
        }
    }

    /// LISTs the session directory: adds new sessions, drops vanished ones, cleans up
    /// sessions that ended long ago.
    async fn discover(&self) -> Result<()> {
        let t = &self.0;
        let dir = Path::from(sessions_dir(&t.prefix));
        let listed: Vec<_> = t.store.list(Some(&dir)).try_collect().await.map_err(io)?;
        let present: HashSet<SessionId> = listed
            .iter()
            .filter_map(|m| session_id_of(&t.prefix, &m.location))
            .collect();
        let mut garbage = Vec::new();
        let mut changed = false;
        let mut ended = false;
        {
            let mut table = t.table.lock().expect("mutex");
            let before = table.len();
            let vanished: Vec<SessionId> = table
                .iter()
                .filter(|(id, s)| !present.contains(*id) && !s.state.is_terminal())
                .map(|(id, _)| id.clone())
                .collect();
            for id in &vanished {
                self.note_ended(id);
            }
            let vanished = vanished.len();
            table.retain(|id, _| present.contains(id));
            changed |= table.len() != before;
            ended |= vanished > 0;
            for id in &present {
                table.entry(id.clone()).or_insert_with(|| Seen::unread(id));
            }
            for (id, s) in table.iter() {
                if s.terminal_since
                    .is_some_and(|at| at.elapsed() >= t.ttl * GC_AFTER_TTLS)
                {
                    garbage.push(id.clone());
                }
            }
        }
        for id in garbage {
            // Only **ended** sessions are deleted: deleting a live session could swallow
            // the renewal it just wrote, and others would see "absent" ⇒ orphaned without
            // going through any timing argument.
            match t.store.delete(&session_path(&t.prefix, &id)).await {
                Ok(()) | Err(OsError::NotFound { .. }) => {
                    tracing::debug!(session = %id, "cleaned up an ended session");
                    t.table.lock().expect("mutex").remove(&id);
                    changed = true;
                }
                Err(e) => tracing::debug!(session = %id, error = %e, "session cleanup failed"),
            }
        }
        if changed {
            self.bump();
        }
        if ended {
            self.bump_ended();
        }
        Ok(())
    }

    /// GETs one session, updates the observation, marks it `expired` if it stood still.
    async fn observe(&self, id: &SessionId) -> Result<()> {
        let t = &self.0;
        let Some((raw, decoded, version)) = read_session_raw(&t.store, &t.prefix, id).await? else {
            let gone = t.table.lock().expect("mutex").remove(id);
            if let Some(s) = gone {
                self.bump();
                if !s.state.is_terminal() {
                    self.note_ended(id);
                    self.bump_ended();
                }
            }
            return Ok(());
        };
        let received = Instant::now();
        let mut changed = false;
        let mut ended = false;
        let mut expire: Option<SessionRecord> = None;
        // Challenge: the content of the owner's `solo` record, plus a `challenged=<us>`
        // line. Overwrite-as-challenge (`rearm` of a `solo` record).
        let mut challenge: Option<Vec<u8>> = None;
        let mut rearm: Option<SessionRecord> = None;
        let me = t.me.lock().expect("mutex").clone();
        // Our record delegates to this very session ⇒ do not challenge its `solo` record
        // — we are living on its acks.
        let defer_challenge = t.leaning_on.lock().expect("mutex").as_ref() == Some(id);
        // CAS `expired` under the coordinator's rule (LS-15) — the tripwire and the
        // `coordinator::before_expire` failpoint before the PUT.
        let mut by_coordinator = false;
        {
            let mut table = t.table.lock().expect("mutex");
            // A record delegated to `(X, t)`: session X ending on the bucket is proof that
            // the role is lost (yield rule).
            let target = match &decoded {
                Ok(SessionRecord {
                    mode: SessionMode::Delegated { coord, .. },
                    ..
                }) => Some(coord.clone()),
                _ => None,
            };
            let holder_ended = target.as_ref().and_then(|c| {
                table
                    .get(c)
                    .filter(|h| h.state.is_terminal())
                    .and_then(|h| h.terminal_since)
            });
            let Some(s) = table.get_mut(id) else {
                return Ok(());
            };
            s.observed_at = Some(received);
            match &decoded {
                Ok(rec) => {
                    if s.address != rec.address
                        || s.ingress != rec.ingress
                        || s.owns != rec.owns
                        || s.health.draining != rec.draining
                    {
                        s.address = rec.address.clone();
                        s.ingress = rec.ingress.clone();
                        s.owns = rec.owns.clone();
                        changed = true;
                    }
                    // Score and p99 change every round: update without waking anyone.
                    s.health = crate::health::Published {
                        slow: rec.slow,
                        p99_put_ms: rec.p99_put_ms,
                        draining: rec.draining,
                    };
                    // A new version of the record: the beat increased (the owner wrote), or —
                    // for a `solo` record — the `challenged` line or the mode changed
                    // while the beat did not increase (a challenger, or an overwrite). The
                    // silence mark counts from when that version was **received**.
                    let newer = rec.beat > s.best_beat
                        || s.unreadable
                        || (rec.mode != SessionMode::Direct
                            && (rec.mode != s.mode || rec.challenged != s.challenged));
                    s.unreadable = false;
                    s.mode = rec.mode.clone();
                    s.challenged = rec.challenged.clone();
                    s.knows = rec.knows;
                    if rec.beat >= s.best_beat {
                        s.etag = version.e_tag.clone();
                        s.version = Some(version.clone());
                    }
                    if rec.state.is_terminal() {
                        if s.state != rec.state {
                            ended |= !s.state.is_terminal();
                            t.coord
                                .lock()
                                .expect("mutex")
                                .ended
                                .entry(id.clone())
                                .or_insert(received);
                            s.state = rec.state;
                            s.terminal_since = Some(received);
                            changed = true;
                        }
                    } else if newer {
                        if rec.beat > s.best_beat {
                            s.expire_failures = 0;
                        }
                        s.best_beat = s.best_beat.max(rec.beat);
                        s.since = received;
                        // A `solo` record nobody has challenged yet: challenge it now —
                        // the LS-1 silence rule does not apply to it —, unless it is the
                        // coordinator we are delegating to.
                        if rec.mode == SessionMode::Solo
                            && rec.challenged.is_none()
                            && !defer_challenge
                        {
                            challenge = me.as_ref().map(|m| challenge_body(&raw, m));
                        }
                    } else if s.alive() {
                        match &rec.mode {
                            // LS-1: one TTL of silence since receiving the largest beat.
                            SessionMode::Direct if s.since.elapsed() >= t.ttl => {
                                expire = Some(SessionRecord {
                                    state: SessionState::Expired,
                                    ..rec.clone()
                                });
                            }
                            // LS-16: nobody has challenged yet (our previous challenge
                            // lost or was ambiguous and not seen yet) ⇒ challenge; already
                            // challenged and the record unchanged for one TTL since we
                            // received the version with that line ⇒ `expired`.
                            SessionMode::Solo if rec.challenged.is_none() => {
                                if !defer_challenge {
                                    challenge = me.as_ref().map(|m| challenge_body(&raw, m));
                                }
                            }
                            SessionMode::Solo if s.since.elapsed() >= t.ttl => {
                                expire = Some(SessionRecord {
                                    state: SessionState::Expired,
                                    ..rec.clone()
                                });
                            }
                            // LS-15: only the coordinator of exactly that role counts
                            // silence (from the latest proof of life); others yield until
                            // one TTL after seeing the role lost **and** one TTL after
                            // this version.
                            SessionMode::Delegated { coord, term } => {
                                let k = t.coord.lock().expect("mutex");
                                match delegated_due(
                                    &k,
                                    me.as_ref(),
                                    s,
                                    coord,
                                    *term,
                                    holder_ended,
                                    t.ttl,
                                ) {
                                    Due::No => {}
                                    due => {
                                        by_coordinator = due == Due::ByCoordinator;
                                        expire = Some(SessionRecord {
                                            state: SessionState::Expired,
                                            ..rec.clone()
                                        });
                                    }
                                }
                            }
                            // A mode this binary does not know: nothing to prove — no
                            // expiry, no challenge.
                            _ => {}
                        }
                    }
                }
                // A `solo` record that is no longer readable: the owner cannot renew from
                // it (its self-check GET sees foreign content), but the mark must be when
                // we **saw** it corrupt, not the last beat — the owner renews by GET right
                // up to the moment it broke. One TTL later, overwrite it with a `solo`
                // record carrying our `challenged` line (a live owner answers), then go on
                // as with a challenge.
                Err(_) if s.mode == SessionMode::Solo => {
                    if !s.unreadable {
                        s.unreadable = true;
                        s.since = received;
                    } else if s.since.elapsed() >= t.ttl && s.alive() {
                        if let Some(m) = &me {
                            rearm = Some(SessionRecord {
                                node: id.node.clone(),
                                beat: s.best_beat,
                                address: None,
                                ingress: None,
                                knows: None,
                                slow: None,
                                p99_put_ms: None,
                                draining: false,
                                owns: BTreeMap::new(),
                                state: SessionState::Live,
                                mode: SessionMode::Solo,
                                challenged: Some(m.to_string()),
                            });
                        }
                    }
                }
                // A cluster that has had a coordinator: an unreadable record may be a torn
                // delegating CAS — we do not know which role it delegates to, so **only**
                // the current coordinator expires it, and only once its fence has passed
                // **after** we saw the record corrupt (nobody took the role after it before
                // the corruption — the record cannot delegate to a newer role), with τ ≥
                // when it took the role (the old role's acks have lapsed). The count starts
                // when the corruption was seen.
                Err(_) if delegation_possible(&t.coord.lock().expect("mutex")) => {
                    if !s.unreadable {
                        s.unreadable = true;
                        s.since = received;
                    }
                    let k = t.coord.lock().expect("mutex");
                    if s.alive() && unknown_due(&k, s, t.ttl) {
                        by_coordinator = true;
                        expire = Some(SessionRecord {
                            node: id.node.clone(),
                            beat: s.best_beat,
                            address: None,
                            ingress: None,
                            knows: None,
                            slow: None,
                            p99_put_ms: None,
                            draining: false,
                            owns: BTreeMap::new(),
                            state: SessionState::Expired,
                            mode: s.mode.clone(),
                            challenged: None,
                        });
                    }
                }
                // An unreadable record cannot advance any beat: after one silent TTL it is
                // marked like any other session standing still.
                Err(_) => {
                    if s.since.elapsed() >= t.ttl && s.alive() {
                        expire = Some(SessionRecord {
                            node: id.node.clone(),
                            beat: s.best_beat,
                            address: None,
                            ingress: None,
                            knows: None,
                            slow: None,
                            p99_put_ms: None,
                            draining: false,
                            owns: BTreeMap::new(),
                            state: SessionState::Expired,
                            mode: SessionMode::Direct,
                            challenged: None,
                        });
                    }
                }
            }
        }
        if let Some(body) = challenge {
            self.challenge(id, body, version.clone(), me.as_ref()).await;
        }
        if let Some(rec) = expire {
            // Ack rule 5: remember the issue **before** the PUT — any outcome (even
            // `Precondition` or `Err`) may be a PUT that applied (rule 7).
            if let Some(s) = t.table.lock().expect("mutex").get_mut(id) {
                s.expire_issued = true;
            }
            if by_coordinator {
                tracing::info!(session = %id, "coordinator: delegated session silent for one TTL — CAS expired");
                // `return` ⇒ no PUT is issued (the issue is still remembered).
                crate::failpoint!("coordinator::before_expire", |_| Ok(()));
            }
            let put = t
                .store
                .put_opts(
                    &session_path(&t.prefix, id),
                    PutPayload::from(rec.encode()),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await;
            let mut table = t.table.lock().expect("mutex");
            if let Some(s) = table.get_mut(id) {
                match put {
                    Ok(_) => {
                        tracing::info!(session = %id, "session stood still past the TTL — marked expired");
                        t.coord
                            .lock()
                            .expect("mutex")
                            .ended
                            .entry(id.clone())
                            .or_insert(received);
                        ended |= !s.state.is_terminal();
                        s.state = SessionState::Expired;
                        s.terminal_since = Some(Instant::now());
                        changed = true;
                    }
                    Err(OsError::Precondition { .. }) => {
                        s.expire_failures += 1;
                        if s.expire_failures >= 2
                            && s.since.elapsed() >= t.ttl * UNEXPIRABLE_AFTER_TTLS
                        {
                            // The `solo` record is overwritten **with our `challenged`
                            // line**: a bare overwrite would restore exactly the content
                            // the owner last wrote, and the owner's self-check GET would
                            // renew from it.
                            let challenged = match (&rec.mode, &me) {
                                (SessionMode::Solo, Some(m)) => Some(m.to_string()),
                                _ => rec.challenged.clone(),
                            };
                            rearm = Some(SessionRecord {
                                state: SessionState::Live,
                                beat: s.best_beat.max(rec.beat),
                                challenged,
                                ..rec
                            });
                        }
                    }
                    Err(e) => {
                        tracing::debug!(session = %id, error = %e, "could not mark the session")
                    }
                }
            }
        }
        if let Some(rec) = rearm {
            self.rearm(id, rec).await;
        }
        if changed {
            self.bump();
        }
        if ended {
            self.bump_ended();
        }
        Ok(())
    }

    /// **Challenges** the `solo` record of session `id` (LS-16): CAS If-Match `version`
    /// (the etag we just read) with content `body` — the owner's record verbatim plus a
    /// `challenged=<us>` line; the beat is unchanged.
    ///
    /// - Won (or ambiguous and the re-read finds exactly `body`, rule 7 —
    ///   `cas::put_resolving`; `body` carries our incarnation, so a match is ours):
    ///   τ = **when the answer was received**, after the store applied the write. Every
    ///   self-check GET of the owner served after that sees the `challenged` line and does
    ///   not renew; GETs served before that were issued before τ. If the record is
    ///   unchanged at τ + TTL, `observe` CASes `expired`.
    /// - Lost: the record changed under us (the owner just wrote `owns`, answered, or
    ///   someone else challenged first) — the next round re-reads; an increased beat means
    ///   the owner is still alive. Losing forever while the beat does not increase (a torn
    ///   object) ⇒ overwrite as in `rearm`.
    /// - Ambiguous and unresolvable: no τ. If a late PUT does apply, the next read sees
    ///   our `challenged` line as a new version (τ = when received).
    async fn challenge(
        &self,
        id: &SessionId,
        body: Vec<u8>,
        version: UpdateVersion,
        me: Option<&SessionId>,
    ) {
        let t = &self.0;
        let Some(me) = me else { return };
        crate::failpoint!("session::before_challenge_cas", |_| ());
        let put = crate::cas::put_resolving(
            &t.store,
            &session_path(&t.prefix, id),
            bytes::Bytes::from(body.clone()),
            PutMode::Update(version),
        )
        .await;
        let mut rearm = None;
        if let Some(s) = t.table.lock().expect("mutex").get_mut(id) {
            match put {
                Ok(crate::cas::Cas::Written(_)) => {
                    tracing::info!(session = %id, "solo record of another session — challenged, waiting one TTL");
                    s.since = Instant::now();
                    s.challenged = Some(me.to_string());
                    s.expire_failures = 0;
                }
                Ok(crate::cas::Cas::Lost) => {
                    s.expire_failures += 1;
                    if s.expire_failures >= 2 && s.since.elapsed() >= t.ttl * UNEXPIRABLE_AFTER_TTLS
                    {
                        rearm = SessionRecord::decode(&body).ok();
                    }
                }
                Err(e) => {
                    tracing::debug!(session = %id, error = %e, "could not challenge the session")
                }
            }
        }
        if let Some(rec) = rearm {
            self.rearm(id, rec).await;
        }
    }

    /// Session `id` stood still for more than [`UNEXPIRABLE_AFTER_TTLS`] TTLs while its
    /// `expired` CAS keeps losing — a torn object (MinIO EC after a half-done PUT: GETs
    /// flip the etag back and forth) or a stale read layer. The tracker used to consider
    /// it dead at once (`dead_unexpirable`), by our clock alone: an owner that is still
    /// alive while we read stale data could still renew, and our `claim` would create two
    /// leaders.
    ///
    /// Now we build a fence on the bucket: **overwrite** the record, unconditionally (a CAS
    /// never wins on a torn object), with a `live` record carrying the largest beat we have
    /// seen. The overwrite changes the version, so the owner's next renewal (a CAS on the
    /// old version) loses, re-reads and sees a foreign beat ⇒ `lose`. We count silence
    /// again **from receiving the answer** to the overwrite, then CAS `expired` like any
    /// session standing still — the proof at the top of `session/mod.rs` applies intact
    /// with that mark: every renewal issued before the overwrite lapses before
    /// mark + ⅔·TTL, every one issued after loses. Only that CAS winning makes the session
    /// "dead" for `claim`.
    ///
    /// Count from scratch even when the overwrite fails: an `Err` may have written (rule
    /// 7), and an `expired` CAS winning on that very overwrite before one TTL is a CAS that
    /// proves nothing. A failure only warns + bumps a metric — the session stays alive for
    /// everyone; the next round, after standing still another [`UNEXPIRABLE_AFTER_TTLS`]
    /// TTLs, tries again.
    async fn rearm(&self, id: &SessionId, rec: SessionRecord) {
        let t = &self.0;
        let rec = &rec;
        let put = t
            .store
            .put_opts(
                &session_path(&t.prefix, id),
                PutPayload::from(rec.encode()),
                PutOptions::from(PutMode::Overwrite),
            )
            .await;
        if let Some(s) = t.table.lock().expect("mutex").get_mut(id) {
            s.since = Instant::now();
            s.expire_failures = 0;
            // The next read sees exactly what we just wrote: not a new version.
            s.mode = rec.mode.clone();
            s.challenged = rec.challenged.clone();
            s.unreadable = false;
        }
        match put {
            Ok(_) => {
                rearm_stats::WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    session = %id,
                    "session stood still and could not be marked — overwrote the record; marking it expired after another TTL"
                );
            }
            Err(e) => {
                rearm_stats::FAILED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    session = %id,
                    error = %e,
                    "session stood still and could not be marked, the overwrite failed too — NOT considered dead"
                );
            }
        }
    }

    pub(super) fn spawn(&self, runtime: &tokio::runtime::Handle) -> tokio::task::AbortHandle {
        let me = self.clone();
        // The `session::before_challenge_cas` point knows the node (failpoints need a node condition).
        let node = me.0.node.clone();
        let rounds = async move {
            loop {
                if let Err(e) = me.refresh().await {
                    tracing::debug!(error = %e, "session observation round failed");
                }
                crate::pace::beat(MIN_REFRESH_GAP.min(me.0.interval)).await;
                // `biased`: both arms lead to the same refresh — the order only exists to
                // keep the simulation deterministic.
                tokio::select! {
                    biased;
                    _ = me.0.poke.notified() => {}
                    _ = tokio::time::sleep(me.0.interval.saturating_sub(MIN_REFRESH_GAP)) => {}
                }
            }
        };
        runtime
            .spawn(crate::io_source::tagged("session.tracker", async move {
                crate::failpoint::in_node(&node, rounds).await
            }))
            .abort_handle()
    }
}
