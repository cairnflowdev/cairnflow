// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The cluster coordinator**: the object `control/coordinator`.
//!
//! One node of the cluster holds the coordinator role: it observes every node's session on
//! the bucket (the tracker), listens to the other nodes' heartbeats over the network and
//! returns them a `view`, so they do not poll the bucket themselves. The role is a
//! **signal**, not the truth: nobody decides to take or delete anything based on it (LS-3),
//! and every problem (a missing record, a dead coordinator, a broken network) brings the
//! nodes back to the protocol without a coordinator — a full tracker on every node.
//!
//! # Object
//!
//! ```text
//! cairnflow-coordinator/1
//! session=<node>/<inc>
//! term=<n>
//! address=<internal port>
//! released            (only once released)
//! ```
//!
//! **Read leniently, absence means "no coordinator"** (rule 2 of AGENTS.md): unknown lines
//! are skipped; a header of the same family with a higher number belongs to a newer binary
//! — not followed, not overwritten; an undecodable record counts as vacant (it can be taken,
//! by CAS on the version just read).
//!
//! # Who takes it, and when
//!
//! The record is **vacant** when absent, garbage, `released`, or pointing at a session that
//! has ended **on the bucket** ([`Session::is_session_dead`] — the tracker or a direct GET).
//! The node running GC (the leader of the smallest range) tries at once; other nodes try
//! when that situation is still unchanged after a TTL + jitter — the schedule lives in the
//! caller (`cairn-server/src/control.rs`). Taking = a CAS (`cas::put_resolving`, rule 7) on
//! the version just read with `term + 1`; the content carries session + term so a re-read
//! that matches is ours. Before taking, the node reads its own session record: an
//! incarnation that has been expired (a frozen node just woken up) does not take it.
//!
//! **Sticky role:** held until the session ends or the node releases it ([`Session::close`],
//! an incarnation change), not changing as ranges move. Releasing = a CAS of `released` on
//! our version. A node that has just released or lost the role does not take it back within
//! [`RETAKE_AFTER_TTLS`] TTLs (so a weak coordinator does not keep releasing and retaking).
//!
//! # The coordinator fence ([`Session::coordinator_fence`])
//!
//! Before each batch of heartbeat answers, and **after** the batch's heartbeats have been
//! received: one GET with `If-None-Match` of `control/coordinator` and one of this very
//! incarnation's session record (in parallel). It passes when the coordinator record still
//! carries exactly our session + term, not released, **and** that session is the **current**
//! incarnation (a node that has just opened a new incarnation does not pass the fence thanks
//! to its own new record), **and** the session record is still our `live` one. The second
//! half of that rule — the term in the heartbeat equals the current term — is the job of
//! whoever answers each heartbeat (`cairn-server/src/control.rs`). When answers were only
//! membership hints, the fence was already built in full, so that acking grant renewals
//! later did not have to change its shape.

use {
    super::*,
    crate::cas::{put_resolving, Cas},
    bytes::Bytes,
    slatedb::object_store::GetOptions,
};

/// Path under the cluster prefix.
pub const COORDINATOR_OBJECT: &str = "control/coordinator";

/// The header this binary writes.
pub const COORDINATOR_HEADER: &str = "cairnflow-coordinator/1";

/// A node that has just released or lost the role does not take it back within this many
/// TTLs.
pub const RETAKE_AFTER_TTLS: u32 = 3;

/// Content of `control/coordinator`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorRecord {
    pub session: SessionId,
    pub term: u64,
    /// The coordinator's internal port — where heartbeats are sent.
    pub address: Option<String>,
    pub released: bool,
}

/// What can be read at `control/coordinator`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoordinatorSeen {
    /// No record: nobody coordinates.
    Absent,
    Held(CoordinatorRecord),
    /// A header of the same family with a higher number: a newer binary's — not followed,
    /// not overwritten.
    Newer,
    /// Undecodable — counts as vacant.
    Garbage,
}

impl CoordinatorRecord {
    pub fn encode(&self) -> Bytes {
        let mut out = format!(
            "{COORDINATOR_HEADER}\nsession={}\nterm={}\n",
            self.session, self.term
        );
        if let Some(a) = &self.address {
            out.push_str(&format!("address={a}\n"));
        }
        if self.released {
            out.push_str("released\n");
        }
        Bytes::from(out)
    }

    pub fn decode(raw: &[u8]) -> CoordinatorSeen {
        let Ok(text) = std::str::from_utf8(raw) else {
            return CoordinatorSeen::Garbage;
        };
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != COORDINATOR_HEADER {
            if crate::format::newer_header(COORDINATOR_OBJECT, COORDINATOR_HEADER, header).is_some()
            {
                return CoordinatorSeen::Newer;
            }
            return CoordinatorSeen::Garbage;
        }
        let (mut session, mut term, mut address, mut released) = (None, None, None, false);
        for kv in lines.flat_map(str::split_whitespace) {
            match kv.split_once('=') {
                Some(("session", v)) => session = SessionId::parse(v),
                Some(("term", v)) => term = v.parse().ok(),
                Some(("address", v)) => address = Some(v.to_string()),
                None if kv == "released" => released = true,
                _ => {}
            }
        }
        match (session, term) {
            (Some(session), Some(term)) => CoordinatorSeen::Held(CoordinatorRecord {
                session,
                term,
                address,
                released,
            }),
            _ => CoordinatorSeen::Garbage,
        }
    }
}

pub(super) fn coordinator_path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(COORDINATOR_OBJECT)
    } else {
        Path::from(format!("{}/{COORDINATOR_OBJECT}", prefix.as_ref()))
    }
}

/// This session's coordinator role, and the latest read of the record.
#[derive(Default)]
pub(super) struct Role {
    st: Mutex<RoleState>,
}

impl Role {
    /// The term of the role we hold under session `me` (a role of another incarnation does
    /// not count). No I/O.
    pub(super) fn held_by(&self, me: &SessionId) -> Option<u64> {
        let st = self.st.lock().expect("mutex");
        st.held
            .as_ref()
            .filter(|h| &h.record.session == me)
            .map(|h| h.record.term)
    }
}

#[derive(Default)]
struct RoleState {
    /// The latest read and its version mark. `None` = not read yet.
    last: Option<(CoordinatorSeen, Option<UpdateVersion>)>,
    /// The role we hold.
    held: Option<Held>,
    /// When we last released or lost the role.
    released_at: Option<Instant>,
}

#[derive(Clone)]
struct Held {
    record: CoordinatorRecord,
    version: UpdateVersion,
}

/// What a node reports about itself in its heartbeat: incarnation, in-memory beat, etag of
/// the last renewal, the `owns` hint, health.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelfReport {
    pub id: SessionId,
    pub beat: u64,
    pub etag: Option<String>,
    pub address: Option<String>,
    pub ingress: Option<String>,
    pub owns: BTreeMap<u32, u64>,
    pub health: crate::health::Published,
    pub knows: u32,
}

/// One GET of `control/coordinator`, conditional when there is an etag.
enum Got {
    NotModified,
    Body(Bytes, UpdateVersion),
    Absent,
}

impl Session {
    /// What this node reports about itself in its heartbeat — no I/O. `None` = no live
    /// incarnation.
    pub fn self_report(&self) -> Option<SelfReport> {
        let st = self.0.state.lock().expect("mutex");
        let cur = st.current.as_ref()?;
        Some(SelfReport {
            id: cur.id.clone(),
            beat: cur.beat,
            etag: cur.version.e_tag.clone(),
            address: self.0.cfg.address.clone(),
            ingress: self.0.cfg.ingress.clone(),
            owns: st.owns.clone(),
            health: published(&self.0),
            knows: self.0.cfg.knows,
        })
    }

    /// The coordinator record of the latest read — no I/O. `None` = never read.
    pub fn coordinator_seen(&self) -> Option<CoordinatorSeen> {
        let st = self.0.coordinator.st.lock().expect("mutex");
        st.last.as_ref().map(|(s, _)| s.clone())
    }

    /// The term of the coordinator role the **current incarnation** holds — no I/O. A role
    /// held under a dropped incarnation does not count: the caller releases it with
    /// [`Self::drop_stale_coordinator`].
    pub fn coordinating(&self) -> Option<u64> {
        let current = self.current()?;
        let st = self.0.coordinator.st.lock().expect("mutex");
        st.held
            .as_ref()
            .filter(|h| h.record.session == current)
            .map(|h| h.record.term)
    }

    /// We released or lost the role within `within` — do not take it back.
    pub fn released_coordinator_within(&self, within: Duration) -> bool {
        let st = self.0.coordinator.st.lock().expect("mutex");
        st.released_at.is_some_and(|at| at.elapsed() < within)
    }

    /// Reads `control/coordinator` (`If-None-Match` with the previous etag), bound TTL/3.
    pub async fn read_coordinator(&self) -> Result<CoordinatorSeen> {
        let etag = {
            let st = self.0.coordinator.st.lock().expect("mutex");
            st.last
                .as_ref()
                .and_then(|(_, v)| v.as_ref())
                .and_then(|v| v.e_tag.clone())
        };
        let got = self.get_coordinator(etag).await?;
        let mut st = self.0.coordinator.st.lock().expect("mutex");
        let seen = match got {
            Got::NotModified => match &st.last {
                Some((s, _)) => return Ok(s.clone()),
                None => CoordinatorSeen::Garbage,
            },
            Got::Body(raw, version) => {
                let seen = CoordinatorRecord::decode(&raw);
                st.last = Some((seen.clone(), Some(version)));
                seen
            }
            Got::Absent => {
                st.last = Some((CoordinatorSeen::Absent, None));
                CoordinatorSeen::Absent
            }
        };
        drop(st);
        self.0.tracker.coordinator_observed(&seen);
        Ok(seen)
    }

    /// Whether the record of the latest read is vacant: absent, garbage, released, or
    /// pointing at a session that has ended on the bucket. A record pointing at our current
    /// incarnation that we do not remember holding (an ambiguous take resolved late) ⇒ adopt
    /// the role again.
    pub async fn coordinator_vacant(&self) -> Result<bool> {
        let Some(seen) = self.coordinator_seen() else {
            return Ok(false);
        };
        self.vacant(&seen).await
    }

    async fn vacant(&self, seen: &CoordinatorSeen) -> Result<bool> {
        Ok(match seen {
            CoordinatorSeen::Absent | CoordinatorSeen::Garbage => true,
            CoordinatorSeen::Newer => false,
            CoordinatorSeen::Held(r) if r.released => true,
            CoordinatorSeen::Held(r) => {
                if Some(&r.session) == self.current().as_ref() {
                    self.adopt_coordinator(r);
                    return Ok(false);
                }
                // The tracker has read (or CASed) an ended state ⇒ vacant; the tracker sees
                // it alive ⇒ no — the tracker observes it every tick, so it lags by up to one
                // tick. The tracker does not know it yet ⇒ a direct GET.
                match self.0.tracker.state_of(&r.session) {
                    Some(state) => state.is_terminal(),
                    None => self.session_is_dead(&r.session).await?,
                }
            }
        })
    }

    /// The record says our current incarnation holds the role while `held` is empty: adopt
    /// it again (the version of the latest read).
    fn adopt_coordinator(&self, r: &CoordinatorRecord) {
        let mut st = self.0.coordinator.st.lock().expect("mutex");
        // A role we have just dropped ourselves (the fence saw our session end, or we
        // released it): do not adopt it again.
        let ttl = self.0.cfg.ttl;
        if st.held.is_some()
            || st
                .released_at
                .is_some_and(|at| at.elapsed() < ttl * RETAKE_AFTER_TTLS)
        {
            return;
        }
        let Some(version) = st.last.as_ref().and_then(|(_, v)| v.clone()) else {
            return;
        };
        tracing::info!(session = %r.session, term = r.term, "adopting the coordinator role the record says is ours");
        st.held = Some(Held {
            record: r.clone(),
            version,
        });
        drop(st);
        self.0.tracker.coordinating(Some(r.term));
    }

    /// Tries to take the role: re-reads the record, and if still vacant CASes `term + 1`.
    /// `Ok(Some)` = we hold the role at that term. The schedule (who tries, after how long)
    /// is the caller's job.
    pub async fn try_coordinate(&self) -> Result<Option<u64>> {
        let Some(me) = self.current() else {
            return Ok(None);
        };
        if self.coordinating().is_some() {
            return Ok(self.coordinating());
        }
        // A full read (no `If-None-Match`): the CAS needs the version of this very read.
        let (seen, version) = match self.get_coordinator(None).await? {
            Got::Body(raw, v) => (CoordinatorRecord::decode(&raw), Some(v)),
            Got::Absent => (CoordinatorSeen::Absent, None),
            Got::NotModified => return Ok(None),
        };
        self.0.coordinator.st.lock().expect("mutex").last = Some((seen.clone(), version.clone()));
        self.0.tracker.coordinator_observed(&seen);
        if !self.vacant(&seen).await? {
            return Ok(self.coordinating());
        }
        // Our incarnation is still `live` on the bucket: a frozen node just woken up
        // (already expired without knowing it) does not take the role for a dead session.
        if !self.own_session_live().await {
            return Ok(None);
        }
        let term = match &seen {
            CoordinatorSeen::Held(r) => r.term + 1,
            _ => 1,
        };
        let record = CoordinatorRecord {
            session: me.clone(),
            term,
            address: self.0.cfg.address.clone(),
            released: false,
        };
        let mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let path = coordinator_path(&self.0.prefix);
        let put = put_resolving(&self.0.store, &path, record.encode(), mode);
        let cas =
            crate::retry::deadline(&crate::retry::ops::CONTROL_COORDINATOR, self.bound(), put)
                .await
                .map_err(StoreError::from)?
                .map_err(io)?;
        match cas {
            Cas::Written(version) => {
                tracing::info!(session = %me, term, "holding the coordinator role");
                let mut st = self.0.coordinator.st.lock().expect("mutex");
                st.last = Some((CoordinatorSeen::Held(record.clone()), Some(version.clone())));
                let seen = CoordinatorSeen::Held(record.clone());
                st.held = Some(Held { record, version });
                drop(st);
                self.0.tracker.coordinator_observed(&seen);
                self.0.tracker.coordinating(Some(term));
                Ok(self.coordinating())
            }
            Cas::Lost => Ok(None),
        }
    }

    /// **The coordinator fence** — see the module header. Call **after** the batch's
    /// heartbeats have been received. `false` ⇒ do not answer this batch; the role is dropped
    /// when the read proves it is lost (the record changed or is gone); an I/O error ⇒ only
    /// this batch misses.
    pub async fn coordinator_fence(&self) -> bool {
        let Some((held, own)) = self.fence_inputs() else {
            return false;
        };
        if !self.keeper_alive_if_solo() {
            tracing::debug!(
                term = held.record.term,
                "coordinator fence: in solo but the renewal loop has not woken for over a TTL — no ack"
            );
            return false;
        }
        let issued_at = Instant::now();
        let (rec, mine) = tokio::join!(
            self.get_coordinator(held.version.e_tag.clone()),
            self.own_session_live_since(&held.record.session, own),
        );
        // Our own session has ended on the bucket: the role goes with it (`lose` follows
        // later, in the renewal loop).
        if mine == Some(false) {
            let mut st = self.0.coordinator.st.lock().expect("mutex");
            if st.held.take().is_some() {
                tracing::info!(
                    term = held.record.term,
                    "our session has ended — giving up the coordinator role"
                );
                st.released_at = Some(Instant::now());
            }
        }
        let mine = mine == Some(true);
        let rec_ok = match rec {
            Ok(Got::NotModified) => true,
            Ok(Got::Body(raw, version)) => {
                let seen = CoordinatorRecord::decode(&raw);
                self.0.tracker.coordinator_observed(&seen);
                let still = matches!(&seen, CoordinatorSeen::Held(r)
                    if r.session == held.record.session && r.term == held.record.term && !r.released);
                let mut st = self.0.coordinator.st.lock().expect("mutex");
                st.last = Some((seen, Some(version.clone())));
                if still {
                    if let Some(h) = st.held.as_mut() {
                        h.version = version;
                    }
                } else {
                    tracing::info!(
                        term = held.record.term,
                        "the coordinator record has changed — giving up the role"
                    );
                    st.held = None;
                    st.released_at = Some(Instant::now());
                }
                still
            }
            Ok(Got::Absent) => {
                tracing::info!(
                    term = held.record.term,
                    "the coordinator record has disappeared — giving up the role"
                );
                let mut st = self.0.coordinator.st.lock().expect("mutex");
                st.last = Some((CoordinatorSeen::Absent, None));
                st.held = None;
                st.released_at = Some(Instant::now());
                false
            }
            Err(e) => {
                tracing::debug!(error = %e, "coordinator fence: cannot read the record");
                false
            }
        };
        // The incarnation may change while the GET is in flight.
        let passed = rec_ok && mine && self.coordinating() == Some(held.record.term);
        if passed {
            self.0.tracker.fenced(issued_at);
        } else if self.coordinating().is_none() {
            self.0.tracker.coordinating(None);
        }
        passed
    }

    /// In `solo`, our renewal loop has woken within the last TTL. Only that loop answers
    /// challenges; nodes delegating to us postpone their challenge as long as we keep acking
    /// (`SessionTracker::lean_on`), so if the renewal loop dies (task dead, stuck) while we
    /// keep acking, nobody challenges and our ranges are stuck. Stop acking ⇒ the node falls
    /// back to `direct` after TTL/3 ⇒ challenge ⇒ expired after a TTL. Like the self-touch
    /// rule of a delegated node; it only reduces acks — safety is untouched (model M1: an
    /// ack is an over-approximation). Not in `solo` ⇒ always passes (a `direct` record goes
    /// by LS-1).
    fn keeper_alive_if_solo(&self) -> bool {
        let solo = {
            let st = self.0.state.lock().expect("mutex");
            st.current.as_ref().is_some_and(|c| c.solo)
        };
        !solo
            || self
                .0
                .keeper_tick
                .lock()
                .expect("mutex")
                .is_some_and(|at| at.elapsed() < self.0.cfg.ttl)
    }

    /// The role held under the current incarnation, and the etag of that incarnation's last
    /// renewal.
    fn fence_inputs(&self) -> Option<(Held, Option<String>)> {
        let st = self.0.state.lock().expect("mutex");
        let cur = st.current.as_ref()?;
        let role = self.0.coordinator.st.lock().expect("mutex");
        let held = role.held.as_ref().filter(|h| h.record.session == cur.id)?;
        Some((held.clone(), cur.version.e_tag.clone()))
    }

    /// A role still recorded under a dropped incarnation (session lost, a new incarnation
    /// opened): release it (the role does not move to the new incarnation).
    pub async fn drop_stale_coordinator(&self) {
        let current = self.current();
        let stale = {
            let st = self.0.coordinator.st.lock().expect("mutex");
            st.held
                .as_ref()
                .is_some_and(|h| Some(&h.record.session) != current.as_ref())
        };
        if stale {
            self.release_coordinator().await;
        }
    }

    /// Releases the role (a CAS of `released` on our version), best effort, bounded. Sets
    /// the no-retake mark. Not holding the role ⇒ nothing to do.
    pub async fn release_coordinator(&self) {
        let held = {
            let mut st = self.0.coordinator.st.lock().expect("mutex");
            let held = st.held.take();
            if held.is_some() {
                st.released_at = Some(Instant::now());
            }
            held
        };
        let Some(held) = held else { return };
        self.0.tracker.coordinating(None);
        let record = CoordinatorRecord {
            released: true,
            ..held.record.clone()
        };
        let path = coordinator_path(&self.0.prefix);
        let put = put_resolving(
            &self.0.store,
            &path,
            record.encode(),
            PutMode::Update(held.version),
        );
        match crate::retry::deadline(&crate::retry::ops::CONTROL_COORDINATOR, self.bound(), put)
            .await
        {
            Ok(Ok(Cas::Written(_))) => {
                tracing::info!(session = %held.record.session, term = held.record.term, "released the coordinator role")
            }
            Ok(Ok(Cas::Lost)) => {
                tracing::debug!(term = held.record.term, "release: record already changed")
            }
            Ok(Err(e)) => tracing::debug!(error = %e, "releasing the coordinator role failed"),
            Err(e) => tracing::debug!(error = %e, "releasing the coordinator role timed out"),
        }
    }

    /// The bound of one I/O of the role: TTL/3 — any longer and the decision would be late
    /// anyway.
    fn bound(&self) -> Duration {
        self.0.cfg.ttl / 3
    }

    async fn get_coordinator(&self, etag: Option<String>) -> Result<Got> {
        let path = coordinator_path(&self.0.prefix);
        let opts = GetOptions {
            if_none_match: etag,
            ..Default::default()
        };
        let get = async {
            match self.0.store.get_opts(&path, opts).await {
                Err(OsError::NotModified { .. }) => Ok(Got::NotModified),
                Err(OsError::NotFound { .. }) => Ok(Got::Absent),
                Err(e) => Err(e),
                Ok(got) => {
                    let version = UpdateVersion {
                        e_tag: got.meta.e_tag.clone(),
                        version: got.meta.version.clone(),
                    };
                    Ok(Got::Body(got.bytes().await?, version))
                }
            }
        };
        crate::retry::deadline(&crate::retry::ops::CONTROL_COORDINATOR, self.bound(), get)
            .await
            .map_err(StoreError::from)?
            .map_err(io)
    }

    /// Whether the current incarnation's session record on the bucket is still our `live`
    /// one.
    async fn own_session_live(&self) -> bool {
        let inputs = {
            let st = self.0.state.lock().expect("mutex");
            st.current
                .as_ref()
                .map(|c| (c.id.clone(), c.version.e_tag.clone()))
        };
        match inputs {
            Some((id, etag)) => self.own_session_live_since(&id, etag).await == Some(true),
            None => false,
        }
    }

    /// A GET with `If-None-Match` `etag` of session record `id`: `304`, or `200` carrying
    /// `live` + our node ⇒ `Some(true)`; ended, unknown or gone ⇒ `Some(false)`; failed or
    /// timed out ⇒ `None`.
    async fn own_session_live_since(&self, id: &SessionId, etag: Option<String>) -> Option<bool> {
        let path = session_path(&self.0.prefix, id);
        let opts = GetOptions {
            if_none_match: etag,
            ..Default::default()
        };
        let get = async {
            match self.0.store.get_opts(&path, opts).await {
                Err(OsError::NotModified { .. }) => Ok(true),
                Ok(got) => {
                    let raw = got.bytes().await?;
                    Ok(SessionRecord::decode(&raw).is_ok_and(|r| {
                        r.state == SessionState::Live && r.node == self.0.cfg.node_id
                    }))
                }
                Err(OsError::NotFound { .. }) => Ok(false),
                Err(e) => Err(e),
            }
        };
        match crate::retry::deadline(&crate::retry::ops::CONTROL_COORDINATOR, self.bound(), get)
            .await
        {
            Ok(Ok(live)) => {
                if !live {
                    // The renewal loop will miss its CAS and `lose`.
                    self.0.wake.notify_one();
                }
                Some(live)
            }
            Ok(Err(e)) => {
                tracing::debug!(session = %id, error = %e, "cannot read our own session record");
                None
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule},
        slatedb::object_store::memory::InMemory,
    };

    const TTL: Duration = Duration::from_millis(300);

    async fn session_on(store: &Arc<dyn ObjectStore>, node: &str) -> Session {
        let cfg = LeaseConfig::new()
            .with_node_id(node)
            .with_ttl(TTL)
            .with_renew_interval(Duration::from_millis(50))
            .with_address(format!("http://{node}"));
        Session::establish(Arc::clone(store), &Path::from(""), cfg)
            .await
            .unwrap()
    }

    async fn on_bucket(store: &Arc<dyn ObjectStore>) -> CoordinatorSeen {
        match store.get(&coordinator_path(&Path::from(""))).await {
            Ok(got) => CoordinatorRecord::decode(&got.bytes().await.unwrap()),
            Err(OsError::NotFound { .. }) => CoordinatorSeen::Absent,
            Err(e) => panic!("{e}"),
        }
    }

    fn held_by(seen: &CoordinatorSeen) -> Option<(SessionId, u64, bool)> {
        match seen {
            CoordinatorSeen::Held(r) => Some((r.session.clone(), r.term, r.released)),
            _ => None,
        }
    }

    /// Absent ⇒ the first taker gets term 1; others cannot take the role of a live session;
    /// released ⇒ someone else takes term 2; whoever just released does not take it back
    /// within [`RETAKE_AFTER_TTLS`] TTLs.
    #[tokio::test]
    async fn terms_count_up_and_the_role_sticks_until_released() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        let b = session_on(&store, "b").await;
        assert_eq!(a.try_coordinate().await.unwrap(), Some(1));
        assert_eq!(a.coordinating(), Some(1));
        b.read_coordinator().await.unwrap();
        assert!(
            !b.coordinator_vacant().await.unwrap(),
            "role of a live session"
        );
        assert_eq!(b.try_coordinate().await.unwrap(), None);
        assert!(
            a.coordinator_fence().await,
            "the role holder's fence must pass"
        );

        a.release_coordinator().await;
        assert_eq!(a.coordinating(), None);
        assert!(a.released_coordinator_within(TTL * RETAKE_AFTER_TTLS));
        assert_eq!(
            held_by(&on_bucket(&store).await),
            Some((a.current().unwrap(), 1, true))
        );
        assert_eq!(b.try_coordinate().await.unwrap(), Some(2));
        assert!(!a.coordinator_fence().await, "a no longer has the role");
        let b_id = b.current().unwrap();
        a.close().await;
        b.close().await;
        assert_eq!(
            held_by(&on_bucket(&store).await),
            Some((b_id, 2, true)),
            "close releases the role"
        );
    }

    /// Rule 7: a take and a release that reached the store while the answer was ambiguous
    /// (`Precondition` from an automatic object_store retry, or an error) — a re-read finding
    /// exactly our content ⇒ we won; no second take with another term.
    #[tokio::test]
    async fn ambiguous_take_and_release_are_resolved_by_reading_back() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let a = session_on(&store, "a").await;
            let ambiguous = || {
                Rule::new(
                    OpSet::PUT,
                    PathMatch::Contains(COORDINATOR_OBJECT.into()),
                    Action::Ambiguous {
                        reply,
                        skip: 0,
                        times: Some(1),
                    },
                )
            };
            let r = faulty.add_rule(ambiguous());
            assert_eq!(a.try_coordinate().await.unwrap(), Some(1), "{reply:?}");
            faulty.remove_rule(r);
            let r = faulty.add_rule(ambiguous());
            a.release_coordinator().await;
            faulty.remove_rule(r);
            assert!(
                matches!(on_bucket(&store).await, CoordinatorSeen::Held(r) if r.released && r.term == 1),
                "{reply:?}: the release reached the store"
            );
            let b = session_on(&store, "b").await;
            assert_eq!(b.try_coordinate().await.unwrap(), Some(2), "{reply:?}");
            a.close().await;
            b.close().await;
        }
    }

    /// The fence: the record changed to someone else ⇒ give up the role; the incarnation
    /// changed ⇒ the role does not move to the new incarnation, and the old role is released.
    #[tokio::test]
    async fn fence_drops_the_role_when_the_record_moves_or_the_incarnation_changes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        assert_eq!(a.try_coordinate().await.unwrap(), Some(1));
        // Someone else (cairn-ctl, a newer binary) overwrites the record.
        let other = CoordinatorRecord {
            session: SessionId {
                node: "z".into(),
                inc: 1,
            },
            term: 2,
            address: None,
            released: false,
        };
        store
            .put(&coordinator_path(&Path::from("")), other.encode().into())
            .await
            .unwrap();
        assert!(!a.coordinator_fence().await);
        assert_eq!(a.coordinating(), None);
        assert!(a.released_coordinator_within(TTL * RETAKE_AFTER_TTLS));

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        assert_eq!(a.try_coordinate().await.unwrap(), Some(1));
        let old = a.current().unwrap();
        a.fail_stop(&old, crate::failstop::Cause::WalStall, "test")
            .await;
        let t0 = Instant::now();
        while a.current().is_none_or(|c| c == old) {
            assert!(t0.elapsed() < TTL * 10, "no new incarnation opened yet");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(a.coordinating(), None, "role stays off the new incarnation");
        assert!(!a.coordinator_fence().await);
        a.drop_stale_coordinator().await;
        assert!(
            matches!(on_bucket(&store).await, CoordinatorSeen::Held(r) if r.released && r.session == old),
            "the old incarnation's role is released"
        );
        a.close().await;
    }

    /// A record pointing at a session that has ended on the bucket ⇒ vacant; an incarnation
    /// already expired without knowing it (a frozen node just woken up) does not take it.
    #[tokio::test]
    async fn ended_holder_frees_the_role_and_an_expired_self_does_not_take_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(&store, "a").await;
        let b = session_on(&store, "b").await;
        assert_eq!(a.try_coordinate().await.unwrap(), Some(1));
        let a_id = a.current().unwrap();
        a.abort();
        // b observes a silent for longer than a TTL ⇒ CASes `expired`.
        let t0 = Instant::now();
        while !b.tracker().is_dead(&a_id) {
            assert!(t0.elapsed() < TTL * 10, "b has not expired a yet");
            b.tracker().refresh().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        b.read_coordinator().await.unwrap();
        assert!(b.coordinator_vacant().await.unwrap());
        // a wakes up (renewal loop dead, `current` still there): the fence sees its own
        // session has ended ⇒ gives up the role, and does not take it back.
        assert!(!a.coordinator_fence().await);
        assert_eq!(a.coordinating(), None);
        assert_eq!(
            a.try_coordinate().await.unwrap(),
            None,
            "a's session has ended"
        );
        assert_eq!(b.try_coordinate().await.unwrap(), Some(2));
        b.close().await;
    }

    fn rec(node: &str, inc: u64, term: u64, released: bool) -> CoordinatorRecord {
        CoordinatorRecord {
            session: SessionId {
                node: node.into(),
                inc,
            },
            term,
            address: Some("http://10.0.0.1:9000".into()),
            released,
        }
    }

    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy(COORDINATOR_OBJECT).map(|p| p.header),
            Some(COORDINATOR_HEADER)
        );
    }

    #[test]
    fn round_trips_and_reads_leniently() {
        for r in [rec("a/b", 7, 3, false), rec("x", 1, 1, true)] {
            assert_eq!(
                CoordinatorRecord::decode(&r.encode()),
                CoordinatorSeen::Held(r)
            );
        }
        let lenient = format!("{COORDINATOR_HEADER}\nsession=a/2 term=5 odd=1\njunk\n");
        assert_eq!(
            CoordinatorRecord::decode(lenient.as_bytes()),
            CoordinatorSeen::Held(CoordinatorRecord {
                address: None,
                ..rec("a", 2, 5, false)
            })
        );
        assert_eq!(
            CoordinatorRecord::decode(b"cairnflow-coordinator/2\nsession=a/1\nterm=1\n"),
            CoordinatorSeen::Newer
        );
        assert_eq!(
            CoordinatorRecord::decode(b"not a header\n"),
            CoordinatorSeen::Garbage
        );
        assert_eq!(
            CoordinatorRecord::decode(format!("{COORDINATOR_HEADER}\nterm=1\n").as_bytes()),
            CoordinatorSeen::Garbage
        );
    }
}
