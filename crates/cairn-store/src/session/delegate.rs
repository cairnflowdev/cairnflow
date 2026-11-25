// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Delegated renewal** (LS-14): a node stops PUTting session renewals to the bucket every
//! TTL/5 and renews its grant by the coordinator's **ack** over the heartbeat instead.
//!
//! # Entering the mode
//!
//! Coordinator `C` (term `t`) returns `Grant` to our heartbeat (the `control/quiet-lease`
//! gate is on, every session knows the `lease/quiet` step, our record is `direct`). The
//! keeper (`renew.rs:keeper_loop` — **every** mode-changing CAS goes through it,
//! sequentially) CASes the record into `mode=delegated coord=C:t`, `beat+1`. That CAS is an
//! ordinary `direct` renewal (LS-2). Only once it is **confirmed** (won, or ambiguous and the
//! re-read finds ours — `adopt`) does `Current::delegated` exist, and only from then on can
//! acks renew the grant.
//!
//! # Renewing by ack
//!
//! A heartbeat sent at `t_sent` (our clock, **before** the first attempt) reports the beat
//! and etag of the confirmed delegated record. The coordinator returns `Ack` when the
//! record it reads is exactly that version, delegated to exactly its role, it has issued no
//! `expired` CAS for us, and we have just touched the bucket (ack rules 1–6). An ack
//! renews the grant until `t_sent + ⅔·TTL` ([`Session::on_lease_answer`]) — **only** while
//! we still delegate to exactly `C:t` at exactly that beat. A revoked grant (after `lose`:
//! past the suspension cap, or a self-check GET finding the record ended) is never revived
//! by an ack (`ValidityHandle::extend_until` refuses a revoked or capped root).
//!
//! # Self-checking the bucket
//!
//! Every renewal tick, one GET with `If-None-Match` of our own record (merged with the read
//! fence loop): `304`, or exactly the content we wrote ⇒ a touch of the bucket
//! (`Current::touched_at`, whose age travels in the heartbeat); ended, gone, or changed by
//! someone else ⇒ `lose`. That GET does **not** renew the grant — only acks renew.
//! Exception (rule 7): a record read back that is our own ambiguous CAS (same beat) ⇒ adopt
//! it like `renew_once`'s re-read branch, no `lose`.
//!
//! # Falling back to `direct`
//!
//! The reference is the `t_sent` of the last acked heartbeat (or when the delegating CAS was
//! issued). Past reference + TTL/3 + jitter (≤ TTL/10) with no new ack, or when the
//! coordinator returns `Fallback`/`Expired`: the keeper CASes the record back to `direct`,
//! `beat+1` — an LS-2 renewal —, and runs today's renewal loop again. After falling back we
//! stay in `direct` for at least [`REDELEGATE_AFTER_TTLS`] TTLs before delegating again.
//!
//! Our CASes while delegated (`owns`, `draining`) do **not** renew the grant (redundant for
//! safety when observers recount by version, kept that way because it is cheap).

use {super::*, crate::failstop::Cause};

/// After a fallback, stay in `direct` at least this many TTLs before delegating again.
pub const REDELEGATE_AFTER_TTLS: u32 = 2;

/// The coordinator's word about our lease in a heartbeat answer (the two kinds are separate
/// on the wire — `Grant` only **permits** entering the mode, only `Ack` renews).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseAnswer {
    /// Permission to delegate (we are `direct`).
    Grant,
    /// Renewal: our delegated record, at the version we reported, has been confirmed.
    Ack,
    /// The coordinator has issued an `expired` CAS for us, or reads that we have ended.
    Expired,
    /// Back to `direct` (gate off, an old binary in the cluster, the coordinator declines).
    Fallback,
}

/// The record on the bucket is our **confirmed** `delegated` record.
#[derive(Clone, Debug)]
pub(super) struct Delegation {
    pub(super) coord: SessionId,
    pub(super) term: u64,
    /// Beat of the confirmed delegating CAS: a heartbeat reporting a smaller beat was sent
    /// **before** the delegation was confirmed — its ack does not renew.
    pub(super) entered_beat: u64,
    /// The fallback reference: when the confirmed delegating CAS was issued, or the `t_sent`
    /// of the last acked heartbeat.
    pub(super) base: Instant,
    /// Fallback jitter (≤ TTL/10), drawn once per entry into the mode.
    pub(super) jitter: Duration,
}

impl Delegation {
    pub(super) fn fallback_at(&self, ttl: Duration) -> Instant {
        self.base + ttl / 3 + self.jitter
    }
}

/// Coordinator messages waiting for the keeper.
#[derive(Default)]
pub(super) struct Inbox {
    /// The latest `Grant`: coordinator, term, when received.
    pub(super) offer: Option<(SessionId, u64, Instant)>,
    /// An unprocessed `Fallback`/`Expired`.
    pub(super) refused: Option<LeaseAnswer>,
    /// The latest fallback ([`REDELEGATE_AFTER_TTLS`]).
    pub(super) fell_back_at: Option<Instant>,
}

impl Session {
    /// The word of coordinator `coord` (term `term`) for the heartbeat we sent at `sent`
    /// reporting beat `beat` — see the module header. No I/O: an `Ack` moves the deadline at
    /// once; the rest is left for the keeper.
    pub fn on_lease_answer(
        &self,
        coord: &SessionId,
        term: u64,
        answer: LeaseAnswer,
        sent: Instant,
        beat: u64,
    ) {
        let inner = &self.0;
        match answer {
            LeaseAnswer::Ack => {
                let mut st = inner.state.lock().expect("mutex");
                let Some(c) = st.current.as_mut() else { return };
                let Some(d) = c.delegated.as_mut() else {
                    return;
                };
                // Only ack for exactly the role we delegated to, for a heartbeat sent
                // **after** that delegation was confirmed (beat ≥ the beat of the CAS into
                // the mode). A heartbeat reporting a version older than the current one (we
                // just wrote `owns`) can still renew: that write keeps the mode, and every
                // observer recounts from the new version (model M1 lets an ack renew whatever
                // version the coordinator holds).
                if &d.coord != coord || d.term != term || beat < d.entered_beat || beat > c.beat {
                    return;
                }
                let paused = !c.root.is_valid();
                // A revoked root (after `lose`) or past the cap ⇒ `false`, not moved.
                if c.root.extend_until(renew_deadline(sent, inner.cfg.ttl)) {
                    d.base = d.base.max(sent);
                    if paused && c.root.is_valid() {
                        pause_stats::REVIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::info!(session = %c.id, "coordinator ack — revived");
                    }
                } else {
                    drop(st);
                    inner.wake.notify_one();
                }
            }
            LeaseAnswer::Grant => {
                let delegated = self.delegated_to();
                let ready = {
                    let mut inbox = inner.delegation.lock().expect("mutex");
                    inbox.offer = Some((coord.clone(), term, Instant::now()));
                    inbox
                        .fell_back_at
                        .is_none_or(|at| at.elapsed() >= inner.cfg.ttl * REDELEGATE_AFTER_TTLS)
                };
                // Nudge the keeper only when the next write will really delegate (or
                // delegate again to a new role) — otherwise one PUT per heartbeat.
                if ready && delegated != Some((coord.clone(), term)) {
                    inner.wake.notify_one();
                }
            }
            LeaseAnswer::Expired | LeaseAnswer::Fallback => {
                let mut inbox = inner.delegation.lock().expect("mutex");
                inbox.offer = None;
                if self.delegated_to().is_some() {
                    inbox.refused = Some(answer);
                    drop(inbox);
                    inner.wake.notify_one();
                }
            }
        }
    }

    /// Who we currently delegate to (confirmed), and at which term.
    pub fn delegated_to(&self) -> Option<(SessionId, u64)> {
        let st = self.0.state.lock().expect("mutex");
        st.current
            .as_ref()
            .and_then(|c| c.delegated.as_ref())
            .map(|d| (d.coord.clone(), d.term))
    }

    /// Age of the latest successful touch of the bucket — travels in the heartbeat.
    pub fn touched_age(&self) -> Option<Duration> {
        let st = self.0.state.lock().expect("mutex");
        st.current
            .as_ref()
            .and_then(|c| c.touched_at)
            .map(|at| at.elapsed())
    }
}

/// Whether the next write should delegate, and to whom: there is a fresh (TTL/3) `Grant`
/// from a coordinator other than us, we are not `solo`, not draining, have been in `direct`
/// long enough since the previous fallback, and the grant is valid.
pub(super) fn want_delegate(inner: &Inner, root_valid: bool) -> Option<(SessionId, u64)> {
    // A draining node (LS-11) in `direct`: its PUT renewals are the samples it recovers by.
    if !root_valid || published(inner).draining {
        return None;
    }
    let me = inner
        .state
        .lock()
        .expect("mutex")
        .current
        .as_ref()?
        .id
        .clone();
    let inbox = inner.delegation.lock().expect("mutex");
    let ttl = inner.cfg.ttl;
    if inbox
        .fell_back_at
        .is_some_and(|at| at.elapsed() < ttl * REDELEGATE_AFTER_TTLS)
    {
        return None;
    }
    let (coord, term, at) = inbox.offer.clone()?;
    (at.elapsed() < ttl / 3 && coord != me).then_some((coord, term))
}

/// Takes the pending refusal (if any).
pub(super) fn take_refusal(inner: &Inner) -> Option<LeaseAnswer> {
    inner.delegation.lock().expect("mutex").refused.take()
}

/// Just fell back to `direct`: remember when, drop the old permission.
pub(super) fn fell_back(inner: &Inner) {
    let mut inbox = inner.delegation.lock().expect("mutex");
    inbox.fell_back_at = Some(Instant::now());
    inbox.offer = None;
    inbox.refused = None;
}

/// The self-check GET in delegated mode: a touch of the bucket; finding the record ended or
/// changed ⇒ lost. No renewal — unless the record read back is our own ambiguous CAS
/// (`unconfirmed`, rule 7): then adopt it like `renew_once`'s re-read branch.
pub(super) async fn delegated_check(
    inner: &Arc<Inner>,
    cap: Instant,
    unconfirmed: &mut Option<(u64, Instant)>,
) -> Renewal {
    let (id, etag, written, beat) = {
        let st = inner.state.lock().expect("mutex");
        match st.current.as_ref() {
            Some(c) if c.delegated.is_some() => (
                c.id.clone(),
                c.version.e_tag.clone(),
                c.written.clone(),
                c.beat,
            ),
            _ => return Renewal::Unknown,
        }
    };
    let path = session_path(&inner.prefix, &id);
    let issued_at = Instant::now();
    let opts = slatedb::object_store::GetOptions {
        if_none_match: etag.clone(),
        ..Default::default()
    };
    let get = async {
        match inner.store.get_opts(&path, opts).await {
            Err(OsError::NotModified { .. }) => Ok(None),
            Ok(got) => {
                let version = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                got.bytes().await.map(|raw| Some((raw, version)))
            }
            Err(e) => Err(e),
        }
    };
    let bound = (issued_at + inner.cfg.ttl / 3).min(cap);
    match tokio::time::timeout_at(bound, get).await {
        Ok(Ok(None)) => {
            touch(inner, &id, etag.as_deref(), issued_at);
            Renewal::Done
        }
        Ok(Ok(Some((raw, version)))) => match SessionRecord::decode(&raw) {
            Ok(rec) if rec.state.is_terminal() => {
                revoked_while_valid(inner, &id, rec.state);
                Renewal::Lost(Cause::SessionLost, "session has been marked ended", false)
            }
            // Exactly the content we wrote (the store changed the etag without changing
            // the content).
            Ok(rec) if rec == written => {
                {
                    let mut st = inner.state.lock().expect("mutex");
                    if let Some(c) = st.current.as_mut().filter(|c| c.id == id) {
                        c.version = version.clone();
                    }
                }
                touch(inner, &id, version.e_tag.as_deref(), issued_at);
                Renewal::Done
            }
            // Our record carries the beat of our ambiguous CAS (fallback, keeping the mode,
            // delegating again — an `Err` or the `session::renew::after_put` failpoint): our
            // write has been applied. The next round usually CASes again, misses, and the
            // re-read recognises it; but an ack arriving while the fallback CAS is in flight
            // moves the fallback reference, so the next round gets here first (seen in a
            // chaos run). Adopt it like `renew_once`'s re-read branch: same beat, the
            // reference is the earliest ambiguous issue.
            Ok(rec)
                if rec.node == inner.cfg.node_id
                    && unconfirmed.is_some_and(|(b, _)| b == rec.beat && b > beat) =>
            {
                let (b, at) = unconfirmed.take().expect("just checked");
                tracing::info!(session = %id, beat = b, mode = ?rec.mode, "the self-check GET found our own ambiguous CAS applied — adopting it");
                let r = super::renew::adopt(inner, &id, version, b, at, &rec);
                let delegated = {
                    let st = inner.state.lock().expect("mutex");
                    st.current.as_ref().is_some_and(|c| c.delegated.is_some())
                };
                if !delegated && !matches!(r, Renewal::Lost(..)) {
                    fell_back(inner);
                }
                r
            }
            // Changed by someone else (an observer's overwrite): our next CAS will miss —
            // lost, as in `direct`.
            _ => Renewal::Lost(
                Cause::SessionLost,
                "the delegated record was changed by someone else",
                false,
            ),
        },
        Ok(Err(OsError::NotFound { .. })) => {
            Renewal::Lost(Cause::SessionLost, "the session disappeared", false)
        }
        Ok(Err(e)) => {
            tracing::debug!(session = %id, error = %e, "self-check GET (delegated) failed");
            Renewal::Unknown
        }
        Err(_) => Renewal::Unknown,
    }
}

/// Tripwire counter of the delegated mode — metric
/// `cairnflow_lease_delegated_revoked_while_valid_total`.
pub mod stats {
    use std::sync::atomic::AtomicU64;

    /// A delegated node's self-check GET found its own record ended while the grant was
    /// still valid by its clock. This must never happen if the delegation rules hold (the
    /// coordinator expires one TTL after the last heartbeat it acked; an ack only moves the
    /// deadline to `t_sent + ⅔·TTL`): seeing it is a protocol bug (or a clock rate off by more
    /// than 1.5×). Chaos and soak runs treat > 0 as FAIL.
    pub static REVOKED_WHILE_VALID: AtomicU64 = AtomicU64::new(0);
}

/// **The release-build tripwire**: our record has ended (expired by the coordinator or a
/// deferring observer, or `closed`) while the grant is still valid. Revoke at once — even
/// before the keeper's `lose` —, count, log at `error`. Runs in every build, not a
/// failpoint.
fn revoked_while_valid(inner: &Inner, id: &SessionId, state: SessionState) {
    let root = {
        let st = inner.state.lock().expect("mutex");
        st.current
            .as_ref()
            .filter(|c| &c.id == id && c.delegated.is_some())
            .map(|c| c.root.clone())
    };
    let Some(root) = root.filter(|r| r.is_valid()) else {
        return;
    };
    root.revoke();
    stats::REVOKED_WHILE_VALID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tracing::error!(
        session = %id,
        state = ?state,
        "DELEGATION PROTOCOL ERROR: our record ended while the grant was still valid — revoking at once"
    );
}

/// A GET (by the renewal loop or the read fence) issued at `issued_at` found our record
/// unchanged (`etag`) — a touch of the bucket, and the keeper's self-check tick.
pub(super) fn touch(inner: &Inner, id: &SessionId, etag: Option<&str>, issued_at: Instant) {
    let mut st = inner.state.lock().expect("mutex");
    if let Some(c) = st
        .current
        .as_mut()
        .filter(|c| &c.id == id && c.version.e_tag.as_deref() == etag)
    {
        c.touched_at = Some(c.touched_at.map_or(issued_at, |at| at.max(issued_at)));
        if c.delegated.is_some() {
            c.checked_at = Some(c.checked_at.map_or(issued_at, |at| at.max(issued_at)));
        }
    }
}

/// Reads the `control/quiet-lease` gate (cached for [`LeaseConfig::quiet_lease_poll`]) —
/// the **only** path, for both the renewal loop (solo mode) and the coordinator: it updates
/// both "gate on" and "delegation on" from the same read. Failure ≡ off.
pub(super) async fn gate_poll(inner: &Inner) -> bool {
    let (on, checked) = *inner.gate_seen.lock().expect("mutex");
    if checked.is_some_and(|at| at.elapsed() < inner.cfg.quiet_lease_poll) {
        return on;
    }
    let poll = inner.gate.poll_state(&inner.store, &inner.prefix);
    let (on, delegation) = tokio::time::timeout(inner.cfg.ttl / 3, poll)
        .await
        .unwrap_or((false, false));
    *inner.gate_seen.lock().expect("mutex") = (on, Some(Instant::now()));
    *inner.gate_delegation.lock().expect("mutex") = delegation;
    on
}

/// A slowness score from this value up (scale 1–100) makes the coordinator consider itself
/// "weak" (expire rule 3).
pub const WEAK_SLOW_SCORE: u32 = 100;

impl Session {
    /// Whether the `control/quiet-lease` gate is on — re-read (GET with `If-None-Match`) when
    /// the previous read is older than [`LeaseConfig::quiet_lease_poll`]; failure ⇒ off. For
    /// the coordinator (the gate's runtime rule).
    pub async fn quiet_lease_on(&self) -> bool {
        gate_poll(&self.0).await
    }

    /// Drops the gate cache: the next read GETs again at once, without waiting
    /// [`LeaseConfig::quiet_lease_poll`]. The builder of a new bucket calls it after writing
    /// the gate (`migrate::run_cluster_with`) — its session may already have read the gate
    /// while it was still absent.
    pub fn recheck_quiet_lease(&self) {
        self.0.gate_seen.lock().expect("mutex").1 = None;
    }

    /// Delegation enabled on the gate (`delegation=on`) according to the latest gate read of
    /// [`Self::quiet_lease_on`] — no I/O. The coordinator only delegates when both are on.
    pub fn delegation_on(&self) -> bool {
        *self.0.gate_delegation.lock().expect("mutex")
    }

    /// We are "weak" (the coordinator's expire rule 3, as in Lifeguard): the current
    /// incarnation's grant is suspended (a late renewal), or the slowness score is at the cap.
    pub fn weak(&self) -> bool {
        let paused = {
            let st = self.0.state.lock().expect("mutex");
            st.current.as_ref().is_some_and(|c| !c.root.is_valid())
        };
        paused
            || published(&self.0)
                .slow
                .is_some_and(|s| s >= WEAK_SLOW_SCORE)
    }
}

impl Session {
    /// An ack just came back from coordinator `holder` at term `term`: what we know about
    /// `control/coordinator` (the source of the deferral rule, LS-15) — a node with a fresh
    /// ack does not re-read the record.
    pub fn heard_coordinator(&self, holder: &SessionId, term: u64) {
        self.0
            .tracker
            .coordinator_observed(&CoordinatorSeen::Held(CoordinatorRecord {
                session: holder.clone(),
                term,
                address: None,
                released: false,
            }));
    }
}
