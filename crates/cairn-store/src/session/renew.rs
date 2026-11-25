// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The session's background loops: owner watch, opening incarnations, renewal
//! (`keeper_loop`, `renew_once`), solo mode (`self_check`, `answer`), losing the session,
//! releasing ranges.
//!
//! # Solo mode (LS-16)
//!
//! The node is the **only** unfinished session on the bucket (`SessionTracker::alone`,
//! after a LIST since the incarnation opened) and the `control/quiet-lease` gate is on ⇒ the
//! next renewal writes `mode=solo`, `beat+1` — an ordinary `direct` renewal (LS-2). Only once
//! that CAS is **confirmed** (won, or ambiguous and the re-read finds exactly our record,
//! rule 7) does the renewal loop stop PUTting (`Current::solo`). From then on every renewal
//! tick is a GET with `If-None-Match` of our own record (`self_check`): issued at `t`,
//! returning `304` (compared with the etag of the confirmed `solo` record) or `200` with
//! **exactly** the content we last wrote ⇒ the deadline moves to `t + ⅔·TTL`. The read fence
//! loop can merge that GET (`solo_extend` from `fence::confirm`). Correct because a newcomer
//! never expires a `solo` record by silence: it **challenges** (the `challenged=` line,
//! changing the etag) and then waits one TTL from when the challenging CAS was confirmed; a
//! GET seeing "nobody has challenged" at `t` means the challenge applies after `t`, so
//! `t + ⅔·TTL < τ + TTL`.
//!
//! A GET seeing a record different from what we wrote (a `challenged` line, an overwrite,
//! undecodable) ⇒ **no** renewal, and an **answer** (`answer`): a CAS If-Match the new etag
//! back to `direct`, `beat+1`, dropping the `challenged` line — if that CAS wins it is an
//! LS-2 renewal like any other. Our own CAS (keeping `solo` when `owns` changes, or leaving
//! the mode) missing because a challenge applied before it is also answered: the re-read
//! finds our record, an old beat, carrying the `challenged` line ⇒ CAS back to `direct` from
//! the version just read (`renew_once`, the `Contested` branch), no `lose`. Finding the
//! record ended or gone ⇒ `lose`. A failed GET ⇒ no renewal: the grant is suspended and
//! then lost at the cap, as when a renewal PUT fails. After `lose` the grant is revoked for
//! good — `solo_extend` only moves the deadline of the current incarnation.
//!
//! Leave `solo` (one CAS back to `direct`) on learning of another session (a challenger
//! naming itself, the LIST safety net) or when the gate turns off. `owns`/`draining`
//! changing while alone ⇒ one CAS keeping `mode=solo`, `beat+1` — a win is a renewal.

use {super::*, crate::failstop::Cause};

/// The owner-record watch loop — see [`Session::establish`]. Runs on a report
/// ([`Session::owner_changed`]: only the reported ranges) and on every safety-net tick
/// ([`LeaseConfig::owner_watch`], default [`super::DEFAULT_OWNER_WATCH`]: every range we
/// hold). Waits on `hints` — a separate `Arc` — so as not to keep `Inner` alive.
pub(super) async fn owner_watch_loop(inner: std::sync::Weak<Inner>, hints: Arc<super::OwnerHints>) {
    let every = match inner.upgrade() {
        Some(i) => i.cfg.owner_watch.unwrap_or(super::DEFAULT_OWNER_WATCH),
        None => return,
    };
    let mut due = tokio::time::Instant::now() + every;
    loop {
        // `biased`: the order only makes the simulation deterministic.
        tokio::select! {
            biased;
            () = hints.notify.notified() => {}
            () = tokio::time::sleep_until(due) => {}
        }
        let Some(inner) = inner.upgrade() else { return };
        if *inner.stop.borrow() {
            return;
        }
        let hinted = std::mem::take(&mut *hints.ranges.lock().expect("mutex"));
        let only = if tokio::time::Instant::now() >= due {
            due = tokio::time::Instant::now() + every;
            None
        } else {
            Some(hinted)
        };
        if let Err(e) = watch_owners(&inner, only.as_ref()).await {
            tracing::debug!(error = %e, "owner record watch");
        }
    }
}

/// One watch round: for each range we hold (in `only`, if given), one conditional GET of
/// the owner record (`If-None-Match` = the version mark remembered from our last write —
/// almost always `304`); no longer our session's ⇒ revoke the grant. No LIST: holding `R`
/// ranges costs `R` GETs per safety-net tick, cheaper than one LIST per TTL until `R` is a
/// few hundred. A failed GET for one range moves on to the other ranges; the first error is
/// returned at the end.
pub(super) async fn watch_owners(inner: &Inner, only: Option<&BTreeSet<u32>>) -> Result<()> {
    let held: Vec<(u32, SessionId, Option<String>)> = {
        let mut w = inner.watched.lock().expect("mutex");
        // A suspended grant is still watched: on revival it is ours again.
        w.retain(|_, v| !v.validity.is_lost());
        w.iter()
            .filter(|(p, _)| only.is_none_or(|o| o.contains(p)))
            .map(|(p, v)| (*p, v.session.clone(), v.marker.clone()))
            .collect()
    };
    let mut failed = None;
    for (p, session, marker) in held {
        let path = owner::owner_path(&inner.prefix, PartitionId(p));
        let opts = slatedb::object_store::GetOptions {
            if_none_match: marker,
            ..Default::default()
        };
        let observed = match inner.store.get_opts(&path, opts).await {
            // Unchanged since our last write (or since the previous watch saw it ours).
            Err(OsError::NotModified { .. }) => continue,
            Err(OsError::NotFound { .. }) => None,
            Err(e) => {
                failed.get_or_insert(StoreError::LeaseIo(e.to_string()));
                continue;
            }
            Ok(got) => {
                let etag = got.meta.e_tag.clone();
                match got.bytes().await {
                    Ok(raw) => match OwnerRecord::decode(&raw) {
                        Ok(rec) => Some((rec, etag)),
                        Err(e) => {
                            failed.get_or_insert(e);
                            continue;
                        }
                    },
                    Err(e) => {
                        failed.get_or_insert(StoreError::LeaseIo(e.to_string()));
                        continue;
                    }
                }
            }
        };
        let ours = observed
            .as_ref()
            .is_some_and(|(r, _)| r.session == session && !r.released && !r.retired);
        let mut w = inner.watched.lock().expect("mutex");
        let Some(entry) = w.get_mut(&p) else { continue };
        if entry.session != session {
            continue; // claimed back while we were reading
        }
        if ours {
            entry.marker = observed.and_then(|(_, etag)| etag);
        } else {
            tracing::warn!(
                range = p,
                "the range's owner record no longer belongs to this session — revoking the grant"
            );
            entry.validity.revoke();
            w.remove(&p);
        }
    }
    failed.map_or(Ok(()), Err)
}

/// What the node publishes in the session record — empty without a meter.
pub(super) fn published(inner: &Inner) -> crate::health::Published {
    inner
        .cfg
        .meter
        .as_ref()
        .map(|m| m.published())
        .unwrap_or_default()
}

/// Writes an ended state onto incarnation `c`, **unconditionally**, bounded. Best effort: on
/// failure others mark it themselves after a silent TTL.
pub(super) async fn write_terminal(inner: &Inner, c: &Current, state: SessionState) {
    let rec = SessionRecord {
        node: inner.cfg.node_id.clone(),
        beat: c.beat + 1,
        address: inner.cfg.address.clone(),
        ingress: inner.cfg.ingress.clone(),
        knows: Some(inner.cfg.knows),
        slow: published(inner).slow,
        p99_put_ms: published(inner).p99_put_ms,
        draining: published(inner).draining,
        owns: BTreeMap::new(),
        state,
        mode: SessionMode::Direct,
        challenged: None,
    };
    let path = session_path(&inner.prefix, &c.id);
    let put = inner.store.put(&path, PutPayload::from(rec.encode()));
    match crate::retry::deadline(
        &crate::retry::ops::SESSION_TERMINAL,
        RELEASE_WRITE_BOUND,
        put,
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            tracing::debug!(session = %c.id, error = %e, "could not write the ended state")
        }
        Err(_) => tracing::debug!(session = %c.id, "writing the ended state timed out"),
    }
}

/// One I/O of [`open_incarnation`]: retried according to `policy` when given (the first
/// open at boot), once when not (the renewal loop backs off itself and opens again).
async fn open_io<T, F, Fut>(policy: Option<&crate::retry::Policy>, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, OsError>>,
{
    match policy {
        Some(p) => crate::retry::run(&crate::retry::ops::BOOT_SESSION, p, crate::retry::Never, op)
            .await
            .map_err(io),
        None => op().await.map_err(io),
    }
}

/// Opens a new incarnation: `inc` = the largest existing one (sessions, markers) + 1,
/// claimed with the marker `incarnations/<node>/<inc>`, then the session is written with
/// `Create`.
pub(super) async fn open_incarnation(
    inner: &Arc<Inner>,
    policy: Option<&crate::retry::Policy>,
) -> Result<()> {
    let node = inner.cfg.node_id.clone();
    let dir = Path::from(format!("{}/{}", sessions_dir(&inner.prefix), node));
    let listed: Vec<_> = open_io(policy, || inner.store.list(Some(&dir)).try_collect()).await?;
    // Incarnation markers (`incarnation.rs`): every `inc` ever claimed leaves its largest
    // marker on the bucket, even when its session has been cleaned up. An old bucket without
    // markers ⇒ an empty list, and only the old lower bound below remains.
    let markers_dir = incarnation::incarnations_dir(&inner.prefix, &node);
    let markers: Vec<_> = open_io(policy, || {
        inner.store.list(Some(&markers_dir)).try_collect()
    })
    .await?;
    let claimed: Vec<u64> = markers
        .iter()
        .filter_map(|m| incarnation::marker_inc(&inner.prefix, &node, &m.location))
        .collect();
    // `inc` must never be reused for the same node: owner records and the shared WAL log
    // (`nodewal/<node>/<inc>/…`) carry it as an identity. A LIST of `sessions/` only sees
    // sessions **still** on the bucket — an ended session is cleaned up after
    // `GC_AFTER_TTLS` TTLs, and a node restarting after that (same `node_id`) would open
    // `inc = 1` again: the new log has the old log's name, writes below the old log's GC
    // floor, and whoever seals that log (scanning from the floor) does not see it — acked
    // rows lost (found in simulation, seed 12). The real lower bound is the largest marker:
    // independent of the clock. The wall clock in milliseconds is only a lower bound for a
    // bucket without markers — there it is still correct unless the clock goes back by more
    // than the time since the session cleanup.
    let now_ms = crate::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    let mut inc = listed
        .iter()
        .filter_map(|m| session_id_of(&inner.prefix, &m.location))
        .filter(|id| id.node == node)
        .map(|id| id.inc)
        .chain(claimed.iter().copied())
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(now_ms);
    // One nonce for the whole open: a retry according to `policy` sends the same content,
    // so if the earlier attempt reached the store the re-read recognises it as ours (rule 7).
    let marker = incarnation::marker_body(&node);
    loop {
        let id = SessionId {
            node: node.clone(),
            inc,
        };
        // Claim `inc` **before** using it. `AlreadyExists` with content that is not ours ⇒
        // another open (of an earlier run) has used it.
        let marker_path = incarnation::marker_path(&inner.prefix, &id);
        match open_io(policy, || {
            crate::cas::put_resolving(&inner.store, &marker_path, marker.clone(), PutMode::Create)
        })
        .await?
        {
            crate::cas::Cas::Written(_) => {}
            crate::cas::Cas::Lost => {
                inc += 1;
                continue;
            }
        }
        let rec = SessionRecord {
            node: node.clone(),
            beat: 1,
            address: inner.cfg.address.clone(),
            ingress: inner.cfg.ingress.clone(),
            knows: Some(inner.cfg.knows),
            slow: published(inner).slow,
            p99_put_ms: published(inner).p99_put_ms,
            draining: published(inner).draining,
            owns: BTreeMap::new(),
            state: SessionState::Live,
            mode: SessionMode::Direct,
            challenged: None,
        };
        let issued_at = Instant::now();
        // Ambiguity resolved by re-reading (rule 7): only this node writes
        // `sessions/<node>/…`, so an `<inc>` carrying exactly our record is our write.
        // Before this, a retry's `AlreadyExists` ⇒ opening `<inc + 1>` and leaving `<inc>` as
        // a phantom `live` session nobody renewed — peers counted it as a live node until its
        // TTL ran out.
        let (path, body) = (
            session_path(&inner.prefix, &id),
            bytes::Bytes::from(rec.encode()),
        );
        match open_io(policy, || {
            crate::cas::put_resolving(&inner.store, &path, body.clone(), PutMode::Create)
        })
        .await
        {
            Ok(crate::cas::Cas::Written(version)) => {
                let root = ValidityHandle::root_with_pause(
                    renew_deadline(issued_at, inner.cfg.ttl),
                    pause_after_deadline(inner.cfg.ttl),
                    Some(fence::handle(&inner.fence, &id)),
                );
                {
                    let mut st = inner.state.lock().expect("mutex");
                    st.owns.clear();
                    st.pending_release.clear();
                    st.current = Some(Current {
                        id: id.clone(),
                        root: root.clone(),
                        version,
                        beat: 1,
                        issued_at,
                        written: rec.clone(),
                        solo: false,
                        checked_at: None,
                        delegated: None,
                        touched_at: Some(issued_at),
                    });
                }
                // Re-read the gate at the first round of the new incarnation.
                inner.gate_seen.lock().expect("mutex").1 = None;
                inner.tracker.set_me(Some(id.clone()));
                inner.live.send_replace(Some((id.clone(), root)));
                tracing::info!(session = %id, "session opened");
                incarnation::forget_older(
                    inner,
                    &id,
                    claimed.iter().copied().filter(|&c| c < id.inc).collect(),
                )
                .await;
                return Ok(());
            }
            Ok(crate::cas::Cas::Lost) => inc += 1,
            Err(e) => return Err(e),
        }
    }
}

/// The current incarnation is lost: revoke every grant **first**, before doing anything
/// else.
///
/// `still_live` = the record may still be `live` (lost by timing out): write `closed` so
/// others can claim the partitions at once. An already ended record is left as is.
///
/// The **only** place counting `cairnflow_failstop_total{cause}`: once per incarnation
/// really dropped.
pub(super) async fn lose(inner: &Arc<Inner>, cause: Cause, reason: &str, still_live: bool) {
    let current = {
        let mut st = inner.state.lock().expect("mutex");
        let c = st.current.take();
        if let Some(c) = &c {
            st.abandoned.insert(c.id.clone());
        }
        // Owner records pointing at a dropped incarnation are orphans — no need to yield.
        st.owns.clear();
        st.pending_release.clear();
        c
    };
    inner.live.send_replace(None);
    inner.tracker.set_me(None);
    if let Some(c) = current {
        c.root.revoke();
        crate::failstop::record(cause);
        tracing::warn!(session = %c.id, cause = %cause, reason = reason, "session lost — dropping every partition");
        if still_live {
            write_terminal(inner, &c, SessionState::Closed).await;
        }
    }
}

/// The outcome of one renewal attempt.
pub(super) enum Renewal {
    Done,
    /// Lost; `true` = the record may still be `live`.
    Lost(Cause, &'static str, bool),
    /// Unknown — the deadline clock keeps running: past the deadline it is suspended, past
    /// the suspension cap it is lost.
    Unknown,
}

pub(super) async fn keeper_loop(inner: Arc<Inner>) {
    let mut stop = inner.stop.subscribe();
    // A renewal issued whose outcome is ambiguous (`Err` — rule 7 of AGENTS: it may have been
    // written): the next CAS misses — and we recognise our own content.
    let mut unconfirmed: Option<(u64, Instant)> = None;
    // When the latest renewal was issued. The tick is measured from **the issue**, not from
    // when the previous one finished: measured from the finish, a PUT slow by L costs
    // `2L + tick` between two issues, and a node whose PUTs are slower than
    // `(⅔ TTL − tick) / 2` loses its session even though every write meets its deadline
    // (chaos `--slow-node`).
    let mut last_issue: Option<Instant> = None;
    loop {
        if *stop.borrow() {
            return;
        }
        *inner.keeper_tick.lock().expect("mutex") = Some(Instant::now());
        let snapshot = {
            let st = inner.state.lock().expect("mutex");
            st.current.as_ref().map(|c| {
                (
                    c.issued_at,
                    c.root.clone(),
                    c.solo,
                    c.checked_at,
                    c.delegated.clone(),
                )
            })
        };
        let Some((issued_at, root, solo, checked_at, delegated)) = snapshot else {
            if let Err(e) = open_incarnation(&inner, None).await {
                tracing::warn!(error = %e, "could not open the session, retrying");
                crate::pace::retry_pause(inner.cfg.renew_interval).await;
            }
            unconfirmed = None;
            last_issue = None;
            continue;
        };
        let deadline = renew_deadline(issued_at, inner.cfg.ttl);
        // The suspension cap: the root's **last** validity deadline plus the suspension
        // window — a winning CAS whose new deadline is still in the past does not move it.
        // A root has no deadline only when pinned (`CrashHandle`, failure-building tests):
        // measure the cap from the issue as before pinning — **no** `lose` here, because this
        // loop may still run one more tick after being pinned before it is cancelled, and a
        // `closed` write at that point would break the "frozen process" scenario.
        let cap = root
            .lost_at()
            .unwrap_or_else(|| deadline + pause_after_deadline(inner.cfg.ttl));
        // In `solo`, a self-check GET the read fence loop has just merged also counts as a
        // tick.
        let paced_from = last_issue
            .map_or(issued_at, |at| at.max(issued_at))
            .max(checked_at.unwrap_or(issued_at));
        let wake = if let Some(d) = &delegated {
            // Delegated: self-check GETs on the tick, fall back exactly at the reference,
            // lost at the cap. When the fallback reference has passed and the fallback CAS
            // (or the re-delegation, a write keeping the mode) has just failed, retry **at a
            // pace** — the floor is the previous attempt + `renew_interval/4`, as when
            // suspended in `direct`. Without the floor, a reference in the past makes the
            // keeper wake at once, the PUT fails at once, it wakes again at once: a hot loop
            // (30 thousand PUTs every 600 ms; `sim_cluster` hung at one virtual ms).
            let retry_floor = last_issue.map(|at| at + inner.cfg.renew_interval / 4);
            let fallback = d.fallback_at(inner.cfg.ttl);
            (paced_from + inner.cfg.renew_interval)
                .min(retry_floor.map_or(fallback, |f| fallback.max(f)))
                .min(cap)
        } else if root.is_valid() {
            (paced_from + inner.cfg.renew_interval).min(if solo { cap } else { deadline })
        } else {
            // Suspended: the previous renewal has come back (won while the deadline is still
            // in the past, or a fast error) — retry early, up to the cap.
            (paced_from + inner.cfg.renew_interval / 4).min(cap)
        };
        // `biased`: stop first, then wake-ups, then the deadline — whichever branch fires,
        // the rest re-checks the deadline.
        tokio::select! {
            biased;
            _ = stop.changed() => return,
            _ = inner.wake.notified() => {}
            _ = tokio::time::sleep_until(wake) => {}
        }
        // Past the self-demotion deadline the grant is **suspended**, not `lose`: the grant is
        // invalid by the clock (no acks, no reads), the renewal keeps trying — a CAS winning
        // from exactly the previous version revives it. Only past the cap (or when revoked)
        // is it lost.
        if root.is_lost() || Instant::now() >= cap {
            lose(
                &inner,
                Cause::SessionExpired,
                "suspended past the cap without any renewal winning its CAS",
                true,
            )
            .await;
            continue;
        }
        if let Some(d) = delegated {
            match delegated_round(
                &inner,
                &d,
                &root,
                cap,
                paced_from,
                &mut unconfirmed,
                &mut last_issue,
            )
            .await
            {
                Renewal::Done | Renewal::Unknown => {}
                Renewal::Lost(cause, reason, still_live) => {
                    lose(&inner, cause, reason, still_live).await
                }
            }
            continue;
        }
        // The mode of the next write: `solo` when alone and the gate is on.
        let want_solo = want_solo(&inner).await;
        // Delegation only when not `solo` (the two modes cannot both be stable at once:
        // `solo` is when nobody else is left, delegation is for someone else).
        let want_delegate = if want_solo || solo {
            None
        } else {
            want_delegate(&inner, root.is_valid())
        };
        let outcome = if solo && want_solo && !needs_write(&inner) {
            // A wake-up (`owns` unchanged, the read fence seeing `challenged`, …) is not a
            // tick: self-check GETs still follow the tick, unless the read fence has just
            // seen the record change (it nudges this loop to answer at once).
            let due = Instant::now() >= paced_from + inner.cfg.renew_interval
                || !root.is_valid()
                || std::mem::take(&mut *inner.solo_poke.lock().expect("mutex"));
            if !due {
                continue;
            }
            last_issue = Some(Instant::now());
            self_check(&inner, cap, &mut unconfirmed).await
        } else {
            last_issue = Some(Instant::now());
            // In `solo` but no longer wanting it (another session is known, the gate is
            // off) ⇒ one CAS back to `direct`: leave the mode. `owns`/`draining` changed ⇒ one
            // CAS keeping `solo`. Otherwise: a `direct` renewal as today — including the CAS
            // into the mode (`want_solo`), only while the grant is valid.
            let mode = if want_solo && (solo || root.is_valid()) {
                SessionMode::Solo
            } else if let Some((coord, term)) = want_delegate {
                SessionMode::Delegated { coord, term }
            } else {
                SessionMode::Direct
            };
            if matches!(mode, SessionMode::Delegated { .. }) {
                tracing::info!(session = ?inner.cfg.node_id, mode = ?mode, "the coordinator permits it — CAS mode=delegated");
                delegate_once(&inner, cap, &mut unconfirmed, mode).await
            } else {
                renew_once(&inner, cap, &mut unconfirmed, mode).await
            }
        };
        match outcome {
            Renewal::Done | Renewal::Unknown => {}
            Renewal::Lost(cause, reason, still_live) => {
                lose(&inner, cause, reason, still_live).await;
                continue;
            }
        }
    }
}

/// Whether the next write should be `solo`: the `control/quiet-lease` gate is on, **and**
/// one of two —
///
/// - we are the only unfinished session according to the tracker;
/// - we are the coordinator every other session delegates to, the gate has the delegation
///   flag, and we are not draining ("the grant is valid" is the caller's job). A
///   coordinator's `solo` record is identical to a lone node's: others challenge and then
///   wait one TTL (LS-16) — nodes delegating to us postpone it
///   ([`SessionTracker::lean_on`]).
///
/// The gate is read (one conditional GET) only when one of the two holds, on the
/// incarnation's first round and every [`LeaseConfig::quiet_lease_poll`]. A failed read ⇒
/// treated as off.
async fn want_solo(inner: &Inner) -> bool {
    let alone = inner.tracker.alone();
    if !alone && !quiet_coordinator(inner) {
        return false;
    }
    super::delegate::gate_poll(inner).await
        && (alone || *inner.gate_delegation.lock().expect("mutex"))
}

/// The coordinator conditions of quiet renewal: we hold the coordinator role under the
/// current incarnation, every other unfinished session delegates to exactly that role, and
/// we are not draining. No I/O.
fn quiet_coordinator(inner: &Inner) -> bool {
    let Some(me) = inner
        .state
        .lock()
        .expect("mutex")
        .current
        .as_ref()
        .map(|c| c.id.clone())
    else {
        return false;
    };
    let Some(term) = inner.coordinator.held_by(&me) else {
        return false;
    };
    !published(inner).draining && inner.tracker.all_delegated_to(&me, term)
}

/// In `solo`: `owns` or `draining` differing from the record on the bucket ⇒ a CAS is
/// needed (keeping the mode), as a `direct` renewal carries them today.
fn needs_write(inner: &Inner) -> bool {
    let st = inner.state.lock().expect("mutex");
    let now = published(inner);
    st.current.as_ref().is_some_and(|c| {
        c.written.owns != st.owns
            || c.written.draining != now.draining
            // Delegated: the slowness score and p99 change every round — they travel over
            // the heartbeat into the `view`, not written to the bucket; only **crossing the
            // slowness threshold** is written (readers of the record — old binaries, the
            // sparse tracker — still see the node is slow).
            || (c.delegated.is_some() && slow_side(c.written.slow) != slow_side(now.slow))
    })
}

/// Whether the slowness score is at the cap ([`super::delegate::WEAK_SLOW_SCORE`]) or
/// below.
fn slow_side(score: Option<u32>) -> bool {
    score.is_some_and(|s| s >= super::delegate::WEAK_SLOW_SCORE)
}

/// The CAS into delegated mode (or re-delegation) — a `direct` renewal. Failpoint
/// `session::before_delegate_cas`: `return` ⇒ treated as a failed PUT.
async fn delegate_once(
    inner: &Arc<Inner>,
    cap: Instant,
    unconfirmed: &mut Option<(u64, Instant)>,
    mode: SessionMode,
) -> Renewal {
    crate::failpoint!("session::before_delegate_cas", |_| Renewal::Unknown);
    renew_once(inner, cap, unconfirmed, mode).await
}

/// One keeper round when our record is the confirmed `delegated` record (see the header of
/// `delegate.rs`): fall back (reference expired, the coordinator refuses), delegate again to
/// a new role, CAS keeping the mode when `owns`/`draining` change (no renewal), or a
/// self-check GET on the tick (no renewal).
async fn delegated_round(
    inner: &Arc<Inner>,
    d: &Delegation,
    root: &ValidityHandle,
    cap: Instant,
    paced_from: Instant,
    unconfirmed: &mut Option<(u64, Instant)>,
    last_issue: &mut Option<Instant>,
) -> Renewal {
    // Re-read the fallback reference after sleeping: an ack arriving meanwhile has moved it.
    let fresh = {
        let st = inner.state.lock().expect("mutex");
        st.current.as_ref().and_then(|c| c.delegated.clone())
    };
    let Some(d) = fresh.filter(|f| f.coord == d.coord && f.term == d.term) else {
        return Renewal::Unknown;
    };
    let d = &d;
    let refusal = take_refusal(inner);
    let now = Instant::now();
    // A draining node (LS-11): back to `direct` — PUT renewals are the slowness samples of
    // an idle node; in delegated mode there are no samples, so the score would stand still
    // and the node would drain forever.
    let draining = published(inner).draining;
    if refusal.is_some() || draining || now >= d.fallback_at(inner.cfg.ttl) {
        tracing::info!(
            session = ?inner.cfg.node_id,
            coordinator = %d.coord,
            term = d.term,
            reason = ?refusal,
            draining = draining,
            "lost the coordinator's ack (or draining) — falling back to mode=direct"
        );
        *last_issue = Some(now);
        // `return` ⇒ the fallback CAS is treated as a failed PUT (retried next time).
        crate::failpoint!("session::before_fallback_cas", |_| {
            inner.delegation.lock().expect("mutex").refused = refusal;
            Renewal::Unknown
        });
        let r = renew_once(inner, cap, unconfirmed, SessionMode::Direct).await;
        match r {
            Renewal::Done => fell_back(inner),
            Renewal::Unknown => inner.delegation.lock().expect("mutex").refused = refusal,
            Renewal::Lost(..) => {}
        }
        return r;
    }
    if let Some((coord, term)) =
        want_delegate(inner, root.is_valid()).filter(|(c, t)| (c, *t) != (&d.coord, d.term))
    {
        // The coordinator changed: delegate again to the new role — one CAS, one LS-2
        // renewal like the CAS into the mode.
        *last_issue = Some(now);
        tracing::info!(session = ?inner.cfg.node_id, coordinator = %coord, term, "delegating again to a new coordinator");
        return delegate_once(
            inner,
            cap,
            unconfirmed,
            SessionMode::Delegated { coord, term },
        )
        .await;
    }
    if needs_write(inner) {
        {
            let st = inner.state.lock().expect("mutex");
            tracing::debug!(session = ?inner.cfg.node_id, owns = ?st.owns, written = ?st.current.as_ref().map(|c| c.written.owns.clone()), "delegated: owns/draining changed — CAS keeping the mode");
        }
        *last_issue = Some(now);
        let mode = SessionMode::Delegated {
            coord: d.coord.clone(),
            term: d.term,
        };
        return renew_once(inner, cap, unconfirmed, mode).await;
    }
    // The read fence has just seen the record change: self-check at once.
    let poked = std::mem::take(&mut *inner.solo_poke.lock().expect("mutex"));
    if poked || now >= paced_from + inner.cfg.renew_interval {
        *last_issue = Some(now);
        return delegated_check(inner, cap, unconfirmed).await;
    }
    Renewal::Done
}

/// The self-check GET of solo mode: `If-None-Match` = the etag of the confirmed `solo`
/// record, issued at `issued_at`. See the module header.
pub(super) async fn self_check(
    inner: &Arc<Inner>,
    cap: Instant,
    unconfirmed: &mut Option<(u64, Instant)>,
) -> Renewal {
    let (id, etag, written) = {
        let st = inner.state.lock().expect("mutex");
        match st.current.as_ref() {
            Some(c) if c.solo => (c.id.clone(), c.version.e_tag.clone(), c.written.clone()),
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
    // Bound: one third of the TTL (like the read fence) and no further than the suspension
    // cap — a GET coming back later would only renew into the past.
    let bound = (issued_at + inner.cfg.ttl / 3).min(cap);
    match tokio::time::timeout_at(bound, get).await {
        Ok(Ok(None)) => solo_extend(inner, &id, etag.as_deref(), issued_at),
        Ok(Ok(Some((raw, version)))) => match SessionRecord::decode(&raw) {
            Ok(rec) if rec.state.is_terminal() => {
                Renewal::Lost(Cause::SessionLost, "session has been marked ended", false)
            }
            Ok(rec) if rec.node != inner.cfg.node_id => {
                Renewal::Lost(Cause::SessionLost, "session changed by others", false)
            }
            // Exactly the content we wrote (the store changed the etag without changing the
            // content): nobody has challenged — renew, and remember the new etag.
            Ok(rec) if rec == written => {
                {
                    let mut st = inner.state.lock().expect("mutex");
                    if let Some(c) = st.current.as_mut().filter(|c| c.id == id && c.solo) {
                        c.version = version.clone();
                    }
                }
                solo_extend(inner, &id, version.e_tag.as_deref(), issued_at)
            }
            Ok(rec) => answer(inner, &id, version, Some(rec), cap, unconfirmed).await,
            Err(_) => answer(inner, &id, version, None, cap, unconfirmed).await,
        },
        Ok(Err(OsError::NotFound { .. })) => {
            Renewal::Lost(Cause::SessionLost, "the session disappeared", false)
        }
        Ok(Err(e)) => {
            tracing::debug!(session = %id, error = %e, "self-check GET failed — not renewing");
            Renewal::Unknown
        }
        Err(_) => Renewal::Unknown,
    }
}

/// Renews from a self-check GET that found our `solo` record unchanged (`304` with `etag`, or
/// `200` with exactly the content), issued at `issued_at` — from the renewal loop or the read
/// fence loop. Only when `id` is still the current incarnation, in `solo`, and `etag` is the
/// etag of the confirmed `solo` record. A revoked grant (after `lose`) or one past the cap
/// moves nothing.
pub(super) fn solo_extend(
    inner: &Inner,
    id: &SessionId,
    etag: Option<&str>,
    issued_at: Instant,
) -> Renewal {
    let root = {
        let mut st = inner.state.lock().expect("mutex");
        let Some(c) = st
            .current
            .as_mut()
            .filter(|c| &c.id == id && c.solo && c.version.e_tag.as_deref() == etag)
        else {
            return Renewal::Unknown;
        };
        c.checked_at = Some(c.checked_at.map_or(issued_at, |at| at.max(issued_at)));
        c.root.clone()
    };
    let paused = !root.is_valid();
    if root.extend_until(renew_deadline(issued_at, inner.cfg.ttl)) {
        if paused && root.is_valid() {
            pause_stats::REVIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!(session = %id, "the self-check GET found the solo record intact — revived");
        }
        Renewal::Done
    } else {
        Renewal::Lost(Cause::SessionExpired, "self-check past the pause cap", true)
    }
}

/// **Answer**: the self-check GET found our record changed but not ended — a
/// `challenged=<newcomer>` line, an overwrite, or undecodable. That read does **not** renew.
/// A CAS If-Match `version` (the record just read) back to `direct`, `beat+1` (above the
/// largest beat seen), dropping the `challenged` line — a `renew_once` renewal like any other:
/// won ⇒ the deadline follows LS-2; missed and the re-read finds it ended (the challenger has
/// CASed `expired`) ⇒ `lose`. The challenger's name goes into the tracker, so we do not
/// re-enter `solo` while it is alive.
async fn answer(
    inner: &Arc<Inner>,
    id: &SessionId,
    version: UpdateVersion,
    seen: Option<SessionRecord>,
    cap: Instant,
    unconfirmed: &mut Option<(u64, Instant)>,
) -> Renewal {
    learn_challenger(inner, seen.as_ref());
    tracing::info!(
        session = %id,
        challenger = seen.as_ref().and_then(|r| r.challenged.as_deref()).unwrap_or("?"),
        "the solo record has been challenged — answering, back to direct"
    );
    // `return` ⇒ this answer is treated as a failed PUT (the deadline clock keeps running,
    // the record still carries the `challenged` line).
    crate::failpoint!("session::before_answer_challenge", |_| Renewal::Unknown);
    if !heed_challenge(inner, id, version, seen.as_ref()) {
        return Renewal::Unknown;
    }
    renew_once(inner, cap, unconfirmed, SessionMode::Direct).await
}

/// The challenger (the `challenged` line of `seen`) goes into the tracker, so we do not
/// re-enter `solo` while it is alive.
fn learn_challenger(inner: &Inner, seen: Option<&SessionRecord>) {
    if let Some(by) = seen
        .and_then(|r| r.challenged.as_deref())
        .and_then(SessionId::parse)
    {
        inner.tracker.learn(&by);
    }
}

/// The state part of an answer: the next CAS goes from `version` (the record just read)
/// with a beat above the largest beat seen, leaving `solo`. `false` when `id` is no longer
/// the current incarnation.
fn heed_challenge(
    inner: &Inner,
    id: &SessionId,
    version: UpdateVersion,
    seen: Option<&SessionRecord>,
) -> bool {
    {
        let mut st = inner.state.lock().expect("mutex");
        let Some(c) = st.current.as_mut().filter(|c| &c.id == id) else {
            return false;
        };
        c.version = version;
        c.beat = c.beat.max(seen.map_or(0, |r| r.beat));
        c.solo = false;
    }
    inner.tracker.set_quiet(false);
    true
}

/// How one renewal ended (before the re-read).
pub(super) enum Attempt {
    Landed(slatedb::object_store::PutResult),
    /// `Precondition`/`NotFound`: the record changed under us — possibly by ourselves.
    Contested,
    /// Every attempt returned another error — ambiguous (rule 7).
    Failed,
    /// The suspension cap was reached before any attempt answered.
    Expired,
}

/// One renewal, waiting until `deadline` — the suspension cap, not the self-demotion
/// deadline: one that comes back after the deadline and wins its CAS can still revive the
/// grant.
pub(super) async fn renew_once(
    inner: &Arc<Inner>,
    deadline: Instant,
    unconfirmed: &mut Option<(u64, Instant)>,
    mode: SessionMode,
) -> Renewal {
    let (id, version, beat, owns) = {
        let st = inner.state.lock().expect("mutex");
        let Some(c) = st.current.as_ref() else {
            return Renewal::Unknown;
        };
        (c.id.clone(), c.version.clone(), c.beat + 1, st.owns.clone())
    };
    let rec = SessionRecord {
        node: inner.cfg.node_id.clone(),
        beat,
        address: inner.cfg.address.clone(),
        ingress: inner.cfg.ingress.clone(),
        knows: Some(inner.cfg.knows),
        slow: published(inner).slow,
        p99_put_ms: published(inner).p99_put_ms,
        draining: published(inner).draining,
        owns,
        state: SessionState::Live,
        mode,
        challenged: None,
    };
    let path = session_path(&inner.prefix, &id);
    let payload = PutPayload::from(rec.encode());
    let issued_at = Instant::now();
    // Every attempt between two successful renewals carries the same beat; the **oldest**
    // ambiguous one is the safe reference when a re-read finds that beat (observers may have
    // seen it since that attempt wrote). An ambiguous attempt of an older beat is moot.
    if unconfirmed.is_some_and(|(b, _)| b != beat) {
        *unconfirmed = None;
    }
    // `return` ⇒ this renewal is treated as a failed PUT (the deadline clock keeps running).
    crate::failpoint!("session::renew::before_put", |_| Renewal::Unknown);
    let attempt = || {
        inner.store.put_opts(
            &path,
            payload.clone(),
            PutOptions::from(PutMode::Update(version.clone())),
        )
    };
    // The renewal's bound is **the remaining suspension cap**, not one tick: a PUT slower
    // than the tick that still meets the deadline must keep the session, and a PUT coming
    // back after the deadline that wins its CAS can revive the grant. A hung PUT (a stuck
    // connection; the S3 client waits up to 30 s by itself) does not eat the whole deadline:
    // every tick without an answer issues one more identical attempt **in parallel**, without
    // cancelling the previous one — the same number of PUTs as cutting each attempt at one
    // tick, but the slow one can still come back. Every attempt is a CAS on the same version:
    // at most one can write, the others miss.
    let mut inflight = futures::stream::FuturesUnordered::new();
    inflight.push(attempt());
    let mut hedge_at = issued_at + inner.cfg.renew_interval;
    let outcome = loop {
        tokio::select! {
            biased;
            Some(res) = futures::StreamExt::next(&mut inflight) => match res {
                Ok(put) => break Attempt::Landed(put),
                Err(OsError::Precondition { .. } | OsError::NotFound { .. }) => break Attempt::Contested,
                Err(e) => {
                    // Rule 7: an `Err` may have written. If the next CAS misses, re-read.
                    tracing::warn!(error = %e, "session renewal failed, retrying");
                    unconfirmed.get_or_insert((beat, issued_at));
                    if inflight.is_empty() {
                        break Attempt::Failed;
                    }
                }
            },
            _ = tokio::time::sleep_until(hedge_at), if hedge_at < deadline => {
                inflight.push(attempt());
                hedge_at += inner.cfg.renew_interval;
            }
            _ = tokio::time::sleep_until(deadline) => break Attempt::Expired,
        }
    };
    // Attempts still in flight are dropped: CASes on a version the record has left
    // (`Landed`, `Contested`), or the suspension cap has passed (`Expired`) — none of them
    // can change anything we need to know any more.
    drop(inflight);
    // Slowness signal: the renewal is one conditional PUT every TTL/5 even when the node is
    // idle — measuring it costs no extra request. A timeout is measured up to when we stopped
    // waiting.
    if let Some(m) = &inner.cfg.meter {
        m.renewed(issued_at.elapsed());
    }
    // The safe issue reference when the record carries our beat but it is unknown which
    // attempt wrote it.
    let earliest = unconfirmed.map_or(issued_at, |(_, at)| at.min(issued_at));
    match outcome {
        Attempt::Landed(res) => {
            // `return` ⇒ the PUT was written without our knowing — like an ambiguous attempt:
            // the next CAS misses and we must recognise our own content.
            crate::failpoint!("session::renew::after_put", |_| {
                unconfirmed.get_or_insert((beat, issued_at));
                Renewal::Unknown
            });
            // A CAS winning from `version` is the only attempt that could write this beat:
            // every ambiguous attempt before it did not write (otherwise this CAS would miss).
            *unconfirmed = None;
            adopt(inner, &id, res.into(), beat, issued_at, &rec)
        }
        Attempt::Failed | Attempt::Expired => Renewal::Unknown,
        Attempt::Contested => {
            // The record changed under us — but it may be **our own write** (an earlier
            // ambiguous attempt, or object_store retrying a 5xx of this one by itself).
            // Re-read.
            let read =
                tokio::time::timeout_at(deadline, read_session(&inner.store, &inner.prefix, &id))
                    .await;
            match read {
                Ok(Ok(Some((Ok(found), v)))) if found.state == SessionState::Live => {
                    if found.beat == beat && found.node == inner.cfg.node_id {
                        *unconfirmed = None;
                        // The record carries our beat: our write has applied. It may have been
                        // **challenged** after that (a `challenged` line on our `solo`
                        // record) — the LS-2 renewal is still correct (the challenger read the
                        // record **after** our write applied), but `adopt` seeing that line does
                        // not treat it as a confirmed `solo`, and answers next time.
                        adopt(inner, &id, v, beat, earliest, &found)
                    } else if found.node == inner.cfg.node_id
                        && found.beat < beat
                        && found.challenged.is_some()
                    {
                        // Our record, an old beat, carrying a `challenged` line: a challenge
                        // (or a `rearm` of the `solo` record) applied between when we issued
                        // this CAS (keeping `solo` because `owns` changed, or leaving the
                        // mode) and when it reached the store. Like `self_check` seeing that
                        // line: **answer** from the version just read, no `lose` (seen in a
                        // chaos run). The challenger only CASes `expired` If-Match this
                        // version after one TTL, so an answer winning before that is an LS-2
                        // renewal; if it has already CASed, the record has ended and the
                        // answer misses and then `lose`s in the branch below.
                        learn_challenger(inner, Some(&found));
                        tracing::info!(
                            session = %id,
                            challenger = found.challenged.as_deref().unwrap_or("?"),
                            "renewal CAS missed because the solo record was challenged — answering, back to direct"
                        );
                        // The same failpoint as `answer`: right before the answering CAS (in the
                        // next keeper round). `return` ⇒ no answer this time; the next round CASes
                        // again from the old version, misses, and gets here once more.
                        crate::failpoint!("session::before_answer_challenge", |_| {
                            Renewal::Unknown
                        });
                        if heed_challenge(inner, &id, v, Some(&found)) {
                            // The next keeper round CASes back to `direct` at once.
                            inner.wake.notify_one();
                        }
                        Renewal::Unknown
                    } else {
                        Renewal::Lost(Cause::SessionLost, "session changed by others", false)
                    }
                }
                Ok(Ok(Some((Ok(_), _)))) => {
                    Renewal::Lost(Cause::SessionLost, "session has been marked ended", false)
                }
                Ok(Ok(None)) => Renewal::Lost(Cause::SessionLost, "the session disappeared", false),
                _ => Renewal::Unknown,
            }
        }
    }
}

pub(super) fn adopt(
    inner: &Arc<Inner>,
    id: &SessionId,
    version: UpdateVersion,
    beat: u64,
    issued_at: Instant,
    written: &SessionRecord,
) -> Renewal {
    // A confirmed `solo` ⇔ the record on the bucket is exactly the `solo` record we wrote,
    // with no `challenged` line.
    let solo = written.mode == SessionMode::Solo && written.challenged.is_none();
    // A confirmed delegation ⇔ the record on the bucket is the `delegated` record we wrote.
    let delegated = match &written.mode {
        SessionMode::Delegated { coord, term } if written.challenged.is_none() => {
            Some((coord.clone(), *term))
        }
        _ => None,
    };
    let (root, extend) = {
        let mut st = inner.state.lock().expect("mutex");
        let Some(c) = st.current.as_mut().filter(|c| &c.id == id) else {
            return Renewal::Unknown;
        };
        c.version = version;
        c.beat = beat;
        c.issued_at = issued_at;
        c.written = written.clone();
        c.touched_at = Some(c.touched_at.map_or(issued_at, |at| at.max(issued_at)));
        if solo && !c.solo {
            tracing::info!(session = %id, "alone on the bucket (or the coordinator every other session delegates to), gate on — entering mode=solo, no more renewal PUTs");
        }
        c.solo = solo;
        // A CAS keeping the mode while delegated (same role) does not renew — only acks
        // renew. The CAS into the mode, re-delegation, fallback: an LS-2 renewal.
        let same = matches!((&c.delegated, &delegated),
            (Some(d), Some((coord, term))) if &d.coord == coord && d.term == *term);
        c.delegated = match delegated {
            Some(_) if same => c.delegated.take(),
            Some((coord, term)) => {
                tracing::info!(session = %id, coordinator = %coord, term, "entering mode=delegated — no more renewal PUTs, renewing by ack");
                Some(Delegation {
                    coord,
                    term,
                    entered_beat: beat,
                    base: issued_at,
                    jitter: crate::pace::jittered(inner.cfg.ttl / 10),
                })
            }
            None => None,
        };
        // The tracker does not challenge the `solo` record of the coordinator we delegate
        // to.
        inner
            .tracker
            .lean_on(c.delegated.as_ref().map(|d| d.coord.clone()));
        (c.root.clone(), !same)
    };
    if !extend {
        return if root.is_lost() {
            Renewal::Lost(
                Cause::SessionExpired,
                "CAS keeping the delegated mode came back after the suspension cap",
                true,
            )
        } else {
            Renewal::Done
        };
    }
    // The LIST of `sessions/` is only a safety net while we are **alone**. A coordinator in
    // `solo` still LISTs every TTL: a session that sends no heartbeat (an old binary slipping
    // in, a node cut off from us) only reaches its tracker through the LIST, and the gate's
    // runtime rule (seeing `knows` < K ⇒ the whole cluster falls back) relies on it.
    inner.tracker.set_quiet(solo && inner.tracker.alone());
    if let Some(by) = written.challenged.as_deref().and_then(SessionId::parse) {
        inner.tracker.learn(&by);
        // The record already carries a `challenged` line: answer at once on the next round.
        inner.wake.notify_one();
    }
    // A CAS winning from exactly the previous version (or an ambiguous one whose re-read
    // shows ours, with the earliest issue as reference) moves the deadline — even while the
    // grant is suspended: an unchanged version means nobody has CASed `expired` or
    // overwritten, so nobody has taken a range. Only past the suspension cap is it lost.
    let paused = !root.is_valid();
    if root.extend_until(renew_deadline(issued_at, inner.cfg.ttl)) {
        if paused && root.is_valid() {
            pause_stats::REVIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!(session = %id, "a late renewal won its CAS — revived, keeping every range");
        }
        Renewal::Done
    } else {
        Renewal::Lost(Cause::SessionExpired, "renewal after suspension cap", true)
    }
}

/// Writes the "released" record for `p`. `Ok(true)` = done (written, or someone has changed
/// the record — the same outcome: no longer ours).
///
/// `retired` ⇒ write a **tombstone** instead of a plain release. `Ok(false)` = the CAS
/// missed: someone changed the record before us.
pub(super) async fn write_release(inner: &Inner, p: u32, r: &PendingRelease) -> Result<bool> {
    let record = OwnerRecord {
        term: r.term,
        session: r.session.clone(),
        address: None,
        released: true,
        wal_kind: r.wal.kind.clone(),
        retired: r.retired,
        wal_segments: r.wal.segments.clone(),
        wal_floor_id: r.wal.floor_id,
        data: r.data,
        // The tombstone's age reference for D-d's `min_age`.
        retired_ms: r.retired.then(|| {
            crate::entropy::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        }),
    };
    let range = PartitionId(p);
    if owner::write_owner(
        &inner.store,
        &inner.prefix,
        range,
        &record,
        Some(r.version.clone()),
    )
    .await?
    .is_some()
    {
        return Ok(true);
    }
    // The CAS missed — possibly on **this keeper's own earlier write** that it does not
    // know has landed (a clean-record CAS abandoned past its bound; an ambiguous segment add
    // whose re-read failed). The record still carries our session + term and is not
    // released ⇒ nobody claimed the range after us (every claim bumps the term): CAS again
    // from that version, with the **union** of both segment lists — never dropping a segment
    // the record lists (it may carry frames the keeper does not know about). A union wider
    // than ours makes the release dirty: the taker seals and replays, costing more but
    // losing nothing (NW-6).
    match owner::read_observed(&inner.store, &inner.prefix, range).await? {
        Some(o)
            if o.record.session == r.session && o.record.term == r.term && !o.record.released =>
        {
            let mut record = record;
            // A segment of **this very incarnation** that the keeper does not know can only be
            // the keeper's own ambiguous segment add: a record with the same session + term is
            // only written by this keeper, and the keeper knows every other segment add it
            // made. That acquisition failed right after its CAS — before the first frame —, so
            // the log has no frame of the range under that segment: drop it. Keeping it would
            // make the release dirty, pointing at the node's **live** log, and since the keeper
            // has no `shared` yet it does not rotate — the taker seals it and the whole node
            // fail-stops. Unknown segments of other incarnations are kept.
            let mut segments: Vec<_> = o
                .record
                .wal_segments
                .iter()
                .filter(|s| s.log.session != r.session || r.held.contains(s))
                .cloned()
                .collect();
            for s in &record.wal_segments {
                if !segments.contains(s) {
                    segments.push(s.clone());
                }
            }
            segments.sort_by_key(|s| s.first_wal_id);
            record.wal_segments = segments;
            record.wal_floor_id = record.wal_floor_id.max(o.record.wal_floor_id);
            owner::write_owner(&inner.store, &inner.prefix, range, &record, Some(o.version))
                .await
                .map(|v| v.is_some())
        }
        _ => Ok(false),
    }
}

/// Removes `p` from the `owns` hint once the release is done — **after**, not before:
/// removing it before would let others see "nobody holds it" while the owner record still
/// says us.
pub(super) fn released(inner: &Inner, p: u32, term: u64) {
    let mut st = inner.state.lock().expect("mutex");
    if st.owns.get(&p) == Some(&term) {
        st.owns.remove(&p);
    }
    drop(st);
    inner.wake.notify_one();
}

/// Bound of each attempt to write a release in the background. Past the bound, give that
/// attempt up (the write is conditional, so the next one re-reads and resolves it if it
/// actually landed) and move on to the other ranges: one hung PUT does not hold up the
/// whole release queue. One TTL: waiting longer, our owner record may already have gone to
/// someone else.
pub(super) fn release_retry_bound(cfg: &LeaseConfig) -> Duration {
    cfg.ttl
}

/// The background release loop: retries every release not yet written, on every renewal
/// tick or as soon as there is a new one. Separate from `keeper_loop`, so a hung release
/// PUT never delays a renewal.
pub(super) async fn release_loop(inner: Arc<Inner>) {
    let mut stop = inner.stop.subscribe();
    loop {
        if *stop.borrow() {
            return;
        }
        retry_releases(&inner).await;
        // `biased`: stop first; the other two branches both lead to one more release
        // attempt.
        tokio::select! {
            biased;
            _ = stop.changed() => return,
            _ = inner.release_wake.notified() => {}
            _ = tokio::time::sleep(inner.cfg.renew_interval) => {}
        }
    }
}

/// Drops the pending release of `p` — only when the map entry **is still exactly** the
/// snapshot taken (`session`, `term`, `version`). While an attempt is in flight (up to a
/// TTL), the node may have taken the range back itself (clearing the old entry) and then
/// failed to release again in a new term: the entry is now a **newer** release, and
/// deleting it would leave the owner record pointing at our live session forever.
fn forget_pending_release(inner: &Inner, p: u32, snapshot: &PendingRelease) {
    let mut st = inner.state.lock().expect("mutex");
    let same = st.pending_release.get(&p).is_some_and(|r| {
        r.session == snapshot.session && r.term == snapshot.term && r.version == snapshot.version
    });
    if same {
        st.pending_release.remove(&p);
    }
}

pub(super) async fn retry_releases(inner: &Arc<Inner>) {
    let (current, pending) = {
        let st = inner.state.lock().expect("mutex");
        (
            st.current.as_ref().map(|c| c.id.clone()),
            st.pending_release.clone(),
        )
    };
    for (p, r) in pending {
        // A dropped incarnation: the record becomes an orphan by itself, no release needed.
        if current.as_ref() != Some(&r.session) {
            forget_pending_release(inner, p, &r);
            continue;
        }
        let bound = release_retry_bound(&inner.cfg);
        match crate::retry::deadline(
            &crate::retry::ops::SESSION_RELEASE,
            bound,
            write_release(inner, p, &r),
        )
        .await
        {
            Ok(Ok(_)) => {
                forget_pending_release(inner, p, &r);
                released(inner, p, r.term);
            }
            Ok(Err(e)) => {
                tracing::debug!(partition = p, error = %e, "release not yet written, retrying")
            }
            Err(_) => tracing::debug!(partition = p, "release write exceeded {bound:?}, retrying"),
        }
    }
}
