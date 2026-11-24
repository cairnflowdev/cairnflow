// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The node's [`Session`]: building it, claiming/releasing ranges ([`Claim`]), the owner's
//! WAL.

use super::*;

// ───────────────────────────────────────────────────────────── session

pub(super) struct Current {
    pub(super) id: SessionId,
    pub(super) root: ValidityHandle,
    /// The version of our last **confirmed** write (a winning CAS, or an ambiguous one whose
    /// re-read found ours) — or, while answering a challenge, of the record carrying the
    /// `challenged` line that the answering CAS is based on.
    pub(super) version: UpdateVersion,
    pub(super) beat: u64,
    pub(super) issued_at: Instant,
    /// Content of the last confirmed write — a `200` self-check GET only renews when it
    /// returns **exactly** this.
    pub(super) written: SessionRecord,
    /// The record on the bucket is our **confirmed** `solo` record, version `version` (a
    /// `304` only counts when compared with the etag of that very record, not with the etag
    /// from before the CAS into the mode).
    pub(super) solo: bool,
    /// When the latest renewing self-check GET was issued (by the renewal loop or the read
    /// fence loop) — the renewal loop's tick is measured from here.
    pub(super) checked_at: Option<Instant>,
    /// The record on the bucket is our **confirmed** `delegated` record (`delegate.rs`):
    /// acks of exactly that coordinator renew the grant.
    pub(super) delegated: Option<super::delegate::Delegation>,
    /// When the latest successful touch of the bucket was issued (our CAS won, a self-check
    /// GET or the read fence saw our record): its age travels in the heartbeat, and the
    /// coordinator only acks when it is < TTL/3.
    pub(super) touched_at: Option<Instant>,
}

#[derive(Default)]
pub(super) struct State {
    pub(super) current: Option<Current>,
    /// Dropped incarnations of **our own**: owner records pointing at them can be claimed
    /// back at once.
    pub(super) abandoned: HashSet<SessionId>,
    /// The `owns` hint to write on the next renewal.
    pub(super) owns: BTreeMap<u32, u64>,
    /// Releases not yet written. Retried every tick until written or until someone has
    /// changed the record.
    pub(super) pending_release: BTreeMap<u32, PendingRelease>,
}

/// The WAL part of the owner record that the keeper carries: every owner-record write of
/// the keeper — adding a segment, dropping segments, releasing, writing the tombstone —
/// writes exactly this part back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerWal {
    pub kind: owner::WalKind,
    pub segments: Vec<owner::WalSegment>,
    pub floor_id: Option<u64>,
}

impl OwnerWal {
    fn of(record: &OwnerRecord) -> Self {
        OwnerWal {
            kind: record.wal_kind.clone(),
            segments: record.wal_segments.clone(),
            floor_id: record.wal_floor_id,
        }
    }
}

/// A release not yet written.
#[derive(Clone)]
pub(super) struct PendingRelease {
    /// The session at claim time.
    pub(super) session: SessionId,
    pub(super) term: u64,
    /// The owner record version to CAS against.
    pub(super) version: UpdateVersion,
    /// Whether it is a tombstone.
    pub(super) retired: bool,
    /// The WAL part to write — "clean or not" already computed.
    pub(super) wal: OwnerWal,
    /// The segments the owner record listed as of the keeper's last write — the "clean or
    /// not" decision in `wal` only holds for exactly these segments
    /// ([`Session::reclaim_wal`]).
    pub(super) held: Vec<owner::WalSegment>,
    /// The range's `data=` — a release does not change the data version.
    pub(super) data: Option<u32>,
}

pub(super) struct Inner {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) prefix: Path,
    pub(super) cfg: LeaseConfig,
    pub(super) state: Mutex<State>,
    /// The live incarnation, if any — partition claimers wait on this.
    pub(super) live: watch::Sender<Option<(SessionId, ValidityHandle)>>,
    /// Nudges the renewal loop: `owns` changed (so others see it early).
    pub(super) wake: Notify,
    /// Nudges the release loop (`release_loop`): there is a new release to retry.
    pub(super) release_wake: Notify,
    pub(super) stop: watch::Sender<bool>,
    pub(super) tracker: SessionTracker,
    /// Membership hints for users outside the lease layer — wraps `tracker`.
    membership: Membership,
    pub(super) tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    /// Ranges this session holds, for the owner-record watch loop.
    // BTreeMap: a fixed order for re-reading owner records (see `table`).
    pub(super) watched: Mutex<BTreeMap<u32, Watched>>,
    /// The owner-record watch loop — **not** in `tasks`: it is a layer independent of the
    /// renewal loop (see [`Session::establish`]).
    owner_watch: Mutex<Option<tokio::task::AbortHandle>>,
    /// Channel to the read fence loop (`fence.rs`) — each incarnation's fence sends into
    /// it.
    pub(super) fence: tokio::sync::mpsc::UnboundedSender<super::fence::Request>,
    /// The read fence loop — outside `tasks`, like `owner_watch`.
    fence_task: Mutex<Option<tokio::task::AbortHandle>>,
    /// "One of our ranges just changed owner" reports waiting for the owner-record watch
    /// loop. A separate `Arc`: the watch loop waits on it without keeping `Inner` alive.
    pub(super) owner_hints: Arc<OwnerHints>,
    /// Listener of takeovers of ranges from another session — set by the node to report to
    /// the old owner over the network.
    owner_taken: Mutex<Option<OwnerTakenHook>>,
    /// The coordinator role and the latest read of `control/coordinator`
    /// (`coordinator.rs`).
    pub(super) coordinator: super::coordinator::Role,
    /// Reader of the `control/quiet-lease` gate and its last conclusion: on or not, read
    /// when (`None` = not read since the incarnation opened).
    pub(super) gate: crate::quiet_lease::Watch,
    pub(super) gate_seen: Mutex<(bool, Option<Instant>)>,
    /// Delegation enabled on the gate at the latest read — see `Session::delegation_on`.
    pub(super) gate_delegation: Mutex<bool>,
    /// The read fence loop saw our `solo` record change (challenged): the renewal loop
    /// self-checks at once instead of waiting for its tick.
    pub(super) solo_poke: Mutex<bool>,
    /// Coordinator messages waiting for the keeper (`delegate.rs`).
    pub(super) delegation: Mutex<super::delegate::Inbox>,
    /// When the renewal loop (`keeper_loop`) last woke — the only thing that answers
    /// challenges in `solo`. The coordinator fence in `solo` requires it to be fresh
    /// (`Session::coordinator_fence`).
    pub(super) keeper_tick: Mutex<Option<Instant>>,
}

/// A range the session holds, as the owner-record watch loop knows it.
pub(super) struct Watched {
    pub(super) session: SessionId,
    pub(super) validity: ValidityHandle,
    /// Version mark (etag) of the owner record the last time it was seen still ours — the
    /// watch loop GETs with `If-None-Match` set to it. `None` ⇒ unknown (the store returns
    /// no etag): a full GET.
    pub(super) marker: Option<String>,
}

/// The result of one attempt to claim a partition.
pub enum Claim {
    Won(LeadershipGrant, LeaseKeeper),
    /// Not claimed. `Some` = there is a live owner; `None` = someone just changed the
    /// record, or our session was just lost — retry after a tick.
    Held(Option<OwnerRecord>),
}

/// This node's session. Cheap `Clone`, every copy shares one session.
#[derive(Clone)]
pub struct Session(pub(super) Arc<Inner>);

impl Session {
    /// Opens the first incarnation, then runs the renewal and observation loops in the
    /// background — on [`LeaseConfig::runtime`] if set, otherwise the current runtime.
    ///
    /// Returns an error if the first incarnation cannot be opened: a node should not start
    /// when it has not yet written anything into the lease layer.
    pub async fn establish(
        store: Arc<dyn ObjectStore>,
        prefix: &Path,
        cfg: LeaseConfig,
    ) -> Result<Session> {
        Self::open(store, prefix, cfg, None).await
    }

    /// Like [`Self::establish`], retrying transient I/O errors of opening the first
    /// incarnation according to `policy` (the boot path). The `Create` retries **the same**
    /// `<inc>` with the same content: if the earlier attempt reached the store,
    /// `put_resolving` re-reads and finds ours (rule 7), instead of opening `<inc + 1>` and
    /// leaving a phantom `live` session behind.
    pub async fn establish_within(
        store: Arc<dyn ObjectStore>,
        prefix: &Path,
        cfg: LeaseConfig,
        policy: &crate::retry::Policy,
    ) -> Result<Session> {
        Self::open(store, prefix, cfg, Some(policy)).await
    }

    async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: &Path,
        cfg: LeaseConfig,
        policy: Option<&crate::retry::Policy>,
    ) -> Result<Session> {
        let runtime = cfg
            .runtime
            .clone()
            .unwrap_or_else(tokio::runtime::Handle::current);
        let tracker = SessionTracker::new(
            Arc::clone(&store),
            prefix,
            cfg.ttl,
            cfg.observe_interval.unwrap_or(cfg.renew_interval),
            cfg.quiet_list_every,
            &cfg.node_id,
        );
        let (fence, fence_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            store,
            prefix: prefix.clone(),
            cfg,
            state: Mutex::new(State::default()),
            live: watch::Sender::new(None),
            wake: Notify::new(),
            release_wake: Notify::new(),
            stop: watch::Sender::new(false),
            tracker: tracker.clone(),
            membership: Membership::new(tracker.clone()),
            tasks: Mutex::new(Vec::new()),
            watched: Mutex::new(BTreeMap::new()),
            owner_watch: Mutex::new(None),
            fence,
            fence_task: Mutex::new(None),
            owner_hints: Arc::new(OwnerHints::default()),
            owner_taken: Mutex::new(None),
            coordinator: Default::default(),
            gate: crate::quiet_lease::Watch::default(),
            gate_seen: Mutex::new((false, None)),
            solo_poke: Mutex::new(false),
            delegation: Mutex::new(Default::default()),
            gate_delegation: Mutex::new(false),
            keeper_tick: Mutex::new(None),
        });
        open_incarnation(&inner, policy).await?;
        let keeper = {
            let inner = Arc::clone(&inner);
            // The `session::renew::*` failpoints know the node.
            let node = inner.cfg.node_id.clone();
            runtime
                .spawn(async move {
                    crate::failpoint::in_node(
                        &node,
                        crate::io_source::tagged("session.renew", keeper_loop(inner)),
                    )
                    .await
                })
                .abort_handle()
        };
        // Release retries run in the background **separately**, not inside the renewal
        // loop: a hung release PUT (the S3 client retries for up to 180 s by itself) must
        // not make the next renewal miss — missing ⅔ TTL means losing the session and every
        // range.
        let releasing = {
            let inner = Arc::clone(&inner);
            let node = inner.cfg.node_id.clone();
            runtime
                .spawn(async move {
                    crate::failpoint::in_node(
                        &node,
                        crate::io_source::tagged("session.release", release_loop(inner)),
                    )
                    .await
                })
                .abort_handle()
        };
        let watching = tracker.spawn(&runtime);
        inner
            .tasks
            .lock()
            .expect("mutex")
            .extend([keeper, releasing, watching]);
        // The owner-record watch loop: one of our ranges whose owner record has moved to
        // another session (someone claimed it after our session was considered dead) ⇒
        // revoke the grant at once, even when the clock (layer 1) and the renewal loop
        // (layer 2) both fall short. This is layer 3 — once a manifest poll of **each** Db,
        // then a LIST of `owners/` per TTL; now the taker's report
        // ([`Self::owner_changed`]: a GET of exactly the reported range) plus the
        // [`DEFAULT_OWNER_WATCH`] safety net (a conditional GET of every range we hold).
        //
        // Deliberately **not** in `tasks`: `abort`/`CrashHandle` simulate the renewal loop
        // dying, and layer 3 must stand independently of it, as SlateDB's poll once did. It
        // exits when the session closes or `Inner` is gone.
        let owner_watch = runtime
            .spawn(crate::io_source::tagged(
                "session.owner_watch",
                owner_watch_loop(Arc::downgrade(&inner), Arc::clone(&inner.owner_hints)),
            ))
            .abort_handle();
        *inner.owner_watch.lock().expect("mutex") = Some(owner_watch);
        // The read fence on the leader: also outside `tasks` — a node "frozen" by
        // `CrashHandle` and then woken must meet the fence, like a real process.
        let fencing = runtime
            .spawn(crate::io_source::tagged(
                "session.read_fence",
                super::fence::read_fence_loop(Arc::downgrade(&inner), fence_rx),
            ))
            .abort_handle();
        *inner.fence_task.lock().expect("mutex") = Some(fencing);
        Ok(Session(inner))
    }

    pub fn node_id(&self) -> &str {
        &self.0.cfg.node_id
    }

    /// The HTTP address (internal port) this session declares.
    pub fn address(&self) -> Option<&str> {
        self.0.cfg.address.as_deref()
    }

    /// The ingress address this session declares.
    pub fn ingress(&self) -> Option<&str> {
        self.0.cfg.ingress.as_deref()
    }

    /// The loop observing sessions on the bucket — the source of "dead" for
    /// [`Self::claim`] and [`Self::is_session_dead`], and for tests (`refresh`). Product code
    /// outside the lease layer reads membership through [`Self::membership`]: its sources
    /// are no longer just this tracker.
    pub fn tracker(&self) -> SessionTracker {
        self.0.tracker.clone()
    }

    /// Cluster membership hints (who is alive, where, holding what) — see [`Membership`].
    pub fn membership(&self) -> Membership {
        self.0.membership.clone()
    }

    /// The renewal mode of the current incarnation according to the last confirmed write:
    /// [`SessionMode::Solo`] ⇔ the record on the bucket is our confirmed `solo` record and we
    /// renew by self-check GET.
    pub fn mode(&self) -> SessionMode {
        match self.0.state.lock().expect("mutex").current.as_ref() {
            Some(c) if c.solo => SessionMode::Solo,
            Some(c) => match &c.delegated {
                Some(d) => SessionMode::Delegated {
                    coord: d.coord.clone(),
                    term: d.term,
                },
                None => SessionMode::Direct,
            },
            None => SessionMode::Direct,
        }
    }

    /// The live incarnation, if any.
    pub fn current(&self) -> Option<SessionId> {
        self.0.live.borrow().as_ref().map(|(id, _)| id.clone())
    }

    /// Partition → the term this session holds (our own hint).
    pub fn owned(&self) -> BTreeMap<u32, u64> {
        self.0.state.lock().expect("mutex").owns.clone()
    }

    /// Changes whenever the incarnation changes (a new one opened, or lost).
    pub fn live_changes(&self) -> watch::Receiver<Option<(SessionId, ValidityHandle)>> {
        self.0.live.subscribe()
    }

    /// Tries to claim partition `p` — **one** decision, it does not measure time itself.
    ///
    /// Claimed when the owner record: does not exist yet; has been released; points at the
    /// current incarnation or a dropped one of ours; or points at a dead session (according
    /// to the tracker, or a direct GET finding it absent / ended). A session never seen is
    /// **not** considered dead — "not seen" only means not LISTed yet.
    ///
    /// Waits until there is a live incarnation.
    pub async fn claim(&self, p: PartitionId) -> Result<Claim> {
        self.claim_with(p, None, false).await
    }

    /// Like [`Self::claim`], but when the owner record **already belongs to the current
    /// incarnation** (not released) it writes nothing and returns `Held(Some(record))` — the
    /// caller compares the record's `session` with [`Self::current`] to know the range is
    /// already ours.
    ///
    /// For the merge coordinator gathering ranges onto its own node (D-e). `claim` on a
    /// record that is already ours rewrites it with a new term, so the keeper the node's own
    /// engine holds (the node's balancer just took the range from a dead owner) has an old
    /// `owner_version`: its next CAS — `writer_sealed` at quiesce, or `add_wal_segment` in
    /// the middle of an acquisition — misses. The "already ours" decision rests on the read
    /// the CAS is based on, so there is no window: an engine taking the range after this read
    /// makes the CAS here miss.
    pub async fn claim_from_others(&self, p: PartitionId) -> Result<Claim> {
        self.claim_with(p, None, true).await
    }

    /// Like [`Self::claim`], for a **child range** of a split/merge: the new owner record
    /// already carries `data` (= the smallest of the parents') — the child Db is a copy of
    /// the parent Db, so it is already at the parent's data version. An existing record
    /// keeps its own `data`.
    pub async fn claim_child(&self, p: PartitionId, data: Option<u32>) -> Result<Claim> {
        self.claim_with(p, data, false).await
    }

    async fn claim_with(
        &self,
        p: PartitionId,
        new_data: Option<u32>,
        keep_mine: bool,
    ) -> Result<Claim> {
        crate::failpoint!("session::claim", err);
        let inner = &self.0;
        let (id, root) = {
            let mut rx = inner.live.subscribe();
            let got = rx
                .wait_for(|v| v.is_some())
                .await
                .map_err(|_| StoreError::LeaseIo("session closed".into()))?;
            got.clone().expect("just waited for Some")
        };
        let observed = owner::read_observed(&inner.store, &inner.prefix, p).await?;
        // A new range uses the shared WAL (the native WAL is gone); a range that already has
        // a record keeps its WAL kind and index — the taker needs exactly those segments to
        // seal and replay. An old record carrying `slatedb` is read intact: the
        // `wal/shared` migration step needs to see it.
        // The old owner may still believe it holds the range (not released, not us): tell
        // it after the CAS wins.
        let mut taken_from: Option<(SessionId, Option<String>)> = None;
        let (expected, term, wal, data) = match observed {
            None => (
                None,
                1,
                OwnerWal {
                    kind: owner::WalKind::Shared,
                    segments: Vec::new(),
                    floor_id: None,
                },
                new_data,
            ),
            Some(o) => {
                let r = &o.record;
                // Tombstone: nobody claims it, not even us, even when the record also
                // carries `released`.
                if r.retired {
                    return Err(StoreError::RangeRetired { range: p.0 });
                }
                if keep_mine && r.session == id && !r.released {
                    return Ok(Claim::Held(Some(r.clone())));
                }
                let mine = r.session == id
                    || inner
                        .state
                        .lock()
                        .expect("mutex")
                        .abandoned
                        .contains(&r.session);
                if !(r.released || mine || self.session_is_dead(&r.session).await?) {
                    return Ok(Claim::Held(Some(r.clone())));
                }
                // Claimable — but only if we know how to read its WAL (D-f). Write nothing:
                // the record must stay intact for a binary that understands it.
                if let owner::WalKind::Unknown(kind) = &r.wal_kind {
                    return Err(StoreError::UnknownWalKind {
                        range: p.0,
                        kind: kind.clone(),
                    });
                }
                if !(r.released || mine) {
                    taken_from = Some((r.session.clone(), r.address.clone()));
                }
                let wal = self
                    .reclaim_wal(p, &id, r)
                    .unwrap_or_else(|| OwnerWal::of(r));
                (Some(o.version), r.term + 1, wal, r.data)
            }
        };
        let record = OwnerRecord {
            term,
            session: id.clone(),
            address: inner.cfg.address.clone(),
            released: false,
            wal_kind: wal.kind.clone(),
            retired: false,
            wal_segments: wal.segments.clone(),
            wal_floor_id: wal.floor_id,
            data,
            retired_ms: None,
        };
        let Some(version) =
            owner::write_owner(&inner.store, &inner.prefix, p, &record, expected).await?
        else {
            return Ok(Claim::Held(None));
        };
        // The owner record is written, the session's liveness not yet checked: tests lose
        // (or suspend) the session in this window.
        crate::failpoint!("session::claim::after_write");
        {
            let mut st = inner.state.lock().expect("mutex");
            // The session was lost during the write: the record just written points at a
            // dropped incarnation — anyone can claim it back, us included.
            if st.current.as_ref().map(|c| &c.id) != Some(&id) || !root.is_valid() {
                return Ok(Claim::Held(None));
            }
            st.owns.insert(p.0, term);
            st.pending_release.remove(&p.0);
        }
        inner.wake.notify_one();
        let grant = LeadershipGrant::from_session(p, term, &root);
        inner.watched.lock().expect("mutex").insert(
            p.0,
            Watched {
                session: id.clone(),
                validity: grant.validity_handle(),
                marker: version.e_tag.clone(),
            },
        );
        if let Some((from, address)) = taken_from {
            let hook = inner.owner_taken.lock().expect("mutex").clone();
            if let Some(hook) = hook {
                hook(OwnerTaken {
                    range: p,
                    from,
                    address,
                    term,
                });
            }
        }
        let keeper = LeaseKeeper {
            inner: Arc::clone(inner),
            partition: p,
            term,
            session: id,
            validity: grant.validity_handle(),
            owner_version: Some(version),
            wal,
            data,
            wal_clean: false,
            shared: None,
            unserved_segment: false,
        };
        Ok(Claim::Won(grant, keeper))
    }

    /// **Taking a range back ourselves after a release that never reached the bucket.**
    /// The owner record `r` still belongs to this very incarnation `id`, not released, and a
    /// release of an earlier keeper is waiting in the background (`pending_release`) —
    /// usually: the seal finished, but both the clean-record CAS
    /// (`LeaseKeeper::writer_sealed`) and the release missed. The record still lists segments
    /// pointing at our **live** log; taking it back as is, `take_over` would hit C1 and the
    /// whole node would fail-stop (18 of 27 `c1` fail-stops in one chaos run) — even though
    /// every frame of the range is already in L0.
    ///
    /// When the record still lists **exactly** the segments the keeper knows (`held`), the
    /// takeover CAS writes that release's WAL part instead of the WAL part on the bucket: the
    /// result on the bucket is as if the release had arrived first and the takeover after —
    /// an interleaving that can happen anyway (the background loop winning the CAS). An
    /// unknown segment the keeper does not know (an ambiguous segment add) ⇒ `None`: keep the
    /// bucket's WAL part as before. A pending tombstone ⇒ `None`.
    fn reclaim_wal(&self, p: PartitionId, id: &SessionId, r: &OwnerRecord) -> Option<OwnerWal> {
        if r.session != *id || r.released {
            return None;
        }
        let st = self.0.state.lock().expect("mutex");
        let pending = st.pending_release.get(&p.0)?;
        if pending.session != *id
            || pending.term != r.term
            || pending.retired
            || pending.held != r.wal_segments
        {
            return None;
        }
        let mut wal = OwnerWal::of(r);
        wal.segments = pending.wal.segments.clone();
        wal.floor_id = wal.floor_id.max(pending.wal.floor_id);
        Some(wal)
    }

    /// Sets the listener of **takeovers of ranges from another session**: after each winning
    /// takeover CAS on an unreleased record of another session, `hook` is called —
    /// synchronously, on `claim`'s path, so it must return at once (the node only pushes a
    /// report into a background task, never calls the network in place: `claim` runs in the
    /// engine's factory — D16). Setting it again replaces the old one.
    pub fn on_owner_taken(&self, hook: impl Fn(OwnerTaken) + Send + Sync + 'static) {
        *self.0.owner_taken.lock().expect("mutex") = Some(Arc::new(hook));
    }

    /// The report "our range `range` may have just changed owner": the owner-record watch
    /// loop GETs that record at once (if we still hold the range), without waiting for the
    /// safety net. The report is not trusted: only a bucket read finding the record no
    /// longer ours revokes the grant (LS-3 — the network is not a source of truth), so a
    /// false report only costs one GET.
    pub fn owner_changed(&self, range: PartitionId) {
        let hints = &self.0.owner_hints;
        hints.ranges.lock().expect("mutex").insert(range.0);
        hints.notify.notify_one();
    }

    /// Whether session `id` is dead — according to the tracker (silent for more than a TTL),
    /// or a direct GET finding it gone / ended. A session never seen is **not** considered
    /// dead. Used for locks outside the owner record (the migration runner).
    pub async fn is_session_dead(&self, id: &SessionId) -> Result<bool> {
        self.session_is_dead(id).await
    }

    pub(super) async fn session_is_dead(&self, id: &SessionId) -> Result<bool> {
        if self.0.tracker.is_dead(id) {
            return Ok(true);
        }
        Ok(
            match read_session(&self.0.store, &self.0.prefix, id).await? {
                None => true,
                Some((Ok(rec), _)) => rec.state.is_terminal(),
                // The record cannot be read: let the tracker handle it after a silent TTL.
                Some((Err(_), _)) => false,
            },
        )
    }

    /// Closes the session: stops renewing, revokes every grant, writes `closed` so others can
    /// claim the partitions at once without waiting a TTL.
    pub async fn close(&self) {
        // Release the coordinator role **before** closing the session: other nodes see
        // `released` and take it at once, without waiting to see the session end.
        self.release_coordinator().await;
        let inner = &self.0;
        inner.stop.send_replace(true);
        for t in inner.tasks.lock().expect("mutex").drain(..) {
            t.abort();
        }
        if let Some(t) = inner.owner_watch.lock().expect("mutex").take() {
            t.abort();
        }
        if let Some(t) = inner.fence_task.lock().expect("mutex").take() {
            t.abort();
        }
        let current = inner.state.lock().expect("mutex").current.take();
        inner.live.send_replace(None);
        if let Some(c) = current {
            c.root.revoke();
            write_terminal(inner, &c, SessionState::Closed).await;
        }
    }

    /// **Fail-stops** incarnation `id`: revokes every grant, writes `closed` so others can
    /// claim at once, then the renewal loop opens a new incarnation — with a new WAL log.
    /// Does nothing if `id` is no longer the current incarnation (already lost earlier).
    ///
    /// Called when the incarnation's shared WAL log has been sealed, when one of its PUTs has
    /// an unknown outcome, or when the watchdog sees it stalled: from then on no frame of this
    /// incarnation is acked any more, and all its ranges must go to whoever seals the log.
    pub async fn fail_stop(
        &self,
        id: &SessionId,
        cause: crate::failstop::Cause,
        reason: &'static str,
    ) {
        let current = self
            .0
            .state
            .lock()
            .expect("mutex")
            .current
            .as_ref()
            .map(|c| c.id.clone());
        if current.as_ref() == Some(id) {
            lose(&self.0, cause, reason, true).await;
            self.0.wake.notify_one();
        }
    }

    /// Stops renewal and observation **without** writing anything — like `kill -9`. Grants
    /// expire by their deadline; others mark the session after a TTL.
    pub fn abort(&self) {
        for t in self.0.tasks.lock().expect("mutex").iter() {
            t.abort();
        }
    }

    /// A failure-building tool: stops renewal **and** observation, pins the validity — a
    /// frozen node that still believes it is the leader (kill -9, GC pause).
    pub fn crash_handle(&self) -> CrashHandle {
        CrashHandle {
            inner: Arc::clone(&self.0),
            validity: None,
        }
    }
}
