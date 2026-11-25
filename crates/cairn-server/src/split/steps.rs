// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Steps shared by split and merge: the upgrade gate, WAL, L0, taking children, gathering ranges, rolling back in place.

use super::*;

impl RangeOps {
    /// **Upgrade gate**: a cluster-level migration runner holding the `upgrading`
    /// lock ⇒ split/merge waits (`Busy`, `503`). Called before the intent CAS (a
    /// clean refusal, writes nothing) and right after it (closes the gap with a
    /// runner that just took the lock; an error there takes the roll-back path).
    pub(super) async fn refuse_while_upgrading(&self) -> OpResult<()> {
        match cairn_store::migrate::upgrading(
            self.stores.main().store(),
            self.stores.main().prefix(),
        )
        .await?
        {
            Some(u) => Err(RangeOpError::Busy(format!(
                "cluster is upgrading ({u}) — split/merge paused until the migration finishes, retry later"
            ))),
            None => Ok(()),
        }
    }

    /// Every quiesced source must have an owner record **without `wal_segment` any
    /// more** before clone/union (SM-4): that is exactly the question
    /// `SharedWalAdmin::is_empty` will ask, and answering "no" makes SlateDB refuse
    /// the source (`InvalidCloneSourceWithWal`, which reaches the client as `500`). A
    /// segment left after a successful seal means the "clean record" CAS of
    /// `writer_sealed` failed (a flaky store, or the keeper lost the record) —
    /// transient: roll back and call again, and the engine's hand-over cleans that
    /// segment. Hence `Busy` (`503`), not `Failed`.
    /// The native WAL has no segments; F3 in the fork is still the net for it.
    pub(super) async fn ensure_wal_drained(&self, sources: &[PartitionId]) -> OpResult<()> {
        let (store, prefix) = (self.stores.main().store(), self.stores.main().prefix());
        for p in sources {
            let rec = cairn_store::read_owner(store, prefix, *p).await?;
            if let Some(r) = rec.filter(|r| !r.wal_segments.is_empty()) {
                tracing::warn!(
                    range = p.0,
                    segments = r.wal_segments.len(),
                    "source range quiesced but its owner record still has wal_segment — rolling back, retry later"
                );
                return Err(RangeOpError::Busy(format!(
                    "range {} has not drained the shared WAL (owner record still has {} wal_segment after seal) — \
                     retry",
                    p.0,
                    r.wal_segments.len()
                )));
            }
        }
        Ok(())
    }

    /// Engine of range `e` on this node, **if it is leading**.
    pub(super) async fn leader_handle(&self, e: &RangeEntry) -> OpResult<EngineHandle> {
        let not_leader = || RangeOpError::NotLeader {
            range: e.id.0,
            // A hint for the **client** — the ingress port, not the internal port.
            leader: self.routing.leader_of(e.id).and_then(|l| l.ingress),
        };
        let h = self.partitions.get(e.id).ok_or_else(not_leader)?;
        if is_leader(&h).await {
            Ok(h)
        } else {
            Err(not_leader())
        }
    }

    /// L0 precondition (step 0 of the protocol), **counting views**: however many
    /// L0 views each new Db — one element of `new_dbs`, made of its sources
    /// `(range, pk span)` — inherits, it must still leave room for two more flushes
    /// under `l0_max_ssts`. Hitting the limit makes SlateDB stop writes until a compaction tick.
    pub(super) async fn ensure_l0_headroom(
        &self,
        new_dbs: &[&[(PartitionId, (PartitionKey, PartitionKey))]],
    ) -> OpResult<()> {
        let max = self.stores.l0_max_ssts();
        for sources in new_dbs {
            let mut views = 0;
            for (src, span) in sources.iter() {
                views += self.stores.projected_l0_views(*src, *span).await?;
            }
            if views + 2 > max {
                return Err(RangeOpError::Busy(format!(
                    "L0 has no room yet: the new range would inherit {views} L0 views against a limit of {max} — wait \
                     for compaction and retry"
                )));
            }
        }
        Ok(())
    }

    /// Take the child ranges in the node's session (step 6 of the protocol) — `Create`: the
    /// children are brand new, an owner record cannot already exist.
    ///
    /// The children's owner record already carries `data` = the lowest of the parents':
    /// a child Db is a copy (or union) of the parent Db, so it is already at the data
    /// version of the slowest parent — no range-level step runs again.
    pub(super) async fn claim_children(
        &self,
        parents: &[PartitionId],
        children: &[PartitionId],
    ) -> OpResult<Vec<LeaseKeeper>> {
        let session = &self.session;
        let mut data: Option<u32> = None;
        for p in parents {
            // A parent without a record or without `data` ≡ 0.
            let d = cairn_store::read_owner(
                self.stores.main().store(),
                self.stores.main().prefix(),
                *p,
            )
            .await?
            .and_then(|r| r.data)
            .unwrap_or(0);
            data = Some(data.map_or(d, |m| m.min(d)));
        }
        let data = data.filter(|d| *d > 0);
        let mut keepers = Vec::new();
        for c in children {
            match session.claim_child(*c, data).await {
                Ok(Claim::Won(_, k)) => keepers.push(k),
                // `Held(None)`: the node's session was just lost or is suspended
                // in the middle of writing the owner record (the record points to an
                // incarnation that was abandoned or is no longer valid — the roll-back
                // cleans it up together with the child Db), or the record just changed
                // underfoot (`Create` really lost — our own ambiguous outcome has already
                // been resolved as a win by `put_resolving`; a brand new child has no other
                // writer: `factory` only opens ranges in the table). Both are transient:
                // `503`, the caller retries (SM-7).
                Ok(Claim::Held(None)) => {
                    forget(keepers);
                    return Err(RangeOpError::Busy(format!(
                        "cannot take child range {}: the node's session was just lost or is suspended, \
                         or the owner record just changed — rolled back, retry",
                        c.0
                    )));
                }
                // Another live session holds a brand new child: abnormal — `500`.
                Ok(Claim::Held(Some(by))) => {
                    forget(keepers);
                    return Err(RangeOpError::Failed(format!(
                        "cannot take child range {}: it is at {:?}",
                        c.0, by.session
                    )));
                }
                Err(e) => {
                    forget(keepers);
                    return Err(e.into());
                }
            }
        }
        Ok(keepers)
    }

    /// Gather range `e` onto this node (D-e): if it is already led here, done; otherwise
    /// ask its owner to hand it over (`released`), then take it in the node's session —
    /// the range has an intent, so other nodes' balancers do not fight over it — and
    /// wait for this node's engine to become its leader.
    ///
    /// The range still being in someone else's hands is a **legitimate race**, not a
    /// failure: the owner has not handed it over yet (slow store), or a node took it
    /// after the rescue grace period, saw an intent it does not coordinate and rolled
    /// it back (SM-3). While waiting, every [`INTENT_RECHECK`] we ask the bucket whether
    /// `intent` (on range `ours.0`) is still ours — rolled back ⇒ stop at once; `settle`
    /// expired ⇒ stop as well. Both are `Busy` (`503`): the coordinator rolls back, the
    /// caller retries.
    pub(super) async fn gather_here(
        &self,
        e: &RangeEntry,
        ours: (PartitionId, RangeIntent),
    ) -> OpResult<EngineHandle> {
        if let Ok(h) = self.leader_handle(e).await {
            return Ok(h);
        }
        let session = &self.session;
        // Ask the right owner **by the owner record** (the source of truth, D20) — the routing
        // table may still point at this very node from when it held the range earlier.
        let owner = cairn_store::read_owner(
            self.stores.main().store(),
            self.stores.main().prefix(),
            e.id,
        )
        .await
        .ok()
        .flatten()
        .filter(|r| !r.released && Some(&r.session) != session.current().as_ref());
        let addr = owner
            .and_then(|r| r.address)
            .or_else(|| self.routing.leader_of(e.id).and_then(|l| l.address));
        if let Some(addr) = addr {
            let url = format!(
                "{}/internal/ranges/{}/resign",
                addr.trim_end_matches('/'),
                e.id.0
            );
            let req = self.client.post(&url).timeout(Duration::from_secs(10));
            if let Err(err) = self.secret.send(&self.client, req).await {
                tracing::debug!(range = e.id.0, error = %err, "cannot ask the old owner to hand over");
            }
        }
        let t0 = Instant::now();
        let mut checked = t0;
        loop {
            // Reduce the result before any `await`: the grant inside `Claim` is `!Send`.
            //
            // `claim_from_others`, not `claim`: if the old owner died, this node's own
            // balancer may already have taken the range (the owner record is already ours)
            // and the engine is opening it. `claim` would rewrite the record with a new term,
            // the old engine's keeper would go, `writer_sealed` at quiesce would miss, the
            // shared WAL segment would stay and the union would refuse the source. If the
            // range is already ours, just wait for the engine to become leader.
            let held = match session.claim_from_others(e.id).await? {
                Claim::Won(_, k) => {
                    k.forget();
                    None
                }
                Claim::Held(Some(r)) if Some(&r.session) == session.current().as_ref() => None,
                Claim::Held(by) => Some(by),
            };
            let Some(by) = held else { break };
            if checked.elapsed() >= INTENT_RECHECK {
                checked = Instant::now();
                self.ensure_intent_ours(cairn_split::Stage::Gather, ours.0, ours.1)
                    .await?;
            }
            if t0.elapsed() >= self.settle {
                let holder = by.map(|r| r.session);
                return Err(RangeOpError::Busy(format!(
                    "range {} did not come to this node after {:?} (it is at {holder:?}) — rolled back, retry",
                    e.id.0, self.settle
                )));
            }
            cairn_store::pace::poll_gap(Duration::from_millis(50)).await;
        }
        self.wait_led_here(e.id).await;
        self.leader_handle(e).await
    }

    /// Wait (bounded) until the engine of range `p` on this node is leading **and**
    /// has recovered its queues (`restore_flow` done, the slot-granting gate open).
    ///
    /// The second half is FL-31: the `flow` probe command goes ahead of the background
    /// work at the start of a term, so "answers commands" no longer implies "the VO lock
    /// rebuild is done" as it did when the mutex was purely FIFO — the report would carry
    /// `restoreMs` = null. Once there is a leader we only read the node's measurement
    /// (`restore_ms` set at the end of `restore_flow`, cleared when each term opens) and
    /// send no more commands: a probe every 2 ms would make the rebuild yield to it
    /// (`YIELD_MAX`). If the rebuild fails the range steps down, and the wait runs out
    /// `settle` as when there was no leader.
    pub(super) async fn wait_led_here(&self, p: PartitionId) {
        let t0 = Instant::now();
        let mut led = false;
        while t0.elapsed() < self.settle {
            if !led {
                if let (Some(h), Some(_)) = (self.partitions.get(p), self.cluster.range(p)) {
                    led = is_leader(&h).await;
                }
            }
            if led && self.stats.get(p.0).restore_ms.is_some() {
                return;
            }
            // Small probe interval: the duration of the report's `open` phase is no finer
            // than this interval.
            cairn_store::pace::poll_gap(Duration::from_millis(2)).await;
        }
        tracing::warn!(
            range = p.0,
            "new range has no leader on the coordinating node after {:?}",
            self.settle
        );
    }

    /// The intent CAS did not clearly win: an error that reading back cannot resolve
    /// either (unknown whether the write landed), or a loss where the winner's table still
    /// carries our intent (someone changing the table after our write copied it over).
    /// Our intent may be lying on the bucket, and nobody else rolls it back while the
    /// range's leader is alive — auto-split stops cluster-wide. Re-read the table; if the
    /// range still carries **exactly** our intent, roll it back.
    ///
    /// A failed re-read or roll-back (a 503 spell on `cluster.conf` still going on)
    /// **retries in the background** instead of giving up (bounded by `SETTLE_FOR`; the
    /// intent reconciliation round takes over after that).
    pub(super) async fn undo_stray_intent(&self, p: PartitionId, intent: RangeIntent) {
        match roll_back_if(&self.cluster, &self.stores, p, intent, Who::StrayUndo).await {
            Ok(true) => {
                tracing::warn!(
                    range = p.0,
                    ?intent,
                    "intent CAS with an unknown outcome left our intent on the table — rolled back"
                );
                self.discard_own_orphans(intent).await;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(range = p.0, error = %e, "intent CAS with an unknown outcome not resolved yet — retrying in the background");
                if core::settle_after(Who::StrayUndo) {
                    self.settle_in_background(p, intent);
                }
            }
        }
    }

    /// **Is our intent still on the table — reading the bucket.** A coordinator that hangs
    /// (or loses its session) longer than 2×TTL has its source range taken by another node,
    /// which rolls it back: CAS the intent away, clean up the children it sees at that point.
    /// A coordinator that wakes up and keeps creating child Dbs or owner records leaks them:
    /// the Db and its final checkpoint on the live source range, an owner record pointing at
    /// its **live** session (SM-11 keeps it forever with `ownerLive`). Ask before each
    /// child-creating step. A gap between the question and the creating step remains;
    /// [`Self::roll_back_here`] and the orphan-child round (SM-14) are the net for it.
    ///
    /// The core decides (`Event::Enter`): `Go` ⇒ go on, with the ledger phase to write
    /// **before** that step (`None` = no ledger change); otherwise ⇒ `Busy`, the coordinator
    /// rolls back.
    pub(super) async fn ensure_intent_ours(
        &self,
        stage: cairn_split::Stage,
        p: PartitionId,
        intent: RangeIntent,
    ) -> OpResult<Option<Phase>> {
        self.cluster.reload().await?;
        core::stage(
            stage,
            core::standing(&self.cluster.table(), core::op_of(p, intent)),
            p,
        )
    }

    /// The coordinator rolls back by itself after a failed step: drop the intent, clean up
    /// the children, then let the quiesced engines reopen the range (`Resign` on a quiescing range).
    ///
    /// If the intent was already rolled back by someone else, `roll_back` does nothing
    /// — it only cleans up the children of the intent it sees, and whoever rolled back on
    /// our behalf only cleaned up what existed at that point. Children **we** created
    /// afterwards are known only to us: clean them up when provable (SM-14).
    ///
    /// Only roll back **exactly** our intent ([`roll_back_matching`]): another intent on the
    /// range (someone rolled ours back and split again) is not our business.
    /// A failed roll-back is retried in the background ([`Self::settle_in_background`]) —
    /// it used to leave only an `error!` line, and for a merge that failed before quiesce
    /// nobody `Resign`ed and no `factory` ran: the intent stayed until the next leader change.
    pub(super) async fn roll_back_here(
        &self,
        p: PartitionId,
        intent: RangeIntent,
        quiesced: &[&EngineHandle],
    ) {
        let undone = roll_back_matching(
            &self.cluster,
            &self.stores,
            p,
            Some(intent),
            cairn_split::Who::Coordinator,
        )
        .await;
        if let Err(e) = &undone {
            tracing::warn!(range = p.0, error = %e, "cannot roll back the intent — retrying in the background");
        }
        if core::own_orphans_after_failure() {
            self.discard_own_orphans(intent).await;
        }
        for h in quiesced {
            let _ = h.resign().await;
        }
        if undone.is_err() && core::settle_after(Who::Coordinator) {
            self.settle_in_background(p, intent);
        }
    }

    /// Roll back our `intent` on `p` **in the background**, retrying with exponential
    /// backoff (200 ms → 10 s) until a fresh table no longer carries it, within the limit
    /// [`SETTLE_FOR`]. A task of the node: holds no guard — the intent on the table
    /// already blocks every new operation on `p`, and a `factory` rolling back in parallel
    /// does the same CAS (idempotent). Past the limit the intent reconciliation round
    /// (SM-15) takes over.
    fn settle_in_background(&self, p: PartitionId, intent: RangeIntent) {
        let ops = self.clone();
        spawn_in_node(async move {
            // Pause 200 ms before the first attempt (the in-place roll-back just failed), then
            // the shared primitive: 400 ms doubling up to 10 s, jitter, every error transient
            // — within the limit [`SETTLE_FOR`].
            cairn_store::pace::retry_pause(Duration::from_millis(200)).await;
            let undone = cairn_store::retry::run_classified(
                &cairn_store::retry::ops::SPLIT_SETTLE,
                &SETTLE_POLICY,
                cairn_store::retry::Never,
                |_| cairn_store::retry::Verdict::Transient,
                || roll_back_if(&ops.cluster, &ops.stores, p, intent, Who::Settle),
            )
            .await;
            match undone {
                Ok(undone) => {
                    if undone {
                        tracing::info!(range = p.0, ?intent, "rolled back intent in background");
                    }
                    ops.discard_own_orphans(intent).await;
                }
                Err(e) => {
                    tracing::error!(range = p.0, error = %e, "background intent roll-back exceeded {SETTLE_FOR:?} — the intent reconciliation round takes over");
                }
            }
        });
    }

    /// Whether this node has a coordinator holding `p` — the answer of
    /// `GET /internal/ranges/{id}/coordinator` (SM-15).
    pub fn coordinates(&self, p: PartitionId) -> bool {
        self.active.contains(p)
    }

    /// Whether any coordinator holds an intent on `sources`, asking the node leading each
    /// source: a coordinator always leads its sources (split: the parent range; merge: at
    /// least one side, the other is gathered over), and a node that took a source from it
    /// has already rolled back in `factory` (SM-3). `Err` = could not ask — the caller
    /// treats it as **held**.
    pub(super) async fn intent_held(
        &self,
        sources: &[PartitionId],
    ) -> std::result::Result<bool, String> {
        for s in sources {
            if self.active.contains(*s) {
                return Ok(true);
            }
            let Some(leader) = self.routing.leader_of(*s) else {
                // Nobody leads it: whoever takes it will roll back in `factory`.
                continue;
            };
            if leader.node_id == self.node_id {
                continue;
            }
            let Some(addr) = leader.address else {
                return Err(format!(
                    "leader {} of range {} advertises no address",
                    leader.node_id, s.0
                ));
            };
            let url = format!(
                "{}/internal/ranges/{}/coordinator",
                addr.trim_end_matches('/'),
                s.0
            );
            let req = self.client.get(&url).timeout(Duration::from_secs(2));
            let held = async {
                let r = self
                    .secret
                    .send(&self.client, req)
                    .await
                    .map_err(|e| e.to_string())?;
                if !r.status().is_success() {
                    return Err(format!("{url}: {}", r.status()));
                }
                let v: serde_json::Value = r.json().await.map_err(|e| e.to_string())?;
                v["held"].as_bool().ok_or_else(|| format!("{url}: {v}"))
            };
            if held.await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
