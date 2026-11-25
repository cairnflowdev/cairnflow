// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`LeaseKeeper`] — writes the owner record, shared WAL segments, retirement;
//! [`CrashHandle`].

use super::*;

// ───────────────────────────────────────────────────────────── keeper

/// Leadership of **one** partition under the node's session. The same API as the keeper of
/// the old per-partition lease, so the actor does not change.
///
/// Dropping it without [`Self::release`] still yields: the session writes the "released"
/// record on its next tick.
pub struct LeaseKeeper {
    pub(super) inner: Arc<Inner>,
    pub(super) partition: PartitionId,
    pub(super) term: u64,
    pub(super) session: SessionId,
    pub(super) validity: ValidityHandle,
    /// `None` once yielded (or handed to the session to yield).
    pub(super) owner_version: Option<UpdateVersion>,
    /// The WAL part of the owner record as we last wrote it.
    pub(super) wal: OwnerWal,
    /// `data=` of the owner record as we last wrote it.
    pub(super) data: Option<u32>,
    /// The range's Db has finished sealing — every frame of the range is in L0 — so the
    /// release may clear `wal_segment`. When not set, the release **keeps** the segments:
    /// the taker seals the log and replays, nothing is lost.
    pub(super) wal_clean: bool,
    /// The shared WAL the range's Db is writing to — set by [`crate::acquire_shared`];
    /// `None` with the native WAL.
    pub(super) shared: Option<crate::nodewal::NodeWal>,
    /// The last segment in `wal` is ours, just added, and the Db has **not** been handed to
    /// anyone yet (`acquire_shared` failed or was cancelled after CASing the segment): our
    /// log has no frame of the range. A release drops that segment — keeping it would make
    /// the next taker seal our **live** log (killing the whole node), and if we took the
    /// range back ourselves we would hit rule C1 (found in simulation, seed 48).
    pub(super) unserved_segment: bool,
}

impl LeaseKeeper {
    pub fn term(&self) -> u64 {
        self.term
    }

    /// The session that claimed the range — the range's shared WAL log lives under this
    /// incarnation.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// The WAL part of the owner record: the WAL kind and the range's WAL index.
    pub fn wal(&self) -> &OwnerWal {
        &self.wal
    }

    /// CASes the new WAL part onto the owner record. The CAS misses ⇒ someone claimed the
    /// range after us ⇒ [`StoreError::LeaseLost`]: the range was lost behind our back, and
    /// with the shared WAL that is a reason to fail-stop the whole incarnation
    /// (counterexample C1).
    async fn write_wal(&mut self, wal: OwnerWal) -> Result<()> {
        self.write_record(wal, self.data).await
    }

    /// The data version of the range Db according to the owner record (`data=`; absent ≡ 0).
    pub fn data(&self) -> u32 {
        self.data.unwrap_or(0)
    }

    /// CASes the new `data=` onto the owner record — **after** the range-tier migration
    /// steps are durable. A miss ⇒ [`StoreError::LeaseLost`] like every keeper write.
    pub async fn set_data(&mut self, data: u32) -> Result<()> {
        self.write_record(self.wal.clone(), Some(data)).await
    }

    /// CASes the new WAL kind onto the owner record — the `wal/shared` migration step,
    /// **after** the `wal_name` mark in the manifest has changed. Only the kind changes: a
    /// native-WAL range has no segments, the floor stays. A miss ⇒ [`StoreError::LeaseLost`]
    /// like every keeper write; ambiguous ⇒ re-read (rule 7, [`Self::write_record`]).
    pub async fn set_wal_kind(&mut self, kind: owner::WalKind) -> Result<()> {
        let wal = OwnerWal {
            kind,
            ..self.wal.clone()
        };
        self.write_record(wal, self.data).await
    }

    /// CASes the owner record carrying `wal` and `data`. See [`Self::write_wal`].
    async fn write_record(&mut self, wal: OwnerWal, data: Option<u32>) -> Result<()> {
        let Some(version) = self.owner_version.clone() else {
            return Err(StoreError::LeaseLost);
        };
        let record = OwnerRecord {
            term: self.term,
            session: self.session.clone(),
            address: self.inner.cfg.address.clone(),
            released: false,
            wal_kind: wal.kind.clone(),
            retired: false,
            wal_segments: wal.segments.clone(),
            wal_floor_id: wal.floor_id,
            data,
            retired_ms: None,
        };
        let written = owner::write_owner(
            &self.inner.store,
            &self.inner.prefix,
            self.partition,
            &record,
            Some(version),
        )
        .await;
        match written {
            Ok(Some(v)) => {
                self.note_written(&v);
                self.owner_version = Some(v);
                self.wal = wal;
                self.data = data;
                Ok(())
            }
            Ok(None) => Err(StoreError::LeaseLost),
            // A PUT with an unknown outcome (timeout, error after the write): **re-read**.
            // Treating it as a miss when it really was written would leave the record
            // pointing at our log while we believe it does not — the range is stuck until we
            // take it back ourselves and fail-stop (C1). Found in simulation, seed 2.
            Err(e) => {
                match owner::read_observed(&self.inner.store, &self.inner.prefix, self.partition)
                    .await
                {
                    Ok(Some(o)) if o.record == record => {
                        self.note_written(&o.version);
                        self.owner_version = Some(o.version);
                        self.wal = wal;
                        self.data = data;
                        Ok(())
                    }
                    Ok(Some(o))
                        if o.record.session != self.session || o.record.term != self.term =>
                    {
                        Err(StoreError::LeaseLost)
                    }
                    _ => Err(e),
                }
            }
        }
    }

    /// The owner record has just been rewritten by **this very keeper** (`version`): the
    /// owner-record watch loop remembers the new version mark, so the next conditional GET
    /// (`If-None-Match`) gets a `304` instead of downloading the record again. Without it,
    /// every takeover would cost one extra full GET per range on the next watch tick — for a
    /// lone node, that is the whole marginal idle cost of a range. Only changes entries that
    /// still belong to this session; overwriting the mark with the mark of a watch round in
    /// flight only costs one extra GET, and never hides a change of owner (someone else's
    /// mark always differs from ours).
    fn note_written(&self, version: &UpdateVersion) {
        let mut w = self.inner.watched.lock().expect("mutex");
        if let Some(entry) = w.get_mut(&self.partition.0) {
            if entry.session == self.session {
                entry.marker = version.e_tag.clone();
            }
        }
    }

    /// Adds our segment to `wal_segment` — **before** our first frame for this range
    /// (counterexample C4).
    pub async fn add_wal_segment(&mut self, seg: owner::WalSegment) -> Result<()> {
        let mut wal = self.wal.clone();
        if wal.segments.last() == Some(&seg) {
            // The record already lists exactly our segment — an ambiguous segment add of an
            // earlier acquisition that this takeover inherits. The caller only gets here
            // when `take_over` skipped it because the stream had no frame of the range yet
            // (with frames it would already be C1): the segment is **unserved**, like a
            // segment just CASed. Without marking it, a failed acquisition after this would
            // yield dirty keeping it — the keeper has no `shared` yet so it does not rotate —
            // and the taker would seal the node's live log.
            self.unserved_segment = true;
            return Ok(());
        }
        wal.segments.push(seg);
        self.write_wal(wal).await?;
        self.unserved_segment = true;
        Ok(())
    }

    /// Drops every **leading** segment lying entirely below `replay_after` — segment `i`
    /// ends at the `first_wal_id` of segment `i+1` minus 1 (counterexample C7). The last
    /// segment is never dropped here.
    ///
    /// Gate: the mark is the [`crate::proof::DurableReplayMark`] of exactly this range — read
    /// from the stored manifest — plus the cover `covered` the caller derived from it (never
    /// beyond what the frames read prove, see `acquire_shared_with_hint`).
    pub async fn prune_wal_segments(
        &mut self,
        mark: crate::proof::DurableReplayMark,
        covered: u64,
    ) -> Result<()> {
        if mark.range() != self.partition.0 {
            return Err(StoreError::Io(format!(
                "durable mark of range {} given to the owner record of range {}",
                mark.range(),
                self.partition.0
            )));
        }
        let replay_after = covered;
        let mut wal = self.wal.clone();
        let keep_from = (0..wal.segments.len().saturating_sub(1))
            .take_while(|&i| wal.segments[i + 1].first_wal_id.saturating_sub(1) <= replay_after)
            .count();
        if keep_from == 0 {
            return Ok(());
        }
        wal.segments.drain(..keep_from);
        self.write_wal(wal).await
    }

    /// The range's Db has finished sealing (every frame in L0): the next release or
    /// tombstone clears `wal_segment` and writes `wal_floor_id = floor_id`.
    pub fn mark_wal_clean(&mut self, floor_id: u64) {
        self.wal_clean = true;
        self.wal.floor_id = Some(floor_id.max(self.wal.floor_id.unwrap_or(0)));
    }

    pub(crate) fn attach_shared(&mut self, wal: crate::nodewal::NodeWal) {
        self.shared = Some(wal);
        self.unserved_segment = false;
    }

    /// The range's writer has **finished sealing** (`PartitionWriter::seal` returned `Ok`):
    /// every frame of the range is in L0. With the shared WAL: detach the Db from the node
    /// WAL and mark it clean with the last wal id the range used as the floor, so the next
    /// release / tombstone clears `wal_segment` (a cooperative handover replays nothing).
    /// With the native WAL: nothing to do.
    ///
    /// The owner record is CASed **at once** (dropping `wal_segment`, writing
    /// `wal_floor_id`), without waiting for the release: a quiesced range still holds its
    /// lease while the coordinator clones the Db, and the clone asks "does the Db's WAL still
    /// have rows" through this record (`SharedWalAdmin::is_empty`). The CAS misses ⇒ the
    /// range is lost — [`StoreError::LeaseLost`]; the clean mark is still kept for the
    /// release.
    ///
    /// The CAS is bounded by [`RELEASE_WRITE_BOUND`], like the release: the range is sealed,
    /// so waiting longer only keeps a range without a leader — the owner record still points
    /// at our live session, nobody competes. Past the bound ⇒ an error, the clean flag stays,
    /// and the release cleans up the segments; an abandoned CAS that actually landed is
    /// recognised by the release as our own record (`write_release`).
    pub async fn writer_sealed(&mut self) -> Result<()> {
        let Some(wal) = self.shared.take() else {
            return Ok(());
        };
        let p = self.partition.0;
        wal.forget_db(p);
        let Some(floor) = wal.last_wal_id(p) else {
            return Ok(());
        };
        self.mark_wal_clean(floor);
        let mut clean = self.wal.clone();
        clean.segments.clear();
        let r = match crate::retry::deadline(
            &crate::retry::ops::KEEPER_SEALED,
            RELEASE_WRITE_BOUND,
            self.write_wal(clean),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(StoreError::LeaseIo(format!(
                "clean owner record CAS took longer than {RELEASE_WRITE_BOUND:?} — the release will clean up the segments"
            ))),
        };
        if r.is_err() {
            // The owner record on the bucket still has segments pointing at a live log.
            wal.note_dirty_yield(p);
        }
        r
    }

    /// The range Db's seal failed or timed out while stepping down: the release after this
    /// leaves segments pointing at our live log in the owner record (a dirty yield) — count
    /// it and mark it in the shared WAL ([`crate::NodeWal::note_dirty_yield`]). With the
    /// native WAL: nothing to do.
    pub fn note_dirty_yield(&self) {
        if let Some(wal) = &self.shared {
            wal.note_dirty_yield(self.partition.0);
        }
    }

    /// The `stall_max` budget of the shared WAL the range's Db writes to; `None` with the
    /// native WAL. The seal bound on release must exceed it: the seal waits for the range's
    /// rows to become durable through this very log stream, and a seal past its bound is a
    /// **dirty** yield — the taker seals our live log, killing the whole node.
    pub fn wal_stall_max(&self) -> Option<Duration> {
        self.shared.as_ref().map(|w| w.config().stall_max())
    }

    /// The range's Db no longer writes through this node's shared WAL.
    fn detach_shared(&mut self) {
        if let Some(wal) = self.shared.take() {
            wal.forget_db(self.partition.0);
        }
    }

    /// The WAL part a release (or tombstone) will write onto the owner record.
    fn released_wal(&self) -> OwnerWal {
        let mut wal = self.wal.clone();
        if self.wal_clean {
            wal.segments.clear();
        } else if self.unserved_segment {
            // Our segment never carried any frame: drop it, keep the earlier segments (the
            // taker must still seal and replay them).
            wal.segments.pop();
        } else {
            // A dirty yield: the segments stay, and the taker seals our log — a rotated log
            // ([`Self::rotate_live_logs`]). The old `wal_floor_id` is still correct as a
            // lower bound.
        }
        wal
    }

    /// **A dirty yield does not seal a live log**: call **before** writing the owner record
    /// of a release. The record will still list a live log of this incarnation (the Db has
    /// not sealed cleanly: the seal failed or timed out while stepping down, the keeper was
    /// dropped midway) ⇒ rotate that log's stream
    /// ([`crate::NodeWal::rotate_for_dirty_yield`]): the stream stops acking and closes all
    /// its ranges before returning. The range taker seals a log that has stopped acking —
    /// the node lives; before this, the taker sealed a live log and the whole node
    /// fail-stopped, or this very node took the range back and hit C1.
    ///
    /// Does nothing on a clean release, when the Db has never served on this node's shared
    /// WAL (`shared` absent — an unserved segment is dropped, not listed), or when every
    /// segment that stays is a log of another incarnation or a rotated log. Also does nothing
    /// when the keeper's incarnation is over (session lost, fail-stop): its log is already a
    /// dead log, the taker seals it as usual, and a new stream under a dropped incarnation
    /// would only be garbage.
    fn rotate_live_logs(&self) {
        if self.wal_clean {
            return;
        }
        let Some(wal) = &self.shared else {
            return;
        };
        let current = self
            .inner
            .state
            .lock()
            .expect("mutex")
            .current
            .as_ref()
            .is_some_and(|c| c.id == self.session);
        if !current {
            return;
        }
        let own: Vec<crate::owner::LogId> = self
            .released_wal()
            .segments
            .into_iter()
            .map(|s| s.log)
            .filter(|l| l.session == self.session)
            .collect();
        if !own.is_empty() {
            wal.rotate_for_dirty_yield(self.partition.0, &own);
        }
    }

    fn pending(&self, version: UpdateVersion, retired: bool) -> PendingRelease {
        PendingRelease {
            session: self.session.clone(),
            term: self.term,
            version,
            retired,
            wal: self.released_wal(),
            held: self.wal.segments.clone(),
            data: self.data,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.validity.is_valid()
    }

    /// The grant is lost for good — unlike `!is_valid()` during the session's suspension
    /// window.
    pub fn is_lost(&self) -> bool {
        self.validity.is_lost()
    }

    /// Returns when the grant is **lost**: revoked, the session lost, or past the suspension
    /// cap. A suspension (past the deadline but still revivable) does not wake it.
    pub async fn lost(&self) {
        self.validity.lost().await
    }

    /// Yields: revokes the grant, writes "released" (waiting at most
    /// [`RELEASE_WRITE_BOUND`]); if the write does not make it, the session retries in the
    /// background — a partition must not be orphaned by one failed write.
    pub async fn release(mut self) {
        self.rotate_live_logs();
        self.detach_shared();
        self.validity.revoke();
        let Some(version) = self.owner_version.take() else {
            return;
        };
        let p = self.partition.0;
        let pending = self.pending(version, false);
        let write = write_release(&self.inner, p, &pending);
        match crate::retry::deadline(
            &crate::retry::ops::KEEPER_RELEASE,
            RELEASE_WRITE_BOUND,
            write,
        )
        .await
        {
            Ok(Ok(_)) => released(&self.inner, p, self.term),
            _ => self.hand_off_release(pending),
        }
    }

    /// Retirement: revokes the grant, writes a **tombstone** — `retired` + `released` — onto
    /// the owner record by CAS. From then on nobody claims this range again
    /// ([`Session::claim`] returns [`StoreError::RangeRetired`]).
    ///
    /// A write that does not make it within [`RELEASE_WRITE_BOUND`] ⇒ the session retries in
    /// the background and the function returns an error so the caller knows the tombstone
    /// is **not** necessarily on the bucket yet. The CAS misses (someone claimed the range
    /// after we lost the session) ⇒ [`StoreError::LeaseLost`]: the tombstone cannot be
    /// written, and the range is no longer ours to retire.
    pub async fn retire(mut self) -> Result<()> {
        self.detach_shared();
        self.validity.revoke();
        let Some(version) = self.owner_version.take() else {
            return Err(StoreError::LeaseLost);
        };
        // The SM-11 window: the engine has stopped leading, the tombstone is not written
        // yet. `return` here = the leader dies right at this point — nothing is written:
        // the keeper has dropped `owner_version`, so `Drop` does not yield either.
        crate::failpoint!("session::retire::before_write", err);
        let p = self.partition.0;
        let pending = self.pending(version, true);
        let write = write_release(&self.inner, p, &pending);
        match crate::retry::deadline(
            &crate::retry::ops::KEEPER_RELEASE,
            RELEASE_WRITE_BOUND,
            write,
        )
        .await
        {
            Ok(Ok(true)) => {
                released(&self.inner, p, self.term);
                Ok(())
            }
            Ok(Ok(false)) => Err(StoreError::LeaseLost),
            Ok(Err(e)) => {
                self.hand_off_release(pending);
                Err(e)
            }
            Err(_) => {
                self.hand_off_release(pending);
                Err(StoreError::LeaseIo(
                    "tombstone write timed out — the session retries in the background".into(),
                ))
            }
        }
    }

    fn hand_off_release(&self, pending: PendingRelease) {
        self.inner
            .state
            .lock()
            .expect("mutex")
            .pending_release
            .insert(self.partition.0, pending);
        self.inner.release_wake.notify_one();
    }

    /// Drops the keeper **without** yielding: the owner record still points at this
    /// session.
    ///
    /// For the split/merge coordinator: it claims the child ranges in the node's session so
    /// nobody else opens them, then lets the node's **engine** take them back as its own
    /// (`claim` sees a record pointing at the current incarnation). This keeper's grant is
    /// no longer used; the session keeps renewing.
    pub fn forget(mut self) {
        self.owner_version = None;
    }

    /// Stops **without** revoking, pinning the validity open — only for tests building
    /// failures.
    pub fn abort_without_revoking(mut self) -> ValidityHandle {
        self.owner_version = None;
        self.validity.pin_for_fault_injection();
        self.validity.clone()
    }

    /// A failure-building tool: stops renewal of **the whole session** and pins this
    /// partition's validity — a frozen process that still believes it is the leader.
    pub fn crash_handle(&self) -> CrashHandle {
        CrashHandle {
            inner: Arc::clone(&self.inner),
            validity: Some(self.validity.clone()),
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        if self.owner_version.is_some() {
            self.rotate_live_logs();
        }
        self.detach_shared();
        if let Some(version) = self.owner_version.take() {
            self.validity.revoke();
            let pending = self.pending(version, false);
            self.hand_off_release(pending);
        }
    }
}

/// Stops the session remotely, simulating `kill -9`. See [`LeaseKeeper::crash_handle`].
#[derive(Clone)]
pub struct CrashHandle {
    pub(super) inner: Arc<Inner>,
    pub(super) validity: Option<ValidityHandle>,
}

impl CrashHandle {
    pub fn crash(&self) {
        for t in self.inner.tasks.lock().expect("mutex").iter() {
            t.abort();
        }
        if let Some(c) = self.inner.state.lock().expect("mutex").current.as_ref() {
            c.root.pin_for_fault_injection();
        }
        if let Some(v) = &self.validity {
            v.pin_for_fault_injection();
        }
    }
}
