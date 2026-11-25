// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The lifecycle of a range's Db: switching the WAL kind, clone/union/discard on
//! split/merge, references between Dbs, and deleting retired Dbs (rule 5 — delete only
//! when it can be proven).

use super::*;

/// The bound on the drain part of [`ObjectStores::native_wal_to_shared`]: the replay is
/// already done at open, what remains is one WAL flush and one memtable flush (up to
/// about one L0).
const WAL_DRAIN_BOUND: std::time::Duration = std::time::Duration::from_secs(120);

/// How many live-range manifests [`ObjectStores::referenced_ranges_with`] reads at once.
/// Each read is the latest manifest of one Db (LIST + GET); 16 keeps a scan of ~200
/// ranges around one second without piling a large burst of requests onto the store.
const REFS_READS_IN_FLIGHT: usize = 16;

impl ObjectStores {
    /// **A range that has a Db but no owner record yet**: write a **released** owner
    /// record carrying the Db's own WAL kind, so the next claim of the range keeps that
    /// kind instead of treating the range as new and taking the node's default kind.
    ///
    /// A bucket built by a single-process node (from before multi-node local
    /// directories) has Dbs with the native WAL and usually no owner record at all — the
    /// record is only written when a range-tier migration step runs (`data=`). Without a
    /// record `Session::claim` assigns the "new range" WAL kind (the shared WAL in a
    /// cluster), and then the open either changes the Db's WAL-kind marker (empty WAL) or
    /// fails outright (`WalNotEmpty`) — a WAL-kind change outside the migration
    /// framework.
    ///
    /// The record has the same shape as the record a single-process node still writes to
    /// carry `data=` (`term=0`, session `<node>/0`, `released`): every format 9 binary
    /// reads it, and anyone can claim the range right away. It does not carry `data=` —
    /// absent ≡ 0, as when there is no record.
    ///
    /// Written with `Create` through [`crate::cas::put_resolving`] (rule 7): if someone
    /// wrote first (another node, or its own claim) we drop ours. No Db yet, a record
    /// already there, or a manifest with an unknown WAL kind ⇒ nothing is written.
    /// Returns `true` when it wrote. Repeatable: it only reads and then `Create`s.
    pub async fn adopt_unowned_db(&self, range: RangeId, node: &str) -> Result<bool> {
        let (store, prefix) = (self.main.store(), self.main.prefix());
        if crate::owner::read_owner(store, prefix, range)
            .await?
            .is_some()
        {
            return Ok(false);
        }
        let wal_kind = match self.range_wal_kind(range).await? {
            Some(k @ (crate::WalKind::SlateDb | crate::WalKind::Shared)) => k,
            Some(crate::WalKind::Unknown(_)) | None => return Ok(false),
        };
        let record = crate::owner::OwnerRecord {
            term: 0,
            session: crate::owner::SessionId {
                node: node.to_string(),
                inc: 0,
            },
            address: None,
            released: true,
            wal_kind,
            retired: false,
            wal_segments: Vec::new(),
            wal_floor_id: None,
            data: None,
            retired_ms: None,
        };
        let written = crate::owner::write_owner(store, prefix, range, &record, None)
            .await?
            .is_some();
        if written {
            tracing::info!(
                range = range.0,
                wal = record.wal_kind.as_str(),
                "range Db has no owner record yet — writing a released record that keeps the Db's WAL kind"
            );
        }
        Ok(written)
    }

    /// Bring the WAL-kind marker of the Db of range `range` to the shared WAL **when the
    /// Db's WAL is empty**. Used for Dbs born from a clone (split/merge) of a native-WAL
    /// parent (the parent is sealed, so the child has no rows in the WAL), and for the
    /// `wal/shared` migration step after the native WAL has been drained. A Db that still
    /// has rows in the WAL ⇒ SlateDB refuses (`WalNotEmpty`) and the error propagates —
    /// rows are never silently dropped. No Db yet, or already on the shared WAL ⇒ nothing
    /// to do. The opposite direction (back to the native WAL) no longer exists.
    ///
    /// The caller must hold the range and must not have opened its Db yet.
    ///
    /// Returns the `replay_after_wal_id` of the manifest just read (`None` when there is
    /// no Db yet): a hint for the shared-WAL range claim
    /// ([`crate::acquire_shared_with_hint`]), sparing another manifest read.
    pub async fn adopt_shared_wal(&self, range: u32) -> Result<Option<u64>> {
        let want = Some(crate::SHARED_WAL_NAME);
        let Some(head) = self.manifest_head(range).await? else {
            return Ok(None);
        };
        let current = head.wal_name;
        if current.as_deref() == want {
            return Ok(Some(head.replay_after_wal_id));
        }
        tracing::info!(range, from = ?current, to = ?want, "changing the WAL-kind marker of a Db with an empty WAL");
        admin_io(
            self.admin_with(self.range_path(range), current.as_deref())
                .set_wal_name(want),
        )
        .await?;
        // Changing the marker only copies the manifest under the new name: the replay
        // point does not change.
        Ok(Some(head.replay_after_wal_id))
    }

    /// **The Db part of the `wal/shared` migration step**: move the native-WAL Db of range
    /// `range` to the shared WAL. The caller holds the range (lease) and has not opened its
    /// Db yet; the owner record is the caller's job, **after** this function.
    ///
    /// 1. **Drain.** Open the Db with the native WAL — SlateDB replays the WAL into the
    ///    memtable and bumps `writer_epoch` (any old writer still alive is fenced) — write
    ///    a fence row like [`crate::PartitionWriter::seal`] (so the memtable flush also
    ///    covers the WAL fence object the open just wrote), flush the WAL then the
    ///    memtable, close. Every row of the native WAL now lives in SSTs the manifest
    ///    points at.
    /// 2. **Change the marker** `wal_name` to the shared WAL ([`Self::adopt_shared_wal`]).
    ///    The "drain finished" fence lives here, not in step 1's word: SlateDB reads the
    ///    manifest and the native WAL, and refuses if any frame is not covered yet
    ///    (`WalNotEmpty`) — rows are never silently dropped.
    ///
    /// Repeatable at every crash point: no Db yet ⇒ nothing to do (the first open creates
    /// a shared-WAL Db); the marker is already the shared WAL ⇒ nothing to do (crash after
    /// step 2); otherwise redo from the start (draining again changes nothing).
    ///
    /// **The old native-WAL objects** (`<root>/wal/*.sst`, on the WAL store) stay: after
    /// the marker change no reader or writer reads them any more (their ids are all ≤ the
    /// manifest's `replay_after_wal_id`), but a live checkpoint may point at them
    /// (`cairn-ctl fork`, the compactor's checkpoints) — proving "nobody needs them" is
    /// not cheap, so they are kept (rule 5). They go with the Db: D-d deletes the whole
    /// `<root>/` (both stores) through the trash when the range retires.
    pub async fn native_wal_to_shared(&self, range: u32) -> Result<()> {
        match self.manifest_wal_name(range).await? {
            None => return Ok(()),
            Some(Some(name)) if name == crate::SHARED_WAL_NAME => return Ok(()),
            Some(Some(other)) => {
                return Err(StoreError::Corrupt(format!(
                    "range {range}: unknown WAL kind {other:?} in the manifest"
                )))
            }
            Some(None) => {}
        }
        use crate::writer::{Batch, Flush, RangeStore};
        tracing::info!(range, "draining the native WAL to SSTs for the shared WAL");
        let db = self.open_db(range).await?;
        // SlateDB's memtable flush retries without bound (every background I/O must be
        // bounded): bound the whole drain. Past the bound ⇒ an error, the next range open
        // redoes it (reopening the Db fences this attempt).
        let drained =
            crate::retry::deadline(&crate::retry::ops::WAL_DRAIN, WAL_DRAIN_BOUND, async {
                let flushed = async {
                    let mut barrier = Batch::default();
                    barrier.delete(crate::keyspace::seal_barrier().into());
                    RangeStore::write(&db, barrier)
                        .await?
                        .await_durable()
                        .await
                        .map_err(StoreError::from_slate)?;
                    for what in [Flush::Wal, Flush::MemTable] {
                        RangeStore::flush(&db, what).await?;
                    }
                    Ok::<_, StoreError>(())
                }
                .await;
                let closed = RangeStore::close(&db).await;
                flushed.and(closed)
            })
            .await;
        match drained {
            Ok(r) => r?,
            Err(e) => return Err(e.into()),
        }
        crate::failpoint!("migrate::wal_shared::drained", err);
        self.adopt_shared_wal(range).await?;
        Ok(())
    }

    /// Clone the Db of range `src` into a new Db for range `dst`, **keeping only the
    /// partition key span `span`** (both ends inclusive): a projection onto two byte
    /// ranges — the hot region and the bulk region (F4). No data is copied: the child
    /// references the parent's SSTs.
    ///
    /// The parent must **already be closed** with [`crate::PartitionWriter::seal`]:
    /// SlateDB (F3) refuses a source whose WAL still carries data the manifest does not
    /// cover — cloning a Db that is still open silently loses acked data.
    pub async fn clone_range(
        &self,
        src: RangeId,
        dst: RangeId,
        span: (PartitionKey, PartitionKey),
    ) -> Result<()> {
        self.union_ranges(&[(src, span)], dst).await
    }

    /// Merge several Dbs (each one partition key span, projected as in
    /// [`Self::clone_range`]) into a new Db for range `dst` — the merge. The spans must
    /// not overlap; every source must already be closed.
    pub async fn union_ranges(
        &self,
        sources: &[(RangeId, (PartitionKey, PartitionKey))],
        dst: RangeId,
    ) -> Result<()> {
        let Some((head, rest)) = sources.split_first() else {
            return Err(StoreError::BadClusterConfig {
                reason: "union has no sources".into(),
            });
        };
        crate::format::ensure(self.main.store(), self.main.prefix(), false).await?;
        let spec = |&(src, (first, last)): &(RangeId, (PartitionKey, PartitionKey))| {
            // One byte range per entity region (D21); the derived region is outside every range.
            CloneSourceSpec::new(self.range_path(src.0)).with_projection_ranges(
                crate::keyspace::owned_ranges(first, last)
                    .into_iter()
                    .map(|(lo, hi)| {
                        (
                            Bound::Included(Bytes::from(lo)),
                            Bound::Excluded(Bytes::from(hi)),
                        )
                    }),
            )
        };
        // The child carries the parent's WAL kind (SlateDB requires every source to have
        // the same kind as the admin that builds the child). A parent on the shared WAL has
        // sealed cleanly — `SharedWalAdmin` copies no frame, and refuses a source that
        // still has frames not yet down in L0.
        let wal_name = self.manifest_wal_name(head.0 .0).await?.flatten();
        let admin = self.admin_with(self.range_path(dst.0), wal_name.as_deref());
        let mut builder = admin.create_clone_builder_from_source(spec(head));
        for s in rest {
            builder = builder.with_source(spec(s));
        }
        builder.build().await.map_err(StoreError::from_slate)?;
        Ok(())
    }

    /// Delete the Db of a range that **never entered the table** — the orphan child of a
    /// split/merge that died midway (the back-off of a split/merge) — together with the
    /// final checkpoints it left on its ancestor Dbs. Without deleting those checkpoints
    /// the ancestors keep forever the SSTs that only the orphan child references.
    ///
    /// `sources` are the **live** source ranges of the intent being rolled back (the
    /// splitting range, or the two merging ranges): Dbs standing on those same ancestors,
    /// read as well to cross-check before deleting.
    ///
    /// Idempotent: a child that does not exist (or is already half deleted) gets the rest
    /// finished. It does **not** check whether the range is in the table — that is the
    /// caller's job (the split coordinator), and deleting a live range by mistake is data
    /// loss.
    ///
    /// `Err` ⇒ the child's Db **remains** (its manifest is the evidence of the
    /// checkpoints it placed on its ancestors): the caller keeps the child's owner record
    /// so the next cleanup sees it.
    ///
    /// Gate: `proof` must be [`crate::proof::DeleteProof::abandoned_child_db`] for exactly
    /// this Db; for a proof based on a table version, `cluster` must be given, and the
    /// reloaded table must still be at that version.
    pub async fn discard_range_db(
        &self,
        proof: crate::proof::DeleteProof,
        sources: &[RangeId],
        cluster: Option<&crate::ClusterConfig>,
    ) -> Result<()> {
        let child = match proof.subject() {
            crate::trash::Subject::RangeDb(c) => crate::ids::PartitionId(*c),
            other => {
                return Err(StoreError::Io(format!(
                    "discard_range_db needs a proof for a Db, got {other}"
                )))
            }
        };
        proof
            .check(&crate::trash::Subject::RangeDb(child.0), cluster)
            .await?;
        let manifest = admin_io(self.admin(child.0).read_manifest(None)).await?;
        if let Some(m) = manifest {
            // **Only delete checkpoints the orphan child itself created (SM-12).** Those are
            // the `final_checkpoint_id` of each `external_dbs` entry: SlateDB issues new ids
            // for every entry on each clone/union, **writes them into the child's manifest
            // first** and only then creates the checkpoints on the ancestors — the child's
            // manifest is the write-ahead record of every checkpoint the clone placed on the
            // ancestors. Nobody else stands on them: only a clone **of** the orphan child
            // would use them as a source, and an orphan child that never entered the table
            // is never split. Same rule as SlateDB's `delete_db` and detach.
            //
            // **Never delete `source_checkpoint_id`.** In an inherited entry (an ancestor of
            // the source), it is the final checkpoint **of the live source** — what the
            // source stands on. Union merges the inherited entries of the two sources that
            // share an ancestor, and only adds an entry for a direct source when the source
            // still owns SSTs after projection; so from the child's manifest one cannot
            // tell which source stands on that checkpoint. The previous version worked it
            // out by reading the manifests of the paths in `external_dbs` and deleting the
            // rest: two sources just split that had not written anything yet were absent,
            // and rolling back the merge of 40+41 deleted the checkpoint 40 stood on (seen
            // in a chaos run, `500 checkpoint missing` forever). In a direct source's entry,
            // it is the temporary checkpoint the clone created on the source: it expires on
            // its own after 300 s.
            let owned: Vec<_> = m
                .external_dbs()
                .iter()
                .filter_map(|e| e.final_checkpoint_id.map(|id| (e.path.clone(), id)))
                .collect();
            // Cross-check: no Db in the lineage (ancestors, live sources) may mention a
            // checkpoint about to be deleted. If the lineage cannot be read, delete
            // nothing — a leaked checkpoint only keeps SSTs longer, a wrong delete loses
            // data.
            let mut referenced = crate::entropy::HashSet::default();
            let mut lineage_known = true;
            // An ancestor with no manifest left (D-d has deleted the whole Db): the
            // checkpoints on it went with it.
            let mut gone = crate::entropy::HashSet::default();
            let paths = m
                .external_dbs()
                .iter()
                .map(|e| Path::from(e.path.clone()))
                .chain(sources.iter().map(|s| self.range_path(s.0)));
            for path in paths {
                match admin_io(self.admin_at(path.clone()).read_manifest(None)).await {
                    Ok(Some(sm)) => {
                        for e in sm.external_dbs() {
                            referenced.insert(e.source_checkpoint_id);
                            referenced.extend(e.final_checkpoint_id);
                        }
                    }
                    Ok(None) => {
                        gone.insert(path.to_string());
                    }
                    Err(e) => {
                        tracing::warn!(source = %path, error = %e, "cannot read a manifest in the lineage — keeping every checkpoint of the orphan child");
                        lineage_known = false;
                    }
                }
            }
            // **Keep the evidence until cleanup is done**: the child's manifest is the only
            // record of the checkpoints it placed on its ancestors — when rolling back a
            // split, that ancestor is the **live** parent range, and a checkpoint with no
            // expiry pins its SSTs forever. While any checkpoint could not be removed yet,
            // do not delete the child's Db and return an error: the caller keeps the child's
            // owner record, and the orphan-child cleanup loop (`gc::OrphanRetirer`)
            // carries on.
            if !lineage_known {
                return Err(StoreError::Io(format!(
                    "range {}: cannot read the lineage — keeping the orphan child's Db until the next cleanup",
                    child.0
                )));
            }
            let mut failed = Vec::new();
            for (path, id) in owned {
                if referenced.contains(&id) {
                    // Tripwire (AGENTS.md §6.4): a live Db stands on a checkpoint that the
                    // orphan child thought was its own. Must never happen.
                    crate::failpoint!("split::discard::referenced_checkpoint");
                    tracing::warn!(child = child.0, parent = %path, checkpoint = %id, "checkpoint of the orphan child is still referenced — keeping it");
                    continue;
                }
                if gone.contains(&Path::from(path.clone()).to_string()) {
                    continue;
                }
                // A checkpoint already deleted in an earlier cleanup: `delete_checkpoint`
                // does not report an error.
                let deleted: Result<()> = async {
                    crate::failpoint!("split::discard::delete_checkpoint", err);
                    admin_io(
                        self.admin_at(Path::from(path.clone()))
                            .delete_checkpoint(id),
                    )
                    .await
                }
                .await;
                if let Err(e) = deleted {
                    tracing::warn!(parent = %path, checkpoint = %id, error = %e, "cannot delete a checkpoint of the orphan child — keeping the child's Db");
                    failed.push(format!("{path}@{id}: {e}"));
                }
            }
            if !failed.is_empty() {
                return Err(StoreError::Io(format!(
                    "range {}: could not yet remove the orphan child's checkpoints on its ancestors ({}) — keeping its Db until the next cleanup",
                    child.0,
                    failed.join("; ")
                )));
            }
        }
        self.delete_range_objects(child, "split/merge rollback: orphan child's Db")
            .await
            .map(|_| ())
    }

    /// Every object under `ranges/<id>` on both stores, **moved into the trash**
    /// ([`crate::trash`]): one entry with the same id on each store, `reason` recorded in
    /// the entry. Returns the number of objects moved.
    ///
    /// **The manifest goes last** (G-b): the WAL store (when separate) → `wal/` → the rest
    /// (`compacted/`, …) → `manifest/`. A failure midway leaves the manifest in place,
    /// [`Self::range_db_exists`] still sees the Db, and the next round carries on. The
    /// previous version deleted in LIST order (`compacted/` < `manifest/` < `wal/`): a
    /// failure after the manifest was gone made every later round skip that range, and
    /// the remaining `wal/` part was orphaned forever.
    async fn delete_range_objects(&self, range: RangeId, reason: &str) -> Result<usize> {
        let prefix = self.range_path(range.0);
        let mut stores = Vec::new();
        if let Some(wal) = &self.wal {
            stores.push(wal.store());
        }
        stores.push(self.main.store());
        let depth = prefix.parts().count();
        let rank = |p: &Path| match p.parts().nth(depth).as_ref().map(|s| s.as_ref()) {
            Some("wal") => 0,
            Some("manifest") => 2,
            _ => 1,
        };
        let id = crate::trash::EntryId::new(crate::entropy::now_ms());
        let mut n = 0;
        for store in stores {
            let mut listed: Vec<_> = store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|e| StoreError::Io(e.to_string()))?;
            // Stable: inside `manifest/`, older versions first, the newest last.
            listed.sort_by_key(|m| rank(&m.location));
            let paths: Vec<Path> = listed.into_iter().map(|m| m.location).collect();
            n += crate::trash::Trash::new(Arc::clone(store), self.main.prefix())
                .move_into(
                    &id,
                    &crate::trash::Subject::RangeDb(range.0),
                    reason,
                    &paths,
                )
                .await?;
        }
        Ok(n)
    }

    /// **D-d:** the Db of the retired range `retired` is listed in the `external_dbs` of
    /// no live range in `live` ⇒ nobody reads its SSTs any more.
    ///
    /// Why `external_dbs` is enough: SlateDB copies the source's `external_dbs` into the
    /// child (every ancestor, not only the direct parent), shrinks `sst_ids` by the
    /// manifest on every compaction (`prune_external_sst_ids`), and the `detach` GC drops
    /// an entry when no SST — in the current manifest **and** in every live checkpoint —
    /// is referenced any more (removing the checkpoint pinned on the parent at the same
    /// time). An absent entry means the child has cut its own tie to the parent.
    ///
    /// Conservative at every ambiguous point: a failed read of a live range's manifest ⇒
    /// `false`; a live range's Db with no manifest yet (being opened for the first time)
    /// ⇒ `false`.
    ///
    /// **Identity by `RangeId`, not by string** (G-a). Each `external_dbs` entry is split
    /// into prefix + id ([`split_range_db_path`]). An entry that cannot be split, or that
    /// carries a prefix other than the current one (the bucket moved to another
    /// `--prefix`, the store changed), is a reference we **cannot prove** does not point
    /// at `retired` ⇒ `false` with a `warn`. The previous version compared strings: with
    /// a mismatched prefix no line matched, and the parent Db was deleted from under its
    /// children.
    pub async fn retired_db_unreferenced(
        &self,
        retired: RangeId,
        live: &[RangeId],
    ) -> Result<bool> {
        if live.contains(&retired) {
            return Ok(false);
        }
        Ok(self
            .referenced_ranges(live)
            .await?
            .blocker(retired)
            .is_none())
    }

    /// Every range that the Dbs of the live ranges `live` still list in `external_dbs` —
    /// one manifest read per live range, for the whole D-d round (`GET /admin/gc/plan`
    /// asks for every candidate at once).
    ///
    /// Same conservative rule as [`Self::retired_db_unreferenced`]: a live range's
    /// manifest that fails to read or does not exist yet, or an entry with another prefix
    /// / that cannot be split ⇒ [`RangeRefs::unknown`] — every range counts as still
    /// referenced.
    pub async fn referenced_ranges(&self, live: &[RangeId]) -> Result<RangeRefs> {
        self.referenced_ranges_with(live, &[]).await
    }

    /// Like [`Self::referenced_ranges`], plus the **promised children** `promised`
    /// (children of in-flight intents — `PartitionTable::promised`). A child with no
    /// manifest yet does **not** turn the set `unknown`: it can only stand on its source
    /// and what the source references (G7 — a clone copies the source's `external_dbs`),
    /// and the source is a live range in `live` whose `external_dbs` only shrinks over
    /// time — the set read here already covers everything a later clone copies. A child
    /// that already has a manifest is read like a live range (it may keep an entry its
    /// source has detached). A failed read is still `unknown`. Model M2 (`GcPlan`)
    /// already describes exactly this rule.
    ///
    /// Manifests are read in parallel, at most `REFS_READS_IN_FLIGHT` at once: read one
    /// by one, a scan of ~211 live ranges under the `s3-standard` profile took ~18 s, and
    /// D-d rebuilds the proof (including this scan) for **each** range about to be
    /// deleted. Results come in exactly the order of `live` then `promised` (`buffered`,
    /// not `buffer_unordered`): the first `unknown` reason and the range recorded in `by`
    /// are the same as with sequential reads.
    pub async fn referenced_ranges_with(
        &self,
        live: &[RangeId],
        promised: &[RangeId],
    ) -> Result<RangeRefs> {
        use futures::StreamExt;
        let own_prefix = normalize_path(self.main.prefix().as_ref());
        let mut refs = RangeRefs::default();
        // Owned entries (not `&RangeId`): the function's future must be `Send` for every
        // lifetime when the caller `tokio::spawn`s it.
        let all: Vec<(RangeId, bool)> = live
            .iter()
            .map(|r| (*r, false))
            .chain(promised.iter().map(|r| (*r, true)))
            .collect();
        let mut reads = futures::stream::iter(all)
            .map(|(r, is_promised)| {
                let admin = self.admin(r.0);
                async move { (r, is_promised, admin_io(admin.read_manifest(None)).await) }
            })
            .buffered(REFS_READS_IN_FLIGHT);
        while let Some((r, is_promised, read)) = reads.next().await {
            let m = match read {
                Ok(Some(m)) => m,
                Ok(None) if is_promised => continue,
                Ok(None) => {
                    refs.unknown
                        .get_or_insert(format!("Db of live range {} has no manifest yet", r.0));
                    continue;
                }
                Err(e) => {
                    refs.unknown.get_or_insert(format!(
                        "cannot read the manifest of live range {}: {e}",
                        r.0
                    ));
                    continue;
                }
            };
            for e in m.external_dbs() {
                match split_range_db_path(&e.path) {
                    Some((prefix, id)) if prefix == own_prefix => {
                        refs.by.entry(id).or_insert(r.0);
                    }
                    _ => {
                        tracing::warn!(
                            song = r.0,
                            source = %e.path,
                            prefix = %own_prefix,
                            "D-d: external_dbs of a live range points outside the current prefix — keeping every retired Db"
                        );
                        refs.unknown.get_or_insert(format!(
                            "live range {} references {:?} outside prefix {own_prefix:?}",
                            r.0, e.path
                        ));
                    }
                }
            }
        }
        Ok(refs)
    }

    /// Delete the Db of a **retired** range that [`Self::retired_db_unreferenced`] has
    /// just said nobody references. Idempotent. Returns the number of objects deleted.
    ///
    /// Gate: `proof` must be [`crate::proof::DeleteProof::retired_db`] for exactly this
    /// range; for a proof based on a table version the delete reloads `cluster` and
    /// refuses when the version has changed.
    pub async fn delete_retired_db(
        &self,
        proof: crate::proof::DeleteProof,
        cluster: Option<&crate::ClusterConfig>,
    ) -> Result<usize> {
        let retired = match proof.subject() {
            crate::trash::Subject::RangeDb(r) => crate::ids::PartitionId(*r),
            other => {
                return Err(StoreError::Io(format!(
                    "delete_retired_db needs a proof for a Db, got {other}"
                )))
            }
        };
        proof
            .check(&crate::trash::Subject::RangeDb(retired.0), cluster)
            .await?;
        self.delete_range_objects(retired, "D-d: retired range Db no longer referenced")
            .await
    }

    /// The trash on each store of the cluster (the main store, then the WAL store if
    /// separate), with the main store's prefix as the common root.
    pub fn trash_bins(&self) -> Vec<crate::trash::Trash> {
        let mut bins = vec![crate::trash::Trash::new(
            Arc::clone(self.main.store()),
            self.main.prefix(),
        )];
        if let Some(wal) = &self.wal {
            bins.push(crate::trash::Trash::new(
                Arc::clone(wal.store()),
                self.main.prefix(),
            ));
        }
        bins
    }

    /// Every checkpoint on the Db of range `range`, per the latest manifest. Empty if the
    /// Db has no manifest. The final checkpoint a clone places on its source is a
    /// checkpoint **with no expiry** (`expires == false`).
    pub async fn range_checkpoints(&self, range: RangeId) -> Result<Vec<RangeCheckpoint>> {
        let admin = self.admin(range.0);
        if admin_io(admin.read_manifest(None)).await?.is_none() {
            return Ok(Vec::new());
        }
        Ok(admin_io(admin.list_checkpoints(None))
            .await?
            .into_iter()
            .map(|c| RangeCheckpoint {
                id: c.id.to_string(),
                expires: c.expire_time.is_some(),
                created_by: self.checkpoint_child(c.name.as_deref()),
                name: c.name,
            })
            .collect())
    }

    /// The range whose clone/union placed a checkpoint named `name` (G15: the child Db's
    /// path), when that Db has our prefix.
    pub(crate) fn checkpoint_child(&self, name: Option<&str>) -> Option<crate::ids::PartitionId> {
        let own = normalize_path(self.main.prefix().as_ref());
        name.and_then(split_range_db_path)
            .filter(|(p, _)| *p == own)
            .map(|(_, id)| crate::ids::PartitionId(id))
    }

    /// **D-d's second barrier**: the final checkpoints (no expiry) on the manifest **of the
    /// Db of range `range` itself**, classified by name (G15: clone/union names them by the
    /// child Db's path):
    ///
    /// - the name is the Db of a range with the same prefix that **carries no tombstone**
    ///   and whose Db **still has a manifest** ⇒ someone (possibly) still stands on it;
    ///   if it is in `live` (live ranges + in-flight children of the caller's table
    ///   snapshot) it is recorded separately — a scan that says "nobody" is then out of
    ///   step;
    /// - the name is the Db of a range **carrying a tombstone** (retired: nobody opens it,
    ///   nobody reads it as a Db) or a Db **with no manifest left** (deleted before it
    ///   could detach) ⇒ nobody reads through that checkpoint, skip it. Its **live**
    ///   descendants standing on `range` have checkpoints of their own (G7: a clone copies
    ///   every ancestor; G15: it places a checkpoint on each one; G6: only detach removes
    ///   them);
    /// - a name outside the prefix (a `cairn-ctl fork` copy) or an unknown name ⇒ cannot
    ///   be checked, counts as still there;
    /// - no name (created before the fork patch that names them) ⇒ unknown owner, counted
    ///   separately.
    ///
    /// Checkpoints **with an expiry** (the temporary one of a clone, of a reader) are not
    /// considered: no Db stands on them long term, and a retired Db has no writer left to
    /// clean them up when they expire. A Db with no manifest ⇒ no checkpoints.
    ///
    /// Cost: one manifest read of `range`, plus for each named Db with the same prefix one
    /// owner-record GET (and one manifest read if it carries no tombstone) — only on D-d
    /// candidates that have passed every other gate.
    pub async fn checkpoint_pins(&self, range: RangeId, live: &[RangeId]) -> Result<RangePins> {
        let mut out = RangePins::default();
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(out);
        };
        let own = normalize_path(self.main.prefix().as_ref());
        for c in m.checkpoints() {
            if c.expire_time.is_some() {
                continue;
            }
            let Some(name) = c.name.as_deref() else {
                out.unnamed += 1;
                continue;
            };
            let Some((_, by)) = split_range_db_path(name).filter(|(p, _)| *p == own) else {
                out.foreign.push(name.to_string());
                continue;
            };
            let tombed = matches!(
                crate::owner::read_owner(
                    self.main.store(),
                    self.main.prefix(),
                    crate::ids::PartitionId(by)
                )
                .await?,
                Some(rec) if rec.retired
            );
            if tombed
                || admin_io(self.admin(by).read_manifest(None))
                    .await?
                    .is_none()
            {
                out.dangling += 1;
                continue;
            }
            out.named.push(by);
            if live.iter().any(|l| l.0 == by) {
                out.by_live.push(by);
            }
        }
        Ok(out)
    }

    /// Whether the Db of range `range` is still on the bucket (has a manifest).
    /// The number of **distinct** ancestor Dbs in `external_dbs` of the stored manifest
    /// of range `range` — one manifest read; `None` when there is no Db yet. Like
    /// [`crate::WriterHealth::external_depth`] but read from the store, for places with no
    /// writer at hand (`split_blocker`, tests).
    pub async fn range_external_depth(&self, range: RangeId) -> Result<Option<usize>> {
        Ok(self.range_external_paths(range).await?.map(|p| p.len()))
    }

    /// The **distinct** paths (sorted) of the ancestor Dbs in `external_dbs` of the stored
    /// manifest of range `range` — one manifest read; `None` when there is no Db yet. The
    /// fresh version of [`crate::WriterHealth::external_paths`].
    pub async fn range_external_paths(&self, range: RangeId) -> Result<Option<Vec<String>>> {
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(None);
        };
        let mut paths: Vec<String> = m.external_dbs().iter().map(|e| e.path.clone()).collect();
        paths.sort_unstable();
        paths.dedup();
        Ok(Some(paths))
    }

    /// The range id of the Db at `path` (an `external_dbs` entry) if it lies under the
    /// prefix of **this cluster**; `None` for a Db of another cluster (the source of
    /// `cairn-ctl fork`) or an unknown path.
    pub fn own_range_of(&self, path: &str) -> Option<RangeId> {
        let own = normalize_path(self.main.prefix().as_ref());
        split_range_db_path(path)
            .filter(|(prefix, _)| *prefix == own)
            .map(|(_, id)| crate::ids::PartitionId(id))
    }

    pub async fn range_db_exists(&self, range: RangeId) -> Result<bool> {
        Ok(admin_io(self.admin(range.0).read_manifest(None))
            .await?
            .is_some())
    }

    /// **Close mark on the Db itself** (SM-17): write the `closed` flag into the manifest
    /// of the Db of range `range` (fork patch `Admin::close_for_writes`). From then on
    /// SlateDB refuses **every** writer opening this Db — regardless of the opener's
    /// in-RAM range table — and a writer that is open (if any) is fenced at its next
    /// manifest update. Readers, clones, checkpoints keep working. The split/merge
    /// coordinator closes the sources right after quiescing; on commit the flag stays
    /// forever, on rollback [`Self::reopen_range_db`]. Idempotent.
    pub async fn close_range_db(&self, range: RangeId) -> Result<()> {
        admin_io(self.admin(range.0).close_for_writes()).await
    }

    /// Like [`Self::close_range_db`], but **only if** the `writer_epoch` in the manifest
    /// is still `writer_epoch` — the epoch of the writer the caller has just sealed (fork
    /// patch `Admin::close_for_writes_at_epoch`). If anyone has reopened the Db since, the
    /// epoch is higher: a `Fenced` error, nothing changes — that writer can still write
    /// until its next manifest update, so the caller must back off rather than treat the
    /// Db as closed (a finding of model M1). Retrying after a success sees the flag at
    /// epoch + 1 and succeeds again.
    pub async fn close_range_db_at(&self, range: RangeId, writer_epoch: u64) -> Result<()> {
        admin_io(self.admin(range.0).close_for_writes_at_epoch(writer_epoch)).await
    }

    /// Remove the [`Self::close_range_db`] flag **unconditionally**. The rollback and
    /// self-heal paths use [`Self::reopen_range_db_at`]; this one is for tests and manual
    /// tools. Idempotent.
    pub async fn reopen_range_db(&self, range: RangeId) -> Result<()> {
        admin_io(self.admin(range.0).reopen_for_writes()).await
    }

    /// Remove the [`Self::close_range_db`] flag **only if** the `writer_epoch` in the
    /// manifest is still `writer_epoch` — the epoch the caller read
    /// ([`Self::range_db_close_mark`]) **before** deciding to remove it (fork patch
    /// `Admin::reopen_for_writes_at_epoch`). A later close (of a later intent) has already
    /// bumped the epoch: a `Fenced` error, the flag stays. The condition is re-checked on
    /// every SlateDB retry of the manifest CAS, so a late removal never removes a new mark.
    /// A Db without the flag ⇒ `Ok`, nothing changes.
    pub async fn reopen_range_db_at(&self, range: RangeId, writer_epoch: u64) -> Result<()> {
        admin_io(self.admin(range.0).reopen_for_writes_at_epoch(writer_epoch)).await
    }

    /// The `closed` flag and `writer_epoch` in the stored manifest of the Db of range
    /// `range`, read together; `None` when the Db has no manifest yet. The reference point
    /// for [`Self::reopen_range_db_at`].
    pub async fn range_db_close_mark(&self, range: RangeId) -> Result<Option<CloseMark>> {
        Ok(admin_io(self.admin(range.0).read_manifest(None))
            .await?
            .map(|m| CloseMark {
                closed: m.closed_for_writes(),
                writer_epoch: m.writer_epoch(),
            }))
    }

    /// The `closed` flag in the stored manifest of the Db of range `range`; `None` when the
    /// Db has no manifest yet.
    pub async fn range_db_closed(&self, range: RangeId) -> Result<Option<bool>> {
        Ok(admin_io(self.admin(range.0).read_manifest(None))
            .await?
            .map(|m| m.closed_for_writes()))
    }
}

/// A range Db's close mark as the stored manifest sees it, together with the
/// `writer_epoch` of that read ([`ObjectStores::range_db_close_mark`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseMark {
    pub closed: bool,
    pub writer_epoch: u64,
}

/// One checkpoint on the Db of a range ([`ObjectStores::range_checkpoints`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeCheckpoint {
    pub id: String,
    /// Whether it expires (a clone's temporary checkpoint) or not (a final checkpoint).
    pub expires: bool,
    /// `CheckpointOptions.name` at creation. The fork names a clone's final checkpoint
    /// by the child Db's path.
    pub name: Option<String>,
    /// The range (same prefix) whose clone created this checkpoint — read from the name.
    /// `None`: an unnamed checkpoint (created before the fork named them), or the target
    /// Db lies outside this bucket/prefix (`cairn-ctl fork`).
    pub created_by: Option<RangeId>,
}

/// The final checkpoints on a Db's own manifest
/// ([`ObjectStores::checkpoint_pins`], D-d's second barrier).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangePins {
    /// Ranges (same prefix), carrying no tombstone and whose Db still has a manifest, that
    /// have a named final checkpoint on this Db.
    pub named: Vec<u32>,
    /// The part of `named` that is a live range or an in-flight child of the caller's
    /// table snapshot.
    pub by_live: Vec<u32>,
    /// Names outside the prefix (`cairn-ctl fork`) or unknown names: cannot be checked.
    pub foreign: Vec<String>,
    /// Unnamed final checkpoints (from before the fork named them).
    pub unnamed: usize,
    /// Named final checkpoints whose creating range already carries a tombstone or whose
    /// Db has no manifest left (skipped).
    pub dangling: usize,
}

impl RangePins {
    /// What the `cairn_split` core needs (`View::pins`).
    pub fn core(&self) -> cairn_split::Pins {
        cairn_split::Pins {
            named: !self.named.is_empty() || !self.foreign.is_empty(),
            by_live: !self.by_live.is_empty(),
            unnamed: self.unnamed > 0,
        }
    }

    /// Why it is kept, human-readable; `None` = no final checkpoint keeps it.
    pub fn blocker(&self) -> Option<String> {
        let mut why = Vec::new();
        if !self.by_live.is_empty() {
            why.push(format!(
                "live ranges {:?} still have named final checkpoints on it",
                self.by_live
            ));
        } else if !self.named.is_empty() {
            why.push(format!(
                "Db of ranges {:?} still there, with named final checkpoints on it",
                self.named
            ));
        }
        if !self.foreign.is_empty() {
            why.push(format!(
                "final checkpoints named outside the prefix {:?}",
                self.foreign
            ));
        }
        if self.unnamed > 0 {
            why.push(format!(
                "{} unnamed final checkpoints (from before checkpoint naming)",
                self.unnamed
            ));
        }
        (!why.is_empty()).then(|| why.join("; "))
    }
}

/// Which ranges are still referenced by live ranges — the result of
/// [`ObjectStores::referenced_ranges`].
#[derive(Clone, Debug, Default)]
pub struct RangeRefs {
    /// Referenced range → one live range that references it.
    pub by: std::collections::BTreeMap<u32, u32>,
    /// Something could not be proven (manifest corrupt/missing, unknown prefix): every
    /// range counts as still referenced.
    pub unknown: Option<String>,
}

impl RangeRefs {
    /// Why `r` cannot be deleted yet; `None` = no live range references it.
    /// What this set says about `r`, for the `cairn_split` core (abandoned-child gate, D-d).
    pub fn refs_of(&self, r: RangeId) -> cairn_split::Refs {
        if self.by.contains_key(&r.0) {
            cairn_split::Refs::Referenced
        } else if self.unknown.is_some() {
            cairn_split::Refs::Unknown
        } else {
            cairn_split::Refs::Clear
        }
    }

    pub fn blocker(&self, r: RangeId) -> Option<String> {
        if let Some(why) = &self.unknown {
            return Some(why.clone());
        }
        self.by
            .get(&r.0)
            .map(|live| format!("live range {live} still lists it in external_dbs"))
    }
}

/// Split the Db path of a range — `<prefix>/ranges/<id>`, as [`ObjectStores::range_path`]
/// produces it and SlateDB copies it into `external_dbs` — into the normalized prefix and
/// the id. `None` if it does not have that shape; the id must be written canonically
/// (`ranges/03` is a different Db from `ranges/3`, so it is not range 3).
pub(crate) fn split_range_db_path(path: &str) -> Option<(String, u32)> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match parts.as_slice() {
        [prefix @ .., "ranges", id] => {
            let n: u32 = id.parse().ok()?;
            (n.to_string() == *id).then(|| (prefix.join("/"), n))
        }
        _ => None,
    }
}
