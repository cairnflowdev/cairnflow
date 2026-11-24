// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Rolling back intents and cleaning up orphan children: `roll_back*`, `reopen_sources`, `discard_*`.

use super::*;

impl RangeOps {
    /// Clean up the children of `intent` (**our** intent) that are permanently out of
    /// the table — after the intent is gone because of a roll-back (ours or someone
    /// else's). Ambiguity keeps: a failed table read, a child still in the table or
    /// promised (commit won, or the intent is still there), a child with a Db that
    /// cannot be proven never to have entered the table (SM-14). A child without a Db
    /// that is permanently out of the table: its owner record (which we just created) is garbage.
    pub(super) async fn discard_own_orphans(&self, intent: RangeIntent) {
        let children = match intent {
            RangeIntent::Splitting { left, right, .. } => vec![left, right],
            RangeIntent::Merging { into, .. } => vec![into],
        };
        if let Err(e) = self.cluster.reload().await {
            tracing::warn!(error = %e, "cannot re-read the table after roll-back — the orphan-child round continues");
            return;
        }
        let t = self.cluster.table();
        let promised = t.promised();
        for c in children {
            if t.contains(c) || promised.contains(&c) || c.0 >= t.next_range_id() {
                continue;
            }
            let (store, prefix) = (self.stores.main().store(), self.stores.main().prefix());
            match crate::gc::discard_if_orphan_child(
                &self.stores,
                &self.cluster,
                &t,
                c,
                &mut Default::default(),
                Remover::OwnOrphans,
            )
            .await
            {
                Ok(crate::gc::OrphanChild::Discarded) => {}
                Ok(crate::gc::OrphanChild::Kept(by, detail)) => {
                    tracing::warn!(range = c.0, reason = ?by, detail = %detail, "orphan child of this run not cleaned up yet — the orphan-child round continues")
                }
                Ok(crate::gc::OrphanChild::NotProven) => {
                    // The Db is gone (or never existed): only the owner record is left —
                    // the proof checks both facts itself.
                    let proof = cairn_store::proof::DeleteProof::abandoned_child_owner(
                        &self.stores,
                        &self.cluster,
                        c,
                    )
                    .await;
                    if let Ok(Ok(proof)) = proof {
                        if let Err(e) = delete_owner(store, prefix, proof).await {
                            tracing::warn!(range = c.0, error = %e, "cannot delete the owner record of the orphan child range");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(range = c.0, error = %e, "orphan child of this run not cleaned up yet — the orphan-child round continues")
                }
            }
        }
    }
}

pub(super) fn forget(keepers: Vec<LeaseKeeper>) {
    for k in keepers {
        k.forget();
    }
}

/// Whether the engine is leading range `e`.
///
/// Asked through `flow` — a serving-loop command answered **in place**, without taking
/// the engine: a busy range (the recovery storm after a split, tens of thousands of
/// turns queued for the engine) still answers right away, whereas `status` would have
/// to queue behind them. An engine not (or no longer) leading (taking, quiescing,
/// retired) answers `NotLeader`. An engine's range is fixed, so asking the right engine is enough.
pub(super) async fn is_leader(h: &EngineHandle) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(2), h.flow()).await,
        Ok(Ok(_))
    )
}

/// **Roll back** an uncommitted split/merge whose intent is on range `p` (recovery).
/// Idempotent; with no intent (or the range already gone from the table —
/// committed) it does nothing.
///
/// The order is deliberate: **CAS the intent away first** — that is the decision
/// point, the counterpart of the commit CAS; exactly one of the two wins — and only
/// then clean up the children's Db and owner records (ids are never reused, so a
/// failed cleanup only leaves garbage).
pub async fn roll_back(
    cluster: &ClusterConfig,
    stores: &ObjectStores,
    p: PartitionId,
    by: Who,
) -> cairn_store::Result<()> {
    roll_back_matching(cluster, stores, p, None, by)
        .await
        .map(|_| ())
}

/// Like [`roll_back`], on a table **re-read** from the bucket, and only while `p`
/// still carries exactly `intent`. `Ok(true)` = just CASed it away; `Ok(false)` =
/// nothing of `intent` left to roll back (committed, rolled back, or never on the table).
pub async fn roll_back_if(
    cluster: &ClusterConfig,
    stores: &ObjectStores,
    p: PartitionId,
    intent: RangeIntent,
    by: Who,
) -> cairn_store::Result<bool> {
    cluster.reload().await?;
    roll_back_matching(cluster, stores, p, Some(intent), by).await
}

/// Core of [`roll_back`]: `want` = roll back only this intent. If the table changes
/// underfoot it starts over on the new table — and a **different** intent on `p`
/// at that point (from a later run) is left alone: a slow roller must not roll back
/// an operation that is not its own. Returns `true` if it CASed away this call's own intent.
///
/// Every decision goes through the core: `Clear` on the table just seen (CAS away or
/// stop), `Cleared` after the CAS (the `Cleaned` ledger, how to remove the closed
/// mark), `Discard(RollBack)` for each child.
pub(super) async fn roll_back_matching(
    cluster: &ClusterConfig,
    stores: &ObjectStores,
    p: PartitionId,
    want: Option<RangeIntent>,
    by: Who,
) -> cairn_store::Result<bool> {
    use cairn_split::{Event, Plan, View};
    cairn_store::failpoint!("split::roll_back", err);
    loop {
        let t = cluster.table();
        let Some(entry) = t.range(p).copied() else {
            return Ok(false);
        };
        let Some(intent) = entry.intent else {
            return Ok(false);
        };
        if want.is_some_and(|w| w != intent) {
            return Ok(false);
        }
        let op = core::op_of(p, intent);
        let view = View {
            standing: Some(Ok(core::standing(&t, op))),
            ..View::default()
        };
        match cairn_split::step(&view, &Event::Clear) {
            Plan::CasClear => {}
            Plan::Nothing => return Ok(false),
            other => unreachable!("core returned {other:?} for Clear"),
        }
        let (clear, children): (Vec<PartitionId>, Vec<PartitionId>) = match intent {
            RangeIntent::Splitting { left, right, .. } => (vec![p], vec![left, right]),
            RangeIntent::Merging { with, into } => (vec![p, with], vec![into]),
        };
        // The `Aborting` ledger **before** the intent-removal CAS (an op without a ledger
        // or already done: go on — the table is the arbiter).
        mark_ledger(stores, op, Phase::Aborting).await?;
        // The sources' close marks are read **before** the CAS: a later intent only closes
        // the source again after this CAS wins (one intent per range), so any mark present
        // before this read belongs to the intent being rolled back or an older one — never
        // to a later intent. Remove by exactly that epoch.
        let marks = close_marks(stores, &clear).await;
        let cleared = cluster
            .update(t.version(), |t| {
                let ranges = t
                    .ranges()
                    .iter()
                    .map(|r| {
                        if clear.contains(&r.id) {
                            RangeEntry { intent: None, ..*r }
                        } else {
                            *r
                        }
                    })
                    .collect();
                PartitionTable::from_entries(t.version() + 1, t.next_range_id(), ranges)
            })
            .await;
        match cairn_split::step(
            &View::default(),
            &Event::Cleared {
                cas: core::cas(&cleared),
                by,
            },
        ) {
            Plan::Cleanup { ledger, reopen } => {
                tracing::info!(range = p.0, ?intent, "rolled back intent — cleaning up");
                // `Cleaned` — the durable fact "the children never entered the table" —
                // before removing the closed mark and cleaning up the children. If the write
                // fails we still clean up: this cleanup rests on the CAS just won, not on the ledger.
                if let Some(to) = ledger {
                    if let Err(e) = mark_ledger(stores, op, to).await {
                        tracing::warn!(range = p.0, error = %e, "cannot write the Cleaned ledger after roll-back");
                    }
                }
                cairn_store::failpoint!("split::roll_back::before_reopen");
                let _ = reopen_sources(stores, &marks, reopen).await;
                for c in children {
                    discard_orphan(stores, cluster, c, &clear, Remover::RollBack).await;
                }
                return Ok(true);
            }
            // The table changed underfoot (a commit, or another roller):
            // re-read and start over.
            Plan::Reread => continue,
            Plan::Fail => {
                return Err(match cleared {
                    Err(e) => e,
                    Ok(_) => unreachable!("Fail only for an unclear CAS"),
                })
            }
            other => unreachable!("core returned {other:?} for Cleared"),
        }
    }
}

/// Epoch of the writer that engine `h` of range `r` just sealed at quiesce. Missing ⇒
/// cannot prove which writer was closed — roll back (rule 5).
pub(super) fn sealed_epoch(h: &EngineHandle, r: PartitionId) -> OpResult<u64> {
    h.sealed_writer_epoch().ok_or_else(|| {
        RangeOpError::Busy(format!(
            "range {}: the engine did not report the epoch of the writer just quiesced — rolled back, retry",
            r.0
        ))
    })
}

/// Close mark ([`cairn_store::CloseMark`]) of the Db of each source in `sources`.
/// A failed read or a Db without a manifest ⇒ `None`: the roll-back does not remove that
/// source's mark (rule 5 — keep); the next Db open removes it itself (`heal_stale_close`).
pub(crate) async fn close_marks(
    stores: &ObjectStores,
    sources: &[PartitionId],
) -> Vec<(PartitionId, Option<cairn_store::CloseMark>)> {
    let mut out = Vec::with_capacity(sources.len());
    for s in sources {
        let mark = match stores.range_db_close_mark(*s).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(range = s.0, error = %e, "cannot read the source's closed mark before roll-back — keeping the mark");
                None
            }
        };
        out.push((*s, mark));
    }
    out
}

/// Remove the closed mark (SM-17) from the Db of the sources of an intent **just
/// CASed away** — only after the CAS: if it were removed first, the CAS could still lose to a
/// commit, leaving a retired source without its flag. Each source only removes the close
/// with mark `mark` (read before the CAS — [`close_marks`]), conditioned on the epoch:
/// a slow removal does not remove the mark of a later intent. A failure or a fence is
/// only logged: the next Db open that meets the flag on a live range without an intent
/// removes it itself (`RangeSpawner::factory`, reading a fresh table).
pub async fn reopen_sources(
    stores: &ObjectStores,
    marks: &[(PartitionId, Option<cairn_store::CloseMark>)],
    reopen: Reopen,
) -> Vec<String> {
    let mut failed = Vec::new();
    for (s, mark) in marks {
        let Some(mark) = mark.filter(|m| m.closed) else {
            continue;
        };
        let done = match reopen {
            Reopen::AtMark => stores.reopen_range_db_at(*s, mark.writer_epoch).await,
            Reopen::Any => stores.reopen_range_db(*s).await,
        };
        match done {
            Ok(()) => {}
            Err(cairn_store::StoreError::Fenced) => tracing::info!(
                range = s.0,
                "source was closed again after the mark was read — that mark is not this roll-back's, keeping it"
            ),
            Err(e) => {
                tracing::warn!(range = s.0, error = %e, "cannot remove the closed mark of the source just rolled back — the next Db open removes it");
                failed.push(format!(
                    "cannot remove the closed mark of source {} — the next Db open removes it: {e}",
                    s.0
                ));
            }
        }
    }
    failed
}

/// Clean up orphan child `c` of an intent that was CASed away: the Db (together with its
/// final checkpoint on ancestors), **then** the owner record. If the Db cleanup fails,
/// **keep** the owner record: the Db is still the evidence of the checkpoint it placed
/// on the live source range, and the orphan-child round (`gc::OrphanRetirer`, LISTing
/// both `ranges/` and `owners/`) continues. Deleting the record before the Db is gone
/// means nobody sees that Db again — its checkpoint pins the live range's SSTs forever.
pub async fn discard_orphan(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    c: PartitionId,
    sources: &[PartitionId],
    by: Remover,
) {
    if let Err(e) = discard_abandoned_child(stores, cluster, c, sources, by).await {
        tracing::warn!(range = c.0, error = %e, "orphan child not fully cleaned up — the orphan-child round continues");
    }
}

/// The path that cleans up children right after the caller's **own** intent-removal CAS
/// (`by` = `RollBack` or `Ctl`): asks the core (`Event::Discard`) on the table just CASed
/// — the child is permanently out of the table, the CAS just won is the proof it never
/// entered the table — then [`discard_child_now`]. Does not first read whether the Db
/// has a manifest: `discard_range_db` handles an absent Db itself (cleans up what is
/// left under `ranges/<c>`).
pub async fn discard_abandoned_child(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    c: PartitionId,
    sources: &[PartitionId],
    by: Remover,
) -> cairn_store::Result<()> {
    use cairn_split::{Child, Event, Plan, View};
    let t = cluster.table();
    let view = View {
        child: Some(Child {
            in_table: t.contains(c),
            promised: t.promised().contains(&c),
            allocated: c.0 < t.next_range_id(),
            db: true,
            owner: true,
            stands_on_live: false,
        }),
        ..View::default()
    };
    match cairn_split::step(&view, &Event::Discard(by)) {
        Plan::DiscardDb => discard_child_now(stores, cluster, c, sources).await,
        Plan::Keep(k) => Err(cairn_store::StoreError::Io(format!(
            "cannot prove range {} is an abandoned child: {k:?}",
            c.0
        ))),
        other => unreachable!("core returned {other:?} for Discard({by:?})"),
    }
}

/// Clean up the Db **then** the owner record of abandoned child `c` — the core has
/// decided `DiscardDb` — each step behind its proof
/// ([`cairn_store::proof::DeleteProof`]): the `abandoned_child_db` gate (core
/// `Event::Gate`) reloads the table itself; once the cleanup is done or failed the core
/// decides the owner record (`Event::Discarded`) — the owner record only goes once the Db's manifest is gone.
pub async fn discard_child_now(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    c: PartitionId,
    sources: &[PartitionId],
) -> cairn_store::Result<()> {
    use {
        cairn_split::{Event, Plan, View},
        cairn_store::proof::DeleteProof,
    };
    let unproven = |why: String| {
        cairn_store::StoreError::Io(format!(
            "cannot prove range {} is an abandoned child: {why}",
            c.0
        ))
    };
    let proof = DeleteProof::abandoned_child_db(stores, cluster, c, crate::gc::wall_ms())
        .await?
        .map_err(unproven)?;
    let discarded = stores.discard_range_db(proof, sources, Some(cluster)).await;
    match cairn_split::step(
        &View::default(),
        &Event::Discarded {
            ok: discarded.is_ok(),
        },
    ) {
        Plan::DeleteOwner => discarded?,
        Plan::Keep(_) => return discarded,
        other => unreachable!("core returned {other:?} for Discarded"),
    }
    let proof = DeleteProof::abandoned_child_owner(stores, cluster, c)
        .await?
        .map_err(unproven)?;
    delete_owner(stores.main().store(), stores.main().prefix(), proof).await
}
