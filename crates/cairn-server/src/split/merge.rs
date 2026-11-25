// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Merge** two adjacent ranges: `RangeOps::merge`.

use super::*;

impl RangeOps {
    /// **Merge** two adjacent ranges `a`, `b` into one (D-e: this node leads one of
    /// the two; the other is gathered here first). Returns the post-commit table.
    pub async fn merge(
        &self,
        a: PartitionId,
        b: PartitionId,
    ) -> OpResult<(Arc<PartitionTable>, OpReport)> {
        let mut clock = Clock::start("merge");
        let table = self.cluster.table();
        let find = |p: PartitionId| {
            table
                .range(p)
                .copied()
                .ok_or_else(|| RangeOpError::NotFound(format!("no range {}", p.0)))
        };
        let (ea, eb) = (find(a)?, find(b)?);
        let (lo, hi) = if ea.start < eb.start {
            (ea, eb)
        } else {
            (eb, ea)
        };
        if lo.end.checked_add(1) != Some(hi.start) {
            return Err(RangeOpError::BadRequest(format!(
                "ranges {} and {} are not adjacent",
                a.0, b.0
            )));
        }
        if lo.intent.is_some() || hi.intent.is_some() {
            return Err(RangeOpError::Conflict(
                "one of the two ranges has unfinished work".into(),
            ));
        }
        // This node must lead at least one side; the other is gathered after the intent.
        let (mine, other) = match self.leader_handle(&lo).await {
            Ok(h) => ((lo, h), hi),
            Err(_) => ((hi, self.leader_handle(&hi).await?), lo),
        };
        self.ensure_l0_headroom(&[&[
            (lo.id, (PartitionKey(lo.start), PartitionKey(lo.end))),
            (hi.id, (PartitionKey(hi.start), PartitionKey(hi.end))),
        ]])
        .await?;
        clock.lap("precondition")?;

        self.refuse_while_upgrading().await?;
        let active = self.active.hold(&[lo.id, hi.id]).ok_or_else(|| {
            RangeOpError::Conflict(format!(
                "range {} or {} has an unfinished split/merge on this node",
                lo.id.0, hi.id.0
            ))
        })?;
        let into = PartitionId(table.next_range_id());
        let marked = self
            .cluster
            .update(table.version(), |t| {
                let ranges = t
                    .ranges()
                    .iter()
                    .map(|r| {
                        if r.id == lo.id {
                            RangeEntry {
                                intent: Some(RangeIntent::Merging { with: hi.id, into }),
                                ..*r
                            }
                        } else if r.id == hi.id {
                            RangeEntry {
                                intent: Some(RangeIntent::Merging { with: lo.id, into }),
                                ..*r
                            }
                        } else {
                            *r
                        }
                    })
                    .collect();
                PartitionTable::from_entries(t.version() + 1, t.next_range_id() + 1, ranges)
            })
            .await;
        let marked = match core::intent_cas(marked) {
            Ok(t) => t,
            Err(e) => {
                self.undo_stray_intent(lo.id, RangeIntent::Merging { with: hi.id, into })
                    .await;
                return Err(e);
            }
        };
        tracing::info!(
            left = lo.id.0,
            right = hi.id.0,
            into = into.0,
            "starting merge"
        );

        let (mine_entry, mine_handle) = mine;
        let mut quiesced: Vec<EngineHandle> = Vec::new();
        let done = self
            .merge_after_intent(
                &mut clock,
                &active,
                (mine_entry, mine_handle.clone()),
                other,
                (lo, hi),
                into,
                marked.version(),
                &mut quiesced,
            )
            .await;
        match done {
            Ok(t) => {
                let report = clock.report(&self.stats, &[lo.id, hi.id], &[into]);
                tracing::info!(into = into.0, window_ms = report.window_ms, phases = ?report.phases, "merge measurements");
                Ok((t, report))
            }
            Err(e) => {
                tracing::warn!(left = lo.id.0, right = hi.id.0, error = %e, "merge failed — rolling back");
                let handles: Vec<&EngineHandle> = quiesced.iter().collect();
                self.roll_back_here(lo.id, RangeIntent::Merging { with: hi.id, into }, &handles)
                    .await;
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn merge_after_intent(
        &self,
        clock: &mut Clock,
        active: &ActiveGuard,
        mine: (RangeEntry, EngineHandle),
        other: RangeEntry,
        (lo, hi): (RangeEntry, RangeEntry),
        into: PartitionId,
        marked_version: u64,
        quiesced: &mut Vec<EngineHandle>,
    ) -> OpResult<Arc<PartitionTable>> {
        clock.lap("intentCas")?;
        // Durable operation ledger — right after the intent CAS wins.
        let intent = RangeIntent::Merging { with: hi.id, into };
        let mut ledger = self.open_ledger(core::op_of(lo.id, intent)).await?;
        // Upgrade gate, second time — see `split_after_intent`.
        self.refuse_while_upgrading().await?;
        // ── Gather the other range onto this node (D-e): its owner hands it over, we take it.
        let other_handle = self.gather_here(&other, (lo.id, intent)).await?;
        clock.lap("gather")?;
        // ── Flush, quiesce both.
        for h in [&mine.1, &other_handle] {
            h.flush_memtable().await?;
        }
        clock.lap("flush")?;
        reset_quiesce_stats(&self.stats, lo.id);
        reset_quiesce_stats(&self.stats, hi.id);
        clock.window_opens();
        // SM-13 — after gathering: the other range was just opened here to be quiesced.
        active.mark_quiesced();
        for h in [&mine.1, &other_handle] {
            h.quiesce_held(QUIESCE_GRACE, active.quiesce_hold()).await?;
            quiesced.push(h.clone());
        }
        // Epochs of the two writers just quiesced: the closed mark is conditioned on them.
        let quiesced_epochs = [
            (mine.0.id, sealed_epoch(&mine.1, mine.0.id)?),
            (other.id, sealed_epoch(&other_handle, other.id)?),
        ];
        clock.lap("quiesce")?;
        self.ensure_wal_drained(&[lo.id, hi.id]).await?;
        // ── Union. Before each child-creating step: the intent is still ours.
        let closed = self.ensure_intent_ours(Stage::Close, lo.id, intent).await?;
        // `Closed` ledger (with the epochs of the two writers just quiesced) before the closed mark.
        self.advance_ledger(&mut ledger, closed, |r| {
            r.sealed = quiesced_epochs.iter().map(|(p, e)| (p.0, *e)).collect()
        })
        .await?;
        // Closed mark on the very Db of both sources (SM-17).
        cairn_store::failpoint!("merge::before_close");
        for (r, epoch) in quiesced_epochs {
            self.stores.close_range_db_at(r, epoch).await?;
        }
        let cloned = core::stage(Stage::CloneFirst, cairn_split::Standing::Pending, lo.id)?;
        self.advance_ledger(&mut ledger, cloned, |_| {}).await?;
        bounded(
            &cairn_store::retry::ops::SPLIT_CLONE,
            CLONE_BOUND,
            "union",
            async {
                Ok(self
                    .stores
                    .union_ranges(
                        &[
                            (lo.id, (PartitionKey(lo.start), PartitionKey(lo.end))),
                            (hi.id, (PartitionKey(hi.start), PartitionKey(hi.end))),
                        ],
                        into,
                    )
                    .await?)
            },
        )
        .await?;
        clock.lap("union")?;
        self.ensure_intent_ours(Stage::Claim, lo.id, intent).await?;
        let keepers = bounded(
            &cairn_store::retry::ops::SPLIT_CLAIM,
            CLAIM_BOUND,
            "taking the children",
            self.claim_children(&[lo.id, hi.id], &[into]),
        )
        .await?;
        clock.lap("ownerClaim")?;
        let prepared = core::stage(Stage::Commit, cairn_split::Standing::Pending, lo.id)?;
        if let Err(e) = self.advance_ledger(&mut ledger, prepared, |_| {}).await {
            forget(keepers);
            return Err(e);
        }
        // ── Commit.
        let committed = self
            .cluster
            // Same transformation as the roll-forward.
            .update(marked_version, |t| {
                super::forward::committed_table(t, lo.id, intent)
            })
            .await;
        let (committed, done) = match core::commit_cas(committed) {
            Ok(t) => t,
            Err(e) => {
                forget(keepers);
                return Err(e);
            }
        };
        forget(keepers);
        clock.lap_committed("commitCas");
        self.ledger_committed(&mut ledger, done).await;
        tracing::info!(left = lo.id.0, right = hi.id.0, into = into.0, "merge done");
        self.wait_led_here(into).await;
        clock.lap_committed("open");
        clock.window_closes();
        for h in [&mine.1, &other_handle] {
            let _ = h.retire().await;
        }
        clock.lap_committed("retire");
        Ok(committed)
    }
}
