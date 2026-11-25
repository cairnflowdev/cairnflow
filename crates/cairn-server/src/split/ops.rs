// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Accepting operations and **splitting** a range: `RangeOps::{start, split, split_exclusive, split_blocker}`.

use super::*;

/// A failed roll-back by the coordinator is retried in the background up to this long,
/// then left to the intent reconciliation round (SM-15).
pub(super) const SETTLE_FOR: Duration = Duration::from_secs(600);

/// Retry policy of the background roll-back (after the first 200 ms pause).
pub(super) const SETTLE_POLICY: cairn_store::retry::Policy =
    cairn_store::retry::Policy::for_duration(
        Duration::from_millis(400),
        Duration::from_secs(10),
        SETTLE_FOR,
    );

impl RangeOps {
    /// **Accept** an operation: run it in a task of the node, record it in the book,
    /// return a ticket. The operation does not die with its waiter: if the HTTP client
    /// disconnects the handler is dropped, but the task still runs to commit or to a
    /// finished roll-back.
    ///
    /// A supervising task wraps the operation: its panic becomes `Failed` in the book
    /// (the [`ActiveIntents`] guard drops on unwind, the quiesce hold wire breaks — SM-15).
    pub fn start(&self, kind: OpKind) -> OpTicket {
        let id = self.book.begin(&self.node_id, kind);
        let (tx, done) = tokio::sync::oneshot::channel();
        let ops = self.clone();
        let book = self.book.clone();
        let op_id = id.clone();
        spawn_in_node(async move {
            let run = spawn_in_node(async move {
                match kind {
                    OpKind::Split { range, at } => ops.split(range, at).await,
                    OpKind::Merge { left, right } => ops.merge(left, right).await,
                }
            });
            let r = match run.await {
                Ok(r) => r,
                Err(e) => {
                    // Core (`Event::Vanished`): release the quiesced sources — the quiesce
                    // hold wire breaks when the `ActiveIntents` guard drops on unwind, the
                    // engine yields as on `Resign` — then whoever takes the source or the
                    // reconciliation round decides.
                    let plan = cairn_split::step(
                        &cairn_split::View::default(),
                        &cairn_split::Event::Vanished,
                    );
                    debug_assert_eq!(plan, cairn_split::Plan::Release);
                    tracing::error!(op = %op_id, error = %e, "split/merge operation panicked — the intent reconciliation round rolls back the rest");
                    Err(RangeOpError::Failed(format!("op {op_id} panicked: {e}")))
                }
            };
            book.finish(&op_id, &r);
            let _ = tx.send(r);
        });
        OpTicket { id, done }
    }

    /// **Split** range `p` at `at` (the first key of the right child). No `at` ⇒ the
    /// middle of the key range. Returns the post-commit table.
    pub async fn split(
        &self,
        p: PartitionId,
        at: Option<u64>,
    ) -> OpResult<(Arc<PartitionTable>, OpReport)> {
        self.split_with(p, at, false).await
    }

    /// Like [`Self::split`], for **auto-split**: only proceeds when the table has no
    /// intent at all — one unfinished split/merge in the whole cluster is enough.
    ///
    /// Checked on the **same** table snapshot whose version the intent CAS compares:
    /// another intent slipping in after the check has changed the table version, the CAS
    /// misses (`Conflict`). The check only saves a wasted flush; the CAS is the arbiter.
    pub async fn split_exclusive(
        &self,
        p: PartitionId,
        at: u64,
    ) -> OpResult<(Arc<PartitionTable>, OpReport)> {
        self.split_with(p, Some(at), true).await
    }

    /// What would block an auto-split of range `p` at `at` right now, **writing
    /// nothing**: an intent anywhere in the table (RAM), the range standing at or above
    /// the ancestor depth limit (one manifest GET), the cluster upgrading (one GET),
    /// a child's L0 without enough room (one manifest GET). `None` = nothing
    /// blocks.
    pub async fn split_blocker(&self, p: PartitionId, at: u64) -> OpResult<Option<SplitBlock>> {
        let table = self.cluster.table();
        if table.ranges().iter().any(|r| r.intent.is_some()) {
            return Ok(Some(SplitBlock::Intent));
        }
        let parent = *table
            .range(p)
            .ok_or_else(|| RangeOpError::NotFound(format!("no range {}", p.0)))?;
        if let Some(why) = self.too_deep(p).await? {
            return Ok(Some(SplitBlock::Depth(why)));
        }
        if let Err(RangeOpError::Busy(m)) = self.refuse_while_upgrading().await {
            return Ok(Some(SplitBlock::Upgrading(m)));
        }
        if !(parent.start < at && at <= parent.end) {
            return Err(RangeOpError::BadRequest(format!(
                "split point {at:016x} outside range {}",
                p.0
            )));
        }
        let left_span = (PartitionKey(parent.start), PartitionKey(at - 1));
        let right_span = (PartitionKey(at), PartitionKey(parent.end));
        match self
            .ensure_l0_headroom(&[&[(p, left_span)], &[(p, right_span)]])
            .await
        {
            Err(RangeOpError::Busy(m)) => Ok(Some(SplitBlock::L0(m))),
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    /// Range `p` stands at or above the auto-split depth limit: one manifest read;
    /// limit 0 ⇒ reads nothing.
    async fn too_deep(&self, p: PartitionId) -> OpResult<Option<String>> {
        if self.lineage.max_auto_split_depth == 0 {
            return Ok(None);
        }
        let depth = self.stores.range_external_depth(p).await?.unwrap_or(0);
        Ok(self.lineage.blocks_auto_split(depth).then(|| {
            format!(
                "range {} still reads SSTs of {depth} ancestor ranges (cap {}); it splits again \
                 after it compacts away from them",
                p.0, self.lineage.max_auto_split_depth
            )
        }))
    }

    async fn split_with(
        &self,
        p: PartitionId,
        at: Option<u64>,
        exclusive: bool,
    ) -> OpResult<(Arc<PartitionTable>, OpReport)> {
        let mut clock = Clock::start("split");
        let table = self.cluster.table();
        let parent = *table
            .range(p)
            .ok_or_else(|| RangeOpError::NotFound(format!("no range {}", p.0)))?;
        if parent.intent.is_some() {
            return Err(RangeOpError::Conflict(format!(
                "range {} has unfinished work",
                p.0
            )));
        }
        if exclusive {
            if let Some(other) = table.ranges().iter().find(|r| r.intent.is_some()) {
                return Err(RangeOpError::Busy(format!(
                    "range {} has an unfinished split/merge — auto-split waits for it to finish",
                    other.id.0
                )));
            }
        }
        if parent.start == parent.end {
            return Err(RangeOpError::BadRequest(format!(
                "range {} has only one key",
                p.0
            )));
        }
        let at = at.unwrap_or(parent.start + (parent.end - parent.start) / 2 + 1);
        if !(parent.start < at && at <= parent.end) {
            return Err(RangeOpError::BadRequest(format!(
                "split point {at:016x} must lie in ({:016x}, {:016x}]",
                parent.start, parent.end
            )));
        }
        let handle = self.leader_handle(&parent).await?;
        let left_span = (PartitionKey(parent.start), PartitionKey(at - 1));
        let right_span = (PartitionKey(at), PartitionKey(parent.end));
        self.ensure_l0_headroom(&[&[(p, left_span)], &[(p, right_span)]])
            .await?;
        // Depth limit: auto-split refuses, a manual split only warns — the operator
        // has their reasons, and the children will compact themselves later.
        match self.too_deep(p).await {
            Ok(Some(why)) if exclusive => return Err(RangeOpError::Busy(why)),
            Ok(Some(why)) => {
                tracing::warn!(range = p.0, reason = %why, "manual split of a range with a deep ancestor chain — the children stand on one more ancestor until they compact")
            }
            Ok(None) => {}
            Err(e) if exclusive => return Err(e),
            Err(e) => {
                tracing::debug!(range = p.0, error = %e, "cannot read the lineage depth before a manual split")
            }
        }
        clock.lap("precondition")?;

        self.refuse_while_upgrading().await?;
        let active = self.active.hold(&[p]).ok_or_else(|| {
            RangeOpError::Conflict(format!("range {}: split/merge pending on this node", p.0))
        })?;
        let (left, right) = (
            PartitionId(table.next_range_id()),
            PartitionId(table.next_range_id() + 1),
        );
        // ── 1. Intent — the decision point.
        let intent = RangeIntent::Splitting { at, left, right };
        let marked = self
            .cluster
            .update(table.version(), |t| {
                let ranges = t
                    .ranges()
                    .iter()
                    .map(|r| {
                        if r.id == p {
                            RangeEntry {
                                intent: Some(intent),
                                ..*r
                            }
                        } else {
                            *r
                        }
                    })
                    .collect();
                PartitionTable::from_entries(t.version() + 1, t.next_range_id() + 2, ranges)
            })
            .await;
        let marked = match core::intent_cas(marked) {
            Ok(t) => t,
            Err(e) => {
                self.undo_stray_intent(p, intent).await;
                return Err(e);
            }
        };
        tracing::info!(range = p.0, at = %format!("{at:016x}"), left = left.0, right = right.0, "starting split");

        let done = self
            .split_after_intent(
                &mut clock,
                &active,
                &handle,
                parent,
                at,
                left,
                right,
                marked.version(),
            )
            .await;
        match done {
            Ok(t) => {
                let report = clock.report(&self.stats, &[p], &[left, right]);
                tracing::info!(range = p.0, window_ms = report.window_ms, phases = ?report.phases, "split measurements");
                Ok((t, report))
            }
            Err(e) => {
                tracing::warn!(range = p.0, error = %e, "split failed — rolling back");
                self.roll_back_here(p, intent, &[&handle]).await;
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn split_after_intent(
        &self,
        clock: &mut Clock,
        active: &ActiveGuard,
        handle: &EngineHandle,
        parent: RangeEntry,
        at: u64,
        left: PartitionId,
        right: PartitionId,
        marked_version: u64,
    ) -> OpResult<Arc<PartitionTable>> {
        // The intent CAS lap is timed here, not in `split`: when the failpoint
        // `split::intentCas` returns an error it must take the roll-back path like every later step.
        clock.lap("intentCas")?;
        // Durable operation ledger — right after the intent CAS wins (the child ids belong to
        // the op from here), before any other side effect.
        let intent = RangeIntent::Splitting { at, left, right };
        let mut ledger = self.open_ledger(core::op_of(parent.id, intent)).await?;
        // Upgrade gate, second time — **after** the intent: a migration runner that writes
        // its lock in the gap between the pre-intent check and the intent is seen by us
        // here and we roll back (the runner writes its lock and then runs; we write the
        // intent and then check — one of the two sees the other).
        self.refuse_while_upgrading().await?;
        // ── 2–3. Flush while still serving, then quiesce: answers once the Db is closed.
        handle.flush_memtable().await?;
        clock.lap("flush")?;
        reset_quiesce_stats(&self.stats, parent.id);
        clock.window_opens();
        // SM-13: from here until done, the parent engine does not open the Db even if it
        // takes the lease back.
        active.mark_quiesced();
        handle
            .quiesce_held(QUIESCE_GRACE, active.quiesce_hold())
            .await?;
        // Epoch of the writer just quiesced: the closed mark is conditioned on it.
        let quiesced_epoch = sealed_epoch(handle, parent.id)?;
        clock.lap("quiesce")?;
        self.ensure_wal_drained(&[parent.id]).await?;
        // ── 5. Clone the two children (F3 is the net: if the parent is not closed SlateDB refuses).
        //
        // Before each child-creating step (Db, owner record): the intent is still ours
        // — see [`Self::ensure_intent_ours`].
        let left_span = (PartitionKey(parent.start), PartitionKey(at - 1));
        let right_span = (PartitionKey(at), PartitionKey(parent.end));
        let closed = self
            .ensure_intent_ours(Stage::Close, parent.id, intent)
            .await?;
        // `Closed` ledger (with the epoch of the writer just quiesced) before the closed mark.
        self.advance_ledger(&mut ledger, closed, |r| {
            r.sealed = vec![(parent.id.0, quiesced_epoch)]
        })
        .await?;
        // ── 4b. Closed mark on the very source Db (SM-17): from here SlateDB refuses
        // every writer of the source, whatever the opener's in-RAM table says.
        // Commit ⇒ the flag stays forever; roll-back ⇒ removed after the intent-removal CAS.
        cairn_store::failpoint!("split::before_close");
        self.stores
            .close_range_db_at(parent.id, quiesced_epoch)
            .await?;
        // Table just read at the closed-mark step: the op is still pending.
        let cloned = core::stage(Stage::CloneFirst, cairn_split::Standing::Pending, parent.id)?;
        self.advance_ledger(&mut ledger, cloned, |_| {}).await?;
        bounded(
            &cairn_store::retry::ops::SPLIT_CLONE,
            CLONE_BOUND,
            "clone of the left child",
            async { Ok(self.stores.clone_range(parent.id, left, left_span).await?) },
        )
        .await?;
        clock.lap("cloneLeft")?;
        self.ensure_intent_ours(Stage::CloneNext, parent.id, intent)
            .await?;
        bounded(
            &cairn_store::retry::ops::SPLIT_CLONE,
            CLONE_BOUND,
            "clone of the right child",
            async {
                Ok(self
                    .stores
                    .clone_range(parent.id, right, right_span)
                    .await?)
            },
        )
        .await?;
        clock.lap("cloneRight")?;
        // ── 6. Take the two children in the node's session.
        self.ensure_intent_ours(Stage::Claim, parent.id, intent)
            .await?;
        let keepers = bounded(
            &cairn_store::retry::ops::SPLIT_CLAIM,
            CLAIM_BOUND,
            "taking the children",
            self.claim_children(&[parent.id], &[left, right]),
        )
        .await?;
        clock.lap("ownerClaim")?;
        // `Prepared` ledger before the commit CAS: every child has a Db and an owner record.
        let prepared = core::stage(Stage::Commit, cairn_split::Standing::Pending, parent.id)?;
        if let Err(e) = self.advance_ledger(&mut ledger, prepared, |_| {}).await {
            forget(keepers);
            return Err(e);
        }
        // ── 7. Commit.
        let committed = self
            .cluster
            // Same transformation as the roll-forward: two CASes from the same
            // version write out the same table.
            .update(marked_version, |t| {
                super::forward::committed_table(t, parent.id, intent)
            })
            .await;
        let (committed, done) = match core::commit_cas(committed) {
            Ok(t) => t,
            Err(e) => {
                forget(keepers);
                return Err(e);
            }
        };
        // From here the two children are real ranges: the node's engine takes them back as
        // its own (their owner records point at this session).
        forget(keepers);
        clock.lap_committed("commitCas");
        self.ledger_committed(&mut ledger, done).await;
        tracing::info!(
            range = parent.id.0,
            left = left.0,
            right = right.0,
            "split done"
        );
        // ── 8. The two children have their leader here (the follow_table loop builds the engines).
        for c in [left, right] {
            self.wait_led_here(c).await;
        }
        clock.lap_committed("open");
        clock.window_closes();
        // ── 9. The parent retires. The follow_table loop also does this when it sees the parent
        // leave the table; whoever comes first wins, the later one meets an engine that already exited.
        let _ = handle.retire().await;
        clock.lap_committed("retire");
        Ok(committed)
    }
}
