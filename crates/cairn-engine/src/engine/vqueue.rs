// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Virtual queues: slots, stages, turns, dequeue, requeue.

use super::*;

/// A stage change the lifecycle core's shell hands to [`Engine::apply_lifecycle`]
/// (`cairn_lifecycle::StageChange` except `Insert` — building an entry goes
/// through [`Engine::create`]/[`Engine::enqueue_existing`], which need the queue and a `seq`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageWrite {
    Keep,
    Move(Stage),
    Remove,
}

/// A retry-row change the shell hands to [`Engine::apply_lifecycle`]: `old_due`
/// is the deadline of the existing row if the shell knows it (read it, or wrote
/// it itself), so the old index row is deleted in the same commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowWrite {
    Keep,
    Set {
        old_due: Option<u64>,
        row: cairn_lifecycle::RetryRow,
    },
    Clear {
        old_due: Option<u64>,
    },
}

impl<S: RangeStore> Engine<S> {
    /// The vqueue entry of an invocation, through the reverse index.
    pub async fn vqueue_of(&self, id: &InvocationId) -> Result<Option<VQueueSlot>> {
        Ok(self
            .writer
            .vqueue_of(id)
            .await?
            .map(|(name, queue, stage, seq)| VQueueSlot {
                name,
                queue,
                stage,
                seq,
            }))
    }

    /// Puts an **already existing** invocation into the queue.
    ///
    /// There is only one user, and it is a repair path: invocations created by
    /// a binary from before flow control have no vqueue entry, and running one
    /// without a queue is running it without the VO lock. The normal creation
    /// path writes the entry **in the same commit** as journal entry 0
    /// ([`Self::create`]) and does not go through here.
    pub async fn enqueue_existing(
        &mut self,
        id: &InvocationId,
        queue: &QueueName,
        seq: u64,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        muts.push_vqueue(&queue.encode(), Stage::Inbox, seq, id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Moves an invocation to another stage. No-op if it is no longer in the queue.
    ///
    /// One commit: delete the old row, write the new one, update the index.
    /// Split apart, a death exactly in between would leave two entries for one
    /// invocation, and the scan on becoming leader would count it twice.
    pub async fn set_stage(&mut self, id: &InvocationId, to: Stage) -> Result<()> {
        let Some(slot) = self.vqueue_of(id).await? else {
            return Ok(());
        };
        if slot.stage == to {
            return Ok(());
        }
        let mut muts = Mutations::new();
        muts.move_vqueue(&slot.queue, slot.seq, slot.stage, to, id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// **Starts a turn**: moves the stage to `Running` and — if `watch_due` —
    /// writes the **watchdog row** in **the same commit**.
    ///
    /// The watchdog row is a `retries` row with the [`WATCH_FLAG`] flag: it
    /// fires at `watch_due` like a retry, unless the commit ending the turn
    /// ([`Self::end_turn`]) removes it. Thanks to that, every invocation that
    /// entered a turn has at least one durable wake-up, even when the turn dies
    /// somewhere nobody remembers to reschedule (the process died, the task
    /// panicked, a forgotten `?`). The call site drops it when there is already
    /// another durable wake-up (a retry scheduled before the turn).
    ///
    /// Lenient read: an old binary reads the first 8 bytes and sees a retry
    /// with `attempt` 0 — one extra turn when it fires, not wrong. No
    /// `DATA_FORMAT` change.
    ///
    /// `retry = (deadline, attempt, journal length)` is the scheduled retry of
    /// a turn run by a scheduled retry: **the attempt is counted here**, in the
    /// same `Running` commit, not when the background tick hands the turn over.
    /// A turn that is throttled, cancelled or fails before this point uses no
    /// attempt; a death after this point has the attempt counted.
    pub async fn begin_turn(
        &mut self,
        id: &InvocationId,
        watch_due: Option<u64>,
        retry: Option<(u64, u32, u32)>,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        if let Some(slot) = self.vqueue_of(id).await? {
            if slot.stage != Stage::Running {
                muts.move_vqueue(&slot.queue, slot.seq, slot.stage, Stage::Running, id);
            }
        }
        if let Some(due) = watch_due {
            muts.add_retry(due, id, encode_watch().as_slice());
            self.next_due.offer(due);
        }
        if let Some((due, attempt, journal_len)) = retry {
            muts.add_retry(due, id, encode_retry(attempt, journal_len).as_slice());
            self.next_due.offer(due);
        }
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
        }
        Ok(())
    }

    /// **Ends a turn in one commit** (ending a turn is one atomic commit, so
    /// there is no durable state between "left the queue" and "watchdog
    /// removed"): leaves the queue (`terminal`) or moves to `stage`; removes
    /// the turn's watchdog row (`watch_due`); and sets the retry `retry =
    /// (deadline, attempt, journal length)` if any. On failure nothing
    /// changes: the watchdog row is still there and is still the invocation's
    /// durable wake-up.
    pub async fn end_turn(
        &mut self,
        id: &InvocationId,
        terminal: bool,
        stage: Stage,
        watch_due: Option<u64>,
        retry: Option<(u64, u32, u32)>,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        if let Some(slot) = self.vqueue_of(id).await? {
            if terminal {
                muts.pop_vqueue(&slot.queue, slot.stage, slot.seq, id);
            } else if slot.stage != stage {
                muts.move_vqueue(&slot.queue, slot.seq, slot.stage, stage, id);
            }
        }
        // Ended for good: the engine's ending commit (`Effect::Finish`, or
        // `fail_with`) already dropped the watchdog row together with the
        // terminal status — removing it again here is a commit (and a durable
        // wait for the caller) for nothing.
        if let Some(w) = watch_due.filter(|_| !terminal) {
            muts.remove_retry(w, id);
        }
        if let Some((due, attempt, journal_len)) = retry {
            muts.add_retry(due, id, encode_retry(attempt, journal_len).as_slice());
            self.next_due.offer(due);
        }
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
        }
        Ok(())
    }

    /// **The durable part of a `cairn_lifecycle::Plan`**: the stage and retry
    /// row of `id` in **one** commit — the path of the lifecycle core's shell,
    /// replacing the chain of `begin_turn`/`end_turn`/`schedule_retry`/
    /// `reschedule_retry`/`cancel_retry`/`undefer_retry` that the turn paths
    /// used to call directly.
    ///
    /// `slot`: the queue entry the shell read for the core (`Some(None)` =
    /// none), or `None` = not read (read here if the stage changes). A stage
    /// change without an entry is skipped, like `begin_turn`/`end_turn`. Rows
    /// are written at the codec's canonical width; the index row at the old
    /// deadline (if the shell knows it and the deadline changed) is deleted in
    /// the same commit, otherwise it becomes a stale row, cleaned up by a scan.
    pub async fn apply_lifecycle(
        &mut self,
        id: &InvocationId,
        slot: Option<&Option<VQueueSlot>>,
        stage: StageWrite,
        row: RowWrite,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        self.stage_lifecycle(&mut muts, id, slot, stage, row)
            .await?;
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
            #[cfg(feature = "lifecycle-check")]
            self.assert_lifecycle(id).await?;
        }
        Ok(())
    }

    /// Like [`Self::apply_lifecycle`], plus the error of the attempt that just
    /// failed — **in the same commit** that schedules the next attempt, so that
    /// attempt sees this error even if the leader changes midway. The error is
    /// not lifecycle state: the commit happens even when the Plan changes
    /// neither stage nor row.
    pub async fn apply_lifecycle_noting(
        &mut self,
        id: &InvocationId,
        slot: Option<&Option<VQueueSlot>>,
        stage: StageWrite,
        row: RowWrite,
        failure: &cairn_store::keys::LastFailure,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        self.stage_lifecycle(&mut muts, id, slot, stage, row)
            .await?;
        muts.set_last_failure(id, failure);
        self.writer.commit(muts).await?;
        #[cfg(feature = "lifecycle-check")]
        self.assert_lifecycle(id).await?;
        Ok(())
    }

    /// Stages the stage and retry row of a Plan into `muts` — the part shared
    /// by [`Self::apply_lifecycle`] and the status commits that carry a message
    /// ([`Self::pause_in`], [`Self::fail_in`], [`Self::restart_in`]).
    pub(super) async fn stage_lifecycle(
        &mut self,
        muts: &mut Mutations,
        id: &InvocationId,
        slot: Option<&Option<VQueueSlot>>,
        stage: StageWrite,
        row: RowWrite,
    ) -> Result<()> {
        self.take_audit_for(id, muts);
        if stage != StageWrite::Keep {
            let read;
            let slot = match slot {
                Some(s) => s.as_ref(),
                None => {
                    read = self.vqueue_of(id).await?;
                    read.as_ref()
                }
            };
            if let Some(slot) = slot {
                match stage {
                    StageWrite::Keep => {}
                    StageWrite::Move(to) if to != slot.stage => {
                        muts.move_vqueue(&slot.queue, slot.seq, slot.stage, to, id);
                    }
                    StageWrite::Move(_) => {}
                    StageWrite::Remove => {
                        muts.pop_vqueue(&slot.queue, slot.stage, slot.seq, id);
                    }
                }
            }
        }
        self.stage_row(muts, id, row);
        Ok(())
    }

    /// Many retry rows in **one** commit: `(invocation, old deadline, new row)`
    /// — the busy deferral of a whole background tick.
    pub async fn set_retry_rows(
        &mut self,
        rows: &[(InvocationId, Option<u64>, cairn_lifecycle::RetryRow)],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut muts = Mutations::new();
        for (id, old_due, row) in rows {
            self.stage_row(
                &mut muts,
                id,
                RowWrite::Set {
                    old_due: *old_due,
                    row: *row,
                },
            );
        }
        self.writer.commit(muts).await?;
        #[cfg(feature = "lifecycle-check")]
        for (id, ..) in rows {
            self.assert_lifecycle(id).await?;
        }
        Ok(())
    }

    pub(super) fn stage_row(&mut self, muts: &mut Mutations, id: &InvocationId, row: RowWrite) {
        match row {
            RowWrite::Keep => {}
            RowWrite::Set { old_due, row } => {
                if let Some(old) = old_due.filter(|d| *d != row.due) {
                    muts.remove_retry(old, id);
                }
                muts.add_retry(row.due, id, row.encode().as_slice());
                self.next_due.offer(row.due);
            }
            RowWrite::Clear { old_due: Some(d) } => {
                muts.remove_retry(d, id);
            }
            RowWrite::Clear { old_due: None } => {
                muts.drop_retry(id);
            }
        }
    }

    /// Removes an invocation from the queue for good — it has finished for good.
    pub async fn dequeue(&mut self, id: &InvocationId) -> Result<()> {
        let Some(slot) = self.vqueue_of(id).await? else {
            return Ok(());
        };
        let mut muts = Mutations::new();
        muts.pop_vqueue(&slot.queue, slot.stage, slot.seq, id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// The whole vqueue table — the **one-time** scan on becoming leader (leader-up step 1).
    pub async fn all_vqueues(&self) -> Result<Vec<VQueueRow>> {
        Ok(self
            .writer
            .all_vqueues_unbounded()
            .await?
            .into_iter()
            .map(|(name, queue, stage, seq, id)| VQueueRow {
                slot: VQueueSlot {
                    name,
                    queue,
                    stage,
                    seq,
                },
                id,
            })
            .collect())
    }

    /// **Leader-up step 2.** Moves every `Running` entry out of `Running` in one
    /// combined commit: back to `Inbox`, **except** entries of queues holding a
    /// VO lock — those go to `Suspended`.
    ///
    /// An entry in `Running` means the old leader died mid-turn. They are
    /// **not** counted — they ask for a permit again through the normal path.
    ///
    /// **But the VO lock must not be released.** A turn's journal is committed
    /// incrementally, so a turn that died midway may already have written a
    /// state-read command together with its completion. Going back to `Inbox`
    /// declares the object unlocked: a call arriving later runs first and
    /// writes state, then the old turn replays exactly the value it read from
    /// the journal and overwrites it. `Suspended` is the "holds the lock, not
    /// running" stage — exactly where a turn that failed midway belongs, like
    /// `drive`'s error path.
    ///
    /// This is where the whole design locks in an invariant: **after becoming
    /// leader, no counter is nonzero.** A system that naturally falls back to a
    /// clean state on every leader change cannot accumulate counter leaks —
    /// they are wiped out before they can become an incident. The price: every
    /// invocation running when the leader was lost must ask for a permit
    /// again, i.e. a burst right after failover. That is the right trade-off.
    ///
    /// Returns the number of entries moved back to `Inbox`.
    pub async fn requeue_running(&mut self) -> Result<usize> {
        Ok(self.recover_vqueues().await?.1)
    }

    /// **One chunk of the VO lock rebuild on becoming leader** (leader-up step
    /// 1): the queue entries of every **unfinished** invocation within at most
    /// `limit` `Invocations` index rows after `after`
    /// (exclusive), in id order, with the next chunk's cursor — `None` when
    /// done. The shell asks the core (`Event::LeaderUp`) for each entry, writes
    /// the chunk's stage changes ([`Self::apply_stages`]) and rebuilds the locks
    /// **within the same engine hold**, then releases the engine before the
    /// next chunk.
    ///
    /// **Why walk the `Invocations` index, not the queue index.** The queue
    /// index key is `(queue, stage, seq, id)`: every finished invocation
    /// leaves a tombstone in `Inbox` and one in `Running`, spread over the
    /// queues, and SlateDB's filter walks over them **inside one read** — a
    /// chunk of "at most N live rows" can still walk the whole history in one
    /// hold (measured: 1.1–1.8 s at 10⁶). The `Invocations` index keeps the
    /// rows of finished invocations until purge — **live**, countable rows —
    /// so the chunk has a real cap, like `recover` ([`Self::runnable_chunk`]).
    /// Entries are looked up by id (one GET of the `VQueueIndex` entity row)
    /// only for unfinished invocations.
    ///
    /// Entries of finished invocations are not read: every ending path removes
    /// the entry in the same commit that writes the status, and an orphan
    /// entry from old data never becomes a lock (FL-8) — it is only a leak,
    /// cured by one more cancel.
    pub async fn restore_chunk(
        &self,
        after: Option<&InvocationId>,
        limit: usize,
    ) -> Result<(Vec<VQueueRow>, Option<InvocationId>)> {
        let limit = limit.max(1);
        let tags = self
            .writer
            .invocation_tags_after(after, limit, RECOVER_SCAN)
            .await?;
        let next = (tags.len() >= limit)
            .then(|| tags.last().map(|(id, _)| *id))
            .flatten();
        let mut rows = Vec::new();
        for (id, tag) in tags {
            if InvocationStatus::tag_is_finished(tag) {
                continue;
            }
            if let Some(slot) = self.vqueue_of(&id).await? {
                rows.push(VQueueRow { slot, id });
            }
        }
        Ok((rows, next))
    }

    /// The VO lock rebuild has walked every chunk ([`Self::restore_chunk`]):
    /// `max_seq` is the largest `seq` among the entries it saw. From here the
    /// queue `seq` counter exists (if it did not), and the writer knows the
    /// queue `seq` for sure — the next commit makes it durable for the next
    /// leader (`floors::QUEUE_HIGH`).
    /// Never goes backwards: a watermark already known for sure (mark matches)
    /// and `seq`s issued during the rebuild are kept if higher.
    pub fn finish_restore(&mut self, max_seq: u64) {
        let known = self.writer.queue_seq_high().unwrap_or(0);
        let next = max_seq.max(known) + 1;
        let next = self.next_seq.map_or(next, |n| n.max(next));
        self.next_seq = Some(next);
        self.writer.learn_queue_high(next - 1);
    }

    /// Changes the stage of many entries in **one** commit — the durable part
    /// of one `restore_flow` chunk (leader-up step 2 and cleaning up orphan
    /// entries). The entries come from a chunk of
    /// [`Self::restore_chunk`].
    pub async fn apply_stages(&mut self, writes: &[(VQueueRow, StageWrite)]) -> Result<()> {
        let mut muts = Mutations::new();
        for (r, w) in writes {
            match *w {
                StageWrite::Keep => {}
                StageWrite::Move(to) if to != r.slot.stage => {
                    muts.move_vqueue(&r.slot.queue, r.slot.seq, r.slot.stage, to, &r.id);
                }
                StageWrite::Move(_) => {}
                StageWrite::Remove => {
                    muts.pop_vqueue(&r.slot.queue, r.slot.stage, r.slot.seq, &r.id);
                }
            }
        }
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
        }
        #[cfg(feature = "lifecycle-check")]
        for (r, _) in writes {
            self.assert_lifecycle(&r.id).await?;
        }
        Ok(())
    }

    /// **Leader-up steps 1 and 2 on one snapshot**: scans the whole queue
    /// table **once**, seeds the queue `seq` counter
    /// ([`Self::next_queue_seq`]) from it, then moves `Running` entries out of
    /// `Running` like [`Self::requeue_running`]. Returns the snapshot
    /// **before** the move (the call site classifies locks on it) and the
    /// number of entries moved back to `Inbox`.
    ///
    /// One scan instead of three (lock rebuild, move, `seq` seeding) — each is
    /// a pass re-reading the same SST blocks on becoming leader. Seeding `seq`
    /// from the snapshot is exactly like lazy seeding: the move keeps `seq`,
    /// and every queue written afterwards takes its `seq` from this very
    /// counter.
    pub async fn recover_vqueues(&mut self) -> Result<(Vec<VQueueRow>, usize)> {
        let rows = self.all_vqueues().await?;
        if self.next_seq.is_none() {
            let max = rows.iter().map(|r| r.slot.seq).max();
            self.next_seq = Some(max.map_or(1, |m| m + 1));
        }
        let mut muts = Mutations::new();
        let mut moved = 0usize;
        for r in &rows {
            if r.slot.stage == Stage::Running {
                let to = if r.slot.name.lock_name().is_some() {
                    Stage::Suspended
                } else {
                    moved += 1;
                    Stage::Inbox
                };
                muts.move_vqueue(&r.slot.queue, r.slot.seq, Stage::Running, to, &r.id);
            }
        }
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
        }
        Ok((rows, moved))
    }
}
