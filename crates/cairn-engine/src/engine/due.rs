// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Due work: scheduled purges, delayed calls, retries, timers.

use super::*;

impl<S: RangeStore> Engine<S> {
    /// Scheduled cleanups that are due.
    pub async fn due_purges(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId)>> {
        let got = self.writer.due_purges(now_ms, limit).await?;
        self.forget_stale(got.stale).await?;
        Ok(got.rows)
    }

    /// Moves one-way calls whose time has come from the `delayed` table to the
    /// outbox.
    ///
    /// One commit per row: add the outbox row + delete the delayed row. Two
    /// commits would open a duplication window (a death midway ⇒ the row is in
    /// both places) — harmless thanks to the deterministic key, but there is no
    /// reason to open it.
    ///
    /// Returns the number of rows moved — the call site uses it to know whether
    /// there is anything new for `drain_outbox`.
    pub async fn promote_delayed(&mut self, now_ms: u64, limit: usize) -> Result<usize> {
        let due = self.writer.due_delayed(now_ms, limit).await?;
        self.next_due.offer_scanned(due.next_due);
        self.forget_stale(due.stale).await?;
        let moved = due.rows.len();
        for (due_ms, id, completion_id, payload) in due.rows {
            let mut muts = Mutations::new();
            self.stage_outbox(
                &mut muts,
                partition_key_of(&id),
                (id, completion_id),
                &payload,
            )
            .await?;
            muts.remove_delayed(due_ms, &id, completion_id);
            self.writer.commit(muts).await?;
        }
        Ok(moved)
    }

    /// Schedules a rerun of an invocation at `due_ms`, as attempt number `attempt`.
    ///
    /// `journal_len` is the journal length **at scheduling time**. It is there
    /// to answer exactly the question `retry_count_since_last_stored_entry`
    /// asks: is the next attempt still stuck at *the same place*? Without it, a
    /// ten-step workflow where each step fails transiently once would count up
    /// to ten — and an SDK configured with `maxRetryAttempts: 3` would wrongly
    /// give up at the fourth step, although no step is stuck.
    pub async fn schedule_retry(
        &mut self,
        id: &InvocationId,
        due_ms: u64,
        attempt: u32,
        journal_len: u32,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        muts.add_retry(due_ms, id, encode_retry(attempt, journal_len).as_slice());
        self.next_due.offer(due_ms);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Moves a scheduled retry: deletes the old one and sets the new one in
    /// **one** commit.
    ///
    /// Setting the next retry **before** running again is on purpose. A death
    /// midway still leaves a retry in the future, so the invocation is never
    /// stuck forever in the "should have been rerun but nobody calls it" state.
    /// The price is at-least-once retries — exactly what we want for retries.
    ///
    /// `fires` is the number of fires not yet counted as attempts, including
    /// this one ([`encode_retry_fired`], FL-21).
    pub async fn reschedule_retry(
        &mut self,
        id: &InvocationId,
        old_due_ms: u64,
        new_due_ms: u64,
        attempt: u32,
        journal_len: u32,
        fires: u32,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        muts.remove_retry(old_due_ms, id).add_retry(
            new_due_ms,
            id,
            encode_retry_fired(attempt, journal_len, fires).as_slice(),
        );
        self.next_due.offer(new_due_ms);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// The **pending** scheduled retry of an invocation, if any: `due_ms`.
    ///
    /// Reads the retry entity row — an invocation has at most one. A row ⇒ the
    /// invocation is in a retry cycle, and that scheduled retry (with its
    /// attempt count) is what must run it next — see the server's `recover`.
    ///
    /// **Watchdog rows do not count**: a watchdog row is a safety net for a
    /// turn that died, not a retry with a backoff step — the caller (recovery
    /// on becoming leader, the rescue net) must continue the invocation right
    /// away rather than wait for its deadline.
    pub async fn pending_retry(&self, id: &InvocationId) -> Result<Option<u64>> {
        Ok(self
            .writer
            .get(&cairn_store::keys::retry(id))
            .await?
            .and_then(|v| {
                cairn_store::keys::decode_due(&v)
                    .filter(|(_, payload)| !is_watch(payload))
                    .map(|(due, _)| due)
            }))
    }

    /// [`Self::pending_retry`] for a whole set: the invocations in `ids` with a
    /// pending scheduled retry (watchdog rows not counted).
    ///
    /// For recovery on becoming leader: one scan of the retry index **from the
    /// durable watermark** instead of one entity-row read per `running`
    /// invocation; only ids in both must read the entity row (check the
    /// watchdog flag, and check the index row is not stale). A wrong watermark
    /// — past a live row, which FL-18 forbids — only makes an invocation rerun
    /// early, it does not make it stuck.
    pub async fn pending_retries_among(
        &self,
        ids: &[InvocationId],
    ) -> Result<std::collections::BTreeSet<InvocationId>> {
        let wanted: std::collections::BTreeSet<&InvocationId> = ids.iter().collect();
        let mut checked = std::collections::BTreeSet::new();
        let mut out = std::collections::BTreeSet::new();
        for (_, id) in self.writer.retry_index_unbounded().await? {
            if !wanted.contains(&id) || !checked.insert(id) {
                continue;
            }
            if self.pending_retry(&id).await?.is_some() {
                out.insert(id);
            }
        }
        Ok(out)
    }

    /// Moves **many** scheduled retries in **one** commit, keeping each one's
    /// attempt count, journal length and uncounted fires: `(invocation, old
    /// deadline, new deadline, attempt, journal_len, fires)`.
    ///
    /// For the background tick meeting a busy invocation: its scheduled retry
    /// is the safety net, the task holding the lock owns the next attempt — the
    /// task cancels it when done, reschedules it on failure, and both overwrite
    /// the entity row. The old index row of the move becomes a stale row,
    /// cleaned up by the next scan. Moving them one commit each would be one
    /// commit per waiting invocation per tick — exactly what this replaces.
    pub async fn defer_retries(
        &mut self,
        rows: &[(InvocationId, u64, u64, u32, u32, u32)],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut muts = Mutations::new();
        let mut earliest = u64::MAX;
        for (id, old_due, new_due, attempt, journal_len, fires) in rows {
            muts.remove_retry(*old_due, id).add_retry(
                *new_due,
                id,
                encode_retry_fired(*attempt, *journal_len, *fires).as_slice(),
            );
            earliest = earliest.min(*new_due);
        }
        self.next_due.offer(earliest);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Undoes the busy deferral ([`Self::defer_retries`]) of the scheduled
    /// retry that the turn holding the lock had already set at `planned_ms`:
    /// the turn just failed retryably, the lock is about to be released, so the
    /// next attempt must run at `max(planned_ms, now_ms)` — not after a whole
    /// permit-wait cap.
    ///
    /// While the lock is held only the background tick can move the row, so a
    /// row with a deadline later than the target is a row that was deferred.
    /// The attempt count and journal length on the row are kept. Watchdog rows
    /// and rows that no longer exist (cancelled) are left alone. Returns `true`
    /// when it moved the row back.
    pub async fn undefer_retry(
        &mut self,
        id: &InvocationId,
        planned_ms: u64,
        now_ms: u64,
    ) -> Result<bool> {
        let Some(raw) = self.writer.get(&cairn_store::keys::retry(id)).await? else {
            return Ok(false);
        };
        let Some((due, payload)) = cairn_store::keys::decode_due(&raw) else {
            return Ok(false);
        };
        let to = planned_ms.max(now_ms);
        if is_watch(payload) || due <= to {
            return Ok(false);
        }
        let mut muts = Mutations::new();
        muts.remove_retry(due, id).add_retry(to, id, payload);
        self.next_due.offer(to);
        self.writer.commit(muts).await?;
        Ok(true)
    }

    pub async fn cancel_retry(&mut self, id: &InvocationId, due_ms: u64) -> Result<()> {
        let mut muts = Mutations::new();
        muts.remove_retry(due_ms, id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Scheduled retries that are due: `(due_ms, invocation, attempts so far,
    /// journal length at scheduling time)`.
    ///
    /// Rows written by old binaries have only 4 bytes (no journal length). They
    /// return [`u32::MAX`], meaning "unknown" — and the progress comparison is
    /// then always false, i.e. it never resets the count. Choosing that rather
    /// than `0` is on purpose: `0` would make **every** old row reset its count
    /// once during the upgrade, and a cluster doing a rolling restart would
    /// silently extend invocations that should already have run out of
    /// attempts.
    pub async fn due_retries(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId, u32, u32)>> {
        Ok(self
            .due_retries_fired(now_ms, limit)
            .await?
            .into_iter()
            .map(|(due, id, attempt, journal_len, _)| (due, id, attempt, journal_len))
            .collect())
    }

    /// [`Self::due_retries`] with the uncounted fires of each row
    /// ([`encode_retry_fired`], FL-21): `(due_ms, invocation, attempt,
    /// journal length, fires)`. A row without the tail returns `fires` 0.
    pub async fn due_retries_fired(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId, u32, u32, u32)>> {
        Ok(self
            .due_retry_rows(now_ms, limit)
            .await?
            .into_iter()
            .map(|(id, r)| (r.due, id, r.attempt, r.journal_len, r.fires))
            .collect())
    }

    /// Due scheduled retries, decoded with the lifecycle core's codec:
    /// `(invocation, row)`. The path of the core's shell (`turn::fire_retries`).
    pub async fn due_retry_rows(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(InvocationId, cairn_lifecycle::RetryRow)>> {
        let got = self.writer.due_retries(now_ms, limit).await?;
        self.next_due.offer_scanned(got.next_due);
        self.forget_stale(got.stale).await?;
        Ok(got
            .rows
            .into_iter()
            .map(|(due, id, raw)| (id, decode_row(due, &raw)))
            .collect())
    }

    /// Retry rows with a deadline in `(from_ms, until_ms]` (**future**
    /// deadlines), after cursor `after`, reading at most `limit` index rows —
    /// for the shell to pull timed rows in when a tenant is unsuspended, batch
    /// by batch. Also returns the cursor to continue the scan (`None` = range
    /// exhausted). Does not change the background loop's next deadline and does
    /// not clean up stale rows: that is the job of [`Self::due_retry_rows`]
    /// with `now`.
    #[allow(clippy::type_complexity)]
    pub async fn retry_rows_between(
        &self,
        from_ms: u64,
        after: Option<(u64, InvocationId)>,
        until_ms: u64,
        limit: usize,
    ) -> Result<(
        Vec<(InvocationId, cairn_lifecycle::RetryRow)>,
        Option<(u64, InvocationId)>,
    )> {
        let got = self
            .writer
            .retry_rows_between(from_ms, after, until_ms, limit)
            .await?;
        Ok((
            got.rows
                .into_iter()
                .map(|(due, id, raw)| (id, decode_row(due, &raw)))
                .collect(),
            got.next,
        ))
    }

    /// The retry row (retry or watchdog) of `id`, decoded — the `retry` slot of
    /// `cairn_lifecycle::View`. An invocation has at most one.
    pub async fn retry_row(&self, id: &InvocationId) -> Result<Option<cairn_lifecycle::RetryRow>> {
        Ok(self
            .writer
            .get(&cairn_store::keys::retry(id))
            .await?
            .and_then(|v| {
                cairn_store::keys::decode_due(&v).map(|(due, payload)| decode_row(due, payload))
            }))
    }

    /// Due timers, in due order, reading at most `limit` index rows. Read on
    /// the leader, so it also sees timers committed in the previous step. Stale
    /// index rows are cleaned up, not returned. Cap reached ⇒ the next deadline
    /// is the next due row (the background loop wakes again right away) and the
    /// next call continues after the last row this one read
    /// ([`cairn_store::PartitionWriter::due_timers`]).
    pub async fn due_timers(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId, u32)>> {
        let got = self.writer.due_timers(now_ms, limit).await?;
        self.next_due.offer_scanned(got.next_due);
        self.forget_stale(got.stale).await?;
        Ok(got.rows)
    }

    /// Kept as is for the timer path.
    pub async fn fire_timer(
        &mut self,
        inv: &mut Invocation,
        due_ms: u64,
        completion_id: u32,
    ) -> Result<Vec<Message>> {
        self.deliver(
            inv,
            Completion {
                id: completion_id,
                result: CompletionResult::Timer,
            },
            Source::Timer { due_ms },
        )
        .await
    }

    /// Deletes a timer without delivering any completion.
    ///
    /// Used when its invocation stopped before the timer fired (cancelled).
    /// Delivering a completion to a terminal invocation is running it again —
    /// much worse than a superfluous timer row.
    pub async fn drop_timer(
        &mut self,
        due_ms: u64,
        id: &InvocationId,
        completion_id: u32,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        muts.remove_timer(due_ms, id, completion_id);
        self.writer.commit(muts).await?;
        Ok(())
    }
}
