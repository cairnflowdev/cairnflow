// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Server-side schedules: read, write, cross-range fire.
//!
//! A schedule row is pending work (INV-3): "fire X at T" lives in the
//! `Schedules` table of the range holding the schedule's pk, with the
//! `ScheduleDue` index row written in the same commit. The firing decision
//! (missed instants, overlap, the L0 throttle) belongs to the background loop
//! in `cairn-server/src/turn/schedule.rs`; the engine only provides the
//! **single-commit** writes that decision needs.

use {
    super::*,
    crate::{
        outbox::OutboundFire,
        schedule::{RowError, ScheduleRow},
    },
};

impl<S: RangeStore> Engine<S> {
    /// Adds schedule row `w` to `muts` (entity row + index) and tells the
    /// background loop the new deadline.
    ///
    /// **Tripwire** `schedule::live_without_next` (CG-12): a live (not paused)
    /// schedule without a next deadline is a schedule that died silently —
    /// nobody fires it anymore.
    pub(super) fn stage_schedule(&mut self, muts: &mut Mutations, w: &ScheduleWrite<'_>) {
        self.take_audit_for(&w.id, muts);
        if !w.row.paused && w.due == u64::MAX {
            cairn_store::failpoint!("schedule::live_without_next");
        }
        muts.put_schedule(&w.id, w.old_due, w.due, &w.row.encode());
        if w.due != u64::MAX {
            self.next_due.offer(w.due);
        }
    }

    /// Schedule row `id`: `(next fire deadline, row)`. An unreadable row ⇒ a
    /// `Corrupt` error (strict read).
    pub async fn schedule(&self, id: &InvocationId) -> Result<Option<(u64, ScheduleRow)>> {
        let Some((due, raw)) = self.writer.schedule(id).await? else {
            return Ok(None);
        };
        Ok(Some((due, decode_row(&raw)?)))
    }

    /// Writes one schedule row — one commit.
    pub async fn put_schedule(&mut self, w: ScheduleWrite<'_>) -> Result<()> {
        let mut muts = Mutations::new();
        self.stage_schedule(&mut muts, &w);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Deletes schedule `id` currently at deadline `due` — one commit. A
    /// logical delete inside the Db (class L, `docs/reliability/irreversible.md`):
    /// runs already created are not touched.
    pub async fn delete_schedule(&mut self, id: &InvocationId, due: u64) -> Result<()> {
        let mut muts = Mutations::new();
        self.take_audit_for(id, &mut muts);
        muts.remove_schedule(id, due);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Schedules due at `now_ms`, at most `limit`: `(deadline, id, row)`.
    /// Stale index rows are cleaned up; unreadable rows are skipped (they stay
    /// — a newer binary can read them) with a `warn` line.
    pub async fn due_schedules(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId, ScheduleRow)>> {
        let got = self.writer.due_schedules(now_ms, limit).await?;
        self.next_due.offer_scanned(got.next_due);
        self.forget_stale(got.stale).await?;
        let mut out = Vec::with_capacity(got.rows.len());
        for (due, id, raw) in got.rows {
            match ScheduleRow::decode(&raw) {
                Ok(row) => out.push((due, id, row)),
                Err(e) => tracing::warn!(
                    schedule = %cairn_store::id_hex(&id),
                    error = %e,
                    "unreadable schedule row — skipped"
                ),
            }
        }
        Ok(out)
    }

    /// Every schedule of the range (by index), at most `limit`: `(deadline,
    /// id, row)`. The range's number of schedules — counts `ScheduleDue` index
    /// rows (the derived zone, without reading schedule rows). For the
    /// `cairnflow_schedules` gauge on becoming leader.
    pub async fn schedule_count(&self) -> Result<usize> {
        Ok(self.writer.schedule_index(usize::MAX).await?.len())
    }

    /// One chunk of the range's schedules by index, from **after** cursor
    /// `after`: at most `limit` index rows. Returns the rows still matching the
    /// index and the next cursor — `None` when the chunk is not full (index
    /// exhausted).
    #[allow(clippy::type_complexity)]
    pub async fn list_schedules_page(
        &self,
        after: Option<(u64, InvocationId)>,
        limit: usize,
    ) -> Result<(
        Vec<(u64, InvocationId, ScheduleRow)>,
        Option<(u64, InvocationId)>,
    )> {
        let index = self.writer.schedule_index_after(after, limit).await?;
        let next = if index.len() == limit {
            index.last().copied()
        } else {
            None
        };
        let mut out = Vec::new();
        for (due, id) in index {
            if let Some((d, row)) = self.schedule(&id).await? {
                if d == due {
                    out.push((due, id, row));
                }
            }
        }
        Ok((out, next))
    }

    pub async fn list_schedules(
        &self,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId, ScheduleRow)>> {
        let mut out = Vec::new();
        for (due, id) in self.writer.schedule_index(limit).await? {
            if let Some((d, row)) = self.schedule(&id).await? {
                if d == due {
                    out.push((due, id, row));
                }
            }
        }
        Ok(out)
    }

    /// A cross-range fire: the `Fire` outbox row and the schedule row with
    /// `next_at` moved, in **one** commit (CG-12). The drainer sends the
    /// `Fire` as a `StartCall`; the target range deduplicates on the
    /// idempotency key (CG-10).
    pub async fn fire_schedule_remote(
        &mut self,
        w: ScheduleWrite<'_>,
        fire: OutboundFire,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        let key = (fire.schedule, fire.fire_seq);
        self.stage_outbox(
            &mut muts,
            partition_key_of(&fire.schedule),
            key,
            &OutboxEntry::Fire(fire).encode(),
        )
        .await?;
        self.stage_schedule(&mut muts, &w);
        self.writer.commit(muts).await?;
        Ok(())
    }
}

fn decode_row(raw: &[u8]) -> Result<ScheduleRow> {
    ScheduleRow::decode(raw).map_err(|e: RowError| {
        EngineError::Store(StoreError::Corrupt(format!("schedule row: {e}")))
    })
}
