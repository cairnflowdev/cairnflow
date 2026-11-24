// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Reads: journal, invocation lists, pins, head, status.

use super::*;

impl<S: RangeStore> Engine<S> {
    /// The raw journal of an invocation: `(entry_index, message_type, payload)`.
    ///
    /// Enough to compare two runs bit-exactly — that is the core durability
    /// criterion, and without a journal read path it could only check the
    /// output.
    pub async fn journal_entries(&self, id: &InvocationId) -> Result<Vec<JournalRow>> {
        let mut out = Vec::new();
        for (index, raw) in self.writer.journal_unbounded(id).await? {
            let entry = JournalEntry::decode(raw)?;
            out.push((index, entry.ty.raw(), entry.payload.to_vec()));
        }
        Ok(out)
    }

    /// One page of the raw journal: like [`Self::journal_entries`] from index
    /// `from`, capped in entries and bytes; with the next entry index when
    /// there is more.
    pub async fn journal_page(
        &self,
        id: &InvocationId,
        from: u32,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(Vec<JournalRow>, Option<u32>)> {
        let (rows, next) = self
            .writer
            .journal_page(id, from, max_entries, max_bytes)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (index, raw) in rows {
            let entry = JournalEntry::decode(raw)?;
            out.push((index, entry.ty.raw(), entry.payload.to_vec()));
        }
        Ok((out, next))
    }

    /// Invocations that have not finished for good: `running` and `suspended`.
    /// The recovery path uses [`Self::runnable_invocations_unbounded`], not
    /// this function.
    ///
    /// Reads the `Invocations` index (status tag), does not scan the entity zone.
    pub async fn unfinished_invocations_unbounded(&self) -> Result<Vec<InvocationId>> {
        self.invocations_tagged(InvocationStatus::tag_is_live).await
    }

    /// **`cairn_lifecycle::valid` over every invocation of the range**: the
    /// testkit's check after every step, like
    /// `PartitionWriter::rows_below_floors` (FL-18). Gathers ids from the
    /// status index, the queue table and the retry index, then reads the three
    /// slots (P1, P2, P3) of each id. Reads whole tables — only for tests and
    /// tools, not for the turn path.
    pub async fn lifecycle_violations(
        &self,
    ) -> Result<Vec<(InvocationId, cairn_lifecycle::Invalid)>> {
        let mut ids = std::collections::BTreeSet::new();
        ids.extend(
            self.writer
                .invocation_tags_unbounded()
                .await?
                .into_iter()
                .map(|(id, _)| id),
        );
        ids.extend(self.all_vqueues().await?.into_iter().map(|r| r.id));
        ids.extend(
            self.writer
                .retry_index_unbounded()
                .await?
                .into_iter()
                .map(|(_, id)| id),
        );
        let mut out = Vec::new();
        for id in ids {
            let view = cairn_lifecycle::View::new(
                0,
                cairn_lifecycle::Knobs::DEFAULT,
                cairn_lifecycle::Attendance::IDLE,
            )
            .with_status(lifecycle_status(self.status(&id).await?.as_ref()))
            .with_slot(self.vqueue_of(&id).await?.as_ref().map(lifecycle_slot))
            .with_retry(self.retry_row(&id).await?);
            if let Err(e) = cairn_lifecycle::valid(&view) {
                out.push((id, e));
            }
        }
        Ok(out)
    }

    /// Invocations **that have work to run** — the recovery path on becoming
    /// leader.
    ///
    /// Only `running`: invocations that never ran (waiting for a lock/permit),
    /// were mid-run when the old leader died, or are `suspended` with a
    /// completion/signal already fed in (see [`Self::deliver`]) but whose turn
    /// has not run yet. `suspended` is not included: it waits for an event that
    /// has not arrived, and the normal paths (timers, outbox, inbound, signals)
    /// wake it when it does — rerunning it here is an SDK turn only to suspend
    /// again (a recovery storm after every split).
    pub async fn runnable_invocations_unbounded(&self) -> Result<Vec<InvocationId>> {
        self.invocations_tagged(InvocationStatus::tag_is_runnable)
            .await
    }

    /// One chunk of the recovery scan: the `running` invocations within at
    /// most `limit` `Invocations` index rows **after** `after`
    /// (exclusive), in id order, with the next chunk's cursor — `None` when the
    /// scan is done. The caller releases the engine between chunks: the index
    /// includes finished invocations until purge, so one hold over the whole
    /// index is O(history).
    ///
    /// Large read-ahead and block caching ([`RECOVER_SCAN`]): the next chunk
    /// reopens the scan at the cursor, and the blocks the previous chunk's read
    /// pulled in are already in the cache.
    pub async fn runnable_chunk(
        &self,
        after: Option<&InvocationId>,
        limit: usize,
    ) -> Result<(Vec<InvocationId>, Option<InvocationId>)> {
        let limit = limit.max(1);
        let tags = self
            .writer
            .invocation_tags_after(after, limit, RECOVER_SCAN)
            .await?;
        let next = (tags.len() >= limit)
            .then(|| tags.last().map(|(id, _)| *id))
            .flatten();
        let ids = tags
            .into_iter()
            .filter(|(_, tag)| InvocationStatus::tag_is_runnable(*tag))
            .map(|(id, _)| id)
            .collect();
        Ok((ids, next))
    }

    /// Reads the `Invocations` index (status tag), does not scan the entity zone.
    async fn invocations_tagged(&self, keep: fn(u8) -> bool) -> Result<Vec<InvocationId>> {
        Ok(self
            .writer
            .invocation_tags_unbounded()
            .await?
            .into_iter()
            .filter(|(_, tag)| keep(*tag))
            .map(|(id, _)| id)
            .collect())
    }

    /// Where to return the result, as recorded in entry 0 of an **existing** invocation.
    ///
    /// There is exactly one user, and it is the distinction between "a real
    /// duplicate" and "our own outbox row processed again" on a workflow's
    /// `run` call path — see
    /// `turn::dispatch_call`.
    /// The error of `id`'s most recent failed attempt, if any. Lenient read: a
    /// broken row reads as absent.
    pub async fn last_failure(
        &self,
        id: &InvocationId,
    ) -> Result<Option<cairn_store::keys::LastFailure>> {
        Ok(self
            .writer
            .get(&cairn_store::keys::last_failure(id))
            .await?
            .and_then(|v| cairn_store::keys::LastFailure::decode(&v)))
    }

    pub async fn reply_to_of(&self, id: &InvocationId) -> Result<Option<ReplyTo>> {
        let Some(raw) = self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        else {
            return Ok(None);
        };
        Ok(JournalEntry::decode(raw)
            .ok()
            .as_ref()
            .and_then(reply_to_of))
    }

    /// The invocations `id` has called with `ctx.call` and **not yet received
    /// a result from**.
    ///
    /// Reads the journal directly, no reverse table needed: a `Call` command
    /// declares two completions — one receives the callee's id, one receives
    /// the result. That pair is enough to answer both questions that need
    /// answering: *who is the child* (the `0x800E` notification carries the
    /// id) and *is it done* (whether the result completion has a notification).
    ///
    /// **`ctx.send` is not in this list.** `OneWayCall` declares no result
    /// completion, so the caller never waits for it — it is *detached* work,
    /// not a branch of the running tree, so cancelling the caller does not
    /// reach it (the same holds for delayed calls). This is a debatable
    /// boundary, so it must be documented rather than left implicit in the
    /// code.
    pub async fn live_children(&self, id: &InvocationId) -> Result<Vec<(u32, InvocationId)>> {
        use cairn_protocol::{decode, Flags, Frame};

        // `Call`s issued: id_completion → result_completion.
        let mut calls: Vec<(u32, u32)> = Vec::new();
        // id_completion → the callee's id, read from notification 0x800E.
        let mut callee_of: cairn_store::entropy::HashMap<u32, InvocationId> =
            cairn_store::entropy::HashMap::default();
        // Result completions that have a notification — i.e. the child has answered.
        let mut answered: cairn_store::entropy::HashSet<u32> =
            cairn_store::entropy::HashSet::default();

        for (_, raw) in self.writer.journal_unbounded(id).await? {
            let entry = JournalEntry::decode(raw)?;
            let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
            let Ok(msg) = decode(&frame) else { continue };
            match msg {
                Message::Call(c) => {
                    calls.push((c.invocation_id_notification_idx, c.result_completion_id));
                }
                Message::CallInvocationIdCompletion(n) => {
                    if let Some(callee) = unhex(&n.invocation_id) {
                        callee_of.insert(n.completion_id, callee);
                    }
                }
                Message::CallCompletion(n) => {
                    answered.insert(n.completion_id);
                }
                _ => {}
            }
        }

        let mut out = Vec::new();
        for (id_completion, result_completion) in calls {
            // The child is not known yet ⇒ the call is still in the outbox,
            // nobody has created it. Nothing to cancel, and the outbox row dies
            // with the parent in `deliver_to_caller` (the caller is terminal ⇒
            // the row is dropped).
            let Some(callee) = callee_of.get(&id_completion).copied() else {
                continue;
            };
            // Already has a result ⇒ the child has finished for good.
            // Cancelling it is a no-op costing a round of writes, and worse: it
            // fills the log with meaningless cancel commands.
            if answered.contains(&result_completion) {
                continue;
            }
            out.push((id_completion, callee));
        }
        Ok(out)
    }

    /// Unfinished invocations pinned to `deployment` on this range: the count,
    /// and at most `limit` ids.
    pub async fn pinned_to(
        &self,
        deployment: &str,
        limit: usize,
    ) -> Result<(usize, Vec<InvocationId>)> {
        Ok(self.writer.pinned_to_unbounded(deployment, limit).await?)
    }

    /// The number of unfinished invocations per deployment, on this range.
    pub async fn pinned_counts(&self) -> Result<std::collections::BTreeMap<String, usize>> {
        Ok(self.writer.pinned_counts_unbounded().await?)
    }

    /// The target and deployment of an invocation, read from entry 0 — **one**
    /// GET, without loading the journal. `None` = no such invocation.
    pub async fn head(&self, id: &InvocationId) -> Result<Option<(Target, Option<String>)>> {
        let Some(raw) = self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        else {
            return Ok(None);
        };
        let first = JournalEntry::decode(raw)?;
        let pinned = self.pinned_of(id, &first).await?;
        Ok(target_of(&first).map(|t| (t, pinned)))
    }

    /// The deployment the invocation is pinned to and the `attempt` of its
    /// pending scheduled retry (0 = none) — for `/invocations`. Two GETs.
    pub async fn invocation_extras(&self, id: &InvocationId) -> Result<(Option<String>, u32)> {
        let deployment = match self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        {
            Some(raw) => self.pinned_of(id, &JournalEntry::decode(raw)?).await?,
            None => None,
        };
        let attempt = self
            .writer
            .get(&cairn_store::keys::retry(id))
            .await?
            .and_then(|v| {
                cairn_store::keys::decode_due(&v)
                    .and_then(|(_, payload)| payload.get(..4).and_then(|b| b.try_into().ok()))
                    .map(u32::from_be_bytes)
            })
            .unwrap_or(0);
        Ok((deployment, attempt))
    }

    /// The deployment the invocation is **currently** pinned to: the `Pins` row
    /// if present (moved by [`Self::migrate`]), otherwise entry 0's header (at
    /// creation).
    pub(super) async fn pinned_of(
        &self,
        id: &InvocationId,
        first: &JournalEntry,
    ) -> Result<Option<String>> {
        if let Some(raw) = self.writer.get(&cairn_store::keys::pin(id)).await? {
            if let Ok(d) = std::str::from_utf8(&raw) {
                return Ok(Some(d.to_string()));
            }
        }
        Ok(deployment_of(first))
    }

    /// The number of journal entries present. Used to tell whether a turn made
    /// **progress**.
    ///
    /// Reads whole values rather than only keys — SlateDB has no key-only
    /// scan. Only runs on the failure path (rescheduling, and when the
    /// scheduled retry fires), so that cost sits exactly where it belongs.
    pub async fn journal_len(&self, id: &InvocationId) -> Result<u32> {
        Ok(self.writer.journal_unbounded(id).await?.len() as u32)
    }

    /// Lists the invocations of this partition, with the routing target when readable.
    ///
    /// The introspection path: scans the `invocation_status` table, then reads
    /// entry 0 of each invocation to get its `Target`. `limit` bounds the cost
    /// — every row adds one GET.
    /// `status`: only invocations whose [`InvocationStatus::name`] matches.
    ///
    /// **The filter must live here, not in the HTTP layer.** Filtering after
    /// cutting at `limit` turns `?status=failed&limit=5` into "among the
    /// *first* 5 invocations, which failed" — so on a system that has run a
    /// few hundred invocations, `cairn invocations --status failed` returns
    /// empty **while work is failing**. An empty list looks exactly like an
    /// answer, and that is the hardest kind of lie to spot during an incident
    /// — the same thing `/invocations` carefully avoids when it is honest about
    /// partition scope.
    ///
    /// Filtering first is also cheaper: reading entry 0 for the `Target` is one
    /// GET per row, and rows that do not match need not pay for that GET.
    pub async fn list_invocations(
        &self,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<(InvocationId, InvocationStatus, Option<Target>)>> {
        Ok(self
            .list_invocations_where(limit, &ListFilter::status(status))
            .await?
            .rows
            .into_iter()
            .map(|(id, s, t, _)| (id, s, t))
            .collect())
    }

    /// Like [`Self::list_invocations`], with the full set of [`ListFilter`]
    /// filters — every filter before `limit`. Each line comes with [`Times`]:
    /// the status row and entry 0 are read through `get_meta` — the same two
    /// GETs as before.
    ///
    /// A capped scan ([`ListFilter::capped`]: by target, by time, or sorted by
    /// time) stops after [`ListFilter::scan_cap`] lines whose status was read,
    /// or after [`LIST_SCAN_BUDGET`], and reports `capped`. The `ended_*`
    /// filters decide on the status row, before the entry-0 GET — a line
    /// excluded there costs no second GET.
    pub async fn list_invocations_where(&self, limit: usize, f: &ListFilter) -> Result<Listing> {
        let mut scan = ListScan::default();
        while !scan.is_done() {
            self.list_invocations_step(limit, f, &mut scan, LIST_TAG_CHUNK)
                .await?;
        }
        Ok(scan.finish(limit, f))
    }

    /// One chunk (≤ `chunk` status-index rows) of
    /// [`Self::list_invocations_where`], continuing from `scan`. The caller
    /// releases the engine between chunks (the actor): a status-only filter
    /// has no scan cap, and the whole index of a large range in one engine hold
    /// is seconds that every turn of the range must wait.
    pub async fn list_invocations_step(
        &self,
        limit: usize,
        f: &ListFilter,
        scan: &mut ListScan,
        chunk: usize,
    ) -> Result<()> {
        if scan.done {
            return Ok(());
        }
        let capped = f.capped();
        let started = self.clock.mono();
        let tags = self
            .writer
            .invocation_tags_after(
                scan.after.as_ref(),
                chunk.max(1),
                cairn_store::ScanHow::Small,
            )
            .await?;
        if tags.len() < chunk.max(1) {
            scan.done = true;
        }
        // Filter by status tag and id prefix on the index first, then read the
        // full status row — non-matching rows cost no GET.
        for (id, tag) in tags {
            scan.after = Some(id);
            let out = &mut scan.out;
            if !f.created_desc && out.rows.len() >= limit {
                scan.done = true;
                break;
            }
            if capped
                && (scan.scanned >= f.scan_cap()
                    || scan.held + self.clock.mono().duration_since(started) >= LIST_SCAN_BUDGET)
            {
                out.capped = true;
                scan.done = true;
                break;
            }
            if f.status
                .as_deref()
                .is_some_and(|want| Some(want) != InvocationStatus::tag_name(tag))
            {
                continue;
            }
            if f.id_prefix.as_ref().is_some_and(|p| !id.starts_with(p)) {
                continue;
            }
            let Some((decoded, modified_ms)) = self.status_meta(&id).await? else {
                continue;
            };
            scan.scanned += 1;
            let out = &mut scan.out;
            if f.by_ended() {
                if !decoded.is_terminal() {
                    continue;
                }
                let Some(ended) = modified_ms else {
                    out.untimed += 1;
                    continue;
                };
                if !within(ended, f.ended_after, f.ended_before) {
                    continue;
                }
            }
            let (target, created_ms) = self.target_created(&id).await?;
            let off = |want: &Option<String>, got: Option<&String>| {
                want.as_ref().is_some_and(|w| got != Some(w))
            };
            if off(&f.service, target.as_ref().map(|t| &t.service))
                || off(&f.handler, target.as_ref().map(|t| &t.handler))
                || f.tenant
                    .as_ref()
                    .is_some_and(|w| target.as_ref().map(|t| &t.tenant) != Some(w))
            {
                continue;
            }
            if f.by_created() {
                let Some(created) = created_ms else {
                    out.untimed += 1;
                    continue;
                };
                if !within(created, f.created_after, f.created_before) {
                    continue;
                }
            }
            let times = Times {
                created_ms,
                modified_ms,
            };
            out.rows.push((id, decoded, target, times));
        }
        scan.held += self.clock.mono().duration_since(started);
        Ok(())
    }

    /// The status with `modifiedAt` (the status row's `create_ts`) — one GET,
    /// like [`Self::status`].
    pub async fn status_meta(
        &self,
        id: &InvocationId,
    ) -> Result<Option<(InvocationStatus, Option<u64>)>> {
        let raw = self
            .writer
            .get_meta(&cairn_store::keys::invocation_status(id))
            .await?;
        Ok(raw.and_then(|(v, ts)| Some((InvocationStatus::decode(v)?, Times::stamp(Some(ts))))))
    }

    /// The target with `createdAt` — one GET of the **metadata row** (hot
    /// zone), without reading entry 0 in the bulk zone. `createdAt` is the
    /// metadata's `created_ms` (the engine clock at creation), unchanged for
    /// the invocation's whole life. An absent row (impossible for a format-10
    /// invocation) falls back to entry 0 and its `create_ts`. `(None, None)` =
    /// nothing.
    pub async fn target_created(&self, id: &InvocationId) -> Result<(Option<Target>, Option<u64>)> {
        if let Some(m) = self.meta(id).await? {
            return Ok((super::meta::target_of_meta(&m), Some(m.created_ms)));
        }
        Ok(
            match self
                .writer
                .get_meta(&cairn_store::keys::journal_entry(id, 0))
                .await?
            {
                Some((bytes, ts)) => (
                    JournalEntry::decode(bytes)
                        .ok()
                        .as_ref()
                        .and_then(target_of),
                    Times::stamp(Some(ts)),
                ),
                None => (None, None),
            },
        )
    }

    pub async fn status(&self, id: &InvocationId) -> Result<Option<InvocationStatus>> {
        let raw = self
            .writer
            .get(&cairn_store::keys::invocation_status(id))
            .await?;
        Ok(raw.and_then(InvocationStatus::decode))
    }

    /// The tenant of invocation `id`: the metadata row, if absent the entry-0
    /// header. `None` = cannot be read (does not exist, or a broken row).
    /// The target of invocation `id` in this range: the metadata, if absent
    /// entry 0 (like [`Self::tenant_of`]). `None` = cannot be read.
    pub async fn target_of_id(&self, id: &InvocationId) -> Result<Option<Target>> {
        if let Some(t) = self.meta(id).await?.as_ref().and_then(target_of_meta) {
            return Ok(Some(t));
        }
        let Some(raw) = self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        else {
            return Ok(None);
        };
        Ok(JournalEntry::decode(raw).ok().and_then(|e| target_of(&e)))
    }

    pub async fn tenant_of(&self, id: &InvocationId) -> Result<Option<cairn_store::TenantId>> {
        if let Some(m) = self.meta(id).await? {
            return Ok(cairn_store::TenantId::new(&m.tenant).ok());
        }
        let Some(raw) = self
            .writer
            .get(&cairn_store::keys::journal_entry(id, 0))
            .await?
        else {
            return Ok(None);
        };
        Ok(JournalEntry::decode(raw)
            .ok()
            .and_then(|e| target_of(&e))
            .map(|t| t.tenant))
    }

    /// The status of `id` **as seen from tenant `tenant`**: a target in
    /// another tenant, or a tenant that cannot be read, is treated as
    /// nonexistent — the call site takes the "absent" branch, and the response
    /// is indistinguishable from a random id. The shared gate of
    /// `attach`/`getOutput`/`signal` by id from the SDK, same range or not.
    pub async fn visible_status(
        &self,
        tenant: &cairn_store::TenantId,
        id: &InvocationId,
    ) -> Result<Option<InvocationStatus>> {
        let Some(s) = self.status(id).await? else {
            return Ok(None);
        };
        Ok(match self.tenant_of(id).await? {
            Some(t) if &t == tenant => Some(s),
            _ => None,
        })
    }

    /// Whether `id` exists and belongs to `tenant` — the signal gate.
    pub async fn visible_to(
        &self,
        tenant: &cairn_store::TenantId,
        id: &InvocationId,
    ) -> Result<bool> {
        Ok(self.tenant_of(id).await?.as_ref() == Some(tenant))
    }
}

/// Validity check after a shell commit (feature `lifecycle-check`, tests
/// only): the durable state of `id` must be in the valid set — like FL-18's
/// `rows_below_floors`, but after **every** commit rather than only at open.
/// Wrong ⇒ panic: a tripwire for a path that "must never happen".
#[cfg(feature = "lifecycle-check")]
impl<S: RangeStore> Engine<S> {
    pub(super) async fn assert_lifecycle(&self, id: &InvocationId) -> Result<()> {
        let view = cairn_lifecycle::View::new(
            0,
            cairn_lifecycle::Knobs::DEFAULT,
            cairn_lifecycle::Attendance::IDLE,
        )
        .with_status(lifecycle_status(self.status(id).await?.as_ref()))
        .with_slot(self.vqueue_of(id).await?.as_ref().map(lifecycle_slot))
        .with_retry(self.retry_row(id).await?);
        // Every ✘ class is a tripwire — including `LiveButUnqueued` (a
        // preflight `NotFound` now ends `failed` 404 in the same commit that
        // removes the queue row).
        match cairn_lifecycle::valid(&view) {
            Ok(()) => {}
            Err(e) => panic!(
                "lifecycle check: {} outside the valid set after the shell commit: {e:?} — {view:?}",
                hex(id)
            ),
        }
        Ok(())
    }
}

/// P1 condensed for the lifecycle core (`cairn_lifecycle::Status`) — **one**
/// mapping for the shell (`turn::life`) and the testkit ([`Engine::lifecycle_violations`]).
pub fn lifecycle_status(s: Option<&InvocationStatus>) -> cairn_lifecycle::Status {
    use cairn_lifecycle::{Pause, Status};
    match s {
        None => Status::Missing,
        Some(InvocationStatus::Running) => Status::Running,
        Some(InvocationStatus::Suspended) => Status::Suspended,
        Some(InvocationStatus::Completed(_)) => Status::Completed,
        Some(InvocationStatus::Failed { .. }) => Status::Failed,
        Some(InvocationStatus::Paused { code, .. }) => Status::Paused(match *code {
            MAX_ATTEMPTS_EXHAUSTED => Pause::MaxAttempts,
            QUARANTINED => Pause::Quarantine,
            INFRA_STUCK => Pause::InfraStuck,
            _ => Pause::Sdk,
        }),
    }
}

/// The stage of a queue entry, for the core.
pub fn lifecycle_stage(s: Stage) -> cairn_lifecycle::Stage {
    match s {
        Stage::Inbox => cairn_lifecycle::Stage::Inbox,
        Stage::Running => cairn_lifecycle::Stage::Running,
        Stage::Suspended => cairn_lifecycle::Stage::Suspended,
        Stage::Paused => cairn_lifecycle::Stage::Paused,
    }
}

/// P2 condensed for the core: the stage, and whether the queue holds a VO lock.
pub fn lifecycle_slot(s: &VQueueSlot) -> cairn_lifecycle::Slot {
    cairn_lifecycle::Slot {
        stage: lifecycle_stage(s.stage),
        exclusive: s.name.lock_name().is_some(),
    }
}
