// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`PartitionWriter`] reads by table: state, promises, journal, due times, vqueue,
//! outbox/inbound/signal by seq, rebuilding the derived indexes.

use super::*;

impl<S: RangeStore> PartitionWriter<S> {
    /// A read on the leader: `memtable → block cache → SST`.
    /// Fresh up to `T`, read-your-own-writes for free.
    pub async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        self.db.get(key).await
    }

    /// Like [`Self::get`], plus the row's `create_ts` ([`RangeStore::get_meta`]).
    pub async fn get_meta(&self, key: &[u8]) -> Result<Option<(bytes::Bytes, i64)>> {
        self.db.get_meta(key).await
    }

    /// Scans one prefix, counting keys by region.
    async fn scan_prefix(&self, prefix: &[u8], bulk: bool) -> Result<Scan<'_, S::Iter>> {
        let how = if bulk {
            ScanHow::Bulk {
                cache_blocks: false,
            }
        } else {
            ScanHow::Small
        };
        let it = self.db.scan_prefix(prefix, how).await?;
        Ok(Scan {
            it,
            counts: &self.scanned,
        })
    }

    /// At most `limit` rows under `prefix` — the "anything left" check of the hint-row
    /// cleanup loop.
    pub async fn rows_under(
        &self,
        prefix: &[u8],
        bulk: bool,
        limit: usize,
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let mut s = self.scan_prefix(prefix, bulk).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            match s.next().await? {
                Some(kv) => out.push((kv.key, kv.value)),
                None => break,
            }
        }
        Ok(out)
    }

    /// At most `limit` **keys** in `[lo, hi)`, in order — the deletion loop of one tenant's
    /// rows scans the whole range in batches.
    pub async fn keys_between(
        &self,
        lo: Vec<u8>,
        hi: Vec<u8>,
        bulk: bool,
        limit: usize,
    ) -> Result<Vec<bytes::Bytes>> {
        let mut s = self.scan_range(lo, hi, bulk).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            match s.next().await? {
                Some(kv) => out.push(kv.key),
                None => break,
            }
        }
        Ok(out)
    }

    /// Scans one range `[lo, hi)`, counting keys by region.
    pub(super) async fn scan_range(
        &self,
        lo: Vec<u8>,
        hi: Vec<u8>,
        bulk: bool,
    ) -> Result<Scan<'_, S::Iter>> {
        let how = if bulk {
            ScanHow::Bulk {
                cache_blocks: false,
            }
        } else {
            ScanHow::Small
        };
        self.scan_with(lo, hi, how).await
    }

    /// Scans a whole **derived** table on the way to becoming leader: like [`bulk_scan`],
    /// but **caching blocks** until the open is done ([`Self::end_opening`]).
    ///
    /// On becoming leader, a few whole-table scans (queues, statuses, the three ordered
    /// indexes) and then the small scans of the background tick and the flusher all go
    /// through a handful of derived-region blocks of each SST. Without a cache each of them
    /// GETs those same blocks again ("reading the same SST 6–8 times"). The derived region
    /// is small compared with the hot region, and a freshly opened Db's block cache is
    /// still cold, so keeping it during the open pushes no hot block out. Once the open is
    /// done, whole-table scans stop caching again, as before.
    async fn scan_open(&self, lo: Vec<u8>, hi: Vec<u8>) -> Result<Scan<'_, S::Iter>> {
        let how = ScanHow::Bulk {
            cache_blocks: self.opening.get(),
        };
        self.scan_with(lo, hi, how).await
    }

    async fn scan_with(&self, lo: Vec<u8>, hi: Vec<u8>, how: ScanHow) -> Result<Scan<'_, S::Iter>> {
        let it = self.db.scan(lo, hi, how).await?;
        Ok(Scan {
            it,
            counts: &self.scanned,
        })
    }

    /// The open is done (`recover` has scanned): from here on whole-table scans stop caching blocks.
    pub fn end_opening(&self) {
        self.opening.set(false);
    }

    pub async fn state(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
    ) -> Result<Option<bytes::Bytes>> {
        // Blocked on the read path too, not only the write path: a wrapped-around key
        // points into *another object's* slot, so reading with it steals their state.
        let key = keyspace::state(owner_pk, tenant, scope, service, object_key, user_key);
        StoreError::key("state", &key)?;
        self.get(&key).await
    }

    /// Every `(user_key, value)` state pair of one virtual object.
    ///
    /// Reads **everything** — for `GetStateKeys`, `ClearAllState`, state cleanup. The
    /// eager state path uses [`Self::state_entries_capped`].
    pub async fn state_entries_unbounded(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let prefix = keyspace::state_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("state", &prefix)?;
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            let Some(user_key) = keyspace::parse_state_user_key(&kv.key) else {
                continue;
            };
            let user_key = kv.key.slice_ref(user_key);
            out.push((user_key, kv.value));
        }
        Ok(out)
    }

    /// The first part of a virtual object's state, in key order, **stopping the scan at the
    /// cap** `(max_entries, max_bytes)`: `((user_key, value) pairs, partial)`. `partial` =
    /// the object has keys beyond the returned part — there is a row number
    /// `max_entries + 1`, or a row that pushes the byte total (key + value) past
    /// `max_bytes`. Reads at most `max_entries + 1` rows.
    ///
    /// The eager state path (`StartMessage.state_map`/`partial_state`): an object with
    /// 100 000 keys does not force every turn to read 100 000 rows to send 64.
    pub async fn state_entries_capped(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        (max_entries, max_bytes): (usize, usize),
    ) -> Result<(Vec<(bytes::Bytes, bytes::Bytes)>, bool)> {
        let prefix = keyspace::state_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("state", &prefix)?;
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        let mut total = 0usize;
        while let Some(kv) = s.next().await? {
            let Some(user_key) = keyspace::parse_state_user_key(&kv.key) else {
                continue;
            };
            total = total.saturating_add(user_key.len() + kv.value.len());
            if out.len() >= max_entries || total > max_bytes {
                return Ok((out, true));
            }
            let user_key = kv.key.slice_ref(user_key);
            out.push((user_key, kv.value));
        }
        Ok((out, false))
    }

    /// Does the object have at least one segmented state key — reads at most one
    /// [`keyspace::Table::SegmentedState`] row.
    pub async fn has_segmented_state(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
    ) -> Result<bool> {
        let prefix = keyspace::segmented_state_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("segmented_state", &prefix)?;
        let mut s = self.scan_prefix(&prefix, true).await?;
        while let Some(kv) = s.next().await? {
            if keyspace::parse_segmented_state_user_key(&kv.key).is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The raw [`keyspace::Table::SegmentedState`] row of one state key; the caller
    /// decodes it with [`crate::value::SegmentedRow::decode`] (strict read).
    pub async fn segmented_state(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        user_key: &[u8],
    ) -> Result<Option<bytes::Bytes>> {
        let key = keyspace::segmented_state(owner_pk, tenant, scope, service, object_key, user_key);
        StoreError::key("segmented_state", &key)?;
        self.get(&key).await
    }

    /// Every `(user_key, raw row)` pair of [`keyspace::Table::SegmentedState`] of one
    /// object, in key order.
    pub async fn segmented_state_entries_unbounded(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let prefix = keyspace::segmented_state_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("segmented_state", &prefix)?;
        let mut s = self.scan_prefix(&prefix, true).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            let Some(user_key) = keyspace::parse_segmented_state_user_key(&kv.key) else {
                continue;
            };
            let user_key = kv.key.slice_ref(user_key);
            out.push((user_key, kv.value));
        }
        Ok(out)
    }

    /// Every [`keyspace::Table::ValueRefs`] row under `prefix` (of one owner, or of one
    /// holder): raw `(key, value)` in key order.
    pub async fn value_refs_under_unbounded(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        StoreError::key("value_refs", prefix)?;
        let mut s = self.scan_prefix(prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            out.push((kv.key, kv.value));
        }
        Ok(out)
    }

    /// Number of state keys of one object, counted up to `cap` (`max-state-keys=`): stops
    /// at `cap`, so the scan is bounded by the tenant's cap rather than by the object's
    /// size.
    pub async fn state_key_count(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        cap: usize,
    ) -> Result<usize> {
        let prefix = keyspace::state_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("state", &prefix)?;
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut n = 0;
        while n < cap {
            match s.next().await? {
                Some(kv) if keyspace::parse_state_user_key(&kv.key).is_some() => n += 1,
                Some(_) => {}
                None => break,
            }
        }
        Ok(n)
    }

    /// At most `limit` audit rows waiting for export: `(key, record)`. Goes through the
    /// index; orphan index rows (the entity row is gone) are skipped — the next deletion
    /// cleans them up too.
    pub async fn audit_rows(&self, limit: usize) -> Result<Vec<(Vec<u8>, bytes::Bytes)>> {
        let mut s = self
            .scan_prefix(&keyspace::index_prefix(keyspace::Index::Audit), false)
            .await?;
        let mut keys = Vec::new();
        while keys.len() < limit {
            match s.next().await? {
                Some(kv) => {
                    if let Some(row) = keyspace::parse_ix_audit(&kv.key) {
                        keys.push(row.to_vec());
                    }
                }
                None => break,
            }
        }
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(v) = self.get(&k).await? {
                out.push((k, v));
            }
        }
        Ok(out)
    }

    /// The result of a durable promise, if present.
    pub async fn promise(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
    ) -> Result<Option<bytes::Bytes>> {
        let key = keyspace::promise(owner_pk, tenant, scope, service, object_key, name);
        StoreError::key("promise", &key)?;
        self.get(&key).await
    }

    /// Everyone waiting on a promise: `(waiter, completion_id)`.
    pub async fn promise_waiters_unbounded(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
    ) -> Result<Vec<(InvocationId, u32)>> {
        let prefix =
            keyspace::promise_waiters_prefix(owner_pk, tenant, scope, service, object_key, name);
        StoreError::key("promise_waiter", &prefix)?;
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some((_, waiter, cid)) = keyspace::parse_promise_waiter(&kv.key) {
                out.push((waiter, cid));
            }
        }
        Ok(out)
    }

    /// The names of **every** promise of an object that has a result.
    ///
    /// The cleanup path: purging a workflow must clean up its promises too, and promise
    /// names are chosen by user code, so there is no way to know the list in advance.
    pub async fn promise_names_unbounded(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
    ) -> Result<Vec<String>> {
        let prefix = keyspace::promises_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("promise", &prefix)?;
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some(Ok(name)) = keyspace::parse_promise_name(&kv.key).map(std::str::from_utf8) {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    /// Everyone waiting on **any** promise of an object:
    /// `(promise name, waiter, completion_id)`.
    pub async fn promise_waiters_of_object_unbounded(
        &self,
        owner_pk: PartitionKey,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
    ) -> Result<Vec<(String, InvocationId, u32)>> {
        let prefix =
            keyspace::promise_waiters_object_prefix(owner_pk, tenant, scope, service, object_key);
        StoreError::key("promise_waiter", &prefix)?;
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some((name, waiter, cid)) = keyspace::parse_promise_waiter(&kv.key) {
                if let Ok(name) = std::str::from_utf8(name) {
                    out.push((name.to_string(), waiter, cid));
                }
            }
        }
        Ok(out)
    }

    /// The journal of one invocation, in command index order.
    ///
    /// Read on the leader rather than through [`crate::PartitionReader`]: reloading an
    /// invocation must see **everything just committed**, while a reader is only as fresh
    /// as `manifest_poll_interval`. Loading from a reader would load a journal that may be
    /// missing its tail — that is, replay from a state older than reality.
    pub async fn journal_unbounded(&self, id: &InvocationId) -> Result<Vec<(u32, bytes::Bytes)>> {
        let prefix = keyspace::journal_prefix(id);
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some(i) = keyspace::parse_journal_index(&kv.key) {
                out.push((i, kv.value));
            }
        }
        Ok(out)
    }

    /// One journal page: entries from index `from`, at most `max_entries` entries and —
    /// after the first entry — until the byte total reaches `max_bytes`. Also returns the
    /// next entry index when there is more: reading pages never holds the whole journal in
    /// RAM.
    pub async fn journal_page(
        &self,
        id: &InvocationId,
        from: u32,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(Vec<(u32, bytes::Bytes)>, Option<u32>)> {
        let prefix = keyspace::journal_prefix(id);
        let hi = crate::query_index::prefix_end(&prefix);
        let mut s = self
            .scan_range(keyspace::journal_entry(id, from), hi, false)
            .await?;
        let mut page = keyspace::JournalCut::new(max_entries, max_bytes);
        while let Some(kv) = s.next().await? {
            if let Some(i) = keyspace::parse_journal_index(&kv.key) {
                if page.push(i, kv.value) {
                    break;
                }
            }
        }
        Ok(page.finish())
    }

    /// Every artifact row of `id`, by sha. A corrupt row ⇒ `(sha, None)`: the caller treats
    /// it as still present (keeps the object).
    pub async fn artifacts_of_unbounded(
        &self,
        id: &InvocationId,
    ) -> Result<Vec<([u8; 32], Option<keyspace::ArtifactRow>)>> {
        let prefix = keyspace::artifacts_prefix(id);
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some((_, sha)) = keyspace::parse_artifact(&kv.key) {
                out.push((sha, keyspace::ArtifactRow::decode(&kv.value)));
            }
        }
        Ok(out)
    }

    /// The artifact epoch of `id`: that of the first readable row. `None` = no artifact
    /// yet (the epoch is issued lazily with the first artifact).
    ///
    /// Stops the scan at the first readable row: it does not gather every artifact of the
    /// invocation to take one row.
    pub async fn artifact_epoch(&self, id: &InvocationId) -> Result<Option<u64>> {
        let prefix = keyspace::artifacts_prefix(id);
        let mut s = self.scan_prefix(&prefix, false).await?;
        while let Some(kv) = s.next().await? {
            if keyspace::parse_artifact(&kv.key).is_some() {
                if let Some(r) = keyspace::ArtifactRow::decode(&kv.value) {
                    return Ok(Some(r.epoch));
                }
            }
        }
        Ok(None)
    }

    /// `(name, first offset)` of every durable stream chunk of `id` — for the cleanup
    /// commit. Scans the bulk region, without caching blocks (like the journal of a
    /// finished invocation: nobody reads it again).
    pub async fn stream_chunk_keys_unbounded(
        &self,
        id: &InvocationId,
    ) -> Result<Vec<(String, u64)>> {
        let prefix = keyspace::streams_prefix(id);
        let mut s = self.scan_prefix(&prefix, true).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some((_, name, first)) = keyspace::parse_stream_chunk(&kv.key) {
                out.push((name, first));
            }
        }
        Ok(out)
    }

    /// The chunks of durable stream `name` that have records at offset `from` onwards, in
    /// offset order, until `max_bytes` bytes of values are gathered (the first chunk is
    /// always returned even if larger). A corrupt chunk ⇒ `(first, None)`: the reader sees
    /// a `gap` instead of the whole stream being broken.
    ///
    /// Read on the leader (AP-13), like [`Self::journal_unbounded`]: what was just
    /// committed must be visible. There is no reverse scan, so it starts from the first
    /// chunk; a durable stream is capped at 8 MiB per invocation, so that is the cap of
    /// the scan.
    pub async fn stream_chunks_unbounded(
        &self,
        id: &InvocationId,
        name: &str,
        from: u64,
        max_bytes: usize,
    ) -> Result<Vec<(u64, Option<keyspace::StreamChunk>)>> {
        let prefix = keyspace::stream_prefix(id, name);
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        // The previous chunk: only when the next chunk is seen do we know whether it contains `from`.
        let mut held: Option<(u64, bytes::Bytes)> = None;
        while let Some(kv) = s.next().await? {
            let Some((_, n, first)) = keyspace::parse_stream_chunk(&kv.key) else {
                continue;
            };
            if n != name {
                continue;
            }
            if first <= from {
                held = Some((first, kv.value));
                continue;
            }
            if let Some((f, raw)) = held.take() {
                bytes += raw.len();
                out.push((f, keyspace::StreamChunk::decode(f, &raw)));
            }
            if !out.is_empty() && bytes >= max_bytes {
                return Ok(out);
            }
            bytes += kv.value.len();
            out.push((first, keyspace::StreamChunk::decode(first, &kv.value)));
        }
        if let Some((f, raw)) = held {
            let chunk = keyspace::StreamChunk::decode(f, &raw);
            // The last chunk lies entirely before `from`: nothing from `from` on.
            if chunk.as_ref().is_none_or(|c| c.next_offset(f) > from) {
                out.push((f, chunk));
            }
        }
        Ok(out)
    }

    /// The **last** chunk of durable stream `name` — the next offset, the running counter
    /// and the last identifier, for a new leader term. `None` = the stream has no durable
    /// chunk yet.
    pub async fn stream_tail_unbounded(
        &self,
        id: &InvocationId,
        name: &str,
    ) -> Result<Option<(u64, Option<keyspace::StreamChunk>)>> {
        let prefix = keyspace::stream_prefix(id, name);
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut last = None;
        while let Some(kv) = s.next().await? {
            if let Some((_, n, first)) = keyspace::parse_stream_chunk(&kv.key) {
                if n == name {
                    last = Some((first, kv.value));
                }
            }
        }
        Ok(last.map(|(f, raw)| (f, keyspace::StreamChunk::decode(f, &raw))))
    }

    /// Timers that are due, in due order: `(due_ms, id, completion)` — reads at most
    /// `limit` index rows.
    ///
    /// Reads the index, then **checks the entity row** (the due time still matches what
    /// the index says). Stale index rows are returned in [`Hinted::stale`] for the call
    /// site to clean up.
    ///
    /// **Cap and cursor.** Hitting `limit` while due rows remain ⇒ [`Hinted::next_due`] is
    /// that row's due time (≤ `now_ms`: the background loop wakes again at once), and the
    /// key of the last row read becomes the cursor (`timer_resume`, the term's RAM): the
    /// next call reads on **after** it, not from the floor. Without a cursor, due rows that
    /// cannot be deleted yet (the invocation is mid-turn — the timer was handed to that
    /// turn, the row stays until the turn reads it) sit at the head of the index and take
    /// the whole cap of every tick: timers after them never get their turn. A scan from the
    /// cursor that reaches the end of the due rows drops the cursor — the next round starts
    /// at the floor again; a row written below the cursor meanwhile (already due) waits at
    /// most until that round.
    ///
    /// The floor (FL-18) only advances in a scan starting from the floor, up to the due
    /// time of the **first** row it meets — never past an unprocessed row. A scan from the
    /// cursor does not touch the floor.
    pub async fn due_timers(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, InvocationId, u32)>> {
        // Scan from the floor or from the cursor, stopping at the first row not yet due or
        // past the cap: its due time is [`Hinted::next_due`].
        let slot = floors::due_slot(Index::Timer);
        let floor = self.due_floors[slot].get();
        let bound = keyspace::ix_timer_due_upper_bound(now_ms);
        let resume = self.timer_resume.take();
        let mut s = match &resume {
            None => self.scan_due_from(Index::Timer, floor).await?,
            Some(after) => {
                let mut lo = after.clone();
                lo.push(0);
                let (_, hi) = keyspace::index_bounds(Index::Timer);
                self.scan_range(lo, hi, false).await?
            }
        };
        let mut out = Hinted::default();
        let mut first = None;
        let mut read = 0usize;
        let mut last: Option<Vec<u8>> = None;
        while let Some(kv) = s.next().await? {
            first.get_or_insert_with(|| first_due(&kv.key, floor));
            let due = kv.key.as_ref() < bound.as_slice();
            if !due || read >= limit {
                out.next_due = keyspace::parse_ix_timer(&kv.key).map(|(due, ..)| due);
                if due {
                    // The cap: next time, read on after the last row read.
                    self.timer_resume.replace(last.take());
                }
                break;
            }
            read += 1;
            last = Some(kv.key.to_vec());
            let row = keyspace::parse_ix_timer(&kv.key);
            let live = match row {
                Some((due, id, cid)) => self.due_of(&keyspace::timer(&id, cid)).await? == Some(due),
                None => false,
            };
            match row {
                Some(r) if live => out.rows.push(r),
                _ => out.stale.push(kv.key.to_vec()),
            }
        }
        drop(s);
        if resume.is_none() {
            self.settle_due_floor(slot, floor, first, u64::MAX);
        }
        Ok(out)
    }

    /// A scan of one due-time index from floor `floor` to the end of the index.
    async fn scan_due_from(&self, ix: Index, floor: u64) -> Result<Scan<'_, S::Iter>> {
        #[cfg(any(test, feature = "testkit"))]
        self.due_scans.borrow_mut().push((ix, floor));
        let (lo, hi) = keyspace::index_bounds(ix);
        let from = keyspace::index(ix, &floor.to_be_bytes()).max(lo);
        self.scan_range(from, hi, false).await
    }

    /// `due_ms` written at the start of a timer entity row's value.
    async fn due_of(&self, key: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .get(key)
            .await?
            .and_then(|v| keyspace::decode_due(&v).map(|(due, _)| due)))
    }

    /// Every entry of one queue, by `(stage, seq)`.
    pub async fn vqueue_entries_unbounded(
        &self,
        queue: &[u8],
    ) -> Result<Vec<(Stage, u64, InvocationId)>> {
        let prefix = keyspace::ix_vqueue_prefix(queue);
        StoreError::key("vqueue", &prefix)?;
        self.scan_vqueue(prefix).await
    }

    /// Entries of one queue in **one** stage, by `seq` — one prefix seek.
    pub async fn vqueue_stage_unbounded(
        &self,
        queue: &[u8],
        stage: Stage,
    ) -> Result<Vec<(Stage, u64, InvocationId)>> {
        let prefix = keyspace::ix_vqueue_stage_prefix(queue, stage);
        StoreError::key("vqueue", &prefix)?;
        self.scan_vqueue(prefix).await
    }

    async fn scan_vqueue(&self, prefix: Vec<u8>) -> Result<Vec<(Stage, u64, InvocationId)>> {
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some((_, stage, seq, id)) = keyspace::parse_ix_vqueue(&kv.key) {
                out.push((stage, seq, id));
            }
        }
        Ok(out)
    }

    /// The **whole** queue index, grouped by queue.
    ///
    /// Only runs once on becoming leader, and only reads the derived region.
    pub async fn all_vqueues_unbounded(
        &self,
    ) -> Result<Vec<(QueueName, Vec<u8>, Stage, u64, InvocationId)>> {
        let (lo, hi) = keyspace::index_bounds(Index::VQueue);
        let mut s = self.scan_open(lo, hi).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            let Some((queue, stage, seq, id)) = keyspace::parse_ix_vqueue(&kv.key) else {
                continue;
            };
            let Some(name) = QueueName::decode(&queue) else {
                // A vqueue key whose queue name cannot be read back is corrupt data. Skip
                // it rather than stop the whole recovery because of one row.
                tracing::error!("vqueue key with an undecodable queue name — skipping");
                continue;
            };
            out.push((name, queue, stage, seq, id));
        }
        Ok(out)
    }

    /// The vqueue entry of one invocation, looked up through the entity row.
    pub async fn vqueue_of(
        &self,
        id: &InvocationId,
    ) -> Result<Option<(QueueName, Vec<u8>, Stage, u64)>> {
        let Some(raw) = self.get(&keyspace::vqueue_index(id)).await? else {
            return Ok(None);
        };
        let Some((queue, stage, seq)) = keyspace::decode_vqueue_index(&raw) else {
            return Ok(None);
        };
        Ok(QueueName::decode(&queue).map(|name| (name, queue, stage, seq)))
    }

    /// Every invocation with a status, plus its **status code** (the first byte of the
    /// status value) — reads the [`Index::Invocations`] index, without scanning the entity
    /// region.
    ///
    /// Includes finished invocations (until they are cleaned up after retention); the call
    /// site filters by code.
    pub async fn invocation_tags_unbounded(&self) -> Result<Vec<(InvocationId, u8)>> {
        let (lo, hi) = keyspace::index_bounds(Index::Invocations);
        let mut s = self.scan_open(lo, hi).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let (Some(id), Some(tag)) = (
                keyspace::parse_ix_invocation(&kv.key),
                kv.value.first().copied(),
            ) {
                out.push((id, tag));
            }
        }
        Ok(out)
    }

    /// One chunk of the `Invocations` index (id → status code) **after** `after`
    /// (exclusive), at most `limit` rows, in id order — the invocation list scans in chunks
    /// and releases the engine between two chunks, and so does the recovery scan. `how`
    /// caches blocks: the next chunk reads the last block of the previous one again — the
    /// list scans small ([`ScanHow::Small`]), recovery reads ahead in large blocks
    /// ([`ScanHow::Bulk`] `{ cache_blocks: true }`).
    pub async fn invocation_tags_after(
        &self,
        after: Option<&InvocationId>,
        limit: usize,
        how: ScanHow,
    ) -> Result<Vec<(InvocationId, u8)>> {
        let (lo0, hi) = keyspace::index_bounds(Index::Invocations);
        let lo = match after {
            Some(id) => {
                let mut k = keyspace::ix_invocation(id);
                k.push(0);
                k
            }
            None => lo0,
        };
        let mut s = self.scan_with(lo, hi, how).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = s.next().await? else { break };
            if let (Some(id), Some(tag)) = (
                keyspace::parse_ix_invocation(&kv.key),
                kv.value.first().copied(),
            ) {
                out.push((id, tag));
            }
        }
        Ok(out)
    }

    /// The metadata row of `id`. `Ok(None)` both when absent and when corrupt — the caller
    /// treats it as missing (the status row is still the truth).
    pub async fn invocation_meta(
        &self,
        id: &InvocationId,
    ) -> Result<Option<crate::query_index::InvocationMeta>> {
        Ok(self
            .get(&crate::query_index::meta_key(id))
            .await?
            .and_then(|raw| crate::query_index::InvocationMeta::decode(&raw)))
    }

    /// Every row in `[lo, hi)` of the derived region the query indexes use, in key order —
    /// `verify`, the background build and tests read through here. Capped at `limit`.
    pub async fn query_rows(
        &self,
        lo: Vec<u8>,
        hi: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let mut s = self.scan_range(lo, hi, false).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = s.next().await? else { break };
            out.push((kv.key, kv.value));
        }
        Ok(out)
    }

    /// Metadata rows from `after` (exclusive) on, at most `limit` — the background build
    /// of the query indexes proceeds in chunks.
    pub async fn meta_rows_after(
        &self,
        after: Option<&InvocationId>,
        limit: usize,
    ) -> Result<Vec<(InvocationId, bytes::Bytes)>> {
        let lo = match after {
            Some(id) => {
                let mut k = crate::query_index::meta_key(id);
                k.push(0);
                k
            }
            None => keyspace::entity(PartitionKey(0), Table::InvocationMeta, &[]),
        };
        let (_, hi) = keyspace::Region::Hot.bounds();
        let mut s = self.scan_range(lo, hi, false).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = s.next().await? else { break };
            if let Some(id) = crate::query_index::parse_meta_key(&kv.key) {
                out.push((id, kv.value));
            }
        }
        Ok(out)
    }

    /// Unfinished invocations pinned to `deployment`, through [`Index::Deployment`].
    /// `limit` caps the number of ids returned; the count is complete.
    pub async fn pinned_to_unbounded(
        &self,
        deployment: &str,
        limit: usize,
    ) -> Result<(usize, Vec<InvocationId>)> {
        let mut s = self
            .scan_prefix(&keyspace::ix_deployment_prefix(deployment), true)
            .await?;
        let (mut n, mut ids) = (0, Vec::new());
        while let Some(kv) = s.next().await? {
            if let Some((d, id)) = keyspace::parse_ix_deployment(&kv.key) {
                if d == deployment {
                    n += 1;
                    if ids.len() < limit {
                        ids.push(id);
                    }
                }
            }
        }
        Ok((n, ids))
    }

    /// Unfinished invocations across **every** deployment: `deployment → count`.
    pub async fn pinned_counts_unbounded(
        &self,
    ) -> Result<std::collections::BTreeMap<String, usize>> {
        let mut s = self
            .scan_prefix(&keyspace::index_prefix(Index::Deployment), true)
            .await?;
        let mut out = std::collections::BTreeMap::new();
        while let Some(kv) = s.next().await? {
            if let Some((d, _)) = keyspace::parse_ix_deployment(&kv.key) {
                *out.entry(d).or_insert(0) += 1;
            }
        }
        Ok(out)
    }

    /// The invocation holding an idempotency key, if any.
    pub async fn idempotency_target(
        &self,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        handler: &str,
        object_key: &str,
        idem_key: &str,
    ) -> Result<Option<InvocationId>> {
        let key = keyspace::idempotency(tenant, scope, service, handler, object_key, idem_key);
        StoreError::key("idempotency", &key)?;
        let raw = self.get(&key).await?;
        Ok(raw.and_then(|b| b.as_ref().try_into().ok()))
    }

    /// Everyone waiting for the result of `target`.
    pub async fn attachments_unbounded(
        &self,
        target: &InvocationId,
    ) -> Result<Vec<(InvocationId, u32)>> {
        let prefix = keyspace::attachments_prefix(target);
        let mut s = self.scan_prefix(&prefix, false).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some(a) = keyspace::parse_attachment(&kv.key) {
                out.push(a);
            }
        }
        Ok(out)
    }

    /// Retry schedules that are due, in due order: `(due, id, payload)`.
    pub async fn due_retries(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, InvocationId, bytes::Bytes)>> {
        let bound = keyspace::ix_retry_due_upper_bound(now_ms);
        // Start from the floor (see `due_floors`): the stretch below the floor is only tombstones.
        let slot = floors::due_slot(Index::Retry);
        let floor = self.due_floors[slot].get();
        let mut s = self.scan_due_from(Index::Retry, floor).await?;
        let mut out = Hinted::default();
        // The due time of the first index row still on the store (live or stale) — the new floor.
        let mut first: Option<u64> = None;
        while let Some(kv) = s.next().await? {
            first.get_or_insert_with(|| first_due(&kv.key, floor));
            // Like `due_delayed`: a row past `limit` has a due time ≤ `now_ms`, so the
            // background loop wakes again at once and looks at `limit` more rows — a
            // backlog of a thousand retries does not hold the engine thread for a whole
            // tick. Stale rows count toward the cap: a cluster of stale index rows with the
            // same due time is not read and cleaned in full within one hold.
            if kv.key.as_ref() >= bound.as_slice() || out.rows.len() + out.stale.len() >= limit {
                out.next_due = keyspace::parse_ix_retry(&kv.key).map(|(due, _)| due);
                break;
            }
            let mut row = None;
            if let Some((due, id)) = keyspace::parse_ix_retry(&kv.key) {
                if let Some(v) = self.get(&keyspace::retry(&id)).await? {
                    if let Some((d, payload)) = keyspace::decode_due(&v) {
                        if d == due {
                            row = Some((due, id, v.slice_ref(payload)));
                        }
                    }
                }
            }
            match row {
                Some(r) => out.rows.push(r),
                None => out.stale.push(kv.key.to_vec()),
            }
        }
        drop(s);
        self.settle_due_floor(slot, floor, first, u64::MAX);
        Ok(out)
    }

    /// Retry rows with a due time in `(from_ms, until_ms]`, by `(due, id)`, starting
    /// **after** the cursor `after` if any — for the shell to pull schedules back when a
    /// tenant is unsuspended, batch by batch over several ticks. Reads at most `limit`
    /// index rows (stale rows count toward the cap and are skipped without cleanup — that
    /// is [`Self::due_retries`]'s job); returns the cursor of the last index row read when
    /// rows remain after it in the interval, `None` when the scan is done. Does not touch
    /// the floor.
    pub async fn retry_rows_between(
        &self,
        from_ms: u64,
        after: Option<(u64, InvocationId)>,
        until_ms: u64,
        limit: usize,
    ) -> Result<RetryBatch> {
        let mut lo = keyspace::ix_retry_due_upper_bound(from_ms);
        if let Some((due, id)) = after {
            // The key right after the cursor: append one 0 byte.
            let mut k = keyspace::ix_retry(due, &id);
            k.push(0);
            lo = lo.max(k);
        }
        let hi = keyspace::ix_retry_due_upper_bound(until_ms);
        let mut out = RetryBatch::default();
        if lo >= hi {
            return Ok(out);
        }
        let mut s = self.scan_range(lo, hi, false).await?;
        let mut read = 0;
        let mut last = None;
        while let Some(kv) = s.next().await? {
            if read >= limit {
                out.next = last;
                break;
            }
            read += 1;
            let Some((due, id)) = keyspace::parse_ix_retry(&kv.key) else {
                continue;
            };
            last = Some((due, id));
            if let Some(v) = self.get(&keyspace::retry(&id)).await? {
                if let Some((d, payload)) = keyspace::decode_due(&v) {
                    if d == due {
                        out.rows.push((due, id, v.slice_ref(payload)));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Every retry index row from the floor: `(due, invocation)`, by due time — both
    /// watchdog rows and stale rows (the call site checks the entity row).
    ///
    /// Lets recovery on becoming leader know which invocations **may** have a schedule
    /// without reading the retry entity row of each `running` invocation: correct thanks
    /// to FL-18 — no row is below the floor.
    pub async fn retry_index_unbounded(&self) -> Result<Vec<(u64, InvocationId)>> {
        let floor = self.due_floors[floors::due_slot(Index::Retry)].get();
        let mut s = self.scan_due_from(Index::Retry, floor).await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some(r) = keyspace::parse_ix_retry(&kv.key) {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// One-way calls whose time has come, in due order:
    /// `(due, caller, completion, outbox row)`.
    pub async fn due_delayed(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, InvocationId, u32, bytes::Bytes)>> {
        let bound = keyspace::ix_delayed_due_upper_bound(now_ms);
        let slot = floors::due_slot(Index::Delayed);
        let floor = self.due_floors[slot].get();
        let mut s = self.scan_due_from(Index::Delayed, floor).await?;
        let mut out = Hinted::default();
        let mut first = None;
        while let Some(kv) = s.next().await? {
            first.get_or_insert_with(|| first_due(&kv.key, floor));
            // A row not yet due, or a due row past `limit` — both are "the first row not
            // returned". The latter has a due time ≤ `now_ms`: the background loop wakes
            // again at once, and that round forwards `limit` more rows.
            if kv.key.as_ref() >= bound.as_slice() || out.rows.len() >= limit {
                out.next_due = keyspace::parse_ix_delayed(&kv.key).map(|(due, ..)| due);
                break;
            }
            let mut row = None;
            if let Some((due, id, cid)) = keyspace::parse_ix_delayed(&kv.key) {
                if let Some(v) = self.get(&keyspace::delayed(&id, cid)).await? {
                    if let Some((d, payload)) = keyspace::decode_due(&v) {
                        if d == due {
                            row = Some((due, id, cid, v.slice_ref(payload)));
                        }
                    }
                }
            }
            match row {
                Some(r) => out.rows.push(r),
                None => out.stale.push(kv.key.to_vec()),
            }
        }
        drop(s);
        self.settle_due_floor(slot, floor, first, u64::MAX);
        Ok(out)
    }

    /// Schedules due to fire, in due order: `(due, schedule id, schedule row)`. Stale
    /// index rows (entity row absent or a different due time) go into `stale`.
    ///
    /// No durable scan floor: a range has few schedules, and this index is also the
    /// schedule list (paused schedules at `u64::MAX`) — scan from the start and stop at
    /// the first row not yet due.
    pub async fn due_schedules(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, InvocationId, bytes::Bytes)>> {
        let bound = keyspace::ix_schedule_due_upper_bound(now_ms);
        let mut s = self
            .scan_prefix(&keyspace::index_prefix(Index::ScheduleDue), false)
            .await?;
        let mut out = Hinted::default();
        while let Some(kv) = s.next().await? {
            if kv.key.as_ref() >= bound.as_slice() || out.rows.len() >= limit {
                out.next_due = keyspace::parse_ix_schedule_due(&kv.key)
                    .map(|(due, _)| due)
                    .filter(|d| *d != u64::MAX);
                break;
            }
            let mut row = None;
            if let Some((due, id)) = keyspace::parse_ix_schedule_due(&kv.key) {
                if let Some(v) = self.get(&keyspace::schedule(&id)).await? {
                    if let Some((d, payload)) = keyspace::decode_due(&v) {
                        if d == due {
                            row = Some((due, id, v.slice_ref(payload)));
                        }
                    }
                }
            }
            match row {
                Some(r) => out.rows.push(r),
                None => out.stale.push(kv.key.to_vec()),
            }
        }
        Ok(out)
    }

    /// Schedule row `id`: `(next fire time, schedule row)`, or `None`.
    pub async fn schedule(&self, id: &InvocationId) -> Result<Option<(u64, bytes::Bytes)>> {
        Ok(self.get(&keyspace::schedule(id)).await?.and_then(|v| {
            let (due, payload) = keyspace::decode_due(&v)?;
            let payload = v.slice_ref(payload);
            Some((due, payload))
        }))
    }

    /// Every schedule of the range from the index, at most `limit`: `(due, schedule id)` —
    /// the list for `GET /schedules` (the call site reads the entity rows).
    pub async fn schedule_index(&self, limit: usize) -> Result<Vec<(u64, InvocationId)>> {
        let mut s = self
            .scan_prefix(&keyspace::index_prefix(Index::ScheduleDue), false)
            .await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = s.next().await? else {
                break;
            };
            if let Some(r) = keyspace::parse_ix_schedule_due(&kv.key) {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// One chunk of the schedule index **after** cursor `after` (exclusive), at most
    /// `limit` rows: `(due, schedule id)`. A bounded scan — the call site lists in chunks,
    /// releasing the engine between two chunks (AGENTS §6).
    pub async fn schedule_index_after(
        &self,
        after: Option<(u64, InvocationId)>,
        limit: usize,
    ) -> Result<Vec<(u64, InvocationId)>> {
        let (lo, hi) = keyspace::index_bounds(Index::ScheduleDue);
        let lo = match after {
            Some((due, id)) => {
                let mut k = keyspace::ix_schedule_due(due, &id);
                k.push(0);
                k
            }
            None => lo,
        };
        let mut s = self.scan_range(lo, hi, false).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(kv) = s.next().await? else {
                break;
            };
            if let Some(r) = keyspace::parse_ix_schedule_due(&kv.key) {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// Cleanup schedules that are due, in due order.
    ///
    /// There is a `limit` because each cleanup is a series of deletes (the whole journal);
    /// a background tick swallowing a thousand expired invocations at once would hog the
    /// engine thread — cleanup is low-priority work, it queues behind everything urgent.
    pub async fn due_purges(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, InvocationId)>> {
        let slot = floors::due_slot(Index::Purge);
        let floor = self.due_floors[slot].get();
        let mut out = Hinted::default();
        // The floor is already above `now_ms`: no row is due.
        if floor > now_ms {
            return Ok(out);
        }
        #[cfg(any(test, feature = "testkit"))]
        self.due_scans.borrow_mut().push((Index::Purge, floor));
        let (lo, _) = keyspace::index_bounds(Index::Purge);
        let mut s = self
            .scan_range(
                keyspace::index(Index::Purge, &floor.to_be_bytes()).max(lo),
                keyspace::ix_purge_due_upper_bound(now_ms),
                false,
            )
            .await?;
        let mut first = None;
        while out.rows.len() < limit {
            let Some(kv) = s.next().await? else {
                break;
            };
            first.get_or_insert_with(|| first_due(&kv.key, floor));
            let row = keyspace::parse_ix_purge(&kv.key);
            let live = match row {
                Some((due, id)) => self.due_of(&keyspace::purge(&id)).await? == Some(due),
                None => false,
            };
            match row {
                Some(r) if live => out.rows.push(r),
                _ => out.stale.push(kv.key.to_vec()),
            }
        }
        drop(s);
        // Met no row: no row has a due time ≤ `now_ms`.
        self.settle_due_floor(slot, floor, first, now_ms.saturating_add(1));
        Ok(out)
    }

    /// The rows of an ordered index **from `from_seq` on**, each checked against its entity
    /// row: `(seq, entity key, payload)`.
    ///
    /// An index row pointing at an entity row that no longer exists, or at an entity row
    /// carrying a different `seq` (rewritten with a new `seq`), is stale.
    ///
    /// **Does not rescan a stretch already walked.** Below the [`SeqFloor`] floor, live
    /// rows are read with point reads from `below`; the open scan starts at the floor. The
    /// result is exactly as if scanning from `from_seq` — the same rows, the same `seq`
    /// order — only without walking through the tombstones of flushed rows.
    async fn seq_rows_from(
        &self,
        ix: Index,
        from_seq: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, bytes::Bytes, bytes::Bytes)>> {
        let mut out = Hinted::default();
        let slot = SeqFloor::slot(ix).expect("seq_rows_from only reads ordered indexes");
        let floor = self.seq_floors.borrow()[slot].floor;

        // 1. Rows that may still be live below the floor, by `seq`.
        let mut at = std::ops::Bound::Included((from_seq, bytes::Bytes::new()));
        while from_seq < floor && out.rows.len() < limit {
            let next = self.seq_floors.borrow()[slot]
                .below
                .range((at, std::ops::Bound::Unbounded))
                .next()
                .cloned();
            let Some((seq, key)) = next else {
                break;
            };
            at = std::ops::Bound::Excluded((seq, key.clone()));
            let live = match self.get(&key).await? {
                // The index row was deleted: drop it from `below`, nothing left to clean.
                None => None,
                Some(_) => match self.seq_row(ix, &key).await? {
                    Some(r) => Some(r),
                    None => {
                        out.stale.push(key.to_vec());
                        None
                    }
                },
            };
            match live {
                Some(r) => out.rows.push(r),
                None => {
                    self.seq_floors.borrow_mut()[slot].below.remove(&(seq, key));
                }
            }
        }
        if out.rows.len() >= limit {
            return Ok(out);
        }

        // 2. The open scan, from the floor (or from `from_seq` if that is above the floor).
        let start = from_seq.max(floor);
        let (_, hi) = keyspace::index_bounds(ix);
        #[cfg(any(test, feature = "testkit"))]
        self.seq_scans.borrow_mut().push((ix, start));
        let mut s = self
            .scan_range(keyspace::ix_seq_from(ix, start), hi, false)
            .await?;
        let mut seen = Vec::new();
        let mut last_key_seq = None;
        while out.rows.len() < limit {
            let Some(kv) = s.next().await? else {
                break;
            };
            last_key_seq = keyspace::parse_ix_seq(&kv.key, ix)
                .map(|(seq, _)| seq)
                .or(last_key_seq);
            match self.seq_row(ix, &kv.key).await? {
                Some(r) => {
                    seen.push((r.0, kv.key));
                    out.rows.push(r);
                }
                None => out.stale.push(kv.key.to_vec()),
            }
        }
        // Stopping because enough rows were read: only the stretch up to the last row was walked.
        let exhausted = out.rows.len() < limit;

        // 3. The floor advances over the stretch just walked — only if that stretch adjoins
        // the old floor. At the end of the index, the walked stretch reaches the last key
        // read (stale rows included: the call site cleans them at once); stopping because
        // enough rows were read, it reaches the last row.
        let reached = if exhausted {
            last_key_seq
        } else {
            out.rows.last().map(|r| r.0)
        };
        let mut frozen = false;
        if let Some(reached) = reached.filter(|_| start == floor) {
            let mut floors = self.seq_floors.borrow_mut();
            let f = &mut floors[slot];
            if f.below.len() + seen.len() <= SeqFloor::BELOW_CAP {
                f.below.extend(seen);
                f.floor = f.floor.max(reached + 1);
            } else {
                frozen = true;
            }
        }
        if let Some(stats) = &self.db_stats {
            let floors = self.seq_floors.borrow();
            let f = &floors[slot];
            stats.note_seq_scan(slot, f.floor, f.below.len(), last_key_seq, frozen);
        }
        Ok(out)
    }

    /// One ordered index row, checked against its entity row: `Some` if the entity row
    /// still exists and carries exactly the index row's `seq`, `None` if the index row is
    /// stale.
    pub(super) async fn seq_row(
        &self,
        ix: Index,
        key: &bytes::Bytes,
    ) -> Result<Option<(u64, bytes::Bytes, bytes::Bytes)>> {
        let Some((seq, ekey)) = keyspace::parse_ix_seq(key, ix) else {
            return Ok(None);
        };
        let Some(v) = self.get(ekey).await? else {
            return Ok(None);
        };
        Ok(match keyspace::decode_seq(&v) {
            Some((eseq, payload)) if eseq == seq => {
                Some((seq, key.slice_ref(ekey), v.slice_ref(payload)))
            }
            _ => None,
        })
    }

    /// Work still pending in the outbox **from the cursor on**, in creation order:
    /// `(seq, owner, (invocation, completion), payload)`.
    ///
    /// This is the function a new leader calls after failover: every outgoing call not yet
    /// fully sent is here, not in the dead leader's RAM.
    ///
    /// **Reads the index, does not scan the main table.** The main table is ordered by
    /// owner, then id — hashed numbers — so its "first 32 rows" mean nothing for creation
    /// order. The index is ordered by `seq`, so scanning from `from_seq` *reads on*, and a
    /// steadily advancing cursor misses no row.
    pub async fn outbox_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, PartitionKey, (InvocationId, u32), bytes::Bytes)>> {
        let got = self.seq_rows_from(Index::Outbox, from_seq, limit).await?;
        let mut out = Hinted {
            rows: Vec::with_capacity(got.rows.len()),
            stale: got.stale,
            next_due: None,
        };
        for (seq, ekey, payload) in got.rows {
            match (keyspace::owner_of(&ekey), keyspace::parse_outbox(&ekey)) {
                (Some(owner), Some(k)) => out.rows.push((seq, owner, k, payload)),
                _ => out.stale.push(keyspace::ix_seq(Index::Outbox, seq, &ekey)),
            }
        }
        Ok(out)
    }

    /// Signals still waiting to be sent **from the cursor on**, in creation order:
    /// `(seq, (sender, entry index), payload)`.
    pub async fn signals_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Hinted<(u64, (InvocationId, u32), bytes::Bytes)>> {
        let got = self.seq_rows_from(Index::Signal, from_seq, limit).await?;
        let mut out = Hinted {
            rows: Vec::with_capacity(got.rows.len()),
            stale: got.stale,
            next_due: None,
        };
        for (seq, ekey, payload) in got.rows {
            match keyspace::parse_signal_outbox(&ekey) {
                Some(k) => out.rows.push((seq, k, payload)),
                None => out.stale.push(keyspace::ix_seq(Index::Signal, seq, &ekey)),
            }
        }
        Ok(out)
    }

    /// Cross-partition messages received but not yet processed, **from the cursor on**, in
    /// order of receipt: `(seq, owner, (kind, id, completion), payload)`.
    ///
    /// A new leader calls this after failover for the same reason as [`Self::outbox_from`]:
    /// an acked message is pending work, and it lives in the state, not in the dead
    /// leader's RAM.
    #[allow(clippy::type_complexity)]
    pub async fn inbound_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Result<
        Hinted<(
            u64,
            PartitionKey,
            (keyspace::InboundKind, InvocationId, u32),
            bytes::Bytes,
        )>,
    > {
        let got = self.seq_rows_from(Index::Inbound, from_seq, limit).await?;
        let mut out = Hinted {
            rows: Vec::with_capacity(got.rows.len()),
            stale: got.stale,
            next_due: None,
        };
        for (seq, ekey, payload) in got.rows {
            match (keyspace::owner_of(&ekey), keyspace::parse_inbound(&ekey)) {
                (Some(owner), Some(k)) => out.rows.push((seq, owner, k, payload)),
                _ => out.stale.push(keyspace::ix_seq(Index::Inbound, seq, &ekey)),
            }
        }
        Ok(out)
    }

    /// The whole outbox index, to seed the `seq` counter and the map
    /// `(owner, inv, completion) → seq`: `(seq, owner, (inv, completion))`.
    ///
    /// Scans the whole index because both things it seeds need *every* pending row. Runs
    /// exactly once on becoming leader, and only reads the derived region.
    pub async fn all_outbox_seq_unbounded(
        &self,
    ) -> Result<Vec<(u64, PartitionKey, (InvocationId, u32))>> {
        let mut s = self
            .scan_prefix(&keyspace::index_prefix(Index::Outbox), true)
            .await?;
        let mut out = Vec::new();
        while let Some(kv) = s.next().await? {
            if let Some((seq, ekey)) = keyspace::parse_ix_seq(&kv.key, Index::Outbox) {
                if let (Some(owner), Some(k)) =
                    (keyspace::owner_of(ekey), keyspace::parse_outbox(ekey))
                {
                    out.push((seq, owner, k));
                }
            }
        }
        Ok(out)
    }

    /// The largest `seq` present in an ordered index, `0` if empty.
    pub async fn max_seq_unbounded(&self, ix: Index) -> Result<u64> {
        let mut s = self.scan_prefix(&keyspace::index_prefix(ix), true).await?;
        let mut max = 0;
        while let Some(kv) = s.next().await? {
            if let Some((seq, _)) = keyspace::parse_ix_seq(&kv.key, ix) {
                max = max.max(seq);
            }
        }
        Ok(max)
    }

    /// The pending outbox rows and the largest `seq` of all three ordered indexes — seeds
    /// the engine's `seq` counter and D-O2 map on becoming leader.
    ///
    /// **Each index is scanned from its own floor:** no live row of an index has a `seq`
    /// below that index's durable floor (FL-18), so the stretch below the floor is only
    /// tombstones. The previous version scanned in one sweep from the outbox floor to the
    /// end of inbound (one read for three adjacent indexes) and walked through the
    /// tombstones of every inbound message ever flushed — O(history) within one engine
    /// hold, at the term's first `seq` issue (0.4 s at 3·10⁵ messages). Three seeks
    /// instead of one is far cheaper.
    ///
    /// `max` is not below any of the three floors — `seq`s issued afterwards never fall
    /// below a floor (no lowering, no walking through old tombstones).
    pub async fn seq_heads_unbounded(&self) -> Result<SeqHeads> {
        let floors = self.floors_now();
        // Not below the largest known `seq` (`floors::SEQ_HIGH`), even when the durable
        // floors were dropped for a mismatching stamp and every row has been flushed: a
        // counter going lower would put new rows below the old tombstone band, and every
        // flush turn would walk through it again.
        let mut out = SeqHeads {
            max: floors.seq_lower_bound(),
            ..Default::default()
        };
        for (slot, ix) in SeqFloor::INDEXES.iter().copied().enumerate() {
            let start = floors.seq[slot];
            #[cfg(any(test, feature = "testkit"))]
            self.seq_scans.borrow_mut().push((ix, start));
            let (_, hi) = keyspace::index_bounds(ix);
            let mut s = self.scan_open(keyspace::ix_seq_from(ix, start), hi).await?;
            while let Some(kv) = s.next().await? {
                let Some((seq, ekey)) = keyspace::parse_ix_seq(&kv.key, ix) else {
                    continue;
                };
                out.max = out.max.max(seq);
                self.note_seq_high(seq);
                if ix == Index::Outbox {
                    if let (Some(owner), Some(k)) =
                        (keyspace::owner_of(ekey), keyspace::parse_outbox(ekey))
                    {
                        out.outbox.push((seq, owner, k));
                    }
                }
            }
        }
        Ok(out)
    }

    /// The `DERIVED_READY` marker present, if any.
    pub async fn derived_marker(&self) -> Result<Option<bytes::Bytes>> {
        self.get(&keyspace::derived_ready()).await
    }

    /// Ensures the derived region **matches this Db** before serving: a marker equal to
    /// `marker` ⇒ done, missing or mismatching ⇒ rebuild. Returns `true` if it rebuilt.
    ///
    /// This is the **only** mechanism that brings the derived region back in line after a
    /// split, a merge, or a death midway — projections hide region `0x01`, unions filter
    /// it out, so a child Db always sees it empty and without a marker.
    pub async fn ensure_derived(&mut self, marker: &[u8]) -> Result<Option<RebuildStats>> {
        if self.derived_marker().await?.as_deref() == Some(marker) {
            // The derived region belongs to this Db: the previous writer's durable floors
            // are usable (if its stamp is right — `floors.rs`).
            self.load_floors().await?;
            return Ok(None);
        }
        self.rebuild_derived(marker).await.map(Some)
    }

    /// Rebuilds every index ([`Index::REBUILT`]) from the **hot region**, then writes the
    /// marker `marker`.
    ///
    /// The order is what keeps it safe against a death midway:
    ///
    /// 1. **delete the marker first** — dying from here to step 4 makes the next open see
    ///    the marker missing and rebuild from scratch;
    /// 2. delete every old index row (after a merge they may point into another Db);
    /// 3. scan the hot region once (large read-ahead), producing index rows; ordered
    ///    indexes get **reissued `seq`s** in `(old seq, entity key)` order — merging two
    ///    Dbs yields two overlapping `seq` sets, and the flusher needs unique `seq`s.
    ///    Issued from **above** every known `seq` (the index rows just deleted in step 2,
    ///    the `seq`s in entity rows, the writer's durable lower bound — `floors.rs`), not
    ///    from 1: live rows below step 2's tombstone band would make every flush turn walk
    ///    through that band until compaction removes it;
    /// 4. write the marker.
    ///
    /// Does not read the bulk region: the journal and state have no index.
    pub async fn rebuild_derived(&mut self, marker: &[u8]) -> Result<RebuildStats> {
        const CHUNK: usize = 1024;
        let mut stats = RebuildStats::default();
        let started = tokio::time::Instant::now();
        let scanned_before = self.scan_counts();
        // Indexes are rewritten and `seq`s reissued: floors back to 0 (the first commit
        // just below writes floors 0 stamped by this writer).
        self.reset_floors();

        let mut m = Mutations::new();
        m.delete("derived_ready", keyspace::derived_ready());
        self.commit(m).await?;

        // ── 2. Old index rows. The largest `seq` of the three ordered indexes is the
        //       lower bound for the reissue in step 3 (their tombstones lie below it).
        self.note_floors_high().await?;
        let mut old = Vec::new();
        for ix in Index::REBUILT {
            let mut s = self.scan_prefix(&keyspace::index_prefix(ix), true).await?;
            while let Some(kv) = s.next().await? {
                if matches!(ix, Index::Outbox | Index::Signal | Index::Inbound) {
                    if let Some((seq, _)) = keyspace::parse_ix_seq(&kv.key, ix) {
                        self.note_seq_high(seq);
                    }
                }
                old.push(kv.key.to_vec());
            }
        }
        stats.removed = old.len();
        for chunk in old.chunks(CHUNK) {
            let mut m = Mutations::new();
            for k in chunk {
                m.delete("rebuild", k.clone());
            }
            self.commit(m).await?;
        }

        // ── 3. Scan the hot region.
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut ordered: Vec<(u64, Index, bytes::Bytes, bytes::Bytes)> = Vec::new();
        {
            let (lo, hi) = keyspace::Region::Hot.bounds();
            let mut s = self.scan_range(lo, hi, true).await?;
            while let Some(kv) = s.next().await? {
                stats.scanned += 1;
                stats.bytes += (kv.key.len() + kv.value.len()) as u64;
                let k = &kv.key;
                let v = &kv.value;
                let due = || keyspace::decode_due(v).map(|(d, _)| d);
                match keyspace::table_of(k) {
                    Some(Table::InvocationStatus) => {
                        if let Some(id) = keyspace::parse_invocation_status(k) {
                            puts.push((
                                keyspace::ix_invocation(&id),
                                v.get(..1).unwrap_or(&[]).to_vec(),
                            ));
                        }
                    }
                    Some(Table::Pins) => {
                        if let (Some(id), Ok(d)) = (keyspace::parse_pin(k), std::str::from_utf8(v))
                        {
                            puts.push((keyspace::ix_deployment(d, &id), vec![]));
                        }
                    }
                    Some(Table::Timers) => {
                        if let (Some((id, cid)), Some(due)) = (keyspace::parse_timer(k), due()) {
                            puts.push((keyspace::ix_timer(due, &id, cid), vec![]));
                        }
                    }
                    Some(Table::Retries) => {
                        if let (Some(id), Some(due)) = (keyspace::parse_retry(k), due()) {
                            puts.push((keyspace::ix_retry(due, &id), vec![]));
                        }
                    }
                    Some(Table::Purges) => {
                        if let (Some(id), Some(due)) = (keyspace::parse_purge(k), due()) {
                            puts.push((keyspace::ix_purge(due, &id), vec![]));
                        }
                    }
                    Some(Table::Delayed) => {
                        if let (Some((id, cid)), Some(due)) = (keyspace::parse_delayed(k), due()) {
                            puts.push((keyspace::ix_delayed(due, &id, cid), vec![]));
                        }
                    }
                    Some(Table::Schedules) => {
                        if let (Some(id), Some(due)) = (keyspace::parse_schedule(k), due()) {
                            puts.push((keyspace::ix_schedule_due(due, &id), vec![]));
                        }
                    }
                    Some(Table::Audit) => {
                        if keyspace::parse_audit(k).is_some() {
                            puts.push((keyspace::ix_audit(k), vec![]));
                        }
                    }
                    Some(Table::VQueueIndex) => {
                        if let (Some(id), Some((queue, stage, seq))) = (
                            keyspace::parse_vqueue_index(k),
                            keyspace::decode_vqueue_index(v),
                        ) {
                            puts.push((keyspace::ix_vqueue(&queue, stage, seq, &id), vec![]));
                        }
                    }
                    Some(t @ (Table::Outbox | Table::SignalOutbox | Table::Inbound)) => {
                        let ix = match t {
                            Table::Outbox => Index::Outbox,
                            Table::SignalOutbox => Index::Signal,
                            _ => Index::Inbound,
                        };
                        if let Some((seq, _)) = keyspace::decode_seq(v) {
                            ordered.push((seq, ix, kv.key.clone(), kv.value.clone()));
                        }
                    }
                    _ => {}
                }
            }
        }
        ordered.sort_by(|a, b| (a.0, &a.2).cmp(&(b.0, &b.2)));
        let base = ordered
            .iter()
            .map(|o| o.0)
            .fold(self.seq_high.get(), u64::max);
        for (i, (old_seq, ix, ekey, value)) in ordered.into_iter().enumerate() {
            let seq = base + i as u64 + 1;
            self.note_seq_high(seq);
            if seq != old_seq {
                if let Some((_, payload)) = keyspace::decode_seq(&value) {
                    puts.push((ekey.to_vec(), keyspace::encode_seq(seq, payload)));
                }
            }
            puts.push((keyspace::ix_seq(ix, seq, &ekey), vec![]));
        }
        stats.written = puts.len();
        for chunk in puts.chunks(CHUNK) {
            let mut m = Mutations::new();
            for (k, v) in chunk {
                m.put("rebuild", k.clone(), v);
            }
            self.commit(m).await?;
        }

        // ── 4. The marker, last. Every queue entry was just rewritten through a commit
        //       (`queue_seen`): from here the queue `seq` is known for sure, and the
        //       marker's commit writes it durably (`floors::QUEUE_HIGH`).
        self.learn_queue_high(0);
        let mut m = Mutations::new();
        m.put("derived_ready", keyspace::derived_ready(), marker);
        self.commit(m).await?;
        stats.regions = self.scan_counts().since(&scanned_before);
        stats.elapsed = started.elapsed();
        Ok(stats)
    }
}

/// `ScanOptions` for every **small** scan: the journal of one invocation, one queue, and
/// the due-time scans of the background tick (`due_timers`, `due_retries`,
/// `due_delayed`, …).
///
/// Differs from SlateDB's default in exactly one place: **block caching**. `ScanOptions`
/// defaults to `cache_blocks: false` (while `ReadOptions` defaults to `true`), so every
/// scan GETs again every block not yet in the cache — every block of a range that just
/// changed owner, and every SST compaction just produced. The background tick scans a
/// few indexes every 200 ms on every range, idle ranges included: `s3_slowdown` measured
/// ~160 GET/s on one SST after the cluster had stopped taking load, enough to eat the
/// bucket's whole read budget under S3-style throttling — and a range that lost its
/// leader could no longer reopen. The blocks of these scans are small and hot (the head
/// of a due-time index, the journal of a running invocation): exactly what the cache
/// exists to keep.
pub(super) fn small_scan() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        cache_blocks: true,
        ..Default::default()
    }
}

/// `ScanOptions` for **whole-table** scans on the way to becoming leader.
///
/// SlateDB's default reads one block (~4 KiB) per GET with one task — right for small
/// prefix scans (the journal of one invocation), quite wrong for whole-table scans: a
/// measurement found 4 MiB read-ahead × 4 tasks ~11× faster on SeaweedFS, and these scans
/// lie on the failover path (`recover`, `restore_flow`). No block caching: scanned once
/// and done, filling the cache would only push hot blocks out.
///
/// Not for small scans: 4 MiB of read-ahead for a journal of a few hundred bytes reads a
/// whole SST for nothing.
///
/// `cache_blocks` is only on for derived-region scans during the open (see
/// `PartitionWriter::scan_open`).
pub(super) fn bulk_scan(cache_blocks: bool) -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        read_ahead_bytes: 4 << 20,
        max_fetch_tasks: 4,
        cache_blocks,
        ..Default::default()
    }
}

/// The due time of the first due-time index row a scan meets — that index's new floor. A
/// key whose due time cannot be read (impossible for keys written by `Mutations`) keeps
/// the old floor.
fn first_due(key: &[u8], floor: u64) -> u64 {
    floors::due_of_index_key(key).map_or(floor, |(_, due)| due)
}
