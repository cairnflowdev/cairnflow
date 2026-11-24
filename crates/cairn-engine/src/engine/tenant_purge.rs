// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Logical deletion of the rows of **one** tenant in a range Db (tenant
//! purge).
//!
//! The rows of different tenants interleave by the `owner_pk` hash, so this is
//! a scan over the whole range in batches with a cursor, one commit per batch.
//! The owner of each row is given by [`cairn_store::keys::row_owner`]; a row
//! keyed by id looks up the id's tenant **in this very Db** —
//! `InvocationMeta`, then entry 0 of the journal, then the schedule row. An id
//! of another range (the recipient of a `Reply`, the sender of an inbound
//! message) ⇒ the tenant of the `owner_pk` **group** (every row with the same
//! `owner_pk` belongs to one local entity, one tenant —
//! `partition_key_for_target` hashes the tenant too).
//!
//! **Two passes**, because some rows are where other rows look up their tenant
//! ("owner-resolving rows": `InvocationStatus`, `InvocationMeta`, `Schedules`,
//! entry 0 of the journal, and the four tables carrying the tenant in the key —
//! `State`, `Promises`, `PromiseWaiters`, `Idempotency`): pass 1 deletes every
//! other row (derived zone, hot zone, bulk zone), pass 2 deletes the
//! owner-resolving rows. A batch cut anywhere never leaves a row without its
//! lookup.
//!
//! A row whose owner cannot be resolved ⇒ **kept** and counted
//! (`unattributed`; delete only when provable). The range's own rows (marks,
//! watermarks) are never touched.
//!
//! RAM: deleted invocations are evicted from the warm machine, the query
//! counters get `−1` through `query_event` (the same path as `Engine::purge`).
//! In-RAM deletion queues (`pending_*`) point at rows already gone: deleting an
//! absent key again is harmless.

use {
    super::*,
    cairn_store::{
        keys::{self as keyspace, RowOwner, Table},
        proof::TenantPurgeGrant,
        PartitionKey,
    },
};

/// The cursor of a tenant row deletion: the segment being scanned and the last key seen.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantPurgeCursor {
    /// Index into [`SEGMENTS`].
    pub segment: u8,
    /// The last key seen in the segment; empty = start of the segment.
    pub after: Vec<u8>,
}

/// The result of one batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantRowsStep {
    /// Rows deleted in this batch (committed).
    pub deleted: u64,
    /// Keys seen.
    pub scanned: u64,
    /// Rows whose owner cannot be resolved — kept.
    pub unattributed: u64,
    /// A **check** batch ([`Engine::find_tenant_rows`]): the number of the
    /// tenant's rows seen (stops at the first row, deletes nothing). A delete
    /// batch: 0.
    pub found: u64,
    /// The cursor for the next batch; `None` = one full round scanned.
    pub next: Option<TenantPurgeCursor>,
}

/// The zone of one scan segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Area {
    Derived,
    Hot,
    Bulk,
}

/// Five segments in order: pass 1 (derived, hot, bulk — skipping
/// owner-resolving rows), pass 2 (hot, bulk — only owner-resolving rows).
const SEGMENTS: [(Area, bool); 5] = [
    (Area::Derived, false),
    (Area::Hot, false),
    (Area::Bulk, false),
    (Area::Hot, true),
    (Area::Bulk, true),
];

/// Rows through which other rows look up their tenant (pass 2).
///
/// An exhaustive `match`: a new table in [`Table`] does not compile until it
/// declares whether it is a lookup for `Engine::tenant_of_id` /
/// `Engine::tenant_of_pk`. Wrongly declaring `false` for a lookup means pass 1
/// deletes it before a row that still needs it ⇒ that row becomes
/// `unattributed` and is kept (a leak, not a loss).
fn is_resolver(key: &[u8]) -> bool {
    let Some(table) = keyspace::table_of(key) else {
        // Derived indexes and range keys: nobody looks up a tenant through them.
        return false;
    };
    match table {
        // Lookups of `tenant_of_id` (metadata, schedule rows) and of
        // `tenant_of_pk` (status, schedules, the four tables carrying the
        // tenant in the key).
        Table::InvocationStatus
        | Table::InvocationMeta
        | Table::Schedules
        | Table::State
        | Table::Promises
        | Table::PromiseWaiters
        | Table::Idempotency => true,
        // Entry 0 is a lookup of `tenant_of_id`; other entries are not.
        Table::Journal => keyspace::parse_journal_index(key) == Some(0),
        // Carry the tenant in the key but `tenant_of_pk` does not look up
        // through them: they resolve their own owner, nobody needs them alive.
        Table::SegmentedState | Table::ValueRefs => false,
        // Keyed by id, owner resolved through the lookups above.
        Table::Timers
        | Table::Outbox
        | Table::Retries
        | Table::Attachments
        | Table::Inbound
        | Table::Purges
        | Table::Delayed
        | Table::SignalOutbox
        | Table::VQueueIndex
        | Table::Pins
        | Table::Chain
        | Table::LastFailure
        | Table::Streams
        | Table::Artifacts
        | Table::Audit => false,
    }
}

/// The verdict for one row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Mine,
    Other,
    Unattributed,
}

/// The lookup memo within one batch.
#[derive(Default)]
struct Lookup {
    ids: HashMap<InvocationId, Option<TenantId>>,
    pks: HashMap<PartitionKey, Option<TenantId>>,
}

impl<S: RangeStore> Engine<S> {
    /// One batch deleting the rows of `grant.tenant()`: from `from` (`None` =
    /// start of a round), looks at most at `8 × budget` keys and deletes at
    /// most `budget` rows, in **one** commit. Call repeatedly until
    /// `next == None`; a round that deletes nothing means the range is clean.
    pub async fn purge_tenant_rows(
        &mut self,
        grant: &TenantPurgeGrant,
        from: Option<TenantPurgeCursor>,
        budget: usize,
    ) -> Result<TenantRowsStep> {
        self.tenant_rows(grant, from, budget, true).await
    }

    /// One **check** batch: the same scan and the same classifier as
    /// [`Self::purge_tenant_rows`], but no deletion, no commit, and it stops at
    /// the tenant's first row (`found = 1`, `next` = where it got to). The
    /// tombstone's final check runs it on every live range: a full round with
    /// `found == 0` means the range has no row of the tenant left.
    pub async fn find_tenant_rows(
        &mut self,
        grant: &TenantPurgeGrant,
        from: Option<TenantPurgeCursor>,
        budget: usize,
    ) -> Result<TenantRowsStep> {
        self.tenant_rows(grant, from, budget, false).await
    }

    async fn tenant_rows(
        &mut self,
        grant: &TenantPurgeGrant,
        from: Option<TenantPurgeCursor>,
        budget: usize,
        delete: bool,
    ) -> Result<TenantRowsStep> {
        let tenant = grant.tenant().clone();
        let budget = budget.max(1);
        let scan_cap = budget.saturating_mul(8);
        let mut cursor = from.unwrap_or_default();
        let mut step = TenantRowsStep::default();
        let mut look = Lookup::default();
        let mut muts = Mutations::new();
        let mut doomed: Vec<Vec<u8>> = Vec::new();
        let mut ended: Vec<InvocationId> = Vec::new();
        let owned =
            keyspace::owned_ranges(PartitionKey(self.range.start), PartitionKey(self.range.end));
        loop {
            let Some(&(area, resolvers)) = SEGMENTS.get(usize::from(cursor.segment)) else {
                step.next = None;
                break;
            };
            let (lo, hi) = match area {
                Area::Derived => keyspace::Region::Derived.bounds(),
                Area::Hot => owned
                    .iter()
                    .find(|(lo, _)| keyspace::Region::of_key(lo) == Some(keyspace::Region::Hot))
                    .cloned()
                    .unwrap_or_default(),
                Area::Bulk => owned
                    .iter()
                    .find(|(lo, _)| keyspace::Region::of_key(lo) == Some(keyspace::Region::Bulk))
                    .cloned()
                    .unwrap_or_default(),
            };
            let start = if cursor.after.is_empty() {
                lo
            } else {
                let mut k = cursor.after.clone();
                k.push(0);
                k
            };
            let room = scan_cap.saturating_sub(step.scanned as usize);
            let keys = if start < hi && room > 0 {
                self.writer
                    .keys_between(start, hi, area == Area::Bulk, room)
                    .await?
            } else {
                Vec::new()
            };
            let exhausted = keys.len() < room;
            for key in keys {
                step.scanned += 1;
                cursor.after = key.to_vec();
                if is_resolver(&key) != resolvers {
                    continue;
                }
                match self.verdict(&tenant, &key, &mut look).await? {
                    Verdict::Mine if !delete => {
                        step.found += 1;
                        step.next = Some(cursor);
                        return Ok(step);
                    }
                    Verdict::Mine => {
                        if let (true, Some(id)) = (
                            keyspace::table_of(&key) == Some(Table::InvocationStatus),
                            keyspace::parse_invocation_status(&key),
                        ) {
                            ended.push(id);
                        }
                        doomed.push(key.to_vec());
                    }
                    Verdict::Other => {}
                    Verdict::Unattributed => step.unattributed += 1,
                }
                if doomed.len() >= budget {
                    break;
                }
            }
            if doomed.len() >= budget || step.scanned as usize >= scan_cap {
                step.next = Some(cursor.clone());
                if exhausted && doomed.len() < budget {
                    // The segment ended exactly at the look cap: the next batch starts the next segment.
                    step.next = Some(TenantPurgeCursor {
                        segment: cursor.segment + 1,
                        after: Vec::new(),
                    });
                }
                break;
            }
            // Segment exhausted: move to the next segment in the same batch.
            cursor = TenantPurgeCursor {
                segment: cursor.segment + 1,
                after: Vec::new(),
            };
        }
        // RAM and query counters of deleted invocations — the same path as
        // `Engine::purge` (the metadata is still there: pass 2 deletes status
        // before meta within the same group, and meta in the same batch or a
        // later one).
        for id in &ended {
            self.warm.forget(id);
            self.spawned.remove(id);
            if let (Some(meta), Some((status, at))) =
                (self.meta(id).await?, self.status_at(id).await?)
            {
                let p = cairn_store::query_index::Placement::new(
                    status.class(),
                    at.unwrap_or(0),
                    &meta,
                );
                muts.query_event_with(self.query_indexes, id, &meta, Some(p), None);
            }
        }
        for key in doomed {
            // Tripwire: a row carrying another tenant in its key slipped into
            // the batch — must never happen.
            if let RowOwner::Tenant(t) = keyspace::row_owner(&key) {
                if t != tenant {
                    cairn_store::failpoint!("tenant_purge::foreign_row");
                    tracing::error!(tenant = %tenant, row_tenant = %t, "tenant purge: a row of another tenant slipped into the batch — kept");
                    continue;
                }
            }
            // Artifact byte counter: artifact rows go in pass 1, metadata (an
            // owner-resolving row) in pass 2 — still readable here.
            if let Some((id, sha)) = keyspace::parse_artifact(&key) {
                let row = self
                    .writer
                    .get(&key)
                    .await?
                    .and_then(|raw| keyspace::ArtifactRow::decode(&raw));
                if let (Some(row), Some(meta)) = (row, self.meta(&id).await?) {
                    muts.artifact_bytes(cairn_store::artifact_bytes::delta(
                        &id,
                        &sha,
                        &meta.tenant,
                        -super::artifact_bytes::signed(row.size),
                    ));
                }
            }
            muts.purge_tenant_row(key);
            step.deleted += 1;
        }
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
        }
        Ok(step)
    }

    async fn verdict(&self, tenant: &TenantId, key: &[u8], look: &mut Lookup) -> Result<Verdict> {
        let owner = match keyspace::row_owner(key) {
            RowOwner::Entity(entity) => keyspace::row_owner(entity),
            o => o,
        };
        let found = match owner {
            RowOwner::Tenant(t) => Some(t),
            RowOwner::Id { id, owner } => match self.tenant_of_id(&id, look).await? {
                Some(t) => Some(t),
                None => match owner {
                    Some(pk) => self.tenant_of_pk(pk, look).await?,
                    None => None,
                },
            },
            RowOwner::Range => return Ok(Verdict::Other),
            RowOwner::Entity(_) | RowOwner::Unknown => None,
        };
        Ok(match found {
            Some(t) if t == *tenant => Verdict::Mine,
            Some(_) => Verdict::Other,
            None => Verdict::Unattributed,
        })
    }

    /// The tenant of `id` in this Db: metadata, entry 0 of the journal, the
    /// schedule row. `None` = the id is not in this range.
    async fn tenant_of_id(&self, id: &InvocationId, look: &mut Lookup) -> Result<Option<TenantId>> {
        if let Some(t) = look.ids.get(id) {
            return Ok(t.clone());
        }
        let mut t = self
            .meta(id)
            .await?
            .and_then(|m| TenantId::new(&m.tenant).ok());
        if t.is_none() {
            if let Some(raw) = self.writer.get(&keyspace::journal_entry(id, 0)).await? {
                t = JournalEntry::decode(raw)
                    .ok()
                    .and_then(|e| target_of(&e))
                    .map(|target| target.tenant);
            }
        }
        if t.is_none() {
            if let Some(raw) = self.writer.get(&keyspace::schedule(id)).await? {
                t = keyspace::decode_due(&raw)
                    .and_then(|(_, row)| crate::schedule::ScheduleRow::decode(row).ok())
                    .and_then(|row| match row.tenant {
                        None => Some(TenantId::default()),
                        Some(t) => TenantId::new(&t).ok(),
                    });
            }
        }
        look.ids.insert(*id, t.clone());
        Ok(t)
    }

    /// The tenant of the `owner_pk` group: the first row that resolves among
    /// the group's owner-resolving tables. `None` = the group has no
    /// resolvable entity left.
    async fn tenant_of_pk(&self, pk: PartitionKey, look: &mut Lookup) -> Result<Option<TenantId>> {
        if let Some(t) = look.pks.get(&pk) {
            return Ok(t.clone());
        }
        let mut found = None;
        for table in [
            Table::State,
            Table::Promises,
            Table::PromiseWaiters,
            Table::Idempotency,
        ] {
            let prefix = keyspace::entity(pk, table, &[]);
            if let Some((k, _)) = self
                .writer
                .rows_under(&prefix, table == Table::State, 1)
                .await?
                .into_iter()
                .next()
            {
                if let RowOwner::Tenant(t) = keyspace::row_owner(&k) {
                    found = Some(t);
                    break;
                }
            }
        }
        if found.is_none() {
            for table in [Table::InvocationStatus, Table::Schedules] {
                let prefix = keyspace::entity(pk, table, &[]);
                for (k, _) in self.writer.rows_under(&prefix, false, 4).await? {
                    if let RowOwner::Id { id, .. } = keyspace::row_owner(&k) {
                        if let Some(t) = self.tenant_of_id(&id, look).await? {
                            found = Some(t);
                            break;
                        }
                    }
                }
                if found.is_some() {
                    break;
                }
            }
        }
        look.pks.insert(pk, found.clone());
        Ok(found)
    }
}
