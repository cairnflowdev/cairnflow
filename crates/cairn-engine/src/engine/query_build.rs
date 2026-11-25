// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-range query-index state and background build.
//!
//! The F1 online-schema-change path for indexes, the `WRITE_ONLY → READABLE`
//! states of the FDB Record Layer: a descriptor present in the table whose
//! `0x01 ‖ 0xFC` mark is missing or has a different version is `building` —
//! live writes already maintain it (`query_event` goes through **every**
//! descriptor of the table), the background build fills in the rest in
//! chunks, and when done it rewrites the mark. The rest are `readable`, and
//! only those are used by the path selector.
//!
//! F1 needs an extra `delete-only` state because two schema versions run on
//! many servers at once. Each cairn range has **one** writer, and a change of
//! the descriptor table comes with a new binary that takes the range through
//! the lease, so there are never two writers with two tables at once.
//!
//! **A chunk lies entirely within one engine hold** (`&mut self`): reading the
//! state and then writing a chunk's rows is atomic with respect to every live
//! transition, so the background build only needs puts — rows of ids already
//! past the chunk are kept by live writes, rows of ids not reached yet are
//! read by a later chunk in their latest state.
//!
//! **Changing the version** of a descriptor is changing the layout under the
//! same code: before building, every row of that code is deleted (including
//! new live-written rows — a later build chunk puts them back from the
//! metadata). A death midway leaves the old mark ⇒ the next run deletes from
//! the start again and rebuilds: costly, not wrong.

use {
    super::*,
    cairn_store::query_index::{self, Placement, QueryIndex},
};

/// Rows per background build / cleanup chunk.
pub const QUERY_BUILD_CHUNK: usize = 1024;

/// The state of one descriptor on this range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexState {
    /// The mark matches name + version: the path selector may use it.
    Readable,
    /// Maintained by live writes; the background build is not finished.
    Building,
}

/// One step of the background build loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildStep {
    /// Every descriptor `readable`, no rows left of removed codes: the loop rests.
    Idle,
    /// Did one chunk (`rows` rows touched); call again.
    Progress { rows: usize },
}

/// The cursor of the running build — lives in the term's RAM. Lost (leader
/// change, death) ⇒ the next run starts from the beginning: the background
/// build only puts, so it can be repeated.
#[derive(Clone, Debug, Default)]
pub struct BuildCursor {
    /// Codes fully deleted (a descriptor that changed version, or a code removed from the table).
    swept: Vec<u8>,
    /// The last metadata id built; `None` = not started.
    after: Option<InvocationId>,
    /// All metadata has been scanned.
    built: bool,
}

/// The cursor of the hint-row cleanup loop ([`Engine::hint_sweep_step`]).
#[derive(Clone, Debug, Default)]
pub struct HintCursor {
    /// The key of the last hint row checked.
    after: Option<Vec<u8>>,
}

impl<S: RangeStore> Engine<S> {
    /// The descriptor table this engine maintains (default
    /// [`query_index::QUERY_INDEXES`]).
    pub fn query_indexes(&self) -> &'static [QueryIndex] {
        self.query_indexes
    }

    /// Changes the descriptor table — tests only (adding an experimental descriptor).
    pub fn with_query_indexes(mut self, indexes: &'static [QueryIndex]) -> Self {
        self.query_indexes = indexes;
        self
    }

    /// This range's state mark for the current table.
    fn query_marker(&self, indexes: &[QueryIndex]) -> String {
        query_index::ready_marker(u64::from(self.range.id.0), self.range.gen, indexes)
    }

    /// The state of each descriptor of the table on this range, read from the mark.
    pub async fn query_index_states(&self) -> Result<Vec<(&'static str, IndexState)>> {
        let marker = self
            .writer
            .get(&query_index::ready_key())
            .await?
            .and_then(|raw| query_index::parse_ready_marker(&raw))
            // A mark of another range/generation (impossible: the derived zone
            // is empty after a split/merge) ⇒ treated as absent.
            .filter(|m| m.range == u64::from(self.range.id.0) && m.gen == self.range.gen);
        Ok(self
            .query_indexes
            .iter()
            .map(|ix| {
                let ok = marker
                    .as_ref()
                    .and_then(|m| m.version_of(ix.name))
                    .is_some_and(|v| v == ix.version);
                (
                    ix.name,
                    if ok {
                        IndexState::Readable
                    } else {
                        IndexState::Building
                    },
                )
            })
            .collect())
    }

    /// The range's query counters — readable without the engine
    /// (`sys_invocation_counts` does not read the store).
    pub fn query_counts(&self) -> &Arc<cairn_store::query_index::QueryCounts> {
        &self.query_counts
    }

    /// The `readable` descriptors on this range.
    pub async fn readable_query_indexes(&self) -> Result<Vec<&'static QueryIndex>> {
        let states = self.query_index_states().await?;
        Ok(self
            .query_indexes
            .iter()
            .filter(|ix| {
                states
                    .iter()
                    .any(|(n, s)| *n == ix.name && *s == IndexState::Readable)
            })
            .collect())
    }

    /// One background build / cleanup step. Call repeatedly until
    /// [`BuildStep::Idle`]; each step is one commit (one chunk ≤ `limit` rows),
    /// entirely within `&mut self`.
    pub async fn query_build_step(
        &mut self,
        cursor: &mut BuildCursor,
        limit: usize,
    ) -> Result<BuildStep> {
        let indexes = self.query_indexes;
        let marker = self
            .writer
            .get(&query_index::ready_key())
            .await?
            .and_then(|raw| query_index::parse_ready_marker(&raw))
            .filter(|m| m.range == u64::from(self.range.id.0) && m.gen == self.range.gen);

        // ── 1. Codes that must be wiped first: codes removed from the table,
        //       and descriptors whose mark records a **different** version (an
        //       old layout under the same code).
        let in_table = |code: u8| indexes.iter().any(|ix| ix.code == code);
        let mut to_sweep: Vec<u8> = query_index::QUERY_CODES.filter(|c| !in_table(*c)).collect();
        if let Some(m) = &marker {
            for ix in indexes {
                if m.version_of(ix.name).is_some_and(|v| v != ix.version) {
                    to_sweep.push(ix.code);
                }
            }
        }
        for code in to_sweep {
            if cursor.swept.contains(&code) {
                continue;
            }
            let lo = vec![cairn_store::keys::Region::Derived as u8, code];
            let hi = vec![cairn_store::keys::Region::Derived as u8, code + 1];
            let rows = self.writer.query_rows(lo, hi, limit).await?;
            if rows.is_empty() {
                cursor.swept.push(code);
                continue;
            }
            let n = rows.len();
            let mut muts = Mutations::new();
            for (k, _) in rows {
                muts.delete_query_row(k.to_vec());
            }
            self.writer.commit(muts).await?;
            return Ok(BuildStep::Progress { rows: n });
        }

        // ── 2. `building` descriptors: fill from the metadata in chunks.
        let building: Vec<&QueryIndex> = indexes
            .iter()
            .filter(|ix| {
                !marker
                    .as_ref()
                    .and_then(|m| m.version_of(ix.name))
                    .is_some_and(|v| v == ix.version)
            })
            .collect();
        if building.is_empty() {
            // Counters on demand: nobody has asked yet ⇒ rest (building them costs a full scan).
            if self.counts_on_demand && !self.query_counts.wanted() {
                return Ok(BuildStep::Idle);
            }
            // Every descriptor `readable` ⇒ build the counters (needs a complete `ByCreated`).
            return Ok(match self.count_step(limit).await? {
                Some(rows) => BuildStep::Progress { rows },
                None => BuildStep::Idle,
            });
        }
        // A descriptor is being built ⇒ the counters (if they started from an
        // old mark) can no longer be trusted: start over once the build is done.
        if building
            .iter()
            .any(|ix| ix.code == query_index::BY_CREATED.code)
        {
            self.query_counts.reset();
        }
        if !cursor.built {
            let metas = self
                .writer
                .meta_rows_after(cursor.after.as_ref(), limit)
                .await?;
            let mut muts = Mutations::new();
            let mut rows = 0;
            for (id, raw) in &metas {
                let (Some(meta), Some((status, at))) = (
                    query_index::InvocationMeta::decode(raw),
                    self.status_at(id).await?,
                ) else {
                    continue;
                };
                let p = Placement::new(status.class(), at.unwrap_or(0), &meta);
                for ix in &building {
                    for (k, v) in query_index::rows(ix, id, &meta, p) {
                        muts.put_query_row(k, &v);
                        rows += 1;
                    }
                }
            }
            cursor.after = metas.last().map(|(id, _)| *id).or(cursor.after);
            cursor.built = metas.len() < limit;
            if !muts.is_empty() {
                self.writer.commit(muts).await?;
            }
            if !cursor.built {
                return Ok(BuildStep::Progress { rows });
            }
        }

        // ── 3. The mark, last: every descriptor of the table at its current version.
        let mut muts = Mutations::new();
        muts.set_query_marker(self.query_marker(indexes).as_bytes());
        self.writer.commit(muts).await?;
        *cursor = BuildCursor::default();
        Ok(BuildStep::Progress { rows: 0 })
    }

    /// One chunk of the **hint-row cleanup loop** (`ObjectKeys`): a row whose
    /// object has no state and no invocation left in the range is deleted —
    /// the write path never deletes them (invocation cleanup does not know what
    /// the object still has). Reads and deletes within the same `&mut self`,
    /// so no turn creating a new invocation for the object slips in between.
    /// `None` when done.
    ///
    /// **Not** on the path to becoming leader (scanning the bulk zone is not
    /// cached — an open reads every SST block once, `open_cost.rs`): the
    /// background loop calls it a while after the background build finishes,
    /// once per term.
    pub async fn hint_sweep_step(
        &mut self,
        cursor: &mut HintCursor,
        limit: usize,
    ) -> Result<Option<usize>> {
        let hints: Vec<&QueryIndex> = self
            .query_indexes
            .iter()
            .filter(|ix| ix.applies == query_index::Applies::PerObject)
            .collect();
        let mut checked = 0;
        let mut muts = Mutations::new();
        for ix in hints {
            let (lo0, hi) = ix.bounds();
            let lo = match &cursor.after {
                Some(k) if k.get(1) == Some(&ix.code) => {
                    let mut k = k.clone();
                    k.push(0);
                    k
                }
                Some(k) if k.get(1).is_some_and(|c| *c > ix.code) => continue,
                _ => lo0,
            };
            let rows = self.writer.query_rows(lo, hi, limit).await?;
            for (k, v) in &rows {
                checked += 1;
                cursor.after = Some(k.to_vec());
                let Some(row) = query_index::decode_row(ix, k, v) else {
                    continue;
                };
                if !self.object_alive(&row).await? {
                    muts.delete_query_row(k.to_vec());
                }
            }
            if rows.len() == limit {
                break;
            }
        }
        if !muts.is_empty() {
            self.writer.commit(muts).await?;
        }
        Ok((checked == limit).then_some(checked))
    }

    /// Whether the hint row's object still has state or an invocation in this
    /// range. Cannot be resolved (broken tenant/scope) ⇒ **still has** (delete
    /// only when provable: ambiguous ⇒ keep).
    async fn object_alive(&self, row: &query_index::IndexRow) -> Result<bool> {
        let (Some(t), Some(service), Some(key)) = (&row.tenant, &row.service, &row.key) else {
            return Ok(true);
        };
        let Ok(tenant) = cairn_store::TenantId::new(t) else {
            return Ok(true);
        };
        let scope = match row.scope.as_deref().filter(|s| !s.is_empty()) {
            Some(s) => match cairn_store::Scope::new(s) {
                Ok(s) => Some(s),
                Err(_) => return Ok(true),
            },
            None => None,
        };
        let Some(pk) = cairn_store::keys::object_owner(&tenant, scope.as_ref(), service, key)
        else {
            return Ok(true);
        };
        let state = cairn_store::keys::state_prefix(pk, &tenant, scope.as_ref(), service, key);
        if !self.writer.rows_under(&state, true, 1).await?.is_empty() {
            return Ok(true);
        }
        // Segmented values are state too.
        let seg =
            cairn_store::keys::segmented_state_prefix(pk, &tenant, scope.as_ref(), service, key);
        if !self.writer.rows_under(&seg, true, 1).await?.is_empty() {
            return Ok(true);
        }
        let metas = cairn_store::keys::entity(pk, cairn_store::keys::Table::InvocationMeta, &[]);
        // Cap: a pk shared by many objects is rare; over the cap without
        // having seen it ⇒ keep (ambiguous).
        let found = self.writer.rows_under(&metas, false, 256).await?;
        if found.len() == 256 {
            return Ok(true);
        }
        for (_, value) in &found {
            let Some(m) = query_index::InvocationMeta::decode(value) else {
                return Ok(true);
            };
            if m.tenant == *t
                && m.service == *service
                && m.key == *key
                && m.scope == row.scope.clone().unwrap_or_default()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One chunk of building the counters: scans `ByCreated` (class in the
    /// value, root + tenant in the key) from the counters' cursor. `None` when
    /// done (or `ByCreated` is not in the table). Same `&mut self` as every
    /// commit, so no change slips between reading the chunk and moving the
    /// cursor.
    async fn count_step(&mut self, limit: usize) -> Result<Option<usize>> {
        cairn_store::failpoint!("query::count_step");
        let ix = query_index::BY_CREATED;
        if !self.query_indexes.iter().any(|q| q.code == ix.code) {
            return Ok(None);
        }
        let counts = self.query_counts.clone();
        let lo = match counts.cursor() {
            None => {
                counts.begin();
                ix.prefix()
            }
            Some(None) => return Ok(None),
            Some(Some(c)) if c.is_empty() => ix.prefix(),
            Some(Some(mut c)) => {
                c.push(0);
                c
            }
        };
        let (_, hi) = ix.bounds();
        let rows = self.writer.query_rows(lo, hi, limit).await?;
        let decoded: Vec<_> = rows
            .iter()
            .filter_map(|(k, v)| query_index::decode_row(&ix, k, v))
            .collect();
        if let Some((k, _)) = rows.last() {
            counts.absorb(&decoded, k.to_vec());
        }
        if rows.len() < limit {
            counts.finish();
        }
        Ok(Some(rows.len()))
    }
}

impl Engine<cairn_store::SlateDb> {
    /// The range's read handle for the query layer: a clone of the `Db`, the
    /// writer's validity, the range, the counters, the descriptor table.
    /// Reading through it does not need the engine.
    pub fn read_handle(&self) -> cairn_store::range_read::RangeReadHandle {
        cairn_store::range_read::RangeReadHandle::new(
            self.writer.read_store(),
            self.writer.validity_handle(),
            self.range,
            Arc::clone(&self.query_counts),
            self.query_indexes,
        )
        .with_artifact_bytes(Arc::clone(&self.artifact_bytes))
        .sharing_live(Arc::clone(&self.query_snapshots))
    }
}
