// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The range's live artifact byte counter: the write and rebuild paths. The
//! counting rule is in [`cairn_store::artifact_bytes`]: a row `(id, sha)`
//! **counts** when it decodes and `id` has metadata; totals are per the
//! metadata's tenant.
//!
//! Every path that changes whether a row counts emits a delta for that row,
//! computed from the state read while holding the engine in the same hold as
//! the commit:
//!
//! | Path | Delta |
//! |---|---|
//! | [`Engine::stage_artifact_put`] (an SDK PUT, a `restart` copy) | `+size − old size` when `id` has metadata |
//! | `restart_in` (the commit creating the new copy's metadata) | `+size` per row copied to the new copy |
//! | `purge` (deletes rows + metadata in the same commit) | `−size` per row |
//! | `purge_tenant_rows` (artifact rows go in pass 1, metadata in pass 2) | `−size` per row |
//!
//! No other path writes/deletes `Artifacts` rows or deletes metadata
//! (`Mutations::put_artifact`/`purge_artifact`/`clear_invocation_meta`/
//! `purge_tenant_row`); adding a path means adding a line here and a comparison in
//! `tests/query_meta.rs::artifact_bytes_match_scan`.

use {
    super::*,
    cairn_store::{
        artifact_bytes::{delta, ArtifactBytes},
        keys::{self as keyspace, ArtifactRow},
    },
};

/// A signed byte count — an artifact's `size` ≤ `MAX_BYTES`, never overflows.
pub(super) fn signed(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

impl<S: RangeStore> Engine<S> {
    /// The range's live artifact byte counter — readable without the engine
    /// (`sys_artifact_bytes` does not read the store).
    pub fn artifact_bytes(&self) -> &Arc<ArtifactBytes> {
        &self.artifact_bytes
    }

    /// The range's billing unit counter — readable without the engine; the
    /// metering drainer takes it through `cairn_store::units::collect`.
    pub fn unit_meter(&self) -> &Arc<cairn_store::units::UnitMeter> {
        &self.unit_meter
    }

    /// Writes the artifact row `(id, sha)` into `muts`, with the counter delta:
    /// `+size − size of the old row` when `id` has metadata (otherwise the row
    /// does not count yet — a `restart` copy counts at the commit creating the
    /// new copy). Two hot-zone point reads; the caller commits `muts` **within
    /// the same engine hold**.
    pub async fn stage_artifact_put(
        &self,
        muts: &mut Mutations,
        id: &InvocationId,
        sha: &[u8; 32],
        row: &ArtifactRow,
    ) -> Result<()> {
        muts.put_artifact(id, sha, row);
        let Some(meta) = self.meta(id).await? else {
            return Ok(());
        };
        let old = self
            .writer
            .get(&keyspace::artifact(id, sha))
            .await?
            .and_then(|raw| ArtifactRow::decode(&raw));
        // Billing units: a **new** row counts by artifact bytes; a PUT of the
        // same `(id, sha)` again (a retry attempt) does not count.
        if old.is_none() {
            muts.meter_units(
                &meta.tenant,
                cairn_store::units::of_payload(usize::try_from(row.size).unwrap_or(usize::MAX)),
            );
        }
        let old = old.map_or(0, |r| r.size);
        muts.artifact_bytes(delta(id, sha, &meta.tenant, signed(row.size) - signed(old)));
        Ok(())
    }

    /// Whether any tenant carries `max-artifact-bytes=` in the range's snapshot
    /// — the rebuild loop only scans when one does (self-hosting pays for no
    /// hot-zone scan).
    pub fn artifact_bytes_wanted(&mut self) -> bool {
        self.refresh_tenant_limits();
        self.tenant_limits
            .limited()
            .any(|(_, l)| l.max_artifact_bytes.is_some())
    }

    /// One chunk of rebuilding the counter: scans the range's hot zone from
    /// the cursor, at most `limit` rows (every table — artifact rows interleave
    /// by `owner_pk`), summing the artifact rows that count. `None` when done.
    /// Same `&mut self` as every commit, so no delta slips between reading the
    /// chunk and moving the cursor.
    pub async fn artifact_bytes_step(&mut self, limit: usize) -> Result<Option<usize>> {
        let limit = limit.max(1);
        let counts = Arc::clone(&self.artifact_bytes);
        let Some((start, hi)) =
            keyspace::owned_ranges(PartitionKey(self.range.start), PartitionKey(self.range.end))
                .into_iter()
                .find(|(lo, _)| keyspace::Region::of_key(lo) == Some(keyspace::Region::Hot))
        else {
            counts.begin();
            counts.finish();
            return Ok(None);
        };
        let lo = match counts.cursor() {
            None => {
                counts.begin();
                start
            }
            Some(None) => return Ok(None),
            Some(Some(c)) if c.is_empty() => start,
            Some(Some(mut c)) => {
                c.push(0);
                c
            }
        };
        let rows = self.writer.query_rows(lo, hi, limit).await?;
        let mut found = Vec::new();
        let mut seen: Option<(InvocationId, Option<String>)> = None;
        for (k, v) in &rows {
            let Some((id, _)) = keyspace::parse_artifact(k) else {
                continue;
            };
            let Some(row) = ArtifactRow::decode(v) else {
                continue;
            };
            let tenant = match &seen {
                Some((i, t)) if *i == id => t.clone(),
                _ => {
                    let t = self.meta(&id).await?.map(|m| m.tenant);
                    seen = Some((id, t.clone()));
                    t
                }
            };
            if let Some(t) = tenant {
                found.push((t, row.size));
            }
        }
        if let Some((k, _)) = rows.last() {
            counts.absorb(&found, k.to_vec());
        }
        if rows.len() < limit {
            counts.finish();
        }
        Ok(Some(rows.len()))
    }
}
