// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The value sweep round: segments
//! `values/<tenant>/<pk>/<owner>/<sha>-<gen>` with no holder left — the holder has
//! removed it (the state changed value, the journal was cleaned), or orphans (the PUT
//! finished but the holder row never committed) — are **held in place** in the trash, then
//! really deleted after the retention time, checked once more. The pattern of
//! [`crate::artifact_sweep`].
//!
//! Each node runs its own round and only touches owners whose `pk` range **it** currently
//! leads: it asks that range's engine once per owner directory
//! ([`crate::actor::EngineHandle::values_referenced`]); not leader, the range changing, a
//! corrupt row, the limit exceeded ⇒ that directory is skipped (kept). The second half of the
//! proof (the object exists and is older than `min_age`) HEADs each segment **after** the
//! engine has been released.
//!
//! One round:
//!
//! 1. List the trash's `Subject::Value` entries ([`Trash::held_entries`]:
//!    GET `entry` only for entries not yet classified).
//! 2. List `values/` page by page (limit [`MAX_PAGES`] × [`PAGE`] objects), starting
//!    after the cursor the previous round left.
//! 3. Directories with segments older than `min_age` or with entries, whose owner is in a
//!    range we lead: a segment with no holder left ⇒ without an entry, write the entry (one
//!    PUT); an entry past its retention time ⇒ delete the object, then the entry. A segment
//!    **still** held that has an entry ⇒ the hold was wrong: tripwire `value::delete_live`,
//!    remove the entry (restore, SM-16). The engine cannot prove it and there is an entry ⇒
//!    remove it too.
//! 4. Entries whose object is already gone (really deleted, died before removing the entry):
//!    remove the entry — nothing left to delete.

use {
    crate::partitions::Partitions,
    cairn_store::{
        proof::DeleteProof,
        trash::{EntryId, Subject, Trash},
        value::{Segment, ValueStore},
        ClusterConfig, StoreError,
    },
    slatedb::object_store::path::Path,
    std::{collections::BTreeMap, time::Duration},
};

/// Default tick of the sweep round.
pub const DEFAULT_EVERY: Duration = Duration::from_secs(3600);
/// Minimum age of a segment before the sweep round touches it (≥ 2 × `T_put`).
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(3600);
/// Objects per LIST page.
pub const PAGE: usize = 1000;
/// Maximum pages per round; the next round continues from the cursor.
pub const MAX_PAGES: usize = 100;
/// Limit on one engine question.
const ASK_BOUND: Duration = Duration::from_secs(5);

/// Result of one round — for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Round {
    pub held: usize,
    pub purged: usize,
    /// Entries removed because the segment is still held or cannot be proven (tripwire).
    pub released: usize,
    /// Entries removed because the object is already gone.
    pub cleared: usize,
}

pub struct Sweep<'a> {
    pub values: &'a ValueStore,
    pub trash: &'a Trash,
    pub cluster: &'a ClusterConfig,
    pub handles: &'a Partitions,
    pub min_age: Duration,
    pub keep: Duration,
    pub now_ms: u64,
}

impl Sweep<'_> {
    /// One round — see the module's top. `cursor`: the last object considered (`None` = from
    /// the start). End of the listing ⇒ `None`.
    pub async fn once(&self, cursor: &mut Option<Path>) -> Result<Round, StoreError> {
        let mut held: BTreeMap<Segment, EntryId> = BTreeMap::new();
        for (id, subject) in self.trash.held_entries().await? {
            if let Subject::Value(s) = subject {
                held.insert(s, id);
            }
        }
        let mut round = Round::default();
        let min_age_ms = self.min_age.as_millis() as u64;
        for _ in 0..MAX_PAGES {
            let (dirs, next) = self.values.list_page(cursor.as_ref(), PAGE).await?;
            for d in dirs {
                let mut segs = Vec::with_capacity(d.segments.len());
                for (sha, gen, ms) in d.segments {
                    let seg = Segment {
                        tenant: d.tenant.clone(),
                        pk: d.pk,
                        owner: d.owner,
                        sha,
                        gen,
                    };
                    let entry = held.remove(&seg);
                    if entry.is_some() || ms.saturating_add(min_age_ms) <= self.now_ms {
                        segs.push((seg, entry));
                    }
                }
                if !segs.is_empty() {
                    self.decide(segs, &mut round).await?;
                }
            }
            *cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        // Entries whose object is in none of the pages this round considered: if the object is
        // gone remove the entry; otherwise leave it for a later round (another page).
        for (seg, entry) in held {
            if self.values.head(&seg).await?.is_none() {
                self.trash.restore(&entry).await?;
                round.cleared += 1;
            }
        }
        Ok(round)
    }

    /// The segments `segs` of **one** owner (same `tenant`, `pk`, `owner`).
    async fn decide(
        &self,
        segs: Vec<(Segment, Option<EntryId>)>,
        round: &mut Round,
    ) -> Result<(), StoreError> {
        let first = &segs[0].0;
        let range = self.cluster.partition_of_key(first.pk);
        let Some(h) = self.handles.get(range) else {
            return Ok(());
        };
        let asked = tokio::time::timeout(
            ASK_BOUND,
            h.values_referenced(first.tenant.clone(), first.pk, first.owner),
        )
        .await;
        let token = match asked {
            Ok(Ok(Ok(token))) => token,
            Ok(Ok(Err(why))) => {
                // Cannot prove it (a corrupt row, the range being split/merged): any existing
                // entry is a promise to delete that has lost its grounds — remove it.
                for (seg, entry) in segs {
                    if let Some(entry) = entry {
                        self.release(&seg, &entry, &why, round).await?;
                    }
                }
                return Ok(());
            }
            // Not leader, the range changing, the limit exceeded: keep.
            Ok(Err(_)) | Err(_) => return Ok(()),
        };
        let min_age_ms = self.min_age.as_millis() as u64;
        for (seg, entry) in segs {
            if token.held().contains(&(seg.sha, seg.gen)) {
                if let Some(entry) = entry {
                    self.release(&seg, &entry, "segment still held", round)
                        .await?;
                }
                continue;
            }
            let proof = match DeleteProof::value_unreferenced(
                &token,
                self.values,
                &seg,
                min_age_ms,
                self.now_ms,
            )
            .await?
            {
                Ok(p) => p,
                Err(_) => continue,
            };
            match entry {
                None => {
                    self.trash
                        .hold_in_place(
                            &EntryId::new(self.now_ms),
                            &proof,
                            "segment: no holder left (value changed, purge or orphan)",
                        )
                        .await?;
                    round.held += 1;
                }
                Some(entry) if entry.created_ms() + self.keep.as_millis() as u64 <= self.now_ms => {
                    let path = self.values.path(&seg);
                    let n = self.trash.purge_in_place(&entry, proof, &[path]).await?;
                    tracing::info!(segment = ?seg, objects = n, "value: really deleting a segment past its retention time");
                    round.purged += 1;
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Tripwire (AGENTS §6.4): an in-place hold entry for a segment still held or that
    /// cannot be proven — the hold was wrong. Remove the entry (restore).
    async fn release(
        &self,
        seg: &Segment,
        entry: &EntryId,
        why: &str,
        round: &mut Round,
    ) -> Result<(), StoreError> {
        cairn_store::failpoint!("value::delete_live");
        tracing::error!(segment = ?seg, reason = %why, "value: in-place hold entry has lost its grounds — removing the entry");
        self.trash.restore(entry).await?;
        round.released += 1;
        Ok(())
    }
}
