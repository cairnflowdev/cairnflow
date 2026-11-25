// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The artifact sweep (AP-2, AP-8): the directory
//! `artifacts/<tenant>/<id>/<epoch>/` of a dead incarnation — purged, or
//! orphaned (PUT done but the row never committed) — is **held in place** in
//! the trash, then deleted for real after the retention period, checked once
//! more.
//!
//! Each node runs its own sweep and only touches ids whose range **it**
//! currently leads: it asks that range's engine
//! ([`crate::actor::EngineHandle::artifacts_unreferenced`]); not the leader,
//! range changing, limit reached ⇒ skip that directory (keep). The second half
//! of the proof (age ≥ `min_age`) lists the directory **after** the engine has
//! been released.
//!
//! One round:
//!
//! 1. List the trash's held-in-place entries ([`Trash::held_entries`]: LIST
//!    with a delimiter, GET `entry` only for entries not yet classified).
//! 2. List `artifacts/` page by page (cap [`MAX_PAGES`] × [`PAGE`] objects),
//!    starting **after the cursor** the previous round left; at the end of the
//!    listing the cursor returns to the start for the next round. The cursor
//!    only decides which directories are *examined* in a round — the proof does
//!    not rely on it: [`DeleteProof::purged_invocation`] lists the whole
//!    directory itself, and the real delete lists it again.
//! 3. A directory older than `min_age` whose id belongs to a range we lead:
//!    provable ⇒ no entry yet, write one (one PUT); an entry past its retention
//!    period, delete the objects then the entry. Not provable but an entry
//!    exists ⇒ holding it was wrong: tripwire `artifact::delete_live`, remove
//!    the entry (restore).
//! 4. A held-in-place entry whose directory is already empty: same check,
//!    delete the entry.

use {
    crate::partitions::Partitions,
    cairn_store::{
        artifact::{ArtifactStore, Owner},
        proof::DeleteProof,
        trash::{EntryId, Subject, Trash},
        ClusterConfig, StoreError,
    },
    slatedb::object_store::path::Path,
    std::{collections::BTreeMap, time::Duration},
};

/// Default interval of the sweep.
pub const DEFAULT_EVERY: Duration = Duration::from_secs(3600);
/// Minimum age of an object before the sweep touches its directory
/// (≥ 2 × `T_put`: an entry pointing at an artifact must commit within
/// `T_put` of the PUT, AP-9).
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(3600);
/// Objects per LIST page.
pub const PAGE: usize = 1000;
/// Maximum pages per round; the next round continues from the cursor.
pub const MAX_PAGES: usize = 100;
/// Limit of one engine query.
const ASK_BOUND: Duration = Duration::from_secs(5);

/// Result of one round — for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Round {
    pub held: usize,
    pub purged: usize,
    /// Entries removed because the incarnation still has rows (tripwire).
    pub released: usize,
}

pub struct Sweep<'a> {
    pub artifacts: &'a ArtifactStore,
    pub trash: &'a Trash,
    pub cluster: &'a ClusterConfig,
    pub handles: &'a Partitions,
    pub min_age: Duration,
    pub keep: Duration,
    pub now_ms: u64,
}

impl Sweep<'_> {
    /// One round — see the module head. `cursor`: the last object examined
    /// (`None` = from the start). Written after each page is done — a round
    /// that fails or runs over its limit midway lets the next round continue
    /// from the failed page; end of the listing ⇒ `None`.
    pub async fn once(&self, cursor: &mut Option<Path>) -> Result<Round, StoreError> {
        let mut held: BTreeMap<Owner, EntryId> = BTreeMap::new();
        for (id, subject) in self.trash.held_entries().await? {
            if let Subject::Artifacts(o) = subject {
                held.insert(o, id);
            }
        }
        let mut round = Round::default();
        let min_age_ms = self.min_age.as_millis() as u64;
        for _ in 0..MAX_PAGES {
            let (dirs, next) = self.artifacts.list_page(cursor.as_ref(), PAGE).await?;
            for d in dirs {
                let entry = held.remove(&d.owner);
                if d.newest_ms.saturating_add(min_age_ms) > self.now_ms {
                    continue;
                }
                self.decide(&d.owner, entry, &mut round).await?;
            }
            *cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        // Entries whose directory is already empty (objects all deleted in an earlier round).
        for (owner, entry) in held {
            if self.artifacts.objects(&owner).await?.is_empty() {
                self.decide(&owner, Some(entry), &mut round).await?;
            }
        }
        Ok(round)
    }

    async fn decide(
        &self,
        owner: &Owner,
        entry: Option<EntryId>,
        round: &mut Round,
    ) -> Result<(), StoreError> {
        let range = self
            .cluster
            .partition_of_key(cairn_store::partition_key_of(&owner.id));
        let Some(h) = self.handles.get(range) else {
            return Ok(());
        };
        let asked = tokio::time::timeout(ASK_BOUND, h.artifacts_unreferenced(owner.clone())).await;
        let token = match asked {
            Ok(Ok(Ok(token))) => token,
            Ok(Ok(Err(why))) => {
                if let Some(entry) = entry {
                    // Tripwire: a held-in-place entry for an
                    // incarnation that still has rows — holding it was wrong
                    // (purge not durable and the node died?).
                    cairn_store::failpoint!("artifact::delete_live");
                    tracing::error!(owner = ?owner, reason = %why, "artifact: held-in-place entry for an incarnation that still has rows — removing the entry");
                    self.trash.restore(&entry).await?;
                    round.released += 1;
                }
                return Ok(());
            }
            // Not the leader, range changing, limit reached: keep.
            Ok(Err(_)) | Err(_) => return Ok(()),
        };
        let proof = match DeleteProof::purged_invocation(
            token,
            self.artifacts,
            self.min_age.as_millis() as u64,
            self.now_ms,
        )
        .await?
        {
            Ok(p) => p,
            Err(_) => return Ok(()),
        };
        match entry {
            None => {
                self.trash
                    .hold_in_place(
                        &EntryId::new(self.now_ms),
                        &proof,
                        "artifact: incarnation has no rows left (purged or orphaned)",
                    )
                    .await?;
                round.held += 1;
            }
            Some(entry) if entry.created_ms() + self.keep.as_millis() as u64 <= self.now_ms => {
                let paths: Vec<_> = self
                    .artifacts
                    .objects(owner)
                    .await?
                    .into_iter()
                    .map(|m| m.location)
                    .collect();
                let n = self.trash.purge_in_place(&entry, proof, &paths).await?;
                tracing::info!(owner = ?owner, objects = n, "artifact: deleting a directory past its retention period");
                round.purged += 1;
            }
            Some(_) => {}
        }
        Ok(())
    }
}
