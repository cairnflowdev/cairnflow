// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Self-contained backup — the part of `cairn-ctl backup` that touches SlateDB.
//!
//! A copy of a Db is **the manifest of a checkpoint** plus every object it references: the SSTs
//! of L0 and the sorted runs — including external SSTs, of the ancestor Db that a child range
//! stands on through `external_dbs` ([`slatedb::PathResolver`] returns the real path) —, the
//! manifests that the **final** checkpoints in it point to (with their SSTs), the current
//! manifest of each external Db (it keeps the last checkpoint the child range references,
//! which `verify` check (c) requires), and SlateDB's native WAL if the Db uses it. The
//! checkpoint keeps those SSTs from SlateDB's GC while copying; a cairn GC barrier (set by the
//! caller) keeps the ancestor Db from D-d.
//!
//! **Checkpoints that expire belong to the source cluster, not to the copy.** SlateDB's
//! compactor places one before every compaction (it lives `checkpoint_lifetime`, 15 min), a
//! `DbReader` replaces its own on every manifest change, another backup or fork places its own.
//! Their manifests are not pinned by our checkpoint: copying a few hundred ranges takes longer
//! than they live, and SlateDB's GC deletes what they pointed to midway (a 24 h soak failed
//! every backup this way). So the copy takes none of their objects and
//! [`drop_temporary_checkpoints`] removes them from the copy's manifest — our own checkpoint
//! included — so that the restored Db names no manifest it does not have. A final checkpoint
//! (no expiry — a clone's pin) is copied whole and a missing object of it is still an error,
//! except the pin of a child out of the copied table (rolled back, or still promised by an
//! intent): the copy does not carry that child, so the checkpoint leaves the copy too
//! ([`ObjectStores::backup_range_db`]).
//!
//! **Only what is already in L0.** With the shared WAL, a checkpoint pins no frame
//! (`docs/guides/nodewal.md`, NW-7): a range's unflushed commits (at most `frame_age_flush`)
//! do not make it into the copy, like `fork`. Each range's copy is a consistent prefix of that
//! range; across ranges there is no common cut.

use {
    crate::{
        config::{admin_io, ObjectStores},
        error::{Result, StoreError},
    },
    sha2::{Digest, Sha256},
    slatedb::{
        config::CheckpointOptions,
        object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode},
        Checkpoint, CheckpointCreateResult, PathResolver, VersionedManifest,
    },
    std::{collections::BTreeSet, sync::Arc, time::Duration},
};

/// Name of the checkpoint `backup` places on the source Db.
pub const CHECKPOINT_NAME: &str = "cairn-ctl-backup";

/// The part of one range Db in the backup.
pub struct DbCopy {
    /// Objects to copy, full paths in the main store (prefix included).
    pub objects: BTreeSet<Path>,
    /// Checkpoint placed on the source Db — removed with [`ObjectStores::backup_drop_checkpoint`].
    pub checkpoint: CheckpointCreateResult,
    /// When the checkpoint was created (Unix ms, SlateDB records it to the second): this range's
    /// copy is what was already in L0 at that point (at most `frame_age_flush` behind the last
    /// commit).
    pub as_of_ms: u64,
    /// The checkpoints in the copied manifest that the copy drops: those that expire (ours
    /// included) and the final ones of children that never committed. The copy has none of
    /// the objects they name — remove them from the copy with [`drop_temporary_checkpoints`]
    /// once the objects are copied.
    pub temporary: Vec<Checkpoint>,
}

fn manifest_path(db: &Path, id: u64) -> Path {
    Path::from(format!("{db}/manifest/{id:020}.manifest"))
}

fn wal_path(db: &Path, id: u64) -> Path {
    Path::from(format!("{db}/wal/{id:020}.sst"))
}

/// SSTs referenced by `m` (the manifest of the Db at `db`), real paths.
fn sst_paths(db: &Path, m: &VersionedManifest, out: &mut BTreeSet<Path>) {
    let resolver = PathResolver::new(db.clone(), m);
    for v in m.l0() {
        out.insert(resolver.sst_path(&v.sst.id));
    }
    for run in m.compacted() {
        for v in run.sst_views() {
            out.insert(resolver.sst_path(&v.sst.id));
        }
    }
}

impl ObjectStores {
    /// Checkpoints the Db of range `range` (lifetime `lifetime`) and every object the manifest
    /// of that checkpoint needs. `Ok(None)` = the Db has no manifest yet (the range was never
    /// opened). Clusters with a separate WAL store are not supported (the caller refuses first).
    ///
    /// `never_committed(child)`: the child range named by a final checkpoint (a clone's pin)
    /// is out of the copied table — rolled back already, or still promised by an intent. That
    /// checkpoint pins SSTs for a Db the copy does not carry, and on the source only the
    /// child's cleanup (SM-14, or the op's roll-back) removes it, through the child Db: the
    /// restored copy has no child Db, so the checkpoint is dropped from the copy like an
    /// expiring one.
    pub async fn backup_range_db(
        &self,
        range: u32,
        lifetime: Duration,
        never_committed: impl Fn(crate::ids::PartitionId) -> bool,
    ) -> Result<Option<DbCopy>> {
        let Some(wal_name) = self.manifest_wal_name(range).await? else {
            return Ok(None);
        };
        match wal_name.as_deref() {
            None | Some(crate::SHARED_WAL_NAME) => {}
            Some(other) => {
                return Err(StoreError::Corrupt(format!(
                    "range {range}: unknown WAL kind {other:?}"
                )))
            }
        }
        let db = self.range_path(range);
        let admin = self.admin(range);
        let checkpoint = admin_io(admin.create_detached_checkpoint(&CheckpointOptions {
            lifetime: Some(lifetime),
            source: None,
            name: Some(CHECKPOINT_NAME.into()),
        }))
        .await?;
        let m = admin_io(admin.read_manifest(Some(checkpoint.manifest_id)))
            .await?
            .ok_or_else(|| {
                StoreError::Corrupt(format!(
                    "range {range}: manifest {} of the checkpoint just placed is gone",
                    checkpoint.manifest_id
                ))
            })?;
        let as_of_ms = m
            .checkpoints()
            .iter()
            .find(|cp| cp.id == checkpoint.id)
            .map(|cp| u64::try_from(cp.create_time.timestamp_millis()).unwrap_or(0))
            .ok_or_else(|| {
                StoreError::Corrupt(format!(
                    "range {range}: manifest {} does not carry the checkpoint just placed",
                    m.id()
                ))
            })?;
        let mut objects = BTreeSet::new();
        objects.insert(manifest_path(&db, m.id()));
        sst_paths(&db, &m, &mut objects);
        // Manifests that **final** checkpoints point to: SlateDB on the copy still reads them
        // (GC computes live SSTs from them). A checkpoint that expires is the source's own
        // (module doc): none of its objects, and it leaves the copy's manifest.
        let mut temporary = Vec::new();
        for cp in m.checkpoints() {
            if cp.expire_time.is_some()
                || self
                    .checkpoint_child(cp.name.as_deref())
                    .is_some_and(&never_committed)
            {
                temporary.push(cp.clone());
                continue;
            }
            if cp.manifest_id == m.id() {
                continue;
            }
            let older = admin_io(admin.read_manifest(Some(cp.manifest_id)))
                .await?
                .ok_or_else(|| {
                    StoreError::Corrupt(format!(
                        "range {range}: manifest {} of final checkpoint {} is gone",
                        cp.manifest_id, cp.id
                    ))
                })?;
            objects.insert(manifest_path(&db, older.id()));
            sst_paths(&db, &older, &mut objects);
        }
        // External Db (retired ancestor): its current manifest holds the checkpoint the child
        // range stands on. Its SSTs are already in `sst_paths` (resolver).
        for ext in m.external_dbs() {
            let path = Path::from(ext.path.as_str());
            let em = admin_io(self.admin_at(path.clone()).read_manifest(None))
                .await?
                .ok_or_else(|| {
                    StoreError::Corrupt(format!(
                        "range {range}: external Db {} has no manifest",
                        ext.path
                    ))
                })?;
            objects.insert(manifest_path(&path, em.id()));
        }
        // SlateDB's native WAL (a Db not yet moved to the shared WAL): every WAL SST the
        // manifest still has to replay.
        if wal_name.is_none() {
            for id in m.replay_after_wal_id() + 1..m.next_wal_sst_id() {
                objects.insert(wal_path(&db, id));
            }
        }
        Ok(Some(DbCopy {
            objects,
            checkpoint,
            as_of_ms,
            temporary,
        }))
    }

    /// Removes the checkpoint `cp` that [`Self::backup_range_db`] placed on range `range`.
    pub async fn backup_drop_checkpoint(
        &self,
        range: u32,
        cp: &CheckpointCreateResult,
    ) -> Result<()> {
        admin_io(self.admin(range).delete_checkpoint(cp.id)).await
    }
}

/// Removes the checkpoints `temporary` ([`DbCopy::temporary`]) from the copy of a Db at `db` in
/// `dst`, after its objects are copied. Each removal writes the copy's next manifest (SlateDB's
/// `Admin::delete_checkpoint`, a conditional create on a Db no writer opens). Returns the
/// path of the copy's latest manifest — the backup lists it with the other objects — or `None`
/// when there was nothing to remove.
pub async fn drop_temporary_checkpoints(
    dst: &Arc<dyn ObjectStore>,
    db: &Path,
    temporary: &[Checkpoint],
) -> Result<Option<Path>> {
    if temporary.is_empty() {
        return Ok(None);
    }
    let admin = crate::config::admin_builder(db.clone(), Arc::clone(dst)).build();
    for cp in temporary {
        admin_io(admin.delete_checkpoint(cp.id)).await?;
    }
    let m = admin_io(admin.read_manifest(None)).await?.ok_or_else(|| {
        StoreError::Corrupt(format!(
            "the copy of {db} has no manifest after removing checkpoints"
        ))
    })?;
    if let Some(left) = m.checkpoints().iter().find(|c| c.expire_time.is_some()) {
        return Err(StoreError::Corrupt(format!(
            "the copy of {db} still carries the expiring checkpoint {}",
            left.id
        )));
    }
    Ok(Some(manifest_path(db, m.id())))
}

/// One copied object: size and SHA-256 of the content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Copied {
    pub size: u64,
    pub sha256: [u8; 32],
}

/// Copies `from` (store `src`) to `to` (store `dst`) with `Create`, hashing it. `Ok(None)` =
/// `from` does not exist. The destination already has a **different** object ⇒ error; the same
/// content (our own ambiguous write) ⇒ treated as copied.
pub async fn copy_hashed(
    src: &Arc<dyn ObjectStore>,
    from: &Path,
    dst: &Arc<dyn ObjectStore>,
    to: &Path,
) -> Result<Option<Copied>> {
    let raw = match src.get(from).await {
        Ok(g) => g
            .bytes()
            .await
            .map_err(|e| StoreError::Io(format!("reading {from}: {e}")))?,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(StoreError::Io(format!("reading {from}: {e}"))),
    };
    let copied = Copied {
        size: raw.len() as u64,
        sha256: Sha256::digest(&raw).into(),
    };
    match crate::cas::put_resolving(dst, to, raw, PutMode::Create).await {
        Ok(crate::cas::Cas::Written(_)) => Ok(Some(copied)),
        Ok(crate::cas::Cas::Lost) => Err(StoreError::Io(format!(
            "{to} already exists at the destination with different content"
        ))),
        Err(e) => Err(StoreError::Io(format!("write {to}: {e}"))),
    }
}

/// Reads `path` and checks size + hash against `want`. `Ok(None)` = absent.
pub async fn read_checked(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    want: &Copied,
) -> Result<Option<bytes::Bytes>> {
    let raw = match store.get(path).await {
        Ok(g) => g
            .bytes()
            .await
            .map_err(|e| StoreError::Io(format!("reading {path}: {e}")))?,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(StoreError::Io(format!("reading {path}: {e}"))),
    };
    let got: [u8; 32] = Sha256::digest(&raw).into();
    if raw.len() as u64 != want.size || got != want.sha256 {
        return Err(StoreError::Corrupt(format!(
            "{path}: size/hash differs from the backup manifest ({} bytes)",
            raw.len()
        )));
    }
    Ok(Some(raw))
}
