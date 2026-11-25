// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Reading and repairing a bucket **offline** — the store-layer toolkit of `cairn-ctl`.
//!
//! Every function here only does GET/HEAD/LIST, except those whose name and doc say clearly
//! that they write ([`cas_owner`], [`write_audit`], [`ObjectStores::fork_range`]). The tool's
//! repair commands check everything with the read functions first and only then call **one**
//! write function.

use {
    crate::config::admin_io,
    crate::{
        config::{normalize_path, split_range_db_path, ObjectStores, StoreLocation},
        error::{Result, StoreError},
        ids::PartitionId,
        owner::{self, OwnerRecord, SessionId},
        partition_table::RangeId,
        session::{self, SessionRecord},
    },
    async_trait::async_trait,
    futures::{StreamExt, TryStreamExt},
    slatedb::{
        config::CheckpointOptions,
        manifest::ExternalDb,
        object_store::{
            path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
        },
        wal::{WalAdmin, WalError, WalGc},
        CloneSourceSpec,
    },
    std::{sync::Arc, time::Duration},
};

pub use crate::owner::ObservedOwner;

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

// ───────────────────────────────────────── per-item fault-tolerant reads

/// Retry rule for one read of the scanner: a few attempts, short back-off — an item that
/// stays broken longer than this is skipped and counted, rather than holding up the whole
/// round.
pub const SCAN_RETRY: crate::retry::Policy =
    crate::retry::Policy::new(Duration::from_millis(100), Duration::from_secs(2), 4);

/// One read of the scanner through [`crate::retry::run`] (op `scan.read`).
pub async fn scan_read<T, E, F, Fut>(f: F) -> std::result::Result<T, E>
where
    E: crate::retry::Retryable + std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
{
    crate::retry::run(
        &crate::retry::ops::SCAN_READ,
        &SCAN_RETRY,
        crate::retry::Never,
        f,
    )
    .await
}

/// An item the scanner **skipped** because its read failed after [`SCAN_RETRY`] — the round
/// still finishes, this item has not been checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanSkip {
    /// The item, as the operator reads it: `ranges/12`, `nodewal/n1`, `sessions/n1/7`.
    pub item: String,
    pub range: Option<RangeId>,
    pub session: Option<SessionId>,
    pub error: String,
}

fn under(prefix: &Path, rel: &str) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(rel)
    } else {
        Path::from(format!("{}/{rel}", prefix.as_ref()))
    }
}

// ─────────────────────────────────────────────────────── owner record, session

/// The owner record of `p` together with the version for the CAS and `last_modified`.
pub async fn owner_observed(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    p: RangeId,
) -> Result<Option<ObservedOwner>> {
    owner::read_observed(store, prefix, p).await
}

/// **Writes** the owner record of `p`, CAS on `expected` (`None` ⇒ create new).
/// `Ok(false)` = the CAS missed: someone changed the record between the read and the write.
pub async fn cas_owner(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    p: RangeId,
    record: &OwnerRecord,
    expected: Option<UpdateVersion>,
) -> Result<bool> {
    Ok(owner::write_owner(store, prefix, p, record, expected)
        .await?
        .is_some())
}

/// The ids of every range with an owner record `owners/range-<id>`, ascending.
pub async fn owner_ranges(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<RangeId>> {
    let dir = Path::from(owner::owners_dir(prefix));
    let listed: Vec<_> = store.list(Some(&dir)).try_collect().await.map_err(io)?;
    let mut out: Vec<RangeId> = listed
        .iter()
        .filter_map(|m| {
            let n = m.location.filename()?.strip_prefix("range-")?;
            let id: u32 = n.parse().ok()?;
            (id.to_string() == n).then_some(PartitionId(id))
        })
        .collect();
    out.sort_by_key(|r| r.0);
    Ok(out)
}

/// Every session with a `sessions/<node>/<inc>` record (one LIST), in order.
pub async fn session_ids(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<SessionId>> {
    let dir = Path::from(session::sessions_dir_of(prefix));
    let listed: Vec<_> = store.list(Some(&dir)).try_collect().await.map_err(io)?;
    let mut out: Vec<SessionId> = listed
        .iter()
        .filter_map(|m| session::session_id_from(prefix, &m.location))
        .collect();
    out.sort_by(|a, b| (&a.node, a.inc).cmp(&(&b.node, b.inc)));
    Ok(out)
}

/// The session record `sessions/<node>/<inc>` (decoded, or the decode error) together with
/// `last_modified` (ms). `Ok(None)` = not there — the session has been cleaned up.
pub async fn session_record(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &SessionId,
) -> Result<Option<(Result<SessionRecord>, u64)>> {
    let got = match store.get(&session::session_path(prefix, id)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let modified = got.meta.last_modified.timestamp_millis().max(0) as u64;
    let raw = got.bytes().await.map_err(io)?;
    Ok(Some((SessionRecord::decode(&raw), modified)))
}

// ─────────────────────────────────────────────────────────────────── audit

/// Header of `audit/ctl/<ts>` — see `crate::format::OBJECT_POLICIES`.
pub const AUDIT_HEADER: &str = "cairnflow-ctl-audit/1";

/// One manual repair of the bucket (`cairn-ctl … --apply`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    /// Who ran it (as declared by the operator, or `$USER@$HOST`).
    pub who: String,
    pub at_ms: u64,
    /// The command line, verbatim.
    pub command: String,
    /// The object changed (path under the prefix).
    pub object: String,
    /// Content before and after, as text (empty = absent).
    pub before: String,
    pub after: String,
}

impl AuditEntry {
    /// `k=v` for the one-line fields, then two blocks `[before]`/`[after]` to the end of the
    /// object — lenient read: unknown keys are ignored.
    pub fn encode(&self) -> String {
        let one = |s: &str| s.replace('\n', " ");
        format!(
            "{AUDIT_HEADER}\nwho={}\nat_ms={}\ncommand={}\nobject={}\n[before]\n{}\n[after]\n{}\n",
            one(&self.who),
            self.at_ms,
            one(&self.command),
            one(&self.object),
            self.before.trim_end(),
            self.after.trim_end()
        )
    }
}

/// **Writes** `audit/ctl/<at_ms>-<nonce>` (`PutMode::Create`). Returns the path written.
pub async fn write_audit(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    entry: &AuditEntry,
) -> Result<Path> {
    for attempt in 0u32..8 {
        let mut nonce = [0u8; 2];
        getrandom_nonce(&mut nonce, attempt);
        let path = under(
            prefix,
            &format!(
                "audit/ctl/{:013}-{:02x}{:02x}",
                entry.at_ms, nonce[0], nonce[1]
            ),
        );
        // Ambiguity is resolved by reading back (rule 7): the record carries who, when and
        // which command — identical content under the same name is our own write, not a
        // name someone else already used (previously: two records under two nonces).
        match crate::cas::put_resolving(
            store,
            &path,
            entry.encode().into_bytes().into(),
            PutMode::Create,
        )
        .await
        {
            Ok(crate::cas::Cas::Written(_)) => return Ok(path),
            Ok(crate::cas::Cas::Lost) => continue,
            Err(e) => return Err(io(e)),
        }
    }
    Err(StoreError::Io(
        "could not pick an unused audit/ctl/<ts> name".into(),
    ))
}

/// Two "random" bytes are enough so that two writes in the same millisecond do not
/// overwrite each other — no cryptographic quality needed (on a name clash `Create` misses
/// and retries).
fn getrandom_nonce(out: &mut [u8; 2], attempt: u32) {
    let t = crate::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let x = t ^ attempt.wrapping_mul(0x9e37_79b9) ^ crate::entropy::process_id();
    *out = [(x >> 8) as u8, x as u8];
}

// ───────────────────────────────────────────────────────────── range Dbs

/// One `external_dbs` entry of a range's manifest, checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalDbRef {
    /// The source Db path, verbatim as in the manifest.
    pub path: String,
    /// The source's range id when it lies under the current prefix.
    pub range: Option<RangeId>,
    pub status: ExternalStatus,
}

/// Whether a reference to a source Db is still usable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalStatus {
    Ok,
    /// No manifest left at that path — the source's SSTs may be lost.
    DbMissing,
    /// The source is still there, but the checkpoint the child stands on is lost — the next
    /// open / split returns `checkpoint missing`.
    CheckpointMissing(String),
    /// The checkpoint pinned on the source has been removed, but the entry stands on no SST
    /// of the source — neither in the current manifest nor in any manifest pinned by a live
    /// checkpoint of the child: SlateDB's GC `detach` is in the gap between its two steps
    /// (remove the checkpoint on the parent, then drop the entry from the child —
    /// [`detach_pending`]). Nothing is lost; the next detach run drops the entry.
    Detaching,
    /// Reading the source manifest failed.
    Unreadable(String),
}

/// Whether the loss of the final checkpoint that a child's entry `entry` placed on the
/// source is just an unfinished SlateDB GC `detach`: the entry has a final checkpoint, and
/// no manifest in `views` (the child's current manifest and every manifest pinned by a live
/// checkpoint of the child) has an entry carrying the same final checkpoint that still
/// stands on SSTs (non-empty `sst_ids`).
///
/// This is exactly the condition SlateDB's `find_detachable` (`detach_gc.rs`) uses before
/// removing the checkpoint on the parent — **parent first**, then dropping the entry from
/// the child — so in the gap between the two steps the entry is still listed while the
/// checkpoint is gone. While the entry still stands on SSTs in any view, detach never
/// removes its checkpoint: then the loss is real (a reader or a clone at that checkpoint
/// needs the parent's SSTs).
pub fn detach_pending<'a>(
    entry: &ExternalDb,
    views: impl IntoIterator<Item = &'a [ExternalDb]>,
) -> bool {
    let Some(id) = entry.final_checkpoint_id else {
        return false;
    };
    views.into_iter().all(|dbs| {
        dbs.iter()
            .all(|e| e.final_checkpoint_id != Some(id) || e.sst_ids.is_empty())
    })
}

/// The `<db>/compacted` LISTs done in one `verify --deep` run, by directory — the external
/// SSTs of many ranges lie under the same ancestor Db, so one LIST per Db.
#[derive(Debug, Default)]
pub struct SstListing {
    dirs: std::collections::BTreeMap<Path, std::collections::BTreeSet<Path>>,
}

impl SstListing {
    /// Number of `compacted` directories LISTed.
    pub fn dirs(&self) -> usize {
        self.dirs.len()
    }
}

/// `<db>/manifest/<id:020>.manifest` — SlateDB's layout (pinned in `library_contracts`).
fn manifest_object(db: &Path, id: u64) -> Path {
    Path::from(format!("{db}/manifest/{id:020}.manifest"))
}

fn parent_dir(p: &Path) -> Path {
    let s = p.as_ref();
    Path::from(s.rsplit_once('/').map_or("", |(d, _)| d))
}

impl ObjectStores {
    /// The objects the Db of range `range` needs to be readable: the SSTs the current
    /// manifest mentions, and — for each checkpoint **not yet expired** — that checkpoint's
    /// manifest together with the SSTs it mentions (external SSTs resolved to the ancestor
    /// Db). `Ok(None)` = the Db has no manifest.
    async fn referenced_objects(
        &self,
        range: RangeId,
    ) -> Result<Option<std::collections::BTreeSet<Path>>> {
        let admin = self.admin(range.0);
        let Some(m) = admin_io(admin.read_manifest(None)).await? else {
            return Ok(None);
        };
        let db = self.range_path(range.0);
        let mut out = std::collections::BTreeSet::new();
        let add = |m: &slatedb::VersionedManifest, out: &mut std::collections::BTreeSet<Path>| {
            let resolver = slatedb::PathResolver::new(db.clone(), m);
            for v in m.all_sst_views() {
                out.insert(resolver.sst_path(&v.sst.id));
            }
        };
        add(&m, &mut out);
        let now = chrono::DateTime::<chrono::Utc>::from(crate::entropy::now());
        for cp in m.checkpoints() {
            if cp.manifest_id == m.id() || cp.expire_time.is_some_and(|t| t <= now) {
                continue;
            }
            out.insert(manifest_object(&db, cp.manifest_id));
            if let Some(older) = admin_io(admin.read_manifest(Some(cp.manifest_id))).await? {
                add(&older, &mut out);
            }
        }
        Ok(Some(out))
    }

    /// **`verify --deep`**: objects that the Db of range `range` needs
    /// ([`Self::referenced_objects`]) but that are no longer on the bucket — losing single
    /// objects (deleted from outside, a lifecycle rule, a store bug) becomes visible before a
    /// read hits it. One LIST per directory (`listing` keeps the results for the whole run).
    /// If something is missing at the first comparison, re-read the needed set and HEAD each
    /// object still mentioned: a running writer drops an SST from the manifest before
    /// SlateDB's GC deletes it, so only an object that is **still** mentioned and that HEAD
    /// does not find is lost. `Ok(None)` = the Db has no manifest.
    pub async fn missing_objects(
        &self,
        range: RangeId,
        listing: &mut SstListing,
    ) -> Result<Option<Vec<Path>>> {
        let Some(wanted) = self.referenced_objects(range).await? else {
            return Ok(None);
        };
        let store = self.main().store();
        let mut gone = Vec::new();
        for p in &wanted {
            let dir = parent_dir(p);
            if !listing.dirs.contains_key(&dir) {
                let got: std::collections::BTreeSet<Path> = store
                    .list(Some(&dir))
                    .map_ok(|m| m.location)
                    .try_collect()
                    .await
                    .map_err(io)?;
                listing.dirs.insert(dir.clone(), got);
            }
            if !listing.dirs[&dir].contains(p) {
                gone.push(p.clone());
            }
        }
        if gone.is_empty() {
            return Ok(Some(gone));
        }
        let Some(again) = self.referenced_objects(range).await? else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for p in gone.into_iter().filter(|p| again.contains(p)) {
            match store.head(&p).await {
                Ok(_) => {}
                Err(OsError::NotFound { .. }) => out.push(p),
                Err(e) => return Err(io(e)),
            }
        }
        Ok(Some(out))
    }

    /// Ids of the `ranges/<id>` directories that **still have objects** on the main store and
    /// that `wanted` accepts, ascending (one LIST with a delimiter). Non-canonical names
    /// (`ranges/03`, `ranges/x`) are skipped.
    ///
    /// Stores with real directories (the SeaweedFS filer, `LocalFileSystem`) keep an empty
    /// directory after every object below it has been deleted, and LIST still returns it as
    /// a common prefix; S3 does not. So each prefix **accepted by `wanted`** gets one more
    /// LIST to see whether it still has objects — an empty directory is neither a Db nor
    /// garbage. A prefix `wanted` rejects costs no LIST: the orphan-child discard loop only
    /// asks about off-table ranges, and asking about every range would be one LIST per range
    /// per round.
    pub async fn range_dirs_where(&self, wanted: impl Fn(RangeId) -> bool) -> Result<Vec<RangeId>> {
        let (dirs, skipped) = self.range_dirs_scan_where(wanted).await?;
        match skipped.into_iter().next() {
            Some(s) => Err(StoreError::Io(s.error)),
            None => Ok(dirs),
        }
    }

    /// Like [`Self::range_dirs_where`] for every range, **per-item fault-tolerant**: a LIST of
    /// `ranges/` that fails after [`SCAN_RETRY`] is an error; a LIST of one `ranges/<id>` that
    /// fails puts that item on the skipped list — whether it is empty is not known yet.
    pub async fn range_dirs_scan(&self) -> Result<(Vec<RangeId>, Vec<ScanSkip>)> {
        self.range_dirs_scan_where(|_| true).await
    }

    async fn range_dirs_scan_where(
        &self,
        wanted: impl Fn(RangeId) -> bool,
    ) -> Result<(Vec<RangeId>, Vec<ScanSkip>)> {
        let dir = under(self.main().prefix(), "ranges");
        let listed = scan_read(|| self.main().store().list_with_delimiter(Some(&dir)))
            .await
            .map_err(io)?;
        let mut out: Vec<RangeId> = listed
            .common_prefixes
            .iter()
            .filter_map(|p| {
                let n = p.filename()?;
                let id: u32 = n.parse().ok()?;
                (id.to_string() == n).then_some(PartitionId(id))
            })
            .filter(|r| wanted(*r))
            .collect();
        out.sort_by_key(|r| r.0);
        let mut kept = Vec::with_capacity(out.len());
        let mut skipped = Vec::new();
        for r in out {
            let dir = under(self.main().prefix(), &format!("ranges/{}", r.0));
            let nonempty = scan_read(|| async {
                match self.main().store().list(Some(&dir)).next().await {
                    Some(Ok(_)) => Ok(true),
                    Some(Err(e)) => Err(e),
                    None => Ok(false),
                }
            })
            .await;
            match nonempty {
                Ok(true) => kept.push(r),
                Ok(false) => {}
                Err(e) => skipped.push(ScanSkip {
                    item: format!("ranges/{}", r.0),
                    range: Some(r),
                    session: None,
                    error: e.to_string(),
                }),
            }
        }
        Ok((kept, skipped))
    }

    /// Every `external_dbs` entry of the latest manifest of the Db of range `range`, each
    /// entry checked that its source still has a manifest and still has the checkpoint the
    /// child stands on. `Ok(None)` = the range's Db has no manifest.
    pub async fn external_db_refs(&self, range: RangeId) -> Result<Option<Vec<ExternalDbRef>>> {
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(None);
        };
        let range_of_self = range;
        let own = normalize_path(self.main().prefix().as_ref());
        let mut out = Vec::new();
        for e in m.external_dbs() {
            let range = split_range_db_path(&e.path)
                .filter(|(p, _)| *p == own)
                .map(|(_, id)| PartitionId(id));
            let want = e.final_checkpoint_id.unwrap_or(e.source_checkpoint_id);
            let status = match admin_io(
                self.admin_at(Path::from(e.path.clone()))
                    .read_manifest(None),
            )
            .await
            {
                Ok(None) => ExternalStatus::DbMissing,
                Err(err) => ExternalStatus::Unreadable(err.to_string()),
                Ok(Some(sm)) => {
                    if sm.checkpoints().iter().any(|c| c.id == want) {
                        ExternalStatus::Ok
                    } else if self.detach_pending_in(range_of_self, &m, e).await {
                        ExternalStatus::Detaching
                    } else {
                        ExternalStatus::CheckpointMissing(want.to_string())
                    }
                }
            };
            out.push(ExternalDbRef {
                path: e.path.clone(),
                range,
                status,
            });
        }
        Ok(Some(out))
    }

    /// Whether entry `entry` of the Db of range `range` (manifest `m`) is mid-detach —
    /// [`detach_pending`] over the current manifest and every manifest pinned by a checkpoint
    /// of the child. A pinned manifest that cannot be read ⇒ `false` (if it cannot be
    /// proven, report it lost).
    async fn detach_pending_in(
        &self,
        range: RangeId,
        m: &slatedb::VersionedManifest,
        entry: &ExternalDb,
    ) -> bool {
        let admin = self.admin(range.0);
        let mut pinned = Vec::new();
        for c in m.checkpoints() {
            match admin_io(admin.read_manifest(Some(c.manifest_id))).await {
                Ok(Some(pm)) => pinned.push(pm),
                _ => return false,
            }
        }
        detach_pending(
            entry,
            std::iter::once(m.external_dbs().as_slice())
                .chain(pinned.iter().map(|pm| pm.external_dbs().as_slice())),
        )
    }

    /// The ranges (with the same prefix) that the Db of `range` lists in `external_dbs` —
    /// reads only its own manifest. `Ok(None)` = the Db has no manifest. Entries with another
    /// prefix or that cannot be parsed are skipped: the caller may only use the result in the
    /// direction "has a reference ⇒ …", not in the direction "has none ⇒ …".
    pub async fn external_ranges(&self, range: RangeId) -> Result<Option<Vec<RangeId>>> {
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(None);
        };
        let own = normalize_path(self.main().prefix().as_ref());
        Ok(Some(
            m.external_dbs()
                .iter()
                .filter_map(|e| split_range_db_path(&e.path))
                .filter(|(p, _)| *p == own)
                .map(|(_, id)| PartitionId(id))
                .collect(),
        ))
    }

    /// The ranges of the cluster at prefix `source` that the Db of range `range` of **this
    /// bucket** stands on — every `external_dbs` entry in its manifest: the direct source as
    /// well as every ancestor SlateDB copies along on a clone (the set of GC barriers `fork`
    /// places on the source).
    ///
    /// `Ok(None)` = that set **cannot be proven**: the Db has no manifest, or an entry cannot
    /// be parsed into `<prefix>/ranges/<id>` or carries a prefix other than `source`. The
    /// caller then fences every range instead of guessing.
    pub async fn source_ranges_under(
        &self,
        range: RangeId,
        source: &Path,
    ) -> Result<Option<std::collections::BTreeSet<u32>>> {
        let Some(m) = admin_io(self.admin(range.0).read_manifest(None)).await? else {
            return Ok(None);
        };
        let source = normalize_path(source.as_ref());
        let mut out = std::collections::BTreeSet::new();
        for e in m.external_dbs() {
            match split_range_db_path(&e.path) {
                Some((p, id)) if p == source => {
                    out.insert(id);
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(out))
    }

    /// The WAL kind the manifest of the Db of range `range` carries. `Ok(None)` = no Db yet.
    pub async fn range_wal_kind(&self, range: RangeId) -> Result<Option<crate::WalKind>> {
        Ok(self
            .manifest_wal_name(range.0)
            .await?
            .map(|name| match name.as_deref() {
                None => crate::WalKind::SlateDb,
                Some(crate::SHARED_WAL_NAME) => crate::WalKind::Shared,
                Some(other) => crate::WalKind::Unknown(other.to_string()),
            }))
    }

    /// The same store (and WAL store), a different prefix — the destination of
    /// [`Self::fork_range`]. Poll interval, cache and background runtime revert to defaults.
    pub fn at_prefix(&self, prefix: &str) -> Result<ObjectStores> {
        let loc = |l: &StoreLocation| {
            StoreLocation::from_store(
                Arc::clone(l.store()),
                prefix,
                &format!("{} (prefix {prefix})", l.uri()),
            )
        };
        if self.is_split() {
            ObjectStores::split(loc(self.main()), loc(self.wal()))
        } else {
            Ok(ObjectStores::single(loc(self.main())))
        }
    }

    /// Clones the Db of range `range` into a Db with the same id under `dst` — **from a
    /// checkpoint**, without needing to seal the source.
    ///
    /// **Writes to the source:** a temporary checkpoint (`lifetime`) and the final checkpoint
    /// SlateDB pins for the copy (never expires) — the copy reads the source's SSTs through
    /// `external_dbs`, so the source must keep them. Deleting the copy does not remove the
    /// final checkpoint: a space leak, nothing lost.
    ///
    /// A range using the shared WAL: the copy sees **exactly what had reached L0 at snapshot
    /// time** — frames still in the source node's log do not come along (the log lives
    /// outside the Db, and reading it would mean touching a running node). A range using the
    /// native WAL: SlateDB copies the WAL files the checkpoint still references.
    pub async fn fork_range(
        &self,
        range: RangeId,
        dst: &ObjectStores,
        lifetime: Duration,
    ) -> Result<()> {
        let wal_name = self.manifest_wal_name(range.0).await?.ok_or_else(|| {
            StoreError::Corrupt(format!("range {}: the Db has no manifest", range.0))
        })?;
        let cp = admin_io(
            self.admin(range.0)
                .create_detached_checkpoint(&CheckpointOptions {
                    lifetime: Some(lifetime),
                    source: None,
                    name: Some("cairn-ctl-fork".into()),
                }),
        )
        .await?;
        let mut builder =
            crate::config::admin_builder(dst.range_path(range.0), Arc::clone(dst.main().store()));
        if dst.is_split() {
            builder = builder.with_wal_object_store(Arc::clone(dst.wal().store()));
        }
        if wal_name.as_deref() == Some(crate::SHARED_WAL_NAME) {
            builder = builder.with_wal_admin(Arc::new(ForkWalAdmin));
        } else if let Some(other) = wal_name {
            return Err(StoreError::Corrupt(format!(
                "range {}: unknown WAL kind {other:?}",
                range.0
            )));
        }
        builder
            .build()
            .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
                self.range_path(range.0),
                cp.id,
            ))
            .build()
            .await
            .map_err(StoreError::from_slate)?;
        Ok(())
    }
}

/// The `WalAdmin` of a `fork` copy for a range using the shared WAL: it carries the shared
/// WAL name (SlateDB requires the copy to have the same WAL kind as the source) and copies
/// no frames — see [`ObjectStores::fork_range`]. It differs from
/// [`crate::nodewal::SharedWalAdmin`] in that it does **not** refuse a source that still has
/// WAL segments: the copy deliberately takes only what is already in L0.
struct ForkWalAdmin;

#[async_trait]
impl WalAdmin for ForkWalAdmin {
    fn garbage_collector(&self, _path: &Path) -> Arc<dyn WalGc> {
        Arc::new(crate::nodewal::NoopWalGc)
    }

    fn wal_name(&self) -> Option<&str> {
        Some(crate::SHARED_WAL_NAME)
    }

    async fn delete_wal(
        &self,
        _path: &Path,
        _dry_run: bool,
    ) -> std::result::Result<Vec<String>, WalError> {
        Ok(Vec::new())
    }

    async fn is_empty(
        &self,
        _path: &Path,
        _replay_after: u64,
        _last_seen: u64,
    ) -> std::result::Result<bool, WalError> {
        Ok(true)
    }

    async fn clone_wal(
        &self,
        _from_path: &Path,
        from_manifest: slatedb::VersionedManifest,
        _to_path: &Path,
    ) -> std::result::Result<(u64, u64), WalError> {
        Ok((
            from_manifest.replay_after_wal_id(),
            from_manifest.next_wal_sst_id().saturating_sub(1),
        ))
    }
}

/// Copies object `rel` (under the prefix) verbatim from `src` to `dst` if it exists.
/// Returns `true` when it was copied.
pub async fn copy_object(src: &StoreLocation, dst: &StoreLocation, rel: &str) -> Result<bool> {
    let from = under(src.prefix(), rel);
    let raw = match src.store().get(&from).await {
        Ok(g) => g.bytes().await.map_err(io)?,
        Err(OsError::NotFound { .. }) => return Ok(false),
        Err(e) => return Err(io(e)),
    };
    // An ambiguous `Create` is resolved by reading back (rule 7): if the copy reached the
    // destination, `fork` carries on. A **different** object already at the destination is
    // still an error, as before.
    let to = under(dst.prefix(), rel);
    match crate::cas::put_resolving(dst.store(), &to, raw, PutMode::Create).await {
        Ok(crate::cas::Cas::Written(_)) => Ok(true),
        Ok(crate::cas::Cas::Lost) => Err(StoreError::Io(format!(
            "{to} already exists at the destination with different content"
        ))),
        Err(e) => Err(io(e)),
    }
}

/// Every object under `dir` (relative to the prefix), as relative paths.
pub async fn list_relative(loc: &StoreLocation, dir: &str) -> Result<Vec<String>> {
    let base = normalize_path(loc.prefix().as_ref());
    let listed: Vec<_> = loc
        .store()
        .list(Some(&under(loc.prefix(), dir)))
        .try_collect()
        .await
        .map_err(io)?;
    Ok(listed
        .iter()
        .filter_map(|m| {
            let p = m.location.as_ref();
            if base.is_empty() {
                Some(p.to_string())
            } else {
                p.strip_prefix(&format!("{base}/")).map(str::to_string)
            }
        })
        .collect())
}

/// Whether the prefix has any object (one LIST, stopping at the first object).
pub async fn prefix_is_empty(loc: &StoreLocation) -> Result<bool> {
    let mut s = loc.store().list(Some(loc.prefix()));
    Ok(s.try_next().await.map_err(io)?.is_none())
}

/// The full path of object `rel` under the prefix of `loc`.
pub fn object_path(loc: &StoreLocation, rel: &str) -> Path {
    under(loc.prefix(), rel)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule 7: the audit record and the `fork` copy (`Create`) were written but the reply
    /// says otherwise. Must see: exactly **one** audit record (not two, under two nonces),
    /// and copying the object does not make `fork` fail.
    #[tokio::test]
    async fn ambiguous_creates_are_resolved_by_reread() {
        use {
            crate::testkit::{Ambiguity, FaultyStore, Rule},
            futures::TryStreamExt,
        };
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let prefix = Path::from("p");
            let entry = AuditEntry {
                who: "op".into(),
                at_ms: 7,
                command: "cairn-ctl x".into(),
                object: "cluster.conf".into(),
                before: String::new(),
                after: String::new(),
            };
            faulty.add_rule(Rule::ambiguous_put("audit/ctl/", reply));
            let path = write_audit(&store, &prefix, &entry)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: audit written but reported an error: {e}"));
            let all: Vec<_> = store
                .list(Some(&Path::from("p/audit/ctl")))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(all.len(), 1, "{reply:?}: {all:?}");
            assert_eq!(all[0].location, path);

            let src = StoreLocation::from_store(store.clone(), "p", "memory://");
            let dst = StoreLocation::from_store(store.clone(), "q", "memory://");
            faulty.add_rule(Rule::ambiguous_put("q/", reply));
            let rel = path.as_ref().strip_prefix("p/").unwrap().to_string();
            assert!(
                copy_object(&src, &dst, &rel)
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: written copy reported an error: {e}")),
                "{reply:?}"
            );
        }
    }

    #[test]
    fn audit_header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy("audit/ctl/<ts>").map(|p| p.header),
            Some(AUDIT_HEADER)
        );
    }

    #[test]
    fn audit_entry_keeps_fields_on_one_line() {
        let e = AuditEntry {
            who: "op@host".into(),
            at_ms: 7,
            command: "intent clear 4\n--apply".into(),
            object: "cluster.conf".into(),
            before: "a=1\nb=2\n".into(),
            after: "a=1\n".into(),
        };
        let text = e.encode();
        assert!(text.starts_with(
            "cairnflow-ctl-audit/1\nwho=op@host\nat_ms=7\ncommand=intent clear 4 --apply\n"
        ));
        assert!(
            text.contains("[before]\na=1\nb=2\n[after]\na=1\n"),
            "{text}"
        );
    }
}
