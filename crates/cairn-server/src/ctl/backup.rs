// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl backup` / `restore`: a **self-contained** backup to another store
//! (another provider), and rebuilding the cluster from the copy.
//!
//! **Why not `fork`.** A fork is a SlateDB clone: its Db has `external_dbs` pointing at
//! the source Db. Copying it alone to another provider is missing SSTs; losing the
//! source loses the copy too.
//!
//! **Layout at the destination** (`--to <uri>`, under the uri's prefix):
//!
//! - `<id>/data/<rel>` — the cluster's objects, `<rel>` being the path under the source
//!   prefix;
//! - `<id>/BACKUP` — the backup manifest (every object + size + SHA-256), written **last**
//!   with `Create`. Without it the copy is partial: `restore` refuses it, `prune` cleans
//!   it once old enough.
//!
//! `<id>` = `<start ms, 13 digits>-<4 hex>`: lexical order is time order.
//!
//! **Restoring is copying back**, not pointing a cell straight at the copy:
//! `external_dbs` in the SlateDB manifest records absolute paths (including the source
//! prefix), so the rebuilt cluster must live at **the same source prefix** on the new
//! store. `restore` checks the size and hash of every object, writes `format` last — a
//! half-copied destination has `ranges/` without `format`, and nodes refuse to open it
//! (`format::ensure`).
//!
//! Only the part of each range already in L0 (see [`cairn_store::backup`]); owner
//! records are scrubbed of their lease like `fork`. Not copied: sessions, incarnation
//! markers, shared WAL logs, GC fences, the cluster secret, node markers — see
//! [`skip_reason`].

use {
    super::{now_ms, Ctl, CtlError, CtlResult},
    cairn_store::{
        backup::{copy_hashed, drop_temporary_checkpoints, read_checked, Copied},
        gc_barrier::{self, GcBarrier, Protects},
        inspect::{self, AuditEntry},
        retry::{self, Policy},
        OwnerRecord, SessionId, StoreError, StoreLocation,
    },
    futures::{StreamExt, TryStreamExt},
    serde::Serialize,
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode},
    std::{collections::BTreeSet, sync::Arc, time::Duration},
};

/// Header of the backup manifest.
pub const BACKUP_HEADER: &str = "cairnflow-backup/1";
/// Name of the backup manifest under `<id>/`.
pub const BACKUP_MANIFEST: &str = "BACKUP";
/// The checkpoint lifetime `backup` sets on each source Db — outlives the copy; dying
/// midway lets it expire on its own.
pub const BACKUP_CHECKPOINT_LIFETIME: Duration = Duration::from_secs(6 * 3600);
/// TTL of the GC fence `backup` holds on the source; the backup renews it every
/// fifth of the TTL while it runs, so a backup that dies stops holding the GC within one
/// TTL.
pub const BACKUP_FENCE_TTL: Duration = Duration::from_secs(10 * 60);
/// A partial copy (no manifest) older than this is cleaned by `prune`.
pub const STALE_PARTIAL: Duration = Duration::from_secs(24 * 3600);
/// How many times `backup` retries when the range table changes during a copy pass.
pub const BACKUP_ATTEMPTS: usize = 3;

static BACKUP_COPY: retry::Op = retry::Op::new("ctl.backup_copy");
/// How many objects `backup` copies at once. One at a time, a copy is bound by the round
/// trip of the stores (~1–2k objects a minute against S3), and a busy cluster has
/// hundreds of thousands of objects; each copy holds one whole object in memory, so this
/// also bounds memory (×the largest SST).
pub const BACKUP_COPY_CONCURRENCY: usize = 16;
/// One object: 5 attempts, backing off 200 ms → 5 s.
const COPY_POLICY: Policy = Policy::new(Duration::from_millis(200), Duration::from_secs(5), 5);

/// The GC fence `backup` holds on the source while it runs.
///
/// Set for one [`BACKUP_FENCE_TTL`] and renewed every fifth of it by a task that dies with
/// the backup ([`Drop`] aborts it; a killed process takes it along). A backup that dies —
/// SIGKILL, a cron timeout, a crash — stops holding the GC within one TTL, instead of
/// holding every range for [`crate::gc::MAX_BARRIER_TTL`].
///
/// **Lapse.** The renewer marks the fence lapsed when it gets within a fifth of the TTL of
/// the last expiry it wrote without a renewal having succeeded (the GC nodes' clocks may run
/// a little ahead of ours). From then on GC may have run, so the copy is not trusted:
/// [`Fence::check`] fails, before every object and before the backup manifest. A later
/// renewal that succeeds does not clear the mark.
struct Fence {
    id: String,
    lapsed: Arc<std::sync::atomic::AtomicBool>,
    renewer: tokio::task::JoinHandle<()>,
}

impl Fence {
    async fn hold(
        store: &Arc<dyn ObjectStore>,
        prefix: &Path,
        id: String,
        template: GcBarrier,
        ttl: Duration,
    ) -> CtlResult<Fence> {
        let ttl_ms = ttl.as_millis() as u64;
        let mut until = now_ms() + ttl_ms;
        gc_barrier::put_barrier(
            store,
            prefix,
            &id,
            &GcBarrier {
                expires_ms: until,
                ..template.clone()
            },
        )
        .await?;
        let lapsed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let renewer = {
            let (store, prefix, id, lapsed) = (
                Arc::clone(store),
                prefix.clone(),
                id.clone(),
                Arc::clone(&lapsed),
            );
            let margin_ms = ttl_ms / 5;
            tokio::spawn(async move {
                loop {
                    cairn_store::pace::beat(ttl / 5).await;
                    if now_ms() + margin_ms >= until {
                        lapsed.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    // The expiry counts from before the PUT: a slow PUT shortens the fence,
                    // never lengthens it.
                    let next = now_ms() + ttl_ms;
                    let b = GcBarrier {
                        expires_ms: next,
                        ..template.clone()
                    };
                    match renew_fence(&store, &prefix, &id, &b).await {
                        Ok(()) => until = next,
                        Err(e) => tracing::warn!(
                            fence = %id,
                            error = %e,
                            "backup: cannot renew the GC fence on the source"
                        ),
                    }
                }
            })
        };
        Ok(Fence {
            id,
            lapsed,
            renewer,
        })
    }

    /// `Err` once the fence may have expired: GC may have deleted what the copy reads.
    fn check(&self) -> CtlResult<()> {
        self.probe().check()
    }

    /// An owned handle on the lapse mark, for copies running concurrently.
    fn probe(&self) -> FenceProbe {
        FenceProbe {
            id: self.id.clone(),
            lapsed: Arc::clone(&self.lapsed),
        }
    }
}

/// See [`Fence::check`].
#[derive(Clone)]
struct FenceProbe {
    id: String,
    lapsed: Arc<std::sync::atomic::AtomicBool>,
}

impl FenceProbe {
    fn check(&self) -> CtlResult<()> {
        if self.lapsed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(CtlError::Store(StoreError::Io(format!(
                "the GC fence {} on the source lapsed (renewals failed) — GC may have deleted \
                 objects the copy still had to read; run backup again",
                self.id
            ))));
        }
        Ok(())
    }
}

/// One object of the copy, every argument owned (copies run concurrently): `from` →
/// `<data>/<rel>`. `Ok(None)` = `from` is gone.
async fn copy_owned(
    src: Arc<dyn ObjectStore>,
    dst: Arc<dyn ObjectStore>,
    from: Path,
    data: Path,
    rel: String,
    fence: FenceProbe,
) -> CtlResult<Option<(String, Copied)>> {
    fence.check()?;
    let got = copy_one(&src, &from, &dst, &join(&data, &rel)).await?;
    Ok(got.map(|got| (rel, got)))
}

/// One object of `restore`, every argument owned (copies run concurrently): read `from`,
/// check its size and hash against `want`, write it to `to` with `Create`.
async fn restore_one(
    src: Arc<dyn ObjectStore>,
    dst: Arc<dyn ObjectStore>,
    from: Path,
    to: Path,
    rel: String,
    want: Copied,
    id: String,
) -> CtlResult<()> {
    let raw = retry::run(&BACKUP_COPY, &COPY_POLICY, retry::Never, || {
        read_checked(&src, &from, &want)
    })
    .await?
    .ok_or_else(|| {
        CtlError::Bad(format!(
            "backup {id} is missing {rel} listed in its manifest — target is partial, delete it \
             and run again"
        ))
    })?;
    match cairn_store::cas::put_resolving(&dst, &to, raw, PutMode::Create).await {
        Ok(cairn_store::cas::Cas::Written(_)) => Ok(()),
        Ok(cairn_store::cas::Cas::Lost) => Err(CtlError::Refused(format!(
            "{to} already exists at the target"
        ))),
        Err(e) => Err(store_err(e)),
    }
}

impl Drop for Fence {
    fn drop(&mut self) {
        self.renewer.abort();
    }
}

/// One renewal of the fence (failpoint `ctl::backup::renew_fence`).
async fn renew_fence(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &str,
    b: &GcBarrier,
) -> cairn_store::Result<()> {
    cairn_store::failpoint!("ctl::backup::renew_fence", err);
    gc_barrier::put_barrier(store, prefix, id, b).await
}

/// The `restore` destination is empty — except for operations reports
/// `ops-reports/<id>`: the report of an earlier failed `restore --report-to-bucket`
/// lives at the destination itself, and must not block the retry. Copies do not carry
/// `ops-reports/` ([`skip_reason`]), so nothing overwrites them.
async fn empty_but_reports(loc: &StoreLocation) -> CtlResult<bool> {
    let reports = format!(
        "{}/",
        inspect::object_path(loc, cairn_store::ops_report::DIR).as_ref()
    );
    let mut s = loc.store().list(Some(loc.prefix()));
    while let Some(m) = s.try_next().await.map_err(store_err)? {
        if !m.location.as_ref().starts_with(&reports) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Why `rel` (a path under the source prefix) does not go into the copy as an ordinary
/// object. `None` = copied verbatim.
pub fn skip_reason(rel: &str) -> Option<&'static str> {
    const SKIP: &[(&str, &str)] = &[
        ("ranges/", "range Db: copied by the checkpoint manifest"),
        ("owners/", "owner record: copied with the lease scrubbed"),
        (
            "sessions/",
            "session of the source node — the new cluster opens its own sessions",
        ),
        ("incarnations/", "incarnation marker of the source node"),
        ("nodewal/", "shared WAL log: the backup only takes the part in L0"),
        (
            "gc-barriers/",
            "GC fences of the source (including the fence of this backup itself)",
        ),
        ("control/probe/", "gate probe object"),
        (
            "gc-swept",
            "swept-tombstone ledger of the source — an optimization of its D-d; the restored cluster \
             rebuilds its own on its first D-d round",
        ),
        (
            "cluster-history/",
            "range table history of the source — it may hold versions newer \
             than the cluster.conf in the backup; the restored cluster writes its own",
        ),
        ("control/coordinator", "coordinator role of the source node"),
        ("shedding", "marker of a node that stepped down for being slow"),
        (
            "ops-reports/",
            "ops reports of the source cell — not cluster state; restore writes \
             its own report",
        ),
        (
            "auth/cluster-secret",
            "the cluster secret does not leave the secret store",
        ),
    ];
    // An entry ending in `/` is a directory; otherwise it is exactly one object.
    SKIP.iter()
        .find(|(p, _)| {
            if p.ends_with('/') {
                rel.starts_with(p)
            } else {
                rel == *p
            }
        })
        .map(|(_, why)| *why)
}

/// The result of `backup`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupReport {
    pub id: String,
    pub to: String,
    pub ranges: Vec<u32>,
    /// `(range, ms)`: when each range's checkpoint was taken — see [`BackupManifest::as_of`].
    pub as_of: Vec<(u32, u64)>,
    pub objects: usize,
    pub bytes: u64,
    pub warnings: Vec<String>,
    pub audit: Option<String>,
}

/// A copy at the destination, as `backup list` sees it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupEntry {
    pub id: String,
    /// Has a backup manifest.
    pub complete: bool,
    pub started_ms: u64,
    pub objects: Option<usize>,
    pub bytes: Option<u64>,
}

/// The result of `restore`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreReport {
    pub id: String,
    pub from: String,
    pub objects: usize,
    pub bytes: u64,
    pub audit: Option<String>,
}

/// The result of `backup prune`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PruneReport {
    pub applied: bool,
    pub keep: Vec<String>,
    pub delete: Vec<String>,
    pub objects_deleted: usize,
    pub audit: Option<String>,
}

/// The backup manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupManifest {
    pub id: String,
    pub source: String,
    pub source_prefix: String,
    pub started_ms: u64,
    pub finished_ms: u64,
    pub ranges: Vec<u32>,
    /// `(range, Unix ms)`: when each range's checkpoint was taken. A range's copy is the
    /// part already in L0 up to that mark — at most `frame_age_flush` (60 s by default)
    /// behind the last commit; ranges differ from each other, there is no common cut.
    pub as_of: Vec<(u32, u64)>,
    /// `(rel, size + hash)`, in `rel` order.
    pub objects: Vec<(String, Copied)>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

impl BackupManifest {
    pub fn bytes(&self) -> u64 {
        self.objects.iter().map(|(_, c)| c.size).sum()
    }

    /// Text: the header, `k=v` keys, then one line per object
    /// `o <sha256> <size> <rel>`. `rel` contains no newline (an object path).
    pub fn encode(&self) -> String {
        let mut out = format!(
            "{BACKUP_HEADER}\nid={}\nsource={}\nsource_prefix={}\nstarted_ms={}\nfinished_ms={}\nranges={}\nas_of={}\nobjects={}\nbytes={}\n",
            self.id,
            self.source,
            self.source_prefix,
            self.started_ms,
            self.finished_ms,
            self.ranges
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.as_of
                .iter()
                .map(|(r, ms)| format!("{r}:{ms}"))
                .collect::<Vec<_>>()
                .join(","),
            self.objects.len(),
            self.bytes(),
        );
        for (rel, c) in &self.objects {
            out.push_str(&format!("o {} {} {rel}\n", hex(&c.sha256), c.size));
        }
        out
    }

    /// Strict read: an unknown header, a broken line, or object/byte counts that do not
    /// match ⇒ an error (a copy that cannot be used says so, no guessing).
    pub fn decode(raw: &str) -> Result<Self, String> {
        let mut lines = raw.lines();
        if lines.next() != Some(BACKUP_HEADER) {
            return Err(format!("header is not {BACKUP_HEADER}"));
        }
        let mut m = BackupManifest {
            id: String::new(),
            source: String::new(),
            source_prefix: String::new(),
            started_ms: 0,
            finished_ms: 0,
            ranges: Vec::new(),
            as_of: Vec::new(),
            objects: Vec::new(),
        };
        let (mut want_objects, mut want_bytes) = (None, None);
        for line in lines {
            if let Some(rest) = line.strip_prefix("o ") {
                let mut it = rest.splitn(3, ' ');
                let (Some(sha), Some(size), Some(rel)) = (it.next(), it.next(), it.next()) else {
                    return Err(format!("malformed object line: {line:?}"));
                };
                let sha256 = unhex32(sha).ok_or_else(|| format!("malformed hash: {line:?}"))?;
                let size = size
                    .parse()
                    .map_err(|_| format!("malformed size: {line:?}"))?;
                m.objects.push((rel.to_string(), Copied { size, sha256 }));
                continue;
            }
            let Some((k, v)) = line.split_once('=') else {
                return Err(format!("malformed line: {line:?}"));
            };
            let num = |v: &str| {
                v.parse::<u64>()
                    .map_err(|_| format!("malformed {k}=: {v:?}"))
            };
            match k {
                "id" => m.id = v.into(),
                "source" => m.source = v.into(),
                "source_prefix" => m.source_prefix = v.into(),
                "started_ms" => m.started_ms = num(v)?,
                "finished_ms" => m.finished_ms = num(v)?,
                "ranges" => {
                    m.ranges = v
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(|s| s.parse().map_err(|_| format!("malformed ranges=: {v:?}")))
                        .collect::<Result<_, _>>()?
                }
                "as_of" => {
                    m.as_of = v
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(|s| {
                            s.split_once(':')
                                .and_then(|(r, ms)| Some((r.parse().ok()?, ms.parse().ok()?)))
                                .ok_or_else(|| format!("malformed as_of=: {v:?}"))
                        })
                        .collect::<Result<_, _>>()?
                }
                "objects" => want_objects = Some(num(v)? as usize),
                "bytes" => want_bytes = Some(num(v)?),
                _ => return Err(format!("unknown key {k:?}")),
            }
        }
        if want_objects != Some(m.objects.len()) || want_bytes != Some(m.bytes()) {
            return Err("object/byte counts do not match the list — manifest truncated?".into());
        }
        Ok(m)
    }
}

fn join(prefix: &Path, rel: &str) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(rel)
    } else {
        Path::from(format!("{}/{rel}", prefix.as_ref()))
    }
}

/// The `rel` of `p` under `prefix` (`None` when `p` lies outside).
fn rel_of(prefix: &Path, p: &Path) -> Option<String> {
    let base = prefix.as_ref();
    if base.is_empty() {
        return Some(p.as_ref().to_string());
    }
    p.as_ref()
        .strip_prefix(base)
        .and_then(|r| r.strip_prefix('/'))
        .map(str::to_string)
}

/// Whether `to` lies inside `src` or contains it: the same object store (the same
/// `Arc`, or the same root uri) and nested prefixes. Backing up into itself would make
/// the source's LIST see the copy being written.
fn nested(src: &StoreLocation, to: &StoreLocation) -> bool {
    let inside = |a: &str, b: &str| {
        let (a, b) = (a.trim_end_matches('/'), b.trim_end_matches('/'));
        // An empty prefix is the whole store: it contains every other prefix.
        a.is_empty()
            || b.is_empty()
            || a == b
            || a.starts_with(&format!("{b}/"))
            || b.starts_with(&format!("{a}/"))
    };
    let same_store = Arc::ptr_eq(src.store(), to.store());
    if same_store {
        return inside(src.prefix().as_ref(), to.prefix().as_ref());
    }
    // Stores built separately from two uris: compare the uris (`s3://bucket/prefix`,
    // `file:///dir`).
    !src.uri().starts_with("memory://") && inside(src.uri(), to.uri())
}

/// The range table under the prefix (relative path).
const CLUSTER_CONF: &str = "cluster.conf";

/// A new id for a copy starting at `started_ms`.
fn new_backup_id(started_ms: u64) -> String {
    let mut tag = [0u8; 2];
    cairn_store::entropy::fill(&mut tag);
    format!("{started_ms:013}-{}", hex(&tag))
}

/// Writes `raw` to `to`, returning the size and hash like [`copy_one`].
async fn put_hashed(dst: &Arc<dyn ObjectStore>, to: &Path, raw: bytes::Bytes) -> CtlResult<Copied> {
    use sha2::Digest;
    let copied = Copied {
        size: raw.len() as u64,
        sha256: sha2::Sha256::digest(&raw).into(),
    };
    dst.put(to, raw.into()).await.map_err(store_err)?;
    Ok(copied)
}

fn store_err(e: impl std::fmt::Display) -> CtlError {
    CtlError::Store(StoreError::Io(e.to_string()))
}

/// Copies one object, retrying per [`COPY_POLICY`].
async fn copy_one(
    src: &Arc<dyn ObjectStore>,
    from: &Path,
    dst: &Arc<dyn ObjectStore>,
    to: &Path,
) -> CtlResult<Option<Copied>> {
    Ok(retry::run(&BACKUP_COPY, &COPY_POLICY, retry::Never, || {
        copy_hashed(src, from, dst, to)
    })
    .await?)
}

/// Every `<id>` directly under `loc`'s prefix (one LIST with a delimiter).
async fn list_ids(loc: &StoreLocation) -> CtlResult<Vec<String>> {
    let prefix = (!loc.prefix().as_ref().is_empty()).then(|| loc.prefix());
    let listed = loc
        .store()
        .list_with_delimiter(prefix)
        .await
        .map_err(store_err)?;
    let mut ids: Vec<String> = listed
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename().map(str::to_string))
        .filter(|id| started_ms_of(id).is_some())
        .collect();
    ids.sort();
    Ok(ids)
}

fn started_ms_of(id: &str) -> Option<u64> {
    let (ms, tag) = id.split_once('-')?;
    (ms.len() == 13 && tag.len() == 4 && tag.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| ms.parse().ok())
        .flatten()
}

async fn read_manifest(loc: &StoreLocation, id: &str) -> CtlResult<Option<BackupManifest>> {
    let path = join(loc.prefix(), &format!("{id}/{BACKUP_MANIFEST}"));
    let raw = match loc.store().get(&path).await {
        Ok(g) => g.bytes().await.map_err(store_err)?,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(store_err(e)),
    };
    let m = BackupManifest::decode(&String::from_utf8_lossy(&raw))
        .map_err(|e| CtlError::Bad(format!("backup manifest {path}: {e}")))?;
    if m.id != id {
        return Err(CtlError::Bad(format!(
            "backup manifest {path} carries id {:?}",
            m.id
        )));
    }
    Ok(Some(m))
}

impl Ctl {
    /// `backup --to <uri>`: a self-contained backup of the cluster to `to`. See the module.
    ///
    /// 1. a GC fence `backup-<id>` (`all`) on the source, then a checkpoint of every range
    ///    Db in the table;
    /// 2. copy every object the checkpoints reference, every cairn object (except
    ///    [`skip_reason`]), and the owner records scrubbed of their lease;
    /// 3. write the backup manifest **last**;
    /// 4. remove the checkpoints and the fence (a failure at this step is only a warning:
    ///    the checkpoints expire after [`BACKUP_CHECKPOINT_LIFETIME`], the fence — renewed
    ///    while the backup runs, see [`Fence`] — within [`BACKUP_FENCE_TTL`]).
    ///
    /// Deletes nothing on the source. Dying midway (failpoint `ctl::backup::mid_copy`)
    /// leaves `<id>/data/…` without a manifest — a partial copy.
    pub async fn backup(
        &self,
        to: &StoreLocation,
        who: &str,
        command: &str,
    ) -> CtlResult<BackupReport> {
        if self.stores.is_split() {
            return Err(CtlError::Bad(
                "backup does not support clusters with a separate WAL store (--wal-store) yet"
                    .into(),
            ));
        }
        let main = self.stores.main();
        let started_ms = now_ms();
        let id = new_backup_id(started_ms);
        let root = join(to.prefix(), &id);
        let root_loc = StoreLocation::from_store(Arc::clone(to.store()), root.clone(), to.uri());
        if nested(main, to) {
            return Err(CtlError::Bad(format!(
                "target {} is inside (or contains) the source cluster {} — back up to a different \
                 store",
                to.uri(),
                main.uri()
            )));
        }
        if !inspect::prefix_is_empty(&root_loc).await? {
            return Err(CtlError::Refused(format!(
                "target {root} already has objects"
            )));
        }
        // The fence **before** reading the table (like `fork`): no Db the copy is about to
        // stand on is deleted by D-d between reading the table and the checkpoint.
        let barrier_id = format!("backup-{id}");
        let fence = Fence::hold(
            main.store(),
            main.prefix(),
            barrier_id.clone(),
            GcBarrier {
                holder: who.to_string(),
                protects: Protects::All,
                expires_ms: 0,
                reason: format!("cairn-ctl backup {id}: copying"),
            },
            self.backup_fence_ttl,
        )
        .await?;
        let mut checkpoints = Vec::new();
        let mut warnings = Vec::new();
        // The range table changing during a pass (a split/merge commit) ⇒ the pass is
        // dropped — a partial copy without a manifest, cleaned by `prune` — and runs again
        // with a new id, at most [`BACKUP_ATTEMPTS`] times.
        let mut id = id;
        let mut r = Err(CtlError::Refused(format!(
            "the range table changed during each of {BACKUP_ATTEMPTS} backup attempts (a split \
             or merge kept committing) — run backup again when range operations are quiet"
        )));
        for attempt in 1..=BACKUP_ATTEMPTS {
            if attempt > 1 {
                id = new_backup_id(started_ms.max(now_ms()));
                let root = join(to.prefix(), &id);
                let root_loc =
                    StoreLocation::from_store(Arc::clone(to.store()), root.clone(), to.uri());
                if !inspect::prefix_is_empty(&root_loc).await? {
                    r = Err(CtlError::Refused(format!(
                        "target {root} already has objects"
                    )));
                    break;
                }
            }
            match self
                .backup_copy(to, &id, started_ms, &mut checkpoints, &fence)
                .await
            {
                Ok(Some(done)) => {
                    r = Ok(done);
                    break;
                }
                Ok(None) => warnings.push(format!(
                    "attempt {attempt}: the range table changed while copying (a split or merge \
                     committed) — partial copy {id} has no backup manifest and is never used \
                     (backup prune removes it); retrying"
                )),
                Err(e) => {
                    r = Err(e);
                    break;
                }
            }
        }
        // Step 4, even on failure: remove what we set. On failure, let it expire.
        for (range, cp) in &checkpoints {
            if let Err(e) = self.stores.backup_drop_checkpoint(*range, cp).await {
                warnings.push(format!(
                    "range {range}: cannot remove checkpoint {} ({e}) — it expires on its own \
                     after {:?}",
                    cp.id, BACKUP_CHECKPOINT_LIFETIME
                ));
            }
        }
        // Stop renewing before removing: a renewal after the removal would set it again.
        drop(fence);
        if let Err(e) = gc_barrier::delete_barrier(main.store(), main.prefix(), &barrier_id).await {
            warnings.push(format!(
                "cannot remove GC fence {barrier_id} ({e}) — it expires on its own"
            ));
        }
        let (manifest, mut more) = r?;
        warnings.append(&mut more);
        let entry = AuditEntry {
            who: who.to_string(),
            at_ms: now_ms(),
            command: command.to_string(),
            object: format!("ranges/*/manifest (checkpoint for backup {id})"),
            before: String::new(),
            after: format!(
                "backup {id} → {}: {} objects, {} bytes, ranges {:?}",
                to.uri(),
                manifest.objects.len(),
                manifest.bytes(),
                manifest.ranges
            ),
        };
        let audit = inspect::write_audit(main.store(), main.prefix(), &entry)
            .await
            .map(|p| p.to_string())
            .map_err(|e| warnings.push(format!("cannot write audit/ctl: {e}")))
            .ok();
        Ok(BackupReport {
            id,
            to: to.uri().to_string(),
            ranges: manifest.ranges.clone(),
            as_of: manifest.as_of.clone(),
            objects: manifest.objects.len(),
            bytes: manifest.bytes(),
            warnings,
            audit,
        })
    }

    /// Steps 1–3 of [`Self::backup`]; `checkpoints` receives every checkpoint set, even
    /// on a failure midway. `Ok(None)` = the range table changed during the pass: the
    /// pass is dropped before the backup manifest, the caller runs again.
    ///
    /// **One table snapshot for the whole pass**: the `cluster.conf` bytes are read once
    /// at the start (table V); checkpoints are exactly V's ranges, and the copy's
    /// `cluster.conf` is written from those very bytes — the live copy is not copied.
    /// After copying the owner records (read live), the version is read again: different
    /// from V ⇒ a split/merge committed midway, and the owner records and other objects
    /// may belong to a later table ⇒ drop the pass.
    async fn backup_copy(
        &self,
        to: &StoreLocation,
        id: &str,
        started_ms: u64,
        checkpoints: &mut Vec<(u32, slatedb::CheckpointCreateResult)>,
        fence: &Fence,
    ) -> CtlResult<Option<(BackupManifest, Vec<String>)>> {
        let main = self.stores.main();
        let src = main.store();
        let dst = to.store();
        let data = join(to.prefix(), &format!("{id}/data"));
        let mut warnings = Vec::new();
        let (conf, table) = self.table_snapshot().await?;
        // 1. Checkpoint every range Db in the table (the fence is set: no Db is deleted by
        // D-d between reading the table and copying).
        let mut ranges = Vec::new();
        let mut as_of = Vec::new();
        let mut db_objects: BTreeSet<Path> = BTreeSet::new();
        // Per range: the expiring checkpoints its copied manifest names (module doc of
        // `cairn_store::backup`) — removed from the copy after step 2.
        let mut temporary = Vec::new();
        // A child out of table V: the copy carries only V's ranges, so it never carries the
        // child's Db — whether the op rolled back already or is still promised by an intent
        // (the restored cluster can only roll it back: forward needs the child Db). The clone
        // checkpoints of such a child leave the copy.
        let never_committed = |c: cairn_store::PartitionId| !table.contains(c);
        for r in table.partitions() {
            match self
                .stores
                .backup_range_db(r.0, BACKUP_CHECKPOINT_LIFETIME, never_committed)
                .await?
            {
                Some(copy) => {
                    checkpoints.push((r.0, copy.checkpoint));
                    db_objects.extend(copy.objects);
                    temporary.push((r.0, copy.temporary));
                    ranges.push(r.0);
                    as_of.push((r.0, copy.as_of_ms));
                }
                None => warnings.push(format!(
                    "range {}: no Db yet — the rebuilt cluster opens it as a new range",
                    r.0
                )),
            }
            // The gap between two ranges' checkpoints: the copy is not a consistent cut of
            // the whole cluster — tests build lost inter-range messages here.
            cairn_store::failpoint!("ctl::backup::after_range", err);
        }
        // 2. Copy. Db objects the checkpoints reference must exist: a missing one is an error.
        let mut objects: Vec<(String, Copied)> = Vec::new();
        let probe = fence.probe();
        let mut wanted = Vec::with_capacity(db_objects.len());
        for path in &db_objects {
            let rel = rel_of(main.prefix(), path).ok_or_else(|| {
                CtlError::Bad(format!(
                    "{path} is outside the source prefix {}",
                    main.prefix()
                ))
            })?;
            wanted.push((path.clone(), rel));
        }
        let mut copies = futures::stream::iter(wanted.into_iter().map(|(from, rel)| {
            let missing = from.clone();
            let copy = copy_owned(
                Arc::clone(src),
                Arc::clone(dst),
                from,
                data.clone(),
                rel,
                probe.clone(),
            );
            async move {
                copy.await?.ok_or_else(|| {
                    CtlError::Store(StoreError::Corrupt(format!(
                        "{missing}: referenced by a checkpoint but the object is missing"
                    )))
                })
            }
        }))
        .buffer_unordered(BACKUP_COPY_CONCURRENCY);
        // The first error stops the copy; dropping the stream cancels the copies in flight
        // (a partial copy: no backup manifest).
        while let Some(done) = copies.next().await {
            objects.push(done?);
            if objects.len() == 1 {
                // Dying while copying: the destination has objects but no backup
                // manifest.
                cairn_store::failpoint!("ctl::backup::mid_copy", err);
            }
        }
        drop(copies);
        // The copy's manifests name only what the copy has: drop the source's expiring
        // checkpoints, list the manifest that removal wrote.
        for (range, cps) in &temporary {
            let rel = format!("ranges/{range}");
            let Some(written) = drop_temporary_checkpoints(dst, &join(&data, &rel), cps).await?
            else {
                continue;
            };
            let rel = rel_of(&data, &written).ok_or_else(|| {
                CtlError::Bad(format!("{written} is outside the copy's prefix {data}"))
            })?;
            let raw = dst
                .get(&written)
                .await
                .map_err(store_err)?
                .bytes()
                .await
                .map_err(store_err)?;
            use sha2::Digest;
            objects.push((
                rel,
                Copied {
                    size: raw.len() as u64,
                    sha256: sha2::Sha256::digest(&raw).into(),
                },
            ));
        }
        // Cairn objects: everything under the prefix except `skip_reason`. Vanishing
        // between LIST and GET (replaced by a new version, cleaned) ⇒ skipped.
        let listed: Vec<_> = src
            .list(Some(main.prefix()))
            .try_collect()
            .await
            .map_err(store_err)?;
        let wanted: Vec<(Path, String)> = listed
            .into_iter()
            .filter_map(|meta| {
                let rel = rel_of(main.prefix(), &meta.location)?;
                // `cluster.conf` is written from snapshot V below, not from the live copy.
                (skip_reason(&rel).is_none() && rel != CLUSTER_CONF).then_some((meta.location, rel))
            })
            .collect();
        let mut copies = futures::stream::iter(wanted.into_iter().map(|(from, rel)| {
            copy_owned(
                Arc::clone(src),
                Arc::clone(dst),
                from,
                data.clone(),
                rel,
                probe.clone(),
            )
        }))
        .buffer_unordered(BACKUP_COPY_CONCURRENCY);
        while let Some(done) = copies.next().await {
            if let Some(o) = done? {
                objects.push(o);
            }
        }
        drop(copies);
        objects.push((
            CLUSTER_CONF.to_string(),
            put_hashed(dst, &join(&data, CLUSTER_CONF), conf).await?,
        ));
        // Owner records, scrubbed of their lease like `fork`: released, no WAL segment.
        let dst_loc = StoreLocation::from_store(Arc::clone(dst), data.clone(), to.uri());
        let scrubbed = SessionId {
            node: "cairn-ctl-backup".into(),
            inc: 0,
        };
        for r in inspect::owner_ranges(src, main.prefix()).await? {
            let Some(o) = inspect::owner_observed(src, main.prefix(), r).await? else {
                continue;
            };
            let rec = OwnerRecord {
                session: scrubbed.clone(),
                address: None,
                released: true,
                wal_segments: Vec::new(),
                wal_floor_id: None,
                ..o.record
            };
            if !inspect::cas_owner(dst, dst_loc.prefix(), r, &rec, None).await? {
                return Err(CtlError::Refused(format!(
                    "owner record of range {} already exists at the target",
                    r.0
                )));
            }
            let rel = format!("owners/range-{}", r.0);
            let raw = dst
                .get(&join(&data, &rel))
                .await
                .map_err(store_err)?
                .bytes()
                .await
                .map_err(store_err)?;
            use sha2::Digest;
            objects.push((
                rel,
                Copied {
                    size: raw.len() as u64,
                    sha256: sha2::Sha256::digest(&raw).into(),
                },
            ));
        }
        objects.sort_by(|a, b| a.0.cmp(&b.0));
        objects.dedup_by(|a, b| a.0 == b.0);
        // The table is still V after reading the owner records: everything copied belongs to V.
        let (_, now) = self.table_snapshot().await?;
        if now.version() != table.version() {
            tracing::warn!(
                backup = id,
                from = table.version(),
                to = now.version(),
                "backup: range table changed during the copy — attempt dropped"
            );
            return Ok(None);
        }
        // 3. The backup manifest, last — only if the fence held the GC the whole time.
        fence.check()?;
        let manifest = BackupManifest {
            id: id.to_string(),
            source: main.uri().to_string(),
            source_prefix: main.prefix().as_ref().to_string(),
            started_ms,
            finished_ms: now_ms(),
            ranges,
            as_of,
            objects,
        };
        let path = join(to.prefix(), &format!("{id}/{BACKUP_MANIFEST}"));
        match cairn_store::cas::put_resolving(
            dst,
            &path,
            manifest.encode().into_bytes().into(),
            PutMode::Create,
        )
        .await
        {
            Ok(cairn_store::cas::Cas::Written(_)) => {}
            Ok(cairn_store::cas::Cas::Lost) => {
                return Err(CtlError::Refused(format!(
                    "{path} already exists with different content"
                )))
            }
            Err(e) => return Err(store_err(e)),
        }
        Ok(Some((manifest, warnings)))
    }

    /// The source's `cluster.conf` bytes and the table decoded from those very bytes —
    /// one snapshot for the whole `backup`/`fork` pass.
    pub(super) async fn table_snapshot(
        &self,
    ) -> CtlResult<(bytes::Bytes, cairn_store::PartitionTable)> {
        let main = self.stores.main();
        let path = join(main.prefix(), CLUSTER_CONF);
        let raw = match main.store().get(&path).await {
            Ok(g) => g.bytes().await.map_err(store_err)?,
            Err(slatedb::object_store::Error::NotFound { .. }) => {
                return Err(CtlError::Bad(format!(
                    "no cluster.conf at {} — wrong --main-store/--prefix?",
                    main.uri()
                )))
            }
            Err(e) => return Err(store_err(e)),
        };
        let table = cairn_store::decode_table(&raw)?;
        Ok((raw, table))
    }

    /// `backup list --to <uri>`: every copy at the destination, oldest first.
    pub async fn backup_list(&self, to: &StoreLocation) -> CtlResult<Vec<BackupEntry>> {
        let mut out = Vec::new();
        for id in list_ids(to).await? {
            let m = read_manifest(to, &id).await?;
            out.push(BackupEntry {
                started_ms: started_ms_of(&id).unwrap_or(0),
                complete: m.is_some(),
                objects: m.as_ref().map(|m| m.objects.len()),
                bytes: m.as_ref().map(BackupManifest::bytes),
                id,
            });
        }
        Ok(out)
    }

    /// `restore --from <uri> [--id ID]`: copies the backup (the newest with a manifest,
    /// or `id`) back to `self`'s store, checking the size + hash of every object. The
    /// destination must be empty and have **the same source prefix**. `format` is
    /// written last.
    pub async fn restore(
        &self,
        from: &StoreLocation,
        id: Option<&str>,
        who: &str,
        command: &str,
    ) -> CtlResult<RestoreReport> {
        if self.stores.is_split() {
            return Err(CtlError::Bad(
                "restore does not support clusters with a separate WAL store (--wal-store) yet"
                    .into(),
            ));
        }
        let id = match id {
            Some(id) => id.to_string(),
            None => {
                let mut latest = None;
                for id in list_ids(from).await?.into_iter().rev() {
                    if read_manifest(from, &id).await?.is_some() {
                        latest = Some(id);
                        break;
                    }
                }
                latest.ok_or_else(|| {
                    CtlError::Refused(format!("{} has no complete backup", from.uri()))
                })?
            }
        };
        let Some(m) = read_manifest(from, &id).await? else {
            return Err(CtlError::Refused(format!(
                "backup {id} has no backup manifest — partial, not used"
            )));
        };
        let main = self.stores.main();
        if main.prefix().as_ref() != m.source_prefix {
            return Err(CtlError::Refused(format!(
                "target has prefix {:?}, backup is from prefix {:?} — SlateDB external_dbs record \
                 absolute paths, the cluster must be rebuilt at exactly the source prefix",
                main.prefix().as_ref(),
                m.source_prefix
            )));
        }
        if !empty_but_reports(main).await? {
            return Err(CtlError::Refused(format!(
                "target {} already has objects — restore only into an empty place",
                main.uri()
            )));
        }
        let data = join(from.prefix(), &format!("{id}/data"));
        // The copy's table lists a range with an owner record while the copy has no Db
        // for it ⇒ the copy combined the Dbs of one table with the `cluster.conf` of a
        // later table (a backup made by an older version during a split/merge commit).
        // Restoring would open that range as an empty range — refuse. A range with
        // neither an owner record nor a Db is a range never opened: opening it as a new
        // range is right.
        if let Some((_, want)) = m.objects.iter().find(|(rel, _)| rel == CLUSTER_CONF) {
            let path = join(&data, CLUSTER_CONF);
            let raw = retry::run(&BACKUP_COPY, &COPY_POLICY, retry::Never, || {
                read_checked(from.store(), &path, want)
            })
            .await?
            .ok_or_else(|| {
                CtlError::Bad(format!(
                    "backup {id} is missing {CLUSTER_CONF} listed in its manifest"
                ))
            })?;
            let table = cairn_store::decode_table(&raw)?;
            let owners: BTreeSet<u32> = m
                .objects
                .iter()
                .filter_map(|(rel, _)| rel.strip_prefix("owners/range-")?.parse().ok())
                .collect();
            let missing: Vec<u32> = table
                .partitions()
                .map(|r| r.0)
                .filter(|r| !m.ranges.contains(r) && owners.contains(r))
                .collect();
            if !missing.is_empty() {
                return Err(CtlError::Refused(format!(
                    "backup {id} is inconsistent: its range table lists ranges {missing:?} that \
                     have an owner record but no Db in the backup (taken while a split or merge \
                     committed) — restoring it would open them empty; restore another backup"
                )));
            }
        }
        // Every object but `format`, concurrently; `format` last, once all the others are in
        // (a node refuses a store without it).
        let (last, rest): (Vec<_>, Vec<_>) = m.objects.iter().partition(|(rel, _)| rel == "format");
        let put_back = |(rel, want): &(String, Copied)| {
            restore_one(
                Arc::clone(from.store()),
                Arc::clone(main.store()),
                join(&data, rel),
                join(main.prefix(), rel),
                rel.clone(),
                want.clone(),
                id.clone(),
            )
        };
        let mut puts = futures::stream::iter(rest.into_iter().map(put_back))
            .buffer_unordered(BACKUP_COPY_CONCURRENCY);
        while let Some(done) = puts.next().await {
            done?;
        }
        drop(puts);
        for o in last {
            put_back(o).await?;
        }
        let entry = AuditEntry {
            who: who.to_string(),
            at_ms: now_ms(),
            command: command.to_string(),
            object: "* (restore)".into(),
            before: String::new(),
            after: format!(
                "restore {id} from {}: {} objects, {} bytes (source {})",
                from.uri(),
                m.objects.len(),
                m.bytes(),
                m.source
            ),
        };
        let audit = inspect::write_audit(main.store(), main.prefix(), &entry)
            .await
            .ok()
            .map(|p| p.to_string());
        Ok(RestoreReport {
            id,
            from: from.uri().to_string(),
            objects: m.objects.len(),
            bytes: m.bytes(),
            audit,
        })
    }

    /// `backup prune --to <uri> [--keep-daily N] [--keep-weekly M] [--apply]`: keeps the
    /// newest complete copy of each day in the latest `daily` days that have copies, and
    /// of each week in the latest `weekly` weeks (UTC days/weeks by start mark); deletes
    /// other complete copies and partial copies older than [`STALE_PARTIAL`]. A dry run
    /// unless `apply`; deleting records `audit/ctl` on `self`'s cluster. Deletes the
    /// manifest **before** the data: a half-deleted copy is a partial copy, never a
    /// "complete" copy missing objects.
    pub async fn backup_prune(
        &self,
        to: &StoreLocation,
        daily: usize,
        weekly: usize,
        apply: bool,
        who: &str,
        command: &str,
    ) -> CtlResult<PruneReport> {
        let all = self.backup_list(to).await?;
        let now = now_ms();
        let day = |ms: u64| ms / 86_400_000;
        let mut keep = BTreeSet::new();
        let (mut days, mut weeks) = (BTreeSet::new(), BTreeSet::new());
        for b in all.iter().rev().filter(|b| b.complete) {
            let d = day(b.started_ms);
            if days.len() < daily && days.insert(d) {
                keep.insert(b.id.clone());
            }
            let w = d / 7;
            if weeks.len() < weekly && weeks.insert(w) {
                keep.insert(b.id.clone());
            }
        }
        let delete: Vec<String> = all
            .iter()
            .filter(|b| {
                !keep.contains(&b.id)
                    && (b.complete
                        || now.saturating_sub(b.started_ms) > STALE_PARTIAL.as_millis() as u64)
            })
            .map(|b| b.id.clone())
            .collect();
        let mut rep = PruneReport {
            applied: false,
            keep: keep.into_iter().collect(),
            delete,
            objects_deleted: 0,
            audit: None,
        };
        if !apply || rep.delete.is_empty() {
            return Ok(rep);
        }
        for id in &rep.delete {
            let manifest = join(to.prefix(), &format!("{id}/{BACKUP_MANIFEST}"));
            match to.store().delete(&manifest).await {
                Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(store_err(e)),
            }
            let under = join(to.prefix(), id);
            let objs: Vec<_> = to
                .store()
                .list(Some(&under))
                .try_collect()
                .await
                .map_err(store_err)?;
            for o in objs {
                match to.store().delete(&o.location).await {
                    Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => {
                        rep.objects_deleted += 1
                    }
                    Err(e) => return Err(store_err(e)),
                }
            }
        }
        rep.applied = true;
        let main = self.stores.main();
        let entry = AuditEntry {
            who: who.to_string(),
            at_ms: now_ms(),
            command: command.to_string(),
            object: format!("{} (backup)", to.uri()),
            before: rep.delete.join(","),
            after: format!("keep {}", rep.keep.join(",")),
        };
        rep.audit = Some(
            inspect::write_audit(main.store(), main.prefix(), &entry)
                .await
                .map_err(|e| {
                    CtlError::Bad(format!(
                        "backup DELETED but audit/ctl could not be written: {e}\n{}",
                        entry.encode()
                    ))
                })?
                .to_string(),
        );
        Ok(rep)
    }
}

/// The store of `--to`/`--from`: a [`StoreLocation`] from a uri.
///
/// `env_prefix` (`--backup-env-prefix P`): the backup store's S3 client takes its
/// credential/endpoint/region **only** from `P` + `AWS_*`
/// ([`StoreLocation::parse_with_env_prefix`]) — the destination is at a different
/// provider from the source. A missing variable or broken configuration is
/// [`CtlError::Bad`] (exit code 2), before any I/O. `None` = as before, the same
/// `AWS_*` as `--main-store`.
pub fn location(uri: &str, env_prefix: Option<&str>) -> CtlResult<StoreLocation> {
    match env_prefix {
        None => Ok(StoreLocation::parse(uri)?),
        Some(p) => StoreLocation::parse_with_env_prefix(uri, p)
            .map_err(|e| CtlError::Bad(format!("--backup-env-prefix {p}: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip_and_strict() {
        let m = BackupManifest {
            id: "1790000000000-ab12".into(),
            source: "s3://b/p".into(),
            source_prefix: "p".into(),
            started_ms: 1,
            finished_ms: 2,
            ranges: vec![1, 2],
            as_of: vec![(1, 1_790_000_000_001), (2, 1_790_000_000_002)],
            objects: vec![
                (
                    "cluster.conf".into(),
                    Copied {
                        size: 3,
                        sha256: [7; 32],
                    },
                ),
                (
                    "ranges/1/manifest/00000000000000000001.manifest".into(),
                    Copied {
                        size: 9,
                        sha256: [1; 32],
                    },
                ),
            ],
        };
        let raw = m.encode();
        assert_eq!(BackupManifest::decode(&raw).unwrap(), m);
        // Cut the last line ⇒ the object count does not match.
        let cut: String = raw
            .lines()
            .take(raw.lines().count() - 1)
            .map(|l| format!("{l}\n"))
            .collect();
        assert!(BackupManifest::decode(&cut).is_err());
        assert!(BackupManifest::decode(&raw.replace(BACKUP_HEADER, "other/1")).is_err());
        assert!(BackupManifest::decode(&format!("{raw}odd=1\n")).is_err());
    }

    /// Every object named in the policy table is classified: copied verbatim or skipped
    /// with a reason — a new object does not silently drop out of the copy by sharing a
    /// skipped prefix.
    #[test]
    fn every_policy_object_is_classified() {
        let skipped: Vec<&str> = cairn_store::OBJECT_POLICIES
            .iter()
            .map(|p| p.object)
            .filter(|o| skip_reason(o).is_some())
            .collect();
        assert_eq!(
            skipped,
            [
                "cluster-history/<version>",
                "owners/range-<id>",
                "sessions/<node>/<inc>",
                "gc-barriers/<id>",
                "incarnations/<node>/<inc>",
                "gc-swept",
                "shedding",
                "control/coordinator",
                "control/probe/<nonce>",
                "ops-reports/<id>",
                "auth/cluster-secret",
            ],
            "change the skip list ⇒ change this test with the same reason"
        );
        assert!(skip_reason("auth/keys").is_none());
        assert!(skip_reason("auth/cluster-secret").is_some());
        assert!(skip_reason("control/quiet-lease").is_none());
        assert!(skip_reason("sheddingx").is_none());
        assert!(skip_reason("ranges/3/manifest/00000000000000000001.manifest").is_some());
    }

    #[test]
    fn destination_inside_source_is_refused() {
        use slatedb::object_store::memory::InMemory;
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let at = |s: &Arc<dyn ObjectStore>, p: &str, uri: &str| {
            StoreLocation::from_store(Arc::clone(s), p, uri)
        };
        assert!(nested(
            &at(&a, "", "memory://"),
            &at(&a, "bk", "memory://bk")
        ));
        assert!(nested(&at(&a, "cell", "m"), &at(&a, "cell/bk", "m")));
        assert!(!nested(&at(&a, "cell", "m"), &at(&a, "backup", "m")));
        assert!(!nested(&at(&a, "", "memory://"), &at(&b, "", "memory://")));
        assert!(nested(
            &at(&a, "cell", "s3://src/cell"),
            &at(&b, "cell/bk", "s3://src/cell/bk")
        ));
        assert!(!nested(
            &at(&a, "cell", "s3://src/cell"),
            &at(&b, "cell", "s3://r2/cell")
        ));
    }

    #[test]
    fn ids_sort_by_time() {
        assert_eq!(started_ms_of("1790000000000-ab12"), Some(1_790_000_000_000));
        assert_eq!(started_ms_of("x-ab12"), None);
        assert_eq!(started_ms_of("1790000000000-zz12"), None);
    }
}
