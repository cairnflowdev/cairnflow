// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tenant purge: the store layer's part.
//!
//! - [`PrefixKind`]: every place on the bucket that carries **one** tenant's data by
//!   prefix. Paths are built by functions here from a closed enum and a [`TenantId`]
//!   (which has no `/`), so `acme` never touches `acme-2/` or `acme.x`.
//! - [`read_book`]/[`read_line`]: the `tenants` book read **fresh** from the bucket, only
//!   the fields the purge needs (`state=`, `deleting-ms=`, `purging-ms=`, `purged-ms=`).
//!   Read more strictly than the server's copy (`cairn-server/src/tenants.rs`): a corrupt
//!   line, a duplicate line, an unknown header ⇒ `Err` — the proof gate keeps (rule 5).
//!   Other flags of the line are ignored.
//! - [`deleting`]/[`purge_due`]: the conclusions shared by the proof gate
//!   ([`crate::proof::DeleteProof::tenant_deleting`],
//!   [`crate::proof::DeleteProof::tenant_purged`],
//!   [`crate::proof::tenant_purge_grant`]) and the purge loop.

use {
    crate::{
        error::{Result, StoreError},
        scope::TenantId,
    },
    slatedb::object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
};

/// Header of the `tenants` book (same as `cairn-server/src/tenants.rs`).
const HEADER: &str = "cairnflow-tenants/1";

/// The reversible retention period of phase A (7 days, the same as the trash).
pub const KEEP: Duration = crate::trash::DEFAULT_KEEP;

/// One place on the bucket that carries a tenant's data by prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrefixKind {
    /// `archive/<t>/` (including `.artifacts/`).
    Archive,
    /// `artifacts/<t>/`.
    Artifacts,
    /// `values/<t>/` — segments of segmented values.
    Values,
    /// `audit/tenant/<t>/` (deleted together with the purge).
    Audit,
    /// `deployments/<t>/`.
    Deployments,
    /// `metering/<t>/` (the control plane closes the invoice before `deleting`).
    Metering,
    /// `stats/<t>/`.
    Stats,
    /// `captures/<t>/`.
    Captures,
    /// `capture-artifacts/<t>/`.
    CaptureArtifacts,
    /// The object `catalog/<t>`.
    Catalog,
    /// The object `quarantine/<t>`.
    Quarantine,
    /// The object `capture-requests/<t>`.
    CaptureRequests,
    /// `tenant-purge/<t>/` — the purge's own progress entries (per-range "range is clean"
    /// proofs, [`RangeProof`]). Carries no customer data; deleted **last**, right before
    /// the tombstone.
    Progress,
}

impl PrefixKind {
    pub const ALL: [PrefixKind; 13] = [
        PrefixKind::Archive,
        PrefixKind::Artifacts,
        PrefixKind::Values,
        PrefixKind::Audit,
        PrefixKind::Deployments,
        PrefixKind::Metering,
        PrefixKind::Stats,
        PrefixKind::Captures,
        PrefixKind::CaptureArtifacts,
        PrefixKind::Catalog,
        PrefixKind::Quarantine,
        PrefixKind::CaptureRequests,
        PrefixKind::Progress,
    ];

    /// Name in trash entries and in reports.
    pub fn name(self) -> &'static str {
        match self {
            PrefixKind::Archive => "archive",
            PrefixKind::Artifacts => "artifacts",
            PrefixKind::Values => "values",
            PrefixKind::Audit => "audit",
            PrefixKind::Deployments => "deployments",
            PrefixKind::Metering => "metering",
            PrefixKind::Stats => "stats",
            PrefixKind::Captures => "captures",
            PrefixKind::CaptureArtifacts => "capture-artifacts",
            PrefixKind::Catalog => "catalog",
            PrefixKind::Quarantine => "quarantine",
            PrefixKind::CaptureRequests => "capture-requests",
            PrefixKind::Progress => PROGRESS_DIR,
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.name() == s)
    }

    /// A directory (`<…>/<t>/`) or exactly one object (`<…>/<t>`).
    pub fn is_dir(self) -> bool {
        !matches!(
            self,
            PrefixKind::Catalog | PrefixKind::Quarantine | PrefixKind::CaptureRequests
        )
    }

    /// Path under root `root` (the cluster prefix): a directory (LISTed by path segment —
    /// `archive/acme` does not cover `archive/acme-2/…`) or an object.
    pub fn path(self, root: &Path, tenant: &TenantId) -> Path {
        let rel = match self {
            PrefixKind::Audit => format!("{}/{tenant}", crate::audit::DIR),
            k => format!("{}/{tenant}", k.name()),
        };
        if root.as_ref().is_empty() {
            Path::from(rel)
        } else {
            Path::from(format!("{}/{rel}", root.as_ref()))
        }
    }
}

/// Every object of `tenant` under `kind` (LIST of the directory by path segment, or HEAD
/// of one object), capped at `max` + 1 — a caller seeing more than `max` works in
/// batches.
pub async fn objects(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    tenant: &TenantId,
    kind: PrefixKind,
    max: usize,
) -> Result<Vec<Path>> {
    use futures::{StreamExt, TryStreamExt};
    let path = kind.path(root, tenant);
    if !kind.is_dir() {
        return match store.head(&path).await {
            Ok(_) => Ok(vec![path]),
            Err(OsError::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(StoreError::Io(e.to_string())),
        };
    }
    store
        .list(Some(&path))
        .take(max.saturating_add(1))
        .map_ok(|m| m.location)
        .try_collect()
        .await
        .map_err(|e| StoreError::Io(e.to_string()))
}

/// Directory of progress entries under the cluster root: `tenant-purge/<tenant>/<range>`.
pub const PROGRESS_DIR: &str = "tenant-purge";

/// Header of a progress entry ([`RangeProof`]).
pub const PROGRESS_HEADER: &str = "cairnflow-tenant-purge/1";

/// Depth cap when tracing lineage through the operation records ([`inherits`]): past the
/// cap ⇒ no inheritance, the range goes through direct scanning and compaction (it only
/// costs work).
pub const LINEAGE_DEPTH: u32 = 8;

/// Why a range is clean of the tenant's rows **and** bytes — or why it no longer is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeBasis {
    /// A full scan saw no row of the tenant, then the full compaction `compaction` (id
    /// from `Admin::submit_compaction`, `-` = a Db with nothing to compact) completed;
    /// **after** seeing it complete, a fresh table showed the range still alive and
    /// carrying no intent ⇒ every later split/merge of this range has its intent after
    /// the mark, so its clone/union takes the compacted manifest.
    Compacted(String),
    /// A range born from op `ops/<key>` whose every source was clean before the op's
    /// intent ([`inherits`]).
    Inherited(u32),
    /// **Dirty**: the range has (or may have) rows of the deleted tenant that were deleted
    /// but not yet compacted away — the final check saw rows arriving after the proof, or
    /// the direct scan is about to delete on a range that has no entry yet
    /// ([`mark_dirty`]). Not a proof; the range is not inherited, and **its children do not
    /// inherit through it either** ([`inherits`]); it must be scanned, deleted and
    /// compacted again, then a new proof overwrites this mark.
    Dirty,
}

/// The durable progress entry of one `(tenant, range)`: proof that the range is clean
/// for the purge carrying this `purging-ms`, or a dirty mark.
///
/// ```text
/// cairnflow-tenant-purge/1
/// range=<id>
/// purging-ms=<ms>
/// basis=compacted:<id> | inherited:<op key> | dirty
/// by=<node>#<nonce>
/// ```
///
/// A proof is written with `Create`; only a dirty mark is ever overwritten (CAS on the
/// version just read). Read leniently: unknown lines are ignored; an unknown header, a
/// missing field, a mismatching range or mark ⇒ treated as absent (the range is scanned
/// and compacted again — it only costs work).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeProof {
    pub range: u32,
    pub purging_ms: u64,
    pub basis: RangeBasis,
    /// The writer, with a nonce: two runners never write identical bytes, so the
    /// comparison of `put_resolving` is meaningful (rule 7).
    pub by: String,
}

impl RangeProof {
    pub fn encode(&self) -> Vec<u8> {
        let basis = match &self.basis {
            RangeBasis::Compacted(id) => format!("compacted:{id}"),
            RangeBasis::Inherited(op) => format!("inherited:{op}"),
            RangeBasis::Dirty => "dirty".to_string(),
        };
        format!(
            "{PROGRESS_HEADER}\nrange={}\npurging-ms={}\nbasis={basis}\nby={}\n",
            self.range, self.purging_ms, self.by
        )
        .into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(raw).ok()?;
        let mut lines = text.lines();
        if lines.next()? != PROGRESS_HEADER {
            return None;
        }
        let (mut range, mut purging, mut basis, mut by) = (None, None, None, None);
        for l in lines {
            let Some((k, v)) = l.split_once('=') else {
                continue;
            };
            match k {
                "range" => range = v.parse().ok(),
                "purging-ms" => purging = v.parse().ok(),
                "basis" => {
                    basis = match v.split_once(':') {
                        _ if v == "dirty" => Some(RangeBasis::Dirty),
                        Some(("compacted", id)) if !id.is_empty() => {
                            Some(RangeBasis::Compacted(id.to_string()))
                        }
                        Some(("inherited", op)) => op.parse().ok().map(RangeBasis::Inherited),
                        _ => None,
                    }
                }
                "by" => by = Some(v.to_string()),
                _ => {}
            }
        }
        Some(RangeProof {
            range: range?,
            purging_ms: purging?,
            basis: basis?,
            by: by?,
        })
    }

    /// It is a clean proof (not a dirty mark).
    pub fn is_clean(&self) -> bool {
        self.basis != RangeBasis::Dirty
    }
}

/// `<root>/tenant-purge/<tenant>/<range>`.
pub fn proof_path(root: &Path, tenant: &TenantId, range: u32) -> Path {
    Path::from(format!(
        "{}/{range}",
        PrefixKind::Progress.path(root, tenant).as_ref()
    ))
}

/// The readable progress entries of one purge: clean proofs, and ranges carrying a dirty
/// mark.
#[derive(Clone, Debug, Default)]
pub struct Progress {
    pub proofs: BTreeMap<u32, RangeProof>,
    pub dirty: std::collections::BTreeSet<u32>,
}

/// Every readable progress entry of `tenant` for purge `purging_ms`, by range. An object
/// that does not decode, or belongs to another range/mark ⇒ skipped (read leniently: that
/// range is scanned again). A dirty mark of any mark ⇒ `dirty` (never a proof).
pub async fn read_proofs(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    tenant: &TenantId,
    purging_ms: u64,
) -> Result<Progress> {
    let mut out = Progress::default();
    for path in objects(store, root, tenant, PrefixKind::Progress, usize::MAX - 1).await? {
        let Some(range) = path.filename().and_then(|f| f.parse::<u32>().ok()) else {
            continue;
        };
        if proof_path(root, tenant, range) != path {
            continue;
        }
        let raw = match store.get(&path).await {
            Ok(g) => g.bytes().await.map_err(|e| StoreError::Io(e.to_string()))?,
            Err(OsError::NotFound { .. }) => continue,
            Err(e) => return Err(StoreError::Io(e.to_string())),
        };
        match RangeProof::decode(&raw) {
            Some(p) if p.range == range && !p.is_clean() => {
                out.dirty.insert(range);
            }
            Some(p) if p.range == range && p.purging_ms == purging_ms => {
                out.proofs.insert(range, p);
            }
            _ => {
                tracing::debug!(path = %path, "tenant purge: unreadable progress entry — rescanning the range")
            }
        }
    }
    Ok(out)
}

/// A progress entry as read, with its version for the CAS. `None` = absent.
async fn read_versioned(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<Option<(Option<RangeProof>, slatedb::object_store::UpdateVersion)>> {
    let got = match store.get(path).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(StoreError::Io(e.to_string())),
    };
    let version = slatedb::object_store::UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got
        .bytes()
        .await
        .map_err(|e| StoreError::Io(e.to_string()))?;
    Ok(Some((RangeProof::decode(&raw), version)))
}

/// A range's progress entry read **at the start** of the scan–delete–compact pass: a
/// `compacted` proof built from that pass may only overwrite exactly what was read
/// ([`write_proof`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Base {
    /// Absent ⇒ the proof is written with `Create`.
    Absent,
    /// A dirty mark (or an unreadable object) at exactly this version ⇒ the proof is
    /// written with `PutMode::Update` on that version.
    Marked(slatedb::object_store::UpdateVersion),
}

/// The entry of range `range` right now.
#[derive(Clone, Debug)]
pub enum Entry {
    /// A clean proof.
    Clean(RangeProof),
    /// Absent, a dirty mark, or an unreadable object — where a scan–delete–compact pass
    /// starts.
    Start(Base),
}

/// Reads the entry of range `range` (one GET).
pub async fn read_entry(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    tenant: &TenantId,
    range: u32,
) -> Result<Entry> {
    Ok(
        match read_versioned(store, &proof_path(root, tenant, range)).await? {
            None => Entry::Start(Base::Absent),
            Some((Some(p), _)) if p.range == range && p.is_clean() => Entry::Clean(p),
            Some((_, version)) => Entry::Start(Base::Marked(version)),
        },
    )
}

/// Number of re-reads when the dirty mark's CAS loses.
const PROGRESS_TRIES: usize = 4;

/// Writes a range's proof on **exactly** the entry read at the start of the pass
/// (`base`): absent ⇒ `Create`; a dirty mark ⇒ `PutMode::Update` on the version read.
/// Ambiguity is resolved by reading back (rule 7; the content is unique thanks to the
/// nonce in `by`). A lost CAS ⇒ re-read: a clean proof for exactly this purge (from
/// another runner, under the same rule) ⇒ `Ok(true)`; anything else — above all a **new**
/// dirty mark (someone marked it dirty again while we were scanning) — ⇒ `Ok(false)`: our
/// proof was built before the new rows, drop it, the next round scans again. A clean proof
/// is never overwritten.
pub async fn write_proof(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    tenant: &TenantId,
    proof: &RangeProof,
    base: &Base,
) -> Result<bool> {
    use crate::cas::{put_resolving, put_resolving_or_overwrite, Cas};
    use slatedb::object_store::PutMode;
    let path = proof_path(root, tenant, proof.range);
    let body = bytes::Bytes::from(proof.encode());
    let cas = match base {
        Base::Absent => put_resolving(store, &path, body, PutMode::Create).await,
        Base::Marked(version) => {
            put_resolving_or_overwrite(store, &path, body, PutMode::Update(version.clone())).await
        }
    };
    match cas {
        Ok(Cas::Written(_)) => Ok(true),
        Ok(Cas::Lost) => Ok(matches!(
            read_entry(store, root, tenant, proof.range).await?,
            Entry::Clean(p) if p.purging_ms == proof.purging_ms
        )),
        Err(e) => Err(StoreError::Io(e.to_string())),
    }
}

/// Marks range `range` dirty: the final check saw rows of the tenant arriving **after**
/// the proof; or the direct scan is about to delete on a range with no readable entry yet.
/// Called **before** deleting rows, so that a death between the deletion and the
/// recompaction leaves no lying proof, and so that the range's children (split/merged
/// before the compaction finished) do not inherit through it.
///
/// **Each call writes a new mark** (nonce in `by`), even when the range is already dirty:
/// the entry's version always changes, so a runner that read the old mark at the start of
/// its scan pass cannot overwrite the new one ([`write_proof`]). Absent ⇒ `Create` (the
/// range was only inherited at the final check, no entry yet); present — a proof, a dirty
/// mark, or a corrupt object — ⇒ CAS on the version just read. Ambiguity is resolved by
/// reading back (rule 7).
pub async fn mark_dirty(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    tenant: &TenantId,
    range: u32,
    purging_ms: u64,
    by: String,
) -> Result<()> {
    use crate::cas::{put_resolving, put_resolving_or_overwrite, Cas};
    use slatedb::object_store::PutMode;
    let path = proof_path(root, tenant, range);
    let body = bytes::Bytes::from(
        RangeProof {
            range,
            purging_ms,
            basis: RangeBasis::Dirty,
            by,
        }
        .encode(),
    );
    for _ in 0..PROGRESS_TRIES {
        let cas = match read_versioned(store, &path).await? {
            None => put_resolving(store, &path, body.clone(), PutMode::Create).await,
            Some((_, version)) => {
                put_resolving_or_overwrite(store, &path, body.clone(), PutMode::Update(version))
                    .await
            }
        };
        match cas {
            Ok(Cas::Written(_)) => return Ok(()),
            Ok(Cas::Lost) => continue,
            Err(e) => return Err(StoreError::Io(e.to_string())),
        }
    }
    Err(StoreError::Io(format!(
        "{path}: could not mark dirty after {PROGRESS_TRIES} CAS attempts"
    )))
}

/// Does range `c` inherit "clean" through its lineage: `ops` gives the op that produced
/// each range (from the operation records `ops/<key>`, [`lineage`]); `proven` are the
/// ranges with a durable proof. Returns the key of the op that produced `c` when **every**
/// source of it (both sources of a merge) was clean — proven, or itself inheriting —
/// within `depth` levels. No op (a root range, an op from a binary that predates operation
/// records, a cleaned-up record) ⇒ `None`. `dirty` are the ranges carrying a dirty mark:
/// they do not inherit themselves, and **no level passes through them** — a dirty range has
/// deleted rows that are not yet compacted away, so its children's Db (cloned from the
/// uncompacted manifest) still holds the tenant's bytes, even if more distant ancestors
/// have a proof.
///
/// Correct because: a [`RangeBasis::Compacted`] proof is only written when the range is
/// still alive and has no intent **after** the compaction completed, so the intent of an
/// op on that range (and the clone / union that follow the intent) comes after the mark;
/// an inheriting range is clean from birth.
pub fn inherits(
    c: u32,
    proven: &dyn Fn(u32) -> bool,
    dirty: &dyn Fn(u32) -> bool,
    ops: &BTreeMap<u32, cairn_split::Op>,
    depth: u32,
) -> Option<u32> {
    if depth == 0 || dirty(c) {
        return None;
    }
    let op = ops.get(&c)?;
    if !op.children().contains(c) {
        return None;
    }
    op.sources()
        .iter()
        .all(|s| proven(s) || inherits(s, proven, dirty, ops, depth - 1).is_some())
        .then(|| op.key())
}

/// The op that produced `c` and its ancestors without a proof and not dirty (up to
/// `depth` levels), read from the operation records — the input of [`inherits`]. An
/// absent record ⇒ that branch stops.
pub async fn lineage(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    c: u32,
    proven: &(dyn Fn(u32) -> bool + Sync),
    dirty: &(dyn Fn(u32) -> bool + Sync),
    depth: u32,
) -> Result<BTreeMap<u32, cairn_split::Op>> {
    let mut out = BTreeMap::new();
    let mut todo = vec![(c, depth)];
    while let Some((r, d)) = todo.pop() {
        if d == 0 || out.contains_key(&r) {
            continue;
        }
        let Some(o) = crate::ledger::read_op_of_child(store, root, r).await? else {
            continue;
        };
        let op = o.record.op;
        for s in op.sources().iter() {
            if !proven(s) && !dirty(s) {
                todo.push((s, d - 1));
            }
        }
        out.insert(r, op);
    }
    Ok(out)
}

impl std::fmt::Display for PrefixKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A ms mark of a book line.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Mark {
    #[default]
    Absent,
    /// Present but unreadable (verbatim) — present is still present.
    Bad(String),
    At(u64),
}

impl Mark {
    fn parse(v: &str) -> Self {
        v.parse()
            .map_or_else(|_| Mark::Bad(v.to_string()), Mark::At)
    }

    pub fn is_present(&self) -> bool {
        *self != Mark::Absent
    }

    pub fn at(&self) -> Option<u64> {
        match self {
            Mark::At(ms) => Some(*ms),
            _ => None,
        }
    }
}

/// A tenant's line in the `tenants` book, only the fields the purge needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantLine {
    /// The verbatim value of `state=`; absent = `active`.
    pub state: Option<String>,
    pub deleting: Mark,
    pub purging: Mark,
    pub purged: Mark,
}

impl TenantLine {
    fn state_is(&self, s: &str) -> bool {
        self.state.as_deref() == Some(s)
    }
}

fn bad(why: String) -> StoreError {
    StoreError::Io(format!("tenants: {why}"))
}

/// Parses the `tenants` book (strictly — see the module doc).
pub fn decode_book(raw: &[u8]) -> Result<BTreeMap<TenantId, TenantLine>> {
    let text = std::str::from_utf8(raw).map_err(|e| bad(format!("not UTF-8: {e}")))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != HEADER {
        return Err(bad(format!("header {header:?}, expected {HEADER:?}")));
    }
    let mut out = BTreeMap::new();
    for line in lines.filter(|l| !l.trim().is_empty()) {
        let mut f = line.split(' ').filter(|x| !x.is_empty());
        let tenant = f
            .next()
            .and_then(|t| TenantId::new(t).ok())
            .ok_or_else(|| bad(format!("corrupt line {line:?}")))?;
        let mut l = TenantLine::default();
        for x in f {
            let (slot, v) = if let Some(v) = x.strip_prefix("state=") {
                if l.state.replace(v.to_string()).is_some() {
                    return Err(bad(format!("state= repeated in line {line:?}")));
                }
                continue;
            } else if let Some(v) = x.strip_prefix("deleting-ms=") {
                (&mut l.deleting, v)
            } else if let Some(v) = x.strip_prefix("purging-ms=") {
                (&mut l.purging, v)
            } else if let Some(v) = x.strip_prefix("purged-ms=") {
                (&mut l.purged, v)
            } else {
                continue;
            };
            *slot = if slot.is_present() {
                Mark::Bad(format!("repeated: {x}"))
            } else {
                Mark::parse(v)
            };
        }
        if out.insert(tenant.clone(), l).is_some() {
            return Err(bad(format!("tenant {tenant} has two lines")));
        }
    }
    Ok(out)
}

fn book_path(root: &Path) -> Path {
    if root.as_ref().is_empty() {
        Path::from("tenants")
    } else {
        Path::from(format!("{}/tenants", root.as_ref()))
    }
}

/// The `tenants` book read fresh from the bucket. Absent = an empty book.
pub async fn read_book(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
) -> Result<BTreeMap<TenantId, TenantLine>> {
    match store.get(&book_path(root)).await {
        Ok(g) => decode_book(&g.bytes().await.map_err(|e| StoreError::Io(e.to_string()))?),
        Err(OsError::NotFound { .. }) => Ok(BTreeMap::new()),
        Err(e) => Err(StoreError::Io(e.to_string())),
    }
}

/// The line of `tenant`, read fresh. `None` = no line (tenant `active`).
pub async fn read_line(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    tenant: &TenantId,
) -> Result<Option<TenantLine>> {
    Ok(read_book(store, root).await?.remove(tenant))
}

/// Phase A: the line carries `state=deleting` verbatim (a tenant other than `default`).
/// An explanation when not.
pub fn deleting(tenant: &TenantId, line: Option<&TenantLine>) -> std::result::Result<(), String> {
    if tenant.is_default() {
        return Err("tenant default cannot be deleted".into());
    }
    match line {
        Some(l) if l.state_is("deleting") => Ok(()),
        Some(l) => Err(format!(
            "tenant {tenant} is in state={:?}, not deleting",
            l.state
        )),
        None => Err(format!("tenant {tenant} has no line in the book (active)")),
    }
}

/// Phase B: [`deleting`], **plus** a readable `purging-ms=`, and a readable `deleting-ms=`
/// at least `keep` before `now_ms`. Returns `(deleting_ms, purging_ms)`.
pub fn purge_due(
    tenant: &TenantId,
    line: Option<&TenantLine>,
    keep: Duration,
    now_ms: u64,
) -> std::result::Result<(u64, u64), String> {
    deleting(tenant, line)?;
    let l = line.ok_or("no line")?;
    let since = l
        .deleting
        .at()
        .ok_or_else(|| format!("deleting-ms {:?} is unreadable", l.deleting))?;
    let purging = l
        .purging
        .at()
        .ok_or_else(|| format!("purging-ms {:?} is unreadable or absent", l.purging))?;
    let keep_ms = keep.as_millis() as u64;
    if now_ms.saturating_sub(since) < keep_ms {
        return Err(format!(
            "deleting-ms={since} is not yet {} ms old (now {now_ms})",
            keep_ms
        ));
    }
    Ok((since, purging))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Matches [`PrefixKind`] ↔ [`crate::OBJECT_POLICIES`]. The tenant purge and `verify`
    /// (l) `tenant-residue` both iterate [`PrefixKind::ALL`], so an object carrying
    /// `<tenant>` that is missing here is customer data that survives the purge with no
    /// check noticing.
    ///
    /// Must see: every policy line with a `<tenant>` segment maps to exactly one
    /// `PrefixKind` (the path up to the end of the `<tenant>` segment, and `is_dir`
    /// matching whether a segment follows `<tenant>`); every `PrefixKind` has at least one
    /// policy line.
    #[test]
    fn prefix_kinds_match_tenant_object_policies() {
        const T: &str = "<tenant>";
        let tenant = TenantId::new("acme").unwrap();
        let root = Path::from("");
        let kinds: Vec<(PrefixKind, String)> = PrefixKind::ALL
            .iter()
            .map(|&k| {
                let p = k.path(&root, &tenant).to_string();
                let rel = p
                    .strip_suffix("acme")
                    .map(|head| format!("{head}{T}"))
                    .unwrap_or_else(|| panic!("{k:?}: path {p} does not end with the tenant"));
                (k, rel)
            })
            .collect();
        let mut covered = std::collections::BTreeSet::new();
        let mut missing = Vec::new();
        for policy in crate::OBJECT_POLICIES {
            let segs: Vec<&str> = policy.object.split('/').collect();
            let Some(at) = segs.iter().position(|s| *s == T) else {
                assert!(
                    !policy.object.contains(T),
                    "{}: `<tenant>` must be a whole path segment",
                    policy.object
                );
                continue;
            };
            let head = segs[..=at].join("/");
            let is_dir = at + 1 < segs.len();
            let found: Vec<PrefixKind> = kinds
                .iter()
                .filter(|(_, rel)| *rel == head)
                .map(|(k, _)| *k)
                .collect();
            match found.as_slice() {
                [k] => {
                    assert_eq!(
                        k.is_dir(),
                        is_dir,
                        "{}: PrefixKind::{k:?}.is_dir() disagrees with the policy line",
                        policy.object
                    );
                    covered.insert(*k);
                }
                [] => missing.push(policy.object),
                many => panic!("{}: many PrefixKinds on a path {many:?}", policy.object),
            }
        }
        assert!(
            missing.is_empty(),
            "an object carrying <tenant> has no PrefixKind (the purge misses it): {missing:?}"
        );
        let orphan: Vec<PrefixKind> = PrefixKind::ALL
            .into_iter()
            .filter(|k| !covered.contains(k))
            .collect();
        assert!(
            orphan.is_empty(),
            "PrefixKind with no OBJECT_POLICIES line: {orphan:?}"
        );
    }

    #[test]
    fn prefixes_are_built_from_the_tenant_id() {
        let acme = TenantId::new("acme").unwrap();
        let root = Path::from("cell");
        let all: Vec<String> = PrefixKind::ALL
            .iter()
            .map(|k| k.path(&root, &acme).to_string())
            .collect();
        assert_eq!(
            all,
            [
                "cell/archive/acme",
                "cell/artifacts/acme",
                "cell/values/acme",
                "cell/audit/tenant/acme",
                "cell/deployments/acme",
                "cell/metering/acme",
                "cell/stats/acme",
                "cell/captures/acme",
                "cell/capture-artifacts/acme",
                "cell/catalog/acme",
                "cell/quarantine/acme",
                "cell/capture-requests/acme",
                "cell/tenant-purge/acme",
            ]
        );
        for k in PrefixKind::ALL {
            assert_eq!(PrefixKind::parse(k.name()), Some(k));
        }
    }

    fn sample(range: u32) -> RangeProof {
        RangeProof {
            range,
            purging_ms: 1_200,
            basis: RangeBasis::Compacted("01JTESTCOMPACTION".into()),
            by: crate::ledger::writer_tag("t"),
        }
    }

    /// Progress entries are read leniently: unknown lines are ignored; an unknown header, a
    /// missing field, a corrupt `basis` ⇒ `None` (the range is scanned again).
    #[test]
    fn range_proof_round_trips_and_reads_leniently() {
        for p in [
            sample(3),
            RangeProof {
                basis: RangeBasis::Inherited(9),
                ..sample(4)
            },
            RangeProof {
                basis: RangeBasis::Dirty,
                ..sample(5)
            },
        ] {
            assert_eq!(RangeProof::decode(&p.encode()), Some(p.clone()));
            let mut extra = p.encode();
            extra.extend_from_slice(b"future=1\n");
            assert_eq!(RangeProof::decode(&extra), Some(p));
        }
        for bad in [
            "cairnflow-tenant-purge/2\nrange=1\npurging-ms=1\nbasis=compacted:x\nby=a\n",
            "cairnflow-tenant-purge/1\nrange=1\nbasis=compacted:x\nby=a\n",
            "cairnflow-tenant-purge/1\nrange=1\npurging-ms=1\nbasis=compacted:\nby=a\n",
            "cairnflow-tenant-purge/1\nrange=1\npurging-ms=1\nbasis=rumor:3\nby=a\n",
            "cairnflow-tenant-purge/1\nrange=x\npurging-ms=1\nbasis=inherited:3\nby=a\n",
        ] {
            assert_eq!(RangeProof::decode(bad.as_bytes()), None, "{bad:?}");
        }
    }

    /// Lineage. Range 0 has a compaction proof, range 1 does not.
    ///
    /// Must see: the children of splitting range 0 inherit; the children of splitting
    /// range 1 (not compacted) do not; merging a child of 0 with a child of 1 does not
    /// (both sources are checked); merging two children of 0 inherits over two levels; a
    /// range without an op (a root, a cleaned-up record) does not; the depth cap stops it.
    #[test]
    fn inheritance_needs_every_source_proven() {
        use cairn_split::Op;
        let proven = |r: u32| r == 0;
        let mut ops = BTreeMap::new();
        let split0 = Op::Split {
            source: 0,
            left: 2,
            right: 3,
        };
        let split1 = Op::Split {
            source: 1,
            left: 4,
            right: 5,
        };
        for (c, op) in [(2, split0), (3, split0), (4, split1), (5, split1)] {
            ops.insert(c, op);
        }
        ops.insert(
            6,
            Op::Merge {
                lo: 3,
                hi: 4,
                into: 6,
            },
        );
        ops.insert(
            7,
            Op::Merge {
                lo: 2,
                hi: 3,
                into: 7,
            },
        );
        assert_eq!(
            inherits(2, &proven, &|_| false, &ops, LINEAGE_DEPTH),
            Some(2)
        );
        assert_eq!(
            inherits(3, &proven, &|_| false, &ops, LINEAGE_DEPTH),
            Some(2)
        );
        assert_eq!(inherits(4, &proven, &|_| false, &ops, LINEAGE_DEPTH), None);
        assert_eq!(
            inherits(6, &proven, &|_| false, &ops, LINEAGE_DEPTH),
            None,
            "one source is not clean"
        );
        assert_eq!(
            inherits(7, &proven, &|_| false, &ops, LINEAGE_DEPTH),
            Some(7)
        );
        assert_eq!(
            inherits(7, &proven, &|_| false, &ops, 1),
            None,
            "depth limit"
        );
        assert_eq!(
            inherits(1, &proven, &|_| false, &ops, LINEAGE_DEPTH),
            None,
            "root range"
        );
        assert_eq!(
            inherits(8, &proven, &|_| false, &ops, LINEAGE_DEPTH),
            None,
            "no record"
        );
        // The record carries an op that did not produce the range asked about (a corrupt bucket) ⇒ no inheritance.
        ops.insert(9, split0);
        assert_eq!(inherits(9, &proven, &|_| false, &ops, LINEAGE_DEPTH), None);
    }

    /// **Lineage never passes through a dirty source** (found in a security review).
    /// Range 0 has a proof; split 0 → (2, 3); range 2 carries a dirty mark (late rows,
    /// deleted but not yet compacted away); split 2 → (4, 5).
    ///
    /// Must see: 4 and 5 do **not** inherit (their Db is cloned from 2's uncompacted
    /// manifest); 3 still inherits from 0; merging 3 with 4 does not inherit.
    ///
    /// Before the fix: `inherits(4)` recursed through 2 (no proof) up to 0 ⇒ `Some(4)`.
    #[test]
    fn a_dirty_source_is_not_inherited_through() {
        use cairn_split::Op;
        let proven = |r: u32| r == 0;
        let dirty = |r: u32| r == 2;
        let split0 = Op::Split {
            source: 0,
            left: 2,
            right: 3,
        };
        let split2 = Op::Split {
            source: 2,
            left: 4,
            right: 5,
        };
        let mut ops = BTreeMap::new();
        for (c, op) in [(2, split0), (3, split0), (4, split2), (5, split2)] {
            ops.insert(c, op);
        }
        ops.insert(
            6,
            Op::Merge {
                lo: 3,
                hi: 4,
                into: 6,
            },
        );
        assert_eq!(inherits(4, &proven, &dirty, &ops, LINEAGE_DEPTH), None);
        assert_eq!(inherits(5, &proven, &dirty, &ops, LINEAGE_DEPTH), None);
        assert_eq!(inherits(6, &proven, &dirty, &ops, LINEAGE_DEPTH), None);
        assert_eq!(inherits(3, &proven, &dirty, &ops, LINEAGE_DEPTH), Some(2));
        assert_eq!(
            inherits(2, &proven, &dirty, &ops, LINEAGE_DEPTH),
            None,
            "the dirty range itself"
        );
    }

    /// Rule 7 for progress entries: the `Create` reached the store but the reply is
    /// ambiguous (`Retried` ⇒ our own `AlreadyExists`, `Error` ⇒ an error after the write)
    /// — the re-read finds exactly our content ⇒ `true`. Another runner wrote a valid proof
    /// first ⇒ `true`, keeping the existing copy; an existing unreadable object ⇒ `false`,
    /// no overwrite. The re-read skips corrupt entries and entries of another purge.
    #[tokio::test]
    async fn proof_write_resolves_ambiguity_by_reread() {
        use crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule};
        let acme = TenantId::new("acme").unwrap();
        let root = Path::from("cell");
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let f = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = f.clone();
            f.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("tenant-purge/acme/3".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let p = sample(3);
            assert!(
                write_proof(&store, &root, &acme, &p, &Base::Absent)
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: {e}")),
                "{reply:?}"
            );
            let got = read_proofs(&store, &root, &acme, 1_200).await.unwrap();
            assert_eq!(got.proofs.get(&3), Some(&p), "{reply:?}");
        }
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let first = sample(5);
        assert!(write_proof(&store, &root, &acme, &first, &Base::Absent)
            .await
            .unwrap());
        assert!(
            write_proof(&store, &root, &acme, &sample(5), &Base::Absent)
                .await
                .unwrap(),
            "another runner's proof is also a proof"
        );
        assert_eq!(
            read_proofs(&store, &root, &acme, 1_200)
                .await
                .unwrap()
                .proofs[&5],
            first,
            "the existing copy was overwritten"
        );
        store
            .put(&proof_path(&root, &acme, 6), b"junk".to_vec().into())
            .await
            .unwrap();
        assert!(
            !write_proof(&store, &root, &acme, &sample(6), &Base::Absent)
                .await
                .unwrap()
        );
        store
            .put(
                &proof_path(&root, &acme, 8),
                RangeProof {
                    purging_ms: 99,
                    ..sample(8)
                }
                .encode()
                .into(),
            )
            .await
            .unwrap();
        let got = read_proofs(&store, &root, &acme, 1_200).await.unwrap();
        assert_eq!(got.proofs.keys().copied().collect::<Vec<_>>(), [5]);
    }

    /// The entry's `Base` at the start of the scan–delete–compact pass.
    async fn base_of(store: &Arc<dyn ObjectStore>, root: &Path, t: &TenantId, r: u32) -> Base {
        match read_entry(store, root, t, r).await.unwrap() {
            Entry::Start(b) => b,
            Entry::Clean(p) => panic!("range {r} is already clean: {p:?}"),
        }
    }

    /// A dirty mark (rows arriving late after the proof): `mark_dirty` turns a proof into a
    /// dirty mark — `read_proofs` sees the range in `dirty`, not in `proofs`; absent ⇒ a
    /// dirty mark is created; already dirty ⇒ **a new mark**, bytes and version change. A
    /// new proof overwrites a dirty mark only on exactly the version read at the start of
    /// the pass; a clean proof is never overwritten. Rule 7: the CAS of both operations is
    /// ambiguous once (`Retried`, `Error`) ⇒ read back, the outcome is right.
    #[tokio::test]
    async fn dirty_mark_invalidates_and_a_new_proof_replaces_it() {
        use crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule};
        let acme = TenantId::new("acme").unwrap();
        let root = Path::from("cell");
        for reply in [None, Some(Ambiguity::Retried), Some(Ambiguity::Error)] {
            let f = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = f.clone();
            let ambiguous = || {
                if let Some(reply) = reply {
                    f.add_rule(Rule::new(
                        OpSet::PUT,
                        PathMatch::Contains("tenant-purge/acme/3".into()),
                        Action::Ambiguous {
                            reply,
                            skip: 0,
                            times: Some(1),
                        },
                    ));
                }
            };
            let raw = |store: Arc<dyn ObjectStore>| async move {
                store
                    .get(&proof_path(
                        &Path::from("cell"),
                        &TenantId::new("acme").unwrap(),
                        3,
                    ))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
            };
            // Absent ⇒ a dirty mark is created (the range was only inherited at the final
            // check); a proof built from a pass started after that mark overwrites it.
            ambiguous();
            mark_dirty(&store, &root, &acme, 3, 1_200, "a#1".into())
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
            let got = read_proofs(&store, &root, &acme, 1_200).await.unwrap();
            assert!(got.dirty.contains(&3) && got.proofs.is_empty(), "{reply:?}");
            let base = base_of(&store, &root, &acme, 3).await;
            assert!(write_proof(&store, &root, &acme, &sample(3), &base)
                .await
                .unwrap());
            let got = read_proofs(&store, &root, &acme, 1_200).await.unwrap();
            assert!(
                got.dirty.is_empty() && got.proofs.contains_key(&3),
                "{reply:?}"
            );
            ambiguous();
            mark_dirty(&store, &root, &acme, 3, 1_200, "a#2".into())
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
            let got = read_proofs(&store, &root, &acme, 1_200).await.unwrap();
            assert!(
                got.proofs.is_empty(),
                "{reply:?}: a proof remains after the dirty mark"
            );
            assert!(got.dirty.contains(&3), "{reply:?}");
            // Runner A starts a pass on mark E0; runner B marks it dirty again (E1, a new
            // mark even though the range was already dirty) ⇒ A's proof loses, the range
            // stays dirty at exactly E1.
            let e0 = base_of(&store, &root, &acme, 3).await;
            let before = raw(Arc::clone(&store)).await;
            ambiguous();
            mark_dirty(&store, &root, &acme, 3, 1_200, "b#3".into())
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
            let e1_bytes = raw(Arc::clone(&store)).await;
            assert_ne!(before, e1_bytes, "{reply:?}: re-mark kept the same bytes");
            let stale = RangeProof {
                basis: RangeBasis::Compacted("01JSTALE".into()),
                ..sample(3)
            };
            assert!(
                !write_proof(&store, &root, &acme, &stale, &e0)
                    .await
                    .unwrap(),
                "{reply:?}: a proof built on E0 overwrote mark E1"
            );
            assert_eq!(raw(Arc::clone(&store)).await, e1_bytes, "{reply:?}");
            assert!(read_proofs(&store, &root, &acme, 1_200)
                .await
                .unwrap()
                .dirty
                .contains(&3));
            // A new pass started on E1 ⇒ it can write.
            let e1 = base_of(&store, &root, &acme, 3).await;
            let fresh = RangeProof {
                basis: RangeBasis::Compacted("01JNEW".into()),
                ..sample(3)
            };
            ambiguous();
            assert!(
                write_proof(&store, &root, &acme, &fresh, &e1)
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: {e}")),
                "{reply:?}"
            );
            let got = read_proofs(&store, &root, &acme, 1_200).await.unwrap();
            assert_eq!(got.proofs.get(&3), Some(&fresh), "{reply:?}");
            assert!(got.dirty.is_empty(), "{reply:?}");
            // A clean proof is not overwritten by another proof (even when the writer
            // holds an old version, even when it believes the entry is absent).
            for base in [e1, Base::Absent] {
                write_proof(&store, &root, &acme, &sample(3), &base)
                    .await
                    .unwrap();
                assert_eq!(
                    read_proofs(&store, &root, &acme, 1_200)
                        .await
                        .unwrap()
                        .proofs
                        .get(&3),
                    Some(&fresh)
                );
            }
        }
    }

    #[test]
    fn book_is_read_strictly() {
        let book = decode_book(
            b"cairnflow-tenants/1\nacme state=deleting deleting-ms=10 purging-ms=x slow-ms=5\nbeta hash-chain\n",
        )
        .unwrap();
        let acme = &book[&TenantId::new("acme").unwrap()];
        assert_eq!(acme.state.as_deref(), Some("deleting"));
        assert_eq!(acme.deleting, Mark::At(10));
        assert_eq!(acme.purging, Mark::Bad("x".into()));
        assert_eq!(acme.purged, Mark::Absent);
        assert_eq!(book[&TenantId::new("beta").unwrap()], TenantLine::default());
        for raw in [
            "cairnflow-tenants/2\n",
            "cairnflow-tenants/1\n/x state=deleting\n",
            "cairnflow-tenants/1\nacme state=deleting\nacme slow-ms=1\n",
            "cairnflow-tenants/1\nacme state=deleting state=active\n",
        ] {
            assert!(decode_book(raw.as_bytes()).is_err(), "{raw:?}");
        }
        let dup = decode_book(b"cairnflow-tenants/1\nacme deleting-ms=1 deleting-ms=1\n").unwrap();
        assert!(matches!(
            dup[&TenantId::new("acme").unwrap()].deleting,
            Mark::Bad(_)
        ));
    }

    #[test]
    fn purge_is_due_after_keep_with_a_purging_mark() {
        let acme = TenantId::new("acme").unwrap();
        let line = |state: &str, d: Mark, p: Mark| TenantLine {
            state: Some(state.into()),
            deleting: d,
            purging: p,
            purged: Mark::Absent,
        };
        let keep = Duration::from_millis(100);
        let ok = line("deleting", Mark::At(1_000), Mark::At(1_200));
        assert_eq!(purge_due(&acme, Some(&ok), keep, 1_100), Ok((1_000, 1_200)));
        assert!(
            purge_due(&acme, Some(&ok), keep, 1_099).is_err(),
            "not old enough"
        );
        for bad in [
            line("suspended", Mark::At(1), Mark::At(2)),
            line("deleting", Mark::At(1), Mark::Absent),
            line("deleting", Mark::Bad("x".into()), Mark::At(2)),
            line("deleting", Mark::At(1), Mark::Bad("y".into())),
            line("purged", Mark::At(1), Mark::At(2)),
        ] {
            assert!(
                purge_due(&acme, Some(&bad), keep, 10_000).is_err(),
                "{bad:?}"
            );
        }
        assert!(purge_due(&acme, None, keep, 10_000).is_err());
        assert!(purge_due(&TenantId::default(), Some(&ok), keep, 10_000).is_err());
        assert!(deleting(&acme, Some(&line("deleting", Mark::Absent, Mark::Absent))).is_ok());
    }
}
