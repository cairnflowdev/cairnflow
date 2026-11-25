// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cairn-level trash: cairn's paths that delete customer data **move** objects into the
//! trash instead of deleting them, and a purge loop only deletes for real after
//! [`DEFAULT_KEEP`], when a fresh check finds that nobody needs them.
//!
//! ```text
//! <prefix>/trash/<yyyy-mm-dd>/<entry>/entry        description (read leniently)
//! <prefix>/trash/<yyyy-mm-dd>/<entry>/obj/<rel>    copy of <prefix>/<rel>
//! ```
//!
//! An **entry** is one deletion: a range Db (`ranges/<id>/…`, one entry with the same id
//! on each store), an owner record, a dead shared-WAL log, or the operation record of a
//! settled split/merge.
//! `<rel>` is the original path without the prefix, so restoring is copying back to the
//! exact spot.
//!
//! **Move = copy then delete, object by object, in the order the caller gives** (for a
//! Db: the G-b order, manifest last). A death midway leaves every object that has gone
//! from its original spot with a copy; the rerun creates a new entry for the rest.
//! Readers of the original spot see exactly what a deletion looks like: the object
//! vanishes. Nobody reads `trash/` except the purge loop, `cairn-ctl trash` and tests.
//!
//! **Not covered:** SlateDB's internal GC (SSTs, old manifests, checkpoints) and the
//! shared WAL's frame GC below the floor — those rely on bucket versioning.
//!
//! A new, leniently read object, no `DATA_FORMAT` bump: an unreadable entry is **kept**
//! by the purge loop (rule 5) and reported by `cairn-ctl trash list`.

use {
    crate::{
        config::RangeRefs,
        error::{Result, StoreError},
        owner::LogId,
        partition_table::RangeId,
        writer::PartitionId,
    },
    bytes::Bytes,
    futures::{StreamExt as _, TryStreamExt},
    slatedb::object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode},
    std::{collections::BTreeMap, fmt, sync::Arc, time::Duration},
};

/// How long an entry is kept before the purge loop may delete it for real (7 days).
pub const DEFAULT_KEEP: Duration = Duration::from_secs(7 * 24 * 60 * 60);

const DIR: &str = "trash";

/// The purge loop's pace of real deletions: at most this many objects per second. Without
/// a pace, one purge round after an hour of chaos deleted 46 k entries (~16 objects per
/// entry) in one go at ~700 DELETE/s, and a local SeaweedFS was saturated for 17 minutes
/// — every range slowed down with it. On S3, DELETE is free and the limits are much
/// higher; this pace still has headroom over the entry creation rate of production (dead
/// WAL logs, rolled-back splits/merges — a few entries per minute). It must be **faster**
/// than the creation rate under rapid leader changes (chaos `kill -9` every 10 s: ~260
/// objects/s), or the trash grows forever (at 300 objects/s, split across the short lives
/// of the runner, it could not keep up).
pub const PURGE_OBJECTS_PER_SEC: u64 = 600;

/// Cap on real deletions in **one** purge round (~33 s at [`PURGE_OBJECTS_PER_SEC`]).
/// When the cap is hit the round stops (`PurgeRound::capped`) and the next round re-reads
/// the durable facts (range table, references) — facts read once at the start of a round
/// must not be used for a round lasting hours.
pub const PURGE_ROUND_MAX_OBJECTS: usize = 20_000;

/// Maximum age of the durable facts a purge round may use (found in a security review):
/// since deletion happens during the LIST, a round on a slow store, or one going through
/// many kept/skipped entries (not counted in [`PURGE_ROUND_MAX_OBJECTS`]), can last long;
/// past this age the round stops (`capped`) and the next round re-reads the facts.
pub const PURGE_FACTS_MAX_AGE: Duration = Duration::from_secs(60);
const ENTRY: &str = "entry";
const OBJ: &str = "obj";
const HEADER: &str = "cairnflow-trash/1";

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// What is deleted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subject {
    /// A range's Db: every object under `ranges/<id>`.
    RangeDb(u32),
    /// The owner record `owners/range-<id>`.
    Owner(u32),
    /// A dead shared-WAL log (seal, floor, index).
    WalLog(LogId),
    /// The operation record `ops/<key>` of a settled split/merge.
    Op(u32),
    /// A history copy `cluster-history/<version>` that fell out of the
    /// [`crate::cluster_history::KEEP`] newest copies.
    TableHistory(u64),
    /// Artifacts of one lifetime `(tenant, id, epoch)`: **held in place**
    /// ([`Trash::hold_in_place`]) — the objects are not copied into `obj/`, the entry is
    /// only a promise to delete. The artifact sweep (running at the id's leader) deletes
    /// for real with [`Trash::purge_in_place`]; the general purge loop skips this entry.
    /// Restoring = removing the entry.
    Artifacts(crate::artifact::Owner),
    /// An object prefix of a tenant being deleted: **held in place** from phase A — the
    /// entry is a promise to delete, restoring = removing the entry. The runner's tenant
    /// purge loop deletes for real with [`Trash::purge_in_place`] and
    /// [`crate::proof::DeleteProof::tenant_purged`]; the general purge loop skips it.
    TenantPrefix {
        tenant: crate::scope::TenantId,
        kind: crate::tenant_purge::PrefixKind,
    },
    /// A segment of a segmented value: **held in place** like [`Subject::Artifacts`].
    /// The values sweep (at the leader of `pk`) deletes for real with
    /// [`Trash::purge_in_place`] and a [`crate::proof::DeleteProof::value_unreferenced`]
    /// rebuilt at that moment; the general purge loop skips it. Restoring = removing the
    /// entry.
    Value(crate::value::Segment),
}

impl Subject {
    /// An entry held in place (no `obj/`): only the subject's own loop deletes for real.
    pub fn in_place(&self) -> bool {
        matches!(
            self,
            Subject::Artifacts(_) | Subject::TenantPrefix { .. } | Subject::Value(_)
        )
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subject::RangeDb(r) => write!(f, "range-db {r}"),
            Subject::Owner(r) => write!(f, "owner {r}"),
            Subject::WalLog(l) => write!(f, "wal-log {l}"),
            Subject::Op(k) => write!(f, "op {k}"),
            Subject::TableHistory(v) => write!(f, "table-history {v}"),
            Subject::Artifacts(o) => write!(
                f,
                "artifacts {}/{}/{:016x}",
                o.tenant,
                crate::ids::id_hex(&o.id),
                o.epoch
            ),
            Subject::TenantPrefix { tenant, kind } => write!(f, "tenant-prefix {tenant}/{kind}"),
            Subject::Value(s) => write!(
                f,
                "value {}/{:016x}/{}/{}-{:016x}",
                s.tenant,
                s.pk.0,
                s.owner.hex(),
                crate::artifact::sha_hex(&s.sha),
                s.gen
            ),
        }
    }
}

/// The id of an entry: `<ms:013>-<16 hex>`. The ms part gives the date and the age; the
/// random part keeps two deletions in the same millisecond out of one entry.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId(String);

impl EntryId {
    /// A new id, at `now_ms`.
    pub fn new(now_ms: u64) -> Self {
        let mut buf = [0u8; 8];
        crate::entropy::fill(&mut buf);
        EntryId(format!("{now_ms:013}-{:016x}", u64::from_le_bytes(buf)))
    }

    /// Parses an id produced by [`Self::new`]; `None` if malformed.
    pub fn parse(s: &str) -> Option<Self> {
        let (ms, rand) = s.split_once('-')?;
        let ok = ms.len() == 13
            && ms.bytes().all(|b| b.is_ascii_digit())
            && rand.len() == 16
            && rand.bytes().all(|b| b.is_ascii_hexdigit());
        ok.then(|| EntryId(s.to_string()))
    }

    /// Creation time, Unix ms.
    pub fn created_ms(&self) -> u64 {
        self.0[..13].parse().unwrap_or(0)
    }

    fn date(&self) -> String {
        crate::archive::utc_date(self.created_ms())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn encode_entry(subject: &Subject, reason: &str) -> Bytes {
    let subject = match subject {
        Subject::RangeDb(r) => format!("kind=range-db\nrange={r}\n"),
        Subject::Owner(r) => format!("kind=owner\nrange={r}\n"),
        Subject::WalLog(l) => format!("kind=wal-log\nlog={l}\n"),
        Subject::Op(k) => format!("kind=op\nkey={k}\n"),
        Subject::TableHistory(v) => format!("kind=table-history\nversion={v}\n"),
        Subject::Artifacts(o) => format!(
            "kind=artifacts\ntenant={}\ninvocation={}\nepoch={:016x}\n",
            o.tenant,
            crate::ids::id_hex(&o.id),
            o.epoch
        ),
        Subject::TenantPrefix { tenant, kind } => {
            format!("kind=tenant-prefix\ntenant={tenant}\nprefix={kind}\n")
        }
        Subject::Value(s) => format!(
            "kind=value\ntenant={}\npk={:016x}\nowner={}\nsha={}\ngen={:016x}\n",
            s.tenant,
            s.pk.0,
            s.owner.hex(),
            crate::artifact::sha_hex(&s.sha),
            s.gen
        ),
    };
    let reason = reason.replace('\n', " ");
    Bytes::from(format!("{HEADER}\n{subject}reason={reason}\n"))
}

fn decode_entry(raw: &[u8]) -> std::result::Result<(Subject, String), String> {
    let text = std::str::from_utf8(raw).map_err(|e| e.to_string())?;
    let mut lines = text.lines();
    if lines.next() != Some(HEADER) {
        return Err("unknown header".into());
    }
    let fields: BTreeMap<&str, &str> = lines.filter_map(|l| l.split_once('=')).collect();
    let range = || -> std::result::Result<u32, String> {
        fields
            .get("range")
            .and_then(|r| r.parse().ok())
            .ok_or_else(|| "missing range".to_string())
    };
    let subject = match fields.get("kind").copied() {
        Some("range-db") => Subject::RangeDb(range()?),
        Some("owner") => Subject::Owner(range()?),
        Some("wal-log") => Subject::WalLog(
            fields
                .get("log")
                .and_then(|l| LogId::parse(l))
                .ok_or("missing log")?,
        ),
        Some("op") => Subject::Op(
            fields
                .get("key")
                .and_then(|k| k.parse().ok())
                .ok_or("missing key")?,
        ),
        Some("table-history") => Subject::TableHistory(
            fields
                .get("version")
                .and_then(|v| v.parse().ok())
                .ok_or("missing version")?,
        ),
        Some("artifacts") => Subject::Artifacts(crate::artifact::Owner {
            tenant: fields.get("tenant").ok_or("missing tenant")?.to_string(),
            id: fields
                .get("invocation")
                .and_then(|h| crate::ids::id_from_hex(h))
                .ok_or("missing invocation")?,
            epoch: fields
                .get("epoch")
                .and_then(|e| u64::from_str_radix(e, 16).ok())
                .ok_or("missing epoch")?,
        }),
        Some("tenant-prefix") => Subject::TenantPrefix {
            tenant: fields
                .get("tenant")
                .and_then(|t| crate::scope::TenantId::new(t).ok())
                .ok_or("missing tenant")?,
            kind: fields
                .get("prefix")
                .and_then(|k| crate::tenant_purge::PrefixKind::parse(k))
                .ok_or("missing prefix")?,
        },
        Some("value") => {
            let hex16 = |k: &str| {
                fields
                    .get(k)
                    .filter(|v| v.len() == 16)
                    .and_then(|v| u64::from_str_radix(v, 16).ok())
                    .ok_or(format!("missing {k}"))
            };
            let sha32 = |k: &str| {
                fields
                    .get(k)
                    .and_then(|v| crate::artifact::sha_from_hex(v))
                    .ok_or(format!("missing {k}"))
            };
            Subject::Value(crate::value::Segment {
                tenant: fields.get("tenant").ok_or("missing tenant")?.to_string(),
                pk: crate::PartitionKey(hex16("pk")?),
                owner: crate::value::Owner(sha32("owner")?),
                sha: sha32("sha")?,
                gen: hex16("gen")?,
            })
        }
        other => return Err(format!("unknown kind: {other:?}")),
    };
    Ok((subject, fields.get("reason").unwrap_or(&"").to_string()))
}

/// One entry, as [`Trash::entries`] sees it.
#[derive(Clone, Debug)]
pub struct EntryView {
    pub id: EntryId,
    /// `None` when `entry` is absent or unreadable — see `unreadable`.
    pub subject: Option<Subject>,
    pub reason: String,
    /// Number of objects and total bytes under `obj/`.
    pub objects: usize,
    pub bytes: u64,
    pub unreadable: Option<String>,
}

/// Result of one restore.
#[derive(Clone, Debug, Default)]
pub struct Restored {
    /// Objects copied back to their original spot.
    pub restored: usize,
    /// The original spot already holds another object: not overwritten, the entry is kept.
    pub conflicts: Vec<String>,
}

/// The trash on one store, rooted at `root` (the main store's prefix — also on a separate
/// WAL store, because the Db and logs there also live under this prefix).
#[derive(Clone)]
pub struct Trash {
    store: Arc<dyn ObjectStore>,
    root: String,
    /// Classification of entries read by [`Self::held_entries`] — shared between clones
    /// and with the [`Seen`] passed in ([`Self::with_seen`]).
    seen: Seen,
}

/// Entries classified by [`Trash::held_entries`]: id → the subject **held in place**, or
/// `None` = an entry not held in place (or an `entry` that could not be decoded).
///
/// Correct because `entry` is **immutable per id**: [`Trash::move_into`] and
/// [`Trash::hold_in_place`] write it once under a freshly issued id ([`EntryId::new`]),
/// nobody rewrites it; removing an entry ([`Trash::restore`], [`Trash::purge`],
/// [`Trash::purge_in_place`]) deletes it and the id is never reused. Once an id has been
/// read, a later read yields the same thing, or absence. The memory drops ids no longer in
/// the latest LIST, and whoever removes an entry (or finds `entry` already absent)
/// overwrites the classification with `None` ([`Seen::removed`]): a store that keeps empty
/// directories (SeaweedFS) lists the directory of a removed entry forever.
#[derive(Clone, Default)]
pub struct Seen(Arc<std::sync::Mutex<BTreeMap<EntryId, Option<Subject>>>>);

impl Seen {
    /// The `entry` of `id` was deleted (or found absent): the entry no longer holds
    /// anything. Only overwrites ids already in memory — an id not yet classified is read
    /// by the next `held_entries` (absent ⇒ as before).
    fn removed(&self, id: &EntryId) {
        if let Some(v) = self.0.lock().expect("mutex").get_mut(id) {
            *v = None;
        }
    }
}

/// An absent `entry` of an entry created longer ago than this is absent for good — the
/// creator writes `entry` right after issuing the id (before any object of the entry), so
/// an older entry without `entry` is a removed entry that left its directory behind (a
/// store keeping empty directories, like SeaweedFS). A younger entry is re-read next round.
const ABSENT_ENTRY_FINAL_AFTER: Duration = Duration::from_secs(3600);

/// Attempts of one `entry` GET of [`Trash::classify`] (read-only, so always safe to retry).
const ENTRY_READ_RETRY: crate::retry::Policy =
    crate::retry::Policy::new(Duration::from_millis(100), Duration::from_secs(1), 4)
        .with_total(Duration::from_secs(5));

/// `entry` reads (GET or HEAD) in flight at once while classifying entries
/// ([`Trash::held_entries`]). Sequentially, a cold node on a trash of 8 000–9 000 entries
/// under chaos needed more than 15 s for one `GET …/purge` poll; 32 in parallel is ~30
/// times less, still small compared with the S3 client's connection limit.
pub const CLASSIFY_IN_FLIGHT: usize = 32;

/// Result of [`Trash::held_entries_checked`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeldScan {
    /// Entries held in place (rechecked by this call, or just classified), oldest first.
    pub held: Vec<(EntryId, Subject)>,
    /// Entries not yet classified, or held entries not yet rechecked, when the budget ran
    /// out — they may be held entries. `0` = `held` is complete.
    pub unchecked: usize,
}

/// The next item of `s`, or `None` when `s` is exhausted or `deadline` has passed.
async fn next_before<S: futures::Stream + Unpin>(
    s: &mut S,
    deadline: Option<tokio::time::Instant>,
) -> Option<S::Item> {
    match deadline {
        None => s.next().await,
        Some(d) => tokio::time::timeout_at(d, s.next()).await.ok().flatten(),
    }
}

impl Trash {
    pub fn new(store: Arc<dyn ObjectStore>, root: &Path) -> Self {
        Trash {
            store,
            root: root.as_ref().to_string(),
            seen: Seen::default(),
        }
    }

    /// Shares the classification memory `seen` (e.g. that of [`crate::ObjectStores`]:
    /// every loop of a node reads each entry once).
    pub fn with_seen(mut self, seen: Seen) -> Self {
        self.seen = seen;
        self
    }

    fn under(&self, rel: &str) -> Path {
        if self.root.is_empty() {
            Path::from(rel)
        } else {
            Path::from(format!("{}/{rel}", self.root))
        }
    }

    fn rel_of(&self, p: &Path) -> Option<String> {
        if self.root.is_empty() {
            return Some(p.as_ref().to_string());
        }
        p.as_ref()
            .strip_prefix(&format!("{}/", self.root))
            .map(str::to_string)
    }

    fn entry_dir(&self, id: &EntryId) -> String {
        format!("{DIR}/{}/{id}", id.date())
    }

    fn entry_path(&self, id: &EntryId) -> Path {
        self.under(&format!("{}/{ENTRY}", self.entry_dir(id)))
    }

    fn obj_dir(&self, id: &EntryId) -> Path {
        self.under(&format!("{}/{OBJ}", self.entry_dir(id)))
    }

    /// **Moves** `objects` (full paths, under the root) into entry `id`, in exactly the
    /// given order: each object is copied, then deleted. An object already absent from its
    /// original spot (someone moved/deleted it first) is skipped; if no object remains, no
    /// entry is created. Returns the number of objects moved.
    ///
    /// An object outside the root is an error, nothing of it is deleted — there would be
    /// nowhere to restore it to.
    pub async fn move_into(
        &self,
        id: &EntryId,
        subject: &Subject,
        reason: &str,
        objects: &[Path],
    ) -> Result<usize> {
        if objects.is_empty() {
            return Ok(0);
        }
        let rels = objects
            .iter()
            .map(|p| {
                self.rel_of(p).ok_or_else(|| {
                    StoreError::Io(format!("{p} lies outside the trash root {:?}", self.root))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // The entry first, objects after: an object seen in `obj/` always has its reason.
        // The entry is written only when at least one real object exists at its original
        // spot — a repeated (idempotent) deletion leaves no empty entry. An unconditional
        // overwrite: the id belongs to this call alone.
        let obj_dir = self.obj_dir(id);
        let mut entry_written = false;
        let mut n = 0;
        for (from, rel) in objects.iter().zip(rels) {
            if !entry_written {
                match self.store.head(from).await {
                    Ok(_) => {}
                    Err(OsError::NotFound { .. }) => continue,
                    Err(e) => return Err(io(e)),
                }
                self.store
                    .put(&self.entry_path(id), encode_entry(subject, reason).into())
                    .await
                    .map_err(io)?;
                entry_written = true;
            }
            let to = Path::from(format!("{}/{rel}", obj_dir.as_ref()));
            match self.store.copy(from, &to).await {
                Ok(()) => {}
                Err(OsError::NotFound { .. }) => continue,
                Err(e) => return Err(io(e)),
            }
            // The "copied, not yet deleted" gap: dying here leaves the object in both places.
            crate::failpoint!("trash::move::before_delete", |arg: Option<String>| {
                Err(crate::failpoint::error("trash::move::before_delete", arg))
            });
            match self.store.delete(from).await {
                Ok(()) | Err(OsError::NotFound { .. }) => n += 1,
                Err(e) => return Err(io(e)),
            }
        }
        Ok(n)
    }

    /// **Holds in place** (AP-2): writes entry `id` for `subject` without copying or
    /// deleting any object — one PUT. The objects stay at their original spot until
    /// [`Self::purge_in_place`] deletes them for real; restoring is removing the entry
    /// ([`Self::restore`]: nothing to copy back). Demands a proof like every deletion path:
    /// the entry is a promise to delete.
    pub async fn hold_in_place(
        &self,
        id: &EntryId,
        proof: &crate::proof::DeleteProof,
        reason: &str,
    ) -> Result<()> {
        if !proof.subject().in_place() {
            return Err(StoreError::Io(format!(
                "{}: not a held-in-place subject",
                proof.subject()
            )));
        }
        self.store
            .put(
                &self.entry_path(id),
                encode_entry(proof.subject(), reason).into(),
            )
            .await
            .map_err(io)?;
        Ok(())
    }

    /// Deletes held-in-place entry `id` for real: the objects at their original spot
    /// (`objects`, full paths — the caller lists them from the subject's directory), then
    /// the entry. `proof` must be for **exactly** the entry's subject, built **now**
    /// (rechecked after the retention period). Returns the number of objects deleted.
    pub async fn purge_in_place(
        &self,
        id: &EntryId,
        proof: crate::proof::DeleteProof,
        objects: &[Path],
    ) -> Result<usize> {
        let Some(n) = self.purge_objects_in_place(id, proof, objects).await? else {
            return Ok(0);
        };
        match self.store.delete(&self.entry_path(id)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => {
                self.seen.removed(id);
                Ok(n)
            }
            Err(e) => Err(io(e)),
        }
    }

    /// Like [`Self::purge_in_place`] but **keeps the entry**: deletes one batch of the
    /// subject's objects — a large prefix (tenant purge) is deleted in batches, the last
    /// batch going through [`Self::purge_in_place`]. `None` = the entry is gone.
    pub async fn purge_objects_in_place(
        &self,
        id: &EntryId,
        proof: crate::proof::DeleteProof,
        objects: &[Path],
    ) -> Result<Option<usize>> {
        let raw = match self.store.get(&self.entry_path(id)).await {
            Ok(g) => g.bytes().await.map_err(io)?,
            Err(OsError::NotFound { .. }) => {
                self.seen.removed(id);
                return Ok(None);
            }
            Err(e) => return Err(io(e)),
        };
        let (subject, _) = decode_entry(&raw).map_err(StoreError::Io)?;
        if !proof.may_purge() {
            return Err(StoreError::Io(format!(
                "the proof for {} only suffices to hold, not to delete entry {id} for real",
                proof.subject()
            )));
        }
        if &subject != proof.subject() || !subject.in_place() {
            return Err(StoreError::Io(format!(
                "the proof for {} does not cover entry {id} ({subject})",
                proof.subject()
            )));
        }
        let mut n = 0;
        for p in objects {
            match self.store.delete(p).await {
                Ok(()) => n += 1,
                Err(OsError::NotFound { .. }) => {}
                Err(e) => return Err(io(e)),
            }
        }
        Ok(Some(n))
    }

    /// Every entry in this store's trash, oldest first.
    ///
    /// One recursive LIST of the whole `trash/` and one `entry` GET per entry: the cost
    /// grows with the whole retention history. The purge loop uses [`Self::due_entries`].
    pub async fn entries(&self) -> Result<Vec<EntryView>> {
        self.entries_under(&self.under(DIR), |_| true).await
    }

    /// The trash's entries **held in place** ([`Subject::in_place`]), oldest first — what
    /// the tenant purge loop, the artifact sweep and the value sweep need.
    ///
    /// The cost does not follow the trash's size: LISTs with a delimiter (`trash/`, then
    /// each date directory — the entry id is the common prefix, `obj/` is not listed), and
    /// `entry` GETs only for entries **not yet** classified ([`Seen`]): after the process's
    /// first call, an idle round costs `1 + days + pages` LISTs, no GET. Before
    /// ([`Self::entries`]): one recursive LIST and one GET per entry, every round — 8 100
    /// GETs per round after an hour of split/merge chaos. Unclassified entries are read
    /// in parallel ([`CLASSIFY_IN_FLIGHT`] requests at a time).
    ///
    /// Nothing changes for safety: the result is exactly the set [`Self::entries`] filtered
    /// by `in_place` yields (unreadable entries or entries missing `entry` are absent, as
    /// before) — **except** an entry that **another** node removed on a store that still
    /// lists empty directories (SeaweedFS: 2–3 minutes after the last DELETE): this node's
    /// classification memory still remembers it as held. A user of the result that
    /// **deletes** re-reads `entry` before deleting ([`Self::purge_objects_in_place`]:
    /// absent ⇒ does nothing); a user that **reports** or **blocks** (the purge API,
    /// tombstones) uses [`Self::held_entries_checked`].
    pub async fn held_entries(&self) -> Result<Vec<(EntryId, Subject)>> {
        let ids = self.entry_ids().await?;
        self.classify(&ids, None).await?;
        Ok(self.held_of(&ids, |_| true, &Default::default()))
    }

    /// Like [`Self::held_entries`], only held entries that `want` accepts, and
    /// **rechecked**: a held entry classified earlier gets its `entry` HEADed again
    /// (absent ⇒ forgotten — another node removed it). `budget` ⇒ stop reading when it runs
    /// out, even on a large trash with a cold memory: entries not yet classified or
    /// rechecked are **not** in `held` and are counted in [`HeldScan::unchecked`] — never
    /// pretended absent. Classification progress is remembered for the next call. A read
    /// error (other than `NotFound`) ⇒ `Err`.
    pub async fn held_entries_checked(
        &self,
        want: impl Fn(&Subject) -> bool,
        budget: Option<Duration>,
    ) -> Result<HeldScan> {
        let deadline = budget.map(|b| tokio::time::Instant::now() + b);
        let ids = self.entry_ids().await?;
        // Held entries known before this call: recheck first (few, and they are what may
        // be stale); entries classified in this call were just read.
        let known: Vec<EntryId> = {
            let seen = self.seen.0.lock().expect("mutex");
            ids.iter()
                .filter(|id| matches!(seen.get(*id), Some(Some(s)) if want(s)))
                .cloned()
                .collect()
        };
        let stale = self.recheck(known, deadline).await?;
        let unclassified = self.classify(&ids, deadline).await?;
        Ok(HeldScan {
            held: self.held_of(&ids, &want, &stale),
            unchecked: stale.len() + unclassified,
        })
    }

    /// The held entries among `ids` per the classification memory, minus `skip`; drops
    /// from memory the ids no longer in `ids` (the latest LIST).
    fn held_of(
        &self,
        ids: &[EntryId],
        want: impl Fn(&Subject) -> bool,
        skip: &std::collections::BTreeSet<EntryId>,
    ) -> Vec<(EntryId, Subject)> {
        let mut seen = self.seen.0.lock().expect("mutex");
        let listed: std::collections::BTreeSet<&EntryId> = ids.iter().collect();
        seen.retain(|id, _| listed.contains(id));
        ids.iter()
            .filter(|id| !skip.contains(*id))
            .filter_map(|id| Some((id.clone(), seen.get(id)?.clone()?)))
            .filter(|(_, s)| want(s))
            .collect()
    }

    /// Classifies (GET `entry`) every id not yet in memory, in parallel, up to `deadline`
    /// if any. Each entry is remembered as soon as it is classified (without waiting for
    /// the whole batch): a call cancelled midway — a `GET …/purge` on a node not yet warm,
    /// a client giving up after 15 s, on a trash of thousands of entries — still leaves its
    /// progress for the next one. Returns the number of ids not yet classified when the
    /// budget ran out.
    async fn classify(
        &self,
        ids: &[EntryId],
        deadline: Option<tokio::time::Instant>,
    ) -> Result<usize> {
        let unknown: Vec<EntryId> = {
            let seen = self.seen.0.lock().expect("mutex");
            ids.iter()
                .filter(|id| !seen.contains_key(*id))
                .cloned()
                .collect()
        };
        let total = unknown.len();
        let now_ms = crate::entropy::now_ms();
        // A read error ⇒ stop sending new requests, but remember every in-flight entry
        // that finished reading, then return the first error.
        let failed = std::sync::atomic::AtomicBool::new(false);
        let pending = unknown
            .into_iter()
            .take_while(|_| !failed.load(std::sync::atomic::Ordering::Relaxed));
        let reads = futures::stream::iter(pending)
            .map(|id| async move {
                // Read-only: a transient error is tried again a few times (a single fault
                // would otherwise fail the caller's whole round — the tenant purge reads
                // hundreds of entries per round).
                let path = self.entry_path(&id);
                let got = crate::retry::run(
                    &crate::retry::ops::TRASH_ENTRY_READ,
                    &ENTRY_READ_RETRY,
                    crate::retry::Never,
                    || async {
                        match self.store.get(&path).await {
                            Ok(g) => g.bytes().await.map(Some).map_err(io),
                            Err(OsError::NotFound { .. }) => Ok(None),
                            Err(e) => Err(io(e)),
                        }
                    },
                )
                .await;
                (id, got)
            })
            .buffer_unordered(CLASSIFY_IN_FLIGHT);
        let mut done = 0;
        let mut first_err = None;
        let mut reads = std::pin::pin!(reads);
        while let Some((id, got)) = next_before(&mut reads, deadline).await {
            done += 1;
            match got {
                Ok(Some(raw)) => {
                    let held = decode_entry(&raw)
                        .ok()
                        .map(|(s, _)| s)
                        .filter(Subject::in_place);
                    self.seen.0.lock().expect("mutex").insert(id, held);
                }
                // A freshly created entry may not have its `entry` yet: re-read next round.
                Ok(None)
                    if id.created_ms() + ABSENT_ENTRY_FINAL_AFTER.as_millis() as u64 <= now_ms =>
                {
                    self.seen.0.lock().expect("mutex").insert(id, None);
                }
                Ok(None) => {}
                Err(e) => {
                    failed.store(true, std::sync::atomic::Ordering::Relaxed);
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(total - done),
        }
    }

    /// HEADs the `entry` of each known held entry `ids`, in parallel, up to `deadline` if
    /// any: absent ⇒ remember the entry as removed ([`Seen::removed`] — `entry` is
    /// immutable per id and ids are never reused, so absent once is absent forever).
    /// Returns the ids not yet checked when the budget ran out.
    async fn recheck(
        &self,
        ids: Vec<EntryId>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<std::collections::BTreeSet<EntryId>> {
        let mut left: std::collections::BTreeSet<EntryId> = ids.iter().cloned().collect();
        let heads = futures::stream::iter(ids)
            .map(|id| async move {
                let r = self.store.head(&self.entry_path(&id)).await;
                (id, r)
            })
            .buffer_unordered(CLASSIFY_IN_FLIGHT);
        let mut heads = std::pin::pin!(heads);
        while let Some((id, r)) = next_before(&mut heads, deadline).await {
            left.remove(&id);
            match r {
                Ok(_) => {}
                Err(OsError::NotFound { .. }) => self.seen.removed(&id),
                Err(e) => return Err(io(e)),
            }
        }
        Ok(left)
    }

    /// The ids of every entry, oldest first: LIST `trash/` with a delimiter (date
    /// directories), then each date directory (entry directories). No `obj/` object listed.
    async fn entry_ids(&self) -> Result<Vec<EntryId>> {
        let days = self
            .store
            .list_with_delimiter(Some(&self.under(DIR)))
            .await
            .map_err(io)?
            .common_prefixes;
        let mut out = Vec::new();
        for day in days {
            let listed = self
                .store
                .list_with_delimiter(Some(&day))
                .await
                .map_err(io)?;
            out.extend(
                listed
                    .common_prefixes
                    .iter()
                    .filter_map(|p| p.filename().and_then(EntryId::parse)),
            );
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// The date directories that **may** hold entries kept for `keep_ms` at `now_ms` —
    /// the part of the trash the purge loop reads ([`purge_expired`]).
    ///
    /// Entries of date `D` are created no earlier than the start of `D` (`EntryId::date`),
    /// so a date whose start plus `keep_ms` is still after `now_ms` holds no expired entry
    /// — it is not LISTed, and none of its `entry` objects is GET. A date directory with an
    /// unknown name is still read as before. Missing one only means **keeping** longer
    /// (rule 5).
    async fn due_days(&self, keep_ms: u64, now_ms: u64) -> Result<Vec<Path>> {
        let mut days = self
            .store
            .list_with_delimiter(Some(&self.under(DIR)))
            .await
            .map_err(io)?
            .common_prefixes;
        days.sort();
        days.retain(|day| {
            !day.filename()
                .and_then(crate::archive::utc_day_start_ms)
                .is_some_and(|start| start.saturating_add(keep_ms) > now_ms)
        });
        Ok(days)
    }

    /// The entry object `p` belongs to (`trash/<date>/<id>/…`), if well-formed.
    fn entry_of(&self, p: &Path) -> Option<EntryId> {
        let rel = self.rel_of(p)?;
        let mut parts = rel.splitn(4, '/');
        let (Some(DIR), Some(_date), Some(id), Some(_)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        EntryId::parse(id)
    }

    /// Entries under `dir` (the trash root or one date directory) that `wanted` accepts;
    /// `entry` is read only for those entries.
    async fn entries_under(
        &self,
        dir: &Path,
        wanted: impl Fn(&EntryId) -> bool,
    ) -> Result<Vec<EntryView>> {
        let listed: Vec<_> = self.store.list(Some(dir)).try_collect().await.map_err(io)?;
        // trash/<date>/<id>/(entry | obj/…)
        let mut groups: BTreeMap<EntryId, (bool, usize, u64)> = BTreeMap::new();
        for m in &listed {
            let Some(rel) = self.rel_of(&m.location) else {
                continue;
            };
            let parts: Vec<&str> = rel.splitn(5, '/').collect();
            let [DIR, _date, id, rest @ ..] = parts.as_slice() else {
                continue;
            };
            let Some(id) = EntryId::parse(id) else {
                continue;
            };
            if !wanted(&id) {
                continue;
            }
            let g = groups.entry(id).or_default();
            match rest {
                [ENTRY] => g.0 = true,
                [OBJ, _] => {
                    g.1 += 1;
                    g.2 += m.size;
                }
                _ => {}
            }
        }
        let mut out = Vec::with_capacity(groups.len());
        for (id, (has_entry, objects, bytes)) in groups {
            let (subject, reason, unreadable) = if !has_entry {
                (None, String::new(), Some("missing `entry`".to_string()))
            } else {
                match self.read_entry(&id).await? {
                    Ok((s, r)) => (Some(s), r, None),
                    Err(why) => (None, String::new(), Some(why)),
                }
            };
            out.push(EntryView {
                id,
                subject,
                reason,
                objects,
                bytes,
                unreadable,
            });
        }
        Ok(out)
    }

    /// When the `entry` of entry `id` was written, by the **store's clock**
    /// (`last_modified`, Unix ms) — a lower bound on the entry's age independent of the
    /// clock of the mover, whose time the entry id carries. `None` = absent.
    pub async fn entry_stored_ms(&self, id: &EntryId) -> Result<Option<u64>> {
        match self.store.head(&self.entry_path(id)).await {
            Ok(m) => Ok(Some(m.last_modified.timestamp_millis().max(0) as u64)),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// Reads `entry` leniently: `Ok(Err(reason))` when absent or undecodable.
    async fn read_entry(
        &self,
        id: &EntryId,
    ) -> Result<std::result::Result<(Subject, String), String>> {
        match self.store.get(&self.entry_path(id)).await {
            Ok(got) => {
                let raw = got.bytes().await.map_err(io)?;
                Ok(decode_entry(&raw))
            }
            Err(OsError::NotFound { .. }) => Ok(Err("missing `entry`".into())),
            Err(e) => Err(io(e)),
        }
    }

    async fn objects_of(&self, id: &EntryId) -> Result<Vec<Path>> {
        self.store
            .list(Some(&self.obj_dir(id)))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .map_err(io)
    }

    /// Copies every object of entry `id` back to its original spot (`Create`: if the
    /// original spot already holds another object it is not overwritten, and goes into
    /// `conflicts`). For a Db: `manifest/` goes back last, so the Db only appears once
    /// complete. With no conflict, the entry is deleted. An entry not on this store ⇒
    /// `restored == 0`.
    pub async fn restore(&self, id: &EntryId) -> Result<Restored> {
        let obj_dir = self.obj_dir(id);
        let mut objects = self.objects_of(id).await?;
        let rank = |p: &Path| {
            let rel = p.as_ref().strip_prefix(obj_dir.as_ref()).unwrap_or("");
            if rel.contains("/manifest/") {
                1
            } else {
                0
            }
        };
        objects.sort_by_key(|p| (rank(p), p.clone()));
        let mut out = Restored::default();
        for from in &objects {
            let Some(rel) = from
                .as_ref()
                .strip_prefix(&format!("{}/", obj_dir.as_ref()))
            else {
                continue;
            };
            let to = self.under(rel);
            let raw = match self.store.get(from).await {
                Ok(g) => g.bytes().await.map_err(io)?,
                Err(OsError::NotFound { .. }) => continue,
                Err(e) => return Err(io(e)),
            };
            match crate::cas::put_resolving(&self.store, &to, raw, PutMode::Create).await {
                Ok(crate::cas::Cas::Written(_)) => out.restored += 1,
                Ok(crate::cas::Cas::Lost) => out.conflicts.push(to.to_string()),
                Err(e) => return Err(io(e)),
            }
        }
        if out.conflicts.is_empty() {
            self.purge(id).await?;
        }
        Ok(out)
    }

    /// Deletes entry `id` for real: objects first, `entry` last. Returns the number of
    /// objects deleted (not counting `entry`).
    ///
    /// The entry's objects go in **one batch** (`delete_stream`: S3 DeleteObjects, up to
    /// 1 000 keys per request) instead of one sequential DELETE per object — under rapid
    /// leader changes, deleting one by one (~16 objects per dead WAL log) was slower than
    /// the creation rate and the trash grew forever. Same set of objects, same order with
    /// respect to `entry` (last); any error other than `NotFound` stops, keeping `entry` —
    /// the next round deletes again (rule 5).
    pub async fn purge(&self, id: &EntryId) -> Result<usize> {
        let objects = self.objects_of(id).await?;
        let n = objects.len();
        let batch = futures::stream::iter(objects.into_iter().map(Ok)).boxed();
        let mut done = self.store.delete_stream(batch);
        while let Some(r) = done.next().await {
            match r {
                Ok(_) | Err(OsError::NotFound { .. }) => {}
                Err(e) => return Err(io(e)),
            }
        }
        match self.store.delete(&self.entry_path(id)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => {
                self.seen.removed(id);
                Ok(n)
            }
            Err(e) => Err(io(e)),
        }
    }
}

// ───────────────────────────────────────────── purge loop

/// The durable facts the purge loop rechecks before deleting for real, read **after**
/// knowing there is an expired entry.
#[derive(Clone, Debug)]
pub struct Facts {
    /// Ranges in the table (just reloaded).
    pub live: Vec<RangeId>,
    /// Which ranges are still referenced by the Db of a live range.
    pub refs: RangeRefs,
    /// Logs that an owner record — tombstones included — still lists. `Err` = not every
    /// owner record could be read: every log counts as still referenced.
    pub logs: std::result::Result<crate::entropy::HashSet<LogId>, String>,
}

impl Default for Facts {
    /// No live range, no reference, no log listed.
    fn default() -> Self {
        Facts {
            live: Vec::new(),
            refs: RangeRefs::default(),
            logs: Ok(Default::default()),
        }
    }
}

/// Reads [`Facts`] for the live ranges `live` (from the table just reloaded): the manifest
/// of each live range ([`crate::ObjectStores::referenced_ranges`]) and every owner record.
pub async fn facts(stores: &crate::ObjectStores, live: Vec<RangeId>) -> Result<Facts> {
    let refs = stores.referenced_ranges(&live).await?;
    let main = stores.main();
    let logs = crate::owner::referenced_logs(main.store(), main.prefix(), 16)
        .await
        .map_err(|e| format!("could not read the owner records: {e}"));
    Ok(Facts { live, refs, logs })
}

/// The purge loop's decision for one entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Purge,
    /// Keep: cannot prove that nobody needs it (ambiguous).
    Unsure(String),
    /// Keep: **still needed** — the original deletion was wrong. A tripwire.
    StillNeeded(String),
}

/// Rechecks one entry against `facts`.
pub fn verdict(subject: Option<&Subject>, facts: &Facts) -> Verdict {
    let Some(subject) = subject else {
        return Verdict::Unsure("unreadable entry".into());
    };
    let live = |r: u32| facts.live.iter().any(|l| l.0 == r);
    match subject {
        Subject::RangeDb(r) | Subject::Owner(r) if live(*r) => {
            Verdict::StillNeeded(format!("range {r} is still in the table"))
        }
        Subject::RangeDb(r) => {
            if let Some(why) = &facts.refs.unknown {
                return Verdict::Unsure(why.clone());
            }
            match facts.refs.blocker(PartitionId(*r)) {
                Some(why) => Verdict::StillNeeded(why),
                None => Verdict::Purge,
            }
        }
        // An operation record enters the trash only once its op has settled
        // (`settled_op`): nobody reads that op's record any more, and an absent record is
        // the behaviour from before operation records existed.
        Subject::Owner(_) | Subject::Op(_) => Verdict::Purge,
        // A table history copy enters the trash only once the table on the bucket is
        // ≥ `KEEP` versions past it (`stale_table_history`, permanent): `table restore`
        // only uses the highest copy.
        Subject::TableHistory(_) => Verdict::Purge,
        // Held in place: only the subject's own loop (at the id's leader) can recheck it.
        Subject::Artifacts(_) => Verdict::Unsure("artifact: the artifact sweep deletes".into()),
        Subject::Value(_) => Verdict::Unsure("segment: the values sweep deletes".into()),
        Subject::TenantPrefix { .. } => {
            Verdict::Unsure("tenant prefix: the tenant purge deletes".into())
        }
        Subject::WalLog(l) => match &facts.logs {
            Err(why) => Verdict::Unsure(why.clone()),
            Ok(logs) if logs.contains(l) => {
                Verdict::StillNeeded(format!("an owner record still lists log {l}"))
            }
            Ok(_) => Verdict::Purge,
        },
    }
}

/// Result of one purge round.
#[derive(Clone, Debug, Default)]
pub struct PurgeRound {
    pub purged: Vec<EntryId>,
    /// Expired entries the round kept, with the decision.
    pub kept: Vec<(EntryId, Verdict)>,
    /// The round stopped at [`PURGE_ROUND_MAX_OBJECTS`]: expired entries remain that did
    /// not get their turn — the caller runs the next round soon, without waiting a tick.
    pub capped: bool,
}

/// One purge round over the bins `bins`: entries older than `keep` at `now_ms` are
/// rechecked against `facts` (read once, right before the first expired entry) before
/// being deleted for real.
///
/// **Deleting while LISTing page by page:** each date directory that may be expired is
/// LISTed recursively as a stream (page after page); the first object of an entry
/// identifies the entry (`trash/<date>/<id>/…`), and an expired entry has its `entry`
/// read, is checked and deleted **at once** — without waiting for the LIST to finish.
/// Before, the round LISTed every entry of the date before deleting: under leader changes
/// every 10–30 s (the runner is the leader of range 0), when a date had ~50 k entries that
/// prologue lasted longer than the runner's life and the purge loop stalled altogether.
/// Now a round cut off midway still leaves its progress **on the store** (deleted entries
/// vanish), with no cursor: the next round LISTs again and sees the rest.
///
/// It does not rely on LIST order (`file://` does not sort): each entry is decided exactly
/// once per round (`seen`), wherever its objects are scattered in the stream;
/// `Trash::purge` LISTs the entry's part again itself. The deletion rule is unchanged:
/// only entries that are expired **and** whose `verdict` is `Purge`; an absent/corrupt
/// `entry` or unreadable facts ⇒ keep (rule 5).
///
/// Age only by the entry id (the mover's clock): for tests and tools that build entries
/// with fake marks. A node's purge loop uses [`purge_expired_store_aged`].
pub async fn purge_expired<F, Fut>(
    bins: &[Trash],
    keep: Duration,
    now_ms: u64,
    facts: F,
) -> Result<PurgeRound>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Facts>>,
{
    purge_expired_with(bins, keep, now_ms, false, facts).await
}

/// [`purge_expired`] where an entry's age also has a **lower bound by the store's
/// clock**: the id carries the mover's ms — a node that is Δ behind makes an entry
/// "expired" Δ early, shortening the undo window by Δ. An entry is expired only when both
/// the age by id and the age by the `last_modified` of its first object seen (every
/// object of the entry is written at move time) reach `keep`. `now_ms` must be the
/// wall-clock time of a runner that passed the clock-skew gate.
pub async fn purge_expired_store_aged<F, Fut>(
    bins: &[Trash],
    keep: Duration,
    now_ms: u64,
    facts: F,
) -> Result<PurgeRound>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Facts>>,
{
    purge_expired_with(bins, keep, now_ms, true, facts).await
}

async fn purge_expired_with<F, Fut>(
    bins: &[Trash],
    keep: Duration,
    now_ms: u64,
    store_aged: bool,
    facts: F,
) -> Result<PurgeRound>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Facts>>,
{
    use futures::TryStreamExt as _;
    let keep_ms = keep.as_millis() as u64;
    let due = |id: &EntryId| id.created_ms().saturating_add(keep_ms) <= now_ms;
    let mut round = PurgeRound::default();
    let mut ask = Some(facts);
    let mut known: Option<(Facts, tokio::time::Instant)> = None;
    let mut deleted = 0usize;
    'bins: for bin in bins {
        for day in bin.due_days(keep_ms, now_ms).await? {
            let mut seen: std::collections::BTreeSet<EntryId> = Default::default();
            let mut listed = bin.store.list(Some(&day));
            while let Some(m) = listed.try_next().await.map_err(io)? {
                let Some(id) = bin.entry_of(&m.location) else {
                    continue;
                };
                if !seen.insert(id.clone()) || !due(&id) {
                    continue;
                }
                // Age by the store's clock: the id carries the mover's ms — a node that is
                // Δ behind makes an entry "expired" Δ early. Every object of the entry is
                // written at move time, so the `last_modified` of the first object seen is
                // a lower bound on the age by the store; the runner passed the clock-skew
                // gate before its first deletion.
                let stored_ms = m.last_modified.timestamp_millis().max(0) as u64;
                if store_aged && stored_ms.saturating_add(keep_ms) > now_ms {
                    continue;
                }
                let stale = known
                    .as_ref()
                    .is_some_and(|(_, at)| at.elapsed() >= PURGE_FACTS_MAX_AGE);
                if deleted >= PURGE_ROUND_MAX_OBJECTS || stale {
                    round.capped = true;
                    break 'bins;
                }
                let subject = match bin.read_entry(&id).await? {
                    Ok((s, _reason)) => Some(s),
                    Err(_unreadable) => None,
                };
                // Entries held in place have their own loop (the artifact sweep).
                if subject.as_ref().is_some_and(Subject::in_place) {
                    continue;
                }
                if known.is_none() {
                    let f = ask.take().expect("facts are read only once per round");
                    let facts = f().await?;
                    known = Some((facts, tokio::time::Instant::now()));
                }
                let facts = &known.as_ref().expect("just read").0;
                match verdict(subject.as_ref(), facts) {
                    Verdict::Purge => {
                        let n = bin.purge(&id).await?;
                        tracing::info!(entry = %id, subject = ?subject, objects = n, "trash: deleting an expired entry for real");
                        round.purged.push(id);
                        // The pace: `n` objects + `entry`, spread evenly over
                        // [`PURGE_OBJECTS_PER_SEC`].
                        deleted += n + 1;
                        crate::pace::poll_gap(Duration::from_micros(
                            (n as u64 + 1) * 1_000_000 / PURGE_OBJECTS_PER_SEC,
                        ))
                        .await;
                    }
                    Verdict::StillNeeded(why) => {
                        // Tripwire: a deletion took away something still needed.
                        crate::failpoint!("trash::purge::still_needed");
                        tracing::error!(entry = %id, subject = ?subject, reason = %why, "trash: an expired entry is still needed — the original deletion was wrong; keeping it, restore with `cairn-ctl trash restore`");
                        round.kept.push((id, Verdict::StillNeeded(why)));
                    }
                    v @ Verdict::Unsure(_) => {
                        tracing::warn!(entry = %id, subject = ?subject, verdict = ?v, "trash: could not recheck — keeping the expired entry");
                        round.kept.push((id, v));
                    }
                }
            }
        }
    }
    Ok(round)
}

#[cfg(test)]
mod tests {
    use {super::*, slatedb::object_store::memory::InMemory};

    fn bin(root: &str) -> (Arc<dyn ObjectStore>, Trash) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let t = Trash::new(Arc::clone(&store), &Path::from(root));
        (store, t)
    }

    async fn put(store: &Arc<dyn ObjectStore>, p: &str, body: &str) {
        store
            .put(&Path::from(p), Bytes::from(body.to_string()).into())
            .await
            .unwrap();
    }

    async fn get(store: &Arc<dyn ObjectStore>, p: &str) -> Option<String> {
        match store.get(&Path::from(p)).await {
            Ok(g) => Some(String::from_utf8(g.bytes().await.unwrap().to_vec()).unwrap()),
            Err(OsError::NotFound { .. }) => None,
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn entry_id_round_trips_and_carries_its_date() {
        let id = EntryId::new(1_760_400_000_000);
        assert_eq!(EntryId::parse(id.as_str()), Some(id.clone()));
        assert_eq!(id.created_ms(), 1_760_400_000_000);
        assert_eq!(id.date(), "2025-10-14");
        assert_eq!(EntryId::parse("12-ab"), None);
        assert_eq!(EntryId::parse("1760400000000-zzzzzzzzzzzzzzzz"), None);
    }

    #[test]
    fn entry_encoding_round_trips() {
        let log = LogId::parse("3/7:1").unwrap();
        let seg = Subject::Value(crate::value::Segment {
            tenant: "acme".into(),
            pk: crate::PartitionKey(0xdead),
            owner: crate::value::Owner([7; 32]),
            sha: [8; 32],
            gen: u64::MAX,
        });
        for s in [
            Subject::RangeDb(4),
            Subject::Owner(9),
            Subject::WalLog(log),
            seg,
        ] {
            let raw = encode_entry(&s, "why\nso");
            assert_eq!(decode_entry(&raw), Ok((s, "why so".to_string())));
        }
        assert!(decode_entry(b"cairnflow-trash/2\nkind=owner\nrange=1\n").is_err());
        assert!(decode_entry(b"cairnflow-trash/1\nkind=unknown\n").is_err());
    }

    /// Move then restore a Db: the objects leave their original spot, come back to the
    /// same spot with the same content, and the entry vanishes once fully restored.
    #[tokio::test]
    async fn move_then_restore_round_trips() {
        let (store, t) = bin("p");
        let objs = [
            "p/ranges/3/wal/01.sst",
            "p/ranges/3/compacted/a.sst",
            "p/ranges/3/manifest/01.manifest",
        ];
        for o in objs {
            put(&store, o, o).await;
        }
        let id = EntryId::new(1_000);
        let paths: Vec<Path> = objs.iter().map(|o| Path::from(*o)).collect();
        let n = t
            .move_into(&id, &Subject::RangeDb(3), "test", &paths)
            .await
            .unwrap();
        assert_eq!(n, 3);
        for o in objs {
            assert_eq!(get(&store, o).await, None, "{o} still at its original spot");
        }
        let listed = t.entries().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].subject, Some(Subject::RangeDb(3)));
        assert_eq!(listed[0].objects, 3);

        let r = t.restore(&id).await.unwrap();
        assert_eq!(r.restored, 3);
        assert!(r.conflicts.is_empty());
        for o in objs {
            assert_eq!(get(&store, o).await.as_deref(), Some(o));
        }
        assert!(t.entries().await.unwrap().is_empty());
    }

    /// **`held_entries` yields exactly the set of `entries` filtered by `in_place`, and
    /// does not GET an already classified entry again.**
    ///
    /// 1. The trash has two ordinary entries (a Db, an operation record), one entry held in
    ///    place (artifacts), an old entry directory without `entry` (a store keeping empty
    ///    directories) and a young entry that has no `entry` yet.
    /// 2. Call twice, counting `entry` GETs in the second call; remove the held entry; the
    ///    young entry gets a held-in-place `entry`.
    ///
    /// Must see: every call yields only the held entry; the second call GETs only the
    /// young entry's `entry`; a removed entry disappears; the young entry appears once it
    /// has `entry`.
    #[tokio::test]
    async fn held_entries_lists_in_place_entries_and_reads_each_once() {
        use crate::testkit::{FaultyStore, Op, PathClass};
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let faulty = FaultyStore::wrap(Arc::clone(&inner));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let t = Trash::new(Arc::clone(&store), &Path::from("p"));
        let now = crate::entropy::now_ms();
        put(&store, "p/ranges/3/manifest/01.manifest", "m").await;
        put(&store, "p/ops/7", "o").await;
        t.move_into(
            &EntryId::new(now - 10),
            &Subject::RangeDb(3),
            "test",
            &[Path::from("p/ranges/3/manifest/01.manifest")],
        )
        .await
        .unwrap();
        t.move_into(
            &EntryId::new(now - 9),
            &Subject::Op(7),
            "test",
            &[Path::from("p/ops/7")],
        )
        .await
        .unwrap();
        let held = Subject::Artifacts(crate::artifact::Owner {
            tenant: "acme".into(),
            id: [3; 24],
            epoch: 1,
        });
        let held_id = EntryId::new(now - 8);
        put(
            &store,
            &format!("p/{}/{ENTRY}", t.entry_dir(&held_id)),
            std::str::from_utf8(&encode_entry(&held, "x")).unwrap(),
        )
        .await;
        // An old removed entry whose directory remains (a stray object keeps the directory on InMemory).
        let old = EntryId::new(now - 2 * ABSENT_ENTRY_FINAL_AFTER.as_millis() as u64);
        put(&store, &format!("p/{}/{OBJ}/x", t.entry_dir(&old)), "x").await;
        let young = EntryId::new(now);
        put(&store, &format!("p/{}/{OBJ}/y", t.entry_dir(&young)), "y").await;

        let want = vec![(held_id.clone(), held.clone())];
        assert_eq!(t.held_entries().await.unwrap(), want);
        let entry_gets = |c: &crate::testkit::StoreCounts| c.get(Op::Get, PathClass::Other);
        let before = faulty.counts();
        assert_eq!(t.held_entries().await.unwrap(), want);
        assert_eq!(
            entry_gets(&faulty.counts().since(&before)),
            1,
            "the second call only GETs the young entry's `entry`"
        );
        // A clone shares the memory.
        let before = faulty.counts();
        assert_eq!(t.clone().held_entries().await.unwrap(), want);
        assert_eq!(entry_gets(&faulty.counts().since(&before)), 1);

        t.restore(&held_id).await.unwrap();
        assert!(t.held_entries().await.unwrap().is_empty());
        let young_held = Subject::Artifacts(crate::artifact::Owner {
            tenant: "acme".into(),
            id: [4; 24],
            epoch: 2,
        });
        put(
            &store,
            &format!("p/{}/{ENTRY}", t.entry_dir(&young)),
            std::str::from_utf8(&encode_entry(&young_held, "x")).unwrap(),
        )
        .await;
        assert_eq!(t.held_entries().await.unwrap(), vec![(young, young_held)]);
        let filtered: Vec<_> = t
            .entries()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|e| Some((e.id, e.subject.filter(Subject::in_place)?)))
            .collect();
        assert_eq!(t.held_entries().await.unwrap(), filtered);
    }

    /// The original spot already holds another object: restoring does not overwrite, keeps the entry.
    #[tokio::test]
    async fn restore_never_overwrites() {
        let (store, t) = bin("");
        put(&store, "owners/range-5", "old").await;
        let id = EntryId::new(2_000);
        t.move_into(
            &id,
            &Subject::Owner(5),
            "test",
            &[Path::from("owners/range-5")],
        )
        .await
        .unwrap();
        put(&store, "owners/range-5", "new").await;
        let r = t.restore(&id).await.unwrap();
        assert_eq!(r.restored, 0);
        assert_eq!(r.conflicts, vec!["owners/range-5".to_string()]);
        assert_eq!(get(&store, "owners/range-5").await.as_deref(), Some("new"));
        assert_eq!(t.entries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn refuses_objects_outside_its_root() {
        let (store, t) = bin("p");
        put(&store, "q/x", "x").await;
        let id = EntryId::new(3_000);
        assert!(t
            .move_into(&id, &Subject::Owner(1), "", &[Path::from("q/x")])
            .await
            .is_err());
        assert_eq!(get(&store, "q/x").await.as_deref(), Some("x"));
    }

    /// Purge loop: not expired ⇒ untouched (and the facts are not read); expired and not
    /// needed by anyone ⇒ deleted for real; still in the table ⇒ kept, `StillNeeded`.
    #[tokio::test]
    async fn purge_rechecks_before_deleting() {
        let (store, t) = bin("");
        for (r, ms) in [(1u32, 1_000u64), (2, 1_000), (3, 50_000)] {
            put(&store, &format!("ranges/{r}/manifest/1"), "m").await;
            t.move_into(
                &EntryId::new(ms),
                &Subject::RangeDb(r),
                "test",
                &[Path::from(format!("ranges/{r}/manifest/1"))],
            )
            .await
            .unwrap();
        }
        let keep = Duration::from_secs(10);
        let early = purge_expired(std::slice::from_ref(&t), keep, 5_000, || async {
            panic!("no entry is expired — the facts must not be read")
        })
        .await
        .unwrap();
        assert!(early.purged.is_empty() && early.kept.is_empty());

        let facts = Facts {
            live: vec![PartitionId(2)],
            logs: Ok(Default::default()),
            ..Facts::default()
        };
        let round = purge_expired(std::slice::from_ref(&t), keep, 20_000, || async {
            Ok(facts)
        })
        .await
        .unwrap();
        assert_eq!(round.purged.len(), 1);
        assert_eq!(round.kept.len(), 1);
        assert!(matches!(round.kept[0].1, Verdict::StillNeeded(_)));
        let left: Vec<_> = t
            .entries()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|e| e.subject)
            .collect();
        assert_eq!(left, vec![Subject::RangeDb(2), Subject::RangeDb(3)]);
    }

    #[test]
    fn verdict_is_conservative() {
        let log = LogId::parse("1/1:0").unwrap();
        let unsure = Facts {
            refs: RangeRefs {
                unknown: Some("broken".into()),
                ..RangeRefs::default()
            },
            logs: Err("broken".into()),
            ..Facts::default()
        };
        assert!(matches!(
            verdict(Some(&Subject::RangeDb(1)), &unsure),
            Verdict::Unsure(_)
        ));
        assert!(matches!(
            verdict(Some(&Subject::WalLog(log.clone())), &unsure),
            Verdict::Unsure(_)
        ));
        assert!(matches!(verdict(None, &unsure), Verdict::Unsure(_)));
        let mut refs = RangeRefs::default();
        refs.by.insert(1, 7);
        let mut logs = crate::entropy::HashSet::default();
        logs.insert(log.clone());
        let needed = Facts {
            refs,
            logs: Ok(logs),
            ..Facts::default()
        };
        assert!(matches!(
            verdict(Some(&Subject::RangeDb(1)), &needed),
            Verdict::StillNeeded(_)
        ));
        assert!(matches!(
            verdict(Some(&Subject::WalLog(log)), &needed),
            Verdict::StillNeeded(_)
        ));
        assert_eq!(verdict(Some(&Subject::Owner(1)), &needed), Verdict::Purge);
    }

    #[test]
    fn every_header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy("trash/<day>/<id>/entry").map(|p| p.header),
            Some(HEADER)
        );
        assert!(crate::format::object_policy("trash/<day>/<id>/obj/<rel>").is_some());
    }

    /// Rule 7: the PUT of `entry`, the copy and the delete may have landed while the reply
    /// reports an error (object_store retries on its own). Must see: the move can be rerun
    /// to completion without losing any object; a restore with an ambiguous `Create` still
    /// counts as written.
    #[tokio::test]
    async fn ambiguous_writes_lose_nothing() {
        use crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let t = Trash::new(Arc::clone(&store), &Path::from("p"));
            put(&store, "p/owners/range-4", "owner").await;
            faulty.add_rule(Rule::ambiguous_put("p/trash/", reply));
            faulty.add_rule(Rule::new(
                OpSet::DELETE,
                PathMatch::Contains("p/owners/".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let id = EntryId::new(9_000);
            let src = [Path::from("p/owners/range-4")];
            // The first attempt's fake error: call again until done, like the next round.
            let mut moved = false;
            for _ in 0..3 {
                if t.move_into(&id, &Subject::Owner(4), "test", &src)
                    .await
                    .is_ok()
                {
                    moved = true;
                    break;
                }
            }
            assert!(moved, "{reply:?}: the move did not finish after 3 attempts");
            assert_eq!(get(&store, "p/owners/range-4").await, None, "{reply:?}");
            let e = t.entries().await.unwrap();
            assert_eq!(e.len(), 1, "{reply:?}: {e:?}");
            assert_eq!(e[0].objects, 1, "{reply:?}: the copy must remain");

            faulty.add_rule(Rule::ambiguous_put("p/owners/", reply));
            let r = t
                .restore(&id)
                .await
                .unwrap_or_else(|err| panic!("{reply:?}: restore landed yet errored: {err}"));
            assert_eq!(r.restored, 1, "{reply:?}: {r:?}");
            assert_eq!(
                get(&store, "p/owners/range-4").await.as_deref(),
                Some("owner")
            );
        }
    }
}
