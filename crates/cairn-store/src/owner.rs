// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **range ownership** record — the source of truth for "who leads p" (D20).
//!
//! One object per range, `<prefix>/owners/range-<id>`, written only when **the owner
//! changes**: on claim and on release, both by CAS on the etag. A retired range keeps its
//! record as a tombstone — the id is never reused (D22).
//!
//! The record also carries `wal_kind` (D-f): the kind of WAL the range's Db uses. Others
//! read it **before** opening the Db, so a binary that does not know the kind refuses
//! instead of opening with the native WAL and silently dropping unflushed rows that live
//! elsewhere. At idle it costs no requests — the "still alive" part lives in the node's
//! session ([`crate::session`]), renewed once for all of that node's partitions.
//!
//! The record carries no timestamp, for the same reason as the old lease: the clocks of two
//! machines cannot be compared. Whether p's owner is still valid is a question about the
//! **session** the record points to.

use {
    crate::error::{Result, StoreError},
    crate::ids::PartitionId,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{fmt, sync::Arc},
};

/// Identity of a session incarnation: `<node_id>/<inc>`.
///
/// `node_id` may contain `/`; `inc` is the number **after the last `/`**.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId {
    pub node: String,
    pub inc: u64,
}

impl SessionId {
    pub fn parse(s: &str) -> Option<SessionId> {
        let (node, inc) = s.rsplit_once('/')?;
        if node.is_empty() {
            return None;
        }
        Some(SessionId {
            node: node.to_string(),
            inc: inc.parse().ok()?,
        })
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.node, self.inc)
    }
}

/// The WAL kind of a range, recorded in the owner record (D-f).
///
/// An unknown value is **not** swallowed into a default when decoding: it passes through
/// intact so the place opening the range sees it and refuses
/// ([`crate::StoreError::UnknownWalKind`]). That is how `shared` could be added without
/// bumping `DATA_FORMAT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalKind {
    /// SlateDB's native WAL: each Db has its own series of WAL objects. **No longer
    /// served**: only read from old records, so the `wal/shared` migration step can find
    /// the ranges to convert. No new range carries this kind.
    SlateDb,
    /// The per-node shared WAL: the range's frames live in the log of the node holding it,
    /// indexed by `wal_segment`.
    Shared,
    /// A kind this binary cannot read.
    Unknown(String),
}

impl WalKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::SlateDb => "slatedb",
            Self::Shared => "shared",
            Self::Unknown(s) => s,
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "slatedb" => Self::SlateDb,
            "shared" => Self::Shared,
            other => Self::Unknown(other.to_string()),
        }
    }
}

/// One log **stream** (stripe) of a node incarnation: `nodewal/<node>/<inc>/<stripe>/` on
/// the WAL store.
///
/// A log is tied to a session incarnation: when the session ends the log may be sealed,
/// and no other incarnation writes to it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogId {
    pub session: SessionId,
    pub stripe: u16,
}

impl LogId {
    /// `<node>/<inc>:<stripe>` — `stripe` is the number after the **last** `:`.
    pub fn parse(s: &str) -> Option<LogId> {
        let (session, stripe) = s.rsplit_once(':')?;
        Some(LogId {
            session: SessionId::parse(session)?,
            stripe: stripe.parse().ok()?,
        })
    }
}

impl fmt::Display for LogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.session, self.stripe)
    }
}

/// One segment of the range's WAL index (`wal_segment`): the range's frames with
/// `wal_file_id` from `first_wal_id` up to before the next segment's `first_wal_id` live in
/// `log`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalSegment {
    pub log: LogId,
    pub first_wal_id: u64,
}

impl WalSegment {
    /// `<node>/<inc>:<stripe>@<first_wal_id>`.
    pub fn parse(s: &str) -> Option<WalSegment> {
        let (log, first) = s.rsplit_once('@')?;
        Some(WalSegment {
            log: LogId::parse(log)?,
            first_wal_id: first.parse().ok()?,
        })
    }
}

impl fmt::Display for WalSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.log, self.first_wal_id)
    }
}

/// Content of `owners/range-<id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerRecord {
    /// Incremented by 1 on each change of owner. For logs and so routing hints pick the
    /// newer copy — **not** a fence (the fence is `writer_epoch`).
    pub term: u64,
    /// The session that claimed this range.
    pub session: SessionId,
    /// The owner's HTTP address at claim time — for routing before any hint exists.
    pub address: Option<String>,
    /// The owner released voluntarily: anyone can claim it at once.
    pub released: bool,
    /// The WAL kind the range's Db uses. Required: missing means a damaged record.
    pub wal_kind: WalKind,
    /// **Tombstone**: the range was retired after a split/merge commit, its Db is closed
    /// and its children have replaced it. Nobody may claim this range again — even when the
    /// record also carries `released` — because reopening the parent Db would write into a
    /// Db no table points to. One-way.
    pub retired: bool,
    /// The range's WAL index when `wal_kind = shared`: the logs that may still carry
    /// frames of the range not yet in L0, in increasing `first_wal_id` order. Empty for
    /// `slatedb`, and after a **clean** cooperative release.
    pub wal_segments: Vec<WalSegment>,
    /// Highest wal id the previous owner allocated, written on a clean cooperative release:
    /// the taker starts above it.
    pub wal_floor_id: Option<u64>,
    /// Data version of the range Db (`data=`): the last range-tier migration step that has
    /// finished on this Db. `None` ≡ 0 — a record from a binary before the migration
    /// framework, or a range that has never run a step.
    pub data: Option<u32>,
    /// When the tombstone was written (ms, wall clock of the writing node) — the age of
    /// the retired range for D-d's `min_age`. Only on tombstones. Format 9 records do not
    /// have this line: D-d uses the object's `last_modified` (a tombstone is never
    /// rewritten). Read leniently: a damaged value = absent.
    pub retired_ms: Option<u64>,
}

pub(crate) const OWNER_HEADER: &str = "cairnflow-owner/1";

impl OwnerRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "{OWNER_HEADER}\nterm={}\nsession={}\n",
            self.term, self.session
        );
        if let Some(a) = &self.address {
            out.push_str(&format!("address={a}\n"));
        }
        if self.released {
            out.push_str("released=1\n");
        }
        if self.retired {
            out.push_str("retired=1\n");
        }
        if let Some(ms) = self.retired_ms {
            out.push_str(&format!("retired_ms={ms}\n"));
        }
        out.push_str(&format!("wal_kind={}\n", self.wal_kind.as_str()));
        for seg in &self.wal_segments {
            out.push_str(&format!("wal_segment={seg}\n"));
        }
        if let Some(f) = self.wal_floor_id {
            out.push_str(&format!("wal_floor_id={f}\n"));
        }
        if let Some(d) = self.data {
            out.push_str(&format!("data={d}\n"));
        }
        out.into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Result<OwnerRecord> {
        let bad = |reason: String| StoreError::LeaseCorrupt { reason };
        let text = std::str::from_utf8(raw).map_err(|e| bad(format!("not UTF-8: {e}")))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != OWNER_HEADER {
            if let Some(e) = crate::format::newer_header("owners/range-<id>", OWNER_HEADER, header)
            {
                return Err(e);
            }
            return Err(bad(format!("missing header {OWNER_HEADER}")));
        }
        let (mut term, mut session, mut address, mut released) = (None, None, None, false);
        let mut wal_kind = None;
        let mut retired = false;
        let mut wal_segments = Vec::new();
        let mut wal_floor_id = None;
        let mut data = None;
        let mut retired_ms = None;
        for line in lines {
            match line.split_once('=') {
                Some(("term", v)) => term = v.parse().ok(),
                Some(("session", v)) => session = SessionId::parse(v),
                Some(("address", v)) => address = Some(v.to_string()),
                Some(("released", v)) => released = v == "1",
                Some(("wal_kind", v)) => wal_kind = Some(WalKind::parse(v)),
                Some(("retired", v)) => retired = v == "1",
                Some(("retired_ms", v)) => retired_ms = v.parse().ok(),
                // A misread segment is a log nobody seals and nobody replays — acked rows
                // lost. Refuse the whole record, do not skip the line.
                Some(("wal_segment", v)) => wal_segments.push(
                    WalSegment::parse(v).ok_or_else(|| bad(format!("bad wal_segment: {v:?}")))?,
                ),
                Some(("wal_floor_id", v)) => {
                    wal_floor_id = Some(
                        v.parse()
                            .map_err(|_| bad(format!("bad wal_floor_id: {v:?}")))?,
                    )
                }
                // Misreading `data` means skipping a migration step or running one twice.
                Some(("data", v)) => {
                    data = Some(v.parse().map_err(|_| bad(format!("bad data: {v:?}")))?)
                }
                _ => {}
            }
        }
        Ok(OwnerRecord {
            term: term.ok_or_else(|| bad("missing term".into()))?,
            session: session.ok_or_else(|| bad("missing session".into()))?,
            address,
            released,
            wal_kind: wal_kind.ok_or_else(|| bad("missing wal_kind".into()))?,
            retired,
            wal_segments,
            wal_floor_id,
            data,
            retired_ms,
        })
    }
}

pub(crate) fn owners_dir(prefix: &Path) -> String {
    if prefix.as_ref().is_empty() {
        "owners".into()
    } else {
        format!("{}/owners", prefix.as_ref())
    }
}

pub(crate) fn owner_path(prefix: &Path, p: PartitionId) -> Path {
    Path::from(format!("{}/range-{}", owners_dir(prefix), p.0))
}

fn io(e: OsError) -> StoreError {
    StoreError::LeaseIo(e.to_string())
}

/// A record that was read, with the version to CAS against.
#[derive(Clone, Debug)]
pub struct ObservedOwner {
    pub record: OwnerRecord,
    pub version: UpdateVersion,
    /// The object's `last_modified`, ms.
    pub modified_ms: u64,
}

/// Ranges whose owner record carries a **tombstone** (`retired=1`) — D-d candidates.
pub async fn retired_ranges(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Vec<PartitionId>> {
    Ok(list_retired(store, prefix)
        .await?
        .into_iter()
        .map(|r| r.range)
        .collect())
}

/// A range carrying a tombstone, with the tombstone's age reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetiredRange {
    pub range: PartitionId,
    /// The tombstone's `retired_ms=`; `None` for format 9 records.
    pub retired_ms: Option<u64>,
    /// The `last_modified` of the owner record object, ms.
    pub modified_ms: u64,
}

impl RetiredRange {
    /// When the range retired: `retired_ms`, otherwise `last_modified`.
    pub fn since_ms(&self) -> u64 {
        self.retired_ms.unwrap_or(self.modified_ms)
    }
}

/// Every range carrying a tombstone, in LIST order.
pub async fn list_retired(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Vec<RetiredRange>> {
    use futures::TryStreamExt;
    let dir = Path::from(owners_dir(prefix));
    let listed: Vec<_> = store.list(Some(&dir)).try_collect().await.map_err(io)?;
    let mut out = Vec::new();
    for m in listed {
        let Some(id) = m
            .location
            .filename()
            .and_then(|n| n.strip_prefix("range-"))
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        let Some(o) = read_observed(store, prefix, PartitionId(id)).await? else {
            continue;
        };
        if o.record.retired {
            out.push(RetiredRange {
                range: PartitionId(id),
                retired_ms: o.record.retired_ms,
                modified_ms: o.modified_ms,
            });
        }
    }
    Ok(out)
}

/// Reads the owner record of `p`. `Ok(None)` = nobody has ever claimed it.
pub async fn read_owner(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    p: PartitionId,
) -> Result<Option<OwnerRecord>> {
    Ok(read_observed(store, prefix, p).await?.map(|o| o.record))
}

pub(crate) async fn read_observed(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    p: PartitionId,
) -> Result<Option<ObservedOwner>> {
    let got = match store.get(&owner_path(prefix, p)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let modified_ms = got.meta.last_modified.timestamp_millis().max(0) as u64;
    let raw = got.bytes().await.map_err(io)?;
    Ok(Some(ObservedOwner {
        record: OwnerRecord::decode(&raw)?,
        version,
        modified_ms,
    }))
}

/// Retries of one read of [`referenced_logs`]: a read deletes nothing, so trying it again
/// is always safe.
const SNAPSHOT_READ_RETRY: crate::retry::Policy = crate::retry::Policy::new(
    std::time::Duration::from_millis(100),
    std::time::Duration::from_secs(1),
    4,
)
.with_total(std::time::Duration::from_secs(5));

/// Every log that an owner record — tombstones included — still lists in `wal_segments`:
/// one LIST of `owners/` and one GET per range (in parallel, at most `in_flight`). A
/// transient failure of the LIST or of a GET is retried ([`SNAPSHOT_READ_RETRY`]): with
/// hundreds of records, one injected fault per snapshot used to fail most dead-log sweep
/// rounds. An unreadable record (after the retries, or undecodable) is an error — the caller
/// keeps everything (delete only when provable).
pub(crate) async fn referenced_logs(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    in_flight: usize,
) -> Result<crate::entropy::HashSet<LogId>> {
    use futures::{StreamExt, TryStreamExt};
    let dir = Path::from(owners_dir(prefix));
    let listed: Vec<_> = crate::retry::run(
        &crate::retry::ops::OWNER_SNAPSHOT_READ,
        &SNAPSHOT_READ_RETRY,
        crate::retry::Never,
        || async {
            store
                .list(Some(&dir))
                .try_collect::<Vec<_>>()
                .await
                .map_err(io)
        },
    )
    .await?;
    let ranges: Vec<u32> = listed
        .iter()
        .filter_map(|m| {
            m.location
                .filename()
                .and_then(|n| n.strip_prefix("range-"))
                .and_then(|n| n.parse::<u32>().ok())
        })
        .collect();
    let records: Vec<_> = futures::stream::iter(ranges)
        .map(|raw| {
            crate::retry::run(
                &crate::retry::ops::OWNER_SNAPSHOT_READ,
                &SNAPSHOT_READ_RETRY,
                crate::retry::Never,
                move || read_owner(store, prefix, PartitionId(raw)),
            )
        })
        .buffer_unordered(in_flight)
        .try_collect()
        .await?;
    Ok(records
        .into_iter()
        .flatten()
        .flat_map(|rec| rec.wal_segments.into_iter().map(|s| s.log))
        .collect())
}

/// Deletes the owner record of range `p` — only for a range that **has never been in the
/// table**: a child of a rolled-back split/merge. Absent is fine. A range that has ever
/// been in the table keeps its record forever (tombstone, D22).
///
/// The record is **moved into the trash** ([`crate::trash`]), not really deleted. Gate:
/// `proof` must be [`crate::proof::DeleteProof::abandoned_child_owner`] for exactly this
/// range.
pub async fn delete_owner(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    proof: crate::proof::DeleteProof,
) -> Result<()> {
    let p = match proof.subject() {
        crate::trash::Subject::Owner(r) => PartitionId(*r),
        other => {
            return Err(StoreError::Io(format!(
                "delete_owner needs proof for an owner record, got {other}"
            )))
        }
    };
    proof
        .check(&crate::trash::Subject::Owner(p.0), None)
        .await?;
    crate::trash::Trash::new(Arc::clone(store), prefix)
        .move_into(
            &crate::trash::EntryId::new(crate::entropy::now_ms()),
            &crate::trash::Subject::Owner(p.0),
            "child of a rolled-back split/merge",
            &[owner_path(prefix, p)],
        )
        .await
        .map(|_| ())
}

/// Writes the owner record, CAS on `expected` (`None` ⇒ create). `Ok(None)` = the CAS
/// missed: someone changed the record before us. An ambiguous outcome has already been
/// resolved by re-reading; `Err` means it is not known whether the write landed.
pub(crate) async fn write_owner(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    p: PartitionId,
    record: &OwnerRecord,
    expected: Option<UpdateVersion>,
) -> Result<Option<UpdateVersion>> {
    // `return(conflict)` ⇒ CAS misses (`Ok(None)`); `return(<other>)` ⇒ IO error.
    crate::failpoint!("owner::write", |arg: Option<String>| match arg.as_deref() {
        Some("conflict") => Ok(None),
        _ => Err(crate::failpoint::error("owner::write", arg)),
    });
    let mode = match expected {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    // An ambiguous outcome is resolved by re-reading: the record carries the writer's
    // session + term, so identical content is ours. Treating `Precondition` after an
    // automatic object_store retry as a loss would give the keeper a false `LeaseLost` —
    // with the shared WAL, a fail-stop of the whole node.
    match crate::cas::put_resolving(store, &owner_path(prefix, p), record.encode().into(), mode)
        .await
    {
        Ok(crate::cas::Cas::Written(v)) => Ok(Some(v)),
        // Someone changed the record, or it disappeared between the read and the write.
        Ok(crate::cas::Cas::Lost) => Ok(None),
        Err(e) => Err(io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_record_roundtrips() {
        for (address, released, retired) in [
            (None, false, false),
            (Some("http://h:1".to_string()), true, false),
            (None, true, true),
        ] {
            let r = OwnerRecord {
                term: 7,
                session: SessionId {
                    node: "host/12/345".into(),
                    inc: 3,
                },
                address,
                released,
                wal_kind: WalKind::SlateDb,
                retired,
                wal_segments: vec![],
                wal_floor_id: None,
                data: None,
                retired_ms: None,
            };
            assert_eq!(OwnerRecord::decode(&r.encode()).unwrap(), r);
        }
    }

    /// **The WAL kind mark (D-f).** The owner record carries `wal_kind` — `slatedb` or
    /// `shared` — and an unknown value passes through encode/decode intact so whoever opens
    /// the range *can see it* and refuse, instead of it being swallowed into a default.
    #[test]
    fn owner_record_round_trips_wal_kind() {
        for kind in [
            WalKind::SlateDb,
            WalKind::Shared,
            WalKind::Unknown("future-kind".into()),
        ] {
            let r = OwnerRecord {
                term: 2,
                session: SessionId {
                    node: "n".into(),
                    inc: 1,
                },
                address: None,
                released: false,
                wal_kind: kind,
                retired: false,
                wal_segments: vec![],
                wal_floor_id: None,
                data: None,
                retired_ms: None,
            };
            assert_eq!(OwnerRecord::decode(&r.encode()).unwrap(), r);
        }
        let mine = OwnerRecord {
            term: 1,
            session: SessionId {
                node: "n".into(),
                inc: 1,
            },
            address: None,
            released: false,
            wal_kind: WalKind::SlateDb,
            retired: false,
            wal_segments: vec![],
            wal_floor_id: None,
            data: None,
            retired_ms: None,
        };
        let text = String::from_utf8(mine.encode()).unwrap();
        assert!(text.contains("wal_kind=slatedb\n"), "{text}");
    }

    /// **The range's WAL index.** Segments keep their order exactly, `node` may contain
    /// `/` and `:`, and `wal_floor_id` passes through intact.
    #[test]
    fn owner_record_round_trips_wal_segments() {
        let log = |node: &str, inc, stripe| LogId {
            session: SessionId {
                node: node.into(),
                inc,
            },
            stripe,
        };
        let r = OwnerRecord {
            term: 9,
            session: SessionId {
                node: "b".into(),
                inc: 2,
            },
            address: None,
            released: false,
            wal_kind: WalKind::Shared,
            retired: false,
            wal_segments: vec![
                WalSegment {
                    log: log("host:9/a", 1, 1),
                    first_wal_id: 4,
                },
                WalSegment {
                    log: log("b", 2, 0),
                    first_wal_id: 17,
                },
            ],
            wal_floor_id: Some(3),
            data: Some(7),
            retired_ms: None,
        };
        let text = String::from_utf8(r.encode()).unwrap();
        assert!(
            text.contains("wal_segment=host:9/a/1:1@4\nwal_segment=b/2:0@17\n"),
            "{text}"
        );
        assert!(text.contains("data=7\n"), "{text}");
        assert_eq!(OwnerRecord::decode(&r.encode()).unwrap(), r);
    }

    #[test]
    fn broken_wal_segment_rejects_the_record() {
        for seg in ["a/1:x@3", "a/1:0", "a/1@3", "a/1:0@-1"] {
            let raw = format!(
                "cairnflow-owner/1\nterm=1\nsession=a/1\nwal_kind=shared\nwal_segment={seg}\n"
            );
            assert!(OwnerRecord::decode(raw.as_bytes()).is_err(), "{seg}");
        }
    }

    #[test]
    fn broken_owner_records_are_rejected() {
        for raw in [
            &b"junk"[..],
            b"cairnflow-owner/1\nsession=a/1\n",
            b"cairnflow-owner/1\nterm=1\n",
            b"cairnflow-owner/1\nterm=1\nsession=no-inc\n",
            b"cairnflow-owner/2\nterm=1\nsession=a/1\nwal_kind=slatedb\n",
            b"cairnflow-owner/1\nterm=1\nsession=a/1\nwal_kind=slatedb\ndata=x\n",
            // Missing wal_kind: do not guess `slatedb`.
            b"cairnflow-owner/1\nterm=1\nsession=a/1\n",
            &[0xff, 0xfe][..],
        ] {
            assert!(OwnerRecord::decode(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn session_id_takes_inc_after_last_slash() {
        let id = SessionId::parse("host/12/345/7").unwrap();
        assert_eq!(id.node, "host/12/345");
        assert_eq!(id.inc, 7);
        assert_eq!(id.to_string(), "host/12/345/7");
        assert!(SessionId::parse("/3").is_none());
        assert!(SessionId::parse("a/x").is_none());
    }

    /// **`retired_ms`** passes through encode/decode; a format 9 record without that line
    /// reads as `None`, and a damaged value is `None` too (read leniently — D-d uses
    /// `last_modified` instead).
    #[test]
    fn owner_record_retired_ms_is_lenient() {
        let base =
            "cairnflow-owner/1\nterm=1\nsession=a/1\nreleased=1\nretired=1\nwal_kind=slatedb\n";
        let r =
            OwnerRecord::decode(format!("{base}retired_ms=1700000000123\n").as_bytes()).unwrap();
        assert_eq!(r.retired_ms, Some(1_700_000_000_123));
        assert_eq!(OwnerRecord::decode(&r.encode()).unwrap(), r);
        assert_eq!(
            OwnerRecord::decode(base.as_bytes()).unwrap().retired_ms,
            None
        );
        assert_eq!(
            OwnerRecord::decode(format!("{base}retired_ms=x\n").as_bytes())
                .unwrap()
                .retired_ms,
            None
        );
    }
}
