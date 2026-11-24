// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **GC barrier**: a party outside the D-d loop — a backup, an export, an operator who is
//! investigating — says "do not delete the Dbs of these ranges" for a bounded time.
//! Inspired by PD's service safepoint in TiDB: a barrier has an id, a TTL and a scope; the
//! holder renews it; TTL=0 removes it.
//!
//! One object per barrier, `<prefix>/gc-barriers/<id>`, as text:
//!
//! ```text
//! cairnflow-gc-barrier/1
//! holder=<who holds it>
//! protects=all | ranges:<id>,<id>,…
//! expires_ms=<expiry, ms since the epoch>
//! reason=<one line>
//! ```
//!
//! **Lenient read, absence means the default** (rule 2 of `AGENTS.md`, precedent
//! `range-windows`): a format 9 bucket without this directory reads as "no barrier".
//! Unknown keys are ignored — a later version may add fields.
//!
//! **Conservative where ambiguous** (rule 5): an object that cannot be read (an unknown
//! header from a newer binary, missing `protects`/`expires_ms`, garbage) **blocks every
//! range** and is never deleted by the GC runner on its own — a leak only costs space; the
//! operator sees it in `GET /admin/gc/barriers` and removes it with TTL=0.
//!
//! **No barrier is infinite.** The expiry is the wall clock of the node that set the
//! barrier; two machines a few seconds apart are acceptable with TTLs measured in minutes.

use {
    crate::{
        error::{Result, StoreError},
        partition_table::RangeId,
    },
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutPayload,
    },
    std::{collections::BTreeSet, sync::Arc},
};

pub const BARRIER_HEADER: &str = "cairnflow-gc-barrier/1";

/// Maximum length of a barrier's id.
pub const MAX_BARRIER_ID: usize = 64;

/// Which ranges a barrier covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Protects {
    /// Every range: GC deletes nothing while the barrier has not expired.
    All,
    /// Only these ranges.
    Ranges(BTreeSet<u32>),
}

impl Protects {
    pub fn covers(&self, r: RangeId) -> bool {
        match self {
            Self::All => true,
            Self::Ranges(s) => s.contains(&r.0),
        }
    }

    /// `all` or `ranges:<id>,<id>,…` (empty ids ⇒ broken: a barrier covering nothing is a
    /// typo).
    pub fn parse(s: &str) -> Option<Protects> {
        if s == "all" {
            return Some(Self::All);
        }
        let ids = s.strip_prefix("ranges:")?;
        let set: Option<BTreeSet<u32>> = ids.split(',').map(|i| i.trim().parse().ok()).collect();
        set.filter(|s| !s.is_empty()).map(Self::Ranges)
    }
}

impl std::fmt::Display for Protects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => f.write_str("all"),
            Self::Ranges(s) => {
                let ids: Vec<String> = s.iter().map(u32::to_string).collect();
                write!(f, "ranges:{}", ids.join(","))
            }
        }
    }
}

/// The content of one `gc-barriers/<id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcBarrier {
    pub holder: String,
    pub protects: Protects,
    pub expires_ms: u64,
    pub reason: String,
}

impl GcBarrier {
    pub fn expired(&self, now_ms: u64) -> bool {
        self.expires_ms <= now_ms
    }

    pub fn encode(&self) -> Vec<u8> {
        format!(
            "{BARRIER_HEADER}\nholder={}\nprotects={}\nexpires_ms={}\nreason={}\n",
            one_line(&self.holder),
            self.protects,
            self.expires_ms,
            one_line(&self.reason)
        )
        .into_bytes()
    }

    pub fn decode(raw: &[u8]) -> std::result::Result<GcBarrier, String> {
        let text = std::str::from_utf8(raw).map_err(|e| format!("not UTF-8: {e}"))?;
        let mut lines = text.lines();
        match lines.next() {
            Some(BARRIER_HEADER) => {}
            other => return Err(format!("header {other:?}, expected {BARRIER_HEADER}")),
        }
        let (mut holder, mut protects, mut expires_ms, mut reason) =
            (String::new(), None, None, String::new());
        for line in lines {
            match line.split_once('=') {
                Some(("holder", v)) => holder = v.to_string(),
                Some(("protects", v)) => {
                    protects =
                        Some(Protects::parse(v).ok_or_else(|| format!("broken protects: {v:?}"))?)
                }
                Some(("expires_ms", v)) => {
                    expires_ms = Some(v.parse().map_err(|_| format!("broken expires_ms: {v:?}"))?)
                }
                Some(("reason", v)) => reason = v.to_string(),
                _ => {}
            }
        }
        Ok(GcBarrier {
            holder,
            protects: protects.ok_or("missing protects")?,
            expires_ms: expires_ms.ok_or("missing expires_ms")?,
            reason,
        })
    }
}

/// A one-line value: newlines in `holder`/`reason` become spaces — nobody can insert a
/// second `protects=` line through `reason`.
fn one_line(s: &str) -> String {
    s.replace(['\n', '\r'], " ")
}

/// A valid id: 1..=64 characters `[A-Za-z0-9._-]`, not starting with `.`.
pub fn check_barrier_id(id: &str) -> std::result::Result<(), String> {
    let ok = !id.is_empty()
        && id.len() <= MAX_BARRIER_ID
        && !id.starts_with('.')
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid barrier id {id:?} — 1..={MAX_BARRIER_ID} characters [A-Za-z0-9._-], not starting with '.'"
        ))
    }
}

/// A barrier as read from the bucket.
#[derive(Clone, Debug)]
pub struct StoredBarrier {
    pub id: String,
    /// `Err` = could not be read: blocks every range, nobody deletes it on their own.
    pub barrier: std::result::Result<GcBarrier, String>,
}

impl StoredBarrier {
    /// Whether this barrier still blocks range `r` at `now_ms`. A barrier that cannot be
    /// read blocks.
    pub fn blocks(&self, r: RangeId, now_ms: u64) -> bool {
        match &self.barrier {
            Ok(b) => !b.expired(now_ms) && b.protects.covers(r),
            Err(_) => true,
        }
    }
}

fn barriers_dir(prefix: &Path) -> String {
    if prefix.as_ref().is_empty() {
        "gc-barriers".into()
    } else {
        format!("{}/gc-barriers", prefix.as_ref())
    }
}

fn barrier_path(prefix: &Path, id: &str) -> Path {
    Path::from(format!("{}/{id}", barriers_dir(prefix)))
}

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// Every barrier on the bucket, by id. Missing directory ⇒ empty.
pub async fn list_barriers(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Vec<StoredBarrier>> {
    let dir = Path::from(barriers_dir(prefix));
    let listed: Vec<_> = store.list(Some(&dir)).try_collect().await.map_err(io)?;
    let mut out = Vec::new();
    for m in listed {
        let Some(id) = m.location.filename().map(str::to_string) else {
            continue;
        };
        if let Some(b) = read_barrier(store, prefix, &id).await? {
            out.push(b);
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

/// One barrier. `Ok(None)` = not there (or just removed between the LIST and the GET).
pub async fn read_barrier(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &str,
) -> Result<Option<StoredBarrier>> {
    let raw = match store.get(&barrier_path(prefix, id)).await {
        Ok(g) => g.bytes().await.map_err(io)?,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    Ok(Some(StoredBarrier {
        id: id.to_string(),
        barrier: GcBarrier::decode(&raw),
    }))
}

/// Sets (or renews) barrier `id`. Overwrites unconditionally: the holder owns the id.
pub async fn put_barrier(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &str,
    b: &GcBarrier,
) -> Result<()> {
    check_barrier_id(id).map_err(StoreError::Io)?;
    store
        .put(&barrier_path(prefix, id), PutPayload::from(b.encode()))
        .await
        .map(|_| ())
        .map_err(io)
}

/// Removes barrier `id`. If it is not there, nothing happens.
pub async fn delete_barrier(store: &Arc<dyn ObjectStore>, prefix: &Path, id: &str) -> Result<()> {
    check_barrier_id(id).map_err(StoreError::Io)?;
    match store.delete(&barrier_path(prefix, id)).await {
        Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
        Err(e) => Err(io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_roundtrips_and_reads_leniently() {
        for protects in [Protects::All, Protects::Ranges([3, 17].into())] {
            let b = GcBarrier {
                holder: "backup-1".into(),
                protects,
                expires_ms: 1_700_000_000_000,
                reason: "line one\nline two".into(),
            };
            let back = GcBarrier::decode(&b.encode()).unwrap();
            assert_eq!(
                back.reason, "line one line two",
                "reason must not insert lines"
            );
            assert_eq!(back.protects, b.protects);
            assert_eq!(back.expires_ms, b.expires_ms);
        }
        // Unknown keys are ignored (lenient read).
        let raw = format!("{BARRIER_HEADER}\nprotects=ranges:4\nexpires_ms=9\nfuture=x\n");
        let b = GcBarrier::decode(raw.as_bytes()).unwrap();
        assert!(
            b.protects.covers(crate::ids::PartitionId(4))
                && !b.protects.covers(crate::ids::PartitionId(5))
        );
    }

    /// Ambiguous ⇒ block: unknown header, missing field, empty `protects`.
    #[test]
    fn unreadable_barrier_blocks_everything() {
        for raw in [
            "cairnflow-gc-barrier/2\nprotects=ranges:1\nexpires_ms=1\n".to_string(),
            format!("{BARRIER_HEADER}\nprotects=ranges:1\n"),
            format!("{BARRIER_HEADER}\nprotects=ranges:\nexpires_ms=99\n"),
            format!("{BARRIER_HEADER}\nexpires_ms=99\n"),
        ] {
            let s = StoredBarrier {
                id: "x".into(),
                barrier: GcBarrier::decode(raw.as_bytes()),
            };
            assert!(s.barrier.is_err(), "{raw:?}");
            assert!(s.blocks(crate::ids::PartitionId(42), u64::MAX), "{raw:?}");
        }
    }

    #[test]
    fn expired_barrier_blocks_nothing() {
        let s = StoredBarrier {
            id: "x".into(),
            barrier: Ok(GcBarrier {
                holder: String::new(),
                protects: Protects::All,
                expires_ms: 100,
                reason: String::new(),
            }),
        };
        assert!(s.blocks(crate::ids::PartitionId(1), 99));
        assert!(!s.blocks(crate::ids::PartitionId(1), 100));
    }

    #[test]
    fn barrier_ids() {
        for ok in ["a", "backup-2026.10.11", "x_y"] {
            assert!(check_barrier_id(ok).is_ok(), "{ok}");
        }
        for bad in ["", ".hidden", "a/b", "a b", &"x".repeat(65)] {
            assert!(check_barrier_id(bad).is_err(), "{bad:?}");
        }
    }
}
