// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The data plane's audit log on the bucket.
//!
//! Every record is **one small object** under `audit/tenant/<tenant>/<yyyy-mm-dd>/` (lenient
//! read; absent = nothing yet). The `audit/tenant/` prefix keeps clear of `cairn-ctl`'s
//! `audit/ctl/` (a tenant named `ctl` is valid). Two sources:
//!
//! - **Actions inside a range** (cancel, purge, unsuspend, restart, signal, schedules): a
//!   [`crate::keyspace::Table::Audit`] row is written **in the same commit** as the action, then
//!   the range's export loop writes the object `r-<row key hex>.json` — the name is a function of
//!   the key, so across every leader change the object is written **exactly once**
//!   ([`write_once`]); the row is deleted only after the object reads back matching.
//! - **Actions on bucket objects** (deployments, keys, `tenants`, rules, console sessions):
//!   `<ts>-<node>-<boot>-<seq>.intent` **before** the action, `….done` (the outcome) after.
//!   There is never an action without an intent.
//!
//! The content is JSON built by the server; here it is only bytes. The row value in the Db also
//! carries `tenant` and the timestamp so the export loop can compute the object name without
//! understanding JSON ([`encode_row`]).

use {
    crate::{
        cas::{put_resolving, Cas},
        StoreError,
    },
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode},
    std::sync::Arc,
};

/// Root directory (under the cluster's prefix).
pub const DIR: &str = "audit/tenant";

type Result<T> = std::result::Result<T, StoreError>;

fn io(e: impl std::fmt::Display) -> StoreError {
    StoreError::Io(e.to_string())
}

/// `yyyy-mm-dd` (UTC) of a ms timestamp — proleptic Gregorian calendar, reads no clock, depends on
/// no date/time library.
pub fn day_of(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    // The `civil_from_days` algorithm (H. Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// A tenant's directory for one day.
pub fn day_dir(prefix: &Path, tenant: &str, day: &str) -> Path {
    under(prefix, &format!("{DIR}/{tenant}/{day}"))
}

fn under(prefix: &Path, rel: &str) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(rel)
    } else {
        Path::from(format!("{}/{rel}", prefix.as_ref()))
    }
}

/// The object of one record: `audit/tenant/<tenant>/<day of ts>/<name>`.
pub fn object_path(prefix: &Path, tenant: &str, ts_ms: u64, name: &str) -> Path {
    under(prefix, &format!("{DIR}/{tenant}/{}/{name}", day_of(ts_ms)))
}

/// Value of a `Table::Audit` row: `len(tenant) u16 BE ‖ tenant ‖ ts_ms u64 BE ‖ record`. A row
/// that does not decode is left alone by the export loop (verify reports it).
pub fn encode_row(tenant: &str, ts_ms: u64, record: &[u8]) -> Vec<u8> {
    let t = tenant.as_bytes();
    let mut v = Vec::with_capacity(2 + t.len() + 8 + record.len());
    v.extend_from_slice(&(t.len() as u16).to_be_bytes());
    v.extend_from_slice(t);
    v.extend_from_slice(&ts_ms.to_be_bytes());
    v.extend_from_slice(record);
    v
}

/// `(tenant, ts_ms, record)` of a row value.
pub fn decode_row(v: &[u8]) -> Option<(&str, u64, &[u8])> {
    let n = u16::from_be_bytes(v.get(..2)?.try_into().ok()?) as usize;
    let tenant = std::str::from_utf8(v.get(2..2 + n)?).ok()?;
    let ts = u64::from_be_bytes(v.get(2 + n..10 + n)?.try_into().ok()?);
    Some((tenant, ts, v.get(10 + n..)?))
}

/// Object name of a row inside a range: `r-<row key hex>.json`.
pub fn row_name(row_key: &[u8]) -> String {
    let mut s = String::with_capacity(2 + row_key.len() * 2 + 5);
    s.push_str("r-");
    for b in row_key {
        s.push_str(&format!("{b:02x}"));
    }
    s.push_str(".json");
    s
}

/// The object of a `Table::Audit` row (key + value) — `None` when the value is broken.
pub fn row_object(prefix: &Path, row_key: &[u8], value: &[u8]) -> Option<(Path, Bytes)> {
    let (tenant, ts, record) = decode_row(value)?;
    Some((
        object_path(prefix, tenant, ts, &row_name(row_key)),
        Bytes::copy_from_slice(record),
    ))
}

/// Outcome of [`write_once`].
#[derive(Debug, PartialEq, Eq)]
pub enum Written {
    /// The object carries exactly this content (we just wrote it, or an earlier write of this
    /// same row — the previous leader died between writing and deleting the row).
    Done,
    /// The object already has **different content**: must never happen with a deterministic
    /// name; the row is kept, `verify` reports it.
    Conflict,
}

/// Writes `body` to `path` **exactly once**: `PutMode::Create`, with ambiguity resolved by
/// reading back and comparing bytes (`cas::put_resolving`). Same content already there ⇒
/// [`Written::Done`] — re-exporting after a leader change does not duplicate the object.
pub async fn write_once(store: &Arc<dyn ObjectStore>, path: &Path, body: Bytes) -> Result<Written> {
    match put_resolving(store, path, body, PutMode::Create)
        .await
        .map_err(io)?
    {
        Cas::Written(_) => Ok(Written::Done),
        Cas::Lost => Ok(Written::Conflict),
    }
}

/// Writes `body` to `path` with `Create` (the intent/outcome record of an action on an object —
/// the name carries `(node, boot, seq)`, so nobody else can write it).
pub async fn put_new(store: &Arc<dyn ObjectStore>, path: &Path, body: Bytes) -> Result<()> {
    match write_once(store, path, body).await? {
        Written::Done => Ok(()),
        Written::Conflict => Err(StoreError::Io(format!(
            "audit: {path} already has different content — intent names must not collide"
        ))),
    }
}

/// Export target of a range's audit rows: the main store and the cluster's prefix.
#[derive(Clone)]
pub struct AuditStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl std::fmt::Debug for AuditStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditStore")
            .field("prefix", &self.prefix.as_ref())
            .finish()
    }
}

impl AuditStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        Self {
            store,
            prefix: prefix.clone(),
        }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    /// Writes the object of a `Table::Audit` row exactly once ([`write_once`]). `None` = the row
    /// value is broken (left alone, `verify` reports it).
    pub async fn write_row(&self, key: &[u8], value: &[u8]) -> Result<Option<(Path, Written)>> {
        let Some((path, body)) = row_object(&self.prefix, key, value) else {
            return Ok(None);
        };
        let w = write_once(&self.store, &path, body).await?;
        Ok(Some((path, w)))
    }
}

/// Every object (path, bytes) in `tenant`'s `day` directory, at most `cap`, in name order.
pub async fn list_day(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    day: &str,
    cap: usize,
) -> Result<Vec<(Path, Bytes)>> {
    let dir = day_dir(prefix, tenant, day);
    let mut metas: Vec<_> = store
        .list(Some(&dir))
        .try_collect::<Vec<_>>()
        .await
        .map_err(io)?;
    metas.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
    let mut out = Vec::new();
    for m in metas.into_iter().take(cap) {
        let got = store.get(&m.location).await.map_err(io)?;
        let bytes = got.bytes().await.map_err(io)?;
        out.push((m.location, bytes));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_day() {
        assert_eq!(day_of(0), "1970-01-01");
        assert_eq!(day_of(951_782_400_000), "2000-02-29");
        assert_eq!(day_of(1_793_536_986_452), "2026-11-01");
        assert_eq!(day_of(1_793_577_599_999), "2026-11-01");
        assert_eq!(day_of(1_793_577_600_000), "2026-11-02");
    }

    #[test]
    fn row_roundtrip_and_deterministic_name() {
        let v = encode_row("acme", 1_793_536_986_452, b"{\"a\":1}");
        assert_eq!(
            decode_row(&v),
            Some(("acme", 1_793_536_986_452, &b"{\"a\":1}"[..]))
        );
        let (p, body) = row_object(&Path::from("pre"), &[0, 1, 0xab], &v).unwrap();
        assert_eq!(p.as_ref(), "pre/audit/tenant/acme/2026-11-01/r-0001ab.json");
        assert_eq!(&body[..], b"{\"a\":1}");
        assert!(decode_row(b"\x00\x09ab").is_none());
    }

    /// An ambiguous `Create` (the store retried after the write landed, or an error after the
    /// write) is resolved by reading back — `Done`, not an error; rewriting the same content is
    /// also `Done`; different content ⇒ `Conflict`, the object stays as it was.
    #[tokio::test]
    async fn writes_exactly_once_through_ambiguity() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let p = object_path(&Path::from(""), "acme", 0, "r-01.json");
            faulty.add_rule(Rule::ambiguous_put("audit/tenant", reply));
            assert_eq!(
                write_once(&store, &p, Bytes::from_static(b"x"))
                    .await
                    .unwrap(),
                Written::Done,
                "{reply:?}"
            );
            assert_eq!(
                write_once(&store, &p, Bytes::from_static(b"x"))
                    .await
                    .unwrap(),
                Written::Done
            );
            assert_eq!(
                write_once(&store, &p, Bytes::from_static(b"y"))
                    .await
                    .unwrap(),
                Written::Conflict
            );
            let got = store.get(&p).await.unwrap().bytes().await.unwrap();
            assert_eq!(&got[..], b"x");
        }
    }
}
