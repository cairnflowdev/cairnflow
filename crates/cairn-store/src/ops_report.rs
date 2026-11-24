// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Operations reports** `ops-reports/<id>`: the result of one `cairn-ctl …
//! --report-to-bucket <id>` command run **inside** the cell (a one-off job of the cell),
//! so a control plane can read it with a `provision` key through
//! `GET /admin/ops-reports/{id}` instead of holding bucket credentials and running
//! `cairn-ctl` itself.
//!
//! Layout — a one-line header, then JSON built by `cairn-ctl`:
//!
//! ```text
//! cairnflow-ops-report/1
//! {"id": "…", "command": "backup", "exitCode": 0, "result": {…}, …}
//! ```
//!
//! **Read leniently, absence means "none yet"** (rule 2 of `AGENTS.md`, precedent
//! `range-windows`): a bucket without this directory reads as "no reports yet"; unknown
//! JSON keys pass through the route verbatim. No binary reads it to run.
//!
//! **Written once** with `PutMode::Create` + [`crate::cas::put_resolving`] (rule 7): the
//! `id` is assigned by the caller (the control plane, unique enough), and the content
//! carries the id and the start/end times — identical content under the same name is our
//! own write. A name that exists with different content ⇒ [`Written::Taken`]: the id is
//! used, no overwrite.
//!
//! **Pruning** ([`prune`]): after each write, `cairn-ctl` keeps the [`KEEP_COUNT`] newest
//! reports and drops reports older than [`KEEP_AGE`] — age is compared by the `last_modified`
//! of **the store itself** (the report just written is the reference), not by the clock of
//! the machine running it. Reports are not customer data (no secrets, no run data), so they
//! do not go through the trash; only objects named exactly `ops-reports/<valid id>` are
//! deleted, and when in doubt (the reference is not in the listing) ⇒ nothing is deleted
//! (rule 5).

use {
    crate::error::{Result, StoreError},
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode},
    std::{sync::Arc, time::Duration},
};

pub const OPS_REPORT_HEADER: &str = "cairnflow-ops-report/1";

/// Directory under the cluster prefix.
pub const DIR: &str = "ops-reports";

/// Maximum id length.
pub const MAX_ID: usize = 128;

/// Number of newest reports [`prune`] always keeps (within [`KEEP_AGE`]).
pub const KEEP_COUNT: usize = 256;

/// Reports older than the report just written by this much are dropped by [`prune`].
pub const KEEP_AGE: Duration = Duration::from_secs(30 * 24 * 3600);

/// Size cap of one report (bytes) — a report is a JSON summary, not data.
pub const MAX_REPORT: usize = 1024 * 1024;

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// A valid id: 1..=[`MAX_ID`] characters `A-Z a-z 0-9 - _ .`, starting with a letter or
/// digit — one path segment, no `/`, not `.`/`..`, unchanged by encoding.
pub fn check_id(id: &str) -> std::result::Result<(), String> {
    let ok = !id.is_empty()
        && id.len() <= MAX_ID
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "report id must be 1..={MAX_ID} characters A-Z a-z 0-9 - _ ., starting with a letter or digit"
        ))
    }
}

fn dir(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(DIR)
    } else {
        Path::from(format!("{}/{DIR}", prefix.as_ref()))
    }
}

/// `<prefix>/ops-reports/<id>` — `id` has passed [`check_id`].
pub fn report_path(prefix: &Path, id: &str) -> Path {
    Path::from(format!("{}/{id}", dir(prefix).as_ref()))
}

/// Header + JSON.
pub fn encode(json: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(OPS_REPORT_HEADER.len() + 1 + json.len());
    out.extend_from_slice(OPS_REPORT_HEADER.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(json);
    out.into()
}

/// `(header, JSON body)`. The header must be of the `cairnflow-ops-report/<n>` family — a
/// higher number still reads (leniently: the body is JSON, new keys pass through verbatim).
pub fn decode(raw: &[u8]) -> std::result::Result<(String, Bytes), String> {
    let nl = raw
        .iter()
        .position(|b| *b == b'\n')
        .ok_or("no header line")?;
    let header = std::str::from_utf8(&raw[..nl]).map_err(|_| "header is not UTF-8")?;
    let family = OPS_REPORT_HEADER
        .rsplit_once('/')
        .map(|(f, _)| f)
        .unwrap_or(OPS_REPORT_HEADER);
    match header
        .strip_prefix(family)
        .and_then(|r| r.strip_prefix('/'))
    {
        Some(n) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => {}
        _ => return Err(format!("header {header:?}, not {family}/<n>")),
    }
    Ok((header.to_string(), Bytes::copy_from_slice(&raw[nl + 1..])))
}

/// Whether report `id` exists.
pub async fn exists(store: &Arc<dyn ObjectStore>, prefix: &Path, id: &str) -> Result<bool> {
    check_id(id).map_err(StoreError::Io)?;
    match store.head(&report_path(prefix, id)).await {
        Ok(_) => Ok(true),
        Err(OsError::NotFound { .. }) => Ok(false),
        Err(e) => Err(io(e)),
    }
}

/// Result of [`write`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Written {
    Written,
    /// The id already has a different report: nothing written.
    Taken,
}

/// Writes report `id` once (`Create`, ambiguity resolved by re-reading — rule 7).
pub async fn write(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &str,
    json: &[u8],
) -> Result<Written> {
    check_id(id).map_err(StoreError::Io)?;
    if json.len() > MAX_REPORT {
        return Err(StoreError::Io(format!(
            "report {id} is {} bytes, cap {MAX_REPORT}",
            json.len()
        )));
    }
    match crate::cas::put_resolving(
        store,
        &report_path(prefix, id),
        encode(json),
        PutMode::Create,
    )
    .await
    {
        Ok(crate::cas::Cas::Written(_)) => Ok(Written::Written),
        Ok(crate::cas::Cas::Lost) => Ok(Written::Taken),
        Err(e) => Err(io(e)),
    }
}

/// A stored report: `Ok((header, JSON))`, or `Err(reason)` when it cannot be read (unknown
/// header, garbage) — the reader reports it, no guessing.
pub type Stored = std::result::Result<(String, Bytes), String>;

/// Report `id`. `Ok(None)` = none yet.
pub async fn read(store: &Arc<dyn ObjectStore>, prefix: &Path, id: &str) -> Result<Option<Stored>> {
    check_id(id).map_err(StoreError::Io)?;
    match store.get(&report_path(prefix, id)).await {
        Ok(g) => {
            if g.meta.size > MAX_REPORT as u64 + 64 {
                return Ok(Some(Err(format!(
                    "{} bytes, over the cap {MAX_REPORT}",
                    g.meta.size
                ))));
            }
            let raw = g.bytes().await.map_err(io)?;
            Ok(Some(decode(&raw)))
        }
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(e) => Err(io(e)),
    }
}

/// A report as the listing sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportMeta {
    pub id: String,
    /// `last_modified` theo store (ms).
    pub last_modified_ms: u64,
    pub size: u64,
}

/// Every report, newest first (by id for equal times). Objects under `ops-reports/` whose
/// name is not a valid id (nested directories, unknown characters) are skipped — not ours.
pub async fn list(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<ReportMeta>> {
    let d = dir(prefix);
    let listed: Vec<_> = store.list(Some(&d)).try_collect().await.map_err(io)?;
    let base = format!("{}/", d.as_ref());
    let mut out: Vec<ReportMeta> = listed
        .into_iter()
        .filter_map(|m| {
            let id = m.location.as_ref().strip_prefix(&base)?;
            check_id(id).ok()?;
            Some(ReportMeta {
                id: id.to_string(),
                last_modified_ms: m.last_modified.timestamp_millis().max(0) as u64,
                size: m.size,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b.last_modified_ms
            .cmp(&a.last_modified_ms)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(out)
}

/// Result of [`prune`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pruned {
    pub deleted: Vec<String>,
    /// `(id, error)` — the delete failed: left for next time.
    pub failed: Vec<(String, String)>,
    /// Nothing deleted because it could not be proven (the reason).
    pub skipped: Option<String>,
}

/// Which reports [`prune`] drops, from the list `all` (newest first) with the reference
/// `newest` being the report just written: beyond the [`KEEP_COUNT`] newest, or older than
/// the reference by more than [`KEEP_AGE`]. Never `newest` itself.
pub fn prune_plan(all: &[ReportMeta], newest: &str) -> std::result::Result<Vec<String>, String> {
    let Some(anchor) = all.iter().find(|m| m.id == newest) else {
        return Err(format!(
            "the listing does not show the report just written {newest} — no age reference, deleting nothing"
        ));
    };
    let age = KEEP_AGE.as_millis() as u64;
    Ok(all
        .iter()
        .enumerate()
        .filter(|(rank, m)| {
            m.id != newest
                && (*rank >= KEEP_COUNT
                    || anchor.last_modified_ms.saturating_sub(m.last_modified_ms) > age)
        })
        .map(|(_, m)| m.id.clone())
        .collect())
}

/// Prunes old reports after writing `newest` ([`prune_plan`]). A failed listing ⇒ `Err`
/// (nothing deleted).
pub async fn prune(store: &Arc<dyn ObjectStore>, prefix: &Path, newest: &str) -> Result<Pruned> {
    let all = list(store, prefix).await?;
    let plan = match prune_plan(&all, newest) {
        Ok(p) => p,
        Err(why) => {
            return Ok(Pruned {
                skipped: Some(why),
                ..Pruned::default()
            })
        }
    };
    let mut out = Pruned::default();
    for id in plan {
        // Not customer data (module doc): outside the trash, a line in
        // `scripts/lint-delete.allow`.
        match store.delete(&report_path(prefix, &id)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => out.deleted.push(id),
            Err(e) => out.failed.push((id, e.to_string())),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use {super::*, slatedb::object_store::memory::InMemory};

    #[test]
    fn ids_are_single_safe_segments() {
        for ok in ["a", "op-2026-11-01T00.00_x", "0", &"z".repeat(MAX_ID)] {
            assert!(check_id(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            ".hidden",
            "-x",
            "a/b",
            "a b",
            "a%2F",
            "π",
            &"z".repeat(MAX_ID + 1),
        ] {
            assert!(check_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn encode_decode_and_header_family() {
        let raw = encode(br#"{"a":1}"#);
        let (h, body) = decode(&raw).unwrap();
        assert_eq!(h, OPS_REPORT_HEADER);
        assert_eq!(&body[..], br#"{"a":1}"#);
        // A higher number: still read (leniently).
        assert!(decode(b"cairnflow-ops-report/2\n{}").is_ok());
        for bad in [
            &b"cairnflow-ops-report\n{}"[..],
            b"cairnflow-ops-reports/1\n{}",
            b"cairnflow-ops-report/x\n{}",
            b"{}",
        ] {
            assert!(decode(bad).is_err(), "{:?}", std::str::from_utf8(bad));
        }
    }

    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy("ops-reports/<id>").map(|p| p.header),
            Some(OPS_REPORT_HEADER)
        );
        assert!(!crate::format::unlisted_object("ops-reports/op-1"));
    }

    /// Write, read back, a used id ⇒ `Taken` and no overwrite; absent ⇒ `None`.
    #[tokio::test]
    async fn write_once_and_read_back() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::from("cell");
        assert!(read(&store, &p, "op-1").await.unwrap().is_none());
        assert!(!exists(&store, &p, "op-1").await.unwrap());
        assert_eq!(
            write(&store, &p, "op-1", br#"{"n":1}"#).await.unwrap(),
            Written::Written
        );
        assert!(exists(&store, &p, "op-1").await.unwrap());
        assert_eq!(
            write(&store, &p, "op-1", br#"{"n":2}"#).await.unwrap(),
            Written::Taken
        );
        let (_, body) = read(&store, &p, "op-1").await.unwrap().unwrap().unwrap();
        assert_eq!(&body[..], br#"{"n":1}"#);
        assert!(write(&store, &p, "../x", b"{}").await.is_err());
        let all = list(&store, &p).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "op-1");
    }

    /// Rule 7: a `Create` that was written while the answer says otherwise (a 5xx error, or
    /// the `AlreadyExists` of a retry). Must see: `Written`, exactly one object, our
    /// content.
    #[tokio::test]
    async fn ambiguous_create_is_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let p = Path::from("cell");
            faulty.add_rule(Rule::ambiguous_put("ops-reports/", reply));
            let got = write(&store, &p, "op-7", br#"{"id":"op-7","startedMs":1}"#)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: report written but an error reported: {e}"));
            assert_eq!(got, Written::Written, "{reply:?}");
            let all = list(&store, &p).await.unwrap();
            assert_eq!(all.len(), 1, "{reply:?}: {all:?}");
            let (_, body) = read(&store, &p, "op-7").await.unwrap().unwrap().unwrap();
            assert_eq!(&body[..], br#"{"id":"op-7","startedMs":1}"#);
        }
    }

    fn meta(id: &str, ms: u64) -> ReportMeta {
        ReportMeta {
            id: id.into(),
            last_modified_ms: ms,
            size: 1,
        }
    }

    /// Count and age caps; the reference is the report just written; reference not seen ⇒
    /// nothing deleted.
    #[test]
    fn prune_plan_caps_count_and_age() {
        let day = 86_400_000u64;
        let now = 100 * day;
        let mut all: Vec<ReportMeta> = (0..KEEP_COUNT + 5)
            .map(|i| meta(&format!("r{i:04}"), now - i as u64))
            .collect();
        all.push(meta("old", now - 31 * day));
        all.push(meta("month", now - 29 * day));
        let mut sorted = all.clone();
        sorted.sort_by_key(|m| std::cmp::Reverse(m.last_modified_ms));
        let plan = prune_plan(&sorted, "r0000").unwrap();
        // Beyond the 256 newest: r0256..r0260, "month", "old".
        assert_eq!(plan.len(), 7, "{plan:?}");
        assert!(plan.contains(&"old".to_string()));
        assert!(!plan.contains(&"r0000".to_string()));
        // Few reports: age only.
        let few = vec![
            meta("new", now),
            meta("month", now - 29 * day),
            meta("old", now - 31 * day),
        ];
        assert_eq!(prune_plan(&few, "new").unwrap(), vec!["old".to_string()]);
        // The reference's clock: the report just written being the oldest of all (store
        // clock skew) is still the reference, and is never deleted.
        let skew = vec![meta("x", now), meta("new", now - 40 * day)];
        assert!(prune_plan(&skew, "new").unwrap().is_empty());
        assert!(prune_plan(&few, "missing").is_err());
    }

    /// Pruning only touches `ops-reports/<valid id>` — nested objects or unknown names
    /// there, and objects outside the directory, stay whole.
    #[tokio::test]
    async fn prune_deletes_only_own_names() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::from("cell");
        for foreign in [
            "cell/ops-reports/a/b",
            "cell/ops-reports-x",
            "cell/audit/ctl/1",
        ] {
            store
                .put(&Path::from(foreign), Bytes::from_static(b"x").into())
                .await
                .unwrap();
        }
        write(&store, &p, "only", b"{}").await.unwrap();
        let got = prune(&store, &p, "only").await.unwrap();
        assert_eq!(got, Pruned::default());
        let left: Vec<_> = store.list(Some(&p)).try_collect().await.unwrap();
        assert_eq!(left.len(), 4, "{left:?}");
        let got = prune(&store, &p, "missing").await.unwrap();
        assert!(got.skipped.is_some() && got.deleted.is_empty());
    }
}
