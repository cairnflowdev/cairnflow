// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Archiving an invocation before it is purged, and the tamper-evident hash chain.
//!
//! A purged invocation (retention expired, or purged by hand) leaves **one** object
//! `archive/<tenant>/<yyyy-mm-dd>/<id-hex>` in the main store: the raw journal + the final
//! status, zstd-compressed. Written with `PutMode::Create`. The object is written **before**
//! the delete commit; if the write fails, nothing is deleted.
//!
//! **An existing object is read back and compared.** The ids of workflows and of calls with an
//! idempotency key are deterministic: purged and then called again yields a new invocation
//! **with the same id**, and its purge may fall on the same day. So `AlreadyExists` has two
//! meanings: (a) it belongs to this very run — an earlier purge died between the PUT and the
//! delete commit, or the `object_store` client retried a PUT that had already been applied —
//! then we are done; (b) it belongs to another run with the same id — then this run is written
//! to the next slot `<id-hex>.1`, `<id-hex>.2`, … Two runs are "the same" when everything but
//! `purged_at_ms` matches ([`ArchiveRecord::same_run`]): each purge attempt resets that
//! timestamp. An object that does not decode in a slot cannot be proven ours: write to the
//! next slot — one extra object is better than one lost journal (delete only when provable).
//!
//! An `<id-hex>.<n>` object is a **new, leniently read** object: absent means the id ran only
//! once that day, exactly like an old bucket. Its inner layout is identical to the original
//! object. An old binary does not know them: it only reads the first run.
//!
//! **The date in the path** is the UTC date of the **purge deadline** (the `purges` row), not
//! of when the purge runs: the deadline is fixed, so a purge that re-runs after midnight still
//! hits the same old object. A manual purge (no deadline) takes the date at purge time. The
//! purge deadline is always ≤ the purge time, so a reader searching backwards from today
//! finds it.
//!
//! Layout (before compression) — self-describing, no serde needed:
//!
//! ```text
//! cairnflow-archive/1\n
//! id[24] ‖ var(tenant) ‖ purged_at_ms u64 ‖ var(status)
//! ‖ has_chain u8 [‖ len u32 ‖ hash[32]]
//! ‖ n u32 ‖ n × (index u32 ‖ var32(raw entry))
//! ```
//!
//! `var` = u16 length then bytes; `var32` = u32 length then bytes. Entries are bytes **exactly
//! as in the journal** — what the hash chain covers.

use {
    crate::{error::StoreError, scope::TenantId, InvocationId, Result},
    bytes::{Buf, BufMut, Bytes, BytesMut},
    sha2::{Digest, Sha256},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    },
    std::sync::Arc,
};

pub(crate) const HEADER: &[u8] = b"cairnflow-archive/1\n";
/// zstd compression level: journals are highly repetitive text/protobuf — level 3 (zstd's
/// default) already gets most of it; higher levels cost background-thread CPU for a few percent.
const LEVEL: i32 = 3;

/// Cap on the number of runs of one id archived under the same day. A deterministic id that
/// re-runs and is purged more than this many times in one UTC day makes the next purge fail —
/// the journal stays, the purge deadline remains — instead of overwriting or skipping the
/// archive.
const MAX_RUNS_PER_DAY: u32 = 64;

/// The chain's starting hash, before the first entry.
pub const CHAIN_SEED: [u8; 32] = [0u8; 32];

/// One step of the hash chain: `SHA-256(previous hash ‖ index ‖ raw entry)`. The index goes
/// into the hash so that swapping two entries also breaks the chain.
pub fn chain_step(prev: &[u8; 32], index: u32, entry: &[u8]) -> [u8; 32] {
    Sha256::new()
        .chain_update(prev)
        .chain_update(index.to_be_bytes())
        .chain_update(entry)
        .finalize()
        .into()
}

/// The chain of a whole journal `(index, raw entry)`, from [`CHAIN_SEED`].
pub fn chain_of<'a>(entries: impl IntoIterator<Item = (u32, &'a [u8])>) -> (u32, [u8; 32]) {
    let mut h = CHAIN_SEED;
    let mut n = 0u32;
    for (i, e) in entries {
        h = chain_step(&h, i, e);
        n += 1;
    }
    (n, h)
}

/// Content of one archive object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveRecord {
    pub id: InvocationId,
    pub tenant: TenantId,
    /// Purge time (wall-clock ms).
    pub purged_at_ms: u64,
    /// Raw value of the status row at purge time (`InvocationStatus::encode`).
    pub status: Bytes,
    /// Raw journal, in index order.
    pub journal: Vec<(u32, Bytes)>,
    /// Head of the hash chain at purge time — `None` when the tenant has no chain enabled.
    pub chain: Option<(u32, [u8; 32])>,
}

fn bad(reason: impl Into<String>) -> StoreError {
    StoreError::LeaseCorrupt {
        reason: format!("broken archive: {}", reason.into()),
    }
}

impl ArchiveRecord {
    /// Compressed bytes, ready to PUT.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(256);
        b.put_slice(HEADER);
        b.put_slice(&self.id);
        let t = self.tenant.as_str().as_bytes();
        b.put_u16(t.len() as u16);
        b.put_slice(t);
        b.put_u64(self.purged_at_ms);
        b.put_u32(self.status.len() as u32);
        b.put_slice(&self.status);
        match &self.chain {
            Some((len, hash)) => {
                b.put_u8(1);
                b.put_u32(*len);
                b.put_slice(hash);
            }
            None => b.put_u8(0),
        }
        b.put_u32(self.journal.len() as u32);
        for (i, e) in &self.journal {
            b.put_u32(*i);
            b.put_u32(e.len() as u32);
            b.put_slice(e);
        }
        zstd::encode_all(b.as_ref(), LEVEL).expect("in-memory compression cannot fail")
    }

    pub fn decode(raw: &[u8]) -> Result<Self> {
        let plain = zstd::decode_all(raw).map_err(|e| bad(format!("cannot decompress: {e}")))?;
        let mut b = Bytes::from(plain);
        if !b.starts_with(HEADER) {
            let first = b.split(|c| *c == b'\n').next().unwrap_or_default();
            let ours = std::str::from_utf8(&HEADER[..HEADER.len() - 1]).expect("header ASCII");
            if let Some(e) = std::str::from_utf8(first)
                .ok()
                .and_then(|h| crate::format::newer_header("archive/<tenant>/<day>/<id>", ours, h))
            {
                return Err(e);
            }
            return Err(bad("missing header"));
        }
        b.advance(HEADER.len());
        let need = |b: &Bytes, n: usize| {
            if b.remaining() < n {
                Err(bad("truncated"))
            } else {
                Ok(())
            }
        };
        need(&b, 24 + 2)?;
        let mut id = [0u8; 24];
        b.copy_to_slice(&mut id);
        let tlen = b.get_u16() as usize;
        need(&b, tlen)?;
        let tenant = std::str::from_utf8(&b.split_to(tlen))
            .ok()
            .and_then(|t| TenantId::new(t).ok())
            .ok_or_else(|| bad("tenant"))?;
        need(&b, 8 + 4)?;
        let purged_at_ms = b.get_u64();
        let slen = b.get_u32() as usize;
        need(&b, slen + 1)?;
        let status = b.split_to(slen);
        let chain = match b.get_u8() {
            0 => None,
            _ => {
                need(&b, 4 + 32)?;
                let len = b.get_u32();
                let mut h = [0u8; 32];
                b.copy_to_slice(&mut h);
                Some((len, h))
            }
        };
        need(&b, 4)?;
        let n = b.get_u32() as usize;
        let mut journal = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            need(&b, 8)?;
            let i = b.get_u32();
            let len = b.get_u32() as usize;
            need(&b, len)?;
            journal.push((i, b.split_to(len)));
        }
        if b.has_remaining() {
            return Err(bad("trailing bytes"));
        }
        Ok(Self {
            id,
            tenant,
            purged_at_ms,
            status,
            journal,
            chain,
        })
    }

    /// The same run as `other`: every field but `purged_at_ms`. Two runs of a deterministic id
    /// that match in both status and every journal entry need only one object for both — no
    /// byte is lost.
    pub fn same_run(&self, other: &Self) -> bool {
        self.id == other.id
            && self.tenant == other.tenant
            && self.status == other.status
            && self.chain == other.chain
            && self.journal == other.journal
    }

    /// Checks the chain: recomputes it from the journal in the archive and compares it with the
    /// recorded chain head. `None` = the archive carries no chain (tenant has none enabled).
    ///
    /// The chain covers entries `0..len` — entries written after the chain was enabled are not
    /// covered; for a tenant that had it from the start, that is the whole journal.
    pub fn verify(&self) -> Option<bool> {
        let (len, want) = self.chain?;
        if self.journal.len() < len as usize {
            return Some(false);
        }
        let got = chain_of(
            self.journal
                .iter()
                .take(len as usize)
                .map(|(i, e)| (*i, e.as_ref())),
        );
        Some(got == (len, want))
    }
}

/// UTC date `yyyy-mm-dd` of a ms timestamp (Howard Hinnant's civil-from-days algorithm — not
/// pulling in a calendar crate for this one line).
pub fn utc_date(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Inverse of [`utc_date`]: Unix ms of 00:00 UTC on date `date` (`YYYY-MM-DD`). `None` when
/// malformed or not a real date — accepts only exactly the strings [`utc_date`] produces.
pub fn utc_day_start_ms(date: &str) -> Option<u64> {
    let b = date.as_bytes();
    let digits = |r: std::ops::Range<usize>| -> Option<i64> {
        b.get(r.clone())?
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| date[r].parse().ok())?
    };
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, m, d) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let yy = if m <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let ms = u64::try_from(days).ok()?.checked_mul(86_400_000)?;
    (utc_date(ms) == date).then_some(ms)
}

fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Where archives go: `archive/` under the main store's prefix.
#[derive(Clone)]
pub struct ArchiveStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ArchiveStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        Self {
            store,
            prefix: prefix.clone(),
        }
    }

    /// `archive/<tenant>/<yyyy-mm-dd>/<id-hex>`.
    pub fn path(&self, tenant: &TenantId, date: &str, id: &InvocationId) -> Path {
        self.run_path(tenant, date, id, 0)
    }

    /// The slot of run `run` of `id` on date `date`: `0` is [`Self::path`], the others are
    /// `<id-hex>.<run>`.
    fn run_path(&self, tenant: &TenantId, date: &str, id: &InvocationId, run: u32) -> Path {
        let rel = if run == 0 {
            format!("archive/{tenant}/{date}/{}", hex(id))
        } else {
            format!("archive/{tenant}/{date}/{}.{run}", hex(id))
        };
        let p = self.prefix.as_ref();
        Path::from(if p.is_empty() {
            rel
        } else {
            format!("{p}/{rel}")
        })
    }

    /// Copies artifact `from` (full path) of `id` to
    /// `archive/<tenant>/<day>/<id-hex>.artifacts/<sha>` (`--archive-artifacts`) — a server-side
    /// copy, **before** [`Self::put`]. Keyed by sha, so every run of an id on the same day shares
    /// it, and overwriting is fine.
    pub async fn copy_artifact(
        &self,
        tenant: &TenantId,
        id: &InvocationId,
        date_ms: u64,
        sha: &[u8; 32],
        from: &Path,
    ) -> Result<()> {
        let rel = format!(
            "archive/{tenant}/{}/{}.artifacts/{}",
            utc_date(date_ms),
            hex(id),
            crate::artifact::sha_hex(sha)
        );
        let p = self.prefix.as_ref();
        let to = Path::from(if p.is_empty() {
            rel
        } else {
            format!("{p}/{rel}")
        });
        self.store
            .copy(from, &to)
            .await
            .map_err(|e| StoreError::LeaseIo(format!("cannot copy {from} → {to}: {e}")))
    }

    /// Writes `rec` under the day of `date_ms`. The slot already holds an object of **this very**
    /// run ⇒ done; of another run with the same id ⇒ the next slot (see the module docs). `Err` ⇒
    /// not archived, the caller must not delete the journal.
    pub async fn put(&self, rec: &ArchiveRecord, date_ms: u64) -> Result<()> {
        let date = utc_date(date_ms);
        let payload = PutPayload::from(rec.encode());
        for run in 0..MAX_RUNS_PER_DAY {
            let path = self.run_path(&rec.tenant, &date, &rec.id, run);
            match self
                .store
                .put_opts(&path, payload.clone(), PutOptions::from(PutMode::Create))
                .await
            {
                Ok(_) => return Ok(()),
                Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {}
                Err(e) => return Err(StoreError::LeaseIo(format!("cannot write {path}: {e}"))),
            }
            let raw = match self.store.get(&path).await {
                Ok(got) => got
                    .bytes()
                    .await
                    .map_err(|e| StoreError::LeaseIo(format!("cannot read back {path}: {e}")))?,
                // Present at PUT time, gone at GET time (a lifecycle rule, an operator): we do not
                // know whose slot this is — the next tick retries from the start.
                Err(e) => {
                    return Err(StoreError::LeaseIo(format!(
                        "{path} exists but cannot be read back: {e}"
                    )))
                }
            };
            match ArchiveRecord::decode(&raw) {
                Ok(have) if have.same_run(rec) => return Ok(()),
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    path = %path,
                    error = %e,
                    "archive exists but does not decode — writing this run to the next slot"
                ),
            }
        }
        Err(StoreError::LeaseIo(format!(
            "{} already has {MAX_RUNS_PER_DAY} runs on {date} — no slot left to archive",
            hex(&rec.id)
        )))
    }

    /// The archives of one UTC day (`yyyy-mm-dd`) of `tenant`, by object name:
    /// `(id-hex, run, bytes, written-at ms)`, at most `limit` (`sys_archive`). One LIST; skips
    /// the `.artifacts/` directories.
    pub async fn list_day(
        &self,
        tenant: &TenantId,
        date: &str,
        limit: usize,
    ) -> Result<Vec<(String, u32, u64, u64)>> {
        use futures::TryStreamExt;
        let rel = format!("archive/{tenant}/{date}/");
        let p = self.prefix.as_ref();
        let dir = Path::from(if p.is_empty() {
            rel.clone()
        } else {
            format!("{p}/{rel}")
        });
        let listed: Vec<_> = self
            .store
            .list(Some(&dir))
            .try_collect()
            .await
            .map_err(|e| StoreError::LeaseIo(format!("cannot list {dir}: {e}")))?;
        let mut out = Vec::new();
        for m in listed {
            let Some(name) = m.location.filename() else {
                continue;
            };
            // Only objects directly under the day directory (not `.artifacts/<sha>`).
            if m.location.as_ref().rsplit_once('/').map(|(d, _)| d) != Some(dir.as_ref()) {
                continue;
            }
            let (id, run) = match name.split_once('.') {
                Some((id, run)) => match run.parse::<u32>() {
                    Ok(r) => (id, r),
                    Err(_) => continue,
                },
                None => (name, 0),
            };
            if id.len() != 48 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            out.push((
                id.to_string(),
                run,
                m.size,
                m.last_modified.timestamp_millis().max(0) as u64,
            ));
        }
        out.sort();
        out.truncate(limit);
        Ok(out)
    }

    /// Finds the archive of `id` in `tenant`: exactly on date `date` if given, otherwise going
    /// back from the day of `now_ms` for at most `days` days — one GET per day. Returns raw bytes
    /// (not decoded), so the caller decides how to report a decode error.
    ///
    /// On a day that has an archive, returns the id's **latest run** on that day (the largest
    /// `<id-hex>.<n>`) — one extra GET returning 404 after the last one.
    pub async fn find(
        &self,
        tenant: &TenantId,
        id: &InvocationId,
        date: Option<&str>,
        now_ms: u64,
        days: u32,
    ) -> Result<Option<Bytes>> {
        let dates: Vec<String> = match date {
            Some(d) => vec![d.to_string()],
            None => (0..days.max(1) as u64)
                .map(|k| utc_date(now_ms.saturating_sub(k * 86_400_000)))
                .collect(),
        };
        for d in dates {
            let mut last = None;
            for run in 0..MAX_RUNS_PER_DAY {
                let path = self.run_path(tenant, &d, id, run);
                match self.store.get(&path).await {
                    Ok(got) => {
                        last = Some(
                            got.bytes()
                                .await
                                .map_err(|e| StoreError::LeaseIo(e.to_string()))?,
                        )
                    }
                    Err(OsError::NotFound { .. }) => break,
                    Err(e) => {
                        return Err(StoreError::LeaseIo(format!("cannot read slot {path}: {e}")))
                    }
                }
            }
            if last.is_some() {
                return Ok(last);
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(chain: bool) -> ArchiveRecord {
        let journal = vec![
            (0u32, Bytes::from_static(b"input")),
            (1, Bytes::from_static(b"output")),
        ];
        let c = chain.then(|| chain_of(journal.iter().map(|(i, e)| (*i, e.as_ref()))));
        ArchiveRecord {
            id: [7; 24],
            tenant: TenantId::new("acme").unwrap(),
            purged_at_ms: 42,
            status: Bytes::from_static(b"\x01done"),
            journal,
            chain: c,
        }
    }

    #[test]
    fn roundtrip_and_chain_check() {
        for chain in [false, true] {
            let r = rec(chain);
            let back = ArchiveRecord::decode(&r.encode()).unwrap();
            assert_eq!(back, r);
            assert_eq!(back.verify(), chain.then_some(true));
        }
        let mut forged = rec(true);
        forged.journal[1].1 = Bytes::from_static(b"outpuT");
        assert_eq!(forged.verify(), Some(false));
        assert!(ArchiveRecord::decode(b"junk").is_err());
    }

    /// A deterministic id (workflow, idempotency key) re-runs after being purged, and the later
    /// run is also purged **on the same UTC day**: the same path `archive/<tenant>/<day>/<id>`.
    ///
    /// 1. Run 1 is purged: `put` writes the object.
    /// 2. The same id runs again (different content) and is purged the same day: `put` hits run
    ///    1's `AlreadyExists`.
    /// 3. The old code treated that as "archived" ⇒ the caller (`turn::purge_archived`) deleted
    ///    run 2's journal with no archived copy at all.
    ///
    /// Must see: run 2 can be read back from the archive (`find` returns the latest purge), and
    /// run 1 is still intact. A retry of **the same** purge (died between the PUT and the delete
    /// commit, different `purged_at_ms`) does not create a new object.
    #[tokio::test]
    async fn rerun_of_a_deterministic_id_purged_the_same_day_is_archived() {
        let mem: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let store = ArchiveStore::new(mem.clone(), &Path::from(""));
        let day = 1_791_590_400_000;
        let first = rec(false);
        store.put(&first, day).await.unwrap();
        let mut second = rec(false);
        second.journal[1].1 = Bytes::from_static(b"output-round-2");
        second.status = Bytes::from_static(b"\x01round-2");
        second.purged_at_ms = 43;
        store.put(&second, day + 1_000).await.unwrap();

        let tenant = TenantId::new("acme").unwrap();
        let got = store
            .find(&tenant, &first.id, Some("2026-10-10"), day, 1)
            .await
            .unwrap()
            .expect("archive present");
        assert_eq!(
            ArchiveRecord::decode(&got).unwrap(),
            second,
            "the archive of run 2 was not written — its journal has been deleted"
        );

        // A retry of run 2 itself (different `purged_at_ms`): no extra object.
        let mut retry = second.clone();
        retry.purged_at_ms = 99;
        store.put(&retry, day + 2_000).await.unwrap();
        use futures::TryStreamExt;
        let listed: Vec<_> = mem
            .list(Some(&Path::from("archive")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 2, "one object per run: {listed:?}");
        let mut runs = Vec::new();
        for m in listed {
            let raw = mem.get(&m.location).await.unwrap().bytes().await.unwrap();
            runs.push(ArchiveRecord::decode(&raw).unwrap().status);
        }
        runs.sort();
        let mut want = vec![first.status.clone(), second.status.clone()];
        want.sort();
        assert_eq!(runs, want);
    }

    /// `list_day` (`sys_archive`): every run of the day, skipping the `.artifacts/` directory and
    /// unknown names, only the right tenant and day.
    #[tokio::test]
    async fn list_day_lists_runs_of_one_day() {
        let mem: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let store = ArchiveStore::new(mem.clone(), &Path::from("p"));
        let day = 1_791_590_400_000;
        let first = rec(false);
        store.put(&first, day).await.unwrap();
        let mut second = rec(false);
        second.status = Bytes::from_static(b"\x01round-2");
        second.purged_at_ms = 43;
        store.put(&second, day + 1_000).await.unwrap();
        let mut other = rec(false);
        other.id = [8; 24];
        store.put(&other, day + 86_400_000).await.unwrap();
        for junk in [
            format!("p/archive/acme/2026-10-10/{}.artifacts/abc", hex(&first.id)),
            "p/archive/acme/2026-10-10/not-an-id".to_string(),
        ] {
            mem.put(&Path::from(junk), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        let tenant = TenantId::new("acme").unwrap();
        let got = store.list_day(&tenant, "2026-10-10", 100).await.unwrap();
        let names: Vec<(String, u32)> =
            got.iter().map(|(id, run, ..)| (id.clone(), *run)).collect();
        assert_eq!(names, vec![(hex(&first.id), 0), (hex(&first.id), 1)]);
        assert!(got.iter().all(|(_, _, bytes, _)| *bytes > 0));
        assert_eq!(
            store
                .list_day(&tenant, "2026-10-10", 1)
                .await
                .unwrap()
                .len(),
            1
        );
        let zeta = TenantId::new("zeta").unwrap();
        assert!(store
            .list_day(&zeta, "2026-10-10", 100)
            .await
            .unwrap()
            .is_empty());
    }

    /// A slot already holding an object that does not decode (garbage, a newer binary's
    /// header): it cannot be proven to belong to this run ⇒ write to the next slot, and `find`
    /// returns that run.
    #[tokio::test]
    async fn undecodable_archive_slot_is_skipped_not_trusted() {
        let mem: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let store = ArchiveStore::new(mem.clone(), &Path::from(""));
        let day = 1_791_590_400_000;
        let r = rec(true);
        mem.put(
            &store.path(&r.tenant, "2026-10-10", &r.id),
            PutPayload::from_static(b"junk"),
        )
        .await
        .unwrap();
        store.put(&r, day).await.unwrap();
        let got = store
            .find(&r.tenant, &r.id, Some("2026-10-10"), day, 1)
            .await
            .unwrap()
            .expect("archive present");
        assert_eq!(ArchiveRecord::decode(&got).unwrap(), r);
    }

    #[test]
    fn utc_day() {
        assert_eq!(utc_date(0), "1970-01-01");
        assert_eq!(utc_date(951_782_400_000), "2000-02-29");
        assert_eq!(utc_date(1_791_590_400_000), "2026-10-10");
    }

    /// `utc_day_start_ms` is exactly the inverse of `utc_date` on every day 1970–2300, and
    /// rejects strings that are not real dates.
    #[test]
    fn utc_day_start_inverts_utc_date() {
        let day = 86_400_000u64;
        for k in 0..(330 * 366) {
            let start = k * day;
            for ms in [start, start + 1, start + day - 1] {
                assert_eq!(utc_day_start_ms(&utc_date(ms)), Some(start), "{ms}");
            }
        }
        for bad in [
            "",
            "2026-1-01",
            "2026-13-01",
            "2026-02-30",
            "2026-00-10",
            "2026-10-32",
            "20261010xx",
            "2026/10/10",
            "+026-10-10",
            "1969-12-31",
        ] {
            assert_eq!(utc_day_start_ms(bad), None, "{bad}");
        }
    }
}
