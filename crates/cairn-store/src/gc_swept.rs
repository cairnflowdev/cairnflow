// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Swept-tombstone ledger**: the `gc-swept` object lists the ids of tombstoned ranges
//! that the D-d loop has found to have **no Db left** (fully deleted). The D-d loop skips
//! the owner-record GET and the manifest LIST for those ids; `verify` drops them from
//! check (h) when `ranges/<id>` has no objects left.
//!
//! ```text
//! cairnflow-gc-swept/1
//! by=<writer>#<nonce>
//! full-pass-ms=<ms>
//! ranges=1-5,7,9-12
//! ```
//!
//! **Lenient read, absent ≡ empty** (AGENTS §1.2, precedent `range-windows`): a ledger that
//! is absent, broken or has an unknown header reads as empty with `full-pass-ms=0`, so the
//! D-d loop reads every tombstone as it did before the ledger existed. A newer header of
//! the same family is not overwritten.
//!
//! **Why skipping reads is safe** (rule 5 of AGENTS): the ledger only makes the D-d loop
//! consider **fewer** candidates — it never adds a delete. The only mistake it can cause
//! is **keeping** (a space leak): a Db that reappears under a recorded id that the loop
//! does not look at. An id only enters the ledger when that same loop has just read its
//! owner record **carrying a tombstone** and read the manifest and found **none** (after
//! deleting it, or because the Db had already been deleted); a broken or ambiguous read ⇒
//! not added. After that, the Db of that id cannot reappear through any path of the
//! cluster: a tombstone is permanent and nobody re-acquires a tombstoned range (D22,
//! `retired_range_is_never_reacquired`), and the child ids of a split/merge are always
//! freshly allocated. What remains are the manual paths (`cairn-ctl trash restore` of a Db,
//! copying a bucket): the D-d loop **re-reads every tombstone** at least once every
//! [`FULL_PASS_EVERY`] (`full-pass-ms=`), and rebuilds the ledger from that very read — a
//! reappearing Db is missed for at most that long.
//!
//! **Writing** (rule 7): `cas::put_resolving` on the version just read (`Create` when
//! absent); the content carries `by=` with a nonce so two D-d runners cannot write
//! identical content. A lost CAS or an error ⇒ drop the write; the next round re-reads
//! those tombstones and writes again — it only costs requests.
//!
//! **Size cap**: ids are merged into contiguous runs; beyond [`MAX_RUNS`] runs no new id
//! is added (the D-d loop reads them as before).

use {
    crate::{
        cas::{put_resolving, Cas},
        error::{Result, StoreError},
    },
    bytes::Bytes,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{collections::BTreeSet, sync::Arc, time::Duration},
};

/// Object name, under the cluster prefix.
pub const OBJECT: &str = "gc-swept";

/// The header this binary writes.
pub const HEADER: &str = "cairnflow-gc-swept/1";

/// The D-d loop ignores the ledger and re-reads every tombstone at least this often — the
/// upper bound on how long a Db that reappears (by hand) under a recorded id is missed.
pub const FULL_PASS_EVERY: Duration = Duration::from_secs(24 * 3600);

/// Maximum number of contiguous runs in the ledger (~120 KB). Beyond it, new ids do not
/// enter the ledger.
pub const MAX_RUNS: usize = 10_000;

/// The ledger as read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Swept {
    ids: BTreeSet<u32>,
    /// The most recent re-read of every tombstone (wall-clock ms); `0` = none yet.
    pub full_pass_ms: u64,
    /// Version for the CAS; `None` = the object is absent (write with `Create`).
    version: Option<UpdateVersion>,
    /// Do not overwrite: a newer header, or a broken read (what is on the store is
    /// unknown).
    frozen: bool,
}

fn path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(OBJECT)
    } else {
        Path::from(format!("{}/{OBJECT}", prefix.as_ref()))
    }
}

impl Swept {
    /// Whether `id` is recorded in the ledger.
    pub fn contains(&self, id: u32) -> bool {
        self.ids.contains(&id)
    }

    pub fn ids(&self) -> &BTreeSet<u32> {
        &self.ids
    }

    /// Whether this round must re-read every tombstone (ledger absent, broken, or older
    /// than [`FULL_PASS_EVERY`]).
    pub fn full_pass_due(&self, now_ms: u64) -> bool {
        self.full_pass_ms == 0
            || now_ms.saturating_sub(self.full_pass_ms) >= FULL_PASS_EVERY.as_millis() as u64
    }

    /// The contiguous runs of `ids`.
    fn runs(ids: &BTreeSet<u32>) -> Vec<(u32, u32)> {
        let mut out: Vec<(u32, u32)> = Vec::new();
        for &id in ids {
            match out.last_mut() {
                Some((_, hi)) if hi.checked_add(1) == Some(id) => *hi = id,
                _ => out.push((id, id)),
            }
        }
        out
    }

    fn encode(ids: &BTreeSet<u32>, full_pass_ms: u64, by: &str) -> Bytes {
        let ranges: Vec<String> = Self::runs(ids)
            .into_iter()
            .map(|(lo, hi)| {
                if lo == hi {
                    lo.to_string()
                } else {
                    format!("{lo}-{hi}")
                }
            })
            .collect();
        Bytes::from(format!(
            "{HEADER}\nby={by}\nfull-pass-ms={full_pass_ms}\nranges={}\n",
            ranges.join(",")
        ))
    }

    /// Lenient decode; `None` = not trustworthy (unknown header, broken `ranges=`).
    fn decode(raw: &[u8]) -> Option<(BTreeSet<u32>, u64)> {
        let text = std::str::from_utf8(raw).ok()?;
        let mut lines = text.lines();
        if lines.next()? != HEADER {
            return None;
        }
        let (mut ids, mut full) = (BTreeSet::new(), 0);
        for l in lines {
            match l.split_once('=') {
                Some(("full-pass-ms", v)) => full = v.trim().parse().ok()?,
                Some(("ranges", v)) => {
                    for part in v.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                        let (lo, hi) = match part.split_once('-') {
                            Some((a, b)) => (a.parse::<u32>().ok()?, b.parse::<u32>().ok()?),
                            None => {
                                let x = part.parse::<u32>().ok()?;
                                (x, x)
                            }
                        };
                        if lo > hi || u64::from(hi - lo) >= 1 << 24 {
                            return None;
                        }
                        ids.extend(lo..=hi);
                    }
                }
                _ => {}
            }
        }
        Some((ids, full))
    }
}

/// Reads the ledger. Absent, broken or an unknown header ⇒ an empty ledger
/// (`full-pass-ms=0`); a newer header of the same family or a read error ⇒ an empty ledger
/// that **cannot be written** — the loop reads every tombstone and does not overwrite what
/// it does not understand. Never fails: the ledger is only an optimization.
pub async fn read(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Swept {
    let got = match store.get(&path(prefix)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Swept::default(),
        Err(e) => {
            tracing::debug!(error = %e, "swept-tombstone ledger: read failed — this round reads every tombstone");
            return Swept {
                frozen: true,
                ..Swept::default()
            };
        }
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = match got.bytes().await {
        Ok(b) => b,
        Err(_) => {
            return Swept {
                frozen: true,
                ..Swept::default()
            }
        }
    };
    match Swept::decode(&raw) {
        Some((ids, full_pass_ms)) => Swept {
            ids,
            full_pass_ms,
            version: Some(version),
            frozen: false,
        },
        None => {
            let first = raw.split(|b| *b == b'\n').next().unwrap_or_default();
            let newer = crate::format::newer_header(
                OBJECT,
                HEADER,
                std::str::from_utf8(first).unwrap_or_default(),
            )
            .is_some();
            tracing::warn!(
                newer = newer,
                "swept-tombstone ledger cannot be decoded — this round reads every tombstone"
            );
            Swept {
                version: Some(version),
                frozen: newer,
                ..Swept::default()
            }
        }
    }
}

/// The next version of the ledger from one D-d round.
#[derive(Clone, Debug, Default)]
pub struct Update {
    /// Ids this round **proved** swept: the owner record carrying a tombstone was read in
    /// this round, and the manifest read in this round was absent.
    pub swept: Vec<u32>,
    /// This round is a re-read of every tombstone (ignoring the ledger) at `at_ms`: the new
    /// ledger keeps only the ids this round proved again, plus `unread` (ids of the old
    /// ledger whose read failed in this round).
    pub full_pass_at: Option<u64>,
    /// Ids of the old ledger that the re-read could not read — kept in the ledger as they
    /// are.
    pub unread: Vec<u32>,
}

/// Result of [`write`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wrote {
    /// Nothing to change, or the ledger cannot be written (newer header, broken read).
    Unchanged,
    Written,
    /// Someone changed the ledger before us: drop it, the next round does it again.
    Lost,
}

/// Writes the next version of `old` according to `up`. Errors go up to the caller (to be
/// logged); they are not errors of the round — the next round re-reads those tombstones.
pub async fn write(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    old: &Swept,
    up: &Update,
) -> Result<Wrote> {
    if old.frozen {
        return Ok(Wrote::Unchanged);
    }
    let (mut ids, full_pass_ms) = match up.full_pass_at {
        Some(at) => {
            let keep: BTreeSet<u32> = up
                .unread
                .iter()
                .copied()
                .filter(|id| old.ids.contains(id))
                .collect();
            (keep, at)
        }
        None => (old.ids.clone(), old.full_pass_ms),
    };
    let base = ids.clone();
    ids.extend(up.swept.iter().copied());
    if Swept::runs(&ids).len() > MAX_RUNS {
        // Rare: add ids one at a time, dropping any id that would push the ledger over the
        // cap.
        ids = base;
        for &id in &up.swept {
            if ids.insert(id) && Swept::runs(&ids).len() > MAX_RUNS {
                ids.remove(&id);
            }
        }
    }
    if ids == old.ids && full_pass_ms == old.full_pass_ms {
        return Ok(Wrote::Unchanged);
    }
    let body = Swept::encode(&ids, full_pass_ms, &crate::ledger::writer_tag("gc"));
    let mode = match &old.version {
        Some(v) => PutMode::Update(v.clone()),
        None => PutMode::Create,
    };
    match put_resolving(store, &path(prefix), body, mode).await {
        Ok(Cas::Written(_)) => Ok(Wrote::Written),
        Ok(Cas::Lost) => Ok(Wrote::Lost),
        Err(e) => Err(StoreError::Io(format!("writing gc-swept: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule},
        slatedb::object_store::memory::InMemory,
    };

    fn ids(v: &[u32]) -> BTreeSet<u32> {
        v.iter().copied().collect()
    }

    #[test]
    fn encoding_round_trips_and_merges_runs() {
        let set = ids(&[1, 2, 3, 5, 9, 10, u32::MAX]);
        let raw = Swept::encode(&set, 42, "gc#1");
        assert_eq!(
            std::str::from_utf8(&raw).unwrap(),
            format!(
                "{HEADER}\nby=gc#1\nfull-pass-ms=42\nranges=1-3,5,9-10,{}\n",
                u32::MAX
            )
        );
        assert_eq!(Swept::decode(&raw), Some((set, 42)));
        // Unknown keys are ignored (lenient read).
        assert_eq!(
            Swept::decode(format!("{HEADER}\nodd=1\nranges=4\n").as_bytes()),
            Some((ids(&[4]), 0))
        );
        assert_eq!(Swept::decode(b"cairnflow-gc-swept/2\nranges=1\n"), None);
        assert_eq!(
            Swept::decode(format!("{HEADER}\nranges=3-1\n").as_bytes()),
            None
        );
        assert_eq!(
            Swept::decode(format!("{HEADER}\nranges=x\n").as_bytes()),
            None
        );
    }

    #[test]
    fn every_header_is_in_policy() {
        let p = crate::format::object_policy(OBJECT).expect("OBJECT_POLICIES row");
        assert_eq!(p.header, HEADER);
        assert_eq!(p.read, crate::format::ReadMode::Lenient);
    }

    /// Absent ⇒ empty, must re-read every tombstone; write then read back; the re-read of
    /// every tombstone rebuilds the ledger (ids not proved again leave the ledger, except
    /// ids whose read failed); a newer header ⇒ not overwritten.
    #[tokio::test]
    async fn write_read_and_full_pass_rebuild() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::from("cell");
        let s = read(&store, &p).await;
        assert!(s.ids().is_empty() && s.full_pass_due(1));
        let up = Update {
            swept: vec![3, 4, 7],
            full_pass_at: Some(1_000),
            unread: vec![],
        };
        assert_eq!(write(&store, &p, &s, &up).await.unwrap(), Wrote::Written);
        let s = read(&store, &p).await;
        assert_eq!(s.ids(), &ids(&[3, 4, 7]));
        assert!(!s.full_pass_due(1_000 + 1));
        assert!(s.full_pass_due(1_000 + FULL_PASS_EVERY.as_millis() as u64));
        // Normal round: add.
        let up = Update {
            swept: vec![5],
            ..Update::default()
        };
        assert_eq!(write(&store, &p, &s, &up).await.unwrap(), Wrote::Written);
        let s = read(&store, &p).await;
        assert_eq!(s.ids(), &ids(&[3, 4, 5, 7]));
        assert_eq!(s.full_pass_ms, 1_000);
        // Nothing new ⇒ no write.
        assert_eq!(
            write(&store, &p, &s, &Update::default()).await.unwrap(),
            Wrote::Unchanged
        );
        // The re-read: 4 is not proved again (the Db reappeared), 7 failed to read.
        let up = Update {
            swept: vec![3, 5],
            full_pass_at: Some(9_000),
            unread: vec![7, 8],
        };
        assert_eq!(write(&store, &p, &s, &up).await.unwrap(), Wrote::Written);
        let s2 = read(&store, &p).await;
        assert_eq!(s2.ids(), &ids(&[3, 5, 7]));
        assert_eq!(s2.full_pass_ms, 9_000);
        // A CAS on the old version loses.
        let up = Update {
            swept: vec![11],
            ..Update::default()
        };
        assert_eq!(write(&store, &p, &s, &up).await.unwrap(), Wrote::Lost);
        // Newer header: reads as empty, not overwritten.
        store
            .put(
                &path(&p),
                Bytes::from_static(b"cairnflow-gc-swept/2\nx\n").into(),
            )
            .await
            .unwrap();
        let s3 = read(&store, &p).await;
        assert!(s3.ids().is_empty() && s3.full_pass_due(u64::MAX));
        assert_eq!(write(&store, &p, &s3, &up).await.unwrap(), Wrote::Unchanged);
        // Garbage under the same name: reads as empty, can be overwritten (CAS on the
        // garbage's version).
        store
            .put(&path(&p), Bytes::from_static(b"junk").into())
            .await
            .unwrap();
        let s4 = read(&store, &p).await;
        assert!(s4.ids().is_empty());
        assert_eq!(write(&store, &p, &s4, &up).await.unwrap(), Wrote::Written);
    }

    /// An ambiguous write that reached the store (object_store retrying on its own, or an
    /// error after it was applied) is resolved by reading back into `Written`; an error
    /// before reaching the store is an error, and the ledger does not change.
    #[tokio::test]
    async fn ambiguous_write_resolves_by_reading_back() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let p = Path::from("");
            let up = Update {
                swept: vec![1, 2],
                full_pass_at: Some(5),
                unread: vec![],
            };
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Any,
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let s = read(&store, &p).await;
            assert_eq!(
                write(&store, &p, &s, &up).await.unwrap(),
                Wrote::Written,
                "{reply:?}"
            );
            assert_eq!(read(&store, &p).await.ids(), &ids(&[1, 2]));
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Any,
                Action::Fail { times: Some(1) },
            ));
            let s = read(&store, &p).await;
            let more = Update {
                swept: vec![9],
                ..Update::default()
            };
            assert!(write(&store, &p, &s, &more).await.is_err());
            assert_eq!(read(&store, &p).await.ids(), &ids(&[1, 2]));
        }
    }

    /// A full ledger ([`MAX_RUNS`] runs): an id that extends an existing run still gets in,
    /// an id that opens a new run does not.
    #[tokio::test]
    async fn size_is_capped_by_runs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::from("");
        let up = Update {
            swept: (0..MAX_RUNS as u32).map(|i| i * 3).collect(),
            full_pass_at: Some(1),
            unread: vec![],
        };
        let s = read(&store, &p).await;
        write(&store, &p, &s, &up).await.unwrap();
        let s = read(&store, &p).await;
        assert_eq!(Swept::runs(s.ids()).len(), MAX_RUNS);
        let far = 3 * MAX_RUNS as u32 + 100;
        let up = Update {
            swept: vec![1, far],
            ..Update::default()
        };
        assert_eq!(write(&store, &p, &s, &up).await.unwrap(), Wrote::Written);
        let s = read(&store, &p).await;
        assert!(s.contains(1) && !s.contains(far));
        assert_eq!(Swept::runs(s.ids()).len(), MAX_RUNS);
    }
}
