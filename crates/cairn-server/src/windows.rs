// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-range flush windows (D-h): **trading latency for
//! price** per range — the Turso model (10 / 25 / 50 / 100 ms by plan tier).
//!
//! One cluster-level object `range-windows`, text like the rule book:
//!
//! ```text
//! cairnflow-range-windows/1
//! <range id> <ms>
//! ```
//!
//! No object, or no line for a range ⇒ the node's window
//! (`--wal-window-ms` / `--flush-interval-ms`). That is also the migration path from
//! format 9 buckets (D27): a new, purely additive object — a bucket without it reads as
//! "every range follows the node".
//!
//! How it applies:
//! - ranges on the **shared WAL**: the log stream cuts at the smallest window among the
//!   ranges with frames waiting (`Stripe::run`) — applies at once, every node re-reads on
//!   the deployment registry refresh tick;
//! - ranges on the **native WAL**: the flush interval is a Db setting at open — applies at
//!   the next Db open (leader change, restart).
//!
//! Split/merge: a child range is a new id, following the node default until it is set —
//! whoever sells the plan tiers sets it again for the child ranges (the child ids are in the split response).

use {
    cairn_store::cas::{put_resolving_or_overwrite, Cas},
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
        time::Duration,
    },
};

const HEADER: &str = "cairnflow-range-windows/1";

/// Sensible limit for a window: beyond it every commit of the range waits more than a
/// second — no longer a plan tier but a configuration mistake.
pub const MAX_WINDOW: Duration = Duration::from_secs(1);

fn decode(raw: &[u8]) -> Result<BTreeMap<u32, Duration>, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("range-windows is not UTF-8: {e}"))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != HEADER {
        if let Some(e) = cairn_store::newer_header("range-windows", HEADER, header) {
            return Err(e.to_string());
        }
        return Err(format!("range-windows lacks header {HEADER}"));
    }
    let mut out = BTreeMap::new();
    for line in lines.filter(|l| !l.trim().is_empty()) {
        let mut f = line.split(' ');
        match (
            f.next().and_then(|r| r.parse().ok()),
            f.next().and_then(|m| m.parse().ok()),
        ) {
            (Some(r), Some(ms)) => {
                out.insert(r, Duration::from_millis(ms));
            }
            _ => return Err(format!("range-windows: corrupt line {line:?}")),
        }
    }
    Ok(out)
}

fn encode(m: &BTreeMap<u32, Duration>) -> Vec<u8> {
    let mut out = format!("{HEADER}\n");
    for (r, w) in m {
        out.push_str(&format!("{r} {}\n", w.as_millis()));
    }
    out.into_bytes()
}

/// The per-range window book + the in-RAM copy the node applies.
#[derive(Clone)]
pub struct RangeWindows {
    store: Option<(Arc<dyn ObjectStore>, Path)>,
    current: Arc<RwLock<BTreeMap<u32, Duration>>>,
}

impl RangeWindows {
    /// `store` = (object store, cluster prefix); `None` = in RAM only.
    pub fn new(store: Option<(Arc<dyn ObjectStore>, &Path)>) -> Self {
        let path = |p: &Path| {
            let p = p.as_ref();
            Path::from(if p.is_empty() {
                "range-windows".to_string()
            } else {
                format!("{p}/range-windows")
            })
        };
        Self {
            store: store.map(|(s, p)| (s, path(p))),
            current: Arc::default(),
        }
    }

    /// The range's own window for `range`, if any.
    pub fn get(&self, range: u32) -> Option<Duration> {
        self.current.read().ok()?.get(&range).copied()
    }

    /// Every own window.
    pub fn all(&self) -> BTreeMap<u32, Duration> {
        self.current.read().map(|m| m.clone()).unwrap_or_default()
    }

    async fn load(&self) -> Result<(BTreeMap<u32, Duration>, Option<UpdateVersion>), String> {
        let Some((store, path)) = &self.store else {
            return Ok((self.all(), None));
        };
        match store.get(path).await {
            Ok(got) => {
                let v = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got.bytes().await.map_err(|e| e.to_string())?;
                Ok((decode(&raw)?, Some(v)))
            }
            Err(OsError::NotFound { .. }) => Ok((BTreeMap::new(), None)),
            Err(e) => Err(format!("cannot read {path}: {e}")),
        }
    }

    /// Read straight from the bucket **without** touching the RAM copy — for the viewing API: only
    /// the refresh round may update the RAM copy, because its "change" is what triggers applying.
    pub async fn read(&self) -> Result<BTreeMap<u32, Duration>, String> {
        Ok(self.load().await?.0)
    }

    /// Re-read from the bucket. Returns `true` if anything changed.
    pub async fn refresh(&self) -> Result<bool, String> {
        if self.store.is_none() {
            return Ok(false);
        }
        let (m, _) = self.load().await?;
        let mut cur = self
            .current
            .write()
            .map_err(|_| "poisoned lock".to_string())?;
        let changed = *cur != m;
        *cur = m;
        Ok(changed)
    }

    /// Set (`Some`) or remove (`None`) the window of range `range` — CAS, retried when
    /// someone else just wrote; a store without CAS (`--data-dir`) overwrites, like the
    /// rule book.
    pub async fn set(&self, range: u32, window: Option<Duration>) -> Result<(), String> {
        for _ in 0..8 {
            let (mut m, version) = self.load().await?;
            match window {
                Some(w) => m.insert(range, w),
                None => m.remove(&range),
            };
            let Some((store, path)) = &self.store else {
                *self
                    .current
                    .write()
                    .map_err(|_| "poisoned lock".to_string())? = m;
                return Ok(());
            };
            let mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            // Ambiguity is resolved by reading back (rule 7): setting the same window produces
            // the same content, so identical content means our change is already on the bucket.
            match put_resolving_or_overwrite(store, path, encode(&m).into(), mode).await {
                Ok(Cas::Written(_)) => {}
                Ok(Cas::Lost) => continue,
                Err(e) => return Err(format!("cannot write {path}: {e}")),
            }
            // **Do not** update the RAM copy here: the node's refresh round sees the bucket
            // differ from RAM and applies it to the shared WAL — a single apply path for the
            // node that took the command and for the other nodes.
            return Ok(());
        }
        Err("range-windows keeps changing underfoot — retry".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule 7: the `range-windows` CAS was written but the answer
    /// is an error. Must see: `set` returns `Ok` and the bucket carries exactly the window.
    #[tokio::test]
    async fn ambiguous_write_is_resolved_by_reread() {
        use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let w = RangeWindows::new(Some((faulty.clone(), &Path::from("p"))));
            for (range, ms) in [(3u32, 20u64), (4, 40)] {
                faulty.add_rule(Rule::ambiguous_put("range-windows", reply));
                w.set(range, Some(Duration::from_millis(ms)))
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: written but reported an error: {e}"));
            }
            let (m, _) = w.load().await.unwrap();
            assert_eq!(m.get(&3), Some(&Duration::from_millis(20)), "{reply:?}");
            assert_eq!(m.get(&4), Some(&Duration::from_millis(40)), "{reply:?}");
        }
    }

    /// This header is the header of the policy table.
    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            cairn_store::object_policy("range-windows").map(|p| p.header),
            Some(HEADER)
        );
    }

    #[test]
    fn encodes_and_decodes_back() {
        let mut m = BTreeMap::new();
        m.insert(3, Duration::from_millis(100));
        m.insert(17, Duration::from_millis(10));
        assert_eq!(decode(&encode(&m)).unwrap(), m);
        assert!(decode(b"cairnflow-range-windows/1\n3 x\n").is_err());
        assert!(decode(b"other\n").is_err());
    }
}
