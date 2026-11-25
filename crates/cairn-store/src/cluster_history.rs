// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Range-table history** (history kept beside the table; the owner record is unchanged).
//!
//! ```text
//! <prefix>/cluster-history/<version>    the exact bytes of cluster.conf at that version
//! ```
//!
//! `cluster.conf` is the only object of the cluster that cannot be rebuilt from the rest of the
//! bucket: owner records do not carry key spans. Every table that **won** — the first `Create`
//! in [`crate::ClusterConfig::open`], every winning CAS of [`crate::ClusterConfig::update`] —
//! leaves a copy here, so that `cairn-ctl table restore` can rebuild the table when
//! `cluster.conf` has been deleted from outside.
//!
//! - **Written after the CAS, not before**: a copy written before the CAS could be a table that
//!   never won. Dying between the CAS and this write ⇒ the history is one version behind;
//!   `table restore` detects it (the "last copy" proof fails) and refuses.
//! - **The name is the content**: written with `Create`; `AlreadyExists` ⇒ read back and
//!   compare bytes ([`crate::cas::put_resolving`]). Different bytes at the same version (a table
//!   from another timeline — `cluster.conf` was manually rolled back to an old copy through
//!   versioning) ⇒ **keep** the existing copy, log + [`stats::CONFLICT`] (delete only when
//!   provable).
//! - **A new object, read leniently**: absent ⇒ no history (a cluster of a binary from before
//!   table history existed, or a failed write). No gate, no migration step (D27).
//! - **Keep the [`KEEP`] newest copies**; older ones go through the trash with
//!   [`crate::proof::DeleteProof::stale_table_history`] (delete only when provable).
//! - **An error here does not break a table change that already won**: log + [`stats`].
//!
//! Cost: 1 PUT per table change (split/merge: a few per op), plus — from version [`KEEP`] + 1
//! on — one LIST and one move into the trash.

use {
    crate::{
        cas::{put_resolving, Cas},
        error::{Result, StoreError},
        proof::DeleteProof,
        trash::{EntryId, Subject, Trash},
    },
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode},
    std::sync::{atomic::Ordering, Arc},
};

/// History directory under the cluster's prefix.
pub const DIR: &str = "cluster-history";

/// Number of newest copies kept.
pub const KEEP: u64 = 16;

/// Counters for `/metrics` (`cairnflow_cluster_history_failures_total{step}`).
pub mod stats {
    use std::sync::atomic::AtomicU64;

    /// Writing a history copy failed (after the client's retry bound): the history lacks that
    /// version until the table changes again.
    pub static WRITE_FAILED: AtomicU64 = AtomicU64::new(0);
    /// A history copy of the same version already exists with different bytes — the existing
    /// copy is kept.
    pub static CONFLICT: AtomicU64 = AtomicU64::new(0);
    /// Pruning copies older than [`super::KEEP`] failed (LIST, proof or trash).
    pub static PRUNE_FAILED: AtomicU64 = AtomicU64::new(0);
}

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

fn dir_path(prefix: &Path) -> Path {
    let p = prefix.as_ref();
    if p.is_empty() {
        Path::from(DIR)
    } else {
        Path::from(format!("{p}/{DIR}"))
    }
}

/// `<prefix>/cluster-history/<version>`.
pub fn path(prefix: &Path, version: u64) -> Path {
    Path::from(format!("{}/{version}", dir_path(prefix).as_ref()))
}

/// Outcome of [`write`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recorded {
    /// The history copy on the bucket is exactly the bytes we meant to write.
    Written,
    /// A copy of the same version already exists with different bytes — it is kept as is.
    Conflict,
}

/// Writes history copy `version` carrying `body` (the exact bytes of `cluster.conf`).
pub async fn write(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    version: u64,
    body: Bytes,
) -> Result<Recorded> {
    crate::failpoint!("cluster::history::before_write", err);
    match put_resolving(store, &path(prefix, version), body, PutMode::Create).await {
        Ok(Cas::Written(_)) => Ok(Recorded::Written),
        Ok(Cas::Lost) => Ok(Recorded::Conflict),
        Err(e) => Err(io(e)),
    }
}

/// Versions of every history copy on the bucket, ascending (one LIST). Names that are not
/// canonical decimal numbers are skipped — they are not ours.
pub async fn versions(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<u64>> {
    let dir = dir_path(prefix);
    let listed: Vec<_> = store.list(Some(&dir)).try_collect().await.map_err(io)?;
    let mut out: Vec<u64> = listed
        .iter()
        .filter_map(|m| {
            let name = m.location.filename()?;
            let v: u64 = name.parse().ok()?;
            (v.to_string() == name && path(prefix, v) == m.location).then_some(v)
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// Bytes of history copy `version`. `Ok(None)` = absent.
pub async fn read(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    version: u64,
) -> Result<Option<Bytes>> {
    match store.get(&path(prefix, version)).await {
        Ok(g) => Ok(Some(g.bytes().await.map_err(io)?)),
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(e) => Err(io(e)),
    }
}

/// **Moves** the history copy of `proof` into the trash. Gate: `proof` must be
/// [`DeleteProof::stale_table_history`]. A copy already gone ⇒ `Ok` (idempotent).
pub async fn delete(store: &Arc<dyn ObjectStore>, prefix: &Path, proof: DeleteProof) -> Result<()> {
    let version = match proof.subject() {
        Subject::TableHistory(v) => *v,
        other => {
            return Err(StoreError::Io(format!(
                "cluster_history::delete needs a proof for a table history copy, got {other}"
            )))
        }
    };
    proof.check(&Subject::TableHistory(version), None).await?;
    Trash::new(Arc::clone(store), prefix)
        .move_into(
            &EntryId::new(crate::entropy::now_ms()),
            &Subject::TableHistory(version),
            &format!("table history outside the {KEEP} newest copies"),
            &[path(prefix, version)],
        )
        .await
        .map(|_| ())
}

/// Prunes every history copy older than the [`KEEP`] copies counted up to `newest`. Returns the
/// number of copies moved into the trash. A copy that cannot be proven is kept.
pub async fn prune(store: &Arc<dyn ObjectStore>, prefix: &Path, newest: u64) -> Result<usize> {
    if newest <= KEEP {
        return Ok(0);
    }
    let mut n = 0;
    for v in versions(store, prefix).await? {
        if v + KEEP > newest {
            break;
        }
        match DeleteProof::stale_table_history(store, prefix, v).await? {
            Ok(proof) => {
                delete(store, prefix, proof).await?;
                n += 1;
            }
            Err(why) => tracing::debug!(version = v, reason = %why, "keeping table history copy"),
        }
    }
    Ok(n)
}

/// After a table write that **won** (`body` is the exact bytes on the bucket at `version`):
/// writes the history copy, then prunes old copies. Never returns an error — the table change
/// already won; errors here are only logged and counted.
pub(crate) async fn after_won(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    version: u64,
    body: Bytes,
) {
    match write(store, prefix, version, body).await {
        Ok(Recorded::Written) => {}
        Ok(Recorded::Conflict) => {
            stats::CONFLICT.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                version,
                "cluster-history/{version} already exists with bytes different from the table that \
                 just won — keeping the existing copy (was cluster.conf rolled back to an old copy?)"
            );
        }
        Err(e) => {
            stats::WRITE_FAILED.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(version, error = %e, "cannot write the range-table history");
        }
    }
    if let Err(e) = prune(store, prefix, version).await {
        stats::PRUNE_FAILED.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(version, error = %e, "cannot prune old range-table history");
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
            ClusterConfig, PartitionTable, TableUpdate,
        },
        slatedb::object_store::memory::InMemory,
    };

    fn shift(by: u64) -> impl FnOnce(&PartitionTable) -> Result<PartitionTable> + Send {
        move |t: &PartitionTable| {
            let mut r = t.ranges().to_vec();
            r[0].end -= by;
            r[1].start -= by;
            PartitionTable::from_entries(t.version() + 1, t.next_range_id(), r)
        }
    }

    /// A failed history write does not break a table change that already won: `update` is still
    /// `Applied`, the table on the bucket has changed, the history lacks exactly that version
    /// and the counter goes up. The next change (store healthy again) writes normally.
    #[tokio::test]
    async fn failed_history_write_never_fails_the_table_change() {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let p = Path::from("p");
        let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
        faulty.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(DIR.into()),
            Action::Fail { times: Some(1) },
        ));
        let before = stats::WRITE_FAILED.load(Ordering::Relaxed);
        let got = c.update(1, shift(3)).await.unwrap();
        assert!(matches!(got, TableUpdate::Applied(_)), "{got:?}");
        assert_eq!(
            crate::cluster::stored_version(&store, &p).await.unwrap(),
            Some(2)
        );
        assert!(stats::WRITE_FAILED.load(Ordering::Relaxed) > before);
        let got = c.update(2, shift(3)).await.unwrap();
        assert!(matches!(got, TableUpdate::Applied(_)), "{got:?}");
        assert_eq!(versions(&store, &p).await.unwrap(), [1, 3]);
    }

    /// A copy of the same version with different bytes (a table from another timeline) is
    /// **kept**, not overwritten (delete only when provable).
    #[tokio::test]
    async fn conflicting_history_is_kept() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::default();
        store
            .put(&path(&p, 2), Bytes::from_static(b"other").into())
            .await
            .unwrap();
        let got = write(&store, &p, 2, Bytes::from_static(b"ours"))
            .await
            .unwrap();
        assert_eq!(got, Recorded::Conflict);
        assert_eq!(read(&store, &p, 2).await.unwrap().unwrap(), "other");
        // Same bytes ⇒ `Written` (our own retry).
        let got = write(&store, &p, 2, Bytes::from_static(b"other"))
            .await
            .unwrap();
        assert_eq!(got, Recorded::Written);
    }

    /// Proof gate: a copy still among the `KEEP` newest, or a bucket without a table, ⇒ not
    /// provable.
    #[tokio::test]
    async fn stale_history_proof_reads_the_stored_table() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::from("p");
        assert!(DeleteProof::stale_table_history(&store, &p, 1)
            .await
            .unwrap()
            .is_err());
        let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
        for v in 1..=KEEP {
            c.update(v, shift(1)).await.unwrap();
        }
        // Table at version KEEP + 1: copy 1 has just left the window, copy 2 is still in it.
        assert!(DeleteProof::stale_table_history(&store, &p, 1)
            .await
            .unwrap()
            .is_ok());
        assert!(DeleteProof::stale_table_history(&store, &p, 2)
            .await
            .unwrap()
            .is_err());
    }

    /// **Wrong deletion, then restore** (SM-16): a pruned history copy sits in the trash and
    /// `restore` copies it back to the same place, with the same bytes.
    #[tokio::test]
    async fn pruned_history_restores_from_the_trash() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = Path::from("p");
        let c = ClusterConfig::open(&store, &p, 2).await.unwrap();
        let first = read(&store, &p, 1).await.unwrap().unwrap();
        for v in 1..=KEEP {
            c.update(v, shift(1)).await.unwrap();
        }
        assert_eq!(versions(&store, &p).await.unwrap().first(), Some(&2));
        let bin = Trash::new(Arc::clone(&store), &p);
        let entries = bin.entries().await.unwrap();
        let e = entries
            .iter()
            .find(|e| e.subject == Some(Subject::TableHistory(1)))
            .expect("trash entry of copy 1");
        let r = bin.restore(&e.id).await.unwrap();
        assert_eq!(r.restored, 1);
        assert_eq!(read(&store, &p, 1).await.unwrap(), Some(first));
        assert_eq!(
            crate::trash::verdict(
                Some(&Subject::TableHistory(1)),
                &crate::trash::Facts::default()
            ),
            crate::trash::Verdict::Purge
        );
    }
}
