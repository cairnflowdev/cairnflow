// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! [`SharedWalAdmin`] — the shared WAL's `slatedb::wal::WalAdmin`; `NoopWalGc`.

use super::*;

/// The shared WAL's `WalAdmin` for admin operations on **one** Db (clone for split/merge,
/// changing `wal_name`). The Db's frames live in the node log, not under the Db's path, and
/// the stored manifest **lags** the WAL (the writer only writes `next_wal_sst_id` when it
/// flushes the memtable) — so "the Db's WAL is empty" is asked of the range's owner record:
///
/// - empty ⇔ the manifest covers every id it knows **and** the owner record has no
///   `wal_segment` left (nobody has ever acquired the range, or the last owner sealed
///   cleanly — `LeaseKeeper::writer_sealed` CASes the record right after the seal, even when
///   the range quiesces and keeps its lease). Segments left ⇒ there may still be frames not
///   yet in L0 ⇒ "not empty" (conservative: better to refuse a clone than to lose acked
///   rows);
/// - `clone_wal` copies nothing and only accepts a source that is empty as above;
/// - there is nothing to delete or GC per Db (the node's combined GC takes care of it).
pub struct SharedWalAdmin {
    owner_store: Arc<dyn ObjectStore>,
    owner_prefix: Path,
}

impl SharedWalAdmin {
    pub(crate) fn new(owner_store: Arc<dyn ObjectStore>, owner_prefix: Path) -> Self {
        Self {
            owner_store,
            owner_prefix,
        }
    }

    /// `…/ranges/<id>` ⇒ range id.
    fn range_of(path: &Path) -> Option<u32> {
        let mut parts: Vec<_> = path.parts().collect();
        let id = parts.pop()?.as_ref().parse().ok()?;
        (parts.pop()?.as_ref() == "ranges").then_some(id)
    }

    async fn empty(
        &self,
        path: &Path,
        replay_after: u64,
        last_seen: u64,
    ) -> std::result::Result<bool, WalError> {
        if replay_after < last_seen {
            return Ok(false);
        }
        let Some(range) = Self::range_of(path) else {
            return Ok(false);
        };
        let rec = crate::owner::read_owner(
            &self.owner_store,
            &self.owner_prefix,
            crate::PartitionId(range),
        )
        .await
        .map_err(|e| stripe::unavailable(format!("read owner record of range {range}: {e}")))?;
        Ok(rec.is_none_or(|r| r.wal_segments.is_empty()))
    }
}

#[async_trait]
impl slatedb::wal::WalAdmin for SharedWalAdmin {
    fn garbage_collector(&self, _path: &Path) -> Arc<dyn WalGc> {
        Arc::new(NoopWalGc)
    }

    fn wal_name(&self) -> Option<&str> {
        Some(SHARED_WAL_NAME)
    }

    async fn delete_wal(
        &self,
        _path: &Path,
        _dry_run: bool,
    ) -> std::result::Result<Vec<String>, WalError> {
        Ok(Vec::new())
    }

    async fn is_empty(
        &self,
        path: &Path,
        replay_after_wal_id: u64,
        wal_id_last_seen: u64,
    ) -> std::result::Result<bool, WalError> {
        self.empty(path, replay_after_wal_id, wal_id_last_seen)
            .await
    }

    async fn clone_wal(
        &self,
        from_path: &Path,
        from_manifest: slatedb::VersionedManifest,
        _to_path: &Path,
    ) -> std::result::Result<(u64, u64), WalError> {
        let replay_after = from_manifest.replay_after_wal_id();
        let last = from_manifest.next_wal_sst_id().saturating_sub(1);
        if !self.empty(from_path, replay_after, last).await? {
            return Err(stripe::unavailable(format!(
                "clone {from_path}: a Db on the shared WAL may still hold frames not yet in L0 \
                 — it must be sealed before cloning"
            )));
        }
        Ok((replay_after, last))
    }
}

/// The `WalGc` plugged into SlateDB's GC for each Db: deletes nothing. An object of the
/// shared log can only be deleted when **every** range with frames in it agrees (fork patch
/// F1) — that is the job of the node's combined GC ([`gc_loop`]), not of each Db's GC.
pub(crate) struct NoopWalGc;

#[async_trait]
impl WalGc for NoopWalGc {
    async fn collect(
        &self,
        _referenced_ranges: Vec<WalFileRange>,
        _min_age: Duration,
        _dry_run: bool,
    ) -> std::result::Result<(), WalError> {
        Ok(())
    }
}
