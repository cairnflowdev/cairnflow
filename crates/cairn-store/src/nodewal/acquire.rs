// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Opening a Db on the shared WAL and the range acquisition sequence ([`acquire_shared`]).

use super::*;

/// Opens the Db of a range that uses the shared WAL, after the old logs have been sealed and
/// read. Returns the Db and the first wal id the Db will write.
pub(crate) async fn open_db(
    stores: &ObjectStores,
    wal: &NodeWal,
    takeover: Takeover,
) -> Result<(Db, u64)> {
    let range = takeover.range;
    let first = Arc::new(OnceLock::new());
    let init = wal.writer_init(takeover, Arc::clone(&first));
    let db = stores
        .build_db(range, Some((Box::new(init), wal.wal_gc())))
        .await?;
    let first = *first.get().ok_or_else(|| {
        StoreError::Corrupt(format!(
            "range {range}: SlateDB opened the Db without calling WriterInit"
        ))
    })?;
    wal.register_db(range, &db);
    Ok((db, first))
}

/// The sequence for **acquiring a range** that uses the shared WAL:
///
/// 1. seal every log in `wal_segment` and read the range's frames (once per log for the
///    whole node);
/// 2. open the Db: SlateDB bumps `writer_epoch` and replays the frames above
///    `replay_after_wal_id`;
/// 3. CAS our segment into the owner record **before** the first frame (C4);
/// 4. flush the replayed rows to L0 and CAS the old segments away **before serving**
///    (C6, C7).
///
/// An owner-record CAS that misses ⇒ the range was lost behind our back ⇒ fail-stop the
/// incarnation (C1) and return [`StoreError::LeaseLost`].
pub async fn acquire_shared(
    stores: &ObjectStores,
    wal: &NodeWal,
    keeper: &mut LeaseKeeper,
    range: u32,
) -> Result<Db> {
    // Replay mark hint: a lenient read — on failure, read every frame of the range as before.
    let hint = match stores.manifest_head(range).await {
        Ok(head) => head.map(|h| h.replay_after_wal_id),
        Err(e) => {
            tracing::debug!(range, error = %e, "cannot read the manifest before acquiring the range — reading every frame");
            None
        }
    };
    acquire_shared_with_hint(stores, wal, keeper, range, hint).await
}

/// Like [`acquire_shared`], with `replay_after_wal_id` already read from the range Db's
/// manifest (usually by [`ObjectStores::adopt_shared_wal`] just before): reading the dead
/// logs skips the range's frames with wal id ≤ that mark, sparing GETs of objects that only
/// carry frames already in L0. The mark only grows, so an old read is a lower bound of the
/// mark at Db open — SlateDB never replays frames ≤ that mark anyway, and our first id
/// (`SharedWriterInit`) is already ≥ mark + 1. `None` = read every frame.
pub async fn acquire_shared_with_hint(
    stores: &ObjectStores,
    wal: &NodeWal,
    keeper: &mut LeaseKeeper,
    range: u32,
    replay_after_hint: Option<u64>,
) -> Result<Db> {
    if keeper.session() != wal.session() {
        return Err(StoreError::LeaseLost);
    }
    // The range's slot has rotated and has no new stream yet: do not open the Db into it —
    // before any seal or segment CAS. The keeper drops ⇒ yields, and the range goes to
    // another node (the node is draining, the balancer does not take it back).
    if !wal.slot_ready(range) {
        return Err(StoreError::Io(format!(
            "range {range}: the range's shared WAL slot has rotated and has no new stream yet"
        )));
    }
    // Old logs this acquisition may drop from the owner record (C7): a hint for the purge
    // round after the drop — a dead log stays whole until the last range listing it is
    // taken over, so this is the moment it may have just stopped being listed.
    let own = wal.log_of(range);
    let old_logs: Vec<LogId> = keeper
        .wal()
        .segments
        .iter()
        .map(|s| s.log.clone())
        .filter(|l| *l != own)
        .collect();
    let takeover = wal
        .take_over(range, keeper.wal(), replay_after_hint)
        .await?;
    tracing::debug!(
        range,
        frames = takeover.frames.len(),
        "acquire range: old logs sealed and read"
    );
    let max_seen = takeover
        .frames
        .iter()
        .map(|f| f.wal_file_id)
        .max()
        .unwrap_or(0);
    let (db, first) = open_db(stores, wal, takeover).await?;
    tracing::debug!(range, first, "acquire range: Db opened");
    let lost = |db: Db, e: StoreError| async move {
        wal.forget_db(range);
        // Bounded close: if acquiring the range failed because the store hangs, closing
        // hangs too. Past the bound ⇒ drop the Db; the next open bumps `writer_epoch` and
        // fences it.
        let _ =
            crate::retry::deadline(&crate::retry::ops::OPEN_DB, stores.open_bound(), db.close())
                .await;
        if matches!(e, StoreError::LeaseLost) {
            wal.fail_stop(
                crate::failstop::Cause::OwnerCasLost,
                format!("range {range}: range lost while acquiring (owner record CAS missed)"),
            );
        }
        e
    };
    let seg = WalSegment {
        log: wal.log_of(range),
        first_wal_id: first,
    };
    if let Err(e) = keeper.add_wal_segment(seg).await {
        return Err(lost(db, e).await);
    }
    tracing::debug!(range, "acquire range: segment CASed");
    // Cover up to `max_seen`, not just "flush": replayed rows already in L0 get filtered,
    // the memtable is empty, and a plain flush leaves the mark below the last frame of the
    // old log (see `flush_through`).
    //
    // Bounded ([`ObjectStores::open_bound`]): past the bound ⇒ acquiring the range fails
    // and the keeper yields. The segment we just CASed has not served any turn
    // (`unserved_segment`), so the yield drops it — a clean yield, the next taker does not
    // seal the node's live log. The only frame of the range that may already be in our log
    // is the fence of `flush_through` (a delete of a key outside every range): losing it
    // loses nothing.
    let flushed = crate::retry::deadline(
        &crate::retry::ops::FLUSH_THROUGH,
        stores.open_bound(),
        async {
            crate::failpoint::stall_if("nodewal::acquire::flush_stall", range).await;
            let stripe = wal.0.stripes().into_iter().find(|s| s.log == own);
            flush_through_unbounded(&db, max_seen, stripe.as_ref().map(|s| (s, range))).await
        },
    )
    .await
    .unwrap_or_else(|e| Err(e.into()));
    if let Err(e) = flushed {
        return Err(lost(db, e).await);
    }
    // We have read **all** of the canonical part of every old log, except frames ≤ the
    // replay mark hint (≤ `replay_after` below, since the mark only grows): every frame of
    // the range in them has id ≤ max(`max_seen`, `replay_after`). Once `replay_after_wal_id`
    // is past that mark, no old log still holds a frame not yet in L0 — every segment before
    // ours can be dropped, even when our first id sits higher (the `wal_floor_id` floor, or
    // the manifest's `next_wal_sst_id`).
    tracing::debug!(range, "acquire range: replayed rows flushed");
    // The **stored** mark, not `db.manifest()`: SlateDB changes the mark in RAM before it
    // writes the manifest (bug class NW-7), and cutting segments by a mark that is not yet
    // durable drops an old log that still holds frames not yet in L0. `flush_through` only
    // returns after the manifest is written, so the stored copy already has the mark of this
    // flush.
    let mark = match crate::proof::DurableReplayMark::read(stores, range).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            return Err(lost(
                db,
                StoreError::Io(format!("range {range}: Db has no manifest after the flush")),
            )
            .await)
        }
        Err(e) => return Err(lost(db, e).await),
    };
    let replay_after = mark.value();
    let covered = if replay_after >= max_seen {
        replay_after.max(first.saturating_sub(1))
    } else {
        replay_after
    };
    if let Err(e) = keeper.prune_wal_segments(mark, covered).await {
        return Err(lost(db, e).await);
    }
    // Every old log should be gone now (the replay mark covers them all). One that stays
    // keeps a dead log whole until a later takeover of this range drops it — a 24 h soak saw a
    // dead log listed for 13 minutes without knowing by which range. Rare: one line per
    // takeover that leaves one.
    let kept: Vec<String> = keeper
        .wal()
        .segments
        .iter()
        .filter(|s| s.log != own)
        .map(|s| format!("{}@{}", s.log, s.first_wal_id))
        .collect();
    if !kept.is_empty() {
        tracing::warn!(
            range,
            kept = ?kept,
            replay_after,
            covered,
            max_seen,
            first,
            "acquire range: old shared WAL segments still listed after the takeover"
        );
    }
    wal.hint_unlisted(old_logs);
    // The keeper detaches the Db from the node WAL when yielding, and marks it clean after
    // the seal (`LeaseKeeper::writer_sealed`).
    keeper.attach_shared(wal.clone());
    Ok(db)
}
