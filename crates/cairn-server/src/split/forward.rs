// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Roll forward** an op that finished preparing but whose coordinator
//! vanished: CAS the commit in its place.
//!
//! Only when the core says so (`Plan::RollForward`): the ledger `ops/<key>` is at
//! `Prepared` **and** the direct check ([`prepared`]) sees every child with a Db
//! and a live owner record, every source still carrying the closed mark of
//! **exactly** the op's close (`writer_epoch` = the sealed epoch + 1 — nobody
//! reopened the source after that close, so children cloned after it hold every
//! acknowledged write). The commit is the same table transformation as the
//! coordinator's ([`committed_table`]): a slow coordinator that CASes later reads
//! back exactly the table it meant to write (`put_resolving`) — both "win", nobody
//! rolls back. The two children carry owner records of the coordinator's session:
//! any node can take them once that session dies; sources that leave the table
//! retire as on every commit (SM-8, SM-11).

use {super::*, cairn_split::Op, cairn_store::ledger::OpRecord};

/// Table after commit of `intent` on `p`, from `t` (the table at the version the
/// CAS compares): split ⇒ drop `p`, add two children `gen + 1`; merge ⇒ drop both
/// sources, add `into` with `max(gen) + 1`. The coordinator and the roll-forward
/// share it, so two CASes from the same version write out the **same** table.
pub(crate) fn committed_table(
    t: &PartitionTable,
    p: PartitionId,
    intent: RangeIntent,
) -> cairn_store::Result<PartitionTable> {
    let missing = |r: PartitionId| StoreError::BadClusterConfig {
        reason: format!("range {} is no longer in the table at commit", r.0),
    };
    let src = *t.range(p).ok_or_else(|| missing(p))?;
    let mut ranges: Vec<RangeEntry> = Vec::new();
    match intent {
        RangeIntent::Splitting { at, left, right } => {
            ranges.extend(t.ranges().iter().filter(|r| r.id != p).copied());
            let gen = src.gen + 1;
            ranges.push(RangeEntry {
                gen,
                ..RangeEntry::new(src.start, at - 1, left)
            });
            ranges.push(RangeEntry {
                gen,
                ..RangeEntry::new(at, src.end, right)
            });
        }
        RangeIntent::Merging { with, into } => {
            let other = *t.range(with).ok_or_else(|| missing(with))?;
            let (lo, hi) = if src.start < other.start {
                (src, other)
            } else {
                (other, src)
            };
            ranges.extend(
                t.ranges()
                    .iter()
                    .filter(|r| r.id != lo.id && r.id != hi.id)
                    .copied(),
            );
            ranges.push(RangeEntry {
                gen: lo.gen.max(hi.gen) + 1,
                ..RangeEntry::new(lo.start, hi.end, into)
            });
        }
    }
    PartitionTable::from_entries(t.version() + 1, t.next_range_id(), ranges)
}

/// The direct check for rolling forward: every child has a Db (manifest) and an
/// owner record without a tombstone; every source carries the closed mark at exactly
/// `sealed + 1`. No `sealed` for a source (the ledger of the close was not written) ⇒ no.
pub(crate) async fn prepared(
    stores: &ObjectStores,
    op: Op,
    record: &OpRecord,
) -> cairn_store::Result<bool> {
    let (store, prefix) = (stores.main().store(), stores.main().prefix());
    for c in op.children().iter() {
        let c = PartitionId(c);
        if !stores.range_db_exists(c).await? {
            return Ok(false);
        }
        match cairn_store::read_owner(store, prefix, c).await? {
            Some(rec) if !rec.retired => {}
            _ => return Ok(false),
        }
    }
    for s in op.sources().iter() {
        let Some(&(_, sealed)) = record.sealed.iter().find(|(r, _)| *r == s) else {
            return Ok(false);
        };
        match stores.range_db_close_mark(PartitionId(s)).await? {
            Some(m) if m.closed && m.writer_epoch == sealed + 1 => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// CAS the commit in place of the coordinator of `intent` on `p` (table re-read).
/// `Ok(true)` = the table is now the post-commit table; `Ok(false)` = the intent is
/// gone (committed or rolled back, or the table just changed — the caller re-evaluates).
pub async fn roll_forward(
    cluster: &ClusterConfig,
    stores: &ObjectStores,
    p: PartitionId,
    intent: RangeIntent,
) -> cairn_store::Result<bool> {
    cluster.reload().await?;
    let t = cluster.table();
    if t.range(p).and_then(|r| r.intent) != Some(intent) {
        return Ok(false);
    }
    let op = core::op_of(p, intent);
    let applied = matches!(
        cluster
            .update(t.version(), |cur| committed_table(cur, p, intent))
            .await?,
        TableUpdate::Applied(_)
    );
    if applied {
        tracing::warn!(
            range = p.0,
            ?intent,
            "op finished preparing but its coordinator vanished — rolled forward (commit CAS)"
        );
        if let Err(e) = mark_ledger(stores, op, Phase::Committed).await {
            tracing::warn!(range = p.0, error = %e, "cannot write the Committed ledger after rolling forward");
        }
    }
    Ok(applied)
}
