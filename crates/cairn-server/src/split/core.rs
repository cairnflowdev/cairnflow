// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Bridge between the shell and the `cairn_split` core: turns the range table,
//! intents and CAS results into the core's `View`/`Event`. Every decision to roll
//! back, go on, clean up children, or remove a closed mark in `split/` (and in
//! `factory`, the orphan round, ctl) asks [`cairn_split::step`] through here.

use {
    super::*,
    cairn_split::{Cas, Entry, Event, Plan, Stage, Standing, View},
};

// `op_of` and `standing` live in `cairn_store::ledger`: the
// `DeleteProof::settled_op` gate (cairn-store) needs that very read.
pub use cairn_store::ledger::{op_of, standing};

/// Range `p` as table `t` sees it.
pub fn entry(t: &PartitionTable, p: PartitionId) -> Entry {
    let r = t.range(p);
    Entry {
        in_table: r.is_some(),
        intent: r.and_then(|r| r.intent).map(|i| op_of(p, i)),
    }
}

/// Result of `ClusterConfig::update`, for the core.
pub fn cas(r: &cairn_store::Result<TableUpdate>) -> Cas {
    match r {
        Ok(TableUpdate::Applied(_)) => Cas::Applied,
        Ok(TableUpdate::Conflict(_)) => Cas::Lost,
        Err(_) => Cas::Unknown,
    }
}

/// The core's decision for one coordinator step while the op stands at `standing`:
/// `Ok(ledger phase to write before that step)`, or `Busy` (the coordinator rolls back).
pub fn stage(stage: Stage, standing: Standing, p: PartitionId) -> OpResult<Option<Phase>> {
    let view = View {
        standing: Some(Ok(standing)),
        ..View::default()
    };
    match cairn_split::step(&view, &Event::Enter(stage)) {
        Plan::Go { ledger } => Ok(ledger),
        Plan::RollBack { .. } => Err(RangeOpError::Busy(format!(
            "intent on range {} no longer belongs to this run (rolled back by someone else?) — retry",
            p.0
        ))),
        other => unreachable!("core returned {other:?} for Enter({stage:?})"),
    }
}

/// How the core tells the coordinator to roll back in place after a failed step:
/// whether to clean up the op's own children.
pub fn own_orphans_after_failure() -> bool {
    match cairn_split::step(&View::default(), &Event::Failed) {
        Plan::RollBack { own_orphans, .. } => own_orphans,
        other => unreachable!("core returned {other:?} for Failed"),
    }
}

/// Intent CAS result through the core: won ⇒ the new table; not clearly won ⇒ error
/// (the caller runs `undo_stray_intent` — the core returns `UndoStray`).
pub fn intent_cas(r: cairn_store::Result<TableUpdate>) -> OpResult<Arc<PartitionTable>> {
    match cairn_split::step(&View::default(), &Event::IntentCas(cas(&r))) {
        Plan::Go { .. } => match r {
            Ok(TableUpdate::Applied(t)) => Ok(t),
            _ => unreachable!("Go only for Applied"),
        },
        Plan::UndoStray => Err(match r {
            Err(e) => e.into(),
            _ => RangeOpError::Conflict("range table changed midway — retry".into()),
        }),
        other => unreachable!("core returned {other:?} for IntentCas"),
    }
}

/// Commit CAS result through the core: won ⇒ (new table, ledger phase written after
/// commit); lost or unclear ⇒ error, the coordinator rolls back (a roll-back round
/// that sees the op committed stops).
pub fn commit_cas(
    r: cairn_store::Result<TableUpdate>,
) -> OpResult<(Arc<PartitionTable>, Option<Phase>)> {
    match cairn_split::step(&View::default(), &Event::CommitCas(cas(&r))) {
        Plan::Go { ledger } => match r {
            Ok(TableUpdate::Applied(t)) => Ok((t, ledger)),
            _ => unreachable!("Go only for Applied"),
        },
        Plan::RollBack { .. } => Err(match r {
            Err(e) => e.into(),
            _ => RangeOpError::Conflict("range table changed before commit".into()),
        }),
        other => unreachable!("core returned {other:?} for CommitCas"),
    }
}

/// A roll-back by `who` failed: the core says whether to retry in the background.
pub fn settle_after(who: Who) -> bool {
    match cairn_split::step(&View::default(), &Event::RollBackFailed(who)) {
        Plan::Settle => true,
        Plan::Fail | Plan::GiveUp => false,
        other => unreachable!("core returned {other:?} for RollBackFailed({who:?})"),
    }
}

impl RangeOps {
    /// A non-coordinator (`finder`) sees `intent` on range `p`: the core decides
    /// to roll back or go on (`Event::Found`) — reading the ledger `ops/<key>` when
    /// the core asks, and the direct check for going on when the ledger is at `Prepared`.
    pub(crate) async fn found(
        &self,
        p: PartitionId,
        intent: RangeIntent,
        finder: cairn_split::Finder,
    ) -> Plan {
        found_with(&self.stores, p, intent, finder).await
    }
}

/// [`RangeOps::found`] without needing `RangeOps` (source taker, ctl). A failed read
/// ⇒ the core sees `Err` and rolls back (always safe: the intent-removal CAS races the commit CAS).
pub(crate) async fn found_with(
    stores: &ObjectStores,
    p: PartitionId,
    intent: RangeIntent,
    finder: cairn_split::Finder,
) -> Plan {
    let op = op_of(p, intent);
    let (store, prefix) = (stores.main().store(), stores.main().prefix());
    let mut view = View::default();
    let mut record = None;
    loop {
        match cairn_split::step(&view, &Event::Found(finder)) {
            Plan::Need(cairn_split::Need::Ledger) => {
                view.ledger = Some(
                    match cairn_store::ledger::read_op(store, prefix, op.key()).await {
                        Ok(Some(o)) if o.record.op == op => {
                            let phase = o.record.phase;
                            record = Some(o.record);
                            Ok(Some(phase))
                        }
                        // Ledger of another op under the same key: corrupt bucket — do not trust.
                        Ok(Some(_)) => Err(cairn_split::ReadErr),
                        Ok(None) => Ok(None),
                        Err(e) => {
                            tracing::warn!(range = p.0, error = %e, "cannot read the operation ledger — rolling back");
                            Err(cairn_split::ReadErr)
                        }
                    },
                );
            }
            Plan::Need(cairn_split::Need::Prepared) => {
                let rec = record.as_ref().expect("Prepared needs a ledger");
                view.prepared = Some(match super::forward::prepared(stores, op, rec).await {
                    Ok(ok) => Ok(ok),
                    Err(e) => {
                        tracing::warn!(range = p.0, error = %e, "cannot check the prepared op — rolling back");
                        Err(cairn_split::ReadErr)
                    }
                });
            }
            Plan::Need(n) => unreachable!("core asked for {n:?} for Found"),
            plan => return plan,
        }
    }
}
