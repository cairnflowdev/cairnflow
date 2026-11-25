// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Durable operation ledger of the coordinator and of whoever rolls back:
//! `ops/<first child>` through `cairn_store::ledger`.
//!
//! The coordinator creates the ledger right after its intent CAS wins and moves
//! its phase **before** each step's side effects (closed mark, clone, commit CAS);
//! a refused ledger (already on the roll-back branch — someone else is rolling the
//! op back) or a failed write ⇒ the step fails, roll back. Whoever rolls back
//! writes `Aborting` before the intent-removal CAS and `Cleaned` after that CAS
//! wins; an op without a ledger (binaries older than the ledger) or a ledger
//! already at an outcome goes on as before — the table is the arbiter.

use {
    super::*,
    cairn_split::{Advance, Op, Phase},
    cairn_store::ledger::{advance_op, create_op, ObservedOp, OpRecord},
};

/// Ledger of an op the coordinator is running.
pub(super) struct Ledger {
    op: Op,
    who: String,
    seen: Option<ObservedOp>,
}

impl RangeOps {
    fn ledger_who(&self) -> String {
        self.session
            .current()
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.node_id.clone())
    }

    /// Create the ledger at `Intent` — call right after the intent CAS wins.
    pub(super) async fn open_ledger(&self, op: Op) -> OpResult<Ledger> {
        let who = self.ledger_who();
        let (store, prefix) = (self.stores.main().store(), self.stores.main().prefix());
        let seen = create_op(store, prefix, op, &who).await?;
        Ok(Ledger {
            op,
            who,
            seen: Some(seen),
        })
    }

    /// Move the ledger to `to` before that step's side effects. Refused ⇒ `Busy`:
    /// the ledger is on the roll-back branch, someone else is rolling this op back.
    pub(super) async fn advance_ledger(
        &self,
        ledger: &mut Ledger,
        to: Option<Phase>,
        edit: impl Fn(&mut OpRecord),
    ) -> OpResult<()> {
        let Some(to) = to else { return Ok(()) };
        let (store, prefix) = (self.stores.main().store(), self.stores.main().prefix());
        match advance_op(
            store,
            prefix,
            ledger.op.key(),
            to,
            &ledger.who,
            &mut ledger.seen,
            edit,
        )
        .await?
        {
            Advance::Write | Advance::Already => Ok(()),
            Advance::Refused => Err(RangeOpError::Busy(format!(
                "operation ledger ops/{} cannot move to {to:?} (someone rolling the op back?) — rolled back, retry",
                ledger.op.key()
            ))),
        }
    }
}

impl RangeOps {
    /// The `Committed` ledger after the commit CAS wins — only records a fact already on
    /// the table: a failure is logged (the table is the arbiter, nobody decides on this phase alone).
    pub(super) async fn ledger_committed(&self, ledger: &mut Ledger, to: Option<Phase>) {
        if let Err(e) = self.advance_ledger(ledger, to, |_| {}).await {
            tracing::warn!(key = ledger.op.key(), error = %e, "cannot write the Committed ledger after commit");
        }
    }
}

/// Whoever rolls back writes `to` (`Aborting` before the intent-removal CAS, `Cleaned`
/// after that CAS wins). No ledger, or a ledger already at an outcome ⇒ `Ok` — the table
/// is the arbiter. A failed write ⇒ error (`Aborting`: the roller retries; `Cleaned`: only logged by the caller).
pub(crate) async fn mark_ledger(
    stores: &ObjectStores,
    op: Op,
    to: Phase,
) -> cairn_store::Result<()> {
    let (store, prefix) = (stores.main().store(), stores.main().prefix());
    let r = advance_op(store, prefix, op.key(), to, "roll-back", &mut None, |_| {}).await?;
    if r == Advance::Refused {
        tracing::debug!(
            key = op.key(),
            ?to,
            "operation ledger cannot move to this phase (no ledger, or already done) — going on by the table"
        );
    }
    Ok(())
}
