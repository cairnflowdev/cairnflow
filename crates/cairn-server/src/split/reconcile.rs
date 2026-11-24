// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The intent reconciliation round (SM-15): `IntentReconciler`.

use super::*;

// ─────────────────────────────────────── intent reconciliation round (SM-15)

/// Default interval of the intent reconciliation round (`NodeConfig::reconcile_intents_every`).
pub const DEFAULT_RECONCILE_INTENTS_EVERY: Duration = Duration::from_secs(15);

/// Result of one intent reconciliation round.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntentRound {
    /// Table version the round was based on.
    pub table_version: u64,
    /// Ranges (the lowest of the intent) just rolled back.
    pub rolled_back: Vec<u32>,
    /// Ranges (the lowest of the intent) whose prepared op was just rolled forward.
    pub rolled_forward: Vec<u32>,
    /// A coordinator holds it.
    pub held: Vec<u32>,
    /// First time seen unheld — wait for the next round to confirm.
    pub pending: Vec<u32>,
    /// Could not ask who holds it (treated as held), or the roll-back failed.
    pub kept: Vec<(u32, String)>,
}

/// **The intent reconciliation round** (SM-15) — the last net for an intent left
/// on the table while the source's leader is alive (nobody changes leader, so no
/// `factory` rolls back): a coordinator that panicked before quiescing, its
/// background roll-back hitting its limit, an old binary dying midway.
///
/// Runs on the D-d runner (`gc::is_runner`), every
/// [`DEFAULT_RECONCILE_INTENTS_EVERY`] (`--reconcile-intents-every-s`).
/// Each round reloads the table; for each intent, it asks the node leading each
/// source whether its coordinator still holds it ([`RangeOps::intent_held`]). Held
/// by nobody for **two rounds in a row** with the **same** intent ⇒ roll back
/// ([`roll_back_if`]: CAS the intent away first, then clean up the children). Cannot
/// ask ⇒ treated as held.
///
/// Safety does not rest on the answer: the roll-back is a CAS on the table, the
/// counterpart of the commit CAS — wrongly rolling back a live operation only makes
/// it fail (the commit misses, it does not recreate children — SM-3), loses nothing.
/// Two rounds in a row are there to avoid rolling back because of a stale answer (the
/// routing table not caught up yet). It only **rolls back**, never completes: once
/// the commit has won the source has left the table and there is no intent left to
/// see; the post-commit part (tombstones) is the job of SM-8/SM-11.
#[derive(Debug, Default)]
pub struct IntentReconciler {
    /// Intents seen unheld in the previous round, by their lowest range.
    unheld: BTreeMap<u32, RangeIntent>,
}

impl IntentReconciler {
    pub async fn run_once(
        &mut self,
        ops: &RangeOps,
    ) -> std::result::Result<IntentRound, StoreError> {
        ops.cluster.reload().await?;
        let table = ops.cluster.table();
        let mut round = IntentRound {
            table_version: table.version(),
            ..IntentRound::default()
        };
        let mut unheld = BTreeMap::new();
        for r in table.ranges() {
            let Some(intent) = r.intent else { continue };
            // A merge carries the intent on both sources: consider it once, on the lower range.
            let sources = match intent {
                RangeIntent::Splitting { .. } => vec![r.id],
                RangeIntent::Merging { with, .. } if with.0 < r.id.0 => continue,
                RangeIntent::Merging { with, .. } => vec![r.id, with],
            };
            let held = ops.intent_held(&sources).await;
            let finder = cairn_split::Finder::Reconciler {
                held: held.as_ref().map(|h| *h).map_err(|_| cairn_split::ReadErr),
                unheld_before: self.unheld.get(&r.id.0) == Some(&intent),
            };
            match ops.found(r.id, intent, finder).await {
                cairn_split::Plan::Leave => {
                    match held {
                        Ok(_) => {
                            cairn_store::failpoint!("split::reconcile::held");
                            round.held.push(r.id.0);
                        }
                        Err(e) => {
                            tracing::debug!(range = r.id.0, error = %e, "intent reconciliation: cannot ask the coordinator — keeping");
                            round.kept.push((r.id.0, e));
                        }
                    }
                    continue;
                }
                cairn_split::Plan::Remember => {
                    unheld.insert(r.id.0, intent);
                    round.pending.push(r.id.0);
                    continue;
                }
                cairn_split::Plan::Abort { .. } => {}
                // A `Prepared` ledger confirmed by the direct check ⇒ commit CAS.
                cairn_split::Plan::RollForward => {
                    match roll_forward(&ops.cluster, &ops.stores, r.id, intent).await {
                        Ok(true) => round.rolled_forward.push(r.id.0),
                        Ok(false) => {}
                        Err(e) => {
                            unheld.insert(r.id.0, intent);
                            round.kept.push((r.id.0, format!("forward failed: {e}")));
                        }
                    }
                    continue;
                }
                other => unreachable!("core returned {other:?} for Found(Reconciler)"),
            }
            // Tripwire (AGENTS.md §6.4): about to roll back an intent nobody claimed for
            // two rounds in a row. Tests with a live coordinator arm `panic`.
            cairn_store::failpoint!("split::reconcile::roll_back");
            match roll_back_if(&ops.cluster, &ops.stores, r.id, intent, Who::Reconciler).await {
                Ok(undone) => {
                    if undone {
                        tracing::warn!(range = r.id.0, ?intent, version = table.version(), "intent reconciliation: no coordinator held the intent for two rounds — rolled back");
                        round.rolled_back.push(r.id.0);
                    }
                }
                Err(e) => {
                    unheld.insert(r.id.0, intent);
                    round.kept.push((r.id.0, format!("roll-back failed: {e}")));
                }
            }
        }
        self.unheld = unheld;
        Ok(round)
    }
}
