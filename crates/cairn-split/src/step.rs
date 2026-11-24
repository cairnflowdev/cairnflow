// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The decision table: `step(view, event) -> Plan`.
//!
//! Merges the decisions that each actor used to write for itself:
//!
//! | Event | Who asked before the core |
//! |---|---|
//! | `Hold` | `ActiveIntents::hold` (the failed-hold bug) |
//! | `IntentCas`, `Enter`, `CommitCas`, `Failed`, `Vanished` | the coordinator: `split_with`/`split_after_intent`, `merge`/`merge_after_intent`, `ensure_intent_ours` (D5), `roll_back_here`, the supervising task of `RangeOps::start` + the quiesce-holding cord (L1) |
//! | `RollBackFailed` | `undo_stray_intent` (L3), `roll_back_here` (L4), `settle_in_background` |
//! | `Found` | `RangeSpawner::factory` (SM-3), `IntentReconciler` (SM-15), `cairn-ctl intent clear` — one roll-back/roll-forward rule |
//! | `Clear`, `Cleared` | `roll_back_matching` (SM-3: CAS first, cleanup after; R6.3-1: the mark before the CAS) |
//! | `Open`, `StaleClose` | `factory` (D1 fresh table, D2/SM-13 wait, SM-3 roll back before opening), `heal_stale_close` (SM-17) |
//! | `Discard`, `Discarded`, `Gate` | the four child cleanup paths (`discard_orphan`, `discard_own_orphans`, `discard_if_orphan_child`, `cairn-ctl intent clear`) and the `DeleteProof::abandoned_child_db` gate (SM-12, SM-14, R1-i) |
//! | `LeftTable` | `reconcile` of `follow_table` (SM-8, R6.3-6) |
//! | `Tomb` | `OrphanRetirer` (SM-11) |
//! | `ReapLedger` | the `ops/` ledger cleanup round in D-d: `gc::reap_settled_ops`, `DeleteProof::settled_op` |
//! | `ReapPlan`, `ReapProve` | D-d: `gc::plan`, `DeleteProof::retired_db` + version comparison (SM-9, mixed snapshots, version and proof rechecks), the named-checkpoint fence |
//!
//! Pure: no I/O, no clock, no allocation.

use crate::{
    bugs::{bug, Bugs},
    event::{Cas, Event, Finder, Remover, Stage, Who},
    op::Phase,
    plan::{Keep, Need, Plan, Reopen},
    view::{Gate, Refs, Standing, View},
};

/// The split/merge core: one actor's question, seen through `view`, into a
/// [`Plan`].
pub fn step(view: &View, event: &Event) -> Plan {
    decide(view, event, &Bugs::NONE)
}

/// [`step`] with historical bugs planted (M2, `tests/m2.rs::matrix`).
#[cfg(any(test, feature = "mutations"))]
pub fn step_mutated(view: &View, event: &Event, bugs: &Bugs) -> Plan {
    decide(view, event, bugs)
}

/// Reads a view cell or returns the `Need` for it.
macro_rules! read {
    ($v:expr, $field:ident, $need:expr) => {
        match $v.$field {
            Some(x) => x,
            None => return Plan::Need($need),
        }
    };
}

fn decide(v: &View, e: &Event, b: &Bugs) -> Plan {
    match *e {
        Event::Hold { taken } => {
            if taken {
                Plan::Refuse {
                    release: bug!(b.l13_failed_hold_releases),
                }
            } else {
                Plan::Hold
            }
        }
        Event::IntentCas(Cas::Applied) => Plan::Go {
            ledger: ledger(b, Phase::Intent),
        },
        Event::IntentCas(_) => Plan::UndoStray,
        Event::Enter(stage) => enter(v, stage, b),
        Event::CommitCas(Cas::Applied) => Plan::Go {
            ledger: ledger(b, Phase::Committed),
        },
        Event::CommitCas(_) | Event::Failed => roll_back_here(b),
        Event::Vanished => {
            if bug!(b.l1_dies_with_request) {
                Plan::GiveUp
            } else {
                Plan::Release
            }
        }
        Event::RollBackFailed(who) => roll_back_failed(who, b),
        Event::Found(finder) => found(v, finder, b),
        Event::Clear => clear(v, b),
        Event::Cleared { cas, by } => cleared(cas, by, b),
        Event::Open {
            quiesced_here,
            active_here,
            ram,
        } => open(v, quiesced_here, active_here, ram, b),
        Event::StaleClose => stale_close(v, b),
        Event::Discard(by) => discard(v, by, b),
        Event::Discarded { ok } => {
            if ok || bug!(b.d4_owner_before_db) {
                Plan::DeleteOwner
            } else {
                Plan::Keep(Keep::DiscardFailed)
            }
        }
        Event::Gate => gate(v, b),
        Event::LeftTable => left_table(v, b),
        Event::Tomb => tomb(v, b),
        Event::ReapPlan => reap_plan(v, b),
        Event::ReapProve => reap_prove(v, b),
        Event::ReapLedger => reap_ledger(v, b),
    }
}

/// The phase to write to the ledger before the next step — none without a ledger (before the ledger
/// existed).
fn ledger(b: &Bugs, p: Phase) -> Option<Phase> {
    if bug!(b.no_ledger) {
        None
    } else {
        Some(p)
    }
}

// ───────────────────────────────────────── the coordinator

/// The coordinator rolls back in place: `Aborting` before the CAS dropping the
/// intent; after rolling back, clean up the op's own children (D5 — a net for
/// the window between asking the table and the child-creation step).
fn roll_back_here(b: &Bugs) -> Plan {
    Plan::RollBack {
        ledger: ledger(b, Phase::Aborting),
        own_orphans: !bug!(b.d5_recreates_children),
    }
}

/// Before each child-creation step (close mark, clone/union, claiming
/// children): the intent on the **bucket** is still the op's (D5,
/// `ensure_intent_ours`). A failed read or a missing intent ⇒ roll back.
fn enter(v: &View, stage: Stage, b: &Bugs) -> Plan {
    let phase = match stage {
        Stage::Commit => {
            return Plan::Go {
                ledger: ledger(b, Phase::Prepared),
            }
        }
        Stage::Close => Some(Phase::Closed),
        Stage::CloneFirst => Some(Phase::Cloned),
        Stage::CloneNext | Stage::Claim | Stage::Gather => None,
    };
    if !bug!(b.d5_recreates_children) {
        match read!(v, standing, Need::Standing) {
            Ok(Standing::Pending) => {}
            _ => return roll_back_here(b),
        }
    }
    if stage == Stage::Close && bug!(b.before_close_mark) {
        return Plan::Skip;
    }
    Plan::Go {
        ledger: phase.and_then(|p| ledger(b, p)),
    }
}

/// A failed roll-back. If the coordinator and `undo_stray_intent` leave the
/// intent, nobody else rolls it back while the source's leader is alive (L3,
/// L4): retry in the background. The source claimer, the reconciliation round,
/// `ctl`: the error goes up — each already has its own retry loop.
fn roll_back_failed(who: Who, b: &Bugs) -> Plan {
    match who {
        Who::Coordinator if bug!(b.l4_roll_back_gives_up) => Plan::GiveUp,
        Who::StrayUndo if bug!(b.l3_stray_gives_up) => Plan::GiveUp,
        Who::Coordinator | Who::StrayUndo | Who::Settle => Plan::Settle,
        Who::Opener | Who::Reconciler | Who::Ctl => Plan::Fail,
    }
}

// ───────────────────────────────────────── non-coordinators

/// Someone who does not coordinate the op sees its intent on the table.
fn found(v: &View, finder: Finder, b: &Bugs) -> Plan {
    match finder {
        Finder::Opener { active_here: true } => Plan::Leave,
        Finder::Opener { active_here: false } => resolve(v, b),
        Finder::Reconciler { .. } if bug!(b.l1_dies_with_request) => Plan::Leave,
        Finder::Reconciler {
            held: Ok(true) | Err(_),
            ..
        } => Plan::Leave,
        Finder::Reconciler {
            held: Ok(false),
            unheld_before: false,
        } => Plan::Remember,
        Finder::Reconciler {
            held: Ok(false),
            unheld_before: true,
        } => resolve(v, b),
        Finder::Ctl {
            live_owner: true,
            force: false,
        } => Plan::Refused,
        Finder::Ctl { force: false, .. } if !bug!(b.ctl_clears_prepared) => ctl_clear(v, b),
        Finder::Ctl { .. } => Plan::Abort {
            ledger: ledger(b, Phase::Aborting),
        },
    }
}

/// `cairn-ctl intent clear` without `--force`: an op at `Prepared` whose
/// roll-forward check passes ⇒ the commit may already have won (the table was
/// rebuilt from a history version older than the commit) — rolling back would
/// put the Db of a child that has accepted writes into the trash ⇒ refuse.
/// Reading the ledger or the check fails ⇒ refuse (delete only what is
/// provable). Otherwise roll back as before.
fn ctl_clear(v: &View, b: &Bugs) -> Plan {
    let abort = Plan::Abort {
        ledger: ledger(b, Phase::Aborting),
    };
    match read!(v, ledger, Need::Ledger) {
        Ok(Some(p)) if p.may_have_committed() => {}
        Ok(_) => return abort,
        Err(_) => return Plan::Refused,
    }
    match read!(v, prepared, Need::Prepared) {
        Ok(false) => abort,
        Ok(true) | Err(_) => Plan::Refused,
    }
}

/// Roll back or roll forward: the ledger at `Prepared` (or `Committed`) **and**
/// the direct check confirming every child has a Db and an owner record and
/// every source is still closed at exactly the recorded epoch ⇒ the commit CAS;
/// otherwise roll back. Reading the ledger fails ⇒ roll back (rolling back is a
/// CAS, the counterweight of the commit CAS — always safe).
fn resolve(v: &View, b: &Bugs) -> Plan {
    let abort = Plan::Abort {
        ledger: ledger(b, Phase::Aborting),
    };
    if bug!(b.no_ledger) || bug!(b.no_roll_forward) {
        return abort;
    }
    match read!(v, ledger, Need::Ledger) {
        Ok(Some(p)) if p.may_have_committed() => {}
        _ => return abort,
    }
    match read!(v, prepared, Need::Prepared) {
        Ok(true) => Plan::RollForward,
        _ => abort,
    }
}

/// The roll-back round, on the table just read: the op still pending ⇒ the CAS
/// dropping the intent (the close mark read before the CAS); not any more ⇒
/// stop. Roll back **exactly** the op: another intent on the source belongs to
/// a later op.
fn clear(v: &View, b: &Bugs) -> Plan {
    match read!(v, standing, Need::Standing) {
        Ok(Standing::Pending) if bug!(b.sm3_discard_first) => Plan::DiscardFirst,
        Ok(Standing::Pending) => Plan::CasClear,
        Ok(_) => Plan::Nothing,
        Err(_) => Plan::Fail,
    }
}

/// The CAS dropping the intent just finished. Won ⇒ `Cleaned` (the durable
/// fact: the children never enter the table) **before** removing the close mark
/// and cleaning up the children.
fn cleared(cas: Cas, by: Who, b: &Bugs) -> Plan {
    match cas {
        Cas::Applied => Plan::Cleanup {
            ledger: ledger(b, Phase::Cleaned),
            reopen: if bug!(b.r631_reopen_any) || (by == Who::Ctl && bug!(b.ctl_reopen_any)) {
                Reopen::Any
            } else {
                Reopen::AtMark
            },
        },
        Cas::Lost => Plan::Reread,
        Cas::Unknown => Plan::Fail,
    }
}

// ───────────────────────────────────────── opening a Db

/// A node just took the lease of a range and is about to open its Db for writing.
fn open(
    v: &View,
    quiesced_here: bool,
    active_here: bool,
    ram: crate::view::Entry,
    b: &Bugs,
) -> Plan {
    // SM-13 (D2): the source is quiesced by this node's coordinator — wait for it to finish.
    if quiesced_here && !bug!(b.d2_ignores_quiesced) {
        return Plan::WaitUnquiesced;
    }
    // D1: the **fresh** table; a failed read ⇒ do not open.
    let entry = match read!(v, entry, Need::Entry) {
        Ok(e) => e,
        Err(_) if bug!(b.d1_stale_table) => ram,
        Err(_) => return Plan::Retry,
    };
    if !entry.in_table {
        return Plan::Gone;
    }
    // SM-3: an intent nobody here coordinates ⇒ the coordinator has lost the source.
    match entry.intent {
        Some(op) if !active_here => Plan::Resolve(op),
        _ => Plan::OpenDb,
    }
}

/// The open met a close mark: it is the stale mark of a roll-back that died
/// midway only when the table read **after** the mark shows the range in the
/// table without an intent (SM-17, R6.3-1).
fn stale_close(v: &View, b: &Bugs) -> Plan {
    match read!(v, entry, Need::Entry) {
        Ok(e) if e.in_table && e.intent.is_none() => {
            if bug!(b.r631_reopen_any) {
                Plan::ReopenAny
            } else {
                Plan::ReopenAtMark
            }
        }
        _ => Plan::KeepClosed,
    }
}

// ───────────────────────────────────────── child cleanup, tombstones, D-d

/// A child cleanup path considers the child `View::child` of a rolled-back op.
fn discard(v: &View, by: Remover, b: &Bugs) -> Plan {
    let Some(c) = v.child else {
        return Plan::Keep(Keep::Unreadable);
    };
    if c.in_table {
        return Plan::Keep(Keep::InTable);
    }
    if c.promised && !(by == Remover::OrphanRound && bug!(b.sm11_ignores_promised)) {
        return Plan::Keep(Keep::Promised);
    }
    if !c.allocated {
        return Plan::Keep(Keep::Unallocated);
    }
    // The roller-back just won the CAS dropping this very op's intent: the
    // child never entered the table (SM-3) — no further proof needed, the gate
    // still checks.
    let own_cas = matches!(by, Remover::RollBack | Remover::Ctl);
    // Positive proof from the ledger: `Cleaned` ⇒ the child never enters the
    // table; `Prepared`/`Committed` ⇒ it may have — not the business of a
    // child cleanup path.
    let mut proven = own_cas;
    if !own_cas && !bug!(b.no_ledger) && !bug!(b.ledger_not_proof) {
        match read!(v, ledger, Need::Ledger) {
            Ok(Some(p)) if p.may_have_committed() => return Plan::Keep(Keep::MayHaveCommitted),
            Ok(Some(Phase::Cleaned)) => proven = true,
            // No ledger, a ledger midway, a failed read: infer as before the ledger.
            _ => {}
        }
    }
    if !c.db {
        // No Db: only the owner record is left (the `abandoned_child_owner`
        // gate checks itself that the Db is gone). The round leaves it to the
        // tombstone branch (SM-11).
        return match by {
            Remover::OrphanRound => Plan::Keep(Keep::NotProven),
            _ if c.owner => Plan::DeleteOwner,
            _ => Plan::Ignore,
        };
    }
    // SM-14: the two "never entered the table" proofs, inferred from the bucket's history.
    proven = proven || c.stands_on_live || (!c.owner && !bug!(b.r1f_first_proof_only));
    if !proven {
        return Plan::Keep(Keep::NotProven);
    }
    // SM-14 (1)–(3) (R1-i): check directly what the deletion needs. The path
    // right after its own CAS relies on the gate (the same check, `Event::Gate`).
    if !own_cas && !bug!(b.r1i_trusts_history) {
        let g = match read!(v, gate, Need::Gate) {
            Ok(g) => g,
            Err(_) => return Plan::Keep(Keep::Unreadable),
        };
        if let Some(k) = gate_blocks(&g, false, bug!(b.n4_any_refs_version), b) {
            return Plan::Keep(k);
        }
    }
    Plan::DiscardDb
}

/// The `DeleteProof::abandoned_child_db` gate: the child is permanently out of
/// the table **and** no live range or child in flight stands on it, no fence,
/// per version; **and** the ledger of the op that created it does not say it
/// may have entered the table (`Prepared`/`Committed`) — the opposite proof from
/// the ledger, read for every child cleanup path.
fn gate(v: &View, b: &Bugs) -> Plan {
    let Some(c) = v.child else {
        return Plan::Keep(Keep::Unreadable);
    };
    if !c.permanently_out() {
        return Plan::Keep(if c.in_table {
            Keep::InTable
        } else if c.promised {
            Keep::Promised
        } else {
            Keep::Unallocated
        });
    }
    if !bug!(b.no_ledger) && !bug!(b.ledger_not_proof) {
        match read!(v, ledger, Need::Ledger) {
            Ok(Some(p)) if p.may_have_committed() => return Plan::Keep(Keep::MayHaveCommitted),
            Ok(_) => {}
            Err(_) => return Plan::Keep(Keep::Unreadable),
        }
    }
    if bug!(b.r1i_trusts_history) {
        return Plan::Prove;
    }
    let g = match read!(v, gate, Need::Gate) {
        Ok(g) => g,
        Err(_) => return Plan::Keep(Keep::Unreadable),
    };
    match gate_blocks(&g, true, false, b) {
        Some(k) => Plan::Keep(k),
        None => Plan::Prove,
    }
}

/// The check common to every Db deletion: a reference set of the same version
/// (adding children in flight when `in_flight`), nobody standing on it, no
/// fence, the table standing still.
fn gate_blocks(g: &Gate, in_flight: bool, any_version: bool, b: &Bugs) -> Option<Keep> {
    if g.refs_version != g.table_version && !any_version {
        return Some(Keep::StaleRefs);
    }
    if in_flight && !g.refs_in_flight && !bug!(b.dd_ignores_in_flight) {
        return Some(Keep::StaleRefs);
    }
    match g.refs {
        Refs::Clear => {}
        Refs::Unknown if bug!(b.unknown_refs_as_clear) => {}
        Refs::Referenced | Refs::Unknown => return Some(Keep::Referenced),
    }
    if g.barrier {
        return Some(Keep::Barrier);
    }
    if g.moved {
        return Some(Keep::TableMoved);
    }
    None
}

/// A range left the RAM table: retire it only when the table **read from the
/// bucket** no longer has it either (SM-8, R6.3-6) — the RAM table may have
/// been learned from a peer's `409` body.
fn left_table(v: &View, b: &Bugs) -> Plan {
    if bug!(b.r636_retire_on_ram) {
        return Plan::Retire;
    }
    match read!(v, entry, Need::Entry) {
        Ok(e) if !e.in_table => Plan::Retire,
        Ok(_) => Plan::Keep(Keep::InTable),
        Err(_) => Plan::Keep(Keep::Unreadable),
    }
}

/// SM-11: write a proxy tombstone for a range outside the table if and only if
/// (1) it is not in the table, (2) not promised, (3) its id is allocated, (4)
/// its owner has yielded or died per the rules of `Session::claim`. Every
/// ambiguity means keep.
fn tomb(v: &View, b: &Bugs) -> Plan {
    if bug!(b.sm11_no_proxy_tomb) {
        return Plan::Ignore;
    }
    let Some(o) = v.orphan else {
        return Plan::Keep(Keep::Unreadable);
    };
    if o.in_table {
        return Plan::Ignore;
    }
    if !o.allocated {
        return Plan::Keep(Keep::Unallocated);
    }
    if o.promised && !bug!(b.sm11_ignores_promised) {
        return Plan::Keep(Keep::Promised);
    }
    use crate::view::Owner;
    match o.owner {
        Err(_) => Plan::Keep(Keep::Unreadable),
        Ok(Owner::Absent | Owner::Tomb) => Plan::Ignore,
        Ok(Owner::UnknownWal) => Plan::Keep(Keep::UnknownWal),
        Ok(Owner::Released) | Ok(Owner::Held { dead: Ok(true) }) => Plan::Tombstone,
        Ok(Owner::Held { dead: Ok(false) }) => Plan::Keep(Keep::OwnerLive),
        Ok(Owner::Held { dead: Err(_) }) => Plan::Keep(Keep::Unreadable),
    }
}

/// D-d, planning: a retired Db (tombstone, has a Db) outside the table and not
/// promised in the **same** snapshot that the reference set (adding children in
/// flight) uses.
fn reap_plan(v: &View, b: &Bugs) -> Plan {
    let Some(r) = v.retired else {
        return Plan::Keep(Keep::Unreadable);
    };
    if !r.tomb || !r.db {
        return Plan::Keep(Keep::NotTomb);
    }
    if r.in_table {
        return Plan::Keep(Keep::InTable);
    }
    if r.promised {
        return Plan::Keep(Keep::Promised);
    }
    let g = match read!(v, gate, Need::Gate) {
        Ok(g) => g,
        Err(_) => return Plan::Keep(Keep::Unreadable),
    };
    match gate_blocks(
        &Gate { moved: false, ..g },
        true,
        bug!(b.dd_mixed_snapshots),
        b,
    ) {
        Some(k) => Plan::Keep(k),
        None => checkpoint_fence(v, b, Plan::Eligible),
    }
}

/// D-d, right before deleting: the proof is rebuilt on the reloaded table; a
/// table that changed since the snapshot the gate's facts were read on ⇒ keep
/// **just this range** — the shell rebuilds the proof on the new snapshot, and
/// the round goes on with the other ranges. That snapshot is the proof's own;
/// before the proof recheck (no rebuild) it was the plan's. The earlier shell
/// compared with the plan even after rebuilding, and a changed table ⇒
/// abandoned the whole round: under continuous auto-splitting D-d was starved.
fn reap_prove(v: &View, b: &Bugs) -> Plan {
    let Some(r) = v.retired else {
        return Plan::Keep(Keep::Unreadable);
    };
    let g = match read!(v, gate, Need::Gate) {
        Ok(g) => g,
        Err(_) => return Plan::Keep(Keep::Unreadable),
    };
    if g.moved && !bug!(b.dd_no_version_recheck) {
        return if bug!(b.dd_abandons_round) {
            Plan::AbandonRound
        } else {
            Plan::Keep(Keep::TableMoved)
        };
    }
    if bug!(b.dd_no_proof_recheck) {
        // Before the proof recheck: only the fence is checked again.
        return if g.barrier {
            Plan::Keep(Keep::Barrier)
        } else {
            Plan::Delete
        };
    }
    if r.in_table {
        return Plan::Keep(Keep::InTable);
    }
    if r.promised {
        return Plan::Keep(Keep::Promised);
    }
    if !r.tomb {
        return Plan::Keep(Keep::NotTomb);
    }
    match gate_blocks(&Gate { moved: false, ..g }, true, false, b) {
        Some(k) => Plan::Keep(k),
        None => checkpoint_fence(v, b, Plan::Delete),
    }
}

/// The ledger cleanup round: the `ops/<key>` ledger of a **settled** op is
/// deleted (through the trash). Settled means: the ledger is at an outcome and
/// old enough; no intent of the table mentions the op; `Committed` ⇒ the table
/// says the op committed (or the range moved on) and every source is
/// tombstoned; `Cleaned` ⇒ the table says the intent is gone (or the range moved
/// on) and every child has no Db left, its owner record absent or a tombstone.
/// The ledger and the table disagree, or a read failed ⇒ keep. No ledger
/// (`Ok(None)`) ⇒ nothing to consider.
fn reap_ledger(v: &View, b: &Bugs) -> Plan {
    if bug!(b.ledger_kept_forever) {
        return Plan::Keep(Keep::NotSettled);
    }
    let phase = match read!(v, ledger, Need::Ledger) {
        Ok(Some(p)) => p,
        Ok(None) => return Plan::Ignore,
        Err(_) => return Plan::Keep(Keep::Unreadable),
    };
    if !matches!(phase, Phase::Committed | Phase::Cleaned) {
        return Plan::Keep(Keep::NotSettled);
    }
    match (phase, read!(v, standing, Need::Standing)) {
        (_, Err(_)) => return Plan::Keep(Keep::Unreadable),
        (Phase::Committed, Ok(Standing::Committed | Standing::Elsewhere))
        | (Phase::Cleaned, Ok(Standing::Free { .. } | Standing::Elsewhere)) => {}
        _ => return Plan::Keep(Keep::NotSettled),
    }
    let r = match read!(v, remains, Need::Remains) {
        Ok(r) => r,
        Err(_) => return Plan::Keep(Keep::Unreadable),
    };
    if r.mentioned {
        return Plan::Keep(Keep::NotSettled);
    }
    if !r.old {
        return Plan::Keep(Keep::Young);
    }
    let settled = match phase {
        Phase::Committed => r.sources_tombed,
        _ => r.children_gone,
    };
    if !settled && !bug!(b.ledger_reaps_unsettled) {
        return Plan::Keep(Keep::NotSettled);
    }
    Plan::Prove
}

/// D-d's second fence: the scan says nobody stands on the retired Db; its
/// **own** manifest must also have no final checkpoint of a Db that still
/// exists. AND, not instead: each side has its own read path (the table + the
/// live ranges' `external_dbs`; the checkpoints on the Db itself + the manifest
/// of the Db that created each checkpoint). A disagreement — a **live** Db
/// still has a named checkpoint while the scan says nobody — is a tripwire; an
/// unnamed checkpoint is unknown; a failed read means keep (delete only what is
/// provable).
fn checkpoint_fence(v: &View, b: &Bugs, pass: Plan) -> Plan {
    if bug!(b.dd_no_checkpoint_fence) {
        return pass;
    }
    let p = match read!(v, pins, Need::Pins) {
        Ok(p) => p,
        Err(_) => return Plan::Keep(Keep::Unreadable),
    };
    if p.by_live {
        Plan::Keep(Keep::ProofsDisagree)
    } else if p.named {
        Plan::Keep(Keep::NamedCheckpoint)
    } else if p.unnamed && !bug!(b.dd_unnamed_as_clear) {
        Plan::Keep(Keep::UnnamedCheckpoint)
    } else {
        pass
    }
}
