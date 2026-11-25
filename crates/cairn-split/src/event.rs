// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `Event`: what just happened, or the question an actor is asking.
//!
//! The durable facts are in [`crate::View`]; the asker's RAM facts (whether the
//! range has a coordinator on this node, what the previous round saw) come with
//! the event.

use crate::view::Read;

/// The result of a table CAS whose ambiguity was resolved by re-reading
/// (`cas::put_resolving`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cas {
    /// The table on the bucket is the table we meant to write.
    Applied,
    /// Someone else's table won.
    Lost,
    /// Unknown (an error re-reading could not resolve).
    Unknown,
}

/// The coordinator's next step — each step is a side effect on the bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    /// The close mark on the source Db, with an epoch condition (SM-17).
    Close,
    /// The first clone/union (creates a child Db).
    CloneFirst,
    /// The second clone of a split.
    CloneNext,
    /// Claim ownership of every child (`claim_children`).
    Claim,
    /// Asking again while gathering the other range of a merge
    /// (`gather_here`): creates nothing, only stops early when the op has
    /// already been rolled back by someone else.
    Gather,
    /// CAS commit.
    Commit,
}

/// Who is rolling back (or trying to roll back) an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Who {
    /// The coordinator, rolling back in place (`roll_back_here`).
    Coordinator,
    /// The intent CAS did not clearly win (`undo_stray_intent`).
    StrayUndo,
    /// Rolling back in the background (`settle_in_background`).
    Settle,
    /// The node claiming a source (`RangeSpawner::factory`).
    Opener,
    /// The intent reconciliation round (`IntentReconciler`).
    Reconciler,
    /// `cairn-ctl intent clear`.
    Ctl,
}

/// Someone who sees an op's intent without coordinating it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Finder {
    /// The node that just took the lease of a source. `active_here` = a
    /// coordinator on **this node** holds the op (`ActiveIntents`).
    Opener { active_here: bool },
    /// The reconciliation round: `held` = the answer of the source's leader
    /// node (`/internal/ranges/{id}/coordinator`; asking failed ⇒ `Err`, taken
    /// as held); `unheld_before` = the previous round also saw this op held by
    /// nobody.
    Reconciler {
        held: Read<bool>,
        unheld_before: bool,
    },
    /// The operator: `live_owner` = a source or child has a live owner,
    /// `force` = `--force`.
    Ctl { live_owner: bool, force: bool },
}

/// A child cleanup path of a rolled-back op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Remover {
    /// Right after the CAS dropping the intent of the roll-back itself (`discard_orphan`).
    RollBack,
    /// The coordinator, after rolling back (`discard_own_orphans`, D5).
    OwnOrphans,
    /// The orphan-child cleanup round (`discard_if_orphan_child`, SM-14).
    OrphanRound,
    /// `cairn-ctl intent clear`, after its CAS dropping the intent.
    Ctl,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    // ── the coordinator
    /// Hold the sources in `ActiveIntents`: `taken` = a source already has
    /// another coordinator on this node.
    Hold { taken: bool },
    /// The intent CAS just finished.
    IntentCas(Cas),
    /// About to enter a side-effecting step.
    Enter(Stage),
    /// The commit CAS just finished.
    CommitCas(Cas),
    /// A step failed (or the commit lost): roll back in place.
    Failed,
    /// The coordinator's task ended without returning a result (panic, cancelled).
    Vanished,
    /// A roll-back by `Who` failed (re-read or CAS error).
    RollBackFailed(Who),

    // ── non-coordinators
    /// Sees the op's intent on the table.
    Found(Finder),
    /// The roll-back round: the table just read (`View::standing`) — CAS
    /// dropping the intent, or stop.
    Clear,
    /// The CAS dropping the intent just finished.
    Cleared { cas: Cas, by: Who },

    // ── opening a Db
    /// About to open a range's Db for writing. `quiesced_here` = the source has
    /// been quiesced by a coordinator on this node (SM-13); `active_here` as
    /// above; `ram` = the range as the RAM table sees it (used only when the
    /// fresh table cannot be read — D1).
    Open {
        quiesced_here: bool,
        active_here: bool,
        ram: crate::view::Entry,
    },
    /// The open met a close mark (the mark read **before** `View::entry`).
    StaleClose,

    // ── child cleanup, tombstones, D-d
    /// A child cleanup path considers the child in `View::child`.
    Discard(Remover),
    /// Cleaning up the child Db just finished (`discard_range_db`): `ok` = the
    /// Db has no manifest left.
    Discarded { ok: bool },
    /// The `DeleteProof::abandoned_child_db` gate for the child in `View::child`.
    Gate,
    /// The range left the RAM table (`reconcile`): retire it or not?
    LeftTable,
    /// The proxy-tombstone round (SM-11) considers the range in `View::orphan`.
    Tomb,
    /// D-d plans for the candidate `View::retired`.
    ReapPlan,
    /// D-d right before deleting (the `DeleteProof::retired_db` proof).
    ReapProve,
    /// The ledger cleanup round (on the D-d tick) considers the `ops/<key>`
    /// ledger of an op (`View::ledger`; the `DeleteProof::settled_op` proof).
    ReapLedger,
}
