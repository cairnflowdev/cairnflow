// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `Plan`: what the core tells the shell to do.

use crate::op::{Op, Phase};

/// A [`crate::View`] cell the core needs read before it asks again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Need {
    Standing,
    Ledger,
    Prepared,
    Entry,
    Gate,
    Pins,
    Remains,
}

/// How to remove the source's close mark after the CAS dropping the intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reopen {
    /// With the epoch condition of the mark read **before** the CAS (R6.3-1):
    /// a later op's mark is fenced off and stays.
    AtMark,
    /// Unconditionally (historical bug R6.3-1; `cairn-ctl intent clear` before the core read the
    /// ledger).
    Any,
}

/// Why keep (no cleanup, no delete, no tombstone).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Keep {
    InTable,
    Promised,
    Unallocated,
    /// The ledger says the op may have committed (`Prepared`/`Committed`): the
    /// children may have entered the table — not the business of a child
    /// cleanup path.
    MayHaveCommitted,
    /// Cannot prove the child never entered the table — left to SM-11/D-d.
    NotProven,
    Referenced,
    Barrier,
    TableMoved,
    Unreadable,
    /// The reference set of another table version, or one not adding children in flight.
    StaleRefs,
    DiscardFailed,
    OwnerLive,
    UnknownWal,
    NotTomb,
    /// The retired Db's own manifest still has a named final checkpoint whose
    /// creating Db still exists — someone may still stand on it.
    NamedCheckpoint,
    /// An unnamed final checkpoint remains (from before the
    /// checkpoint-naming patch, G15) — unknown who.
    UnnamedCheckpoint,
    /// The scan says no live range stands on it, yet a live Db still has a
    /// named final checkpoint on it — the two proofs disagree (a tripwire).
    ProofsDisagree,
    /// The ledger is not at an outcome yet, or the op still has unsettled ranges
    /// (an intent mentions the op, a source without a tombstone, a child that
    /// still has a Db or owner record), or the ledger and the table disagree.
    NotSettled,
    /// The ledger just reached an outcome and is not old enough yet.
    Young,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Plan {
    Need(Need),

    // ── the coordinator
    /// Go on; `ledger` = the phase to write to the ledger **first** (a refused
    /// write ⇒ [`crate::Event::Failed`]).
    Go {
        ledger: Option<Phase>,
    },
    /// Skip this step (only with a planted bug: no close mark).
    Skip,
    /// Roll back in place: write `ledger` (`Aborting`), then roll back
    /// **exactly** this op; then clean up the op's own children if
    /// `own_orphans` (D5), and reopen the quiesced sources.
    RollBack {
        ledger: Option<Phase>,
        own_orphans: bool,
    },
    /// The intent CAS did not clearly win: re-read the table, roll back if our
    /// intent is still there (`undo_stray_intent`).
    UndoStray,
    /// Retry the roll-back in the background (`settle_in_background`).
    Settle,
    /// Give up — leave the intent (only with planted bugs L1/L3/L4).
    GiveUp,
    /// The coordinator's task died: cut the quiesce-holding cord so the source
    /// engine yields like on `Resign`, and the source claimer decides (SM-15).
    Release,
    /// The sources are held.
    Hold,
    /// Refuse to hold; `release` = the failed guard still removes the holder's mark (planted).
    Refuse {
        release: bool,
    },

    // ── non-coordinators
    /// Do nothing (this node's coordinator still holds it, or the
    /// reconciliation round sees it for the first time, or `ctl` refuses).
    Leave,
    /// The reconciliation round: the first time it sees nobody holding —
    /// remember, decide in the next round.
    Remember,
    /// Roll the op back: write `ledger` (`Aborting`) if the op has a ledger, then the roll-back
    /// round.
    Abort {
        ledger: Option<Phase>,
    },
    /// Roll forward from `Prepared`: the commit CAS (the same table
    /// transformation as the coordinator's), then write `Committed`.
    RollForward,
    /// `ctl` refuses because an owner is still alive and there is no `--force`.
    Refused,

    // ── the roll-back round
    /// Read the source's close mark, **then** the CAS dropping the intent.
    CasClear,
    /// Clean up the children **first**, then the CAS (only with a planted bug: SM-3 reversed).
    DiscardFirst,
    /// No intent of the op left to roll back.
    Nothing,
    /// The CAS dropping the intent won: write `ledger` (`Cleaned`) if the op
    /// has a ledger, remove the close mark per `reopen`, then clean up each child.
    Cleanup {
        ledger: Option<Phase>,
        reopen: Reopen,
    },
    /// The table changed underfoot: re-read, ask [`crate::Event::Clear`] again.
    Reread,
    /// The error goes up (the CAS result is unclear).
    Fail,

    // ── opening a Db
    /// Wait for this node's coordinator to finish committing or rolling back (SM-13).
    WaitUnquiesced,
    /// Do not open; the leadership loop retries the whole open (D1: the fresh table cannot be
    /// read).
    Retry,
    /// The range is no longer in the table.
    Gone,
    /// The range carries the intent of `op` and nobody here coordinates it: ask
    /// [`crate::Event::Found`] for `op`, carry it out, then ask `Open` again.
    Resolve(Op),
    /// Open the Db.
    OpenDb,
    /// Remove the close mark with the epoch condition of the mark read.
    ReopenAtMark,
    /// Remove it unconditionally (planted bug R6.3-1).
    ReopenAny,
    /// Keep the close mark.
    KeepClosed,

    // ── child cleanup, tombstones, D-d
    /// Clean up the child Db (`discard_range_db`), then ask [`crate::Event::Discarded`].
    DiscardDb,
    /// Delete the owner record (the Db is gone).
    DeleteOwner,
    Keep(Keep),
    /// The gate: provable.
    Prove,
    /// Retire the range.
    Retire,
    /// Write a proxy tombstone (CAS on the version just read).
    Tombstone,
    /// Nothing to consider (range in the table, already tombstoned, no owner record).
    Ignore,
    /// D-d: the candidate qualifies.
    Eligible,
    /// D-d: delete.
    Delete,
    /// D-d: the table changed — abandon the whole round (only with a planted
    /// bug: the earlier shell, `Bugs::dd_abandons_round`).
    AbandonRound,
}
