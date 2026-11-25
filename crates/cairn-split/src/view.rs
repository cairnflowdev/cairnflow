// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `View`: the durable facts the shell reads for one question.
//!
//! Every cell is an `Option`: `None` = the shell has not read it — the core
//! returns [`crate::Plan::Need`] when its branch needs that cell (lazy reads,
//! like `cairn-lifecycle`). A cell that was read but failed is `Err(ReadErr)`:
//! the core decides to **keep** (delete only what is provable) or retry, never
//! guesses.

use crate::op::{Op, Phase};

/// A failed durable read (I/O, decoding). The core does not need to know why.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReadErr;

pub type Read<T> = Result<T, ReadErr>;

/// How the op stands on the table (`cluster.conf`) of this read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Standing {
    /// Every source in the table carries **exactly** the op's intent.
    Pending,
    /// The sources are in the table, none carries the op's intent — it never
    /// got onto the table, or was dropped by CAS. `other` = a source carries a
    /// **different** intent (a later op's).
    Free { other: bool },
    /// The children are in the table, the sources have left: the commit CAS won.
    Committed,
    /// Neither sources nor children are in the table: the op is done (committed
    /// or rolled back) and a later op has moved the range on.
    Elsewhere,
}

/// A range as the table read from the bucket sees it (the Db opener, the
/// retirement round, self-removing the close mark).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub in_table: bool,
    /// The intent on the range (if any).
    pub intent: Option<Op>,
}

impl Entry {
    /// The range is not in the table (or there is no table on the bucket).
    pub const OUT: Entry = Entry {
        in_table: false,
        intent: None,
    };
}

/// The facts about child `c` that a child cleanup path considers (SM-11 (1)–(3), SM-14).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Child {
    pub in_table: bool,
    /// An intent of that table promises `c`.
    pub promised: bool,
    /// `c < next_range_id`.
    pub allocated: bool,
    /// `c`'s Db has a manifest.
    pub db: bool,
    /// `owners/range-<c>` exists.
    pub owner: bool,
    /// `c`'s Db is listed in the `external_dbs` of a range **in the table**
    /// (SM-14's first proof).
    pub stands_on_live: bool,
}

impl Child {
    /// Permanently out of the table (SM-1, D22): not in the table, not
    /// promised, id already allocated. All three together mean no later table
    /// brings `c` back.
    pub fn permanently_out(&self) -> bool {
        !self.in_table && !self.promised && self.allocated
    }
}

/// The "live ranges referencing it" set (`referenced_ranges_with`) a deletion uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Refs {
    /// No range lists the range in question.
    Clear,
    /// A range lists it.
    Referenced,
    /// Something cannot be proven (a broken/missing manifest, an odd prefix).
    Unknown,
}

/// The **direct** checks a Db deletion needs (SM-14 (1)–(3), the
/// `DeleteProof::abandoned_child_db` gate, D-d): nobody stands on it, no fence,
/// the table stands still. Each set carries the **table version** it was read
/// at: the core refuses a set from another version (N4, mixed snapshots)
/// instead of trusting that the shell read one snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Gate {
    /// The table version of the other facts (in the table, promised, …).
    pub table_version: u64,
    /// The table version `refs` was computed on.
    pub refs_version: u64,
    pub refs: Refs,
    /// `refs` already adds the children in flight
    /// (`referenced_ranges_with(live, promised)`, N2).
    pub refs_in_flight: bool,
    /// A GC barrier (including one that cannot be read) covers the range.
    pub barrier: bool,
    /// The table re-read right before deleting is at a version other than
    /// `table_version` (D-d: the version of the snapshot the proof was built on).
    pub moved: bool,
}

/// The owner record of a range outside the table, for the proxy-tombstone round (SM-11 (4)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Owner {
    Absent,
    Tomb,
    /// An unknown `wal_kind` from a newer binary: rewriting the record would delete its line.
    UnknownWal,
    Released,
    /// Held by a session; `dead` per exactly the rule of `Session::claim`.
    Held {
        dead: Read<bool>,
    },
}

/// A range outside the table that the proxy-tombstone round considers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Orphan {
    pub in_table: bool,
    pub promised: bool,
    pub allocated: bool,
    pub owner: Read<Owner>,
}

/// A D-d candidate (a retired Db).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Retired {
    /// In the table of the snapshot `gate` uses.
    pub in_table: bool,
    pub promised: bool,
    /// The owner record is a tombstone.
    pub tomb: bool,
    pub db: bool,
}

/// The final checkpoints (no expiry) on the manifest **of the retired Db
/// itself** that D-d considers — D-d's second fence: **positive** proof of who
/// still stands on it, read from the ancestor's side, independent of the
/// reference-set scan (the table + the live ranges' `external_dbs`). Every
/// clone/union puts on every ancestor a final checkpoint named after the child
/// Db's path (G15).
///
/// A named checkpoint whose creating range **is tombstoned** (retired — nobody
/// opens or reads it as a Db) or whose Db **has no manifest left** (deleted
/// before it could detach) is read through by nobody: the shell does not count
/// it. A **live** descendant of that range standing on the ancestor has a
/// checkpoint **of its own** (G7: clone copies every ancestor, G15: puts a
/// checkpoint on each, G6: only detach removes it).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Pins {
    /// A named final checkpoint whose creating range is not tombstoned and
    /// whose Db still has a manifest — or a name pointing outside the prefix
    /// (`cairn-ctl fork`): cannot be checked, taken as still there.
    pub named: bool,
    /// One of them was created by a **live** Db (in the table, or a child in
    /// flight, of the same table snapshot): a scan saying "nobody" means the
    /// two proofs disagree (G6: detach removes the checkpoint **before**
    /// dropping the entry).
    pub by_live: bool,
    /// An **unnamed** final checkpoint (created before the
    /// fork's checkpoint-naming patch, G15):
    /// unknown who stands on it.
    pub unnamed: bool,
}

/// What is left on the bucket of an op that the ledger cleanup round
/// considers (`ops/<key>`, `DeleteProof::settled_op`). The ledger is proof
/// only for those who still have business with the op; once every range of
/// the op has settled, an absent ledger reads as "no ledger" — the behaviour
/// from before the ledger — and no path misreads it (M2, action `LedgerReap`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Remains {
    /// The ledger has stood at an outcome for at least the minimum age: a late
    /// roller-forward or roller-back may just have read it.
    pub old: bool,
    /// An intent of the table just read mentions the op: a source carries the
    /// op's intent, or a child of the op is promised.
    pub mentioned: bool,
    /// Every source of the op is tombstoned (op `Committed`: nobody retires it any more).
    pub sources_tombed: bool,
    /// No child of the op still has a Db, and each child's owner record is
    /// absent or a tombstone (op `Cleaned`: no child cleanup path has work left).
    pub children_gone: bool,
}

/// What the shell has read for this question.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct View {
    /// How the op stands on the table just read (`ensure_intent_ours`, the roll-back round).
    pub standing: Option<Read<Standing>>,
    /// The op's `ops/<key>` ledger: `Ok(None)` = no ledger (an op of a binary
    /// from before the ledger — intent-only behaviour).
    pub ledger: Option<Read<Option<Phase>>>,
    /// The check allowing **roll forward** from `Prepared`: every child has a
    /// Db and an owner record, every source is still closed at exactly the
    /// epoch the ledger recorded.
    pub prepared: Option<Read<bool>>,
    /// The range as the table read from the bucket sees it.
    pub entry: Option<Read<Entry>>,
    pub child: Option<Child>,
    pub gate: Option<Read<Gate>>,
    pub orphan: Option<Orphan>,
    pub retired: Option<Retired>,
    /// The final checkpoints on the retired Db of `retired`.
    pub pins: Option<Read<Pins>>,
    /// What is left of the op that the ledger cleanup round considers.
    pub remains: Option<Read<Remains>>,
}
