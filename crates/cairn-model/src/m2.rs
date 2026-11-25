// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! # M2 — split/merge, GC, fork on the split/merge core **itself**
//!
//! *The model is a hypothesis about the code, like a guide: it drifts. Every
//! **decision** of M2 goes through [`cairn_split::step_mutated`] — the
//! coordinator, six roll-backers, four child-cleanup paths, three ways of
//! removing the closed mark, the Db opener, the proxy-tombstone round, D-d. The
//! part the model writes itself is the **environment**: the bucket (table, Dbs,
//! owner records, op ledger), the SlateDB graph (`external_dbs`, final
//! checkpoints, detach, closed mark and `writer_epoch`), leases, node deaths,
//! injected failures. Changing the environment or the shell's event order means
//! fixing it here in the same commit (AGENTS.md §2).*
//!
//! One retired ancestor `Q` (range 0), one or two live source ranges standing
//! on it, and **one** split (`1 → {2}`) or merge (`{1, 2} → {3}`) — plus a
//! **second** operation on the same sources after the first rolls back
//! ([`M2::second`]; child `3` or `4`), for the bugs that need a later op (marks
//! cleared from a stale read, an orphan Db after two kills). Two nodes: `C` (the
//! coordinator, which can die once and be rebuilt) and `X` (runner of D-d, the
//! reconcile loop, the proxy-tombstone round; its RAM table may be stale).
//!
//! [`M2::deep`]: a lineage two deep — `Q` itself stands on another retired
//! ancestor `G` ([`GRANDPARENT`], range 5), so the sources stand on `{Q, G}` and
//! the op's child stands on `{source, Q, G}` (SlateDB copies every
//! `external_dbs` entry of the source, each with its own named final
//! checkpoint). Detach removes each entry independently; D-d considers `G` too.
//!
//! ## State ↔ real objects
//!
//! | Model | Real |
//! |---|---|
//! | [`State::table`] ([`Table`]) | `cluster.conf` (`cairn-store/src/cluster.rs`, `partition_table.rs`): intent, commit, version |
//! | [`OpState::ledger`] | the op ledger `ops/<first child>`, transition rule `cairn_split::Phase::advance` |
//! | [`Db`] | Db `ranges/<id>`: manifest, `external_dbs`, final checkpoints by the Db that created them (`RangeCheckpoint::created_by`) — named or not (`Db::unnamed`, G15: the name is the child Db's path since the fork's checkpoint-naming patch), the `closed` flag + `writer_epoch` (SM-17), an open writer |
//! | [`State::pins`] | the manifest each Db's final checkpoint pins on the source (G6) |
//! | [`State::owners`] ([`Owner`]) | `owners/range-<id>` |
//! | [`State::lease`] | the range's lease (the node holding it) |
//! | [`State::x_ram`] | node `X`'s RAM table (learned from a `409` body, may be stale) |
//! | [`OpState::pc`] ([`Pc`]) | the coordinator (`cairn-server/src/split/`, `RangeOps`) |
//! | [`OpState::held`], [`OpState::quiesced`] | the coordinating node's `ActiveIntents`: held, quiesced (SM-13) |
//! | [`State::rbs`] ([`RollBacker`]) | a running roll-back: the coordinator's (`roll_back_here`, `undo_stray_intent`, `settle_in_background`) or someone else's (`factory`, `IntentReconciler`, `cairn-ctl intent clear`) |
//! | [`State::gc`] ([`Gc`]) | an unfinished D-d round (`cairn-server/src/gc.rs:plan`/`run_once`) |
//! | [`State::live_refs`], [`State::orphan`] | `gc::LiveRefs`, a candidate of `discard_if_orphan_child` that has been checked |
//! | [`State::fork`] ([`Fork`]) | the `cairn-ctl fork` copy and its barrier `gc-barriers/fork-<prefix>` |
//! | [`Ghost`] | ghost variables to state invariants — not in the code |
//!
//! ## Actions ↔ core events ↔ real functions
//!
//! | Action | Core `Event` | `file:function` |
//! |---|---|---|
//! | [`Action::IntentCas`] | `Hold`, `IntentCas` | `split/ops.rs:split_with`, `split/merge.rs:merge` |
//! | [`Action::Stray`] | `RollBackFailed(StrayUndo)` | `split/steps.rs:undo_stray_intent` |
//! | [`Action::Quiesce`] | — | `split/ops.rs:split_after_intent`: `ActiveGuard::mark_quiesced` + `quiesce_held` |
//! | [`Action::Close`] | `Enter(Close)` | `ensure_intent_ours` + `ObjectStores::close_range_db_at` |
//! | [`Action::Clone`] | `Enter(CloneFirst)` | `ensure_intent_ours` + `cairn-store/src/config/range_db.rs:clone_range`/`union_ranges` |
//! | [`Action::Claim`] | `Enter(Claim)` | `ensure_intent_ours` + `split/steps.rs:claim_children` |
//! | [`Action::Prepare`], [`Action::CommitCas`] | `Enter(Commit)`, `CommitCas` | `ClusterConfig::update(marked_version, …)` |
//! | [`Action::Retire`] | — | `EngineHandle::retire` (tombstone) |
//! | [`Action::Fail`], [`Action::Vanish`], [`Action::Crash`] | `Failed`, `Vanished` | a failed step, a panicking task, a dead node |
//! | [`Action::RbClear`] | `Clear`, `Cleared`, `RollBackFailed` | `split/rollback.rs:roll_back_matching` (marks, then CAS) |
//! | [`Action::RbCleanup`] | `Discard`, `Gate`, `Discarded` | `reopen_sources`, `discard_orphan`, `discard_own_orphans`, `discard_abandoned_child` |
//! | [`Action::Settle`] | — | `split/steps.rs:settle_in_background` |
//! | [`Action::Open`] | `Open`, `Found(Opener)`, `StaleClose` | `node/spawner.rs:factory`, `heal_stale_close`; rolling forward: `split/forward.rs:roll_forward` (check `split/forward.rs:prepared`) |
//! | [`Action::Reconcile`] | `Found(Reconciler)` | `split/reconcile.rs:run_once` (rolls forward as above) |
//! | [`Action::Ctl`] | `Found(Ctl)` | `ctl.rs:intent_clear` |
//! | [`Action::LeftTable`] | `LeftTable` | `node/spawner.rs:reconcile` |
//! | [`Action::SecondHold`] | `Hold` | `ActiveIntents::hold` |
//! | [`Action::OrphanCheck`], [`Action::OrphanDiscard`] | `Discard(OrphanRound)`, `Tomb`, `Gate` | `gc.rs:OrphanRetirer::run_once`, `discard_if_orphan_child`. **When** the round runs (the nudges — `gc.rs:intent_settled`, `SessionTracker::ended`, a runner change — plus an hourly safety net, `node/setup.rs:spawn_gc_loops`) is not in the model: `OrphanCheck` is enabled in every state, so every schedule of the shell is a subset. The round does not LIST the directories of ranges in the RAM table (`range_dirs_where`): the candidate set is exactly as before |
//! | [`Action::GcLoadTable`], [`Action::GcPlan`], [`Action::GcDelete`] | `ReapPlan`, `ReapProve` (the checkpoint fence: slot `pins`) | `gc.rs:plan`, `gc.rs:run_once`, `proof.rs:DeleteProof::retired_db`, `config/range_db.rs:checkpoint_pins`; the reference set once per table version per round (`proof.rs:DeleteProof::retired_db_with` + `RoundRefs`, slot `refs` of [`Gc::Planned`]). The swept-tombstone ledger (`cairn_store::gc_swept`) only skips ranges whose Db an earlier D-d round already saw **absent**; in the model a deleted Db never reappears, so the candidate set is exactly as before — no flag, no new slot |
//! | [`Action::LedgerReap`] | `ReapLedger` | `gc.rs:reap_settled_ops`, `proof.rs:DeleteProof::settled_op`, `ledger.rs:delete_op` (an absent ledger afterwards = "no ledger", the behaviour before ledgers existed) |
//! | [`Action::Detach`] | — | compaction + SlateDB's GC `detach` in the process that has the Db open (`find_detachable`) |
//! | [`Action::Fork`], [`Action::ForkExpires`] | — | `ctl.rs:Ctl::fork`, `gc.rs:reap_expired_barriers` |
//! | [`Action::Ack`], [`Action::LeaseLoss`], [`Action::XLearn`] | — | a write is acked, the engine loses its lease, `X` learns the table |
//!
//! ## The parts of the environment still behind flags ([`Env`], anchored in [`ANCHORS`])
//!
//! - `fork_barrier` — `fork` places a barrier on the source cluster (`ctl.rs`).
//!   Off = the historical bug where the source cluster's GC did not know the
//!   fork stood on it.
//! - `keep_db_until_checkpoints_gone` — SM-12: when `discard_range_db` fails to
//!   read the lineage / remove checkpoints it returns an error and keeps the Db.
//!   Off = the historical half-deleted Db.
//! - `detach_respects_checkpoints` — G6: detach does not drop an entry a live
//!   checkpoint still pins.
//! - `dd_scan_sees` — D-d's reference-set scan (`referenced_ranges_with` on the
//!   loaded table) sees everything. Off = a **blind** scan (it says "nobody" for
//!   every Db — as if the table or `external_dbs` were read short; failpoint
//!   `gc::skip_reference_check`): the named-checkpoint fence still holds.
//!
//! [`M2::unnamed_history`]: a bucket with final checkpoints created before the
//! fork's checkpoint-naming patch (G15) — the initial sources' checkpoints on the ancestor
//! are unnamed.
//!
//! Every other fence is a **core decision**; it is switched off with a
//! `cairn_split::Bugs` flag (matrix `tests/m2.rs::matrix`). The shell calls the
//! core itself (`Bugs::PRODUCTION` = `Bugs::NONE`); the shell's read/write order
//! that M2 models (ledger before side effects, closed marks before the CAS
//! dropping the intent, the ledger-reading gate, cleanup leaving
//! `MayHaveCommitted` to the tombstone branch) must match
//! `cairn-server/src/split/`, `gc.rs:discard_if_orphan_child`,
//! `proof.rs:DeleteProof::abandoned_child_db`.
//!
//! ## Invariants
//!
//! Safety (every state):
//! - [`NO_DELETE_UNDER_LIVE`]: no Db is deleted while a live Db (in the table, an
//!   in-flight child, an unexpired fork) still lists it (SM-9, SM-14; also the
//!   fork without a barrier and deletion decided on a stale RAM table).
//! - [`NO_COMMIT_OF_DISCARDED_CHILD`]: no child commits whose Db or owner record
//!   has been cleaned up (SM-3, SM-11 (2)).
//! - [`NO_LOST_ACK`]: no write is acked into a source after the coordinator
//!   quiesced it while the op still commits, nor into a range that has left the
//!   table (a retired parent opened from a stale table, a quiescing source opened
//!   unaware, SM-13, SM-17, closed marks cleared from a stale read).
//! - [`SM14_OWNER_OUTLIVES_DB`]: the owner record of a range that was ever in the
//!   table lives as long as its Db (does not apply to a `legacy` bucket).
//! - [`NO_ORPHAN_CHECKPOINT_ON_LIVE`]: a final checkpoint on a Db in the table
//!   always still has the Db that created it.
//! - [`D5_NO_STRANDED_OWNER`]: once a live coordinator has finished rolling back,
//!   no owner record of its children points at it while the child has no Db.
//! - [`NO_SELF_ROLL_BACK`]: the coordinating node never rolls back an op its own
//!   coordinator still holds.
//! - [`NO_TOMB_ON_LIVE`]: no range in the table carries a tombstone.
//!
//! Liveness ([`stuck`] — AG EF over **system actions**, without counting on
//! injected failures or node deaths to rescue it): from every reachable state,
//! the system can bring itself to a [`settled`] state — no intent on the table,
//! the children of every op have rolled back their Db and owner record, the
//! sources of a committed op carry tombstones, the ledgers of every op are
//! cleaned up (stuck intents, an orphan Db after two kills, SM-11, ledgers that
//! leaked forever before ledger cleanup existed).
//!
//! D-d's liveness under table changes is the `sometimes` property
//! [`SOMETIMES_REAPED_AFTER_MOVE`]: a round whose table changed version since
//! its plan can still delete. Not AG EF: the number of table changes here is
//! finite, so a shell that "abandons the whole round" (flag
//! `dd_abandons_round`) still settles in a later round; what breaks is progress
//! when the table changes **every round** (continuous auto split) — the
//! property becomes unreachable.
//!
//! ## Deliberately left out of the model
//!
//! - Sessions and leases in detail (M1): a lease is a token on each range; a
//!   dead node's tokens are free; a `Leader` owner may be treated as dead at any
//!   time (SM-11 (4)).
//! - The trash (SM-16): deletion here is real deletion.
//! - One child represents each split (the second child is symmetric).
//! - Union dropping sources without SSTs, `source_checkpoint_id`, temporary
//!   checkpoints, SlateDB's internal SST GC; detach is atomic.
//! - `min_age`, clocks, two concurrent D-d runners, barrier renewal; `fork` is atomic.
//! - Ambiguous CAS (rule 7 of AGENTS.md) apart from the intent CAS: every other
//!   CAS is atomic with a clear result.
//! - A failed query to the coordinator (the reconcile loop treats it as holding — only a delay).

use {
    cairn_split::{
        self as core, Advance, Bugs, Cas, Child, Entry, Event, Finder, Gate, Keep, Op, Orphan,
        Phase, Plan, Refs, Remover, Reopen, Retired, Stage, Standing, View, Who,
    },
    stateright::{Model, Property},
};

pub type RangeId = u8;
/// A set of ranges, bit `r` = range `r`.
pub type Mask = u8;

fn bit(r: RangeId) -> Mask {
    1 << r
}

fn has(m: Mask, r: RangeId) -> bool {
    m & bit(r) != 0
}

fn ids(m: Mask) -> impl Iterator<Item = RangeId> {
    (0..8).filter(move |&r| has(m, r))
}

/// The retired ancestor every source stands on at first.
pub const ANCESTOR: RangeId = 0;
/// The second retired ancestor ([`M2::deep`]): [`ANCESTOR`] was split off it,
/// so the sources stand on both.
pub const GRANDPARENT: RangeId = 5;
/// Number of range slots in the state (ranges 0–4, plus [`GRANDPARENT`]).
const SLOTS: usize = 6;
/// Bit of the `fork` copy in the "Dbs that created checkpoints" set — the copy
/// is not a range of the cluster.
pub const FORK: RangeId = 7;
/// The coordinating node and the other node.
pub const C: u8 = 0;
pub const X: u8 = 1;

pub const NO_DELETE_UNDER_LIVE: &str =
    "no Db deleted while a live Db lists it (SM-9, SM-14; D6, R1-i)";
pub const NO_COMMIT_OF_DISCARDED_CHILD: &str =
    "no child commits after its Db or owner record was discarded (SM-3, SM-11)";
pub const NO_LOST_ACK: &str =
    "no ack lands in a quiesced source of a committing op or in a retired range (D1, D2, SM-17)";
pub const SM14_OWNER_OUTLIVES_DB: &str =
    "SM-14: the owner record of a range that was ever in the table outlives its Db";
pub const NO_ORPHAN_CHECKPOINT_ON_LIVE: &str =
    "no checkpoint on a live Db whose creator Db is gone (SM-12; D4)";
pub const D5_NO_STRANDED_OWNER: &str =
    "no owner record of a Db-less child points at a live coordinator that rolled back (D5)";
pub const NO_SELF_ROLL_BACK: &str =
    "the coordinating node never rolls back an op its live coordinator holds";
pub const NO_TOMB_ON_LIVE: &str = "no range in the table carries a tombstone (R6.3-6)";
pub const SOMETIMES_PARENT_REAPED: &str = "sometimes: split commits, then the parent Db is reaped";
pub const SOMETIMES_ANCESTOR_REAPED: &str = "sometimes: the retired ancestor Db is reaped";
/// [`M2::deep`]: the deepest ancestor is deleted by D-d too (the child stands on
/// its own after the split).
pub const SOMETIMES_GRANDPARENT_REAPED: &str =
    "sometimes: the deepest retired ancestor Db is reaped";
pub const SOMETIMES_ROLLED_BACK_CLEAN: &str =
    "sometimes: roll back leaves no child Db, owner record or checkpoint";
pub const SOMETIMES_ORPHAN_ROUND_DISCARDS: &str = "sometimes: the orphan round discards a Db";
pub const SOMETIMES_ROLLED_FORWARD: &str = "sometimes: a prepared op is rolled forward";
/// D-d's liveness under table changes: a round whose table changed version after
/// its plan can still delete. Abandoning the whole round when the table changes
/// (the earlier behaviour) makes no such state reachable — under continuous auto
/// split D-d never deletes anything (as a 4 h soak found).
pub const SOMETIMES_REAPED_AFTER_MOVE: &str =
    "sometimes: a D-d round reaps a Db after the table moved since its plan";
/// D-d reads the reference set once per table version per round: a delete proof
/// can be built from the round's set, without reading again.
pub const SOMETIMES_REAPED_ON_ROUND_REFS: &str =
    "sometimes: a D-d proof reaps a Db with the round's reference set";

/// Fences and assumptions of the **environment** (not part of the core).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Env {
    pub fork_barrier: bool,
    pub keep_db_until_checkpoints_gone: bool,
    pub detach_respects_checkpoints: bool,
    pub dd_scan_sees: bool,
}

impl Env {
    pub const ALL: Env = Env {
        fork_barrier: true,
        keep_db_until_checkpoints_gone: true,
        detach_respects_checkpoints: true,
        dd_scan_sees: true,
    };
}

/// The anchor of each [`Env`] flag: (flag, file from the repo root, string that
/// must be in the file) — `tests/m2.rs::anchors_exist_in_the_source`. Core
/// fences need no anchor: M2 calls the core itself.
pub const ANCHORS: &[(&str, &str, &str)] = &[
    (
        "fork_barrier",
        "crates/cairn-server/src/ctl.rs",
        "gc_barrier::put_barrier(",
    ),
    (
        "keep_db_until_checkpoints_gone",
        "crates/cairn-store/src/config/range_db.rs",
        "pub async fn discard_range_db(",
    ),
    (
        "detach_respects_checkpoints",
        "crates/cairn-store/tests/range_clone.rs",
        "fn missing_checkpoint_of_an_entry_without_parent_ssts_is_a_pending_detach(",
    ),
    (
        "dd_scan_sees",
        "crates/cairn-store/src/config/range_db.rs",
        "pub async fn referenced_ranges_with(",
    ),
];

/// The shape of the operation: split `1 → {2}` or merge `{1, 2} → {3}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    Split,
    Merge,
}

/// Injected failures and environment events that are enabled (to keep the
/// state space small for each question).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Faults {
    /// Node `C` dies once (and can be rebuilt).
    pub crash: bool,
    /// The engine of one source loses its lease once (`X` claims it).
    pub lease_loss: bool,
    /// A fresh table read fails when opening a Db.
    pub stale_reads: bool,
    /// The coordinator's task dies (panic, a client disconnect in older versions).
    pub vanish: bool,
    /// The intent CAS has an unclear result; a roll-back fails.
    pub ambiguous: bool,
    /// Cleaning up a child Db fails / the roll-backer dies half-way.
    pub discard: bool,
    /// A write is acked into a source (for [`NO_LOST_ACK`]).
    pub acks: bool,
    /// The RAM table learned from a peer says a source has left; a second `hold`.
    pub ram: bool,
}

impl Faults {
    pub const ALL: Faults = Faults {
        crash: true,
        lease_loss: true,
        stale_reads: true,
        vanish: true,
        ambiguous: true,
        discard: true,
        acks: true,
        ram: true,
    };
    pub const NONE: Faults = Faults {
        crash: false,
        lease_loss: false,
        stale_reads: false,
        vanish: false,
        ambiguous: false,
        discard: false,
        acks: false,
        ram: false,
    };
}

/// Model configuration.
#[derive(Clone, Debug)]
pub struct M2 {
    pub shape: Shape,
    /// A bucket a single-process node built **before the range-tier migration
    /// framework**: no range has an owner record, operations write no owner
    /// record or tombstone.
    pub legacy: bool,
    /// The initial sources' final checkpoints on the ancestor are **unnamed**
    /// (a bucket created before the checkpoint-naming patch, G15); new
    /// checkpoints (clone, union, fork) are named.
    pub unnamed_history: bool,
    /// There is one `cairn-ctl fork`.
    pub fork: bool,
    /// A second op on the same sources after the first one rolls back.
    pub second: bool,
    /// The operator runs `cairn-ctl intent clear`.
    pub ctl: bool,
    /// D-d and `detach` run (off to shrink the space for questions that do not touch GC).
    pub gc: bool,
    /// A lineage two deep: [`ANCESTOR`] stands on [`GRANDPARENT`].
    pub deep: bool,
    pub faults: Faults,
    pub bugs: Bugs,
    pub env: Env,
}

impl M2 {
    pub fn split(bugs: Bugs) -> Self {
        M2 {
            shape: Shape::Split,
            legacy: false,
            unnamed_history: false,
            fork: true,
            second: false,
            ctl: false,
            gc: true,
            deep: false,
            faults: Faults::ALL,
            bugs,
            env: Env::ALL,
        }
    }

    pub fn merge(bugs: Bugs) -> Self {
        M2 {
            shape: Shape::Merge,
            ..Self::split(bugs)
        }
    }

    pub fn sources(&self) -> &'static [RangeId] {
        match self.shape {
            Shape::Split => &[1],
            Shape::Merge => &[1, 2],
        }
    }

    /// The (representative) child of op `k`.
    pub fn child(&self, k: usize) -> RangeId {
        match self.shape {
            Shape::Split => 2 + k as RangeId,
            Shape::Merge => 3 + k as RangeId,
        }
    }

    fn ops(&self) -> usize {
        if self.second {
            2
        } else {
            1
        }
    }

    fn range_count(&self) -> usize {
        1 + self.sources().len() + self.ops()
    }

    /// Every range of the model: ancestor, sources, children — plus
    /// [`GRANDPARENT`] with [`M2::deep`].
    fn ranges(&self) -> impl Iterator<Item = RangeId> {
        let deep = self.deep.then_some(GRANDPARENT);
        (0..self.range_count() as RangeId).chain(deep)
    }

    /// The initial retired ancestor: nobody opens it, nobody splits it.
    fn ancestors(&self) -> Mask {
        bit(ANCESTOR) | if self.deep { bit(GRANDPARENT) } else { 0 }
    }

    fn mask(rs: &[RangeId]) -> Mask {
        rs.iter().fold(0, |m, &r| m | bit(r))
    }

    fn src(&self) -> Mask {
        Self::mask(self.sources())
    }

    /// The core's op for op `k` (only to recover `k` through `Op::key`).
    fn core_op(&self, k: usize) -> Op {
        let c = self.child(k) as u32;
        match self.shape {
            Shape::Split => Op::Split {
                source: 1,
                left: c,
                right: c + 100,
            },
            Shape::Merge => Op::Merge {
                lo: 1,
                hi: 2,
                into: c,
            },
        }
    }

    fn op_of_child(&self, r: RangeId) -> Option<usize> {
        (0..self.ops()).find(|&k| self.child(k) == r)
    }

    /// Ranges in table `t`.
    pub fn in_table(&self, t: Table) -> Mask {
        match t.committed {
            Some(k) => bit(self.child(k as usize)),
            None => self.src(),
        }
    }

    /// Children the intent of `t` promises (`PartitionTable::promised`).
    pub fn promised(&self, t: Table) -> Mask {
        t.intent.map_or(0, |k| bit(self.child(k as usize)))
    }
}

/// `cluster.conf`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Table {
    pub version: u8,
    /// The op carrying an intent on the sources.
    pub intent: Option<u8>,
    /// The op has committed (the sources left the table).
    pub committed: Option<u8>,
}

/// Db `ranges/<id>`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Db {
    /// Has a manifest.
    pub exists: bool,
    /// `external_dbs`, by range id.
    pub external: Mask,
    /// Final checkpoints on this Db, by the id of the Db that created them.
    pub checkpoints: Mask,
    /// The unnamed part of `checkpoints` (before the checkpoint-naming patch,
    /// G15): a manifest reader
    /// cannot tell which Db created them.
    pub unnamed: Mask,
    /// The `closed` flag (SM-17).
    pub closed: bool,
    pub epoch: u8,
    /// A writer is open (acks, compaction, GC `detach` can run).
    pub writer: bool,
}

/// `owners/range-<id>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Owner {
    None,
    /// The session of a leader — can die (or yield) at any time.
    Leader,
    /// The session of coordinating node `C` in life `inc` (`claim_children`).
    Coord {
        inc: u8,
    },
    Tomb,
}

/// The phase of one op's coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pc {
    NotStarted,
    /// The intent CAS has an unclear result: `undo_stray_intent` has not run yet.
    Stray,
    Intent,
    Quiesced,
    Closed,
    Cloned,
    Claimed,
    Prepared,
    Committed,
    Done,
    /// Rolling back in place ([`State::rbs`] slot 0).
    RollingBack,
    RolledBack,
    /// The task vanished (the node is still alive).
    Vanished,
    Crashed,
}

impl Pc {
    fn live(self) -> bool {
        matches!(
            self,
            Pc::Intent | Pc::Quiesced | Pc::Closed | Pc::Cloned | Pc::Claimed | Pc::Prepared
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OpState {
    pub pc: Pc,
    pub ledger: Option<Phase>,
    /// Child id issued (the intent CAS reached the bucket).
    pub allocated: bool,
    /// `C`'s `ActiveIntents` holds the sources for this op.
    pub held: bool,
    /// `C`'s "quiesced" mark (SM-13).
    pub quiesced: bool,
    /// The `writer_epoch` of the writer just quiesced, by source.
    pub sealed: [u8; 2],
    /// The coordinator has quiesced the sources (the quiesced writer has sealed).
    pub cut: bool,
    /// A write acked into a source after quiesce.
    pub dirty: bool,
}

/// The step of one roll-back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Rpc {
    /// About to write `Aborting`, read the closed marks, CAS the intent away.
    Clear,
    /// Cleaned up the child first, about to CAS (injected bug SM-3).
    CasAfter,
    /// The CAS won: remove closed marks per `marks`, clean up the child.
    Cleanup { reopen: Reopen },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RollBacker {
    pub op: u8,
    pub by: Who,
    pub node: u8,
    pub own_orphans: bool,
    pub pc: Rpc,
    /// The `writer_epoch` of each source read before the CAS (`None` = no flag).
    pub marks: [Option<u8>; 2],
}

/// An unfinished D-d round.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Gc {
    Idle,
    Loaded {
        table: Table,
    },
    Planned {
        version: u8,
        eligible: Mask,
        /// The round's `proof::RoundRefs`: (table version when read, set, has
        /// unknowns) — the plan reads it, a proof reuses it while the table has
        /// not changed version, reads again (and keeps) when it has.
        refs: (u8, Mask, bool),
    },
}

/// The `cairn-ctl fork` copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fork {
    None,
    Protected { stands_on: Mask, barrier: bool },
    Expired,
}

/// Ghost variables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Ghost {
    pub bad_delete: bool,
    pub bad_commit: bool,
    pub lost_ack: bool,
    pub self_roll_back: bool,
    pub tomb_on_live: bool,
    pub child_cloned: bool,
    pub orphan_round_discarded: bool,
    pub rolled_forward: bool,
    /// A D-d round deleted a Db after the table changed version since its plan.
    pub reaped_after_move: bool,
    /// A D-d proof deleted a Db using the round's reference set (without
    /// re-reading live range manifests).
    pub reaped_on_round_refs: bool,
}

/// Injected-failure budget used.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Spent {
    pub crash: bool,
    pub lease_loss: bool,
    pub left_table: bool,
    pub second_hold: bool,
    pub ctl: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub table: Table,
    pub ops: [OpState; 2],
    pub dbs: [Db; SLOTS],
    pub owners: [Owner; SLOTS],
    /// The node holding the range's lease (`None` = free).
    pub lease: [Option<u8>; SLOTS],
    /// The range's engine is in `quiesced()` (holds the lease, does not serve,
    /// does not run `factory`).
    pub qengine: [bool; SLOTS],
    pub alive: [bool; 2],
    /// `C`'s life (grows when rebuilt).
    pub inc: u8,
    pub x_ram: Table,
    /// Slot 0: the coordinator's roll-back (in place, `undo_stray_intent`,
    /// background); slot 1: someone else's roll-back.
    pub rbs: [Option<RollBacker>; 2],
    /// Background roll-backs waiting to run, by op.
    pub settle: Option<u8>,
    /// The reconcile loop: ops it saw unheld in the previous round.
    pub unheld: Option<u8>,
    pub gc: Gc,
    /// `gc::LiveRefs`: (table version, set of ranges live ranges list, has unknowns).
    pub live_refs: Option<(u8, Mask, bool)>,
    pub pins: [Mask; 8],
    pub orphan: Option<(RangeId, u8)>,
    pub fork: Fork,
    pub spent: Spent,
    pub ghost: Ghost,
}

/// Result of cleaning up a child Db.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Discard {
    Done,
    /// `discard_range_db` fails.
    Fails,
    /// The cleaner dies before cleaning anything (after the CAS dropping the intent).
    Dies,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// The coordinator of op `.0` CASes the intent; `.1` = unclear result (it landed).
    IntentCas(u8, bool),
    /// `undo_stray_intent`; `.1` = the read-back fails.
    Stray(u8, bool),
    Quiesce(u8),
    Close(u8),
    Clone(u8),
    Claim(u8),
    Prepare(u8),
    CommitCas(u8),
    Retire(u8),
    Fail(u8),
    Vanish(u8),
    Crash,
    Restart,
    /// Slot `.0`: the CAS step; `.1` = the roll-back fails.
    RbClear(usize, bool),
    /// Slot `.0`: remove closed marks and clean up the child.
    RbCleanup(usize, Discard),
    Settle,
    /// Node `.0` opens the Db of range `.1`; `.2` = the fresh table read fails.
    Open(u8, RangeId, bool),
    Reconcile,
    /// `cairn-ctl intent clear`; `.0` = `--force`.
    Ctl(bool),
    LeftTable(RangeId),
    SecondHold(u8),
    Ack(RangeId),
    LeaseLoss(RangeId),
    XLearn,
    Detach(RangeId, RangeId),
    GcLoadTable,
    GcPlan(Option<RangeId>),
    GcDelete(RangeId, Option<RangeId>),
    /// The ledger cleanup round (inside D-d) considers the ledger of op `.0`.
    LedgerReap(u8),
    OrphanCheck(RangeId),
    OrphanDiscard(Discard),
    Fork,
    ForkExpires,
}

impl Action {
    /// **System** actions — what the product does by itself when there is no
    /// failure (for [`stuck`]). Injected failures, node deaths, new requests,
    /// operators, client writes: no.
    pub fn is_system(&self) -> bool {
        match *self {
            Action::Quiesce(_)
            | Action::Close(_)
            | Action::Clone(_)
            | Action::Claim(_)
            | Action::Prepare(_)
            | Action::CommitCas(_)
            | Action::Retire(_)
            | Action::Settle
            | Action::Reconcile
            | Action::GcLoadTable
            | Action::GcPlan(None)
            | Action::GcDelete(_, None)
            | Action::LedgerReap(_)
            | Action::OrphanCheck(_)
            | Action::OrphanDiscard(Discard::Done) => true,
            Action::Stray(_, fails) | Action::RbClear(_, fails) => !fails,
            Action::RbCleanup(_, how) => how == Discard::Done,
            Action::Open(_, _, fails) => !fails,
            _ => false,
        }
    }
}

// ─────────────────────────────────────────── environment

impl M2 {
    fn ask(&self, v: &View, e: Event) -> Plan {
        let p = core::step_mutated(v, &e, &self.bugs);
        if let Plan::Need(n) = p {
            panic!("M2 is missing slot {n:?} for {e:?} ({v:?})");
        }
        p
    }

    fn existing(&self, s: &State) -> Mask {
        self.ranges()
            .filter(|&r| s.dbs[r as usize].exists)
            .fold(0, |m, r| m | bit(r))
    }

    /// Live Dbs: ranges in the table and in-flight children.
    fn live_dbs(&self, s: &State) -> Mask {
        (self.in_table(s.table) | self.promised(s.table)) & self.existing(s)
    }

    /// Every Db that a live Db (unexpired forks included) lists.
    fn needed(&self, s: &State) -> Mask {
        let mut m = ids(self.live_dbs(s)).fold(0, |m, d| m | s.dbs[d as usize].external);
        if let Fork::Protected { stands_on, .. } = s.fork {
            m |= stands_on;
        }
        m
    }

    fn barrier(s: &State) -> Mask {
        match s.fork {
            Fork::Protected {
                stands_on,
                barrier: true,
            } => stands_on,
            _ => 0,
        }
    }

    /// `referenced_ranges_with(live, in_flight)` read right now: (set, unknown).
    fn refs(&self, s: &State, live: Mask, in_flight: Mask, fail: Option<RangeId>) -> (Mask, bool) {
        let mut refs = 0;
        let mut unknown = false;
        for l in ids(live | in_flight) {
            let db = s.dbs[l as usize];
            if !db.exists && !has(live, l) {
                continue;
            }
            if !db.exists || fail == Some(l) {
                unknown = true;
            } else {
                refs |= db.external;
            }
        }
        (refs, unknown)
    }

    fn refs_of(m: Mask, unknown: bool, r: RangeId) -> Refs {
        if has(m, r) {
            Refs::Referenced
        } else if unknown {
            Refs::Unknown
        } else {
            Refs::Clear
        }
    }

    /// Final checkpoints on Db `r` as `checkpoint_pins` reads them: named ones
    /// whose creating range carries no tombstone and whose Db still exists
    /// (fork: a name outside the prefix, always counted), among them those of
    /// live Dbs (`live`: ranges in the table and in-flight children with a Db in
    /// the **same** snapshot the scan used), and unnamed ones.
    fn pins(&self, s: &State, r: RangeId, live: Mask) -> core::Pins {
        let db = s.dbs[r as usize];
        let tombed = self
            .ranges()
            .filter(|&c| s.owners[c as usize] == Owner::Tomb)
            .fold(0, |m, c| m | bit(c));
        let named = db.checkpoints & !db.unnamed & ((self.existing(s) & !tombed) | bit(FORK));
        core::Pins {
            named: named != 0,
            by_live: named & live & self.existing(s) != 0,
            unnamed: db.checkpoints & db.unnamed != 0,
        }
    }

    /// D-d's scan: `refs` as read, or blind (`Env::dd_scan_sees` off).
    fn dd_refs(&self, refs: (Mask, bool)) -> (Mask, bool) {
        if self.env.dd_scan_sees {
            refs
        } else {
            (0, false)
        }
    }

    /// Gate on the current table: the set plus in-flight children (unless the old shell does not add them).
    fn gate_now(&self, s: &State, r: RangeId) -> Gate {
        let in_flight = if self.bugs.dd_ignores_in_flight {
            0
        } else {
            self.promised(s.table)
        };
        let (m, unknown) = self.refs(s, self.in_table(s.table), in_flight, None);
        Gate {
            table_version: s.table.version as u64,
            refs_version: s.table.version as u64,
            refs: Self::refs_of(m, unknown, r),
            refs_in_flight: !self.bugs.dd_ignores_in_flight,
            barrier: has(Self::barrier(s), r),
            moved: false,
        }
    }

    fn standing(&self, s: &State, k: usize) -> Standing {
        let t = s.table;
        match (t.intent, t.committed) {
            (Some(i), _) if i as usize == k => Standing::Pending,
            (_, Some(c)) if c as usize == k => Standing::Committed,
            (_, Some(_)) => Standing::Elsewhere,
            (other, None) => Standing::Free {
                other: other.is_some(),
            },
        }
    }

    fn entry(&self, t: Table, r: RangeId) -> Entry {
        Entry {
            in_table: has(self.in_table(t), r),
            intent: match t.intent {
                Some(k) if has(self.src(), r) => Some(self.core_op(k as usize)),
                _ => None,
            },
        }
    }

    fn child_facts(&self, s: &State, r: RangeId) -> Child {
        let db = s.dbs[r as usize];
        let allocated = match self.op_of_child(r) {
            Some(k) => s.ops[k].allocated,
            None => true,
        };
        Child {
            in_table: has(self.in_table(s.table), r),
            promised: has(self.promised(s.table), r),
            allocated,
            db: db.exists,
            owner: s.owners[r as usize] != Owner::None,
            stands_on_live: db.exists && db.external & self.in_table(s.table) != 0,
        }
    }

    fn ledger_of(&self, s: &State, r: RangeId) -> Option<Phase> {
        self.op_of_child(r).and_then(|k| s.ops[k].ledger)
    }

    /// Write op `k`'s ledger by the transition rule. `false` = refused.
    fn write_ledger(&self, s: &mut State, k: usize, p: Option<Phase>) -> bool {
        let Some(p) = p else { return true };
        match Phase::advance(s.ops[k].ledger, p) {
            Advance::Write => {
                s.ops[k].ledger = Some(p);
                true
            }
            Advance::Already => true,
            Advance::Refused => false,
        }
    }

    fn delete_db(&self, s: &mut State, r: RangeId) {
        if s.dbs[r as usize].exists && has(self.needed(s), r) {
            s.ghost.bad_delete = true;
        }
        s.dbs[r as usize] = Db::default();
    }

    /// The owner of `r` is dead by the rule of `Session::claim`.
    fn owner_dead(&self, s: &State, o: Owner) -> bool {
        match o {
            Owner::Leader => true,
            Owner::Coord { inc } => !(s.alive[C as usize] && inc == s.inc),
            Owner::None | Owner::Tomb => false,
        }
    }

    /// A child-cleanup path for `c` (not the orphan-child round): the core
    /// decides, the gate checks, `discard_range_db` per `how`.
    fn discard_child(&self, s: &mut State, c: RangeId, by: Remover, how: Discard) {
        // `discard_own_orphans` reads a fresh set (`LiveRefs::default()`); the
        // cleanup right after the CAS does not use it (the gate checks).
        let v = View {
            child: Some(self.child_facts(s, c)),
            ledger: Some(Ok(self.ledger_of(s, c))),
            gate: Some(Ok(self.orphan_gate(s, c, None))),
            ..View::default()
        };
        match self.ask(&v, Event::Discard(by)) {
            Plan::DiscardDb => {
                self.gated_discard(s, c, how);
            }
            // `abandoned_child_owner`: the Db is gone, permanently outside the table.
            Plan::DeleteOwner
                if !s.dbs[c as usize].exists && self.child_facts(s, c).permanently_out() =>
            {
                s.owners[c as usize] = Owner::None;
            }
            _ => {}
        }
    }

    /// Gate `abandoned_child_db`, then `discard_range_db`, then the owner record.
    /// `true` = the Db is gone.
    fn gated_discard(&self, s: &mut State, c: RangeId, how: Discard) -> bool {
        let v = View {
            child: Some(self.child_facts(s, c)),
            ledger: Some(Ok(self.ledger_of(s, c))),
            gate: Some(Ok(self.gate_now(s, c))),
            ..View::default()
        };
        if self.ask(&v, Event::Gate) != Plan::Prove {
            return false;
        }
        let ok = match how {
            Discard::Done => {
                for e in ids(s.dbs[c as usize].external) {
                    s.dbs[e as usize].checkpoints &= !bit(c);
                    s.dbs[e as usize].unnamed &= !bit(c);
                }
                self.delete_db(s, c);
                true
            }
            Discard::Fails if self.env.keep_db_until_checkpoints_gone => false,
            Discard::Fails => {
                self.delete_db(s, c);
                true
            }
            Discard::Dies => return false,
        };
        if self.ask(&View::default(), Event::Discarded { ok }) == Plan::DeleteOwner {
            s.owners[c as usize] = Owner::None;
        }
        ok
    }

    /// Reference set of the orphan-child round (`LiveRefs`): kept across the
    /// round, re-read when the version changes (unless the old shell does not
    /// lock the version — irreversible.md N4).
    fn orphan_refs(&self, s: &mut State) -> (u8, Mask, bool) {
        let version = s.table.version;
        match s.live_refs {
            Some((v, m, u)) if !u && (v == version || self.bugs.n4_any_refs_version) => (v, m, u),
            _ => {
                let (m, u) = self.refs(s, self.in_table(s.table), 0, None);
                s.live_refs = Some((version, m, u));
                (version, m, u)
            }
        }
    }

    fn orphan_gate(&self, s: &State, r: RangeId, refs: Option<(u8, Mask, bool)>) -> Gate {
        let (v, m, u) = refs.unwrap_or_else(|| {
            let (m, u) = self.refs(s, self.in_table(s.table), 0, None);
            (s.table.version, m, u)
        });
        Gate {
            table_version: s.table.version as u64,
            refs_version: v as u64,
            refs: Self::refs_of(m, u, r),
            refs_in_flight: false,
            barrier: has(Self::barrier(s), r),
            moved: false,
        }
    }

    /// Whether the coordinator of op `k` (on `C`) can still run.
    fn coord_up(&self, s: &State) -> bool {
        s.alive[C as usize]
    }

    /// Start a roll-back in slot `slot`.
    fn spawn_rb(s: &mut State, slot: usize, rb: RollBacker) -> bool {
        if s.rbs[slot].is_some() {
            return false;
        }
        s.rbs[slot] = Some(rb);
        true
    }

    /// Release op `k`'s quiesced sources (`Resign`): lease free, mark removed.
    fn release_sources(&self, s: &mut State, k: usize) {
        for &r in self.sources() {
            if s.lease[r as usize] == Some(C) && s.qengine[r as usize] {
                s.lease[r as usize] = None;
                s.qengine[r as usize] = false;
            }
        }
        s.ops[k].held = false;
        s.ops[k].quiesced = false;
    }

    /// The commit CAS of op `k` (by the coordinator or a roll-forwarder).
    fn commit(&self, s: &mut State, k: usize) -> bool {
        if s.table.intent != Some(k as u8) {
            return false;
        }
        let c = self.child(k);
        if !s.dbs[c as usize].exists || (!self.legacy && s.owners[c as usize] == Owner::None) {
            s.ghost.bad_commit = true;
        }
        if s.ops[k].dirty {
            s.ghost.lost_ack = true;
        }
        s.table = Table {
            version: s.table.version + 1,
            intent: None,
            committed: Some(k as u8),
        };
        true
    }

    /// The check that allows rolling forward: the children have a Db and a live
    /// owner record (no tombstone), the sources are still closed at the epoch of the quiesce.
    fn prepared(&self, s: &State, k: usize) -> bool {
        let c = self.child(k) as usize;
        s.dbs[c].exists
            && (self.legacy || matches!(s.owners[c], Owner::Coord { .. }))
            && self.sources().iter().enumerate().all(|(i, &r)| {
                let d = s.dbs[r as usize];
                d.closed && d.epoch == s.ops[k].sealed[i] + 1
            })
    }

    /// Roll back or forward for non-coordinator `finder` (the opener on `node`,
    /// the reconcile loop, ctl).
    fn resolve(&self, s: &mut State, k: usize, finder: Finder, by: Who, node: u8) -> bool {
        let v = View {
            ledger: Some(Ok(s.ops[k].ledger)),
            prepared: Some(Ok(self.prepared(s, k))),
            ..View::default()
        };
        match self.ask(&v, Event::Found(finder)) {
            Plan::Abort { ledger } => {
                if node == C && s.ops[k].pc.live() && s.alive[C as usize] {
                    s.ghost.self_roll_back = true;
                }
                let _ = ledger;
                Self::spawn_rb(
                    s,
                    1,
                    RollBacker {
                        op: k as u8,
                        by,
                        node,
                        own_orphans: false,
                        pc: Rpc::Clear,
                        marks: [None; 2],
                    },
                )
            }
            Plan::RollForward => {
                if !self.commit(s, k) {
                    return false;
                }
                self.write_ledger(s, k, Some(Phase::Committed));
                s.ghost.rolled_forward = true;
                true
            }
            Plan::Remember => {
                s.unheld = Some(k as u8);
                true
            }
            _ => false,
        }
    }

    fn step(&self, s: &State, a: Action) -> Option<State> {
        let mut s = s.clone();
        let f = self.faults;
        match a {
            // ───────────────────────────── the coordinator
            Action::IntentCas(k, unknown) => {
                let k = k as usize;
                if k >= self.ops()
                    || (unknown && !f.ambiguous)
                    || s.ops[k].pc != Pc::NotStarted
                    || !self.coord_up(&s)
                    || s.table.intent.is_some()
                    || s.table.committed.is_some()
                    || (k == 1 && s.ops[0].pc == Pc::NotStarted)
                    || self
                        .sources()
                        .iter()
                        .any(|&r| s.lease[r as usize] != Some(C) || !s.dbs[r as usize].writer)
                {
                    return None;
                }
                if self.ask(&View::default(), Event::Hold { taken: false }) != Plan::Hold {
                    return None;
                }
                s.table = Table {
                    version: s.table.version + 1,
                    intent: Some(k as u8),
                    committed: None,
                };
                s.ops[k].allocated = true;
                let cas = if unknown { Cas::Unknown } else { Cas::Applied };
                match self.ask(&View::default(), Event::IntentCas(cas)) {
                    Plan::Go { ledger } => {
                        self.write_ledger(&mut s, k, ledger);
                        s.ops[k].held = true;
                        s.ops[k].pc = Pc::Intent;
                    }
                    Plan::UndoStray => s.ops[k].pc = Pc::Stray,
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Stray(k, fails) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Stray || !self.coord_up(&s) {
                    return None;
                }
                if fails {
                    match self.ask(&View::default(), Event::RollBackFailed(Who::StrayUndo)) {
                        Plan::Settle => s.settle = Some(k as u8),
                        Plan::GiveUp => {}
                        p => unreachable!("{p:?}"),
                    }
                    s.ops[k].pc = Pc::RolledBack;
                } else {
                    if !Self::spawn_rb(
                        &mut s,
                        0,
                        RollBacker {
                            op: k as u8,
                            by: Who::StrayUndo,
                            node: C,
                            own_orphans: true,
                            pc: Rpc::Clear,
                            marks: [None; 2],
                        },
                    ) {
                        return None;
                    }
                    s.ops[k].pc = Pc::RollingBack;
                }
            }
            Action::Quiesce(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Intent || !self.coord_up(&s) {
                    return None;
                }
                if self
                    .sources()
                    .iter()
                    .any(|&r| s.lease[r as usize] != Some(C) || !s.dbs[r as usize].writer)
                {
                    // The source changed owner before quiesce: `NotLeader` ⇒ the step fails.
                    return self.coordinator_fails(s, k);
                }
                s.ops[k].quiesced = true;
                s.ops[k].cut = true;
                for (i, &r) in self.sources().iter().enumerate() {
                    s.dbs[r as usize].writer = false;
                    s.qengine[r as usize] = true;
                    s.ops[k].sealed[i] = s.dbs[r as usize].epoch;
                }
                s.ops[k].pc = Pc::Quiesced;
            }
            Action::Close(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Quiesced || !self.coord_up(&s) {
                    return None;
                }
                let v = View {
                    standing: Some(Ok(self.standing(&s, k))),
                    ..View::default()
                };
                match self.ask(&v, Event::Enter(Stage::Close)) {
                    Plan::Skip => s.ops[k].pc = Pc::Closed,
                    Plan::Go { ledger } => {
                        let epochs_ok = self.sources().iter().enumerate().all(|(i, &r)| {
                            s.dbs[r as usize].epoch == s.ops[k].sealed[i]
                                || (s.dbs[r as usize].closed
                                    && s.dbs[r as usize].epoch == s.ops[k].sealed[i] + 1)
                        });
                        if !self.write_ledger(&mut s, k, ledger) || !epochs_ok {
                            // `Fenced`: someone reopened the source after quiesce ⇒ roll back.
                            return self.coordinator_fails(s, k);
                        }
                        for (i, &r) in self.sources().iter().enumerate() {
                            let d = &mut s.dbs[r as usize];
                            if d.epoch == s.ops[k].sealed[i] {
                                d.closed = true;
                                d.epoch += 1;
                                d.writer = false;
                            }
                        }
                        s.ops[k].pc = Pc::Closed;
                    }
                    p @ Plan::RollBack { .. } => return self.start_roll_back(s, k, p),
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Clone(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Closed
                    || !self.coord_up(&s)
                    || !self.sources().iter().all(|&r| s.dbs[r as usize].exists)
                {
                    return None;
                }
                let v = View {
                    standing: Some(Ok(self.standing(&s, k))),
                    ..View::default()
                };
                match self.ask(&v, Event::Enter(Stage::CloneFirst)) {
                    Plan::Go { ledger } => {
                        if !self.write_ledger(&mut s, k, ledger) {
                            return self.coordinator_fails(s, k);
                        }
                        let external = self
                            .sources()
                            .iter()
                            .fold(0, |m, &r| m | bit(r) | s.dbs[r as usize].external);
                        let c = self.child(k);
                        s.dbs[c as usize] = Db {
                            exists: true,
                            external,
                            ..Db::default()
                        };
                        s.pins[c as usize] = external;
                        for e in ids(external) {
                            if s.dbs[e as usize].exists {
                                s.dbs[e as usize].checkpoints |= bit(c);
                            }
                        }
                        s.ghost.child_cloned = true;
                        s.ops[k].pc = Pc::Cloned;
                    }
                    p @ Plan::RollBack { .. } => return self.start_roll_back(s, k, p),
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Claim(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Cloned || !self.coord_up(&s) {
                    return None;
                }
                let v = View {
                    standing: Some(Ok(self.standing(&s, k))),
                    ..View::default()
                };
                match self.ask(&v, Event::Enter(Stage::Claim)) {
                    Plan::Go { .. } => {
                        if !self.legacy {
                            s.owners[self.child(k) as usize] = Owner::Coord { inc: s.inc };
                        }
                        s.ops[k].pc = Pc::Claimed;
                    }
                    p @ Plan::RollBack { .. } => return self.start_roll_back(s, k, p),
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Prepare(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Claimed || !self.coord_up(&s) {
                    return None;
                }
                match self.ask(&View::default(), Event::Enter(Stage::Commit)) {
                    Plan::Go { ledger } => {
                        if !self.write_ledger(&mut s, k, ledger) {
                            // The ledger moved to the roll-back branch: someone else is rolling this op back.
                            return self.coordinator_fails(s, k);
                        }
                        s.ops[k].pc = Pc::Prepared;
                    }
                    p => unreachable!("{p:?}"),
                }
            }
            Action::CommitCas(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Prepared || !self.coord_up(&s) {
                    return None;
                }
                let cas = if self.commit(&mut s, k) {
                    Cas::Applied
                } else {
                    Cas::Lost
                };
                match self.ask(&View::default(), Event::CommitCas(cas)) {
                    Plan::Go { ledger } => {
                        self.write_ledger(&mut s, k, ledger);
                        s.ops[k].pc = Pc::Committed;
                    }
                    p @ Plan::RollBack { .. } => return self.start_roll_back(s, k, p),
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Retire(k) => {
                let k = k as usize;
                if s.ops[k].pc != Pc::Committed || !self.coord_up(&s) {
                    return None;
                }
                for &r in self.sources() {
                    if !self.legacy {
                        s.owners[r as usize] = Owner::Tomb;
                    }
                    s.dbs[r as usize].writer = false;
                    s.lease[r as usize] = None;
                    s.qengine[r as usize] = false;
                }
                s.ops[k].held = false;
                s.ops[k].quiesced = false;
                s.ops[k].pc = Pc::Done;
            }
            Action::Fail(k) => {
                let k = k as usize;
                if !s.ops[k].pc.live() || !self.coord_up(&s) {
                    return None;
                }
                return self.coordinator_fails(s, k);
            }
            Action::Vanish(k) => {
                let k = k as usize;
                if !f.vanish || !s.ops[k].pc.live() || !self.coord_up(&s) {
                    return None;
                }
                match self.ask(&View::default(), Event::Vanished) {
                    Plan::Release => self.release_sources(&mut s, k),
                    Plan::GiveUp => {
                        // Dropping the guard removes the mark; the source engine stays in
                        // `quiesced()` holding the lease (no holding wire).
                        s.ops[k].held = false;
                        s.ops[k].quiesced = false;
                    }
                    p => unreachable!("{p:?}"),
                }
                s.ops[k].pc = Pc::Vanished;
            }
            Action::Crash => {
                if !f.crash || s.spent.crash || !s.alive[C as usize] {
                    return None;
                }
                s.spent.crash = true;
                s.alive[C as usize] = false;
                for r in self.ranges().map(usize::from) {
                    if s.lease[r] == Some(C) {
                        s.lease[r] = None;
                        s.dbs[r].writer = false;
                        s.qengine[r] = false;
                    }
                }
                for op in s.ops.iter_mut() {
                    op.held = false;
                    op.quiesced = false;
                    if op.pc.live() || matches!(op.pc, Pc::Stray | Pc::RollingBack | Pc::Committed)
                    {
                        op.pc = Pc::Crashed;
                    }
                }
                s.rbs[0] = None;
                if s.rbs[1].is_some_and(|rb| rb.node == C) {
                    s.rbs[1] = None;
                }
                s.settle = None;
            }
            Action::Restart => {
                if s.alive[C as usize] {
                    return None;
                }
                s.alive[C as usize] = true;
                s.inc += 1;
            }

            // ───────────────────────────── roll-back
            Action::RbClear(slot, fails) => {
                let rb = s.rbs[slot]?;
                if rb.pc != Rpc::Clear || (fails && !f.ambiguous) || !s.alive[rb.node as usize] {
                    return None;
                }
                let k = rb.op as usize;
                if fails {
                    s.rbs[slot] = None;
                    match self.ask(&View::default(), Event::RollBackFailed(rb.by)) {
                        Plan::Settle => s.settle = Some(rb.op),
                        Plan::GiveUp | Plan::Fail => {}
                        p => unreachable!("{p:?}"),
                    }
                    if rb.by == Who::Coordinator {
                        // `roll_back_here` cleans up its child and reopens the sources
                        // even when the roll-back fails.
                        if rb.own_orphans {
                            self.own_orphans(&mut s, k);
                        }
                        self.release_sources(&mut s, k);
                        s.ops[k].pc = Pc::RolledBack;
                    }
                    return Some(s);
                }
                // `Aborting` before the CAS (refused when the op has no ledger or is
                // done — the table is the arbiter).
                self.write_ledger(&mut s, k, (!self.bugs.no_ledger).then_some(Phase::Aborting));
                let mut marks = [None; 2];
                for (i, &r) in self.sources().iter().enumerate() {
                    let d = s.dbs[r as usize];
                    marks[i] = d.closed.then_some(d.epoch);
                }
                let v = View {
                    standing: Some(Ok(self.standing(&s, k))),
                    ..View::default()
                };
                let mut rb = rb;
                rb.marks = marks;
                match self.ask(&v, Event::Clear) {
                    Plan::CasClear => {
                        s.table = Table {
                            version: s.table.version + 1,
                            intent: None,
                            committed: None,
                        };
                        match self.ask(
                            &View::default(),
                            Event::Cleared {
                                cas: Cas::Applied,
                                by: rb.by,
                            },
                        ) {
                            Plan::Cleanup { ledger, reopen } => {
                                self.write_ledger(&mut s, k, ledger);
                                rb.pc = Rpc::Cleanup { reopen };
                                s.rbs[slot] = Some(rb);
                            }
                            p => unreachable!("{p:?}"),
                        }
                    }
                    Plan::DiscardFirst => {
                        let c = self.child(k);
                        self.discard_child(&mut s, c, Remover::RollBack, Discard::Done);
                        rb.pc = Rpc::CasAfter;
                        s.rbs[slot] = Some(rb);
                    }
                    Plan::Nothing => {
                        s.rbs[slot] = None;
                        self.finish_rb(&mut s, rb);
                    }
                    p => unreachable!("{p:?}"),
                }
            }
            Action::RbCleanup(slot, how) => {
                let rb = s.rbs[slot]?;
                if !s.alive[rb.node as usize] || (how != Discard::Done && !f.discard) {
                    return None;
                }
                let k = rb.op as usize;
                match rb.pc {
                    Rpc::Clear => return None,
                    Rpc::CasAfter => {
                        if s.table.intent == Some(rb.op) {
                            s.table = Table {
                                version: s.table.version + 1,
                                intent: None,
                                committed: None,
                            };
                        }
                    }
                    Rpc::Cleanup { reopen } => {
                        // Remove closed marks by the marks read before the CAS.
                        for (i, &r) in self.sources().iter().enumerate() {
                            let d = &mut s.dbs[r as usize];
                            match (reopen, rb.marks[i]) {
                                (Reopen::AtMark, Some(e)) if d.closed && d.epoch == e => {
                                    d.closed = false
                                }
                                (Reopen::Any, _) => d.closed = false,
                                _ => {}
                            }
                        }
                        if how == Discard::Dies {
                            // The roll-backer dies after removing the marks, before cleaning anything.
                            s.rbs[slot] = None;
                            if rb.by == Who::Coordinator || rb.node == C && slot == 0 {
                                s.ops[k].pc = Pc::Vanished;
                                self.release_sources(&mut s, k);
                            } else if rb.by == Who::Opener {
                                for &r in self.sources() {
                                    if s.lease[r as usize] == Some(rb.node) {
                                        s.lease[r as usize] = None;
                                    }
                                }
                            }
                            return Some(s);
                        }
                        let by = if rb.by == Who::Ctl {
                            Remover::Ctl
                        } else {
                            Remover::RollBack
                        };
                        let c = self.child(k);
                        self.discard_child(&mut s, c, by, how);
                    }
                }
                s.rbs[slot] = None;
                self.finish_rb(&mut s, rb);
            }
            Action::Settle => {
                let k = s.settle? as usize;
                if !self.coord_up(&s)
                    || !Self::spawn_rb(
                        &mut s,
                        0,
                        RollBacker {
                            op: k as u8,
                            by: Who::Settle,
                            node: C,
                            own_orphans: true,
                            pc: Rpc::Clear,
                            marks: [None; 2],
                        },
                    )
                {
                    return None;
                }
                s.settle = None;
            }

            // ───────────────────────────── non-coordinators
            Action::Open(node, r, read_fails) => {
                let n = node as usize;
                if has(self.ancestors(), r)
                    || r as usize >= self.range_count()
                    || (read_fails && !f.stale_reads)
                    || !s.alive[n]
                    || !(s.lease[r as usize].is_none() || s.lease[r as usize] == Some(node))
                    || s.qengine[r as usize]
                    || s.dbs[r as usize].writer
                    || !s.dbs[r as usize].exists
                {
                    return None;
                }
                // The child has committed: while `C`'s owner record is alive only `C` claims it.
                if let Owner::Coord { inc } = s.owners[r as usize] {
                    if node != C && s.alive[C as usize] && inc == s.inc {
                        return None;
                    }
                }
                let (quiesced_here, active_here) = (0..self.ops()).fold((false, false), |a, k| {
                    let src = has(self.src(), r);
                    (
                        a.0 || (node == C && src && s.ops[k].quiesced),
                        a.1 || (node == C && src && s.ops[k].held),
                    )
                });
                let ram_table = if node == X { s.x_ram } else { s.table };
                let v = View {
                    entry: Some(if read_fails {
                        Err(core::ReadErr)
                    } else {
                        Ok(self.entry(s.table, r))
                    }),
                    ..View::default()
                };
                let plan = self.ask(
                    &v,
                    Event::Open {
                        quiesced_here,
                        active_here,
                        ram: self.entry(ram_table, r),
                    },
                );
                match plan {
                    Plan::WaitUnquiesced | Plan::Retry | Plan::Gone => return None,
                    Plan::Resolve(op) => {
                        let k = (0..self.ops()).find(|&k| self.core_op(k) == op)?;
                        if !self.resolve(
                            &mut s,
                            k,
                            Finder::Opener { active_here },
                            Who::Opener,
                            node,
                        ) {
                            return None;
                        }
                        s.lease[r as usize] = Some(node);
                    }
                    Plan::OpenDb => {
                        if s.dbs[r as usize].closed {
                            // SlateDB refuses the writer: remove the stale mark itself? Marks
                            // read first, then the table (failure ⇒ keep the mark).
                            let v = View {
                                entry: Some(if read_fails {
                                    Err(core::ReadErr)
                                } else {
                                    Ok(self.entry(s.table, r))
                                }),
                                ..View::default()
                            };
                            match self.ask(&v, Event::StaleClose) {
                                Plan::ReopenAtMark | Plan::ReopenAny => {
                                    s.dbs[r as usize].closed = false
                                }
                                Plan::KeepClosed => return None,
                                p => unreachable!("{p:?}"),
                            }
                        }
                        let d = &mut s.dbs[r as usize];
                        d.writer = true;
                        d.epoch += 1;
                        s.lease[r as usize] = Some(node);
                    }
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Reconcile => {
                let k = s.table.intent? as usize;
                // Ask the leader node of each source: only `C` has a coordinator.
                let held = s.alive[C as usize]
                    && s.ops[k].held
                    && self
                        .sources()
                        .iter()
                        .any(|&r| s.lease[r as usize] == Some(C));
                let before = s.unheld == Some(k as u8);
                if held {
                    s.unheld?;
                    s.unheld = None;
                    return Some(s);
                }
                let finder = Finder::Reconciler {
                    held: Ok(false),
                    unheld_before: before,
                };
                if !self.resolve(&mut s, k, finder, Who::Reconciler, X) {
                    return None;
                }
                if before {
                    s.unheld = None;
                }
            }
            Action::Ctl(force) => {
                if !self.ctl || s.spent.ctl {
                    return None;
                }
                let k = s.table.intent? as usize;
                let live_owner = self
                    .sources()
                    .iter()
                    .chain(std::iter::once(&self.child(k)))
                    .any(|&r| {
                        let o = s.owners[r as usize];
                        o != Owner::None && o != Owner::Tomb && !self.owner_dead(&s, o)
                    })
                    || (s.alive[C as usize] && s.ops[k].pc.live());
                if !self.resolve(&mut s, k, Finder::Ctl { live_owner, force }, Who::Ctl, X) {
                    return None;
                }
                s.spent.ctl = true;
            }
            Action::LeftTable(r) => {
                if !f.ram || self.legacy || s.spent.left_table || !has(self.src(), r) {
                    return None;
                }
                if !has(self.in_table(s.table), r) {
                    return None;
                }
                let v = View {
                    entry: Some(Ok(self.entry(s.table, r))),
                    ..View::default()
                };
                s.spent.left_table = true;
                if self.ask(&v, Event::LeftTable) == Plan::Retire {
                    s.owners[r as usize] = Owner::Tomb;
                    s.ghost.tomb_on_live = true;
                } else {
                    return None;
                }
            }
            Action::SecondHold(k) => {
                let k = k as usize;
                if !f.ram || s.spent.second_hold || !s.ops[k].held || !self.coord_up(&s) {
                    return None;
                }
                s.spent.second_hold = true;
                match self.ask(&View::default(), Event::Hold { taken: true }) {
                    Plan::Refuse { release: true } => {
                        s.ops[k].held = false;
                    }
                    Plan::Refuse { release: false } => return None,
                    p => unreachable!("{p:?}"),
                }
            }
            Action::Ack(r) => {
                if !f.acks || !s.dbs[r as usize].writer || !has(self.src(), r) {
                    return None;
                }
                if !has(self.in_table(s.table), r) {
                    // A write into a range that left the table: no child has it.
                    s.ghost.lost_ack = true;
                } else {
                    // A write after the quiesce of an op in flight: the child has (or
                    // will have) cloned without it — lost if the op commits.
                    let k = s.table.intent? as usize;
                    if !s.ops[k].cut || s.ops[k].dirty {
                        return None;
                    }
                    s.ops[k].dirty = true;
                }
            }
            Action::LeaseLoss(r) => {
                if !f.lease_loss || s.spent.lease_loss || !has(self.src(), r) {
                    return None;
                }
                s.lease[r as usize]?;
                s.spent.lease_loss = true;
                s.lease[r as usize] = None;
                s.dbs[r as usize].writer = false;
                s.qengine[r as usize] = false;
            }
            Action::XLearn => {
                if s.x_ram == s.table {
                    return None;
                }
                s.x_ram = s.table;
            }

            // ───────────────────────────── SlateDB graph, D-d, orphan round, fork
            Action::Detach(d, e) => {
                let db = s.dbs[d as usize];
                if !self.gc
                    || !db.exists
                    || !db.writer
                    || !has(db.external, e)
                    || (self.env.detach_respects_checkpoints
                        && ids(db.checkpoints).any(|c| has(s.pins[c as usize], e)))
                {
                    return None;
                }
                s.dbs[d as usize].external &= !bit(e);
                s.dbs[e as usize].checkpoints &= !bit(d);
                s.dbs[e as usize].unnamed &= !bit(d);
            }
            Action::GcLoadTable => {
                if !self.gc || s.gc != Gc::Idle {
                    return None;
                }
                s.gc = Gc::Loaded { table: s.table };
            }
            Action::GcPlan(fail) => {
                let Gc::Loaded { table: snap } = s.gc else {
                    return None;
                };
                let live = self.in_table(snap);
                let in_flight = if self.bugs.dd_ignores_in_flight {
                    0
                } else {
                    self.promised(snap)
                };
                if fail.is_some_and(|l| !has(live | (in_flight & self.existing(&s)), l)) {
                    return None;
                }
                // The old shell filters candidates by the RAM table of this moment.
                let filter = if self.bugs.dd_mixed_snapshots {
                    s.table
                } else {
                    snap
                };
                let (refs, unknown) = self.dd_refs(self.refs(&s, live, in_flight, fail));
                let mut eligible = 0;
                for r in self.ranges() {
                    let v = View {
                        retired: Some(Retired {
                            in_table: has(self.in_table(filter), r),
                            promised: has(self.promised(filter), r),
                            tomb: s.owners[r as usize] == Owner::Tomb,
                            db: s.dbs[r as usize].exists,
                        }),
                        gate: Some(Ok(Gate {
                            table_version: filter.version as u64,
                            refs_version: snap.version as u64,
                            refs: Self::refs_of(refs, unknown, r),
                            refs_in_flight: !self.bugs.dd_ignores_in_flight,
                            barrier: has(Self::barrier(&s), r),
                            moved: false,
                        })),
                        pins: Some(Ok(self.pins(&s, r, live | in_flight))),
                        ..View::default()
                    };
                    if self.ask(&v, Event::ReapPlan) == Plan::Eligible {
                        eligible |= bit(r);
                    }
                }
                s.gc = if eligible == 0 {
                    Gc::Idle
                } else {
                    Gc::Planned {
                        version: snap.version,
                        eligible,
                        refs: (snap.version, refs, unknown),
                    }
                };
            }
            Action::GcDelete(r, fail) => {
                let Gc::Planned {
                    version,
                    eligible,
                    refs: round,
                } = s.gc
                else {
                    return None;
                };
                // The round's set is reused while the table has not changed version
                // and the set has no unknowns (`proof::RoundRefs`) — then the proof
                // reads no manifest, so there is no failed read to inject.
                let cached = (round.0 == s.table.version && !round.2).then_some((round.1, round.2));
                let live = self.in_table(s.table);
                let in_flight = if self.bugs.dd_ignores_in_flight {
                    0
                } else {
                    self.promised(s.table)
                };
                if !has(eligible, r)
                    || fail.is_some_and(|l| {
                        self.bugs.dd_no_proof_recheck
                            || cached.is_some()
                            || !has(live | (in_flight & self.existing(&s)), l)
                    })
                {
                    return None;
                }
                let (refs, unknown) =
                    cached.unwrap_or_else(|| self.dd_refs(self.refs(&s, live, in_flight, fail)));
                let rest = eligible & !bit(r);
                let next = if rest == 0 {
                    Gc::Idle
                } else {
                    Gc::Planned {
                        version,
                        eligible: rest,
                        refs: (s.table.version, refs, unknown),
                    }
                };
                // `moved` compared with the snapshot the gate's facts stand on: a
                // proof rebuilt on the current table (`DeleteProof::retired_db`; the
                // delete re-checks its version — atomic here) changes nothing;
                // without the rebuild it is the plan's snapshot. The earlier shell
                // compared with the plan even after rebuilding (`gc.rs:run_once`).
                let basis = if self.bugs.dd_abandons_round || self.bugs.dd_no_proof_recheck {
                    version
                } else {
                    s.table.version
                };
                let v = View {
                    retired: Some(Retired {
                        in_table: has(live, r),
                        promised: has(self.promised(s.table), r),
                        tomb: s.owners[r as usize] == Owner::Tomb,
                        db: s.dbs[r as usize].exists,
                    }),
                    gate: Some(Ok(Gate {
                        table_version: s.table.version as u64,
                        refs_version: s.table.version as u64,
                        refs: Self::refs_of(refs, unknown, r),
                        refs_in_flight: !self.bugs.dd_ignores_in_flight,
                        barrier: has(Self::barrier(&s), r),
                        moved: s.table.version != basis,
                    })),
                    pins: Some(Ok(self.pins(&s, r, live | self.promised(s.table)))),
                    ..View::default()
                };
                match self.ask(&v, Event::ReapProve) {
                    Plan::Delete => {
                        self.delete_db(&mut s, r);
                        if s.table.version != version {
                            s.ghost.reaped_after_move = true;
                        }
                        if cached.is_some() {
                            s.ghost.reaped_on_round_refs = true;
                        }
                        s.gc = next;
                    }
                    Plan::AbandonRound => s.gc = Gc::Idle,
                    Plan::Keep(_) => s.gc = next,
                    p => unreachable!("{p:?}"),
                }
            }
            Action::OrphanCheck(r) => {
                if s.orphan.is_some()
                    || !self.ranges().any(|x| x == r)
                    || has(self.in_table(s.table), r)
                    || s.owners[r as usize] == Owner::Tomb
                    || (!s.dbs[r as usize].exists && s.owners[r as usize] == Owner::None)
                {
                    return None;
                }
                let before = s.clone();
                let refs = self.orphan_refs(&mut s);
                let v = View {
                    child: Some(self.child_facts(&s, r)),
                    ledger: Some(Ok(self.ledger_of(&s, r))),
                    gate: Some(Ok(self.orphan_gate(&s, r, Some(refs)))),
                    ..View::default()
                };
                match self.ask(&v, Event::Discard(Remover::OrphanRound)) {
                    Plan::DiscardDb => s.orphan = Some((r, s.table.version)),
                    // The ledger says the op may have committed: like "cannot prove" —
                    // the tombstone branch (`gc.rs:discard_if_orphan_child`).
                    Plan::Keep(Keep::NotProven | Keep::MayHaveCommitted) => {
                        let o = s.owners[r as usize];
                        let owner = match o {
                            Owner::None => core::Owner::Absent,
                            Owner::Tomb => core::Owner::Tomb,
                            Owner::Leader | Owner::Coord { .. } => core::Owner::Held {
                                dead: Ok(self.owner_dead(&s, o)),
                            },
                        };
                        let v = View {
                            orphan: Some(Orphan {
                                in_table: false,
                                promised: has(self.promised(s.table), r),
                                allocated: self.child_facts(&s, r).allocated,
                                owner: Ok(owner),
                            }),
                            ..View::default()
                        };
                        if self.ask(&v, Event::Tomb) == Plan::Tombstone && !self.legacy {
                            s.owners[r as usize] = Owner::Tomb;
                        }
                    }
                    _ => {}
                }
                if s == before {
                    return None;
                }
            }
            Action::LedgerReap(k) => {
                let k = k as usize;
                // Not tied to `gc` (D-d and detach): the ledger only reads the table,
                // tombstones and the child's Db/owner record — the two-op profile (`gc` off) still needs it.
                if k >= self.ops() || s.ops[k].ledger.is_none() {
                    return None;
                }
                // `DeleteProof::settled_op`: ledger, table, then the source's tombstone /
                // the child's Db and owner record. The minimum age is not modelled
                // (always enough): safety must not rely on it.
                let c = self.child(k) as usize;
                let v = View {
                    ledger: Some(Ok(s.ops[k].ledger)),
                    standing: Some(Ok(self.standing(&s, k))),
                    remains: Some(Ok(core::Remains {
                        old: true,
                        mentioned: s.table.intent == Some(k as u8),
                        sources_tombed: self
                            .sources()
                            .iter()
                            .all(|&r| s.owners[r as usize] == Owner::Tomb),
                        children_gone: !s.dbs[c].exists
                            && matches!(s.owners[c], Owner::None | Owner::Tomb),
                    })),
                    ..View::default()
                };
                if self.ask(&v, Event::ReapLedger) != Plan::Prove {
                    return None;
                }
                s.ops[k].ledger = None;
            }
            Action::OrphanDiscard(how) => {
                let (r, version) = s.orphan?;
                if how == Discard::Dies || (how == Discard::Fails && !f.discard) {
                    return None;
                }
                s.orphan = None;
                let refs = s.live_refs;
                let mut g = self.orphan_gate(&s, r, refs);
                g.moved = s.table.version != version;
                let v = View {
                    child: Some(self.child_facts(&s, r)),
                    ledger: Some(Ok(self.ledger_of(&s, r))),
                    gate: Some(Ok(g)),
                    ..View::default()
                };
                if self.ask(&v, Event::Discard(Remover::OrphanRound)) == Plan::DiscardDb
                    && self.gated_discard(&mut s, r, how)
                {
                    s.ghost.orphan_round_discarded = true;
                }
            }
            Action::Fork => {
                if !self.fork || s.fork != Fork::None {
                    return None;
                }
                let stands_on = ids(self.in_table(s.table) & self.existing(&s))
                    .fold(0, |m, l| m | bit(l) | s.dbs[l as usize].external);
                for e in ids(stands_on) {
                    if s.dbs[e as usize].exists {
                        s.dbs[e as usize].checkpoints |= bit(FORK);
                    }
                }
                s.pins[FORK as usize] = stands_on;
                s.fork = Fork::Protected {
                    stands_on,
                    barrier: self.env.fork_barrier,
                };
            }
            Action::ForkExpires => {
                if !matches!(s.fork, Fork::Protected { .. }) {
                    return None;
                }
                s.fork = Fork::Expired;
            }
        }
        Some(s)
    }

    /// A coordinator step fails: the core says roll back in place.
    fn coordinator_fails(&self, s: State, k: usize) -> Option<State> {
        let p = self.ask(&View::default(), Event::Failed);
        self.start_roll_back(s, k, p)
    }

    fn start_roll_back(&self, mut s: State, k: usize, p: Plan) -> Option<State> {
        let Plan::RollBack { own_orphans, .. } = p else {
            unreachable!("{p:?}");
        };
        if !Self::spawn_rb(
            &mut s,
            0,
            RollBacker {
                op: k as u8,
                by: Who::Coordinator,
                node: C,
                own_orphans,
                pc: Rpc::Clear,
                marks: [None; 2],
            },
        ) {
            return None;
        }
        s.ops[k].pc = Pc::RollingBack;
        Some(s)
    }

    /// `discard_own_orphans` for the child of op `k`.
    fn own_orphans(&self, s: &mut State, k: usize) {
        if s.table.intent == Some(k as u8) {
            return;
        }
        let c = self.child(k);
        self.discard_child(s, c, Remover::OwnOrphans, Discard::Done);
    }

    /// The end of a roll-back: the coordinator cleans up its own child and
    /// reopens the quiesced sources.
    fn finish_rb(&self, s: &mut State, rb: RollBacker) {
        let k = rb.op as usize;
        if matches!(rb.by, Who::Coordinator | Who::StrayUndo | Who::Settle) {
            if rb.own_orphans {
                self.own_orphans(s, k);
            }
            if s.ops[k].pc == Pc::RollingBack {
                self.release_sources(s, k);
                s.ops[k].pc = Pc::RolledBack;
            }
        }
    }

    // ─────────────────────────────────────── invariants

    pub fn owner_outlives_db(&self, s: &State) -> bool {
        if self.legacy {
            return true;
        }
        let mut ever = self.ancestors() | self.src();
        if let Some(k) = s.table.committed {
            ever |= bit(self.child(k as usize));
        }
        ids(ever).all(|r| !s.dbs[r as usize].exists || s.owners[r as usize] != Owner::None)
    }

    pub fn no_orphan_checkpoint_on_live(&self, s: &State) -> bool {
        let existing = self.existing(s);
        ids(self.in_table(s.table) & existing)
            .all(|e| s.dbs[e as usize].checkpoints & !existing & !bit(FORK) == 0)
    }

    pub fn no_stranded_owner(&self, s: &State) -> bool {
        (0..self.ops()).all(|k| {
            let c = self.child(k) as usize;
            !(s.ops[k].pc == Pc::RolledBack
                && s.alive[C as usize]
                && s.table.intent != Some(k as u8)
                && s.table.committed != Some(k as u8)
                && matches!(s.owners[c], Owner::Coord { inc } if inc == s.inc)
                && !s.dbs[c].exists)
        })
    }

    /// A settled state: no intent, the children of every op have rolled back
    /// their Db and owner record, the sources of a committed op carry
    /// tombstones, and (a bucket with tombstones) no ledger in a final outcome
    /// remains. A ledger stuck in a middle phase (`Aborting` when the `Cleaned`
    /// write never lands) is not cleaned up — an open issue.
    pub fn settled(&self, s: &State) -> bool {
        if s.table.intent.is_some() {
            return false;
        }
        if !self.legacy
            && s.ops
                .iter()
                .any(|o| matches!(o.ledger, Some(Phase::Committed | Phase::Cleaned)))
        {
            return false;
        }
        (0..self.ops()).all(|k| {
            let c = self.child(k) as usize;
            if s.table.committed == Some(k as u8) {
                self.legacy
                    || self
                        .sources()
                        .iter()
                        .all(|&r| s.owners[r as usize] == Owner::Tomb)
            } else {
                // A proxy tombstone (SM-11) is permanent: a child tombstoned and then
                // its Db deleted by D-d is settled too.
                !s.dbs[c].exists && matches!(s.owners[c], Owner::None | Owner::Tomb)
            }
        })
    }

    fn all_actions(&self, out: &mut Vec<Action>) {
        let all: Vec<RangeId> = self.ranges().collect();
        for k in 0..self.ops() as u8 {
            out.extend([
                Action::IntentCas(k, false),
                Action::IntentCas(k, true),
                Action::Stray(k, false),
                Action::Stray(k, true),
                Action::Quiesce(k),
                Action::Close(k),
                Action::Clone(k),
                Action::Claim(k),
                Action::Prepare(k),
                Action::CommitCas(k),
                Action::Retire(k),
                Action::Fail(k),
                Action::Vanish(k),
                Action::SecondHold(k),
            ]);
        }
        out.extend([
            Action::Crash,
            Action::Restart,
            Action::Settle,
            Action::Reconcile,
            Action::Ctl(false),
            Action::Ctl(true),
            Action::XLearn,
            Action::GcLoadTable,
            Action::GcPlan(None),
            Action::LedgerReap(0),
            Action::LedgerReap(1),
            Action::Fork,
            Action::ForkExpires,
            Action::OrphanDiscard(Discard::Done),
            Action::OrphanDiscard(Discard::Fails),
        ]);
        for slot in 0..2 {
            out.push(Action::RbClear(slot, false));
            out.push(Action::RbClear(slot, true));
            for how in [Discard::Done, Discard::Fails, Discard::Dies] {
                out.push(Action::RbCleanup(slot, how));
            }
        }
        for &r in &all {
            for node in [C, X] {
                out.push(Action::Open(node, r, false));
                out.push(Action::Open(node, r, true));
            }
            out.push(Action::LeftTable(r));
            out.push(Action::Ack(r));
            out.push(Action::LeaseLoss(r));
            out.push(Action::GcPlan(Some(r)));
            out.push(Action::GcDelete(r, None));
            for &l in &all {
                out.push(Action::GcDelete(r, Some(l)));
            }
            out.push(Action::OrphanCheck(r));
            for &e in &all {
                if e != r {
                    out.push(Action::Detach(r, e));
                }
            }
        }
    }

    /// A liveness counterexample: a (shortest) path to a state from which
    /// **system actions** ([`Action::is_system`]) never lead back to
    /// [`settled`] — a stuck intent, a leaking orphan child, a source without a
    /// tombstone. `None` = every reachable state can settle by itself.
    #[allow(
        clippy::disallowed_types,
        reason = "lookups by state only; the exploration order comes from a Vec, not from the HashMap"
    )]
    pub fn stuck(&self) -> Option<Vec<Action>> {
        use std::collections::{HashMap, VecDeque};
        let init = self.init_states().remove(0);
        let mut index: HashMap<State, usize> = HashMap::new();
        let mut states: Vec<State> = vec![init.clone()];
        let mut parent: Vec<Option<(usize, Action)>> = vec![None];
        let mut sys_rev: Vec<Vec<usize>> = vec![Vec::new()];
        index.insert(init, 0);
        let mut queue = VecDeque::from([0usize]);
        let mut acts = Vec::new();
        self.all_actions(&mut acts);
        while let Some(i) = queue.pop_front() {
            // States carrying a ghost variable are still explored here (safety is
            // the job of `properties`): the path to a settled state may pass through one.
            let s = states[i].clone();
            for &a in &acts {
                let Some(t) = self.step(&s, a) else { continue };
                let j = match index.get(&t) {
                    Some(&j) => j,
                    None => {
                        let j = states.len();
                        index.insert(t.clone(), j);
                        states.push(t);
                        parent.push(Some((i, a)));
                        sys_rev.push(Vec::new());
                        queue.push_back(j);
                        j
                    }
                };
                if a.is_system() && i != j {
                    sys_rev[j].push(i);
                }
            }
        }
        // Walk backwards from the settled states along system edges.
        let mut good = vec![false; states.len()];
        let mut work: Vec<usize> = (0..states.len())
            .filter(|&i| self.settled(&states[i]))
            .collect();
        for &i in &work {
            good[i] = true;
        }
        while let Some(j) = work.pop() {
            for &i in &sys_rev[j] {
                if !good[i] {
                    good[i] = true;
                    work.push(i);
                }
            }
        }
        // The first state (in BFS order — the shortest path) that cannot settle.
        let bad = (0..states.len()).find(|&i| {
            let s = &states[i];
            !good[i] && !(s.ghost.bad_delete || s.ghost.bad_commit || s.ghost.lost_ack)
        })?;
        let mut path = Vec::new();
        let mut at = bad;
        while let Some((p, a)) = parent[at] {
            path.push(a);
            at = p;
        }
        path.reverse();
        Some(path)
    }
}

impl Model for M2 {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.range_count();
        let mut dbs = [Db::default(); SLOTS];
        let mut owners = [Owner::None; SLOTS];
        let mut lease = [None; SLOTS];
        let mut pins = [0; 8];
        dbs[ANCESTOR as usize] = Db {
            exists: true,
            checkpoints: self.src(),
            unnamed: if self.unnamed_history { self.src() } else { 0 },
            closed: true,
            ..Db::default()
        };
        if !self.legacy {
            owners[ANCESTOR as usize] = Owner::Tomb;
        }
        if self.deep {
            // `Q` was split off `G`: `Q` stands on `G` (`Q`'s final checkpoint),
            // and each source — a clone of `Q` — carries the `G` entry copied from
            // `Q` with its own final checkpoint on `G`.
            let on_g = bit(ANCESTOR) | self.src();
            dbs[GRANDPARENT as usize] = Db {
                exists: true,
                checkpoints: on_g,
                unnamed: if self.unnamed_history { on_g } else { 0 },
                closed: true,
                ..Db::default()
            };
            dbs[ANCESTOR as usize].external = bit(GRANDPARENT);
            pins[ANCESTOR as usize] = bit(GRANDPARENT);
            if !self.legacy {
                owners[GRANDPARENT as usize] = Owner::Tomb;
            }
        }
        for &r in self.sources() {
            dbs[r as usize] = Db {
                exists: true,
                external: self.ancestors(),
                writer: true,
                ..Db::default()
            };
            if self.deep {
                pins[r as usize] = self.ancestors();
            }
            lease[r as usize] = Some(C);
            if !self.legacy {
                owners[r as usize] = Owner::Leader;
            }
        }
        let _ = n;
        let table = Table {
            version: 1,
            intent: None,
            committed: None,
        };
        let op = OpState {
            pc: Pc::NotStarted,
            ledger: None,
            allocated: false,
            held: false,
            quiesced: false,
            sealed: [0; 2],
            cut: false,
            dirty: false,
        };
        vec![State {
            table,
            ops: [op; 2],
            dbs,
            owners,
            lease,
            qengine: [false; SLOTS],
            alive: [true, true],
            inc: 0,
            x_ram: table,
            rbs: [None; 2],
            settle: None,
            unheld: None,
            gc: Gc::Idle,
            live_refs: None,
            pins,
            orphan: None,
            fork: Fork::None,
            spent: Spent::default(),
            ghost: Ghost::default(),
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        // A stuck ghost variable is the end of the road: going on only makes the counterexample longer.
        if s.ghost.bad_delete || s.ghost.bad_commit || s.ghost.lost_ack {
            return;
        }
        self.all_actions(out);
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        self.step(s, a)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut p = vec![
            Property::always(NO_DELETE_UNDER_LIVE, |_, s: &State| !s.ghost.bad_delete),
            Property::always(NO_COMMIT_OF_DISCARDED_CHILD, |_, s: &State| {
                !s.ghost.bad_commit
            }),
            Property::always(NO_LOST_ACK, |_, s: &State| !s.ghost.lost_ack),
            Property::always(SM14_OWNER_OUTLIVES_DB, |m: &M2, s: &State| {
                m.owner_outlives_db(s)
            }),
            Property::always(NO_ORPHAN_CHECKPOINT_ON_LIVE, |m: &M2, s: &State| {
                m.no_orphan_checkpoint_on_live(s)
            }),
            Property::always(D5_NO_STRANDED_OWNER, |m: &M2, s: &State| {
                m.no_stranded_owner(s)
            }),
            Property::always(NO_SELF_ROLL_BACK, |_, s: &State| !s.ghost.self_roll_back),
            Property::always(NO_TOMB_ON_LIVE, |_, s: &State| !s.ghost.tomb_on_live),
            Property::sometimes(SOMETIMES_PARENT_REAPED, |m: &M2, s: &State| {
                s.table.committed.is_some()
                    && m.sources().iter().all(|&r| !s.dbs[r as usize].exists)
            }),
            Property::sometimes(SOMETIMES_ANCESTOR_REAPED, |_, s: &State| {
                !s.dbs[ANCESTOR as usize].exists
            }),
            Property::sometimes(SOMETIMES_GRANDPARENT_REAPED, |m: &M2, s: &State| {
                !m.deep || !s.dbs[GRANDPARENT as usize].exists
            }),
            Property::sometimes(SOMETIMES_ROLLED_BACK_CLEAN, |m: &M2, s: &State| {
                let c = m.child(0);
                s.table.committed.is_none()
                    && s.table.intent.is_none()
                    && s.ghost.child_cloned
                    && !s.dbs[c as usize].exists
                    && s.owners[c as usize] == Owner::None
                    && m.sources()
                        .iter()
                        .all(|&r| s.dbs[r as usize].checkpoints & bit(c) == 0)
            }),
            Property::sometimes(SOMETIMES_ORPHAN_ROUND_DISCARDS, |_, s: &State| {
                s.ghost.orphan_round_discarded
            }),
            Property::sometimes(SOMETIMES_REAPED_AFTER_MOVE, |_, s: &State| {
                s.ghost.reaped_after_move
            }),
            Property::sometimes(SOMETIMES_REAPED_ON_ROUND_REFS, |_, s: &State| {
                s.ghost.reaped_on_round_refs
            }),
        ];
        if !self.bugs.no_ledger && !self.bugs.no_roll_forward {
            p.push(Property::sometimes(
                SOMETIMES_ROLLED_FORWARD,
                |_, s: &State| s.ghost.rolled_forward,
            ));
        }
        p
    }
}
