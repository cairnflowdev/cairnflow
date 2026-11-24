// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flags that plant historical bugs into the core.
//!
//! Present only with the `mutations` feature (M2 enables it) and in this
//! crate's own tests. The product build sees an empty [`Bugs`]: every `bug!`
//! branch becomes a constant `false`.
//!
//! Three kinds of flags, all **fixed** behaviour — turned on, the core behaves
//! like the code before the fix:
//! - **historical bugs of the split/merge group** (from the bug log): one bug
//!   per flag, M2 must go red (sometimes only with the layers added after it —
//!   noted on each flag and in `tests/m2.rs::matrix`);
//! - **fence layers added later** that a historical bug needs turned off as
//!   well to show (the SM-17 close mark, the SM-3 order, the D-d rechecks, …);
//! - **product behaviour before the op ledger and before the core read it**
//!   ([`Bugs::BEFORE_3_1`], [`Bugs::BEFORE_3_3`]): no op ledger yet, or not read
//!   yet, `cairn-ctl intent clear` removing the close mark unconditionally.
//!
//! [`Bugs::PRODUCTION`] is the shell's behaviour today — the core itself.

#[cfg(any(test, feature = "mutations"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Bugs {
    // ── historical bugs of the split/merge group
    /// **D1**: reloading the table failed at Db open and the open went on with
    /// the RAM table — reopening a parent range that had left the table.
    pub d1_stale_table: bool,
    /// **D2**: the open did not wait for the "quiesced" mark of a coordinator
    /// on the same node (SM-13).
    pub d2_ignores_quiesced: bool,
    /// **D5**: the coordinator did not require the intent to still be its own
    /// before the child-creation step, and did not clean up its own children
    /// after rolling back.
    pub d5_recreates_children: bool,
    /// **R1-i** (SM-14): the orphan-child cleanup round trusted only the two
    /// "never entered the table" proofs — no direct check of references, fences
    /// or version; the `abandoned_child_db` gate checked only "permanently out
    /// of the table".
    pub r1i_trusts_history: bool,
    /// **SM-11**: nobody wrote a proxy tombstone for a range that left the table
    /// when its owner died before writing one.
    pub sm11_no_proxy_tomb: bool,
    /// **SM-11 (2)** (M2): the tombstone / orphan-child cleanup round did not
    /// skip promised children.
    pub sm11_ignores_promised: bool,
    /// **L1**: the operation died with its request — a vanished task did not
    /// release the quiesced source, and there was no reconciliation round.
    pub l1_dies_with_request: bool,
    /// **L3**: a torn intent CAS whose re-read also failed gave up.
    pub l3_stray_gives_up: bool,
    /// **L4**: the coordinator's failed roll-back gave up (logged only).
    pub l4_roll_back_gives_up: bool,
    /// **Failed hold**: the guard of a failed `hold` removed the holder's mark.
    pub l13_failed_hold_releases: bool,
    /// **R6.3-1**: removing the close mark without an epoch condition — a slow
    /// roll-back removed the mark of a later op.
    pub r631_reopen_any: bool,
    /// **R6.3-6**: `reconcile` retired a range per the RAM table (which may have
    /// been learned from a peer's `409` body) without reading the bucket.
    pub r636_retire_on_ram: bool,
    /// **Mixed snapshots** (found by M2): D-d filtered candidates by one table
    /// and took the reference set by another.
    pub dd_mixed_snapshots: bool,
    /// **Orphan Db after two kills** (R1-f): the orphan-child cleanup round had
    /// only the first proof ("standing on a live range") — an orphan child
    /// whose source had retired stayed forever.
    pub r1f_first_proof_only: bool,

    // ── fence layers added later
    /// Before the close mark: no close mark on the source Db (SM-17).
    pub before_close_mark: bool,
    /// SM-3 reversed: the roller-back cleans up children **before** the CAS
    /// dropping the intent.
    pub sm3_discard_first: bool,
    /// The owner-record half of D4: cleaning up a broken Db still deletes the
    /// owner record.
    pub d4_owner_before_db: bool,
    /// Before the version recheck: D-d did not compare the table version right
    /// before deleting.
    pub dd_no_version_recheck: bool,
    /// The earlier shell: a table that changed since the plan's snapshot ⇒ D-d
    /// abandoned **the whole round** (the shell compared the proof's version
    /// with the plan's version). It does not break safety; it breaks liveness:
    /// under continuous auto-splitting nearly every round was abandoned at the
    /// first candidate (found in a 4 h soak) — M2:
    /// `SOMETIMES_REAPED_AFTER_MOVE` is unreachable.
    pub dd_abandons_round: bool,
    /// Before the proof recheck: D-d did not rebuild the proof (out of the
    /// table, not promised, tombstone, references) right before deleting.
    pub dd_no_proof_recheck: bool,
    /// N2: D-d took a reference set that did not add children in flight.
    pub dd_ignores_in_flight: bool,
    /// N4: the orphan-child cleanup round took the reference set of another
    /// table version.
    pub n4_any_refs_version: bool,
    /// The "unknown means keep" rule broken: an "unknown" reference set taken
    /// as referenced by nobody.
    pub unknown_refs_as_clear: bool,
    /// Before the checkpoint fence: D-d did not read the final checkpoint on
    /// the retired Db's own manifest — it trusted only the reference-set scan.
    pub dd_no_checkpoint_fence: bool,
    /// The checkpoint fence takes an unnamed final checkpoint (buckets from
    /// before the checkpoint-naming patch, G15) as nobody standing on it — on
    /// such a bucket only the scan keeps it safe.
    pub dd_unnamed_as_clear: bool,

    // ── product behaviour before the op ledger / before the core read it
    /// Before the op ledger: no ledger — nothing written, reads find it absent.
    pub no_ledger: bool,
    /// Before the core read the ledger: the source claimer and the
    /// reconciliation round did not read the ledger — they only rolled back,
    /// even an op that had finished preparing.
    pub no_roll_forward: bool,
    /// Before the core read the ledger: the child cleanup paths did not
    /// consider the ledger — `Cleaned` was not a proof, `Prepared`/`Committed`
    /// did not block.
    pub ledger_not_proof: bool,
    /// Before the core read the ledger: `cairn-ctl intent clear` removed the
    /// close mark unconditionally, without reading the mark before the CAS
    /// (the same shape as R6.3-1).
    pub ctl_reopen_any: bool,
    /// Before the fix: `cairn-ctl intent clear` rolled back without reading
    /// the ledger — even an op at `Prepared` whose roll-forward check passes,
    /// i.e. whose commit may already have won (the table rebuilt by `table
    /// restore` from a history version older than the commit): rolling back
    /// puts the Db of a child that has accepted writes into the trash.
    pub ctl_clears_prepared: bool,

    // ── cleaning up the op ledger
    /// Before ledger cleanup: the `ops/<key>` ledger was never deleted (one
    /// object per op, leaking forever).
    pub ledger_kept_forever: bool,
    /// The ledger cleanup round only requires the ledger to be at an outcome
    /// and old enough — not that the source carries a tombstone (`Committed`)
    /// or that the children have no Db and owner record left (`Cleaned`).
    pub ledger_reaps_unsettled: bool,
}

#[cfg(any(test, feature = "mutations"))]
impl Bugs {
    pub const NONE: Bugs = Bugs {
        d1_stale_table: false,
        d2_ignores_quiesced: false,
        d5_recreates_children: false,
        r1i_trusts_history: false,
        sm11_no_proxy_tomb: false,
        sm11_ignores_promised: false,
        l1_dies_with_request: false,
        l3_stray_gives_up: false,
        l4_roll_back_gives_up: false,
        l13_failed_hold_releases: false,
        r631_reopen_any: false,
        r636_retire_on_ram: false,
        dd_mixed_snapshots: false,
        r1f_first_proof_only: false,
        before_close_mark: false,
        sm3_discard_first: false,
        d4_owner_before_db: false,
        dd_no_version_recheck: false,
        dd_abandons_round: false,
        dd_no_proof_recheck: false,
        dd_ignores_in_flight: false,
        n4_any_refs_version: false,
        unknown_refs_as_clear: false,
        dd_no_checkpoint_fence: false,
        dd_unnamed_as_clear: false,
        no_ledger: false,
        no_roll_forward: false,
        ledger_not_proof: false,
        ctl_reopen_any: false,
        ctl_clears_prepared: false,
        ledger_kept_forever: false,
        ledger_reaps_unsettled: false,
    };

    /// The product before the core read the ledger: the ledger was written but
    /// nobody read it to decide.
    pub const BEFORE_3_3: Bugs = Bugs {
        no_roll_forward: true,
        ledger_not_proof: true,
        ctl_reopen_any: true,
        ctl_clears_prepared: true,
        ..Bugs::NONE
    };

    /// The product before the op ledger: no op ledger yet.
    pub const BEFORE_3_1: Bugs = Bugs {
        no_ledger: true,
        ..Bugs::BEFORE_3_3
    };

    /// The shell's behaviour today: the shell (`cairn-server/src/split/`, the
    /// orphan round and D-d of `gc.rs`, `factory`, `ctl`, the gate in
    /// `proof.rs`) calls [`crate::step`] itself — no flag
    /// (`tests::production_is_the_core`).
    pub const PRODUCTION: Bugs = Bugs::NONE;
}

/// Product build: no flag.
#[cfg(not(any(test, feature = "mutations")))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Bugs;

#[cfg(not(any(test, feature = "mutations")))]
impl Bugs {
    pub(crate) const NONE: Bugs = Bugs;
}

/// `bug!(b.flag)` — a constant `false` when there are no flags.
macro_rules! bug {
    ($b:ident . $f:ident) => {{
        #[cfg(any(test, feature = "mutations"))]
        let on = $b.$f;
        #[cfg(not(any(test, feature = "mutations")))]
        let on = {
            let _ = $b;
            false
        };
        on
    }};
}
pub(crate) use bug;
