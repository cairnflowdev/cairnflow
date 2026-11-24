// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Acceptance tests of M2 on the split/merge core **itself**: the core without
//! flags ⇒ exhaustive search, every safety invariant holds and the system can
//! settle by itself; each historical bug re-injected into the core
//! (`cairn_split::Bugs`) ⇒ a counterexample; the matrix pins which flag (with
//! which layers) breaks which invariant.
//!
//! The state space with every injected failure at once is too large (~10⁹), so
//! each question runs on a failure **profile** ([`Faults`]) enough for its path;
//! "all fences hold" runs on each profile alone and a few pairs. The
//! counterexamples printed (`--nocapture`) are the paths the BFS found — short,
//! not necessarily the shortest.

use {
    cairn_model::m2::{self, Action, Discard, Env, Faults, M2},
    cairn_split::Bugs,
    stateright::{Checker, Expectation, HasDiscoveries, Model},
};

/// Pseudo-name for liveness ([`M2::stuck`]).
const STUCK: &str = "liveness: the system settles every op on its own";

fn report(name: &str, c: &impl Checker<M2>) {
    println!(
        "{name}: {} unique states, depth {}, done = {}",
        c.unique_state_count(),
        c.max_depth(),
        c.is_done()
    );
}

fn check_all(name: &str, model: &M2) -> impl Checker<M2> {
    let c = model
        .clone()
        .checker()
        .finish_when(HasDiscoveries::AllFailures)
        .spawn_bfs()
        .join();
    report(name, &c);
    assert!(c.is_done(), "{name}: the BFS must be exhaustive");
    c
}

/// Safety invariants with a counterexample, plus [`STUCK`] if the system cannot settle by itself.
fn broken(name: &str, model: &M2) -> Vec<&'static str> {
    let c = check_all(name, model);
    let mut out: Vec<&'static str> = model
        .properties()
        .into_iter()
        .filter(|p| p.expectation == Expectation::Always && c.discovery(p.name).is_some())
        .map(|p| p.name)
        .collect();
    if let Some(path) = model.stuck() {
        println!("  {name}: stuck after {path:?}");
        out.push(STUCK);
    }
    out
}

fn holds(name: &str, model: &M2) {
    assert_eq!(broken(name, model), Vec::<&str>::new(), "{name}");
}

/// A counterexample of `property` (or a path to a stuck state for [`STUCK`]).
fn counterexample(name: &str, model: &M2, property: &'static str) -> Vec<Action> {
    let path = if property == STUCK {
        model.stuck()
    } else {
        let c = model
            .clone()
            .checker()
            .finish_when(HasDiscoveries::AnyOf([property].into()))
            .spawn_bfs()
            .join();
        report(name, &c);
        c.discovery(property).map(|d| d.into_actions())
    };
    let path = path.unwrap_or_else(|| panic!("{name}: no counterexample for \"{property}\""));
    println!("{name} — \"{property}\" ({} steps):", path.len());
    for (i, a) in path.iter().enumerate() {
        println!("  {:>2}. {a:?}", i + 1);
    }
    path
}

fn pos(path: &[Action], want: impl Fn(&Action) -> bool) -> usize {
    path.iter()
        .position(want)
        .unwrap_or_else(|| panic!("missing step in {path:?}"))
}

fn bugs(f: impl Fn(&mut Bugs)) -> Bugs {
    let mut b = Bugs::NONE;
    f(&mut b);
    b
}

// ───────────────────────────────────────── profiles

const NONE: Faults = Faults::NONE;
/// The source changes owner, a fresh table read fails, a write is acked (SM-13, SM-17).
const OPEN: Faults = Faults {
    lease_loss: true,
    stale_reads: true,
    acks: true,
    ..Faults::NONE
};
const CRASH: Faults = Faults {
    crash: true,
    ..Faults::NONE
};
const VANISH: Faults = Faults {
    vanish: true,
    ..Faults::NONE
};
const AMBIGUOUS: Faults = Faults {
    ambiguous: true,
    ..Faults::NONE
};
const DISCARD: Faults = Faults {
    discard: true,
    ..Faults::NONE
};

fn split(faults: Faults, bugs: Bugs) -> M2 {
    M2 {
        faults,
        ..M2::split(bugs)
    }
}

fn merge(faults: Faults, bugs: Bugs) -> M2 {
    M2 {
        faults,
        ..M2::merge(bugs)
    }
}

fn legacy(m: M2) -> M2 {
    M2 { legacy: true, ..m }
}

/// Two ops one after another on the same sources (no D-d, no fork — smaller).
fn second(faults: Faults, bugs: Bugs) -> M2 {
    M2 {
        second: true,
        gc: false,
        fork: false,
        ..split(faults, bugs)
    }
}

// ───────────────────────────────────────── (a) the core without flags

/// (a) The core without flags (`Bugs::NONE`): on each failure profile,
/// exhaustive, no safety counterexample, the system settles from every state.
#[test]
fn the_core_holds_for_split() {
    for (name, f) in [
        ("no failures", NONE),
        ("node dies", CRASH),
        ("source changes owner, failed read, ack", OPEN),
        ("task dies", VANISH),
        ("ambiguous CAS, failed roll-back", AMBIGUOUS),
        (
            "node dies, failed cleanup",
            Faults {
                discard: true,
                ..CRASH
            },
        ),
        (
            "peer's RAM table, second hold, owner change",
            Faults {
                ram: true,
                lease_loss: true,
                ..Faults::NONE
            },
        ),
    ] {
        holds(&format!("split, {name}"), &split(f, Bugs::NONE));
    }
}

#[test]
fn the_core_holds_for_merge() {
    for (name, f) in [
        ("no failures", NONE),
        ("task dies", VANISH),
        ("failed cleanup", DISCARD),
    ] {
        holds(&format!("merge, {name}"), &merge(f, Bugs::NONE));
    }
}

/// (a') A bucket a single-process node built before the range-tier migration
/// framework (no owner records, no tombstones).
#[test]
fn the_core_holds_on_a_pre_migration_bucket() {
    holds("split, pre-migration", &legacy(split(NONE, Bugs::NONE)));
    holds(
        "split, pre-migration, node dies",
        &legacy(split(CRASH, Bugs::NONE)),
    );
    holds("merge, pre-migration", &legacy(merge(NONE, Bugs::NONE)));
}

/// (a'') A second op on the same sources after a roll-back, including `cairn-ctl
/// intent clear`.
#[test]
fn the_core_holds_across_two_ops() {
    holds(
        "two ops, owner change, failed read, ack, task dies",
        &second(
            Faults {
                vanish: true,
                ..OPEN
            },
            Bugs::NONE,
        ),
    );
    holds(
        "two ops, task dies, failed cleanup",
        &second(
            Faults {
                discard: true,
                ..VANISH
            },
            Bugs::NONE,
        ),
    );
    holds(
        "two ops, ctl",
        &M2 {
            ctl: true,
            ..second(
                Faults {
                    vanish: true,
                    ..OPEN
                },
                Bugs::NONE,
            )
        },
    );
}

/// A lineage two deep: ancestor `Q` itself stands on ancestor `G`
/// ([`m2::GRANDPARENT`]).
fn deep(m: M2) -> M2 {
    M2 { deep: true, ..m }
}

/// (a''') **A lineage two deep** (M2 used to have a single ancestor): the
/// sources stand on `{Q, G}`, the op's child stands on `{source, Q, G}`, detach
/// removes each entry independently. The core without flags keeps every
/// invariant, the system settles by itself, and D-d reaches the state where `G`
/// is deleted too (the child stands on its own, then D-d cleans up the whole chain).
#[test]
fn the_core_holds_on_a_two_deep_lineage() {
    for (name, m) in [
        ("deep split, no failures", deep(split(NONE, Bugs::NONE))),
        (
            "deep split, source changes owner, failed read, ack",
            deep(split(OPEN, Bugs::NONE)),
        ),
        ("deep split, node dies", deep(split(CRASH, Bugs::NONE))),
        ("deep merge, no failures", deep(merge(NONE, Bugs::NONE))),
        (
            "deep split, blind scan, unnamed bucket",
            deep(unnamed(blind(split(NONE, Bugs::NONE)))),
        ),
    ] {
        holds(name, &m);
    }
    check_all("deep split, task dies", &deep(split(VANISH, Bugs::NONE))).assert_properties();
}

/// The model is not empty: a split finishes, then D-d deletes the parent; D-d
/// deletes the ancestor; a roll-back finishes and the child is cleaned up; the
/// orphan-child round cleans up a Db; **a prepared op is rolled forward** when its
/// coordinator vanishes.
#[test]
fn every_sometimes_is_reachable() {
    check_all("split, task dies", &split(VANISH, Bugs::NONE)).assert_properties();
}

/// **D-d under table changes** (found in a 4 h soak): D-d abandoned **the whole
/// round** when the table changed version between plan and proof; under
/// continuous auto split (the table changed every ~31 s on average, a plan plus
/// one proof took ~30–40 s) most rounds gave up at the first candidate.
///
/// Must see: a round whose table changed since its plan can still delete a Db
/// (`SOMETIMES_REAPED_AFTER_MOVE`) — the proof stands on its own snapshot. Flag
/// `dd_abandons_round` (the earlier shell) ⇒ no such round, while safety is
/// unchanged. M2 has only finitely many table changes, so "abandon the whole
/// round" still settles in a later round (not `STUCK`); this property is the
/// part of liveness that can be expressed.
#[test]
fn dd_reaps_in_a_round_whose_table_moved() {
    let c = check_all("split, no failures", &split(NONE, Bugs::NONE));
    let path = c
        .discovery(m2::SOMETIMES_REAPED_AFTER_MOVE)
        .map(|d| d.into_actions())
        .expect("no D-d round deleted anything after the table changed since its plan");
    println!(
        "delete after the table changed ({} steps): {path:?}",
        path.len()
    );
    let plan = pos(&path, |a| matches!(a, Action::GcPlan(_)));
    let delete = pos(&path, |a| matches!(a, Action::GcDelete(_, _)));
    assert!(plan < delete, "{path:?}");

    let old = split(NONE, bugs(|b| b.dd_abandons_round = true));
    let c = check_all("split, no failures, abandoning shell", &old);
    assert!(
        c.discovery(m2::SOMETIMES_REAPED_AFTER_MOVE).is_none(),
        "the earlier shell abandons the whole round when the table changes — no such round can delete"
    );
    holds("split, no failures, abandoning shell", &old);
}

// ───────────────────────────────────────── (b) the product

/// The product behaviour before ledgers (`Bugs::PRODUCTION` = before ledgers)
/// and with ledgers but before the shell called the core keeps every safety
/// invariant — the core only adds, it fixes no safety bug of the normal path.
#[test]
fn production_is_safe() {
    for b in [Bugs::PRODUCTION, Bugs::BEFORE_3_3] {
        for (name, f) in [
            ("node dies", CRASH),
            ("source changes owner", OPEN),
            ("task dies", VANISH),
        ] {
            holds(&format!("product {b:?}, {name}"), &split(f, b));
        }
    }
}

/// **A finding of running M2 on the core** (since fixed): before the op ledger,
/// a **claimed** child of a roll-back that died half-way, whose source later
/// retired (a later op committed), was never cleaned up while the coordinator
/// was alive: it stands on no live range, has an owner record, its owner is not
/// dead — SM-14 cannot prove it, SM-11 keeps `ownerLive` (the "not covered" item
/// of SM-14). The `Cleaned` ledger is the positive proof.
///
/// Must see: op 0 claims the child, fails, CASes the intent away, the
/// roll-backer dies before cleaning up; op 1 commits; from there the system
/// cannot settle by itself. The core (reading the ledger) settles.
#[test]
fn before_the_ledger_a_claimed_orphan_outlives_its_retired_sources() {
    let m = second(
        Faults {
            discard: true,
            ..VANISH
        },
        Bugs::BEFORE_3_3,
    );
    let path = counterexample("two ops, before the core", &m, STUCK);
    let claim = pos(&path, |a| *a == Action::Claim(0));
    let dies = pos(&path, |a| matches!(a, Action::RbCleanup(_, Discard::Dies)));
    let commit = pos(&path, |a| *a == Action::CommitCas(1));
    assert!(claim < dies && dies < commit, "{path:?}");
    assert_eq!(broken("two ops, before the core", &m), [STUCK]);
    holds(
        "two ops, core",
        &second(
            Faults {
                discard: true,
                ..VANISH
            },
            Bugs::NONE,
        ),
    );
}

/// **Ledger cleanup**: deleting the ledger of a settled op reads back as "no
/// ledger" (the behaviour before ledgers) and no path misreads it — the core
/// holds on two ops (with the ledger cleanup round, action `LedgerReap`, no
/// minimum age). Cleaning up a `Cleaned` ledger **before** the child has lost
/// its Db and owner record brings exactly the finding above back: op 0 claims
/// the child, fails, CASes the intent away (ledger `Cleaned`), the ledger is
/// cleaned up, the roll-backer dies before cleaning up the child; op 1 commits;
/// the child is never cleaned up.
#[test]
fn reaping_an_unsettled_ledger_strands_a_claimed_orphan() {
    let kd = Faults {
        discard: true,
        ..VANISH
    };
    holds("two ops, ledger cleanup", &second(kd, Bugs::NONE));
    let m = second(kd, bugs(|b| b.ledger_reaps_unsettled = true));
    let path = counterexample("two ops, unsettled ledger cleanup", &m, STUCK);
    let claim = pos(&path, |a| *a == Action::Claim(0));
    let reap = pos(&path, |a| *a == Action::LedgerReap(0));
    let commit = pos(&path, |a| *a == Action::CommitCas(1));
    assert!(claim < reap && reap < commit, "{path:?}");
}

/// **A finding of running M2 on the core** (since fixed): `cairn-ctl intent
/// clear` removed closed marks **unconditionally**, without reading the marks
/// before the CAS — the same shape the node's roll-back path had already fixed.
/// A later op closes the source in the gap between the CAS and the removal; ctl's
/// removal deletes that mark. On its own SM-13 and the fresh table hold it up;
/// together with opening from a stale RAM table an ack is lost.
#[test]
fn ctl_reopen_without_epoch_needs_only_a_stale_table_to_lose_acks() {
    let m = |b| M2 {
        ctl: true,
        ..second(
            Faults {
                vanish: true,
                ..OPEN
            },
            b,
        )
    };
    let d1 = |b: Bugs| Bugs {
        d1_stale_table: true,
        ..b
    };
    let path = counterexample(
        "ctl before the core + stale table",
        &m(d1(Bugs::BEFORE_3_3)),
        m2::NO_LOST_ACK,
    );
    assert!(path.iter().any(|a| matches!(a, Action::Ctl(_))), "{path:?}");
    holds("ctl before the core", &m(Bugs::BEFORE_3_3));
    holds("ctl core + stale table", &m(d1(Bugs::NONE)));
}

/// **`cairn-ctl intent clear` reads the ledger** and refuses a prepared op whose
/// roll-forward check passes. M2 has no `table restore` (the table never goes
/// back to a version older than a commit that won), so in the model the intent
/// ctl sees always belongs to an uncommitted op, and both rules keep every
/// invariant — flag `ctl_clears_prepared` pins that the new rule breaks nothing;
/// the data-loss case is in
/// `tests/failpoints.rs::table_restore_rolls_forward_a_prepared_split_whose_commit_won`
/// of `cairn-server`.
#[test]
fn ctl_refusing_a_prepared_op_keeps_the_core() {
    let m = |b| M2 {
        ctl: true,
        ..second(
            Faults {
                vanish: true,
                ..OPEN
            },
            b,
        )
    };
    holds("ctl reads the ledger", &m(Bugs::NONE));
    holds(
        "ctl does not read the ledger (earlier)",
        &m(Bugs {
            ctl_clears_prepared: true,
            ..Bugs::NONE
        }),
    );
}

// ───────────────────────────────────────── (c) historical bugs

/// **Opening from a stale RAM table**: reloading the table fails when opening a
/// Db, and the opener goes on with the old RAM table. The closed mark on the Db
/// itself (SM-17) holds it up alone: a committed parent carries the flag forever.
///
/// Must see (before SM-17): `X` holds an old table, the reload fails, it opens
/// the Db of a source that has left the table (or is quiescing), acks — no child
/// has that write.
#[test]
fn d1_stale_table_opens_a_source_whose_children_never_see_the_write() {
    let d1 = bugs(|b| b.d1_stale_table = true);
    holds("stale table, with closed mark", &split(OPEN, d1));
    let before = Bugs {
        before_close_mark: true,
        ..d1
    };
    let path = counterexample("stale, pre-SM-17", &split(OPEN, before), m2::NO_LOST_ACK);
    assert!(
        path.iter()
            .any(|a| matches!(a, Action::Open(m2::X, 1, true))),
        "{path:?}"
    );
    assert!(matches!(
        path.last(),
        Some(Action::Ack(1) | Action::CommitCas(0))
    ));
}

/// **Reopening a quiesced source**: the source engine loses its lease after
/// quiesce, the coordinating node itself claims it back and opens the Db without
/// waiting for the "quiesced" mark (SM-13). The epoch-conditioned closed mark
/// holds it up: the open bumps the epoch, the close command fails, the
/// coordinator rolls back.
///
/// Must see (before SM-17): quiesce, lease lost, `C` reopens, ack, commit.
#[test]
fn d2_reopened_quiesced_source_loses_acks_when_the_op_commits() {
    let d2 = bugs(|b| b.d2_ignores_quiesced = true);
    holds("reopened source, with closed mark", &split(OPEN, d2));
    let before = Bugs {
        before_close_mark: true,
        ..d2
    };
    let path = counterexample("reopen, pre-SM-17", &split(OPEN, before), m2::NO_LOST_ACK);
    let quiesce = pos(&path, |a| *a == Action::Quiesce(0));
    let open = pos(&path, |a| matches!(a, Action::Open(m2::C, 1, _)));
    assert!(quiesce < open, "{path:?}");
}

/// **A stranded owner record**: a coordinator rolled back by someone else wakes
/// up and recreates the child.
///
/// Must see: the source changes owner, `X` rolls the op back on its behalf; the
/// (still live) coordinator claims the child afterwards, rolls back — the owner
/// record of the Db-less child points at its live session, and that child leaks
/// (SM-11 keeps `ownerLive`).
#[test]
fn d5_coordinator_rolled_back_by_another_strands_an_owner_record() {
    let m = split(
        Faults {
            lease_loss: true,
            ..NONE
        },
        bugs(|b| b.d5_recreates_children = true),
    );
    let path = counterexample("D5", &m, m2::D5_NO_STRANDED_OWNER);
    let foreign = pos(&path, |a| matches!(a, Action::RbClear(1, false)));
    let claim = pos(&path, |a| *a == Action::Claim(0));
    assert!(foreign < claim, "{path:?}");
    assert_eq!(broken("D5", &m), [m2::D5_NO_STRANDED_OWNER, STUCK]);
}

/// **The orphan-child round on a pre-migration bucket**: the round trusted only
/// two "never in the table" proofs, the gate only checked "permanently outside the
/// table". It shows only on a bucket whose history breaks the premise of SM-14
/// (pre-migration): a retired parent without an owner record is cleaned up under
/// its child's feet.
#[test]
fn r1i_orphan_round_trusting_history_discards_a_parent_in_use() {
    let r1i = bugs(|b| b.r1i_trusts_history = true);
    let path = counterexample(
        "orphan round, pre-migration",
        &legacy(split(NONE, r1i)),
        m2::NO_DELETE_UNDER_LIVE,
    );
    assert_eq!(path.last(), Some(&Action::OrphanDiscard(Discard::Done)));
    holds("orphan round, post-migration", &split(NONE, r1i));
    counterexample(
        "orphan round, merge, pre-migration",
        &legacy(merge(NONE, r1i)),
        m2::NO_DELETE_UNDER_LIVE,
    );
}

/// **SM-11**: nobody writes a proxy tombstone for a source that left the table
/// when the coordinator died before writing it — the parent Db is orphaned forever.
#[test]
fn sm11_without_proxy_tombstones_a_committed_parent_is_never_retired() {
    let m = split(CRASH, bugs(|b| b.sm11_no_proxy_tomb = true));
    let path = counterexample("SM-11", &m, STUCK);
    assert!(path.contains(&Action::Crash), "{path:?}");
    assert_eq!(broken("SM-11", &m), [STUCK]);
}

/// **An op dying with its request**: the task vanishes while the source is still
/// quiesced holding the lease, and no reconcile loop exists.
#[test]
fn l1_vanished_coordinator_strands_its_intent() {
    let m = split(VANISH, bugs(|b| b.l1_dies_with_request = true));
    let path = counterexample("L1", &m, STUCK);
    assert_eq!(path.last(), Some(&Action::Vanish(0)));
    assert_eq!(broken("L1", &m), [STUCK]);
}

/// **Giving up after a failed roll-back** (two variants). The reconcile loop
/// alone holds up both; without it the intent is stuck.
#[test]
fn l3_l4_abandoned_roll_backs_need_the_reconciler() {
    let l3 = bugs(|b| b.l3_stray_gives_up = true);
    let l4 = bugs(|b| b.l4_roll_back_gives_up = true);
    holds("L3", &split(AMBIGUOUS, l3));
    holds("L4", &split(AMBIGUOUS, l4));
    let l1 = |b: Bugs| Bugs {
        l1_dies_with_request: true,
        ..b
    };
    let path = counterexample("L3 + L1", &split(AMBIGUOUS, l1(l3)), STUCK);
    assert!(path.contains(&Action::Stray(0, true)), "{path:?}");
    let path = counterexample("L4 + L1", &split(AMBIGUOUS, l1(l4)), STUCK);
    assert!(
        path.iter().any(|a| matches!(a, Action::RbClear(0, true))),
        "{path:?}"
    );
}

/// **A failed `hold` guard removing the holder's mark**: the coordinating node
/// claims the source back and rolls back its own live op.
#[test]
fn l13_failed_hold_lets_the_node_roll_back_its_own_live_op() {
    let m = split(
        Faults {
            ram: true,
            lease_loss: true,
            ..NONE
        },
        bugs(|b| b.l13_failed_hold_releases = true),
    );
    let path = counterexample("hold guard", &m, m2::NO_SELF_ROLL_BACK);
    assert!(path.contains(&Action::SecondHold(0)), "{path:?}");
}

/// **A slow roll-back removing closed marks without an epoch condition** —
/// deleting a later op's mark. SM-13 and the fresh table hold it up alone;
/// together with opening from a stale table an ack is lost.
#[test]
fn r631_slow_roll_back_reopens_a_later_ops_close() {
    let m = |b| {
        second(
            Faults {
                vanish: true,
                ..OPEN
            },
            b,
        )
    };
    holds("R6.3-1", &m(bugs(|b| b.r631_reopen_any = true)));
    holds("D1, two ops", &m(bugs(|b| b.d1_stale_table = true)));
    let path = counterexample(
        "R6.3-1 + D1",
        &m(bugs(|b| {
            b.r631_reopen_any = true;
            b.d1_stale_table = true
        })),
        m2::NO_LOST_ACK,
    );
    let second_close = pos(&path, |a| *a == Action::Close(1));
    let cleanup = pos(&path, |a| matches!(a, Action::RbCleanup(_, _)));
    assert!(second_close < cleanup, "{path:?}");
}

/// **`reconcile` retiring a source by a RAM table learned from a peer**.
#[test]
fn r636_ram_table_retires_a_live_range() {
    let m = split(
        Faults { ram: true, ..NONE },
        bugs(|b| b.r636_retire_on_ram = true),
    );
    counterexample("R6.3-6", &m, m2::NO_TOMB_ON_LIVE);
}

/// **D-d filtering candidates by the RAM table while the reference set comes
/// from an old snapshot**. Four layers block it, each enough alone: the core
/// demands the same version, the version is compared before deleting, the proof
/// is rebuilt, and the named-checkpoint fence (a freshly cloned child still has
/// a named final checkpoint on the freshly retired parent).
#[test]
fn dd_mixed_snapshots_need_all_four_layers_off() {
    let mixed = bugs(|b| b.dd_mixed_snapshots = true);
    holds("3.2-#2", &split(NONE, mixed));
    let three = Bugs {
        dd_no_version_recheck: true,
        dd_no_proof_recheck: true,
        ..mixed
    };
    holds(
        "stale snapshot + version check + rebuild off, checkpoint fence on",
        &split(NONE, three),
    );
    let all = Bugs {
        dd_no_checkpoint_fence: true,
        ..three
    };
    let path = counterexample(
        "stale snapshot + version check + rebuild + checkpoint fence off",
        &split(NONE, all),
        m2::NO_DELETE_UNDER_LIVE,
    );
    let load = pos(&path, |a| *a == Action::GcLoadTable);
    let commit = pos(&path, |a| *a == Action::CommitCas(0));
    assert!(load < commit, "{path:?}");
    assert!(
        matches!(path.last(), Some(Action::GcDelete(1, _))),
        "{path:?}"
    );
}

/// **An orphan Db after two kills**: the roll-backer dies while cleaning up the
/// child, then a later op retires the source. SM-14's second proof ("has a Db, no
/// owner record") fixes it; the `Cleaned` ledger is the positive proof replacing it.
#[test]
fn r1f_orphan_of_a_half_done_roll_back_after_its_sources_retire() {
    let kd = Faults {
        discard: true,
        ..VANISH
    };
    let r1f = bugs(|b| b.r1f_first_proof_only = true);
    holds("orphan after two kills, with ledger", &second(kd, r1f));
    let path = counterexample(
        "orphan after two kills, before ledgers",
        &second(
            kd,
            Bugs {
                r1f_first_proof_only: true,
                ..Bugs::BEFORE_3_1
            },
        ),
        STUCK,
    );
    let dies = pos(&path, |a| matches!(a, Action::RbCleanup(_, Discard::Dies)));
    let commit = pos(&path, |a| *a == Action::CommitCas(1));
    assert!(dies < commit, "{path:?}");
}

/// An in-flight child stands on the source's ancestor. D-d is right thanks to
/// four layers: a quiesced source nobody opens (SM-13 + closed mark), detach
/// respecting checkpoints (G6), D-d adding in-flight children, and the
/// named-checkpoint fence (the in-flight child placed a named final checkpoint
/// on the ancestor when cloning).
#[test]
fn n2_in_flight_children_rest_on_four_layers() {
    let env = Env {
        detach_respects_checkpoints: false,
        ..Env::ALL
    };
    let open = bugs(|b| {
        b.d2_ignores_quiesced = true;
        b.before_close_mark = true
    });
    let three = Bugs {
        dd_ignores_in_flight: true,
        ..open
    };
    // With the first two layers off an ack is lost (reopened source + before
    // SM-17) — not the point here; only ask whether D-d deletes under an in-flight child.
    let got = broken(
        "in-flight child, three old layers off, checkpoint fence on",
        &M2 {
            env,
            ..split(OPEN, three)
        },
    );
    assert!(!got.contains(&m2::NO_DELETE_UNDER_LIVE), "{got:?}");
    let all = Bugs {
        dd_no_checkpoint_fence: true,
        ..three
    };
    let m = M2 {
        env,
        ..split(OPEN, all)
    };
    let path = counterexample("in-flight, 4 off", &m, m2::NO_DELETE_UNDER_LIVE);
    assert!(
        matches!(path.last(), Some(Action::GcDelete(m2::ANCESTOR, _))),
        "{path:?}"
    );
    for (name, m) in [
        (
            "in-flight child, only G6 off",
            M2 {
                env,
                ..split(OPEN, Bugs::NONE)
            },
        ),
        (
            "in-flight child, only D-d not adding in-flight children",
            split(OPEN, bugs(|b| b.dd_ignores_in_flight = true)),
        ),
    ] {
        holds(name, &m);
    }
}

// ───────────────────────────────────────── matrix

type Row = (&'static str, fn() -> M2, Vec<&'static str>);

/// Each row: flag (with the layers it needs), profile, the invariants broken. An
/// empty row = that layer is redundant at model level (another layer holds) —
/// recorded in the module doc of `m2.rs`.
fn matrix() -> Vec<Row> {
    use m2::{
        D5_NO_STRANDED_OWNER as D5, NO_DELETE_UNDER_LIVE as DELETE, NO_LOST_ACK as ACK,
        NO_ORPHAN_CHECKPOINT_ON_LIVE as CKPT, NO_SELF_ROLL_BACK as SELF, NO_TOMB_ON_LIVE as TOMB,
    };
    fn b(f: fn(&mut Bugs)) -> Bugs {
        bugs(f)
    }
    vec![
        ("D1", || split(OPEN, b(|b| b.d1_stale_table = true)), vec![]),
        (
            "stale table + before SM-17",
            || {
                split(
                    OPEN,
                    b(|b| {
                        b.d1_stale_table = true;
                        b.before_close_mark = true
                    }),
                )
            },
            vec![ACK],
        ),
        (
            "D2",
            || split(OPEN, b(|b| b.d2_ignores_quiesced = true)),
            vec![],
        ),
        (
            "reopened source + before SM-17",
            || {
                split(
                    OPEN,
                    b(|b| {
                        b.d2_ignores_quiesced = true;
                        b.before_close_mark = true
                    }),
                )
            },
            vec![ACK],
        ),
        (
            "before SM-17",
            || split(OPEN, b(|b| b.before_close_mark = true)),
            vec![],
        ),
        (
            "D5",
            || {
                split(
                    Faults {
                        lease_loss: true,
                        ..NONE
                    },
                    b(|b| b.d5_recreates_children = true),
                )
            },
            vec![D5, STUCK],
        ),
        (
            "R1-i",
            || split(NONE, b(|b| b.r1i_trusts_history = true)),
            vec![],
        ),
        (
            "orphan round, pre-migration",
            || legacy(split(NONE, b(|b| b.r1i_trusts_history = true))),
            vec![DELETE],
        ),
        (
            "SM-11",
            || split(CRASH, b(|b| b.sm11_no_proxy_tomb = true)),
            vec![STUCK],
        ),
        // With the `Prepared` ledger, an in-flight child goes to the tombstone
        // branch (core: `MayHaveCommitted` in the cleanup round), so "not
        // promised" in that branch is no longer redundant: dropping it ⇒ the
        // in-flight child carries a tombstone, D-d deletes its Db, its final
        // checkpoint stays on the live source.
        (
            "SM-11 (2)",
            || split(CRASH, b(|b| b.sm11_ignores_promised = true)),
            vec![CKPT],
        ),
        (
            "L1",
            || split(VANISH, b(|b| b.l1_dies_with_request = true)),
            vec![STUCK],
        ),
        (
            "L3",
            || split(AMBIGUOUS, b(|b| b.l3_stray_gives_up = true)),
            vec![],
        ),
        (
            "L4",
            || split(AMBIGUOUS, b(|b| b.l4_roll_back_gives_up = true)),
            vec![],
        ),
        (
            "hold guard",
            || {
                split(
                    Faults {
                        ram: true,
                        lease_loss: true,
                        ..NONE
                    },
                    b(|b| b.l13_failed_hold_releases = true),
                )
            },
            vec![SELF],
        ),
        (
            "R6.3-1",
            || {
                second(
                    Faults {
                        vanish: true,
                        ..OPEN
                    },
                    b(|b| b.r631_reopen_any = true),
                )
            },
            vec![],
        ),
        (
            "R6.3-6",
            || {
                split(
                    Faults { ram: true, ..NONE },
                    b(|b| b.r636_retire_on_ram = true),
                )
            },
            vec![TOMB],
        ),
        (
            "3.2-#2",
            || split(NONE, b(|b| b.dd_mixed_snapshots = true)),
            vec![],
        ),
        (
            "R1-f",
            || {
                second(
                    Faults {
                        discard: true,
                        ..VANISH
                    },
                    b(|b| b.r1f_first_proof_only = true),
                )
            },
            vec![],
        ),
        (
            "SM-3 reversed",
            || {
                split(
                    Faults {
                        lease_loss: true,
                        ..NONE
                    },
                    b(|b| b.sm3_discard_first = true),
                )
            },
            vec![],
        ),
        (
            "half owner record",
            || split(DISCARD, b(|b| b.d4_owner_before_db = true)),
            vec![],
        ),
        (
            "3.5",
            || split(NONE, b(|b| b.dd_no_version_recheck = true)),
            vec![],
        ),
        (
            "2.2",
            || split(NONE, b(|b| b.dd_no_proof_recheck = true)),
            vec![],
        ),
        (
            "N2",
            || split(NONE, b(|b| b.dd_ignores_in_flight = true)),
            vec![],
        ),
        (
            "N4",
            || legacy(split(NONE, b(|b| b.n4_any_refs_version = true))),
            vec![],
        ),
        // The named-checkpoint fence holds up both rows below: the "unknown" set
        // cannot blind the Db's own manifest, and the `fork` copy places a final
        // checkpoint (a name outside the prefix — counted as alive) on every Db it
        // stands on. Each row only breaks with that fence off as well.
        (
            "rule 5",
            || split(NONE, b(|b| b.unknown_refs_as_clear = true)),
            vec![],
        ),
        (
            "rule 5 + no checkpoint fence",
            || {
                split(
                    NONE,
                    b(|b| {
                        b.unknown_refs_as_clear = true;
                        b.dd_no_checkpoint_fence = true
                    }),
                )
            },
            vec![DELETE],
        ),
        (
            "fork without barrier (environment)",
            || M2 {
                env: Env {
                    fork_barrier: false,
                    ..Env::ALL
                },
                ..split(NONE, Bugs::NONE)
            },
            vec![],
        ),
        (
            "fork without barrier (environment) + no checkpoint fence",
            || M2 {
                env: Env {
                    fork_barrier: false,
                    ..Env::ALL
                },
                ..split(NONE, b(|b| b.dd_no_checkpoint_fence = true))
            },
            vec![DELETE],
        ),
        (
            "half Db (environment)",
            || M2 {
                env: Env {
                    keep_db_until_checkpoints_gone: false,
                    ..Env::ALL
                },
                ..split(DISCARD, Bugs::NONE)
            },
            vec![CKPT],
        ),
        (
            "G6 (environment)",
            || M2 {
                env: Env {
                    detach_respects_checkpoints: false,
                    ..Env::ALL
                },
                ..split(NONE, Bugs::NONE)
            },
            vec![],
        ),
        // D-d's second fence: an AND of two proofs, each enough. With a full scan
        // the checkpoint fence is redundant at model level; with a blind scan the
        // checkpoint fence alone holds — on named and old buckets alike, because an
        // unnamed checkpoint is "unknown" (kept). Treating unnamed checkpoints as
        // nobody leaves only the scan on old buckets.
        (
            "no checkpoint fence",
            || split(OPEN, b(|b| b.dd_no_checkpoint_fence = true)),
            vec![],
        ),
        (
            "blind scan (environment)",
            || blind(split(OPEN, Bugs::NONE)),
            vec![],
        ),
        (
            "blind scan, merge",
            || blind(merge(NONE, Bugs::NONE)),
            vec![],
        ),
        (
            "blind scan + no checkpoint fence",
            || blind(split(NONE, b(|b| b.dd_no_checkpoint_fence = true))),
            vec![DELETE],
        ),
        // A lineage two deep: each of D-d's two layers is still enough alone —
        // it only breaks with both off, as with one ancestor.
        (
            "deep: blind scan",
            || deep(blind(split(NONE, Bugs::NONE))),
            vec![],
        ),
        (
            "deep: blind scan + no checkpoint fence",
            || deep(blind(split(NONE, b(|b| b.dd_no_checkpoint_fence = true)))),
            vec![DELETE],
        ),
        (
            "blind scan, unnamed bucket",
            || unnamed(blind(split(OPEN, Bugs::NONE))),
            vec![],
        ),
        (
            "blind scan, unnamed bucket, unnamed counted as nobody",
            || unnamed(blind(split(NONE, b(|b| b.dd_unnamed_as_clear = true)))),
            vec![DELETE],
        ),
        (
            "unnamed bucket, unnamed counted as nobody",
            || unnamed(split(OPEN, b(|b| b.dd_unnamed_as_clear = true))),
            vec![],
        ),
        // Cleaning up `ops/` ledgers. Without cleanup ledgers leak forever
        // (liveness demands ledgers in a final outcome to be cleaned up). Cleaning
        // up a `Cleaned` ledger while the child still has a Db and owner record
        // loses the positive proof — a claimed child of a roll-back that died
        // half-way leaks forever when the source retires later
        // (`::reaping_an_unsettled_ledger_strands_a_claimed_orphan`); one op breaks nothing.
        (
            "ledgers kept forever (earlier)",
            || split(NONE, b(|b| b.ledger_kept_forever = true)),
            vec![STUCK],
        ),
        (
            "unsettled ledger cleanup",
            || split(DISCARD, b(|b| b.ledger_reaps_unsettled = true)),
            vec![],
        ),
        (
            "unsettled ledger cleanup, merge",
            || merge(DISCARD, b(|b| b.ledger_reaps_unsettled = true)),
            vec![],
        ),
        (
            "unsettled ledger cleanup, two ops",
            || {
                second(
                    Faults {
                        discard: true,
                        ..VANISH
                    },
                    b(|b| b.ledger_reaps_unsettled = true),
                )
            },
            vec![STUCK],
        ),
    ]
}

/// D-d's reference-set scan is blind (`Env::dd_scan_sees` off).
fn blind(m: M2) -> M2 {
    M2 {
        env: Env {
            dd_scan_sees: false,
            ..m.env
        },
        ..m
    }
}

/// A bucket whose initial final checkpoints are unnamed (before the fork's
/// checkpoint-naming patch, G15).
fn unnamed(m: M2) -> M2 {
    M2 {
        unnamed_history: true,
        ..m
    }
}

/// Switch exactly one layer off (or one bug with the layers it needs): which invariants break.
#[test]
fn each_bug_alone() {
    let mut wrong = Vec::new();
    for (name, model, want) in matrix() {
        let got = broken(name, &model());
        if got != want {
            wrong.push(format!("{name}: want {want:?}, got {got:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "matrix mismatch:\n  {}",
        wrong.join("\n  ")
    );
}

/// Every `Bugs` flag is in the matrix or in a dedicated test above.
#[test]
fn every_bug_flag_is_exercised() {
    let dbg = format!("{:?}", Bugs::NONE);
    let src = read_source("crates/cairn-model/tests/m2.rs");
    let body = dbg
        .strip_prefix("Bugs { ")
        .and_then(|b| b.strip_suffix(" }"))
        .unwrap_or_else(|| panic!("the Debug of Bugs changed shape: {dbg}"));
    let missing: Vec<&str> = body
        .split(", ")
        .map(|kv| kv.split_once(": ").expect("field").0)
        .filter(|f| {
            !src.contains(&format!("b.{f} = true"))
                && !src.contains(&format!("{f}: true"))
                && !matches!(
                    *f,
                    "no_ledger" | "no_roll_forward" | "ledger_not_proof" | "ctl_reopen_any"
                )
        })
        .collect();
    assert!(missing.is_empty(), "flags nobody injects: {missing:?}");
}

// ───────────────────────────────────────── drift check

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_source(rel: &str) -> String {
    std::fs::read_to_string(repo_root().join(rel))
        .unwrap_or_else(|e| panic!("cannot read {rel}: {e}"))
}

fn resolve(rel: &str) -> Vec<std::path::PathBuf> {
    let crates = repo_root().join("crates");
    let mut out = Vec::new();
    if rel.starts_with("cairn-") {
        let p = crates.join(rel);
        if p.is_file() {
            out.push(p);
        }
        return out;
    }
    for entry in std::fs::read_dir(&crates).expect("read crates/").flatten() {
        for p in [
            entry.path().join("src").join(rel),
            entry.path().join("src/split").join(rel),
            entry.path().join(rel),
        ] {
            if p.is_file() {
                out.push(p);
            }
        }
    }
    out
}

/// Drift check for the **environment** part of M2: every `Env` flag has an
/// anchor in `m2::ANCHORS` and the anchor string is still in the file; every
/// `file:function` of the `m2.rs` module doc still has its file and its `fn
/// function`. The core part needs no anchor — M2 calls `cairn_split::step` itself.
#[test]
fn anchors_exist_in_the_source() {
    let dbg = format!("{:?}", Env::ALL);
    let names: Vec<String> = dbg
        .strip_prefix("Env { ")
        .and_then(|b| b.strip_suffix(" }"))
        .unwrap_or_else(|| panic!("the Debug of Env changed shape: {dbg}"))
        .split(", ")
        .map(|kv| kv.split_once(": ").expect("field").0.to_string())
        .collect();
    let mut missing = Vec::new();
    for n in &names {
        if !m2::ANCHORS.iter().any(|(f, _, _)| f == n) {
            missing.push(format!("flag {n} has no anchor"));
        }
    }
    for (flag, file, needle) in m2::ANCHORS {
        if !names.iter().any(|n| n == flag) {
            missing.push(format!("anchor for a flag that does not exist: {flag}"));
        } else if !read_source(file).contains(needle) {
            missing.push(format!("{flag}: {file} no longer has {needle:?}"));
        }
    }
    let doc: String = read_source("crates/cairn-model/src/m2.rs")
        .lines()
        // The license header (plain `//` lines) comes before the module doc.
        .skip_while(|l| l.starts_with("//") && !l.starts_with("//!"))
        .take_while(|l| l.starts_with("//!") || l.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let mut refs = 0;
    for span in doc.split('`').skip(1).step_by(2) {
        let (path, func) = match span.split_once(".rs") {
            Some((p, rest))
                if !p.is_empty()
                    && p.chars()
                        .all(|c| c.is_ascii_alphanumeric() || "_/.-".contains(c)) =>
            {
                (format!("{p}.rs"), rest.trim_start_matches(':'))
            }
            _ => continue,
        };
        if !func
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
        {
            continue;
        }
        refs += 1;
        let files = resolve(&path);
        if files.is_empty() {
            missing.push(format!("module doc: no file {path}"));
            continue;
        }
        let Some(name) = func.rsplit("::").next().filter(|n| !n.is_empty()) else {
            continue;
        };
        let pat = format!("fn {name}");
        let found = files.iter().any(|f| {
            let src = std::fs::read_to_string(f).unwrap_or_default();
            src.match_indices(&pat)
                .any(|(i, _)| matches!(src[i + pat.len()..].chars().next(), Some('(' | '<')))
        });
        if !found {
            missing.push(format!("module doc: {path}:{func} — no `fn {name}` found"));
        }
    }
    assert!(
        refs >= 15,
        "the module doc has only {refs} `file:function` references — is the parsing broken?"
    );
    assert!(
        missing.is_empty(),
        "M2 drifted from the code:\n  {}",
        missing.join("\n  ")
    );
}
