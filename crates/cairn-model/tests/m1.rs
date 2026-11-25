// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Acceptance tests of model M1: with every fence on ⇒ the whole space is
//! explored and every invariant holds; switching RD1 or RD2 back on ⇒ the
//! checker finds a counterexample.
//!
//! The printed counterexample (`--nocapture`) is the BFS path stopping at the
//! first batch with a violation — the shortest or nearly the shortest
//! (stateright overwrites the finding within a batch of 1 500 states).

use {
    cairn_model::m1::{self, Action, Cas, Expiry, Fences, Io, Read, State, M1},
    stateright::{Checker, HasDiscoveries, Model},
    std::{
        collections::{BTreeMap, VecDeque},
        hash::{DefaultHasher, Hash, Hasher},
    },
};

fn report(name: &str, c: &impl Checker<M1>) {
    println!(
        "{name}: {} unique states, {} generated, depth {}, done = {}",
        c.unique_state_count(),
        c.state_count(),
        c.max_depth(),
        c.is_done()
    );
}

/// BFS over the **whole** state space.
fn check_all(name: &str, model: M1) -> impl Checker<M1> {
    let c = model
        .checker()
        .finish_when(HasDiscoveries::AllFailures)
        .spawn_bfs()
        .join();
    report(name, &c);
    c
}

/// BFS to the first counterexample of `property`; returns its steps.
fn first_counterexample(name: &str, model: M1, property: &'static str) -> Vec<Action> {
    let c = model
        .checker()
        .finish_when(HasDiscoveries::AnyOf([property].into()))
        .spawn_bfs()
        .join();
    report(name, &c);
    let actions = c
        .discovery(property)
        .unwrap_or_else(|| panic!("{name}: no counterexample found for \"{property}\""))
        .into_actions();
    println!("counterexample \"{property}\" ({} steps):", actions.len());
    for (i, a) in actions.iter().enumerate() {
        println!("  {:>2}. {a:?}", i + 1);
    }
    actions
}

fn position(actions: &[Action], want: impl Fn(&Action) -> bool) -> usize {
    actions
        .iter()
        .position(want)
        .unwrap_or_else(|| panic!("step missing in {actions:?}"))
}

fn coordinator(actions: &[Action]) -> u8 {
    actions
        .iter()
        .find_map(|a| match a {
            Action::IntentCas(n) => Some(*n),
            _ => None,
        })
        .expect("the counterexample has an intent CAS")
}

/// (a) Every fence of the current code: full exploration, no counterexample to
/// any invariant, and every `sometimes` has an example (the model is not
/// vacuous: the split completes and writes into a child, another node rolls the
/// intent back, a source carries a tombstone).
#[test]
fn all_fences_hold_for_split() {
    let c = check_all("split, all fences", M1::split(Fences::ALL));
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// (a') As above, for merging two ranges (RD2 exists for both split and
/// merge). Three ranges make it ~30 times larger than split: run by hand when
/// changing the model or the merge protocol.
#[test]
#[ignore = "~15 million states, a few minutes: run by hand with --ignored when changing M1 or the merge protocol"]
fn all_fences_hold_for_merge() {
    let c = check_all("merge, all fences", M1::merge(Fences::ALL));
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// (a'') As (a), with one node allowed to die (the coordinator dies midway,
/// the `ActiveIntents` in RAM are lost).
#[test]
fn all_fences_hold_for_split_with_a_crash() {
    let c = check_all(
        "split, all fences, one crash",
        M1 {
            max_crashes: 1,
            ..M1::split(Fences::ALL)
        },
    );
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// (b) RD1 switched back on: a failed table read is swallowed, the open carries
/// on with the stale RAM table.
///
/// Must see: the source's owner loses its lease in the middle of the split; the
/// other node — its RAM table still from before the split, so it sees no intent
/// to roll back (SM-3) — takes the source, fails to read the table, opens the
/// parent Db by its RAM table; the commit CAS wins, and that node acks a write
/// into a range that has left the table.
#[test]
fn d1_swallowed_table_read_error_acks_into_retired_parent() {
    let d1 = Fences {
        fresh_table_before_open: false,
        closed_flag: false,
        ..Fences::ALL
    };
    let path = first_counterexample("split, RD1", M1::split(d1), m1::NO_ACK_INTO_RETIRED_DB);
    let n = coordinator(&path);
    let failed_read = position(
        &path,
        |a| matches!(a, Action::ReloadForOpen(m, 0, Read::Failed) if *m != n),
    );
    let open = position(&path, |a| matches!(a, Action::OpenDb(m, 0, _) if *m != n));
    let commit = position(&path, |a| *a == Action::CommitCas);
    let ack = path.len() - 1;
    assert!(matches!(path[ack], Action::Ack(m, 0) if m != n), "{path:?}");
    assert!(
        position(&path, |a| *a == Action::IntentCas(n)) < failed_read,
        "{path:?}"
    );
    assert!(failed_read < open && open < ack && commit < ack, "{path:?}");
    // RD1 slips past exactly one fence: the lease invariants still hold. SM-13
    // breaks along with it — its second fence (another node rolls the intent
    // back, SM-3) stands only when the open reads a fresh table.
    let c = check_all("split, RD1, full", M1::split(d1));
    assert!(c.is_done());
    for p in [
        m1::SINGLE_WRITER,
        m1::LS3_LEADER_IS_RECORDED_OWNER,
        m1::LS8_RETIRED_NEVER_REACQUIRED,
    ] {
        assert!(c.discovery(p).is_none(), "RD1 must not break \"{p}\"");
    }
    assert!(c.discovery(m1::SM13_QUIESCED_SOURCE_CLOSED).is_some());
}

/// (c) RD2 switched back on: the Db-open path does not know the source is
/// quiescing (no `wait_unquiesced`, no tripwire).
///
/// Must see: the coordinator quiesces the source; its node's session is lost
/// (past the self-step-down deadline, a new incarnation); the source engine **on
/// the same node** takes it back, does not roll back (the intent is its own
/// node's), reopens the Db; the coordinator clones, claims the children and the
/// commit CAS still wins; the reopened engine acks into the source — a write
/// missing from the child (after the clone, or after the commit).
#[test]
fn d2_open_path_unaware_of_quiesce_acks_into_retired_source() {
    let d2 = M1::split(Fences {
        wait_unquiesced: false,
        closed_flag: false,
        ..Fences::ALL
    });
    let path = first_counterexample("split, RD2", d2.clone(), m1::NO_ACK_INTO_RETIRED_DB);
    let n = coordinator(&path);
    let quiesce = position(&path, |a| *a == Action::Quiesce);
    let reopen = path
        .iter()
        .rposition(|a| matches!(a, Action::OpenDb(m, 0, _) if *m == n))
        .expect("the coordinating node reopens the source");
    let clone = position(&path, |a| *a == Action::Clone);
    let commit = position(&path, |a| *a == Action::CommitCas);
    let ack = path.len() - 1;
    assert_eq!(path[ack], Action::Ack(n, 0), "{path:?}");
    assert!(
        quiesce < reopen && reopen < ack && clone < ack && commit < ack,
        "{path:?}"
    );
    assert!(
        position(&path, |a| matches!(a, Action::Renew(m) if *m == n)) < reopen,
        "{path:?}"
    );

    let sm13 = first_counterexample("split, RD2 (SM-13)", d2, m1::SM13_QUIESCED_SOURCE_CLOSED);
    let n = coordinator(&sm13);
    assert!(
        matches!(sm13.last(), Some(Action::OpenDb(m, 0, _)) if *m == n),
        "{sm13:?}"
    );
}

/// (c') RD2 for merge: the same bug.
#[test]
fn d2_also_breaks_merge() {
    first_counterexample(
        "merge, RD2",
        M1::merge(Fences {
            wait_unquiesced: false,
            closed_flag: false,
            ..Fences::ALL
        }),
        m1::NO_ACK_INTO_RETIRED_DB,
    );
}

/// SM-3 is SM-13's fence on **another** node: switching it off ⇒ another node
/// takes the quiescing source, opens the Db without rolling the intent back,
/// and the commit still wins.
#[test]
fn without_foreign_roll_back_another_node_opens_the_quiesced_source() {
    let path = first_counterexample(
        "split, no foreign-intent roll-back",
        M1::split(Fences {
            roll_back_foreign_intent: false,
            closed_flag: false,
            ..Fences::ALL
        }),
        m1::NO_ACK_INTO_RETIRED_DB,
    );
    let n = coordinator(&path);
    assert!(
        path.iter()
            .any(|a| matches!(a, Action::Ack(m, 0) if *m != n)),
        "{path:?}"
    );
}

/// LS-3 without the owner-record CAS: two nodes both believe they lead.
#[test]
fn without_owner_cas_two_nodes_believe_they_lead() {
    first_counterexample(
        "split, no owner-record CAS",
        M1::split(Fences {
            owner_cas: false,
            ..Fences::ALL
        }),
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
}

/// ... but `writer_epoch` (LS-4) still keeps "one writer able to write per
/// Db": the real write fence is `writer_epoch`, not the owner record.
#[test]
#[ignore = "~3 million states, ~30 s: run by hand with --ignored when changing M1"]
fn without_owner_cas_writer_epoch_still_keeps_one_writer() {
    let c = check_all(
        "split, no owner-record CAS, full",
        M1::split(Fences {
            owner_cas: false,
            ..Fences::ALL
        }),
    );
    assert!(c.is_done());
    assert!(c.discovery(m1::SINGLE_WRITER).is_none());
}

/// Both layers off (owner record and `writer_epoch`) ⇒ two writers write one Db.
#[test]
fn without_owner_cas_and_writer_epoch_two_writers_meet() {
    first_counterexample(
        "split, no owner-record CAS, no writer_epoch",
        M1::split(Fences {
            owner_cas: false,
            writer_epoch: false,
            ..Fences::ALL
        }),
        m1::SINGLE_WRITER,
    );
}

/// LS-2: grant validity longer than the TTL (a wrong configuration) ⇒ an
/// observer marks it `expired` while the owner still believes it leads, and the
/// other node takes the range.
#[test]
fn grant_outliving_ttl_breaks_ls3() {
    first_counterexample(
        "split, validity > TTL",
        M1 {
            validity: 3,
            ..M1::split(Fences::ALL)
        },
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
}

// ───────────────── Revival by CAS

/// (h) **Unfenced** revival (`revive_by_cas` off): a renewal landing after the
/// deadline comes back to life even when the session record has ended (an
/// observer CASed `expired`, or the owner itself called `lose` and wrote
/// `closed`) — like an unconditional write, or trusting the clock instead of
/// the version.
///
/// Must see: the owner's session ends, the other node takes the range, then the
/// owner "revives" and two engines both believe they lead — LS-3 breaks. With
/// the fence, `all_fences_hold_*` (with [`Action::RenewLate`]) holds.
#[test]
fn revival_without_cas_breaks_ls3() {
    let actions = first_counterexample(
        "split, unfenced revival",
        M1::split(Fences {
            revive_by_cas: false,
            ..Fences::ALL
        }),
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
    let Some(Action::RenewLate(victim)) = actions.last().copied() else {
        panic!("the counterexample must end with the revival: {actions:?}");
    };
    position(
        &actions,
        |a| matches!(a, Action::Claim(n, _) if *n != victim),
    );
}

// ───────────────── The read fence at the leader

/// (i) The node's clock lies (`validity > ttl`: a sleeping machine, a paused VM
/// — the grant is still "valid" after an observer has CASed `expired`), with
/// **no** read fence.
///
/// Must see: the other node takes the range, then the old owner — still
/// trusting its clock — answers a read while the owner record has passed to
/// someone else.
#[test]
fn lying_clock_without_read_fence_serves_a_stale_read() {
    let actions = first_counterexample(
        "split, validity > TTL, no read fence",
        M1 {
            validity: 3,
            ..M1::split(Fences {
                read_fence: false,
                ..Fences::ALL
            })
        },
        m1::NO_STALE_READ,
    );
    let read = position(&actions, |a| matches!(a, Action::ServeRead(..)));
    let Action::ServeRead(stale, _) = actions[read] else {
        unreachable!()
    };
    position(
        &actions[..read],
        |a| matches!(a, Action::Claim(n, _) if *n != stale),
    );
}

/// (i') The same lying clock, **with** the read fence: full exploration, no
/// stale read (the other invariants break — `grant_outliving_ttl_breaks_ls3` —
/// but the read fence does not rely on the clock). One incarnation per node to
/// fit `cargo test`'s time (the counterexample of (i) needs no second
/// incarnation); (i'') explores both.
#[test]
fn read_fence_keeps_reads_fresh_with_a_lying_clock() {
    read_fence_holds_with_a_lying_clock(1);
}

/// (i'') As (i'), two incarnations per node.
#[test]
#[ignore = "many times larger than (i'): run by hand with --ignored when changing M1"]
fn read_fence_keeps_reads_fresh_with_a_lying_clock_two_incarnations() {
    read_fence_holds_with_a_lying_clock(2);
}

fn read_fence_holds_with_a_lying_clock(max_incarnations: u8) {
    let c = M1 {
        validity: 3,
        max_incarnations,
        ..M1::split(Fences::ALL)
    }
    .checker()
    .finish_when(HasDiscoveries::AnyOf([m1::NO_STALE_READ].into()))
    .spawn_bfs()
    .join();
    report("split, validity > TTL, read fence", &c);
    assert!(c.is_done(), "BFS must explore everything");
    assert!(
        c.discovery(m1::NO_STALE_READ).is_none(),
        "the read fence let a stale read through: {:?}",
        c.discovery(m1::NO_STALE_READ).map(|p| p.into_actions())
    );
}

/// (i‴) **Two-phase** reads: the read, the ticket, the proof and the answer are
/// separate steps, interleaving with everything else. Proof is any observation
/// that sees our session record still live — a fence GET **or a winning
/// renewal CAS** (`Renew`/`RenewLate`) — provided it was issued **after** the
/// ticket, i.e. after the read. The same lying clock (`validity > ttl`): full
/// exploration, no stale read. This is the argument that allows reusing a
/// renewal and letting one GET cover every answer that took its ticket before
/// the GET was issued.
#[test]
fn proof_issued_after_the_read_keeps_reads_fresh_with_a_lying_clock() {
    proof_after_read_holds_with_a_lying_clock(1);
}

/// (i⁗) As (i‴), two incarnations per node.
#[test]
#[ignore = "many times larger than (i‴): run by hand with --ignored when changing M1"]
fn proof_issued_after_the_read_keeps_reads_fresh_with_a_lying_clock_two_incarnations() {
    proof_after_read_holds_with_a_lying_clock(2);
}

fn proof_after_read_holds_with_a_lying_clock(max_incarnations: u8) {
    let c = M1 {
        validity: 3,
        max_incarnations,
        split_reads: true,
        ..M1::split(Fences::ALL)
    }
    .checker()
    .finish_when(HasDiscoveries::AnyOf([m1::NO_STALE_READ].into()))
    .spawn_bfs()
    .join();
    report(
        "split, validity > TTL, two-phase read, proof after the read",
        &c,
    );
    assert!(c.is_done(), "BFS must explore everything");
    assert!(
        c.discovery(m1::NO_STALE_READ).is_none(),
        "proof after the read let a stale read through: {:?}",
        c.discovery(m1::NO_STALE_READ).map(|p| p.into_actions())
    );
}

/// (i⁗') The faulty configuration of (i‴): an answer reuses proof that
/// **already existed** (a renewal or fence GET issued before the read) instead
/// of requiring proof issued after the ticket.
///
/// Must see: the old owner has proof, then (lying clock) its session is CASed
/// to `expired` and the other node takes the range; the old owner reads
/// **after** that take-over and answers thanks to the old proof.
#[test]
fn proof_issued_before_the_read_serves_a_stale_read() {
    let actions = first_counterexample(
        "split, validity > TTL, two-phase read, old proof reused",
        M1 {
            validity: 3,
            max_incarnations: 1,
            split_reads: true,
            ..M1::split(Fences {
                proof_after_read: false,
                ..Fences::ALL
            })
        },
        m1::NO_STALE_READ,
    );
    let start = position(&actions, |a| matches!(a, Action::ReadStart(..)));
    let Action::ReadStart(stale, _) = actions[start] else {
        unreachable!()
    };
    // The old owner's only proof lies **before** the read …
    position(
        &actions[..start],
        |a| matches!(a, Action::Renew(n) | Action::RenewLate(n) | Action::ProbeSession(n) if *n == stale),
    );
    assert!(
        !actions[start..]
            .iter()
            .any(|a| matches!(a, Action::ProbeSession(n) if *n == stale)),
        "the counterexample must not use a fence GET after the read: {actions:?}"
    );
    // … and the other node takes the range before the read.
    position(
        &actions[..start],
        |a| matches!(a, Action::Claim(n, _) if *n != stale),
    );
}

// ───────────────── A fenced judgment of death on the bucket

/// (g1) An observer may see a session through a torn/stale object
/// (`torn_views`): the judgment of death after `UNEXPIRABLE_AFTER_TTLS` TTLs can
/// fire at any time. Fenced (overwrite + one TTL + CAS `expired`): full
/// exploration, every invariant holds. One incarnation per node to fit
/// `cargo test`'s time (a node that loses its session stops); (g1') explores
/// both.
#[test]
fn torn_views_hold_with_rearm_for_split() {
    let c = check_all(
        "split, all fences, torn session views, one incarnation",
        M1 {
            torn_views: true,
            max_incarnations: 1,
            ..M1::split(Fences::ALL)
        },
    );
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// (g1') As (g1), two incarnations per node (~15 million states).
#[test]
#[ignore = "~15 million states, ~3 minutes in --release: run by hand with --ignored when changing M1"]
fn torn_views_hold_with_rearm_for_split_two_incarnations() {
    let c = check_all(
        "split, all fences, torn session views",
        M1 {
            torn_views: true,
            ..M1::split(Fences::ALL)
        },
    );
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// (g2) As (g1), unfenced (the historical behaviour: `dead_unexpirable` only in
/// RAM).
///
/// Must see: while the owner can still renew, an observer judges it dead, takes
/// the range, and two engines both believe they lead — LS-3 breaks.
#[test]
fn unfenced_unexpirable_judgment_breaks_ls3() {
    let actions = first_counterexample(
        "split, unfenced judgment of death",
        M1 {
            torn_views: true,
            ..M1::split(Fences {
                unexpirable_rearm: false,
                ..Fences::ALL
            })
        },
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
    let judged = position(&actions, |a| matches!(a, Action::JudgeUnexpirable(_)));
    let Action::JudgeUnexpirable(victim) = actions[judged] else {
        unreachable!()
    };
    position(
        &actions[judged..],
        |a| matches!(a, Action::Claim(n, _) if *n != victim),
    );
}

/// (g3) As (g1), for merge — three ranges, one incarnation per node, run by hand.
#[test]
#[ignore = "many times larger than split: run by hand with --ignored when changing M1"]
fn torn_views_hold_with_rearm_for_merge() {
    let c = check_all(
        "merge, all fences, torn session views, one incarnation",
        M1 {
            torn_views: true,
            max_incarnations: 1,
            ..M1::merge(Fences::ALL)
        },
    );
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

// ───────────────── SM-17: the closed mark on the Db itself

/// (d) **Only the closed mark**, every older fence of the open path off (fresh
/// table, SM-13, rolling back a foreign intent): the closed mark blocks every
/// open **after** it, but not a writer opened **before** it —
/// `close_for_writes` bumps `writer_epoch`, an open writer is fenced only at
/// its next manifest update, while acks go through the WAL.
///
/// Must see (the interleaving order — a finding of the model):
/// 1. the coordinator CASes the intent, quiesces the source;
/// 2. the source engine's lease is lost, an engine (same node or the other)
///    takes it back and **reopens the source Db** — no older fence stops it;
/// 3. the coordinator closes the source (`close_range_db`) — one step late;
/// 4. clone, claim the children, the commit CAS wins;
/// 5. the writer opened in (2) acks into the source — a write missing from the
///    child.
#[test]
fn closed_flag_alone_misses_a_writer_opened_before_the_close() {
    let path = first_counterexample(
        "split, closed mark only",
        M1::split(Fences::CLOSED_FLAG_ONLY),
        m1::NO_ACK_INTO_RETIRED_DB,
    );
    let quiesce = position(&path, |a| *a == Action::Quiesce);
    let close = position(&path, |a| *a == Action::CloseSources);
    let reopen = path[..close]
        .iter()
        .rposition(|a| matches!(a, Action::OpenDb(_, 0, _)))
        .expect("the source is reopened before the close");
    let clone = position(&path, |a| *a == Action::Clone);
    let ack = path.len() - 1;
    assert!(matches!(path[ack], Action::Ack(_, 0)), "{path:?}");
    assert!(
        quiesce < reopen && reopen < close && close < clone && clone < ack,
        "{path:?}"
    );
}

/// (d') As (d), for merge: the same gap between the quiesce and the close.
#[test]
fn closed_flag_alone_misses_a_writer_opened_before_the_close_merge() {
    let path = first_counterexample(
        "merge, closed mark only",
        M1::merge(Fences::CLOSED_FLAG_ONLY),
        m1::NO_ACK_INTO_RETIRED_DB,
    );
    let quiesce = position(&path, |a| *a == Action::Quiesce);
    let close = position(&path, |a| *a == Action::CloseSources);
    assert!(
        path[quiesce..close]
            .iter()
            .any(|a| matches!(a, Action::OpenDb(..))),
        "{path:?}"
    );
}

/// (e) The closed mark plus `close_checks_epoch` (the coordinator closes only
/// while `writer_epoch` is still the epoch of the writer it quiesced —
/// `ObjectStores::close_range_db_at`), every older fence of the open path off:
/// full exploration, no ack into a Db that is or will be retired, the flag of a
/// committed source stays, and the stale mark of a roll-back that died midway
/// is sometimes removed by itself. SM-13 (stated in terms of the old fence:
/// "nobody can write the source after the quiesce") breaks — rightly: the open
/// in the gap still happens, only the operation has to roll back.
#[test]
fn closed_flag_with_epoch_check_keeps_acks_for_split() {
    closed_flag_with_epoch_check_keeps_acks(
        "split, closed mark + epoch check, full",
        M1::split(Fences {
            close_checks_epoch: true,
            ..Fences::CLOSED_FLAG_ONLY
        }),
    );
}

/// (e') As (e), for merge.
#[test]
#[ignore = "~15 million states, a few minutes: run by hand with --ignored when changing M1 or SM-17"]
fn closed_flag_with_epoch_check_keeps_acks_for_merge() {
    closed_flag_with_epoch_check_keeps_acks(
        "merge, closed mark + epoch check, full",
        M1::merge(Fences {
            close_checks_epoch: true,
            ..Fences::CLOSED_FLAG_ONLY
        }),
    );
}

fn closed_flag_with_epoch_check_keeps_acks(name: &str, model: M1) {
    let c = check_all(name, model);
    assert!(c.is_done(), "BFS must explore everything");
    for p in [
        m1::SINGLE_WRITER,
        m1::NO_ACK_INTO_RETIRED_DB,
        m1::LS3_LEADER_IS_RECORDED_OWNER,
        m1::LS8_RETIRED_NEVER_REACQUIRED,
        m1::SM17_COMMITTED_SOURCE_STAYS_CLOSED,
    ] {
        assert!(c.discovery(p).is_none(), "{name}: \"{p}\" broken");
    }
    for p in [
        m1::SOMETIMES_CHILD_ACKED,
        m1::SOMETIMES_SOURCE_TOMBSTONED,
        m1::SOMETIMES_STALE_CLOSE_HEALED,
    ] {
        assert!(
            c.discovery(p).is_some(),
            "{name}: the model has no example for \"{p}\""
        );
    }
    assert!(c.discovery(m1::SM13_QUIESCED_SOURCE_CLOSED).is_some());
}

/// The result of [`trapped`].
struct Trap {
    states: usize,
    premise_states: usize,
    /// The (shortest) BFS path to a state satisfying the premise from which
    /// there is no path left to the goal.
    path: Option<Vec<Action>>,
}

/// Checks `AG(premise ⇒ EF goal)` — "from every reachable state satisfying
/// `premise`, a path to `goal` **still** exists" — over the whole state graph.
/// Stateright has no such operation (its `eventually` looks only at terminal
/// states, and the model has cycles), so this runs its own BFS (64-bit
/// fingerprints like stateright), keeps reverse edges, then floods backwards
/// from the `goal` states.
fn trapped(
    name: &str,
    model: &M1,
    premise: impl Fn(&State) -> bool,
    goal: impl Fn(&State) -> bool,
) -> Trap {
    fn fingerprint(s: &State) -> u64 {
        let mut h = DefaultHasher::new();
        s.hash(&mut h);
        h.finish()
    }
    let mut ids: BTreeMap<u64, u32> = BTreeMap::new();
    let mut parent: Vec<Option<(u32, Action)>> = Vec::new();
    let mut flags: Vec<(bool, bool)> = Vec::new();
    let mut queue: VecDeque<(State, u32)> = VecDeque::new();
    // Reverse edges (target, source).
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for s in model.init_states() {
        let id = parent.len() as u32;
        if ids.insert(fingerprint(&s), id).is_none() {
            parent.push(None);
            flags.push((premise(&s), goal(&s)));
            queue.push_back((s, id));
        }
    }
    let mut actions = Vec::new();
    while let Some((s, from)) = queue.pop_front() {
        actions.clear();
        model.actions(&s, &mut actions);
        for &a in &actions {
            let Some(t) = model.next_state(&s, a) else {
                continue;
            };
            let to = *ids.entry(fingerprint(&t)).or_insert_with(|| {
                let id = parent.len() as u32;
                parent.push(Some((from, a)));
                flags.push((premise(&t), goal(&t)));
                queue.push_back((t, id));
                id
            });
            edges.push((to, from));
        }
    }
    let n = parent.len();
    edges.sort_unstable();
    let mut start = vec![0usize; n + 1];
    for &(to, _) in &edges {
        start[to as usize + 1] += 1;
    }
    for i in 0..n {
        start[i + 1] += start[i];
    }
    let mut reach: Vec<bool> = flags.iter().map(|&(_, g)| g).collect();
    let mut stack: Vec<usize> = (0..n).filter(|&i| reach[i]).collect();
    while let Some(t) = stack.pop() {
        for &(_, from) in &edges[start[t]..start[t + 1]] {
            if !reach[from as usize] {
                reach[from as usize] = true;
                stack.push(from as usize);
            }
        }
    }
    let premise_states = flags.iter().filter(|&&(p, _)| p).count();
    let stuck = (0..n).filter(|&i| flags[i].0 && !reach[i]).count();
    println!(
        "{name}: {n} states, {} edges, {premise_states} satisfy the premise, {stuck} stuck",
        edges.len()
    );
    let path = (0..n).find(|&i| flags[i].0 && !reach[i]).map(|mut i| {
        let mut path = Vec::new();
        while let Some((from, a)) = parent[i] {
            path.push(a);
            i = from as usize;
        }
        path.reverse();
        println!("path to the stuck state ({} steps):", path.len());
        for (k, a) in path.iter().enumerate() {
            println!("  {:>2}. {a:?}", k + 1);
        }
        path
    });
    Trap {
        states: n,
        premise_states,
        path,
    }
}

/// The premise of SM-17's liveness: a live source (in the table, no intent)
/// carries the closed mark, and some node can still lead (a grant still valid,
/// or an incarnation left to open) — otherwise "cannot open" is due to running
/// out of nodes, not to the mark.
fn stale_close_with_a_leader_left(m: &M1, s: &State) -> bool {
    m.sources().iter().any(|&r| m.stale_close(s, r))
        && (0..m.nodes).any(|n| {
            let ss = &s.sessions[n as usize];
            !s.nodes[n as usize].dead
                && ((!ss.ended && ss.age < m.validity) || ss.inc < m.max_incarnations)
        })
}

fn no_stale_close(m: &M1, s: &State) -> bool {
    !m.sources().iter().any(|&r| m.stale_close(s, r))
}

/// (f) Liveness of the closed mark, all fences: from every state with a live
/// source carrying the closed mark (a roll-back that died midway, or a failed
/// removal) where some node can still lead, a path that removes the mark
/// **always remains** (`AG(premise ⇒ EF removed)`): the roll-backer removes it,
/// or the next Db open removes it by itself (`heal_stale_close`).
#[test]
fn stale_close_can_always_be_healed() {
    let m = M1::split(Fences::ALL);
    let t = trapped(
        "split, all fences: stale closed mark removable",
        &m,
        |s| stale_close_with_a_leader_left(&m, s),
        |s| no_stale_close(&m, s),
    );
    assert!(
        t.states > 0 && t.premise_states > 0,
        "the premise must be reachable"
    );
    assert!(t.path.is_none(), "live range stuck closed: {:?}", t.path);
}

/// (g) Without the self-healing step (`heal_stale_close`): the coordinator's
/// roll-back CAS wins, the closed-mark removal fails (or the node dies between
/// the two steps) ⇒ the live source is stuck closed forever — no path removes it
/// although some node can still lead.
///
/// Must see: close the source, roll-back CAS, `ReopenSources(_, Failed)`; from
/// there nothing can remove the mark.
#[test]
fn without_heal_a_live_range_stays_closed() {
    let m = M1::split(Fences {
        heal_stale_close: false,
        ..Fences::ALL
    });
    let t = trapped(
        "split, no self-healing: stale closed mark stuck",
        &m,
        |s| stale_close_with_a_leader_left(&m, s),
        |s| no_stale_close(&m, s),
    );
    let path = t.path.expect("there must be a stuck-closed state");
    let close = position(&path, |a| *a == Action::CloseSources);
    let roll_back = position(&path, |a| *a == Action::RollBack);
    let failed = position(&path, |a| matches!(a, Action::ReopenSources(_, Io::Failed)));
    assert!(close < roll_back && roll_back < failed, "{path:?}");
}

// ───────────────── A lone node (`solo`)

fn solo(fences: Fences) -> M1 {
    M1 {
        solo: true,
        ..M1::lease(fences)
    }
}

/// The lease invariants that must hold when one new fence is switched off
/// (what breaks is LS-3, not single-writer or stale reads — `writer_epoch` and
/// the read fence do not rely on the clock or the renewal protocol).
fn assert_writer_and_reads_hold(name: &str, model: M1) {
    let c = check_all(name, model);
    assert!(c.is_done(), "BFS must explore everything");
    assert!(
        c.discovery(m1::LS3_LEADER_IS_RECORDED_OWNER).is_some(),
        "{name}: LS-3 must break"
    );
    for p in [m1::SINGLE_WRITER, m1::NO_STALE_READ] {
        assert!(
            c.discovery(p).is_none(),
            "{name}: \"{p}\" broken: {:?}",
            c.discovery(p).map(|p| p.into_actions())
        );
    }
}

/// (s1) All fences, only the lease protocol (no split/merge), both nodes may
/// enter `solo` at any time (an over-approximation of "being the only
/// session"), challenge each other, answer or get expired: full exploration,
/// every invariant holds, and both branches have an example (answering a
/// challenge; the challenger expires and then takes the range).
#[test]
fn solo_holds_with_all_fences() {
    let c = check_all("lease, solo, all fences", solo(Fences::ALL));
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// Every invariant (`always`) of M1 — for configurations where some of the
/// split/merge `sometimes` cannot be reached (one incarnation per node).
const INVARIANTS: [&str; 7] = [
    m1::SINGLE_WRITER,
    m1::NO_ACK_INTO_RETIRED_DB,
    m1::SM13_QUIESCED_SOURCE_CLOSED,
    m1::LS3_LEADER_IS_RECORDED_OWNER,
    m1::LS8_RETIRED_NEVER_REACQUIRED,
    m1::SM17_COMMITTED_SOURCE_STAYS_CLOSED,
    m1::NO_STALE_READ,
];

fn assert_invariants_hold(name: &str, model: M1, sometimes: &[&'static str]) {
    let c = check_all(name, model);
    assert!(c.is_done(), "BFS must explore everything");
    for p in INVARIANTS {
        assert!(
            c.discovery(p).is_none(),
            "{name}: \"{p}\" broken: {:?}",
            c.discovery(p).map(|p| p.into_actions())
        );
    }
    for p in sometimes {
        assert!(
            c.discovery(p).is_some(),
            "{name}: the model has no example for \"{p}\""
        );
    }
}

/// (s1') As (s1) on the full split model, one incarnation per node (~6 million
/// states; two incarnations do not fit): `solo` breaks no split/merge
/// invariant (`NO_ACK_INTO_RETIRED_DB`, SM-13, SM-17), and the split still
/// completes.
#[test]
#[ignore = "~6 million states, ~1 minute in --release: run by hand with --ignored when changing M1"]
fn solo_holds_for_split_with_all_fences() {
    assert_invariants_hold(
        "split, solo, all fences, one incarnation",
        M1 {
            solo: true,
            max_incarnations: 1,
            ..M1::split(Fences::ALL)
        },
        &[
            m1::SOMETIMES_CHILD_ACKED,
            m1::SOMETIMES_SOLO_EXPIRED,
            m1::SOMETIMES_CHALLENGE_ANSWERED,
        ],
    );
}

/// (s2) The self-check GET renews even after it has seen the `challenged` line
/// (`solo_extend_needs_unchallenged_read` off).
///
/// Interleaving:
/// 1. the range owner enters `solo` (CAS won, renewed);
/// 2. the other node challenges (CAS adding `challenged`), τ = now;
/// 3. two ticks pass; right before the challenger expires it, the owner's
///    self-check GET sees `challenged` and renews anyway;
/// 4. the challenger CASes `expired` (the record stood unchanged one TTL after
///    the challenge) and takes the range.
///
/// Must see: the old owner still holds a valid grant while the owner record
/// has changed hands — LS-3.
#[test]
fn solo_extend_on_challenged_read_breaks_ls3() {
    let model = solo(Fences {
        solo_extend_needs_unchallenged_read: false,
        ..Fences::ALL
    });
    let path = first_counterexample(
        "lease, solo, renewal on a challenged record",
        model.clone(),
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
    let challenge = position(&path, |a| matches!(a, Action::Challenge(_)));
    let Action::Challenge(victim) = path[challenge] else {
        unreachable!()
    };
    let expire = position(&path, |a| *a == Action::ExpireChallenged(victim));
    let check = path[challenge..expire]
        .iter()
        .rposition(|a| *a == Action::SoloCheck(victim))
        .expect("the owner renews after the challenge");
    assert!(challenge + check < expire, "{path:?}");
    position(
        &path[expire..],
        |a| matches!(a, Action::Claim(m, _) if *m != victim),
    );
    assert_writer_and_reads_hold("lease, solo, renewal on a challenged record", model);
}

/// (s3) A newcomer expires a `solo` record on silence, without a challenge
/// (`expire_solo_needs_challenge_then_ttl` off): the `solo` node writes
/// nothing, so the beat stands still while the self-check GET keeps renewing
/// the grant.
///
/// Must see: no `Challenge` at all; the newcomer expires an owner that still
/// holds a grant (it just did a self-check GET) and takes the range — LS-3.
#[test]
fn expire_solo_without_challenge_breaks_ls3() {
    let model = solo(Fences {
        expire_solo_needs_challenge_then_ttl: false,
        ..Fences::ALL
    });
    let path = first_counterexample(
        "lease, solo, expiry without a challenge",
        model.clone(),
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
    let expire = position(&path, |a| matches!(a, Action::ExpireChallenged(_)));
    let Action::ExpireChallenged(victim) = path[expire] else {
        unreachable!()
    };
    assert!(
        !path.iter().any(|a| matches!(a, Action::Challenge(_))),
        "{path:?}"
    );
    position(&path[..expire], |a| *a == Action::SoloCheck(victim));
    assert_writer_and_reads_hold("lease, solo, expiry without a challenge", model);
}

/// (s4) The `control/quiet-lease` gate off: an observer running an old binary
/// (unaware of `mode=solo`) applies LS-1 to a `solo` record — a TTL of silence
/// since the last beat.
///
/// Must see: `ObserveExpired` (the old tracker's path) on a `solo` record being
/// renewed by self-check GET, then the other node takes the range — LS-3.
#[test]
fn old_observer_vs_solo_breaks_ls3() {
    let model = solo(Fences {
        quiet_lease_gate: false,
        ..Fences::ALL
    });
    let path = first_counterexample(
        "lease, solo, old-binary observer",
        model.clone(),
        m1::LS3_LEADER_IS_RECORDED_OWNER,
    );
    let enter = position(&path, |a| matches!(a, Action::EnterSolo(_)));
    let Action::EnterSolo(victim) = path[enter] else {
        unreachable!()
    };
    let observed = position(&path, |a| *a == Action::ObserveExpired(victim));
    assert!(enter < observed, "{path:?}");
    position(&path[enter..observed], |a| *a == Action::SoloCheck(victim));
    assert_writer_and_reads_hold("lease, solo, old-binary observer", model);
}

/// (s5) A lying clock (`validity > ttl`) in `solo` mode, two-phase reads, all
/// fences: the self-check GET is proof for the read fence just like a fence GET
/// (the same loop in the code), so there is no stale read — the read fence
/// does not depend on which path renewal takes. (LS-3 breaks, as in
/// `grant_outliving_ttl_breaks_ls3`.)
#[test]
fn read_fence_keeps_solo_reads_fresh_with_a_lying_clock() {
    let c = M1 {
        validity: 3,
        max_incarnations: 1,
        split_reads: true,
        ..solo(Fences::ALL)
    }
    .checker()
    .finish_when(HasDiscoveries::AnyOf([m1::NO_STALE_READ].into()))
    .spawn_bfs()
    .join();
    report("lease, solo, validity > TTL, two-phase read", &c);
    assert!(c.is_done(), "BFS must explore everything");
    assert!(
        c.discovery(m1::NO_STALE_READ).is_none(),
        "solo let a stale read through: {:?}",
        c.discovery(m1::NO_STALE_READ).map(|p| p.into_actions())
    );
}

// ───────────────── Delegated renewal

/// Lease, one delegating node (node 1; node 0 is always `direct` — observer and
/// claimer), one incarnation per node; a coordinator with `hub_incarnations`
/// incarnations and `terms` terms.
fn delegated(fences: Fences, hub_incarnations: u8, terms: u8) -> M1 {
    M1 {
        delegation: true,
        max_incarnations: 1,
        hub_incarnations,
        coordinator_terms: terms,
        ..M1::lease(fences)
    }
}

/// A coordinator with two terms and two incarnations: a zombie of the old
/// incarnation, taking the role back, re-delegating to a new term, the yield
/// rule for a record of the old term.
fn delegated_full(fences: Fences) -> M1 {
    delegated(fences, 2, 2)
}

/// The older invariants (`SINGLE_WRITER`, LS-3, LS-8, `NO_STALE_READ`) and every
/// delegation `sometimes`: an ack renews, the coordinator expires, the yield
/// rule.
fn assert_delegation_holds(name: &str, model: M1) {
    let c = check_all(name, model);
    assert!(c.is_done(), "BFS must explore everything");
    c.assert_properties();
}

/// The ack-rule-6 fence only **removes** acks (on, the ack condition is
/// stronger, every other transition is the same), so the configuration with it
/// off has every behaviour of the one with it on: checking the off version
/// (smaller — no tracking of bucket touches) checks the on version too. The
/// test `delegation_holds_with_all_fences` checks the on version directly in a
/// small configuration.
const ALL_BUT_TOUCH: Fences = Fences {
    self_check_gates_ack: false,
    ..Fences::ALL
};

/// (d1) All fences, a coordinator with one incarnation and one term: full
/// exploration, every invariant holds, with examples for an ack renewing, the
/// coordinator expiring and the yield rule.
#[test]
fn delegation_holds_with_all_fences() {
    assert_delegation_holds("lease, delegation, fenced", delegated(Fences::ALL, 1, 1));
}

/// (d1') As (d1), two terms: taking the role back, re-delegating to a new
/// term, the yield rule for a record of the old term.
#[test]
fn delegation_holds_with_takeover() {
    assert_delegation_holds(
        "lease, delegation, all fences, two terms",
        delegated(ALL_BUT_TOUCH, 1, 2),
    );
}

/// (d1'') As (d1'), with one more coordinator incarnation: the old
/// incarnation's role is still in RAM (zombie) next to the new session.
#[test]
#[ignore = "~4 million states, ~1 minute in --release: run by hand with --ignored when changing M1 or the delegation protocol"]
fn delegation_holds_with_takeover_and_reopen() {
    assert_delegation_holds(
        "lease, delegation, all fences, two terms, two incarnations",
        delegated_full(ALL_BUT_TOUCH),
    );
}

/// The breaking row of one delegation fence: the shortest counterexample for
/// LS-3 (printed), then a full exploration of that configuration to see that
/// `SINGLE_WRITER` and `NO_STALE_READ` still hold.
fn delegation_breaks(name: &str, fences: Fences) -> Vec<Action> {
    let model = delegated(fences, 1, 1);
    let path = first_counterexample(name, model.clone(), m1::LS3_LEADER_IS_RECORDED_OWNER);
    assert_writer_and_reads_hold(name, model);
    path
}

/// (d2) Ack rule 5 half off: the coordinator remembers only expiries **known**
/// to have won (`no_ack_after_expire` off).
///
/// Interleaving:
/// 1. the node delegates (CAS won), the coordinator acks and holds the
///    delegated version;
/// 2. a TTL with no heartbeat; the coordinator CASes `expired` If-Match that
///    version — ambiguous outcome, the re-read shows the record unchanged;
/// 3. a heartbeat arrives, the coordinator (not remembering the ambiguous
///    issue) acks, the node renews;
/// 4. the ambiguous `expired` PUT lands late (the record is still at that
///    version: the delegating node writes nothing), the other node takes the
///    range.
///
/// Must see: the node still holds a grant from the ack in (3) while the range
/// has changed hands — LS-3.
#[test]
fn ack_after_expire_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, ack after an ambiguous expiry",
        Fences {
            no_ack_after_expire: false,
            ..Fences::ALL
        },
    );
    let expire = position(&path, |a| {
        *a == Action::CoordinatorExpire(1, Expiry::Ambiguous)
    });
    let lands = position(&path, |a| *a == Action::ExpireLands(1));
    let ack = path[expire..lands]
        .iter()
        .position(|a| *a == Action::HeartbeatAck(1))
        .expect("an ack between the ambiguous issue and the PUT landing");
    assert!(expire + ack < lands, "{path:?}");
    position(&path[lands..], |a| matches!(a, Action::Claim(0, _)));
}

/// (d3) The node uses an ack before its delegation CAS is confirmed
/// (`ack_needs_confirmed_delegation` off).
///
/// Interleaving:
/// 1. the node issues a `mode=delegated` CAS — ambiguous, **not landed**: the
///    record is still `direct`;
/// 2. the node stops PUTting renewals, living on the coordinator's `granted`;
/// 3. a TTL with no beat: a `direct` observer applies LS-1 (the record is
///    `direct`) and CASes `expired`;
/// 4. the coordinator's `granted` arrives (it does not know the record has
///    ended — it did not expire it), the node renews, then the other node takes
///    the range.
///
/// Must see: the node's grant comes from an ack, not from any write — LS-3.
#[test]
fn ack_before_delegation_confirmed_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, ack before the delegation CAS is confirmed",
        Fences {
            ack_needs_confirmed_delegation: false,
            ..Fences::ALL
        },
    );
    let delegate = path
        .iter()
        .rposition(|a| *a == Action::Delegate(1, Cas::AmbiguousLost))
        .expect("an ambiguous delegation CAS that did not land");
    let claim = position(&path, |a| matches!(a, Action::Claim(0, _)));
    assert!(
        path[delegate..claim].contains(&Action::HeartbeatAck(1))
            && path[delegate..claim].contains(&Action::ObserveExpired(1)),
        "{path:?}"
    );
    assert!(
        !path
            .iter()
            .any(|a| matches!(a, Action::Delegate(1, Cas::Won))),
        "{path:?}"
    );
}

/// (d4) The coordinator CASes `expired` blindly, not If-Match the delegated
/// version (`expire_if_match_delegated` off).
///
/// Interleaving:
/// 1. the node delegates, the coordinator acks and holds the version;
/// 2. the node falls back to `direct` (CAS, `beat+1`) and renews `direct` as
///    usual — no more heartbeats;
/// 3. the coordinator's τ is one TTL old; it writes `expired` over the `direct`
///    record being renewed; the other node takes the range.
///
/// Must see: `Fallback` before the expiry, the node still holding a `direct`
/// grant — LS-3.
#[test]
fn blind_expire_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, blind expiry",
        Fences {
            expire_if_match_delegated: false,
            ..Fences::ALL
        },
    );
    let fallback = position(&path, |a| *a == Action::Fallback(1));
    let expire = position(&path, |a| *a == Action::CoordinatorExpire(1, Expiry::Won));
    assert!(fallback < expire, "{path:?}");
}

/// (d5) A zombie coordinator acks without the fence (`coordinator_fence` off).
///
/// Interleaving:
/// 1. the node delegates to the coordinator (term 1);
/// 2. the coordinator's session ends (`lose`, or expired) — the role in RAM
///    remains (the clock stalled, `lose` has not dropped the role yet);
/// 3. the observer sees the role lost and waits one TTL (the yield rule) —
///    meanwhile the zombie keeps acking and the node keeps renewing;
/// 4. the observer expires the delegated record and takes the range.
///
/// Must see: an ack after `CoordinatorLose` (or `CoordinatorRelease`) renews,
/// then `ObserveExpired` — LS-3.
#[test]
fn zombie_coordinator_without_fence_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, unfenced zombie",
        Fences {
            coordinator_fence: false,
            ..Fences::ALL
        },
    );
    let lost = position(&path, |a| {
        matches!(a, Action::CoordinatorLose | Action::CoordinatorRelease)
    });
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    assert!(
        path[lost..observed].contains(&Action::HeartbeatAck(1)),
        "{path:?}"
    );
}

/// (d6) The gate off: an observer running an old binary applies LS-1 to a
/// `delegated` record (`delegation_gate` off).
///
/// Must see: the coordinator keeps its role throughout (no `CoordinatorLose`/
/// `Release`), the node lives on acks, yet the record, silent for a TTL, is
/// expired — LS-3.
#[test]
fn old_observer_without_gate_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, old-binary observer",
        Fences {
            delegation_gate: false,
            ..Fences::ALL
        },
    );
    assert!(
        !path.iter().any(|a| matches!(
            a,
            Action::CoordinatorLose | Action::CoordinatorRelease | Action::CoordinatorTakeover
        )),
        "{path:?}"
    );
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    assert!(
        path[..observed].contains(&Action::HeartbeatAck(1)),
        "{path:?}"
    );
}

/// (d7) An observer (including a node that just fell back) that sees the
/// coordinator lose its role applies LS-1 at once, without waiting a TTL
/// (`observer_defers_to_coordinator` off).
///
/// Interleaving:
/// 1. the node delegates and writes nothing more (record silent ≥ TTL), living
///    on acks;
/// 2. the coordinator acks one last time, then releases its role (or its
///    session ends);
/// 3. the observer sees the role lost, the record already silent for a TTL ⇒
///    expires at once and takes the range — while the grant from the ack in (2)
///    has not run out.
#[test]
fn fallback_observer_expiring_delegated_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, observer does not yield",
        Fences {
            observer_defers_to_coordinator: false,
            ..Fences::ALL
        },
    );
    let ack = path
        .iter()
        .rposition(|a| *a == Action::HeartbeatAck(1))
        .expect("there is an ack");
    let lost = position(&path, |a| {
        matches!(a, Action::CoordinatorLose | Action::CoordinatorRelease)
    });
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    assert!(ack < observed && lost < observed, "{path:?}");
    assert!(
        !path[lost..observed].contains(&Action::Tick)
            || !path[ack..observed].contains(&Action::Tick),
        "expiry in the same tick as the ack or the role loss: {path:?}"
    );
}

/// (d8) An ack revives the grant after the node has called `lose`
/// (`ack_respects_pause_cap` off). A finding of the model: in `direct` the
/// pause cap is only liveness, in delegated mode it is a safety fence.
///
/// Interleaving:
/// 1. the node delegates; the grant is paused (no ack), past the cap ⇒ `lose`:
///    writes `closed`, the session ends on the bucket;
/// 2. the other node takes the range (the owner's session is dead);
/// 3. the node's heartbeat (the heartbeat loop still runs) reaches the
///    coordinator — which has issued no `expired` CAS, so the fence passes —
///    and is acked; the node applies the ack.
#[test]
fn ack_after_own_lose_breaks_ls3() {
    let path = delegation_breaks(
        "lease, delegation, ack revives after lose",
        Fences {
            ack_respects_pause_cap: false,
            ..Fences::ALL
        },
    );
    let lose = path
        .iter()
        .rposition(|a| *a == Action::Renew(1))
        .expect("the node calls `lose` through the renewal loop");
    assert!(path[lose..].contains(&Action::HeartbeatAck(1)), "{path:?}");
}

/// (d9) The literal yield rule: the one-TTL wait counts only from when the
/// coordinator was seen to lose its role, and does not restart when the record
/// gets a new version (`yield_restarts_on_write` off). **A hole in the literal
/// rule**, found by the model.
///
/// Interleaving:
/// 1. the coordinator releases its role (or its session ends); the observer
///    starts counting;
/// 2. one TTL later, the node — still trusting the old `control/coordinator`
///    (a late `granted`, a stale `view`) — CASes `mode=delegated coord=C:1`:
///    the delegation CAS is a winning `direct` renewal, so the grant runs
///    again;
/// 3. the observer sees a record delegated to a role lost ≥ TTL ago ⇒ expires
///    it at once (If-Match that very new version — it wins) and takes the
///    range.
#[test]
fn delegating_to_a_lost_coordinator_breaks_ls3_with_literal_yield() {
    let path = delegation_breaks(
        "lease, delegation, literal yield rule",
        Fences {
            yield_restarts_on_write: false,
            ..Fences::ALL
        },
    );
    let lost = position(&path, |a| {
        matches!(a, Action::CoordinatorLose | Action::CoordinatorRelease)
    });
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    let write = path[lost..observed]
        .iter()
        .rposition(|a| matches!(a, Action::Delegate(1, Cas::Won) | Action::RecordWrite(1)))
        .expect("the node writes a delegated record after the role is lost");
    assert!(lost + write < observed, "{path:?}");
}

/// (d10) `delegated_write_no_extend` off (the node's CASes while delegated
/// renew the grant), the yield rule restarting on every version (proposed by
/// the model): full exploration, every invariant holds — with
/// `yield_restarts_on_write`, that fence is redundant for safety.
#[test]
fn extending_delegated_write_is_safe_with_yield_restart() {
    assert_delegation_holds(
        "lease, delegation, CAS while delegated renews",
        delegated(
            Fences {
                delegated_write_no_extend: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
    );
}

/// (d11) `self_check_gates_ack` off (an ack does not require a bucket-touch age
/// < TTL/3): full exploration, every invariant holds — as predicted: this fence
/// is liveness (a node cut off from the bucket does not keep its session
/// forever), pinned by a Rust test of the session code.
#[test]
fn self_check_gate_is_liveness_only() {
    assert_delegation_holds(
        "lease, delegation, no bucket-touch requirement",
        delegated(ALL_BUT_TOUCH, 1, 1),
    );
}

/// (d12) The incarnation part of the fence off (`fence_checks_incarnation`):
/// the coordinator opens a new incarnation, the old incarnation's role is still
/// in RAM.
///
/// Interleaving:
/// 1. the node delegates to coordinator `C/1` (term 1);
/// 2. session `C/1` ends, `C` opens `C/2`; the role (1, term 1) is still in RAM;
/// 3. fence: `control/coordinator` = `C/1:1` matches the role, the current
///    session `C/2` is still `live` ⇒ passes; ack, the node renews;
/// 4. the observer sees `C/1` ended ≥ TTL ago ⇒ expires the delegated record
///    and takes the range.
#[test]
fn reopened_coordinator_without_incarnation_check_breaks_ls3() {
    let model = delegated(
        Fences {
            fence_checks_incarnation: false,
            ..ALL_BUT_TOUCH
        },
        2,
        1,
    );
    let name = "lease, delegation, fence not comparing incarnation";
    let path = first_counterexample(name, model.clone(), m1::LS3_LEADER_IS_RECORDED_OWNER);
    let reopen = position(&path, |a| *a == Action::CoordinatorReopen);
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    assert!(
        path[reopen..observed].contains(&Action::HeartbeatAck(1)),
        "{path:?}"
    );
    assert_writer_and_reads_hold(name, model);
}

/// (d13) Ack rule 3 off (`ack_checks_heartbeat_term`): the coordinator in term
/// 2 acks a heartbeat from a node still delegated to term 1 — a record the
/// yield rule lets others expire.
///
/// Interleaving:
/// 1. the node delegates to term 1; the coordinator releases its role and takes
///    it back (term 2);
/// 2. the observer sees term 1 lost ≥ TTL ago ⇒ expires the record and takes
///    the range (before or after the take-over — it does not matter);
/// 3. the node has not re-delegated yet; its heartbeat carries term 1, the
///    coordinator (valid role in term 2, the fence passes, never issued
///    `expired` for this session) acks; the node renews.
#[test]
fn ack_for_an_old_term_breaks_ls3() {
    let model = delegated(
        Fences {
            ack_checks_heartbeat_term: false,
            ..ALL_BUT_TOUCH
        },
        1,
        2,
    );
    let name = "lease, delegation, ack of an old-term heartbeat";
    let path = first_counterexample(name, model.clone(), m1::LS3_LEADER_IS_RECORDED_OWNER);
    let takeover = position(&path, |a| *a == Action::CoordinatorTakeover);
    assert!(
        path[takeover..].contains(&Action::HeartbeatAck(1))
            && !path[takeover..]
                .iter()
                .any(|a| matches!(a, Action::Delegate(1, _))),
        "{path:?}"
    );
    position(&path, |a| *a == Action::ObserveExpired(1));
    assert_writer_and_reads_hold(name, model);
}

/// (d1‴) As (d1') with in-flight heartbeats that have their own send age
/// ([`M1::heartbeat_in_flight`]): checks the argument "one-step send-and-receive
/// dominates every delayed heartbeat" — the same result.
#[test]
#[ignore = "~5 million states, ~1 minute in --release: run by hand with --ignored when changing M1 or the delegation protocol"]
fn delegation_holds_with_heartbeats_in_flight() {
    assert_delegation_holds(
        "lease, delegation, all fences, heartbeats in flight, two terms",
        M1 {
            heartbeat_in_flight: true,
            ..delegated(ALL_BUT_TOUCH, 1, 2)
        },
    );
}

/// (d1⁗) Both nodes delegate (each node is both a delegator and the observer of
/// the other, including "a node that just fell back").
#[test]
#[ignore = "~23 million states (28 million before the coordinator counted only acked heartbeats), ~6 minutes and ~4 GB in --release: run by hand with --ignored when changing M1 or the delegation protocol"]
fn delegation_holds_with_two_delegators() {
    assert_delegation_holds(
        "lease, delegation, all fences, two delegating nodes",
        M1 {
            delegators: 2,
            ..delegated(ALL_BUT_TOUCH, 1, 1)
        },
    );
}

/// (d1⁵) Delegation on the full split model, a healthy coordinator
/// ([`M1::delegation_faults`] off — the delegation-specific faults are already
/// checked in the lease configurations, where they can break LS-3; both on is
/// (r3) `delegation_holds_for_split_with_all_faults`, explored by DFS): grants
/// renewed by acks, the coordinator expiring, nodes falling back break no
/// split/merge invariant (`NO_ACK_INTO_RETIRED_DB`, SM-13, SM-17), and the split
/// still completes.
#[test]
#[ignore = "~2 million states: run by hand with --ignored when changing M1 or the delegation protocol"]
fn delegation_holds_for_split() {
    assert_invariants_hold(
        "split, delegation, all fences, healthy coordinator",
        M1 {
            delegation: true,
            delegation_faults: false,
            max_incarnations: 1,
            hub_incarnations: 1,
            coordinator_terms: 1,
            ..M1::split(ALL_BUT_TOUCH)
        },
        &[
            m1::SOMETIMES_CHILD_ACKED,
            m1::SOMETIMES_ACK_EXTENDS,
            m1::SOMETIMES_COORDINATOR_EXPIRES,
        ],
    );
}

// ───────────────── A coordinator renewing passively

/// Lease, the coordinator **is node 0** ([`M1::hub_node`]) and may enter `solo`;
/// node 1 delegates to it. Two mechanisms run at once in the same process: node
/// 1's grant lives on node 0's acks, while node 0's grant (and range) lives on
/// an unchallenged self-check GET. Entering `solo` and challenging can happen at
/// any time (an over-approximation of the entry rule "every other session
/// delegates to us" and of the rule deferring a challenge "our record delegates
/// to that very session" — both only cost and liveness).
///
/// Needs **two** incarnations: the coordinator is node 0, so after its session
/// ends only its own later incarnation can take node 1's range — with one
/// incarnation nobody takes it, and the delegation breaking rows cannot break.
/// `faults` = [`M1::delegation_faults`] (ambiguous CASes, releasing/taking the
/// role): not needed for the zombie here — the old incarnation's role in RAM
/// survives after its session ends.
fn coordinator_solo(fences: Fences, incarnations: u8, terms: u8, faults: bool) -> M1 {
    M1 {
        delegation: true,
        solo: true,
        hub_node: true,
        max_incarnations: incarnations,
        coordinator_terms: terms,
        delegation_faults: faults,
        ..M1::lease(fences)
    }
}

/// The `sometimes` of the passive coordinator plus those of delegation and `solo`.
const COORDINATOR_SOLO_SOMETIMES: [&str; 7] = [
    m1::SOMETIMES_SOLO_HUB_ACKS,
    m1::SOMETIMES_SOLO_HUB_EXPIRED,
    m1::SOMETIMES_SOLO_EXPIRED,
    m1::SOMETIMES_CHALLENGE_ANSWERED,
    m1::SOMETIMES_ACK_EXTENDS,
    m1::SOMETIMES_COORDINATOR_EXPIRES,
    m1::SOMETIMES_YIELD_EXPIRES,
];

/// (q1) All fences (except ack rule 6, see [`ALL_BUT_TOUCH`]), two
/// incarnations, one term, no delegation-specific faults: full exploration,
/// every invariant holds, and the combination has examples — the coordinator in
/// `solo` acks a renewal for node 1; a challenger expires the coordinator's
/// `solo` record while node 1 is still delegated to it; the yield rule; node
/// 0's later incarnation takes the range back.
#[test]
fn coordinator_solo_holds_with_all_fences() {
    assert_invariants_hold(
        "lease, coordinator is a node, solo + delegation, all fences, two incarnations",
        coordinator_solo(ALL_BUT_TOUCH, 2, 1, false),
        &COORDINATOR_SOLO_SOMETIMES,
    );
}

/// (q1') The widest: two incarnations, two terms, **every** delegation fault
/// (ambiguous CASes, releasing and taking the role back, writes while
/// delegated) — ~101 million states, parallel DFS.
#[test]
#[ignore = "~101 million states, ~7 minutes and ~3 GB in --release, 6 threads: run by hand with --ignored when changing M1, the delegation protocol or solo"]
fn coordinator_solo_holds_with_all_faults() {
    assert_invariants_hold_dfs(
        "lease, coordinator is a node, solo + delegation, all fences, all faults, (2, 2)",
        coordinator_solo(ALL_BUT_TOUCH, 2, 2, true),
        false,
        &COORDINATOR_SOLO_SOMETIMES,
    );
}

/// A breaking row in the passive-coordinator configuration: the shortest
/// counterexample for LS-3 (printed), then a full exploration to see that
/// `SINGLE_WRITER` and `NO_STALE_READ` still hold.
fn coordinator_solo_breaks(name: &str, model: M1) -> Vec<Action> {
    let path = first_counterexample(name, model.clone(), m1::LS3_LEADER_IS_RECORDED_OWNER);
    assert_writer_and_reads_hold(name, model);
    path
}

/// (q2) The coordinator's self-check GET renews even when the record has been
/// challenged (`solo_extend_needs_unchallenged_read` off): the `solo` fence
/// still carries the coordinator's range while it also acks for others.
///
/// Interleaving:
/// 1. the coordinator (node 0) enters `solo`;
/// 2. node 1 challenges it, τ = now;
/// 3. two ticks; right before node 1 expires it, node 0's self-check GET sees
///    `challenged` and renews anyway;
/// 4. node 1 CASes `expired` and takes the range.
///
/// Must see: `SoloCheck(0)` between `Challenge(0)` and `ExpireChallenged(0)` — LS-3.
#[test]
fn coordinator_solo_extend_on_challenged_read_breaks_ls3() {
    let path = coordinator_solo_breaks(
        "lease, coordinator is a node, renewal on a challenged record",
        coordinator_solo(
            Fences {
                solo_extend_needs_unchallenged_read: false,
                ..Fences::ALL
            },
            1,
            1,
            false,
        ),
    );
    let challenge = position(&path, |a| *a == Action::Challenge(0));
    let expire = position(&path, |a| *a == Action::ExpireChallenged(0));
    assert!(
        path[challenge..expire].contains(&Action::SoloCheck(0)),
        "{path:?}"
    );
}

/// (q3) The coordinator fence off while the coordinator is a `solo` node: its
/// session ends (a challenger expires it, or `lose`) while the role in RAM
/// still acks.
///
/// Interleaving:
/// 1. node 1 delegates to the coordinator (node 0);
/// 2. node 0's session ends on the bucket — the role in RAM remains (zombie);
/// 3. the observer sees the role lost and waits one TTL (the yield rule) —
///    meanwhile the zombie keeps acking and node 1 keeps renewing;
/// 4. the yield rule expires node 1; node 0's later incarnation takes the
///    range.
///
/// Must see: an ack for node 1 **after** node 0's session ended, then
/// `ObserveExpired(1)` — LS-3.
#[test]
fn coordinator_solo_without_fence_breaks_ls3() {
    let model = coordinator_solo(
        Fences {
            coordinator_fence: false,
            ..ALL_BUT_TOUCH
        },
        2,
        1,
        false,
    );
    let path = coordinator_solo_breaks("lease, coordinator is a node, unfenced", model.clone());
    // Replay to learn when the coordinator's session ended.
    let mut s = model.init_states().remove(0);
    let mut ended_at = None;
    for (i, a) in path.iter().enumerate() {
        s = model.next_state(&s, *a).expect("a valid step");
        if ended_at.is_none() && s.sessions[0].ended {
            ended_at = Some(i);
        }
    }
    let ended = ended_at.expect("the coordinator's session ends");
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    assert!(
        path[ended..observed].contains(&Action::HeartbeatAck(1)),
        "{path:?}"
    );
}

/// (q4) The observer does not yield while the coordinator is a `solo` node
/// (`observer_defers_to_coordinator` off): node 1's record is expired as soon
/// as the role is lost (the coordinator's session ends), while the grant from
/// its last ack has not run out.
///
/// Must see: `ObserveExpired(1)` after the last ack, in the LS-3 trace.
#[test]
fn coordinator_solo_observer_not_deferring_breaks_ls3() {
    let path = coordinator_solo_breaks(
        "lease, coordinator is a node, observer does not yield",
        coordinator_solo(
            Fences {
                observer_defers_to_coordinator: false,
                ..ALL_BUT_TOUCH
            },
            2,
            1,
            false,
        ),
    );
    let ack = path
        .iter()
        .rposition(|a| *a == Action::HeartbeatAck(1))
        .expect("there is an ack");
    let observed = position(&path, |a| *a == Action::ObserveExpired(1));
    assert!(ack < observed, "{path:?}");
}

// ───────────────── Wider models

/// Thread count of the large explorations: `CAIRN_MODEL_THREADS`, half the
/// cores by default (the machine is shared with other work).
fn threads() -> usize {
    std::env::var("CAIRN_MODEL_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| (n.get() / 2).max(1)))
}

/// **Full** exploration by parallel DFS. Unlike [`check_all`]: stateright's BFS
/// keeps a parent fingerprint for every state and the whole state queue (~200
/// bytes per state — 30 million is 6 GB), while DFS keeps only the fingerprint
/// set and a stack, so it fits hundreds of millions of states. The price: the
/// counterexample is not the shortest — the "holds" tests do not need it.
/// `symmetry` switches on [`m1::node_symmetry`] (stateright reduces symmetry
/// only in DFS).
fn check_all_dfs(name: &str, model: M1, symmetry: bool) -> impl Checker<M1> {
    explore_dfs(name, model, symmetry, HasDiscoveries::AllFailures)
}

#[allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "only measures the wall time of one model-check run to print it, does not affect the result"
)]
fn explore_dfs(name: &str, model: M1, symmetry: bool, until: HasDiscoveries) -> impl Checker<M1> {
    assert!(
        !symmetry || model.node_symmetric(),
        "{name}: the configuration is not symmetric under the node permutation"
    );
    let threads = threads();
    let started = std::time::Instant::now();
    let mut b = model.checker().threads(threads).finish_when(until);
    if symmetry {
        b = b.symmetry_fn(m1::node_symmetry);
    }
    let c = b.spawn_dfs().join();
    report(name, &c);
    println!(
        "{name}: DFS {threads} threads, symmetry reduction {symmetry}, {:.0} s",
        started.elapsed().as_secs_f64()
    );
    c
}

/// As [`assert_invariants_hold`], explored with [`check_all_dfs`].
fn assert_invariants_hold_dfs(name: &str, model: M1, symmetry: bool, sometimes: &[&'static str]) {
    let c = check_all_dfs(name, model, symmetry);
    assert!(c.is_done(), "{name}: DFS must explore everything");
    for p in INVARIANTS {
        assert!(
            c.discovery(p).is_none(),
            "{name}: \"{p}\" broken: {:?}",
            c.discovery(p).map(|p| p.into_actions())
        );
    }
    for p in sometimes {
        assert!(
            c.discovery(p).is_some(),
            "{name}: the model has no example for \"{p}\""
        );
    }
}

/// The delegation `sometimes` (an ack renews, the coordinator expires, the yield rule).
const DELEGATION_SOMETIMES: [&str; 3] = [
    m1::SOMETIMES_ACK_EXTENDS,
    m1::SOMETIMES_COORDINATOR_EXPIRES,
    m1::SOMETIMES_YIELD_EXPIRES,
];

/// (r1) The symmetry reduction [`m1::node_symmetry`] is sound — checks the
/// items of the argument in its doc directly on the reachable states (its own
/// BFS, up to `LIMIT` states per configuration) of every symmetric
/// configuration the tests use or could use: the initial state is fixed under
/// π; the action set is closed under π; `next(π(s), π(a)) = π(next(s, a))`;
/// every property gives the same result at s and π(s). A field carrying a node
/// id that [`m1::State::swapped`] forgets to swap, or a node constant in
/// `step`, turns this test red.
#[test]
fn node_symmetry_commutes_with_every_step() {
    const LIMIT: usize = 60_000;
    let configs = [
        (
            "lease, two delegating nodes, (2, 2)",
            M1 {
                delegators: 2,
                ..delegated_full(Fences::ALL)
            },
        ),
        (
            "lease, two delegating nodes, heartbeats in flight",
            M1 {
                delegators: 2,
                heartbeat_in_flight: true,
                ..delegated(Fences::ALL, 1, 2)
            },
        ),
        ("lease, solo", solo(Fences::ALL)),
        (
            "split, one crash, two-phase read, torn objects",
            M1 {
                max_crashes: 1,
                split_reads: true,
                torn_views: true,
                ..M1::split(Fences::ALL)
            },
        ),
        (
            "split, two delegating nodes",
            M1 {
                delegation: true,
                delegators: 2,
                ..M1::split(Fences::ALL)
            },
        ),
    ];
    for (name, model) in configs {
        assert!(model.node_symmetric(), "{name}");
        let properties = model.properties();
        let mut seen = std::collections::BTreeSet::new();
        let mut queue = VecDeque::new();
        for s in model.init_states() {
            assert_eq!(s.swapped(), s, "{name}: initial state not fixed under π");
            if seen.insert(fingerprint(&s)) {
                queue.push_back(s);
            }
        }
        let (mut actions, mut mirrored) = (Vec::new(), Vec::new());
        while let Some(s) = queue.pop_front() {
            let p = s.swapped();
            assert_eq!(p.swapped(), s, "{name}: π is not an involution");
            for prop in &properties {
                assert_eq!(
                    (prop.condition)(&model, &s),
                    (prop.condition)(&model, &p),
                    "{name}: \"{}\" differs at s and π(s): {s:?}",
                    prop.name
                );
            }
            actions.clear();
            mirrored.clear();
            model.actions(&s, &mut actions);
            model.actions(&p, &mut mirrored);
            let mut want: Vec<Action> = actions.iter().map(|a| a.swapped()).collect();
            want.sort_by_key(action_key);
            mirrored.sort_by_key(action_key);
            assert_eq!(want, mirrored, "{name}: action set not closed under π");
            for &a in &actions {
                let next = model.next_state(&s, a);
                assert_eq!(
                    model.next_state(&p, a.swapped()),
                    next.as_ref().map(State::swapped),
                    "{name}: step does not commute with π at {a:?} from {s:?}"
                );
                if let Some(t) = next {
                    if seen.len() < LIMIT && seen.insert(fingerprint(&t)) {
                        queue.push_back(t);
                    }
                }
            }
        }
        println!("{name}: {} states checked", seen.len());
    }
}

fn fingerprint(s: &State) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

/// Sort key of an action (`Action` has no `Ord`): compares two lists as two
/// multisets.
fn action_key(a: &Action) -> u64 {
    let mut h = DefaultHasher::new();
    a.hash(&mut h);
    h.finish()
}

/// (r2) The symmetry reduction hides no bug: every LS-3 breaking row of the
/// delegation matrix (d2–d9, d12, d13), run in the configuration with **two**
/// delegating nodes with the reduction on, still breaks LS-3. Two delegating
/// nodes have every behaviour of one delegating node (delegating is an action
/// a node need not take), so the original row's counterexample exists here; a
/// reduction hiding it turns the test red. (DFS to the first counterexample:
/// the trace is not the shortest, so its shape is not asserted — the shapes are
/// pinned by the original rows.)
#[test]
#[ignore = "ten DFS runs to the first counterexample with two delegating nodes: run by hand with --ignored when changing M1 or node_symmetry"]
fn node_symmetry_keeps_every_delegation_break() {
    let rows: [(&str, Fences, u8, u8); 10] = [
        (
            "no_ack_after_expire",
            Fences {
                no_ack_after_expire: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "ack_needs_confirmed_delegation",
            Fences {
                ack_needs_confirmed_delegation: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "expire_if_match_delegated",
            Fences {
                expire_if_match_delegated: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "coordinator_fence",
            Fences {
                coordinator_fence: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "delegation_gate",
            Fences {
                delegation_gate: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "observer_defers_to_coordinator",
            Fences {
                observer_defers_to_coordinator: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "ack_respects_pause_cap",
            Fences {
                ack_respects_pause_cap: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "yield_restarts_on_write",
            Fences {
                yield_restarts_on_write: false,
                ..Fences::ALL
            },
            1,
            1,
        ),
        (
            "fence_checks_incarnation",
            Fences {
                fence_checks_incarnation: false,
                ..ALL_BUT_TOUCH
            },
            2,
            1,
        ),
        (
            "ack_checks_heartbeat_term",
            Fences {
                ack_checks_heartbeat_term: false,
                ..ALL_BUT_TOUCH
            },
            1,
            2,
        ),
    ];
    for (fence, fences, hub_incarnations, terms) in rows {
        let name = format!("two delegating nodes, symmetry reduction, {fence} off");
        let c = explore_dfs(
            &name,
            M1 {
                delegators: 2,
                ..delegated(fences, hub_incarnations, terms)
            },
            true,
            HasDiscoveries::AnyOf([m1::LS3_LEADER_IS_RECORDED_OWNER].into()),
        );
        let path = c
            .discovery(m1::LS3_LEADER_IS_RECORDED_OWNER)
            .unwrap_or_else(|| panic!("{name}: the reduction hides the LS-3 break"))
            .into_actions();
        println!("{name}: counterexample of {} steps", path.len());
    }
}

/// (r3) Split together with **every** delegation fault (too large for BFS):
/// ambiguous delegation CASes (landed / not landed), the coordinator's
/// ambiguous `expired` CAS and a late-landing PUT, CASes while delegated, the
/// coordinator releasing its role, its session ending (zombie) and taking the
/// role back in term 2 (the node re-delegates to the new term, or is stuck on
/// the old one — the yield rule) — interleaved freely with intent, quiesce,
/// close, clone, claiming the children, commit or roll-back. One delegating
/// node (node 1; node 0 `direct` — an asymmetric configuration, no reduction),
/// one incarnation per node, a coordinator with one incarnation, two terms (⊇
/// the (1, 1) configuration: term 2 is an action, not mandatory). Every M1
/// invariant holds — the split ones too (`NO_ACK_INTO_RETIRED_DB`, SM-13,
/// LS-8, SM-17) —, the split still completes, and every delegation branch has
/// an example.
#[test]
#[ignore = "~213 million states, ~25 minutes and ~3 GB in --release, 6 threads: run by hand with --ignored when changing M1 or the delegation protocol"]
fn delegation_holds_for_split_with_all_faults() {
    let mut sometimes = vec![m1::SOMETIMES_CHILD_ACKED];
    sometimes.extend(DELEGATION_SOMETIMES);
    assert_invariants_hold_dfs(
        "split, delegation (1, 2), all fences, all delegation faults",
        M1 {
            delegation: true,
            delegation_faults: true,
            max_incarnations: 1,
            hub_incarnations: 1,
            coordinator_terms: 2,
            ..M1::split(ALL_BUT_TOUCH)
        },
        false,
        &sometimes,
    );
}

/// (r4) Two delegating nodes with **two terms** (too large for BFS): each node
/// both delegates and observes the other, the coordinator releases and takes
/// its role back (term 2), a node re-delegates to the new term or stays stuck
/// on the old one (the yield rule of the observer — which is the other
/// delegating node), the coordinator opens a second incarnation (the old
/// incarnation's role still in RAM), plus every delegation fault. A symmetric
/// configuration: reduced by [`m1::node_symmetry`] (soundness: (r1), (r2));
/// without the reduction it doubles (measured: (1, 2) 77.1 million unreduced,
/// 38.6 million reduced).
#[test]
#[ignore = "~119 million states (representatives), ~8 minutes and ~3.5 GB in --release, 6 threads: run by hand with --ignored when changing M1 or the delegation protocol"]
fn delegation_holds_with_two_delegators_and_takeover() {
    let c = check_all_dfs(
        "lease, two delegating nodes, (2, 2), all fences",
        M1 {
            delegators: 2,
            ..delegated_full(ALL_BUT_TOUCH)
        },
        true,
    );
    assert!(c.is_done(), "DFS must explore everything");
    c.assert_properties();
}
